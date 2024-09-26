use crate::options::HEADER_SIZE;

use super::*;

/// The value log abstraction.
pub trait Writer
where
  Self: Reader,
  Self::Id: CheapClone + core::fmt::Debug,
{
  /// Returns the mutable reference to the reserved slice.
  ///
  /// ## Safety
  /// - The caller must ensure that the there is no others accessing reserved slice for either read or write.
  /// - This method is not thread-safe, so be careful when using it.
  #[allow(clippy::mut_from_ref)]
  #[inline]
  unsafe fn reserved_slice_mut(&self) -> &mut [u8] {
    let reserved = self.options().reserved();
    if reserved == 0 {
      return &mut [];
    }

    let allocator = self.allocator();
    let reserved_slice = allocator.reserved_slice_mut();
    &mut reserved_slice[HEADER_SIZE..]
  }

  /// Flushes the memory-mapped file to disk.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder};
  /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
  /// # std::fs::remove_file(&path);
  ///
  /// /// Create a new file without automatic syncing.
  /// let arena = unsafe {
  ///   Builder::new()
  ///     .with_sync(false)
  ///     .with_create_new(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .with_capacity(100)
  ///     .map_mut::<ValueLog, _>(&path).unwrap() };
  ///
  /// arena.flush().unwrap();
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  fn flush(&self) -> std::io::Result<()> {
    self.allocator().flush()
  }

  /// Flushes the memory-mapped file to disk asynchronously.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder};
  /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
  /// # std::fs::remove_file(&path);
  ///
  /// /// Create a new file without automatic syncing.
  /// let log = unsafe {
  ///   Builder::new()
  ///     .with_sync(false)
  ///     .with_create_new(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .with_capacity(100)
  ///     .map_mut::<ValueLog, _>(&path).unwrap()
  /// };
  ///
  /// log.flush().unwrap();
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  fn flush_async(&self) -> std::io::Result<()> {
    self.allocator().flush_async()
  }

  /// Flushes outstanding memory map modifications in the range to disk.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder};
  /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
  /// # std::fs::remove_file(&path);
  ///
  /// /// Create a new file without automatic syncing.
  /// let log = unsafe {
  ///   Builder::new()
  ///     .with_sync(false)
  ///     .with_create_new(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .with_capacity(100)
  ///     .map_mut::<ValueLog, _>(&path).unwrap()
  /// };
  ///
  /// log.flush_range(0, 50).unwrap();
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  fn flush_range(&self, offset: usize, len: usize) -> std::io::Result<()> {
    self.allocator().flush_range(offset, len)
  }

  /// Asynchronously flushes outstanding memory map modifications in the range to disk.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder};
  /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
  /// # std::fs::remove_file(&path);
  ///
  /// /// Create a new file without automatic syncing.
  /// let vlog = unsafe {
  ///   Builder::new()
  ///     .with_sync(false)
  ///     .with_create_new(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .with_capacity(100)
  ///     .map_mut::<ValueLog, _>(&path).unwrap()
  /// };
  ///
  /// vlog.flush_range_async(0, 50).unwrap();
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  fn flush_async_range(&self, offset: usize, len: usize) -> std::io::Result<()> {
    self.allocator().flush_async_range(offset, len)
  }

  /// Inserts a value into the log.
  #[inline]
  fn insert(&self, value: &[u8]) -> Result<ValuePointer<Self::Id>, Error> {
    insert_in::<_, ()>(
      self,
      ValueBuilder::new(value.len() as u32, |buf: &mut VacantBuffer<'_>| {
        buf.put_slice_unchecked(value);
        Ok(())
      }),
    )
    .map_err(|e| e.unwrap_right())
  }

  /// Inserts a tombstone value into the log.
  ///
  /// This method is almost the same as the [`insert`] method, the only difference is that
  /// this method will increases the discarded bytes of the log.
  #[inline]
  fn insert_tombstone(&self, value: &[u8]) -> Result<ValuePointer<Self::Id>, Error> {
    self
      .insert(value)
      .inspect(|_| self.allocator().increase_discarded(value.len() as u32))
  }
}

/// The extension trait for the [`Log`] trait.
///
/// The reason having a `LogExt` is that to make [`Log`] object-safe.
pub trait WriterExt
where
  Self: Writer,
  Self::Id: CheapClone + core::fmt::Debug,
{
  /// Inserts a generic value into the log.
  #[inline]
  fn insert_generic<T>(&self, value: &T) -> Result<ValuePointer<Self::Id>, Either<T::Error, Error>>
  where
    T: Type,
  {
    let encoded_len = value.encoded_len();
    self.insert_with(ValueBuilder::new(
      encoded_len as u32,
      |buf: &mut VacantBuffer<'_>| {
        buf.set_len(encoded_len);
        value.encode(buf).map(|_| ())
      },
    ))
  }

  /// Inserts a value into the log with a builder, the value is built in place.
  #[inline]
  fn insert_with<E>(
    &self,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<ValuePointer<Self::Id>, Either<E, Error>> {
    insert_in(self, vb)
  }

  /// Inserts a generic value into the log.
  ///
  /// This method is almost the same as the [`insert_generic`] method, the only difference is that
  /// this method will increases the discarded bytes of the log.
  #[inline]
  fn insert_generic_tombstone<T>(
    &self,
    value: &T,
  ) -> Result<ValuePointer<Self::Id>, Either<T::Error, Error>>
  where
    T: Type,
  {
    let encoded_len = value.encoded_len();
    self.insert_tombstone_with(ValueBuilder::new(
      encoded_len as u32,
      |buf: &mut VacantBuffer<'_>| {
        buf.set_len(encoded_len);
        value.encode(buf).map(|_| ())
      },
    ))
  }

  /// Inserts a value into the log with a builder, the value is built in place.
  ///
  /// This method is almost the same as the [`insert_with`] method, the only difference is that
  /// this method will increases the discarded bytes of the log.
  #[inline]
  fn insert_tombstone_with<E>(
    &self,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<ValuePointer<Self::Id>, Either<E, Error>> {
    let encoded_len = vb.size;
    insert_in(self, vb).inspect(|_| self.allocator().increase_discarded(encoded_len))
  }
}

impl<L> WriterExt for L
where
  L: Writer,
  L::Id: CheapClone + core::fmt::Debug,
{
}

/// Inserts a value into the log with a builder, the value is built in place.
fn insert_in<L: Writer + ?Sized, E>(
  l: &L,
  vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
) -> Result<ValuePointer<L::Id>, Either<E, Error>>
where
  L::Id: CheapClone + core::fmt::Debug,
{
  if vb.size == 0 {
    return Ok(ValuePointer::new(l.id().cheap_clone(), 0, 0));
  }

  let opts = l.options();
  let maximum = opts.max_value_size;
  let (value_len, builder) = vb.into_components();
  let len = value_len as usize + CHECKSUM_LEN;

  if len > maximum as usize {
    return Err(Either::Right(Error::value_too_large(len, maximum as usize)));
  }

  let allocator = l.allocator();
  let mut buf = allocator
    .alloc_bytes(len as u32)
    .map_err(|e| Either::Right(Error::from_insufficient_space(e)))?;

  let begin_offset = buf.offset();

  buf.set_len(value_len as usize);
  let mut vacant_buf =
    unsafe { VacantBuffer::new(value_len as usize, NonNull::new_unchecked(buf.as_mut_ptr())) };
  builder(&mut vacant_buf).map_err(Either::Left)?;

  let checksum = l.checksum(&vacant_buf);

  // SAFETY: `buf` is allocated with the exact size of `value.len() + CHECKSUM_LEN`.
  unsafe {
    buf.put_u64_le_unchecked(checksum);
  }

  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  if opts.sync && allocator.is_ondisk() {
    // TODO: we also need to flush the header
    allocator
      .flush_range(begin_offset, len)
      .map_err(|e| Either::Right(e.into()))?
  }

  // Safety: no need to drop
  unsafe {
    buf.detach();
  }

  Ok(ValuePointer::new(
    l.id().cheap_clone(),
    begin_offset as u32,
    value_len,
  ))
}

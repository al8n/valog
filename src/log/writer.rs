#![allow(clippy::type_complexity)]

use super::*;

/// The mutable value log abstraction.
pub trait LogWriter: Log {
  /// Inserts a value into the log.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{Builder, sync::ValueLog, LogWriter};
  ///
  /// let log = Builder::new().with_capacity(1024).alloc::<ValueLog>(0).unwrap();
  ///
  /// let vp = log.insert(b"Hello, valog!").unwrap();
  /// ```
  #[inline]
  fn insert(&self, value: &[u8]) -> Result<ValuePointer<Self::Id>, Error>
  where
    Self::Id: CheapClone + core::fmt::Debug,
  {
    let vb = ValueBuilder::new(value.len(), |buf: &mut VacantBuffer<'_>| {
      buf.put_slice_unchecked(value);
      Ok(())
    });
    insert_in::<_, ()>(self, vb).map_err(|e| e.unwrap_right())
  }

  /// Inserts a tombstone value into the log.
  ///
  /// This method is almost the same as the [`insert`](LogWriter::insert_tombstone) method, the only difference is that
  /// this method will increases the discarded bytes of the log.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{Builder, sync::ValueLog, LogWriter};
  ///
  /// let log = Builder::new().with_capacity(1024).alloc::<ValueLog>(0).unwrap();
  /// let vp = log.insert_tombstone(b"Hello, valog!").unwrap();
  /// ```
  #[inline]
  fn insert_tombstone(&self, value: &[u8]) -> Result<ValuePointer<Self::Id>, Error>
  where
    Self::Id: CheapClone + core::fmt::Debug,
  {
    self.insert(value).map(|vp| {
      self.allocator().increase_discarded(value.len() as u32);
      vp.with_tombstone()
    })
  }
}

/// The extension trait for the [`LogWriter`] trait.
///
/// The reason having a `LogWriterExt` is that to make [`LogWriter`] object-safe.
pub trait LogWriterExt: LogWriter {
  /// Inserts a generic value into the log.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{Builder, sync::ValueLog, LogWriterExt};
  ///
  /// let log = Builder::new().with_capacity(1024).alloc::<ValueLog>(0).unwrap();
  /// let vp = log.insert_generic(&"Hello, valog!".to_string()).unwrap();
  /// ```
  #[inline]
  fn insert_generic<T>(&self, value: &T) -> Result<ValuePointer<Self::Id>, Either<T::Error, Error>>
  where
    T: Type,
    Self::Id: CheapClone + core::fmt::Debug,
  {
    let encoded_len = value.encoded_len();
    self.insert_with(ValueBuilder::new(
      encoded_len,
      |buf: &mut VacantBuffer<'_>| value.encode_to_buffer(buf).map(|_| ()),
    ))
  }

  /// Inserts a value into the log with a builder, the value is built in place.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{Builder, sync::ValueLog, LogWriterExt, ValueBuilder, VacantBuffer};
  ///
  /// let log = Builder::new().with_capacity(1024).alloc::<ValueLog>(0).unwrap();
  /// let data = b"Hello, valog!";
  /// let vb = ValueBuilder::new(data.len(), |buf: &mut VacantBuffer<'_>| {
  ///   buf.put_slice(data)
  /// });
  /// let vp = log.insert_with(vb).unwrap();
  /// ```
  #[inline]
  fn insert_with<E>(
    &self,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<ValuePointer<Self::Id>, Either<E, Error>>
  where
    Self::Id: CheapClone + core::fmt::Debug,
  {
    insert_in(self, vb)
  }

  /// Inserts a generic value into the log.
  ///
  /// This method is almost the same as the [`insert_generic`](LogWriterExt::insert_generic) method, the only difference is that
  /// this method will increases the discarded bytes of the log.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{Builder, sync::ValueLog, LogWriterExt};
  ///
  /// let log = Builder::new().with_capacity(1024).alloc::<ValueLog>(0).unwrap();
  /// let vp = log.insert_generic_tombstone(&"Hello, valog!".to_string()).unwrap();
  /// ```
  #[inline]
  fn insert_generic_tombstone<T>(
    &self,
    value: &T,
  ) -> Result<ValuePointer<Self::Id>, Either<T::Error, Error>>
  where
    T: Type,
    Self::Id: CheapClone + core::fmt::Debug,
  {
    let encoded_len = value.encoded_len();
    self.insert_tombstone_with(ValueBuilder::new(
      encoded_len,
      |buf: &mut VacantBuffer<'_>| value.encode_to_buffer(buf).map(|_| ()),
    ))
  }

  /// Inserts a value into the log with a builder, the value is built in place.
  ///
  /// This method is almost the same as the [`insert_with`](LogWriterExt::insert_with) method, the only difference is that
  /// this method will increases the discarded bytes of the log.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{Builder, sync::ValueLog, LogWriterExt, ValueBuilder, VacantBuffer};
  ///
  /// let log = Builder::new().with_capacity(1024).alloc::<ValueLog>(0).unwrap();
  /// let data = b"Hello, valog!";
  /// let vb = ValueBuilder::new(data.len(), |buf: &mut VacantBuffer<'_>| {
  ///   buf.put_slice(data)
  /// });
  /// let vp = log.insert_tombstone_with(vb).unwrap();
  /// ```
  #[inline]
  fn insert_tombstone_with<E>(
    &self,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<ValuePointer<Self::Id>, Either<E, Error>>
  where
    Self::Id: CheapClone + core::fmt::Debug,
  {
    let encoded_len = vb.size;
    insert_in(self, vb).map(|vp| {
      self.allocator().increase_discarded(encoded_len as u32);
      vp.with_tombstone()
    })
  }
}

impl<L> LogWriterExt for L where L: LogWriter {}

/// Inserts a value into the log with a builder, the value is built in place.
fn insert_in<L: LogWriter + ?Sized, E>(
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
  let len = value_len + CHECKSUM_LEN;

  if len > maximum as usize {
    return Err(Either::Right(Error::value_too_large(len, maximum as usize)));
  }

  let allocator = l.allocator();
  let mut buf = allocator
    .alloc_bytes(len as u32)
    .map_err(|e| Either::Right(Error::from_insufficient_space(e)))?;

  let begin_offset = buf.offset();
  buf.set_len(value_len);

  // SAFETY: `buf` is allocated with the exact size of `value.len() + CHECKSUM_LEN`.
  unsafe {
    let ptr = NonNull::new_unchecked(buf.as_mut_ptr());
    let mut vacant_buf = VacantBuffer::new(value_len, ptr);
    builder(&mut vacant_buf).map_err(Either::Left)?;
    let checksum = l.checksum(&buf);
    buf.put_u64_le_unchecked(checksum);
  }

  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  if opts.sync && allocator.is_ondisk() {
    allocator
      .flush_header_and_range(begin_offset, len)
      .map_err(|e| Either::Right(e.into()))?;
  }

  // Safety: no need to drop
  unsafe {
    buf.detach();
  }

  Ok(ValuePointer::new(
    l.id().cheap_clone(),
    begin_offset as u32,
    value_len as u32,
  ))
}

/// Generic log writer abstraction.
pub trait GenericLogWriter: Log {
  /// The generic type stored in the log.
  type Type: Type;

  /// Inserts a generic value into the log.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{Builder, sync::GenericValueLog, GenericLogWriter};
  ///
  /// let log = Builder::new().with_capacity(1024).alloc::<GenericValueLog<String>>(0).unwrap();
  /// let vp = log.insert(&"Hello, valog!".to_string()).unwrap();
  /// ```
  fn insert(
    &self,
    value: &Self::Type,
  ) -> Result<ValuePointer<Self::Id>, Either<<Self::Type as Type>::Error, Error>>
  where
    Self::Id: CheapClone + core::fmt::Debug;

  /// Inserts a generic value into the log.
  ///
  /// This method is almost the same as the [`insert`](GenericLogWriter::insert) method, the only difference is that
  /// this method will increases the discarded bytes of the log.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{Builder, sync::GenericValueLog, GenericLogWriter};
  ///
  /// let log = Builder::new().with_capacity(1024).alloc::<GenericValueLog<String>>(0).unwrap();
  /// let vp = log.insert_tombstone(&"Hello, valog!".to_string()).unwrap();
  /// ```
  fn insert_tombstone(
    &self,
    value: &Self::Type,
  ) -> Result<ValuePointer<Self::Id>, Either<<Self::Type as Type>::Error, Error>>
  where
    Self::Id: CheapClone + core::fmt::Debug;
}

impl<L> GenericLogWriter for L
where
  L: common::AsLog,
  L::Type: Type,
  L::Log: LogWriter,
  <L::Log as Log>::Id: CheapClone + core::fmt::Debug,
{
  type Type = L::Type;

  #[inline]
  fn insert(
    &self,
    value: &Self::Type,
  ) -> Result<ValuePointer<Self::Id>, Either<<Self::Type as Type>::Error, Error>>
  where
    Self::Id: CheapClone + core::fmt::Debug,
  {
    self.as_log().insert_generic(value)
  }

  #[inline]
  fn insert_tombstone(
    &self,
    value: &Self::Type,
  ) -> Result<ValuePointer<Self::Id>, Either<<Self::Type as Type>::Error, Error>>
  where
    Self::Id: CheapClone + core::fmt::Debug,
  {
    self.as_log().insert_generic_tombstone(value)
  }
}

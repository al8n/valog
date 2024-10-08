use super::*;

/// The value log reader abstraction.
pub trait LogReader: Log {
  /// Reads a value from the log.
  ///
  /// ## Safety
  /// - The buffer `offset..offset + len` must hold a valid bytes sequence.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{Builder, sync::ValueLog, LogWriter, LogReader, Log};
  ///
  /// let log = Builder::new().with_capacity(1024).alloc::<ValueLog>(0).unwrap();
  ///
  /// let vp = log.insert(b"Hello, valog!").unwrap();
  /// let data = unsafe { log.read(log.id(), vp.offset(), vp.size()).unwrap() };
  /// assert_eq!(data, b"Hello, valog!");
  /// ```
  unsafe fn read(&self, id: &Self::Id, offset: u32, len: u32) -> Result<&[u8], Error>
  where
    Self::Id: Eq,
  {
    if self.id().ne(id) {
      return Err(Error::IdMismatch);
    }

    if len == 0 {
      return Ok(&[]);
    }

    let offset = offset as usize;
    let len = len as usize;

    let allocator = self.allocator();
    let allocated = allocator.allocated();
    let data_offset = allocator.data_offset();
    let opts = self.options();

    if offset < data_offset {
      return Err(Error::out_of_bounds(
        offset as u32,
        (len + CHECKSUM_LEN) as u32,
        data_offset as u32,
        allocated as u32,
      ));
    }

    if (offset + len + CHECKSUM_LEN) > allocated {
      return Err(Error::out_of_bounds(
        offset as u32,
        (len + CHECKSUM_LEN) as u32,
        data_offset as u32,
        allocated as u32,
      ));
    }

    // Safety: we have checked the bounds
    let buf = unsafe { allocator.get_bytes(offset, len + CHECKSUM_LEN) };

    if opts.validate_checksum {
      let checksum = u64::from_le_bytes((&buf[len..len + CHECKSUM_LEN]).try_into().unwrap());
      let digest = self.checksum(&buf[..len]);
      if checksum != digest {
        return Err(Error::checksum_mismatch());
      }
    }

    Ok(&buf[..len])
  }
}

/// The extension trait for the [`LogReader`] trait.
///
/// The reason having a `LogReaderExt` is that to make [`LogReader`] object-safe.
pub trait LogReaderExt: LogReader {
  /// Reads a generic value from the log at the given offset.
  ///
  /// ## Safety
  /// - The buffer `offset..offset + len` must hold a valid bytes sequence which created by encoding a value of type `T` through [`Type::encode`](Type::encode).
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{Builder, sync::ValueLog, LogWriterExt, LogReaderExt, Log};
  ///
  /// let log = Builder::new().with_capacity(1024).alloc::<ValueLog>(0).unwrap();
  ///
  /// let vp = log.insert_generic(&"Hello, valog!".to_string()).unwrap();
  ///
  /// let data = unsafe { log.read_generic::<String>(log.id(), vp.offset(), vp.size()).unwrap() };
  ///
  /// assert_eq!(data, "Hello, valog!");
  /// ```
  unsafe fn read_generic<T: Type>(
    &self,
    id: &Self::Id,
    offset: u32,
    len: u32,
  ) -> Result<T::Ref<'_>, Error>
  where
    Self::Id: Eq,
  {
    self
      .read(id, offset, len)
      .map(|buf| <T::Ref<'_> as TypeRef>::from_slice(buf))
  }
}

impl<L: LogReader> LogReaderExt for L {}

/// The immutable generic value log reader abstraction.
pub trait GenericLogReader: Log {
  /// The generic type stored in the log.
  type Type;

  /// Reads a generic value from the log at the given offset.
  ///
  /// ## Safety
  /// - The buffer `offset..offset + len` must hold a valid bytes sequence which created by encoding a value of type `T` through [`Type::encode`](Type::encode).
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{Builder, sync::GenericValueLog, GenericLogWriter, GenericLogReader, Log};
  ///
  /// let log = Builder::new().with_capacity(1024).alloc::<GenericValueLog<String>>(0).unwrap();
  ///
  /// let vp = log.insert(&"Hello, valog!".to_string()).unwrap();
  ///
  /// let data = unsafe { log.read(log.id(), vp.offset(), vp.size()).unwrap() };
  ///
  /// assert_eq!(data, "Hello, valog!");
  /// ```
  unsafe fn read(
    &self,
    id: &Self::Id,
    offset: u32,
    len: u32,
  ) -> Result<<Self::Type as Type>::Ref<'_>, Error>
  where
    Self::Type: Type,
    Self::Id: Eq;
}

impl<L> GenericLogReader for L
where
  L: common::AsLog,
  L::Log: LogReader,
{
  type Type = L::Type;

  unsafe fn read(
    &self,
    id: &<L::Log as Log>::Id,
    offset: u32,
    len: u32,
  ) -> Result<<Self::Type as Type>::Ref<'_>, Error>
  where
    Self::Type: Type,
    Self::Id: Eq,
  {
    self.as_log().read_generic::<Self::Type>(id, offset, len)
  }
}

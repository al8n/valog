use super::*;

/// The value log reader abstraction.
pub trait LogReader: Log {
  /// Reads a value from the log.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{Builder, sync::ValueLog, LogWriter, LogReader};
  ///
  /// let log = Builder::new().with_capacity(1024).alloc::<ValueLog>(0).unwrap();
  ///
  /// let vp = log.insert(b"Hello, valog!").unwrap();
  /// let data = log.read(vp.offset(), vp.size()).unwrap();
  /// assert_eq!(data, b"Hello, valog!");
  /// ```
  fn read(&self, offset: u32, len: u32) -> Result<&[u8], Error> {
    if offset == 0 && len == 0 {
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
  /// use valog::{Builder, sync::ValueLog, LogWriterExt, LogReaderExt};
  ///
  /// let log = Builder::new().with_capacity(1024).alloc::<ValueLog>(0).unwrap();
  ///
  /// let vp = log.insert_generic(&"Hello, valog!".to_string()).unwrap();
  ///
  /// let data = unsafe { log.read_generic::<String>(vp.offset(), vp.size()).unwrap() };
  ///
  /// assert_eq!(data, "Hello, valog!");
  /// ```
  unsafe fn read_generic<T: Type>(&self, offset: u32, len: u32) -> Result<T::Ref<'_>, Error> {
    self
      .read(offset, len)
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
  /// use valog::{Builder, sync::GenericValueLog, GenericLogWriter, GenericLogReader};
  ///
  /// let log = Builder::new().with_capacity(1024).alloc::<GenericValueLog<String>>(0).unwrap();
  ///
  /// let vp = log.insert(&"Hello, valog!".to_string()).unwrap();
  ///
  /// let data = unsafe { log.read(vp.offset(), vp.size()).unwrap() };
  ///
  /// assert_eq!(data, "Hello, valog!");
  /// ```
  unsafe fn read(&self, offset: u32, len: u32) -> Result<<Self::Type as Type>::Ref<'_>, Error>
  where
    Self::Type: Type;
}

// impl<L> Log for L
// where
//   L: AsLogReader + sealed::Sealed,
//   L::Reader: LogReader,
// {
//   type Id = <L::Reader as Log>::Id;

//   #[inline]
//   fn id(&self) -> &Self::Id {
//     self.as_reader().id()
//   }

//   #[inline]
//   fn checksum(&self, bytes: &[u8]) -> u64 {
//     self.as_reader().checksum(bytes)
//   }

//   #[inline]
//   fn options(&self) -> &Options {
//     self.as_reader().options()
//   }
// }

impl<L> GenericLogReader for L
where
  L: common::AsLog,
  L::Log: LogReader,
{
  type Type = L::Type;

  unsafe fn read(&self, offset: u32, len: u32) -> Result<<Self::Type as Type>::Ref<'_>, Error>
  where
    Self::Type: Type,
  {
    self.as_log().read_generic::<Self::Type>(offset, len)
  }
}

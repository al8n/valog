use super::*;

/// The immutable value log abstraction.
pub trait Reader: sealed::Sealed {
  /// The identifier type (file ID) for the log.
  type Id;

  /// Returns the identifier of the log.
  fn id(&self) -> &Self::Id;

  /// Calculates the checksum of the given bytes.
  fn checksum(&self, bytes: &[u8]) -> u64;

  /// Returns the options of the log.
  fn options(&self) -> &Options;

  /// Returns the magic version of the log.
  fn magic_version(&self) -> u16;

  /// Returns the version of the log.
  #[inline]
  fn version(&self) -> u16 {
    self.allocator().magic_version()
  }

  /// Returns the discarded bytes of the log.
  #[inline]
  fn discarded(&self) -> u32 {
    self.allocator().discarded()
  }

  /// Reads a value from the log.
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

/// The extension trait for the [`Reader`] trait.
///
/// The reason having a `ReaderExt` is that to make [`Reader`] object-safe.
pub trait ReaderExt: Reader {
  /// Reads a generic value from the log at the given offset.
  ///
  /// ## Safety
  /// - The buffer `offset..offset + len` must hold a valid bytes sequence which created by encoding a value of type `T` through [`Type::encode`](Type::encode).
  unsafe fn read_generic<T: Type>(&self, offset: u32, len: u32) -> Result<T::Ref<'_>, Error> {
    self
      .read(offset, len)
      .map(|buf| <T::Ref<'_> as TypeRef>::from_slice(buf))
  }
}

impl<L: Reader> ReaderExt for L {}

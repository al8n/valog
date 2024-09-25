/// Error type for the value logs.
pub enum Error {
  /// Returned there are no more enough space in the value log.
  InsufficientSpace {
    /// The requested size
    requested: u32,
    /// The remaining size
    available: u32,
  },

  /// Returned when the value is too large to be inserted into the value log.
  ValueTooLarge {
    /// The value size
    size: usize,
    /// The maximum value size
    maximum: usize,
  },

  /// Returned when trying to read a value with an offset and len that is out of bounds.
  OutOfBounds {
    /// The offset
    offset: u32,
    /// The length
    len: u32,
    /// The data offset
    data_offset: u32,
    /// The end offset
    end_offset: u32,
  },

  /// Returned when checksum verification fails.
  ChecksumMismatch,

  /// Returned when an IO error occurs.
  IO(std::io::Error),
}

impl From<std::io::Error> for Error {
  fn from(err: std::io::Error) -> Self {
    Error::IO(err)
  }
}

impl Error {
  #[inline]
  pub(crate) const fn value_too_large(size: usize, maximum: usize) -> Self {
    Self::ValueTooLarge { size, maximum }
  }

  #[inline]
  pub(crate) const fn out_of_bounds(
    offset: u32,
    len: u32,
    data_offset: u32,
    end_offset: u32,
  ) -> Self {
    Self::OutOfBounds {
      offset,
      len,
      data_offset,
      end_offset,
    }
  }

  #[inline]
  pub(crate) const fn checksum_mismatch() -> Self {
    Self::ChecksumMismatch
  }

  #[inline]
  pub(crate) const fn from_insufficient_space(err: rarena_allocator::Error) -> Self {
    match err {
      rarena_allocator::Error::InsufficientSpace {
        requested,
        available,
      } => Self::InsufficientSpace {
        requested,
        available,
      },
      _ => unreachable!(),
    }
  }
}

use crate::options::Options;

use super::Allocator;

pub trait Sealed {
  /// The allocator used by the log.
  type Allocator: Allocator;

  /// Returns the allocator used by the log.
  fn allocator(&self) -> &Self::Allocator;
}

/// A trait that means can be constructed to a value log.
///
/// Must have this trait to make `Log`, `GenericLogReader`, and `GenericLogWriter` object-safe.
pub trait Constructor: Sealed {
  /// The checksumer used by the log.
  type Checksumer;
  /// The file id type.
  type Id;

  /// Constructs a value log.
  fn construct(
    fid: Self::Id,
    allocator: Self::Allocator,
    checksumer: Self::Checksumer,
    options: Options,
  ) -> Self;
}

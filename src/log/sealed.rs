use crate::options::Options;

use super::Allocator;

pub trait Sealed {
  /// The allocator used by the log.
  type Allocator: Allocator;

  /// Returns the allocator used by the log.
  fn allocator(&self) -> &Self::Allocator;
}

pub trait Constructor: Sealed {
  type Checksumer;
  type Id;

  fn construct(
    fid: Self::Id,
    allocator: Self::Allocator,
    checksumer: Self::Checksumer,
    options: Options,
  ) -> Self;
}

pub trait Frozen {}

pub trait Mutable {}

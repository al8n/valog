use super::Allocator;

pub trait Sealed {
  /// The allocator used by the log.
  type Allocator: Allocator;

  /// Returns the allocator used by the log.
  fn allocator(&self) -> &Self::Allocator;
}

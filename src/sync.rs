use dbutils::checksum::Crc32;
use rarena_allocator::sync::Arena;

/// A value log that is lock-free, concurrent safe, and can be used in multi-threaded environments.
pub type ValueLog<I = u32, C = Crc32> = super::ValueLog<I, Arena, C>;

/// A generic value log that is lock-free, concurrent safe, and can be used in multi-threaded environments.
pub type GenericValueLog<T, I = u32, C = Crc32> = super::GenericValueLog<T, I, Arena, C>;

/// A value log that is lock-free, concurrent safe, and can be used in multi-threaded environments.
pub type ImmutableValueLog<I = u32, C = Crc32> = super::ImmutableValueLog<I, Arena, C>;

/// A generic value log that is lock-free, concurrent safe, and can be used in multi-threaded environments.
pub type ImmutableGenericValueLog<T, I = u32, C = Crc32> =
  super::ImmutableGenericValueLog<T, I, Arena, C>;

#[cfg(test)]
crate::__common_tests!(sync(crate::sync::ValueLog) {
  basic,
});

#[cfg(all(test, feature = "std"))]
crate::__common_tests!(sync(crate::sync::ValueLog)::spawn {
  concurrent_basic,
});

#[cfg(all(test, feature = "std"))]
crate::__common_tests!(generic_sync(crate::sync::GenericValueLog<String>)::spawn {
  generic_concurrent_basic,
});

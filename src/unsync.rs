use dbutils::checksum::Crc32;
use rarena_allocator::unsync::Arena;

/// A value log that can only be used in single-threaded environments.
pub type ValueLog<I = u32, C = Crc32> = super::ValueLog<I, Arena, C>;

/// A generic value log that can only be used in single-threaded environments.
pub type GenericValueLog<T, I = u32, C = Crc32> = super::GenericValueLog<T, I, Arena, C>;

/// A value log that can only be used in single-threaded environments.
pub type ImmutableValueLog<I = u32, C = Crc32> = super::ImmutableValueLog<I, Arena, C>;

/// A generic value log that can only be used in single-threaded environments.
pub type ImmutableGenericValueLog<T, I = u32, C = Crc32> =
  super::ImmutableGenericValueLog<T, I, Arena, C>;

#[cfg(test)]
crate::__common_tests!(unsync(crate::unsync::ValueLog) {
  basic,
});

use dbutils::checksum::Crc32;
use rarena_allocator::unsync::Arena;

/// A value log that can only be used in single-threaded environments.
pub type ValueLog<I = u32, C = Crc32> = super::ValueLog<Arena, I, C>;

/// A generic value log that can only be used in single-threaded environments.
pub type GenericValueLog<T, I = u32, C = Crc32> = super::GenericValueLog<T, Arena, I, C>;

use dbutils::checksum::Crc32;
use rarena_allocator::sync::Arena;

/// A value log that is lock-free, concurrent safe, and can be used in multi-threaded environments.
pub type ValueLog<I = u32, C = Crc32> = super::ValueLog<I, Arena, C>;

/// A generic value log that is lock-free, concurrent safe, and can be used in multi-threaded environments.
pub type GenericValueLog<T, I = u32, C = Crc32> = super::GenericValueLog<T, I, Arena, C>;

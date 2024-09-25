use core::ptr::NonNull;

use dbutils::{
  buffer::VacantBuffer,
  checksum::{BuildChecksumer, Crc32},
  traits::{Type, TypeRef},
  CheapClone,
};
use rarena_allocator::{either::Either, Allocator, Buffer};

use super::{error::Error, options::Options, ValueBuilder};

mod reader;
pub use reader::*;

mod writer;
pub use writer::*;

pub(super) mod sealed;

const CHECKSUM_LEN: usize = 8;

/// The pointer to the value in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ValuePointer<I> {
  id: I,
  offset: u32,
  size: u32,
}

impl<I: CheapClone> CheapClone for ValuePointer<I> {}

impl<I> ValuePointer<I> {
  /// Creates a new value pointer.
  #[inline]
  pub const fn new(id: I, offset: u32, size: u32) -> Self {
    Self { id, offset, size }
  }

  /// Returns the log id of this value pointer.
  #[inline]
  pub const fn id(&self) -> &I {
    &self.id
  }

  /// Returns the offset of the value.
  #[inline]
  pub const fn offset(&self) -> u32 {
    self.offset
  }

  /// Returns the size of the value.
  #[inline]
  pub const fn size(&self) -> u32 {
    self.size
  }
}

/// The value log implementation.
#[derive(Debug, Clone)]
pub struct ValueLog<I, A, C = Crc32> {
  fid: I,
  allocator: A,
  checksumer: C,
  options: Options,
}

impl<I, A, C> sealed::Sealed for ValueLog<I, A, C>
where
  A: Allocator,
{
  type Allocator = A;

  #[inline]
  fn allocator(&self) -> &Self::Allocator {
    &self.allocator
  }
}

impl<I, A, C> sealed::Constructor for ValueLog<I, A, C>
where
  A: Allocator,
{
  type Checksumer = C;
  type Id = I;

  #[inline]
  fn construct(
    fid: Self::Id,
    allocator: Self::Allocator,
    checksumer: Self::Checksumer,
    options: Options,
  ) -> Self {
    Self {
      fid,
      allocator,
      checksumer,
      options,
    }
  }
}

impl<I, A, C> Reader for ValueLog<I, A, C>
where
  A: Allocator,
  C: BuildChecksumer,
{
  type Id = I;

  #[inline]
  fn checksum(&self, bytes: &[u8]) -> u64 {
    self.checksumer.checksum_one(bytes)
  }

  #[inline]
  fn options(&self) -> &Options {
    &self.options
  }

  #[inline]
  fn id(&self) -> &Self::Id {
    &self.fid
  }

  #[inline]
  fn magic_version(&self) -> u16 {
    self.options.magic_version
  }
}

impl<I, A, C> Writer for ValueLog<I, A, C>
where
  A: Allocator,
  C: BuildChecksumer,
  I: CheapClone + core::fmt::Debug,
{
}

impl<I, A, C> sealed::Mutable for ValueLog<I, A, C> {}

/// The immutable value log implementation.
#[derive(Debug, Clone)]
pub struct ImmutableValueLog<I, C = Crc32> {
  fid: I,
  allocator: rarena_allocator::unsync::Arena,
  checksumer: C,
  options: Options,
}

// Safety: although the `rarena_allocator::unsync::Arena` is not `Send` and `Sync`,
// the `ImmutableValueLog` is `Send` and `Sync` because it is not possible to
// mutate the `Arena` from outside of the `ImmutableValueLog`.
// And the `raena_allocator::unsync::Arena` has the same memory layout as `rarena_allocator::sync::Arena`.
unsafe impl<I, C> Send for ImmutableValueLog<I, C>
where
  C: Send,
  I: Send,
{
}
unsafe impl<I, C> Sync for ImmutableValueLog<I, C>
where
  C: Sync,
  I: Sync,
{
}

impl<I, C> sealed::Sealed for ImmutableValueLog<I, C> {
  type Allocator = rarena_allocator::unsync::Arena;

  #[inline]
  fn allocator(&self) -> &Self::Allocator {
    &self.allocator
  }
}

impl<I, C> Reader for ImmutableValueLog<I, C>
where
  C: BuildChecksumer,
{
  type Id = I;

  #[inline]
  fn checksum(&self, bytes: &[u8]) -> u64 {
    self.checksumer.checksum_one(bytes)
  }

  #[inline]
  fn options(&self) -> &Options {
    &self.options
  }

  #[inline]
  fn id(&self) -> &Self::Id {
    &self.fid
  }

  #[inline]
  fn magic_version(&self) -> u16 {
    self.options.magic_version
  }
}

impl<I, C> sealed::Constructor for ImmutableValueLog<I, C> {
  type Checksumer = C;
  type Id = I;

  #[inline]
  fn construct(
    fid: Self::Id,
    allocator: Self::Allocator,
    checksumer: Self::Checksumer,
    options: Options,
  ) -> Self {
    Self {
      fid,
      allocator,
      checksumer,
      options,
    }
  }
}

impl<I, C> sealed::Frozen for ImmutableValueLog<I, C> {}

/// Generic value log.
pub struct GenericValueLog<T, I, A, C = Crc32> {
  log: ValueLog<I, A, C>,
  _phantom: core::marker::PhantomData<T>,
}

impl<T, I, A, C> sealed::Sealed for GenericValueLog<T, I, A, C>
where
  A: Allocator,
{
  type Allocator = A;

  #[inline]
  fn allocator(&self) -> &Self::Allocator {
    self.log.allocator()
  }
}

impl<T, I, A, C> sealed::Mutable for GenericValueLog<T, I, A, C> {}

impl<T, I, A, C> sealed::Constructor for GenericValueLog<T, I, A, C>
where
  A: Allocator,
{
  type Checksumer = C;
  type Id = I;

  #[inline]
  fn construct(
    fid: Self::Id,
    allocator: Self::Allocator,
    checksumer: Self::Checksumer,
    options: Options,
  ) -> Self {
    Self {
      log: ValueLog::construct(fid, allocator, checksumer, options),
      _phantom: core::marker::PhantomData,
    }
  }
}

impl<T, I, A, C> GenericValueLog<T, I, A, C>
where
  A: Allocator,
  C: BuildChecksumer,
{
  /// Returns the options of the log.
  #[inline]
  pub fn options(&self) -> &Options {
    self.log.options()
  }

  /// Returns the identifier of the log.
  #[inline]
  pub fn id(&self) -> &I {
    self.log.id()
  }

  /// Returns the magic version of the log.
  #[inline]
  pub fn magic_version(&self) -> u16 {
    self.log.magic_version()
  }

  /// Reads a value from the log at the given offset.
  ///
  /// ## Safety
  /// - The buffer `offset..offset + len` must hold a valid bytes sequence which created by encoding a value of type `T` through [`Type::encode`](Type::encode).
  pub unsafe fn read(&self, offset: u32, len: u32) -> Result<T::Ref<'_>, Error>
  where
    T: Type,
  {
    self.log.read_generic::<T>(offset, len)
  }

  /// Inserts a value into the log.
  #[inline]
  pub fn insert(&self, value: &T) -> Result<ValuePointer<I>, Either<T::Error, Error>>
  where
    I: CheapClone + core::fmt::Debug,
    T: Type,
  {
    self.log.insert_generic(value)
  }

  /// Inserts a tombstone value into the log.
  ///
  /// This method is almost the same as the [`insert_generic`] method, the only difference is that
  /// this method will increases the discarded bytes of the log.
  #[inline]
  pub fn insert_tombstone(&self, value: &T) -> Result<ValuePointer<I>, Either<T::Error, Error>>
  where
    I: CheapClone + core::fmt::Debug,
    T: Type,
  {
    self.log.insert_generic_tombstone(value)
  }
}

/// Immutable generic value log.
pub struct ImmutableGenericValueLog<T, I, C = Crc32> {
  log: ImmutableValueLog<I, C>,
  _phantom: core::marker::PhantomData<T>,
}

impl<T, I, C> sealed::Sealed for ImmutableGenericValueLog<T, I, C> {
  type Allocator = rarena_allocator::unsync::Arena;

  #[inline]
  fn allocator(&self) -> &Self::Allocator {
    self.log.allocator()
  }
}

impl<T, I, C> sealed::Frozen for ImmutableGenericValueLog<T, I, C> {}

impl<T, I, C> sealed::Constructor for ImmutableGenericValueLog<T, I, C> {
  type Checksumer = C;
  type Id = I;

  #[inline]
  fn construct(
    fid: Self::Id,
    allocator: Self::Allocator,
    checksumer: Self::Checksumer,
    options: Options,
  ) -> Self {
    Self {
      log: ImmutableValueLog::construct(fid, allocator, checksumer, options),
      _phantom: core::marker::PhantomData,
    }
  }
}

impl<T, I, C> ImmutableGenericValueLog<T, I, C>
where
  C: BuildChecksumer,
{
  /// Returns the options of the log.
  #[inline]
  pub fn options(&self) -> &Options {
    self.log.options()
  }

  /// Returns the identifier of the log.
  #[inline]
  pub fn id(&self) -> &I {
    self.log.id()
  }

  /// Returns the magic version of the log.
  #[inline]
  pub fn magic_version(&self) -> u16 {
    self.log.magic_version()
  }

  /// Reads a value from the log at the given offset.
  ///
  /// ## Safety
  /// - The buffer `offset..offset + len` must hold a valid bytes sequence which created by encoding a value of type `T` through [`Type::encode`](Type::encode).
  pub unsafe fn read(&self, offset: u32, len: u32) -> Result<T::Ref<'_>, Error>
  where
    T: Type,
  {
    self.log.read_generic::<T>(offset, len)
  }
}

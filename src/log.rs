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
pub use reader::{GenericLogReader, LogReader, LogReaderExt};

mod writer;
pub use writer::{GenericLogWriter, LogWriter, LogWriterExt};

mod common;
pub use common::Log;

pub(super) mod sealed;

const CHECKSUM_LEN: usize = 8;

/// A marker trait which means that the log is frozen and cannot be modified.
pub trait Frozen {}

/// A marker trait which means that the log is mutable and can be modified.
pub trait Mutable {}

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

impl<I, A, C> Log for ValueLog<I, A, C>
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
}

impl<I, A, C> LogReader for ValueLog<I, A, C>
where
  C: BuildChecksumer,
  A: Allocator,
{
}

impl<I, A, C> LogWriter for ValueLog<I, A, C>
where
  A: Allocator,
  C: BuildChecksumer,
  I: CheapClone + core::fmt::Debug,
{
}

impl<I, A, C> Mutable for ValueLog<I, A, C> {}

/// The immutable value log implementation.
#[derive(Debug, Clone)]
pub struct ImmutableValueLog<I, A, C = Crc32> {
  fid: I,
  allocator: A,
  checksumer: C,
  options: Options,
}

// Safety: although the `rarena_allocator::unsync::Arena` is not `Send` and `Sync`,
// the `ImmutableValueLog` is `Send` and `Sync` because it is not possible to
// mutate the `Arena` from outside of the `ImmutableValueLog`.
// And the `raena_allocator::unsync::Arena` has the same memory layout as `rarena_allocator::sync::Arena`.
unsafe impl<I, A, C> Send for ImmutableValueLog<I, A, C>
where
  C: Send,
  I: Send,
  A: Send,
{
}
unsafe impl<I, A, C> Sync for ImmutableValueLog<I, A, C>
where
  C: Sync,
  I: Sync,
  A: Sync,
{
}

impl<I, A, C> sealed::Sealed for ImmutableValueLog<I, A, C>
where
  A: Allocator,
{
  type Allocator = A;

  #[inline]
  fn allocator(&self) -> &Self::Allocator {
    &self.allocator
  }
}

impl<I, A, C> Log for ImmutableValueLog<I, A, C>
where
  C: BuildChecksumer,
  A: Allocator,
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
}

impl<I, A, C> LogReader for ImmutableValueLog<I, A, C>
where
  C: BuildChecksumer,
  A: Allocator,
{
}

impl<I, A, C> sealed::Constructor for ImmutableValueLog<I, A, C>
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

impl<I, C> Frozen for ImmutableValueLog<I, C> {}

/// Generic value log.
pub struct GenericValueLog<T, I, A, C = Crc32> {
  log: ValueLog<I, A, C>,
  _phantom: core::marker::PhantomData<T>,
}

impl<T, I: Clone, A: Clone, C: Clone> Clone for GenericValueLog<T, I, A, C> {
  fn clone(&self) -> Self {
    Self {
      log: self.log.clone(),
      _phantom: core::marker::PhantomData,
    }
  }
}

impl<T, I: core::fmt::Debug, A: core::fmt::Debug, C: core::fmt::Debug> core::fmt::Debug
  for GenericValueLog<T, I, A, C>
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    self.log.fmt(f)
  }
}

impl<T, I, A, C> Mutable for GenericValueLog<T, I, A, C> {}

impl<T, I, A, C> From<ValueLog<I, A, C>> for GenericValueLog<T, I, A, C> {
  #[inline]
  fn from(value: ValueLog<I, A, C>) -> Self {
    Self {
      log: value,
      _phantom: core::marker::PhantomData,
    }
  }
}

impl<T, I, A, C> common::AsLog for GenericValueLog<T, I, A, C> {
  type Log = ValueLog<I, A, C>;
  type Type = T;

  #[inline]
  fn as_log(&self) -> &Self::Log {
    &self.log
  }
}

/// Immutable generic value log.
pub struct ImmutableGenericValueLog<T, I, A, C = Crc32> {
  log: ImmutableValueLog<I, A, C>,
  _phantom: core::marker::PhantomData<T>,
}

impl<T, I: core::fmt::Debug, A: core::fmt::Debug, C: core::fmt::Debug> core::fmt::Debug
  for ImmutableGenericValueLog<T, I, A, C>
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    self.log.fmt(f)
  }
}

impl<T, I: Clone, A: Clone, C: Clone> Clone for ImmutableGenericValueLog<T, I, A, C> {
  fn clone(&self) -> Self {
    Self {
      log: self.log.clone(),
      _phantom: core::marker::PhantomData,
    }
  }
}

impl<T, I, A, C> Frozen for ImmutableGenericValueLog<T, I, A, C> {}

impl<T, I, A, C> From<ImmutableValueLog<I, A, C>> for ImmutableGenericValueLog<T, I, A, C> {
  #[inline]
  fn from(value: ImmutableValueLog<I, A, C>) -> Self {
    Self {
      log: value,
      _phantom: core::marker::PhantomData,
    }
  }
}

impl<T, I, A, C> common::AsLog for ImmutableGenericValueLog<T, I, A, C> {
  type Log = ImmutableValueLog<I, A, C>;
  type Type = T;

  #[inline]
  fn as_log(&self) -> &Self::Log {
    &self.log
  }
}

// Safety: although the `rarena_allocator::unsync::Arena` is not `Send` and `Sync`,
// the `ImmutableValueLog` is `Send` and `Sync` because it is not possible to
// mutate the `Arena` from outside of the `ImmutableValueLog`.
// And the `raena_allocator::unsync::Arena` has the same memory layout as `rarena_allocator::sync::Arena`.
unsafe impl<T, I, A, C> Send for ImmutableGenericValueLog<T, I, A, C>
where
  C: Send,
  I: Send,
  A: Send,
{
}
unsafe impl<T, I, A, C> Sync for ImmutableGenericValueLog<T, I, A, C>
where
  C: Sync,
  I: Sync,
  A: Sync,
{
}

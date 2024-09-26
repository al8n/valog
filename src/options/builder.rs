use dbutils::checksum::{BuildChecksumer, Crc32};
use rarena_allocator::Allocator;

use crate::{error::Error, sealed::Constructor, Mutable};

use super::*;

/// The builder to build a `Log`
pub struct Builder<S = Crc32> {
  pub(super) opts: Options,
  pub(super) cks: S,
}

impl Default for Builder {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl Builder {
  /// Create a new `Builder` with default values.
  #[inline]
  pub fn new() -> Self {
    Self {
      opts: Options::new(),
      cks: Crc32::new(),
    }
  }
}

impl<S> Builder<S> {
  /// Returns a new map builder with the new [`BuildChecksumer`](crate::checksum::BuildChecksumer).
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{Builder, checksum::Crc32};
  ///
  /// let builder = Builder::new().with_checksumer(Crc32::new());
  /// ```
  #[inline]
  pub fn with_checksumer<NS>(self, cks: NS) -> Builder<NS> {
    Builder {
      cks,
      opts: self.opts,
    }
  }

  /// Returns a new map builder with the new [`Options`].
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{Builder, options::Options};
  ///
  /// let builder = Builder::new().with_options(Options::new().with_capacity(1024));
  /// ```
  #[inline]
  pub const fn with_options(mut self, opts: Options) -> Self {
    self.opts = opts;
    self
  }

  /// Set the reserved bytes of the `Log`.
  ///
  /// The reserved is used to configure the start position of the `Log`. This is useful
  /// when you want to add some bytes before the `Log`, e.g. when using the memory map file backed `Log`,
  /// you can set the reserved to the size to `8` to store a 8 bytes checksum.
  ///
  /// The default reserved is `0`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let builder = Builder::new().with_reserved(8);
  /// ```
  #[inline]
  pub const fn with_reserved(mut self, reserved: u32) -> Self {
    self.opts.reserved = reserved;
    self
  }

  /// Set if flush the data to the disk when new value is inserted.
  ///
  /// Default is `true`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_sync(false);
  /// ```
  #[inline]
  pub const fn with_sync(mut self, sync: bool) -> Self {
    self.opts.sync = sync;
    self
  }

  /// Set if lock the meta of the `Log` in the memory to prevent OS from swapping out the first page of `Log`.
  /// When using memory map backed `Log`, the meta of the `Log`
  /// is in the first page, meta is frequently accessed,
  /// lock (`mlock` on the first page) the meta can reduce the page fault,
  /// but yes, this means that one `SkipMap` will have one page are locked in memory,
  /// and will not be swapped out. So, this is a trade-off between performance and memory usage.
  ///
  /// Default is `true`.
  ///
  /// This configuration has no effect on windows and vec backed `Log`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_lock_meta(false);
  /// ```
  #[inline]
  pub const fn with_lock_meta(mut self, lock_meta: bool) -> Self {
    self.opts.lock_meta = lock_meta;
    self
  }

  /// Set the magic version of the value log.
  ///
  /// This is used by the application using value log
  /// to ensure that it doesn't open the value log
  /// with incompatible data format.
  ///  
  /// The default value is `0`.
  ///
  /// ## Example
  ///
  /// ```
  /// use valog::Builder;
  ///
  /// let builder = Builder::new().with_magic_version(1);
  /// ```
  #[inline]
  pub const fn with_magic_version(mut self, magic_version: u16) -> Self {
    self.opts.magic_version = magic_version;
    self
  }

  /// Set the [`Freelist`] kind of the value log.
  ///
  /// The default value is [`Freelist::Optimistic`].
  ///
  /// ## Example
  ///
  /// ```
  /// use valog::{Builder, options::Freelist};
  ///
  /// let builder = Builder::new().with_freelist(Freelist::Optimistic);
  /// ```
  #[inline]
  pub const fn with_freelist(mut self, freelist: Freelist) -> Self {
    self.opts.freelist = freelist;
    self
  }

  /// Set if use the unify memory layout of the value log.
  ///
  /// File backed value log has different memory layout with other kind backed value log,
  /// set this value to `true` will unify the memory layout of the value log, which means
  /// all kinds of backed value log will have the same memory layout.
  ///
  /// This value will be ignored if the value log is backed by a file backed memory map.
  ///
  /// The default value is `false`.
  ///
  /// ## Example
  ///
  /// ```
  /// use valog::Builder;
  ///
  /// let builder = Builder::new().with_unify(true);
  /// ```
  #[inline]
  pub const fn with_unify(mut self, unify: bool) -> Self {
    self.opts.unify = unify;
    self
  }

  /// Sets the maximum size of the value.
  ///
  /// Default is `u32::MAX`.
  ///
  /// ## Example
  ///
  /// ```
  /// use valog::Builder;
  ///
  /// let builder = Builder::new().with_maximum_value_size(1024);
  /// ```
  #[inline]
  pub const fn with_maximum_value_size(mut self, size: u32) -> Self {
    self.opts.max_value_size = size;
    self
  }

  /// Sets the capacity of the underlying `Log`.
  ///
  /// Default is `1024`. This configuration will be ignored if the map is memory-mapped.
  ///
  /// ## Example
  ///
  /// ```
  /// use valog::Builder;
  ///
  /// let builder = Builder::new().with_capacity(1024);
  /// ```
  #[inline]
  pub const fn with_capacity(mut self, capacity: u32) -> Self {
    self.opts.capacity = Some(capacity);
    self
  }

  /// Get the reserved of the `Log`.
  ///
  /// The reserved is used to configure the start position of the `Log`. This is useful
  /// when you want to add some bytes before the `Log`, e.g. when using the memory map file backed `Log`,
  /// you can set the reserved to the size to `8` to store a 8 bytes checksum.
  ///
  /// The default reserved is `0`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let builder = Builder::new().with_reserved(8);
  ///
  /// assert_eq!(builder.reserved(), 8);
  /// ```
  #[inline]
  pub const fn reserved(&self) -> u32 {
    self.opts.reserved
  }

  /// Get if flush the data to the disk when new value is inserted.
  ///
  /// Default is `true`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let builder = Builder::new().with_sync(false);
  ///
  /// assert_eq!(builder.sync(), false);
  /// ```
  #[inline]
  pub const fn sync(&self) -> bool {
    self.opts.sync
  }

  /// Get if lock the meta of the `Log` in the memory to prevent OS from swapping out the first page of `Log`.
  /// When using memory map backed `Log`, the meta of the `Log`
  /// is in the first page, meta is frequently accessed,
  /// lock (`mlock` on the first page) the meta can reduce the page fault,
  /// but yes, this means that one `SkipMap` will have one page are locked in memory,
  /// and will not be swapped out. So, this is a trade-off between performance and memory usage.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_lock_meta(false);
  ///
  /// assert_eq!(opts.lock_meta(), false);
  /// ```
  #[inline]
  pub const fn lock_meta(&self) -> bool {
    self.opts.lock_meta
  }

  /// Returns the maximum size of the value.
  ///
  /// Default is `u32::MAX`. The maximum size of the value is `u32::MAX - header`.
  ///
  /// ## Example
  ///
  /// ```
  /// use valog::Builder;
  ///
  /// let builder = Builder::new().with_maximum_value_size(1024);
  /// ```
  #[inline]
  pub const fn maximum_value_size(&self) -> u32 {
    self.opts.max_value_size
  }

  /// Returns the configuration of underlying `Log` size.
  ///
  /// Default is `1024`. This configuration will be ignored if the map is memory-mapped.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let builder = Builder::new().with_capacity(1024);
  /// ```
  #[inline]
  pub const fn capacity(&self) -> u32 {
    self.opts.capacity()
  }

  /// Get if use the unify memory layout of the value log.
  ///
  /// File backed value log has different memory layout with other kind backed value log,
  /// set this value to `true` will unify the memory layout of the value log, which means
  /// all kinds of backed value log will have the same memory layout.
  ///
  /// This value will be ignored if the value log is backed by a file backed memory map.
  ///  
  /// The default value is `false`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let builder = Builder::new().with_unify(true);
  ///
  /// assert_eq!(builder.unify(), true);
  /// ```
  #[inline]
  pub const fn unify(&self) -> bool {
    self.opts.unify
  }

  /// Get the magic version of the value log.
  ///
  /// This is used by the application using value log
  /// to ensure that it doesn't open the value log
  /// with incompatible data format.
  ///
  /// The default value is `0`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let builder = Builder::new().with_magic_version(1);
  ///
  /// assert_eq!(builder.magic_version(), 1);
  /// ```
  #[inline]
  pub const fn magic_version(&self) -> u16 {
    self.opts.magic_version
  }

  /// Get the [`Freelist`] kind of the value log.
  ///
  /// The default value is [`Freelist::Optimistic`].
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{Builder, options::Freelist};
  ///
  /// let builder = Builder::new().with_freelist(Freelist::Optimistic);
  ///
  /// assert_eq!(builder.freelist(), Freelist::Optimistic);
  /// ```
  #[inline]
  pub const fn freelist(&self) -> Freelist {
    self.opts.freelist
  }
}

impl<S: BuildChecksumer> Builder<S> {
  /// Create a new in-memory value log which is backed by a `AlignedVec`.
  ///
  /// **What the difference between this method and [`Builder::map_anon`]?**
  ///
  /// 1. This method will use an `AlignedVec` ensures we are working within Rust's memory safety guarantees.
  ///    Even if we are working with raw pointers with `Box::into_raw`,
  ///    the backend `Log` will reclaim the ownership of this memory by converting it back to a `Box`
  ///    when dropping the backend `Log`. Since `AlignedVec` uses heap memory, the data might be more cache-friendly,
  ///    especially if you're frequently accessing or modifying it.
  ///
  /// 2. Where as [`Builder::map_anon`] will use mmap anonymous to require memory from the OS.
  ///    If you require very large contiguous memory regions, `mmap` might be more suitable because
  ///    it's more direct in requesting large chunks of memory from the OS.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{Builder, sync, unsync};
  ///
  /// // Create a sync in-memory value log.
  /// let map = Builder::new().with_capacity(1024).alloc::<sync::ValueLog>(1u32).unwrap();
  ///
  /// // Create a unsync in-memory value log.
  /// let arena = Builder::new().with_capacity(1024).alloc::<unsync::ValueLog>(1u32).unwrap();
  /// ```
  #[inline]
  pub fn alloc<C>(self, fid: C::Id) -> Result<C, Error>
  where
    C: Constructor<Checksumer = S> + Mutable,
  {
    let Self { opts, cks } = self;

    let unify = opts.unify;
    let mv = opts.magic_version;
    opts
      .to_arena_options()
      .alloc::<C::Allocator>()
      .map_err(Error::from_insufficient_space)
      .map(|arena| {
        if unify {
          unsafe {
            let slice = arena.reserved_slice_mut();
            write_header(slice, mv);
          }
        }

        C::construct(fid, arena, cks, opts)
      })
  }
}

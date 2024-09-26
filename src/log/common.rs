use crate::options::HEADER_SIZE;

use super::*;

/// The abstraction for the common methods of log.
pub trait Log: sealed::Sealed {
  /// The identifier type (file ID) for the log.
  type Id;

  /// Returns the identifier of the log.
  fn id(&self) -> &Self::Id;

  /// Calculates the checksum of the given bytes.
  fn checksum(&self, bytes: &[u8]) -> u64;

  /// Returns the options of the log.
  fn options(&self) -> &Options;

  /// Returns the magic version of the log.
  fn magic_version(&self) -> u16;

  /// Returns the version of the log.
  #[inline]
  fn version(&self) -> u16 {
    self.allocator().magic_version()
  }

  /// Returns the discarded bytes of the log.
  #[inline]
  fn discarded(&self) -> u32 {
    self.allocator().discarded()
  }

  /// Returns the path of the log.
  ///
  /// If the log is in memory, this method will return `None`.
  #[inline]
  fn path(&self) -> Option<&<Self::Allocator as Allocator>::Path> {
    self.allocator().path()
  }

  /// Returns `true` if the log is in memory.
  #[inline]
  fn in_memory(&self) -> bool {
    self.allocator().is_inmemory()
  }

  /// Returns `true` if the log is on disk.
  #[inline]
  fn on_disk(&self) -> bool {
    self.allocator().is_ondisk()
  }

  /// Returns `true` if the log is using a memory map backend.
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  fn is_map(&self) -> bool {
    self.allocator().is_map()
  }

  /// Returns the reserved space in the WAL.
  ///
  /// ## Safety
  /// - The writer must ensure that the returned slice is not modified.
  /// - This method is not thread-safe, so be careful when using it.
  #[inline]
  unsafe fn reserved_slice(&self) -> &[u8] {
    let reserved = self.options().reserved();
    if reserved == 0 {
      return &[];
    }

    let allocator = self.allocator();
    let reserved_slice = allocator.reserved_slice();
    &reserved_slice[HEADER_SIZE..]
  }

  /// Returns the mutable reference to the reserved slice.
  ///
  /// ## Safety
  /// - The caller must ensure that the there is no others accessing reserved slice for either read or write.
  /// - This method is not thread-safe, so be careful when using it.
  #[allow(clippy::mut_from_ref)]
  #[inline]
  unsafe fn reserved_slice_mut(&self) -> &mut [u8]
  where
    Self: Mutable,
  {
    let reserved = self.options().reserved();
    if reserved == 0 {
      return &mut [];
    }

    let allocator = self.allocator();
    let reserved_slice = allocator.reserved_slice_mut();
    &mut reserved_slice[HEADER_SIZE..]
  }

  /// Locks the underlying file for exclusive access, only works on mmap with a file backend.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log};
  /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
  /// # std::fs::remove_file(&path);
  /// /// Create a new file without automatic syncing.
  /// let log = unsafe {
  ///   Builder::new()
  ///     .with_sync(false)
  ///     .with_create(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .with_capacity(100)
  ///     .map_mut::<ValueLog, _>(&path, 0).unwrap()
  /// };
  ///
  /// log.lock_exclusive().unwrap();
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  fn lock_exclusive(&self) -> std::io::Result<()> {
    self.allocator().lock_exclusive()
  }

  /// Locks the underlying file for shared access, only works on mmap with a file backend.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log};
  /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
  /// # std::fs::remove_file(&path);
  /// /// Create a new file without automatic syncing.
  /// let log = unsafe {
  ///   Builder::new()
  ///     .with_sync(false)
  ///     .with_create(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .with_capacity(100)
  ///     .map_mut::<ValueLog, _>(&path, 0).unwrap()
  /// };
  ///
  /// log.lock_shared().unwrap();
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  fn lock_shared(&self) -> std::io::Result<()> {
    self.allocator().lock_shared()
  }

  /// Unlocks the underlying file, only works on mmap with a file backend.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log};
  /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
  /// # std::fs::remove_file(&path);
  /// /// Create a new file without automatic syncing.
  /// let log = unsafe {
  ///   Builder::new()
  ///     .with_sync(false)
  ///     .with_create(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .with_capacity(100)
  ///     .map_mut::<ValueLog, _>(&path, 0).unwrap()
  /// };
  ///
  /// log.lock_exclusive().unwrap();
  ///
  /// log.unlock().unwrap();
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  fn unlock(&self) -> std::io::Result<()> {
    self.allocator().unlock()
  }

  /// `mlock(ptr, len)`—Lock memory into RAM.
  ///
  /// ## Safety
  ///
  /// This function operates on raw pointers, but it should only be used on
  /// memory which the caller owns. Technically, locking memory shouldn't violate
  /// any invariants, but since unlocking it can violate invariants, this
  /// function is also unsafe for symmetry.
  ///
  /// Some implementations implicitly round the memory region out to the nearest
  /// page boundaries, so this function may lock more memory than explicitly
  /// requested if the memory isn't page-aligned. Other implementations fail if
  /// the memory isn't page-aligned.
  ///
  /// # References
  ///  - [POSIX]
  ///  - [Linux]
  ///  - [Apple]
  ///  - [FreeBSD]
  ///  - [NetBSD]
  ///  - [OpenBSD]
  ///  - [DragonFly BSD]
  ///  - [illumos]
  ///  - [glibc]
  ///
  /// [POSIX]: https://pubs.opengroup.org/onlinepubs/9699919799/functions/mlock.html
  /// [Linux]: https://man7.org/linux/man-pages/man2/mlock.2.html
  /// [Apple]: https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/mlock.2.html
  /// [FreeBSD]: https://man.freebsd.org/cgi/man.cgi?query=mlock&sektion=2
  /// [NetBSD]: https://man.netbsd.org/mlock.2
  /// [OpenBSD]: https://man.openbsd.org/mlock.2
  /// [DragonFly BSD]: https://man.dragonflybsd.org/?command=mlock&section=2
  /// [illumos]: https://illumos.org/man/3C/mlock
  /// [glibc]: https://www.gnu.org/software/libc/manual/html_node/Page-Lock-Functions.html#index-mlock
  #[cfg(all(feature = "memmap", not(target_family = "wasm"), not(windows)))]
  #[cfg_attr(
    docsrs,
    doc(cfg(all(feature = "memmap", not(target_family = "wasm"), not(windows))))
  )]
  unsafe fn mlock(&self, offset: usize, len: usize) -> std::io::Result<()> {
    self.allocator().mlock(offset, len)
  }

  /// `munlock(ptr, len)`—Unlock memory.
  ///
  /// ## Safety
  ///
  /// This function operates on raw pointers, but it should only be used on
  /// memory which the caller owns, to avoid compromising the `mlock` invariants
  /// of other unrelated code in the process.
  ///
  /// Some implementations implicitly round the memory region out to the nearest
  /// page boundaries, so this function may unlock more memory than explicitly
  /// requested if the memory isn't page-aligned.
  ///
  /// # References
  ///  - [POSIX]
  ///  - [Linux]
  ///  - [Apple]
  ///  - [FreeBSD]
  ///  - [NetBSD]
  ///  - [OpenBSD]
  ///  - [DragonFly BSD]
  ///  - [illumos]
  ///  - [glibc]
  ///
  /// [POSIX]: https://pubs.opengroup.org/onlinepubs/9699919799/functions/munlock.html
  /// [Linux]: https://man7.org/linux/man-pages/man2/munlock.2.html
  /// [Apple]: https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/munlock.2.html
  /// [FreeBSD]: https://man.freebsd.org/cgi/man.cgi?query=munlock&sektion=2
  /// [NetBSD]: https://man.netbsd.org/munlock.2
  /// [OpenBSD]: https://man.openbsd.org/munlock.2
  /// [DragonFly BSD]: https://man.dragonflybsd.org/?command=munlock&section=2
  /// [illumos]: https://illumos.org/man/3C/munlock
  /// [glibc]: https://www.gnu.org/software/libc/manual/html_node/Page-Lock-Functions.html#index-munlock
  #[cfg(all(feature = "memmap", not(target_family = "wasm"), not(windows)))]
  #[cfg_attr(
    docsrs,
    doc(cfg(all(feature = "memmap", not(target_family = "wasm"), not(windows))))
  )]
  unsafe fn munlock(&self, offset: usize, len: usize) -> std::io::Result<()> {
    self.allocator().munlock(offset, len)
  }

  /// Flushes the memory-mapped file to disk.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log};
  /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
  /// # std::fs::remove_file(&path);
  /// /// Create a new file without automatic syncing.
  /// let arena = unsafe {
  ///   Builder::new()
  ///     .with_sync(false)
  ///     .with_create(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .with_capacity(100)
  ///     .map_mut::<ValueLog, _>(&path, 0).unwrap() };
  ///
  /// arena.flush().unwrap();
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  fn flush(&self) -> std::io::Result<()>
  where
    Self: Mutable,
  {
    self.allocator().flush()
  }

  /// Flushes the memory-mapped file to disk asynchronously.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log};
  /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
  /// # std::fs::remove_file(&path);
  /// /// Create a new file without automatic syncing.
  /// let log = unsafe {
  ///   Builder::new()
  ///     .with_sync(false)
  ///     .with_create(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .with_capacity(100)
  ///     .map_mut::<ValueLog, _>(&path, 0).unwrap()
  /// };
  ///
  /// log.flush_async().unwrap();
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  fn flush_async(&self) -> std::io::Result<()>
  where
    Self: Mutable,
  {
    self.allocator().flush_async()
  }

  /// Flushes outstanding memory map modifications in the range to disk.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log};
  /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
  /// # std::fs::remove_file(&path);
  ///
  /// /// Create a new file without automatic syncing.
  /// let log = unsafe {
  ///   Builder::new()
  ///     .with_sync(false)
  ///     .with_create(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .with_capacity(100)
  ///     .map_mut::<ValueLog, _>(&path, 0).unwrap()
  /// };
  ///
  /// log.flush_range(0, 50).unwrap();
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  fn flush_range(&self, offset: usize, len: usize) -> std::io::Result<()>
  where
    Self: Mutable,
  {
    self.allocator().flush_range(offset, len)
  }

  /// Asynchronously flushes outstanding memory map modifications in the range to disk.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log};
  /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
  /// # std::fs::remove_file(&path);
  /// /// Create a new file without automatic syncing.
  /// let vlog = unsafe {
  ///   Builder::new()
  ///     .with_sync(false)
  ///     .with_create(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .with_capacity(100)
  ///     .map_mut::<ValueLog, _>(&path, 0).unwrap()
  /// };
  ///
  /// vlog.flush_async_range(0, 50).unwrap();
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  fn flush_async_range(&self, offset: usize, len: usize) -> std::io::Result<()>
  where
    Self: Mutable,
  {
    self.allocator().flush_async_range(offset, len)
  }
}

use crate::options::HEADER_SIZE;

use super::*;

/// The abstraction for the common methods of log.
pub trait Log: sealed::Sealed {
  /// The identifier type (file ID) for the log.
  type Id;

  /// Returns the identifier of the log.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log};
  ///
  /// let log = Builder::new()
  ///   .with_capacity(100)
  ///   .alloc::<ValueLog>(1)
  ///   .unwrap();
  ///
  /// assert_eq!(log.id(), &1);
  /// ```
  fn id(&self) -> &Self::Id;

  /// Calculates the checksum of the given bytes.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log, checksum::{Crc32, BuildChecksumer}};
  ///
  /// let log = Builder::new()
  ///   .with_capacity(100)
  ///   .alloc::<ValueLog>(1)
  ///   .unwrap();
  ///
  /// let bytes = b"Hello, valog!";
  /// assert_eq!(log.checksum(bytes), Crc32::new().checksum_one(bytes));
  /// ```
  fn checksum(&self, bytes: &[u8]) -> u64;

  /// Returns the options of the log.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log};
  ///
  /// let log = Builder::new()
  ///   .with_capacity(100)
  ///   .alloc::<ValueLog>(1)
  ///   .unwrap();
  ///
  /// assert_eq!(log.options().capacity(), 100);
  /// ```
  fn options(&self) -> &Options;

  /// Returns the magic version of the log.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log};
  ///
  /// let log = Builder::new()
  ///   .with_capacity(100)
  ///   .with_magic_version(1)
  ///   .alloc::<ValueLog>(1)
  ///   .unwrap();
  ///
  /// assert_eq!(log.magic_version(), 1);
  /// ```
  fn magic_version(&self) -> u16 {
    self.options().magic_version()
  }

  /// Returns the version of the log.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log};
  ///
  /// let log = Builder::new()
  ///   .with_capacity(100)
  ///   .alloc::<ValueLog>(1)
  ///   .unwrap();
  ///
  /// assert_eq!(log.version(), 0);
  /// ```
  #[inline]
  fn version(&self) -> u16 {
    self.allocator().magic_version()
  }

  /// Returns the discarded bytes of the log.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log, LogWriter};
  ///
  ///
  /// let log = Builder::new()
  ///   .with_capacity(100)
  ///   .alloc::<ValueLog>(1)
  ///   .unwrap();
  ///
  /// assert_eq!(log.discarded(), 0);
  ///
  /// log.insert_tombstone(b"Hello, valog!").unwrap();
  /// assert_eq!(log.discarded(), 13);
  /// ```
  #[inline]
  fn discarded(&self) -> u32 {
    self.allocator().discarded()
  }

  /// Returns the data offset of the log.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log};
  ///
  /// let log = Builder::new()
  ///   .with_capacity(100)
  ///   .alloc::<ValueLog>(1)
  ///   .unwrap();
  ///
  /// assert_eq!(log.data_offset(), 9); // header size is 8, so data start at 9.
  ///
  /// let log = Builder::new()
  ///   .with_capacity(100)
  ///   .with_reserved(8)
  ///   .alloc::<ValueLog>(1)
  ///   .unwrap();
  ///
  /// assert_eq!(log.data_offset(), 17); // header size is 8, reserved is 8, so data start at 17.
  /// ```
  fn data_offset(&self) -> usize {
    Allocator::data_offset(self.allocator())
  }

  /// Returns the path of the log.
  ///
  /// If the log is in memory, this method will return `None`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log};
  ///
  /// let log = Builder::new()
  ///   .with_capacity(100)
  ///   .alloc::<ValueLog>(1)
  ///   .unwrap();
  ///
  /// assert_eq!(log.path(), None);
  ///
  /// let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
  /// # std::fs::remove_file(&path);
  /// let log = unsafe {
  ///   Builder::new()
  ///     .with_capacity(100)
  ///     .with_create(true)
  ///     .with_write(true)
  ///     .with_read(true)
  ///     .map_mut::<ValueLog, _>(&path, 0)
  ///     .unwrap()
  /// };
  ///
  /// assert_eq!(log.path().map(|p| p.as_path()), Some(path.as_ref()));
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  fn path(&self) -> Option<&<Self::Allocator as Allocator>::Path> {
    self.allocator().path()
  }

  /// Returns `true` if the log is in memory.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log};
  ///
  /// let log = Builder::new()
  ///   .with_capacity(100)
  ///   .alloc::<ValueLog>(1)
  ///   .unwrap();
  ///
  /// assert!(log.in_memory());
  ///
  /// # #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  /// # {
  /// let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
  /// # std::fs::remove_file(&path);
  /// let log = unsafe {
  ///   Builder::new()
  ///     .with_capacity(100)
  ///     .with_create(true)
  ///     .with_write(true)
  ///     .with_read(true)
  ///     .map_mut::<ValueLog, _>(&path, 0)
  ///     .unwrap()
  /// };
  /// assert!(!log.in_memory());
  /// # }
  ///
  /// ```
  #[inline]
  fn in_memory(&self) -> bool {
    self.allocator().is_inmemory()
  }

  /// Returns `true` if the log is on disk.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log};
  ///
  /// let log = Builder::new()
  ///   .with_capacity(100)
  ///   .alloc::<ValueLog>(1)
  ///   .unwrap();
  ///
  /// assert!(!log.on_disk());
  ///
  /// # #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  /// # {
  /// let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
  /// # std::fs::remove_file(&path);
  /// let log = unsafe {
  ///   Builder::new()
  ///     .with_capacity(100)
  ///     .with_create(true)
  ///     .with_write(true)
  ///     .with_read(true)
  ///     .map_mut::<ValueLog, _>(&path, 0)
  ///     .unwrap()
  /// };
  /// assert!(log.on_disk());
  /// # }
  /// ```
  #[inline]
  fn on_disk(&self) -> bool {
    self.allocator().is_ondisk()
  }

  /// Returns `true` if the log is using a memory map backend.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log};
  ///
  /// let log = Builder::new()
  ///   .with_capacity(100)
  ///   .alloc::<ValueLog>(1)
  ///   .unwrap();
  ///
  /// assert!(!log.is_map());
  ///
  /// let log = Builder::new()
  ///   .with_capacity(100)
  ///   .map_anon::<ValueLog>(0)
  ///   .unwrap();
  ///
  /// assert!(log.is_map());
  ///
  /// # #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  /// # {
  /// let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
  /// # std::fs::remove_file(&path);
  /// let log = unsafe {
  ///   Builder::new()
  ///     .with_capacity(100)
  ///     .with_create(true)
  ///     .with_write(true)
  ///     .with_read(true)
  ///     .map_mut::<ValueLog, _>(&path, 0)
  ///     .unwrap()
  /// };
  /// assert!(log.is_map());
  /// # }
  /// ```
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
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log};
  ///
  /// let log = Builder::new()
  ///   .with_capacity(100)
  ///   .with_reserved(8)
  ///   .alloc::<ValueLog>(1)
  ///   .unwrap();
  ///
  /// let reserved = unsafe { log.reserved_slice() };
  /// assert_eq!(reserved.len(), 8);
  /// ```
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
}

/// Extension methods for [`Log`].
pub trait LogExt: Log {
  /// Flushes the whole log to the given writer.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log, LogExt};
  ///
  /// let log = Builder::new()
  ///   .with_capacity(100)
  ///   .alloc::<ValueLog>(1)
  ///   .unwrap();
  ///
  /// let mut buf = Vec::new();
  /// log.flush_to(&mut buf).unwrap();
  /// let data_offset = log.data_offset();
  /// assert_eq!(buf.len(), data_offset);
  /// ```
  #[cfg(feature = "std")]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  #[inline]
  fn flush_to(&self, w: &mut impl std::io::Write) -> std::io::Result<()> {
    w.write_all(self.allocator().allocated_memory())
  }
}

impl<L: Log> LogExt for L {}

/// The abstraction for the common mutable methods of log.
pub trait MutableLog: Log + Mutable {
  /// Returns the mutable reference to the reserved slice.
  ///
  /// ## Safety
  /// - The caller must ensure that the there is no others accessing reserved slice for either read or write.
  /// - This method is not thread-safe, so be careful when using it.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, Log, MutableLog};
  ///
  /// let log = Builder::new()
  ///   .with_capacity(100)
  ///   .with_reserved(8)
  ///   .alloc::<ValueLog>(1)
  ///   .unwrap();
  ///
  /// let reserved = unsafe { log.reserved_slice_mut() };
  /// assert_eq!(reserved.len(), 8);
  ///
  /// reserved.copy_from_slice(b"mysanity");
  /// assert_eq!(reserved, b"mysanity");
  ///
  /// let reserved = unsafe { log.reserved_slice() };
  /// assert_eq!(reserved, b"mysanity");
  /// ```
  #[allow(clippy::mut_from_ref)]
  #[inline]
  unsafe fn reserved_slice_mut(&self) -> &mut [u8] {
    let reserved = self.options().reserved();
    if reserved == 0 {
      return &mut [];
    }

    let allocator = self.allocator();
    let reserved_slice = allocator.reserved_slice_mut();
    &mut reserved_slice[HEADER_SIZE..]
  }

  /// Flushes the memory-mapped file to disk.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, MutableLog};
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
  fn flush(&self) -> std::io::Result<()> {
    self.allocator().flush()
  }

  /// Flushes the memory-mapped file to disk asynchronously.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, MutableLog};
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
  fn flush_async(&self) -> std::io::Result<()> {
    self.allocator().flush_async()
  }

  /// Flushes outstanding memory map modifications in the range to disk.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, MutableLog};
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
  fn flush_range(&self, offset: usize, len: usize) -> std::io::Result<()> {
    self.allocator().flush_range(offset, len)
  }

  /// Asynchronously flushes outstanding memory map modifications in the range to disk.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder, MutableLog};
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
  fn flush_async_range(&self, offset: usize, len: usize) -> std::io::Result<()> {
    self.allocator().flush_async_range(offset, len)
  }
}

impl<L: Log + Mutable> MutableLog for L {}

pub trait AsLog {
  type Log;
  type Type;

  fn as_log(&self) -> &Self::Log;
}

impl<L> sealed::Sealed for L
where
  L: AsLog,
  L::Log: Log,
{
  type Allocator = <L::Log as sealed::Sealed>::Allocator;

  #[inline]
  fn allocator(&self) -> &Self::Allocator {
    self.as_log().allocator()
  }
}

impl<L> sealed::Constructor for L
where
  L: AsLog,
  L::Log: Log + sealed::Constructor<Id = <L::Log as Log>::Id>,
  L: From<L::Log>,
{
  type Checksumer = <L::Log as sealed::Constructor>::Checksumer;

  type Id = <L::Log as Log>::Id;

  fn construct(
    fid: Self::Id,
    allocator: Self::Allocator,
    checksumer: Self::Checksumer,
    options: Options,
  ) -> Self {
    <L::Log as sealed::Constructor>::construct(fid, allocator, checksumer, options).into()
  }
}

impl<L> Log for L
where
  L: AsLog,
  L::Log: Log,
{
  type Id = <L::Log as Log>::Id;

  #[inline]
  fn id(&self) -> &Self::Id {
    self.as_log().id()
  }

  #[inline]
  fn checksum(&self, bytes: &[u8]) -> u64 {
    self.as_log().checksum(bytes)
  }

  #[inline]
  fn options(&self) -> &Options {
    self.as_log().options()
  }
}

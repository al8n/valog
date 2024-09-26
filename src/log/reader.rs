use crate::options::HEADER_SIZE;

use super::*;

/// The immutable value log abstraction.
pub trait Reader: sealed::Sealed {
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
    !self.allocator().is_ondisk()
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
  fn is_mmap(&self) -> bool {
    self.allocator().is_mmap()
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

  /// Locks the underlying file for exclusive access, only works on mmap with a file backend.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync::ValueLog, Builder};
  /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
  ///
  /// /// Create a new file without automatic syncing.
  /// let log = unsafe {
  ///   Builder::new()
  ///     .with_sync(false)
  ///     .with_create_new(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .with_capacity(100)
  ///     .map_mut::<ValueLog, _>(&path).unwrap()
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
  /// use valog::{sync::ValueLog, Builder};
  /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
  ///
  /// /// Create a new file without automatic syncing.
  /// let log = unsafe {
  ///   Builder::new()
  ///     .with_sync(false)
  ///     .with_create_new(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .with_capacity(100)
  ///     .map_mut::<ValueLog, _>(&path).unwrap()
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
  /// use valog::{sync::ValueLog, Builder};
  /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
  ///
  /// /// Create a new file without automatic syncing.
  /// let log = unsafe {
  ///   Builder::new()
  ///     .with_sync(false)
  ///     .with_create_new(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .with_capacity(100)
  ///     .map_mut::<ValueLog, _>(&path).unwrap()
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

  /// Reads a value from the log.
  fn read(&self, offset: u32, len: u32) -> Result<&[u8], Error> {
    if offset == 0 && len == 0 {
      return Ok(&[]);
    }

    let offset = offset as usize;
    let len = len as usize;

    let allocator = self.allocator();
    let allocated = allocator.allocated();
    let data_offset = allocator.data_offset();
    let opts = self.options();

    if offset < data_offset {
      return Err(Error::out_of_bounds(
        offset as u32,
        (len + CHECKSUM_LEN) as u32,
        data_offset as u32,
        allocated as u32,
      ));
    }

    if (offset + len + CHECKSUM_LEN) > allocated {
      return Err(Error::out_of_bounds(
        offset as u32,
        (len + CHECKSUM_LEN) as u32,
        data_offset as u32,
        allocated as u32,
      ));
    }

    // Safety: we have checked the bounds
    let buf = unsafe { allocator.get_bytes(offset, len + CHECKSUM_LEN) };

    if opts.validate_checksum {
      let checksum = u64::from_le_bytes((&buf[len..len + CHECKSUM_LEN]).try_into().unwrap());
      let digest = self.checksum(&buf[..len]);
      if checksum != digest {
        return Err(Error::checksum_mismatch());
      }
    }

    Ok(&buf[..len])
  }
}

/// The extension trait for the [`Reader`] trait.
///
/// The reason having a `ReaderExt` is that to make [`Reader`] object-safe.
pub trait ReaderExt: Reader {
  /// Reads a generic value from the log at the given offset.
  ///
  /// ## Safety
  /// - The buffer `offset..offset + len` must hold a valid bytes sequence which created by encoding a value of type `T` through [`Type::encode`](Type::encode).
  unsafe fn read_generic<T: Type>(&self, offset: u32, len: u32) -> Result<T::Ref<'_>, Error> {
    self
      .read(offset, len)
      .map(|buf| <T::Ref<'_> as TypeRef>::from_slice(buf))
  }
}

impl<L: Reader> ReaderExt for L {}

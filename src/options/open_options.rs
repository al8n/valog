use rarena_allocator::{either::Either, Allocator};

use super::{
  super::error::{bad_magic_text, bad_magic_version, Error},
  write_header, Builder, Options, HEADER_SIZE, MAGIC_TEXT, MAGIC_TEXT_SIZE,
};
use crate::{sealed::Constructor, Frozen, Mutable};

impl Options {
  /// Sets the option for read access.
  ///
  /// This option, when true, will indicate that the file should be
  /// `read`-able if opened.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::options::Options;
  ///
  /// let opts = Options::new().with_read(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_read(mut self, read: bool) -> Self {
    self.read = read;
    self
  }

  /// Sets the option for write access.
  ///
  /// This option, when true, will indicate that the file should be
  /// `write`-able if opened.
  ///
  /// If the file already exists, any write calls on it will overwrite its
  /// contents, without truncating it.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::options::Options;
  ///
  /// let opts = Options::new().with_write(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_write(mut self, write: bool) -> Self {
    self.write = write;
    self
  }

  /// Sets the option for the append mode.
  ///
  /// This option, when true, means that writes will append to a file instead
  /// of overwriting previous contents.
  /// Note that setting `.write(true).append(true)` has the same effect as
  /// setting only `.append(true)`.
  ///
  /// For most filesystems, the operating system guarantees that all writes are
  /// atomic: no writes get mangled because another process writes at the same
  /// time.
  ///
  /// One maybe obvious note when using append-mode: make sure that all data
  /// that belongs together is written to the file in one operation. This
  /// can be done by concatenating strings before passing them to [`write()`],
  /// or using a buffered writer (with a buffer of adequate size),
  /// and calling [`flush()`] when the message is complete.
  ///
  /// If a file is opened with both read and append access, beware that after
  /// opening, and after every write, the position for reading may be set at the
  /// end of the file. So, before writing, save the current position (using
  /// <code>[seek]\([SeekFrom](std::io::SeekFrom)::[Current]\(opts))</code>), and restore it before the next read.
  ///
  /// ## Note
  ///
  /// This function doesn't create the file if it doesn't exist. Use the
  /// [`Options::with_create`] method to do so.
  ///
  /// [`write()`]: std::io::Write::write "io::Write::write"
  /// [`flush()`]: std::io::Write::flush "io::Write::flush"
  /// [seek]: std::io::Seek::seek "io::Seek::seek"
  /// [Current]: std::io::SeekFrom::Current "io::SeekFrom::Current"
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::options::Options;
  ///
  /// let opts = Options::new().with_append(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_append(mut self, append: bool) -> Self {
    self.write = true;
    self.append = append;
    self
  }

  /// Sets the option for truncating a previous file.
  ///
  /// If a file is successfully opened with this option set it will truncate
  /// the file to opts length if it already exists.
  ///
  /// The file must be opened with write access for truncate to work.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::options::Options;
  ///
  /// let opts = Options::new().with_write(true).with_truncate(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_truncate(mut self, truncate: bool) -> Self {
    self.truncate = truncate;
    self.write = true;
    self
  }

  /// Sets the option to create a new file, or open it if it already exists.
  /// If the file does not exist, it is created and set the lenght of the file to the given size.
  ///
  /// In order for the file to be created, [`Options::with_write`] or
  /// [`Options::with_append`] access must be used.
  ///
  /// See also [`std::fs::write()`][std::fs::write] for a simple function to
  /// create a file with some given data.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::options::Options;
  ///
  /// let opts = Options::new().with_write(true).with_create(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_create(mut self, val: bool) -> Self {
    self.create = val;
    self
  }

  /// Sets the option to create a new file and set the file length to the given value, failing if it already exists.
  ///
  /// No file is allowed to exist at the target location, also no (dangling) symlink. In this
  /// way, if the call succeeds, the file returned is guaranteed to be new.
  ///
  /// This option is useful because it is atomic. Otherwise between checking
  /// whether a file exists and creating a new one, the file may have been
  /// created by another process (a TOCTOU race condition / attack).
  ///
  /// If `.with_create_new(true)` is set, [`.with_create()`](Options::with_create) and [`.with_truncate()`](Options::with_truncate) are
  /// ignored.
  ///
  /// The file must be opened with write or append access in order to create
  /// a new file.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::options::Options;
  ///
  /// let file = Options::new()
  ///   .with_write(true)
  ///   .with_create_new(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_create_new(mut self, val: bool) -> Self {
    self.create_new = val;
    self
  }

  /// Configures the memory map to start at byte `offset` from the beginning of the file.
  ///
  /// This option has no effect on anonymous memory maps or vec backed `Log`.
  ///
  /// By default, the offset is 0.
  ///
  /// ## Example
  ///
  /// ```
  /// use valog::options::Options;
  ///
  /// let opts = Options::new().with_offset(30);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_offset(mut self, offset: u64) -> Self {
    self.offset = offset;
    self
  }

  /// Configures the anonymous memory map to be suitable for a process or thread stack.
  ///
  /// This option corresponds to the `MAP_STACK` flag on Linux. It has no effect on Windows.
  ///
  /// This option has no effect on file-backed memory maps and vec backed `Log`.
  ///
  /// ## Example
  ///
  /// ```
  /// use valog::options::Options;
  ///
  /// let stack = Options::new().with_stack(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_stack(mut self, stack: bool) -> Self {
    self.stack = stack;
    self
  }

  /// Configures the anonymous memory map to be allocated using huge pages.
  ///
  /// This option corresponds to the `MAP_HUGETLB` flag on Linux. It has no effect on Windows.
  ///
  /// The size of the requested page can be specified in page bits. If not provided, the system
  /// default is requested. The requested length should be a multiple of this, or the mapping
  /// will fail.
  ///
  /// This option has no effect on file-backed memory maps and vec backed `Log`.
  ///
  /// ## Example
  ///
  /// ```
  /// use valog::options::Options;
  ///
  /// let stack = Options::new().with_huge(Some(8));
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_huge(mut self, page_bits: Option<u8>) -> Self {
    self.huge = page_bits;
    self
  }

  /// Populate (prefault) page tables for a mapping.
  ///
  /// For a file mapping, this causes read-ahead on the file. This will help to reduce blocking on page faults later.
  ///
  /// This option corresponds to the `MAP_POPULATE` flag on Linux. It has no effect on Windows.
  ///
  /// This option has no effect on vec backed `Log`.
  ///
  /// ## Example
  ///
  /// ```
  /// use valog::options::Options;
  ///
  /// let opts = Options::new().with_populate(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_populate(mut self, populate: bool) -> Self {
    self.populate = populate;
    self
  }
}

impl Options {
  /// Returns `true` if the file should be opened with read access.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::options::Options;
  ///
  /// let opts = Options::new().with_read(true);
  /// assert_eq!(opts.read(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn read(&self) -> bool {
    self.read
  }

  /// Returns `true` if the file should be opened with write access.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::options::Options;
  ///
  /// let opts = Options::new().with_write(true);
  /// assert_eq!(opts.write(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn write(&self) -> bool {
    self.write
  }

  /// Returns `true` if the file should be opened with append access.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::options::Options;
  ///
  /// let opts = Options::new().with_append(true);
  /// assert_eq!(opts.append(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn append(&self) -> bool {
    self.append
  }

  /// Returns `true` if the file should be opened with truncate access.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::options::Options;
  ///
  /// let opts = Options::new().with_truncate(true);
  /// assert_eq!(opts.truncate(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn truncate(&self) -> bool {
    self.truncate
  }

  /// Returns `true` if the file should be created if it does not exist.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::options::Options;
  ///
  /// let opts = Options::new().with_create(true);
  /// assert_eq!(opts.create(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn create(&self) -> bool {
    self.create
  }

  /// Returns `true` if the file should be created if it does not exist and fail if it does.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::options::Options;
  ///
  /// let opts = Options::new().with_create_new(true);
  /// assert_eq!(opts.create_new(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn create_new(&self) -> bool {
    self.create_new
  }

  /// Returns the offset of the memory map.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::options::Options;
  ///
  /// let opts = Options::new().with_offset(30);
  /// assert_eq!(opts.offset(), 30);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn offset(&self) -> u64 {
    self.offset
  }

  /// Returns `true` if the memory map should be suitable for a process or thread stack.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::options::Options;
  ///
  /// let opts = Options::new().with_stack(true);
  /// assert_eq!(opts.stack(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn stack(&self) -> bool {
    self.stack
  }

  /// Returns the page bits of the memory map.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::options::Options;
  ///
  /// let opts = Options::new().with_huge(Some(8));
  /// assert_eq!(opts.huge(), Some(8));
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn huge(&self) -> Option<u8> {
    self.huge
  }

  /// Returns `true` if the memory map should populate (prefault) page tables for a mapping.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::options::Options;
  ///
  /// let opts = Options::new().with_populate(true);
  /// assert_eq!(opts.populate(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn populate(&self) -> bool {
    self.populate
  }
}

impl<S> Builder<S> {
  /// Create a new in-memory value log which is backed by a anonymous memory map.
  ///
  /// **What the difference between this method and [`Builder::alloc`]?**
  ///
  /// 1. This method will use mmap anonymous to require memory from the OS directly.
  ///    If you require very large contiguous memory regions, this method might be more suitable because
  ///    it's more direct in requesting large chunks of memory from the OS.
  ///
  /// 2. Where as [`Builder::alloc`] will use an `AlignedVec` ensures we are working within Rust's memory safety guarantees.
  ///    Even if we are working with raw pointers with `Box::into_raw`,
  ///    the backend Log will reclaim the ownership of this memory by converting it back to a `Box`
  ///    when dropping the backend Log. Since `AlignedVec` uses heap memory, the data might be more cache-friendly,
  ///    especially if you're frequently accessing or modifying it.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync, unsync, Builder};
  ///
  /// // Create a sync in-memory value log.
  /// let map = Builder::new().with_capacity(1024).map_anon::<sync::ValueLog>(0u32).unwrap();
  ///
  /// // Create a unsync in-memory value log.
  /// let arena = Builder::new().with_capacity(1024).map_anon::<unsync::ValueLog>(0u32).unwrap();
  /// ```
  ///
  /// [`Builder::alloc`]: #method.alloc
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  pub fn map_anon<C>(self, fid: C::Id) -> std::io::Result<C>
  where
    C: Constructor<Checksumer = S> + Mutable,
  {
    let Self { opts, cks } = self;
    let unify = opts.unify;
    let mv = opts.magic_version;
    opts
      .to_arena_options()
      .map_anon::<C::Allocator>()
      .map_err(Into::into)
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

  /// Opens a read-only map which backed by file-backed memory map.
  ///
  /// ## Safety
  /// - All file-backed memory map constructors are marked `unsafe` because of the potential for
  ///   *Undefined Behavior* (UB) using the map if the underlying file is subsequently modified, in or
  ///   out of process. Applications must consider the risk and take appropriate precautions when
  ///   using file-backed maps. Solutions such as file permissions, locks or process-private (e.g.
  ///   unlinked) files exist but are platform specific and limited.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync, Builder};
  ///
  /// let dir = tempfile::tempdir().unwrap();
  /// let map = unsafe {
  ///   Builder::new()
  ///     .with_capacity(1024)
  ///     .with_create_new(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .map_mut::<sync::ValueLog, _>(
  ///       dir.path().join("map_example.vlog"),
  ///       1u32
  ///     )
  ///     .unwrap()
  /// };
  ///
  /// drop(map);
  ///
  /// let map = unsafe {
  ///   Builder::new()
  ///     .with_read(true)
  ///     .map::<sync::ImmutableValueLog, _>(
  ///       dir.path().join("map_example.vlog"),
  ///       1u32,
  ///     )
  ///    .unwrap()
  /// };
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  pub unsafe fn map<C, P: AsRef<std::path::Path>>(self, path: P, fid: C::Id) -> std::io::Result<C>
  where
    C: Constructor<Checksumer = S> + Frozen,
  {
    self
      .map_with_path_builder::<C, _, ()>(|| Ok(path.as_ref().to_path_buf()), fid)
      .map_err(Either::unwrap_right)
  }

  /// Opens a read-only map which backed by file-backed memory map with a path builder.
  ///
  /// ## Safety
  /// - All file-backed memory map constructors are marked `unsafe` because of the potential for
  ///   *Undefined Behavior* (UB) using the map if the underlying file is subsequently modified, in or
  ///   out of process. Applications must consider the risk and take appropriate precautions when
  ///   using file-backed maps. Solutions such as file permissions, locks or process-private (e.g.
  ///   unlinked) files exist but are platform specific and limited.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync, Builder};
  ///
  /// let dir = tempfile::tempdir().unwrap();
  /// let map = unsafe {
  ///   Builder::new()
  ///     .with_capacity(1024)
  ///     .with_create_new(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .map_mut::<sync::ValueLog, _>(
  ///       dir.path().join("map_with_path_builder_example.vlog"),
  ///       1u32
  ///     )
  ///     .unwrap()
  /// };
  ///
  /// drop(map);
  ///
  /// let map = unsafe {
  ///   Builder::new()
  ///     .with_read(true)
  ///     .map_with_path_builder::<sync::ImmutableValueLog, _, ()>(
  ///       || Ok(dir.path().join("map_with_path_builder_example.vlog")),
  ///       1u32,
  ///     )
  ///    .unwrap()
  /// };
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  pub unsafe fn map_with_path_builder<C, PB, E>(
    self,
    path_builder: PB,
    fid: C::Id,
  ) -> Result<C, Either<E, std::io::Error>>
  where
    C: Constructor<Checksumer = S> + Frozen,
    PB: FnOnce() -> Result<std::path::PathBuf, E>,
  {
    let Self { opts, cks } = self;

    let magic_version = opts.magic_version();

    opts
      .to_arena_options()
      .with_unify(true)
      .map_with_path_builder::<C::Allocator, _, _>(path_builder)
      .map_err(|e| e.map_right(Error::from_arena_io_err))
      .and_then(|arena| {
        Self::check_header(arena.reserved_slice(), magic_version)
          .map(|_| C::construct(fid, arena, cks, opts.with_magic_version(magic_version)))
          .map_err(Either::Right)
      })
  }

  /// Creates a new map or reopens a map which backed by a file backed memory map.
  ///
  /// ## Safety
  /// - All file-backed memory map constructors are marked `unsafe` because of the potential for
  ///   *Undefined Behavior* (UB) using the map if the underlying file is subsequently modified, in or
  ///   out of process. Applications must consider the risk and take appropriate precautions when
  ///   using file-backed maps. Solutions such as file permissions, locks or process-private (e.g.
  ///   unlinked) files exist but are platform specific and limited.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync, Builder};
  ///
  /// let dir = tempfile::tempdir().unwrap();
  /// let map = unsafe {
  ///   Builder::new()
  ///     .with_capacity(1024)
  ///     .with_create_new(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .map_mut::<sync::ValueLog, _>(
  ///       dir.path().join("map_mut_example.vlog"),
  ///       1u32
  ///     )
  ///     .unwrap()
  /// };
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  pub unsafe fn map_mut<C, P: AsRef<std::path::Path>>(
    self,
    path: P,
    fid: C::Id,
  ) -> std::io::Result<C>
  where
    C: Constructor<Checksumer = S> + Mutable,
  {
    self
      .map_mut_with_path_builder::<C, _, ()>(|| Ok(path.as_ref().to_path_buf()), fid)
      .map_err(Either::unwrap_right)
  }

  /// Creates a new map or reopens a map which backed by a file backed memory map with path builder.
  ///
  /// ## Safety
  /// - All file-backed memory map constructors are marked `unsafe` because of the potential for
  ///   *Undefined Behavior* (UB) using the map if the underlying file is subsequently modified, in or
  ///   out of process. Applications must consider the risk and take appropriate precautions when
  ///   using file-backed maps. Solutions such as file permissions, locks or process-private (e.g.
  ///   unlinked) files exist but are platform specific and limited.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use valog::{sync, Builder};
  ///
  /// let dir = tempfile::tempdir().unwrap();
  /// let map = unsafe {
  ///   Builder::new()
  ///     .with_capacity(1024)
  ///     .with_create_new(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .map_mut_with_path_builder::<sync::ValueLog, _, ()>(
  ///       || Ok(dir.path().join("map_mut_with_path_builder_example.vlog")),
  ///       1u32
  ///     )
  ///     .unwrap()
  /// };
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub unsafe fn map_mut_with_path_builder<C, PB, E>(
    self,
    path_builder: PB,
    fid: C::Id,
  ) -> Result<C, Either<E, std::io::Error>>
  where
    C: Constructor<Checksumer = S> + Mutable,
    PB: FnOnce() -> Result<std::path::PathBuf, E>,
  {
    let Self { opts, cks } = self;

    let magic_version = opts.magic_version();
    let path = path_builder().map_err(Either::Left)?;
    let exist = path.exists();

    opts
      .to_arena_options()
      .with_unify(true)
      .map_mut::<C::Allocator, _>(path)
      .map_err(|e| Either::Right(crate::error::Error::from_arena_io_err(e)))
      .and_then(|arena| {
        if !exist {
          write_header(arena.reserved_slice_mut(), magic_version);
        } else {
          Self::check_header(arena.reserved_slice(), magic_version).map_err(Either::Right)?;
        }

        let log = C::construct(fid, arena, cks, opts);
        Ok(log)
      })
  }

  #[inline]
  fn check_header(buf: &[u8], magic_version: u16) -> std::io::Result<u16> {
    if buf[..MAGIC_TEXT_SIZE] != MAGIC_TEXT {
      return Err(bad_magic_text());
    }

    let magic_version_from_buf =
      u16::from_le_bytes(buf[MAGIC_TEXT_SIZE..HEADER_SIZE].try_into().unwrap());
    if magic_version_from_buf != magic_version {
      return Err(bad_magic_version());
    }

    Ok(magic_version_from_buf)
  }
}

impl<C> Builder<C> {
  /// Sets the option for read access.
  ///
  /// This option, when true, will indicate that the file should be
  /// `read`-able if opened.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_read(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_read(mut self, read: bool) -> Self {
    self.opts.read = read;
    self
  }

  /// Sets the option for write access.
  ///
  /// This option, when true, will indicate that the file should be
  /// `write`-able if opened.
  ///
  /// If the file already exists, any write calls on it will overwrite its
  /// contents, without truncating it.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_write(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_write(mut self, write: bool) -> Self {
    self.opts.write = write;
    self
  }

  /// Sets the option for the append mode.
  ///
  /// This option, when true, means that writes will append to a file instead
  /// of overwriting previous contents.
  /// Note that setting `.write(true).append(true)` has the same effect as
  /// setting only `.append(true)`.
  ///
  /// For most filesystems, the operating system guarantees that all writes are
  /// atomic: no writes get mangled because another process writes at the same
  /// time.
  ///
  /// One maybe obvious note when using append-mode: make sure that all data
  /// that belongs together is written to the file in one operation. This
  /// can be done by concatenating strings before passing them to [`write()`],
  /// or using a buffered writer (with a buffer of adequate size),
  /// and calling [`flush()`] when the message is complete.
  ///
  /// If a file is opened with both read and append access, beware that after
  /// opening, and after every write, the position for reading may be set at the
  /// end of the file. So, before writing, save the current position (using
  /// <code>[seek]\([SeekFrom](std::io::SeekFrom)::[Current]\(opts))</code>), and restore it before the next read.
  ///
  /// ## Note
  ///
  /// This function doesn't create the file if it doesn't exist. Use the
  /// [`Options::with_create`] method to do so.
  ///
  /// [`write()`]: std::io::Write::write "io::Write::write"
  /// [`flush()`]: std::io::Write::flush "io::Write::flush"
  /// [seek]: std::io::Seek::seek "io::Seek::seek"
  /// [Current]: std::io::SeekFrom::Current "io::SeekFrom::Current"
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_append(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_append(mut self, append: bool) -> Self {
    self.opts.write = true;
    self.opts.append = append;
    self
  }

  /// Sets the option for truncating a previous file.
  ///
  /// If a file is successfully opened with this option set it will truncate
  /// the file to opts length if it already exists.
  ///
  /// The file must be opened with write access for truncate to work.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_write(true).with_truncate(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_truncate(mut self, truncate: bool) -> Self {
    self.opts.truncate = truncate;
    self.opts.write = true;
    self
  }

  /// Sets the option to create a new file, or open it if it already exists.
  /// If the file does not exist, it is created and set the lenght of the file to the given size.
  ///
  /// In order for the file to be created, [`Options::with_write`] or
  /// [`Options::with_append`] access must be used.
  ///
  /// See also [`std::fs::write()`][std::fs::write] for a simple function to
  /// create a file with some given data.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_write(true).with_create(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_create(mut self, val: bool) -> Self {
    self.opts.create = val;
    self
  }

  /// Sets the option to create a new file and set the file length to the given value, failing if it already exists.
  ///
  /// No file is allowed to exist at the target location, also no (dangling) symlink. In this
  /// way, if the call succeeds, the file returned is guaranteed to be new.
  ///
  /// This option is useful because it is atomic. Otherwise between checking
  /// whether a file exists and creating a new one, the file may have been
  /// created by another process (a TOCTOU race condition / attack).
  ///
  /// If `.with_create_new(true)` is set, [`.with_create()`](Options::with_create) and [`.with_truncate()`](Options::with_truncate) are
  /// ignored.
  ///
  /// The file must be opened with write or append access in order to create
  /// a new file.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let file = Builder::new()
  ///   .with_write(true)
  ///   .with_create_new(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_create_new(mut self, val: bool) -> Self {
    self.opts.create_new = val;
    self
  }

  /// Configures the memory map to start at byte `offset` from the beginning of the file.
  ///
  /// This option has no effect on anonymous memory maps or vec backed `Log`.
  ///
  /// By default, the offset is 0.
  ///
  /// ## Example
  ///
  /// ```
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_offset(30);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_offset(mut self, offset: u64) -> Self {
    self.opts.offset = offset;
    self
  }

  /// Configures the anonymous memory map to be suitable for a process or thread stack.
  ///
  /// This option corresponds to the `MAP_STACK` flag on Linux. It has no effect on Windows.
  ///
  /// This option has no effect on file-backed memory maps and vec backed `Log`.
  ///
  /// ## Example
  ///
  /// ```
  /// use valog::Builder;
  ///
  /// let stack = Builder::new().with_stack(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_stack(mut self, stack: bool) -> Self {
    self.opts.stack = stack;
    self
  }

  /// Configures the anonymous memory map to be allocated using huge pages.
  ///
  /// This option corresponds to the `MAP_HUGETLB` flag on Linux. It has no effect on Windows.
  ///
  /// The size of the requested page can be specified in page bits. If not provided, the system
  /// default is requested. The requested length should be a multiple of this, or the mapping
  /// will fail.
  ///
  /// This option has no effect on file-backed memory maps and vec backed `Log`.
  ///
  /// ## Example
  ///
  /// ```
  /// use valog::Builder;
  ///
  /// let stack = Builder::new().with_huge(Some(8));
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_huge(mut self, page_bits: Option<u8>) -> Self {
    self.opts.huge = page_bits;
    self
  }

  /// Populate (prefault) page tables for a mapping.
  ///
  /// For a file mapping, this causes read-ahead on the file. This will help to reduce blocking on page faults later.
  ///
  /// This option corresponds to the `MAP_POPULATE` flag on Linux. It has no effect on Windows.
  ///
  /// This option has no effect on vec backed `Log`.
  ///
  /// ## Example
  ///
  /// ```
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_populate(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_populate(mut self, populate: bool) -> Self {
    self.opts.populate = populate;
    self
  }
}

impl<C> Builder<C> {
  /// Returns `true` if the file should be opened with read access.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_read(true);
  /// assert_eq!(opts.read(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn read(&self) -> bool {
    self.opts.read
  }

  /// Returns `true` if the file should be opened with write access.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_write(true);
  /// assert_eq!(opts.write(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn write(&self) -> bool {
    self.opts.write
  }

  /// Returns `true` if the file should be opened with append access.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_append(true);
  /// assert_eq!(opts.append(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn append(&self) -> bool {
    self.opts.append
  }

  /// Returns `true` if the file should be opened with truncate access.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_truncate(true);
  /// assert_eq!(opts.truncate(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn truncate(&self) -> bool {
    self.opts.truncate
  }

  /// Returns `true` if the file should be created if it does not exist.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_create(true);
  /// assert_eq!(opts.create(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn create(&self) -> bool {
    self.opts.create
  }

  /// Returns `true` if the file should be created if it does not exist and fail if it does.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_create_new(true);
  /// assert_eq!(opts.create_new(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn create_new(&self) -> bool {
    self.opts.create_new
  }

  /// Returns the offset of the memory map.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_offset(30);
  /// assert_eq!(opts.offset(), 30);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn offset(&self) -> u64 {
    self.opts.offset
  }

  /// Returns `true` if the memory map should be suitable for a process or thread stack.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_stack(true);
  /// assert_eq!(opts.stack(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn stack(&self) -> bool {
    self.opts.stack
  }

  /// Returns the page bits of the memory map.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_huge(Some(8));
  /// assert_eq!(opts.huge(), Some(8));
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn huge(&self) -> Option<u8> {
    self.opts.huge
  }

  /// Returns `true` if the memory map should populate (prefault) page tables for a mapping.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use valog::Builder;
  ///
  /// let opts = Builder::new().with_populate(true);
  /// assert_eq!(opts.populate(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn populate(&self) -> bool {
    self.opts.populate
  }
}

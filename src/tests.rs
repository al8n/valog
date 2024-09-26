use error::Error;
use rarena_allocator::Allocator;
use sealed::Sealed;

use super::*;

#[test]
fn test_read_out_of_bounds() {
  let log = Builder::new()
    .with_capacity(100)
    .alloc::<crate::sync::ValueLog>(0)
    .unwrap();

  let err = log.read(0, 10).unwrap_err();
  assert!(matches!(err, Error::OutOfBounds { .. }));

  let err = log.read(10, 10).unwrap_err();
  assert!(matches!(err, Error::OutOfBounds { .. }));
}

#[test]
fn test_checksum_mismatch() {
  let log = Builder::new()
    .with_capacity(100)
    .alloc::<crate::sync::ValueLog>(0)
    .unwrap();

  let vp = log.insert(b"Hello, valog!").unwrap();
  assert_eq!(*vp.id(), 0);
  unsafe {
    log
      .allocator()
      .raw_mut_ptr()
      .add(vp.offset() as usize)
      .write(0);
  }
  let err = log.read(vp.offset(), vp.size()).unwrap_err();
  assert!(matches!(err, Error::ChecksumMismatch));
}

#[test]
fn test_insert_big_value() {
  let log = Builder::new()
    .with_capacity(100)
    .with_maximum_value_size(3)
    .alloc::<crate::sync::ValueLog>(0)
    .unwrap();

  let err = log.insert(&[0; 10]).unwrap_err();
  assert!(matches!(err, Error::ValueTooLarge { .. }));
}

#[test]
fn test_insert_insufficient() {
  let log = Builder::new()
    .with_capacity(100)
    .alloc::<crate::sync::ValueLog>(0)
    .unwrap();

  let err = log.insert(&[0; 100]).unwrap_err();
  assert!(matches!(err, Error::InsufficientSpace { .. }));
}

#[test]
fn test_insert_empty_value() {
  let log = Builder::new()
    .with_capacity(100)
    .alloc::<crate::sync::ValueLog>(0)
    .unwrap();

  let vp = log.insert(&[]).unwrap();
  assert_eq!(*vp.id(), 0);
  assert_eq!(vp.offset(), 0);
  assert_eq!(vp.size(), 0);
}

#[test]
fn test_basic() {
  let log = Builder::new()
    .with_capacity(100)
    .alloc::<crate::sync::ValueLog>(0)
    .unwrap();
  assert_eq!(*log.id(), 0);
  assert_eq!(log.options().capacity(), 100);

  let log = Builder::new()
    .with_capacity(100)
    .alloc::<crate::sync::GenericValueLog<String>>(0)
    .unwrap();
  assert_eq!(*log.id(), 0);
  assert_eq!(log.options().capacity(), 100);
}

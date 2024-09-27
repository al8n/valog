use dbutils::CheapClone;
use error::Error;
use rarena_allocator::Allocator;
use sealed::Sealed;

use super::*;

pub(crate) const MB: u32 = 1024 * 1024;

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

#[test]
#[cfg_attr(miri, ignore)]
#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
fn test_reopen_and_concurrent_read() {
  use crate::sync::{ImmutableValueLog, ValueLog};

  let dir = tempfile::tempdir().unwrap();
  let p = dir.path().join("test_reopen_and_concurrent_read");

  let log = unsafe {
    Builder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<ValueLog, _>(&p, 0)
      .unwrap()
  };

  let ptrs = (0..1000u32)
    .map(|i| log.insert(i.to_string().as_bytes()).unwrap())
    .collect::<Vec<_>>();

  drop(log);

  let log = unsafe {
    Builder::new()
      .with_read(true)
      .map::<ImmutableValueLog, _>(&p, 0)
      .unwrap()
  };

  let (tx, rx) = crossbeam_channel::bounded(1000);

  ptrs.into_iter().for_each(|vp| {
    let l = log.clone();
    let tx = tx.clone();

    std::thread::spawn(move || {
      let bytes = l.read(vp.offset(), vp.size()).unwrap();
      let val: u32 = std::str::from_utf8(bytes).unwrap().parse().unwrap();
      tx.send(val).unwrap();
    });
  });

  let mut data = Vec::with_capacity(1000);
  for _ in 0..1000 {
    data.push(rx.recv().unwrap());
  }

  data.sort_unstable();
  assert_eq!(data, (0..1000).collect::<Vec<_>>());
}

#[test]
#[cfg_attr(miri, ignore)]
#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
fn test_reopen_and_read() {
  use crate::unsync::{ImmutableValueLog, ValueLog};

  let dir = tempfile::tempdir().unwrap();
  let p = dir.path().join("test_reopen_and_read");

  let log = unsafe {
    Builder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<ValueLog, _>(&p, 0)
      .unwrap()
  };

  let ptrs = (0..1000u32)
    .map(|i| log.insert(i.to_string().as_bytes()).unwrap())
    .collect::<Vec<_>>();

  drop(log);

  let log = unsafe {
    Builder::new()
      .with_read(true)
      .map::<ImmutableValueLog, _>(&p, 0)
      .unwrap()
  };

  let mut data = ptrs
    .into_iter()
    .map(|vp| {
      let l = log.clone();

      let bytes = l.read(vp.offset(), vp.size()).unwrap();
      let val: u32 = std::str::from_utf8(bytes).unwrap().parse().unwrap();
      val
    })
    .collect::<Vec<_>>();

  data.sort_unstable();
  assert_eq!(data, (0..1000).collect::<Vec<_>>());
}

#[macro_export]
#[doc(hidden)]
macro_rules! __common_tests {
  ($mod:ident($ty:ty) {
    $($method:ident),+ $(,)?
  }) => {
    paste::paste! {
      #[cfg(test)]
      mod $mod {
        $(
          #[test]
          fn [<test_ $method _vec>]() {
            let log = $crate::Builder::new()
              .with_capacity($crate::tests::MB)
              .alloc::<$ty>(0)
              .unwrap();
            $crate::tests::$method(log);
          }

          #[test]
          fn [<test_ $method _vec_unify>]() {
            let log = $crate::Builder::new()
              .with_capacity($crate::tests::MB)
              .with_unify(true)
              .alloc::<$ty>(0)
              .unwrap();
            $crate::tests::$method(log);
          }

          #[test]
          #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
          fn [<test_ $method _map_anon>]() {
            let log = $crate::Builder::new()
              .with_capacity($crate::tests::MB)
              .with_lock_meta(true)
              .map_anon::<$ty>(0)
              .unwrap();
            $crate::tests::$method(log);
          }

          #[test]
          #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
          fn [<test_ $method _map_anon_unify>]() {
            let log = $crate::Builder::new()
              .with_capacity($crate::tests::MB)
              .with_unify(true)
              .with_lock_meta(true)
              .map_anon::<$ty>(0)
              .unwrap();
            $crate::tests::$method(log);
          }

          #[test]
          #[cfg_attr(miri, ignore)]
          #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
          fn [<test_ $method _map_mut>]() {
            let dir = ::tempfile::tempdir().unwrap();
            let p = dir
              .path()
              .join(::std::format!("test_{}_map_mut", stringify!($method)));

            let log = unsafe {
              $crate::Builder::new()
                .with_capacity($crate::tests::MB)
                .with_create_new(true)
                .with_read(true)
                .with_write(true)
                .with_lock_meta(true)
                .map_mut::<$ty, _>(&p, 0)
                .unwrap()
            };
            $crate::tests::$method(log);
          }
        )*
      }
    }
  };
  ($mod:ident($ty:ty)::spawn {
    $($method:ident),+ $(,)?
  }) => {
    paste::paste! {
      #[cfg(test)]
      mod [< concurrent_ $mod >] {
        $(
          #[test]
          #[cfg(feature = "std")]
          fn [<test_ $method _vec>]() {
            let log = $crate::Builder::new()
              .with_capacity($crate::tests::MB)
              .alloc::<$ty>(0)
              .unwrap();
            $crate::tests::$method(log);
          }

          #[test]
          #[cfg(feature = "std")]
          fn [<test_ $method _vec_unify>]() {
            let log = $crate::Builder::new()
              .with_capacity($crate::tests::MB)
              .with_unify(true)
              .alloc::<$ty>(0)
              .unwrap();
            $crate::tests::$method(log);
          }

          #[test]
          #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
          fn [<test_ $method _map_anon>]() {
            let log = $crate::Builder::new()
              .with_capacity($crate::tests::MB)
              .map_anon::<$ty>(0)
              .unwrap();
            $crate::tests::$method(log);
          }

          #[test]
          #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
          fn [<test_ $method _map_anon_unify>]() {
            let log = $crate::Builder::new()
              .with_capacity($crate::tests::MB)
              .with_unify(true)
              .map_anon::<$ty>(0)
              .unwrap();
            $crate::tests::$method(log);
          }

          #[test]
          #[cfg_attr(miri, ignore)]
          #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
          fn [<test_ $method _map_mut>]() {
            let dir = ::tempfile::tempdir().unwrap();
            let p = dir
              .path()
              .join(::std::format!("test_{}_map_mut", stringify!($method)));

            let log = unsafe {
              $crate::Builder::new()
                .with_capacity($crate::tests::MB)
                .with_create_new(true)
                .with_read(true)
                .with_write(true)
                .map_mut::<$ty, _>(&p, 0)
                .unwrap()
            };
            $crate::tests::$method(log);
          }
        )*
      }
    }
  };
}

pub(crate) fn basic<L: LogWriter + LogReader>(l: L)
where
  L::Id: core::fmt::Debug + CheapClone,
{
  #[cfg(not(miri))]
  const N: u32 = 500;
  #[cfg(miri)]
  const N: u32 = 200;

  let data = (0..N)
    .map(|i| {
      let val = i.to_string();
      match i % 6 {
        0 => l.insert(val.as_bytes()).unwrap(),
        1 => l.insert_generic::<String>(&val).unwrap(),
        2 => l
          .insert_with(ValueBuilder::new(
            val.len() as u32,
            |buf: &mut VacantBuffer<'_>| buf.put_slice(val.as_bytes()),
          ))
          .unwrap(),
        3 => l.insert_tombstone(val.as_bytes()).unwrap(),
        4 => l.insert_generic_tombstone::<String>(&val).unwrap(),
        5 => l
          .insert_tombstone_with(ValueBuilder::new(
            val.len() as u32,
            |buf: &mut VacantBuffer<'_>| buf.put_slice(val.as_bytes()),
          ))
          .unwrap(),
        _ => unreachable!(),
      }
    })
    .collect::<Vec<_>>();

  for (i, vp) in data.iter().enumerate() {
    let bytes = l.read(vp.offset(), vp.size()).unwrap();
    let val: u32 = std::str::from_utf8(bytes).unwrap().parse().unwrap();
    assert_eq!(i, val as usize);
  }
}

#[cfg(feature = "std")]
pub(crate) fn concurrent_basic<L>(l: L)
where
  L: Clone + LogWriter + LogReader + Send + 'static,
  L::Id: core::fmt::Debug + CheapClone + Send,
{
  use std::sync::{Arc, Mutex};
  use wg::WaitGroup;

  #[cfg(not(miri))]
  const N: u32 = 1000;

  #[cfg(miri)]
  const N: u32 = 100;

  let (tx, rx) = crossbeam_channel::bounded(N as usize);
  let data = Arc::new(Mutex::new(Vec::new()));
  let wg = WaitGroup::new();

  // concurrent write
  (0..N).for_each(|i| {
    let l = l.clone();
    let tx = tx.clone();
    let wg = wg.add(1);
    std::thread::spawn(move || {
      let val = i.to_string();
      let vp = match i % 6 {
        0 => l.insert(val.as_bytes()).unwrap(),
        1 => l.insert_generic::<String>(&val).unwrap(),
        2 => l
          .insert_with(ValueBuilder::new(
            val.len() as u32,
            |buf: &mut VacantBuffer<'_>| buf.put_slice(val.as_bytes()),
          ))
          .unwrap(),
        3 => l.insert_tombstone(val.as_bytes()).unwrap(),
        4 => l.insert_generic_tombstone::<String>(&val).unwrap(),
        5 => l
          .insert_tombstone_with(ValueBuilder::new(
            val.len() as u32,
            |buf: &mut VacantBuffer<'_>| buf.put_slice(val.as_bytes()),
          ))
          .unwrap(),
        _ => unreachable!(),
      };

      tx.send(vp).unwrap();
      wg.done();
    });
  });

  // concurrent read
  (0..N).for_each(|i| {
    let l = l.clone();
    let rx = rx.clone();
    let data = data.clone();

    let wg = wg.add(1);
    std::thread::spawn(move || {
      for vp in rx {
        let val = if i % 2 == 0 {
          let bytes = l.read(vp.offset(), vp.size()).unwrap();
          let val: u32 = std::str::from_utf8(bytes).unwrap().parse().unwrap();
          val
        } else {
          let bytes = unsafe { l.read_generic::<String>(vp.offset(), vp.size()).unwrap() };
          let val: u32 = bytes.parse().unwrap();
          val
        };

        data.lock().unwrap().push(val);
      }

      wg.done();
    });
  });

  drop(tx);
  wg.wait();

  let mut data = data.lock().unwrap();
  data.sort_unstable();
  assert_eq!(data.as_slice(), &(0..N).collect::<Vec<_>>());
}

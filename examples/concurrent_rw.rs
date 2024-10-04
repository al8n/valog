use std::sync::{Arc, Mutex};
use valog::{
  sync::ValueLog, Builder, Log, LogReader, LogReaderExt, LogWriter, LogWriterExt, VacantBuffer,
  ValueBuilder,
};
use wg::WaitGroup;

fn main() {
  const N: u32 = 500;

  let (tx, rx) = crossbeam_channel::bounded(N as usize);
  let data = Arc::new(Mutex::new(Vec::new()));
  let wg = WaitGroup::new();

  let dir = ::tempfile::tempdir().unwrap();
  let p = dir.path().join("example.vlog");

  let l = unsafe {
    Builder::new()
      .with_capacity(1024 * 1024)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<ValueLog, _>(&p, 0)
      .unwrap()
  };

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
          let bytes = unsafe { l.read(l.id(), vp.offset(), vp.size()).unwrap() };
          let val: u32 = std::str::from_utf8(bytes).unwrap().parse().unwrap();
          val
        } else {
          let bytes = unsafe {
            l.read_generic::<String>(l.id(), vp.offset(), vp.size())
              .unwrap()
          };
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

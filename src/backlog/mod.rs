use ops;
use query;
use chashmap::CHashMap;

use std::sync;
use std::sync::atomic::{Ordering, AtomicIsize};

type S = sync::Arc<CHashMap<query::DataType, Vec<sync::Arc<Vec<query::DataType>>>>>;
pub struct WriteHandle {
    data: S,
    ts: sync::Arc<AtomicIsize>,
    key: usize,
}

#[derive(Clone)]
pub struct BufferedStore {
    data: S,
    ts: sync::Arc<AtomicIsize>,
    key: usize,
}

pub struct BufferedStoreBuilder {
    key: usize,
}

impl WriteHandle {
    pub fn swap(&mut self) {}

    pub fn add<I>(&mut self, rs: I)
        where I: IntoIterator<Item = ops::Record>
    {
        for r in rs {
            // apply to the current write set
            match r {
                ops::Record::Positive(..) => {
                    let (r, _) = r.extract();
                    if let Some(mut rs) = self.data.get_mut(&r[self.key]) {
                        rs.push(r);
                    } else {
                        self.data.insert(r[self.key].clone(), vec![r]);
                    }
                }
                ops::Record::Negative(r) => {
                    let mut now_empty = false;
                    if let Some(mut e) = self.data.get_mut(&r[self.key]) {
                        // find the first entry that matches all fields
                        if let Some(i) = e.iter().position(|er| er == &r) {
                            e.swap_remove(i);
                            now_empty = e.is_empty();
                        }
                    }
                    if now_empty {
                        // no more entries for this key -- free up some space in the map
                        self.data.remove(&r[self.key]);
                    }
                }
            }
        }
    }

    pub fn update_ts(&mut self, ts: i64) {
        self.ts.store(ts as isize, Ordering::SeqCst);
    }
}

/// Allocate a new buffered `Store`.
pub fn new(_: usize, key: usize) -> BufferedStoreBuilder {
    BufferedStoreBuilder { key: key }
}

impl BufferedStoreBuilder {
    pub fn commit(self) -> (BufferedStore, WriteHandle) {
        let store = sync::Arc::new(CHashMap::new());
        let ts = sync::Arc::new(AtomicIsize::new(-1));
        let r = BufferedStore {
            data: store.clone(),
            ts: ts.clone(),
            key: self.key,
        };
        let w = WriteHandle {
            data: store.clone(),
            ts: ts.clone(),
            key: self.key,
        };
        (r, w)
    }
}

impl BufferedStore {
    /// Find all entries that matched the given conditions.
    ///
    /// Returned records are passed to `then` before being returned.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// swapped in by the writer.
    pub fn find_and<F, T>(&self, key: &query::DataType, then: F) -> Result<(T, i64), ()>
        where F: FnOnce(&[sync::Arc<Vec<query::DataType>>]) -> T
    {
        self.data
            .get(key)
            .map(|rs| then(&rs[..]))
            .map(|v| (v, self.ts.load(Ordering::SeqCst) as i64))
            .ok_or(())
    }
}

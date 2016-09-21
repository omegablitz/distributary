use memcache;

struct Memcache(memcache::Memcache);
unsafe impl Send for Memcache {}

use std::ops::Deref;
impl Deref for Memcache {
    type Target = memcache::Memcache;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

use Backend;
use Putter;
use Getter;

pub fn make(dbn: &str, getters: usize) -> Box<Backend> {
    let mut dbn = dbn.splitn(2, ':');
    let host = dbn.next().unwrap();
    let port: u64 = dbn.next().unwrap().parse().unwrap();
    Box::new((0..(getters + 1))
        .into_iter()
        .map(|_| Memcache(memcache::connect(&(host, port)).unwrap()))
        .collect::<Vec<_>>())
}

impl Backend for Vec<Memcache> {
    fn getter(&mut self) -> Box<Getter> {
        Box::new(self.pop().unwrap())
    }

    fn putter(&mut self) -> Box<Putter> {
        Box::new(self.pop().unwrap())
    }
}

impl Putter for Memcache {
    fn article<'a>(&'a mut self) -> Box<FnMut(i64, String) + 'a> {
        Box::new(move |id, title| {
            self.set_raw(&format!("article_{}", id), title.as_bytes(), 0, 0).unwrap();
            self.set_raw(&format!("article_{}_vc", id), b"0", 0, 0).unwrap();
        })
    }

    fn vote<'a>(&'a mut self) -> Box<FnMut(i64, i64) + 'a> {
        Box::new(move |user, id| {
            self.set_raw(&format!("voted_{}_{}", user, id), b"1", 0, 0).unwrap();
            self.increment(&format!("article_{}_vc", id), 1).unwrap();
        })
    }
}

impl Getter for Memcache {
    fn get<'a>(&'a self) -> Box<FnMut(i64) -> Option<(i64, String, i64)> + 'a> {
        Box::new(move |id| {
            let title = self.get_raw(&format!("article_{}", id)).unwrap();
            let vc = self.get_raw(&format!("article_{}_vc", id)).unwrap();
            let vc: i64 = String::from_utf8_lossy(&vc.0[..]).parse().unwrap();
            Some((id, String::from_utf8_lossy(&title.0[..]).into_owned(), vc))
        })
    }
}
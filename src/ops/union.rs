use ops;
use flow;
use query;
use backlog;
use ops::NodeOp;
use ops::NodeType;

use std::collections::HashMap;
use std::cell::RefCell;

use shortcut;

/// A union of a set of views.
#[derive(Debug)]
pub struct Union {
    emit: HashMap<flow::NodeIndex, Vec<usize>>,
    srcs: HashMap<flow::NodeIndex, ops::V>,
    cols: HashMap<flow::NodeIndex, usize>,

    gather: RefCell<HashMap<flow::NodeIndex, Vec<ops::Record>>>,
}

// gather isn't normally Sync, but we know that we're only
// accessing it from one place at any given time, so it's fine..
unsafe impl Sync for Union {}

impl Union {
    /// Construct a new union operator.
    ///
    /// When receiving an update from node `a`, a union will emit the columns selected in `emit[a]`.
    /// `emit` only supports omitting columns, not rearranging them.
    pub fn new(emit: HashMap<flow::NodeIndex, Vec<usize>>) -> Union {
        for emit in emit.values() {
            let mut last = &emit[0];
            for i in emit {
                if i < last {
                    unimplemented!();
                }
                last = i;
            }
        }
        Union {
            emit: emit,
            srcs: HashMap::new(),
            cols: HashMap::new(),

            gather: RefCell::new(HashMap::new()),
        }
    }
}

impl From<Union> for NodeType {
    fn from(b: Union) -> NodeType {
        NodeType::Union(b)
    }
}

impl Union {
    fn drain<I>(&self, it: I) -> flow::ProcessingResult<ops::Update>
        where I: Iterator<Item = (flow::NodeIndex, Vec<ops::Record>)>
    {
        let rs: Vec<_> = it.flat_map(|(from, rs)| {
                rs.into_iter().map(move |rec| {
                    let (r, pos, ts) = rec.extract();

                    // yield selected columns for this source
                    // TODO: avoid the .clone() here
                    let res = self.emit[&from].iter().map(|&col| r[col].clone()).collect();

                    // return new row with appropriate sign
                    if pos {
                        ops::Record::Positive(res, ts)
                    } else {
                        ops::Record::Negative(res, ts)
                    }
                })
            })
            .collect();

        if !rs.is_empty() {
            flow::ProcessingResult::Done(ops::Update::Records(rs))
        } else {
            flow::ProcessingResult::Skip
        }
    }
}

impl NodeOp for Union {
    fn prime(&mut self, g: &ops::Graph) -> Vec<flow::NodeIndex> {
        self.srcs.extend(self.emit.keys().map(|&n| (n, g[n].as_ref().unwrap().clone())));
        self.cols.extend(self.srcs.iter().map(|(ni, n)| (*ni, n.args().len())));
        self.emit.keys().cloned().collect()
    }

    fn forward(&self,
               u: Option<ops::Update>,
               from: flow::NodeIndex,
               _: i64,
               last: bool,
               _: Option<&backlog::BufferedStore>)
               -> flow::ProcessingResult<ops::Update> {

        debug_assert!(u.is_some() || last);
        let mut g = self.gather.borrow_mut();

        match u {
            Some(ops::Update::Records(rs)) => {
                // if we haven't received updates from all our ancestors for this timestamp yet,
                // just buffer this update and delay completing processing of this timestamp.
                if !last {
                    g.insert(from, rs);
                    return flow::ProcessingResult::Accepted;
                }

                // we've received all updates for this ts
                // emit all of them in a single update
                self.drain(g.drain().chain(Some((from, rs)).into_iter()))
            }
            None if last => self.drain(g.drain()),
            _ => unreachable!(),
        }
    }

    fn query(&self, q: Option<&query::Query>, ts: i64) -> ops::Datas {
        use std::iter;

        let mut params = HashMap::new();
        for src in self.srcs.keys() {
            params.insert(*src, None);

            // Avoid scanning rows that wouldn't match the query anyway. We do this by finding all
            // conditions that filter over a field present in left, and use those as parameters.
            let emit = &self.emit[src];
            if let Some(q) = q {
                let p: Vec<_> = q.having
                    .iter()
                    .map(|c| {
                        shortcut::Condition {
                            column: emit[c.column],
                            cmp: c.cmp.clone(),
                        }
                    })
                    .collect();

                if !p.is_empty() {
                    params.insert(*src, Some(p));
                }
            }
        }

        // we select from each source in turn
        params.into_iter()
            .flat_map(move |(src, params)| {
                let emit = &self.emit[&src];
                let mut select: Vec<_> = iter::repeat(false).take(self.cols[&src]).collect();
                for c in emit {
                    select[*c] = true;
                }
                let cs = params.unwrap_or_else(Vec::new);
                // TODO: if we're selecting all and have no conditions, we could pass q = None...
                self.srcs[&src].find(Some(&query::Query::new(&select[..], cs)), Some(ts))
            })
            .filter_map(move |(r, ts)| if let Some(q) = q {
                q.feed(r).map(move |r| (r, ts))
            } else {
                Some((r, ts))
            })
            .collect()
    }

    fn suggest_indexes(&self, _: flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>> {
        // index nothing (?)
        HashMap::new()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(flow::NodeIndex, usize)>> {
        Some(self.emit.iter().map(|(src, emit)| (*src, emit[col])).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;
    use flow;
    use query;
    use petgraph;
    use shortcut;

    use flow::View;
    use ops::NodeOp;
    use std::collections::HashMap;

    fn setup() -> (ops::Node, flow::NodeIndex, flow::NodeIndex) {
        use std::sync;

        let mut g = petgraph::Graph::new();
        let mut l = ops::new("left", &["l0", "l1"], true, ops::base::Base {});
        let mut r = ops::new("right", &["r0", "r1", "r2"], true, ops::base::Base {});

        l.prime(&g);
        r.prime(&g);

        let l = g.add_node(Some(sync::Arc::new(l)));
        let r = g.add_node(Some(sync::Arc::new(r)));

        g[l].as_ref().unwrap().process((vec![1.into(), "a".into()], 0).into(), l, 0);
        g[l].as_ref().unwrap().process((vec![2.into(), "b".into()], 1).into(), l, 1);
        g[r].as_ref().unwrap().process((vec![1.into(), "skipped".into(), "x".into()], 2).into(),
                                       r,
                                       2);

        let mut emits = HashMap::new();
        emits.insert(l, vec![0, 1]);
        emits.insert(r, vec![0, 2]);

        let mut c = Union::new(emits);
        c.prime(&g);
        (ops::new("union", &["u0", "u1"], false, c), l, r)
    }

    #[test]
    fn it_works() {
        let (u, l, r) = setup();

        // forward from left should emit original record
        let left = vec![1.into(), "a".into()];
        match u.process(left.clone().into(), l, 0).unwrap() {
            ops::Update::Records(rs) => {
                assert_eq!(rs, vec![ops::Record::Positive(left, 0)]);
            }
        }

        // forward from right should emit subset record
        let right = vec![1.into(), "skipped".into(), "x".into()];
        match u.process(right.clone().into(), r, 0).unwrap() {
            ops::Update::Records(rs) => {
                assert_eq!(rs,
                           vec![ops::Record::Positive(vec![1.into(), "x".into()], 0)]);
            }
        }
    }

    #[test]
    fn it_queries() {
        let (u, _, _) = setup();

        // do a full query, which should return left + right:
        // [a, b, x]
        let hits = u.find(None, None);
        assert_eq!(hits.len(), 3);
        assert!(hits.iter().any(|&(ref r, ts)| ts == 0 && r[0] == 1.into() && r[1] == "a".into()));
        assert!(hits.iter().any(|&(ref r, ts)| ts == 1 && r[0] == 2.into() && r[1] == "b".into()));
        assert!(hits.iter().any(|&(ref r, ts)| ts == 2 && r[0] == 1.into() && r[1] == "x".into()));

        // query with parameters matching on both sides
        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(1.into())),
                         }]);

        let hits = u.find(Some(&q), None);
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|&(ref r, ts)| ts == 0 && r[0] == 1.into() && r[1] == "a".into()));
        assert!(hits.iter().any(|&(ref r, ts)| ts == 2 && r[0] == 1.into() && r[1] == "x".into()));

        // query with parameter matching only on left
        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                         }]);

        let hits = u.find(Some(&q), None);
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|&(ref r, ts)| ts == 1 && r[0] == 2.into() && r[1] == "b".into()));

        // query with parameter matching only on right
        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 1,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const("x".into())),
                         }]);

        let hits = u.find(Some(&q), None);
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|&(ref r, ts)| ts == 2 && r[0] == 1.into() && r[1] == "x".into()));

        // query with parameter with no matches
        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(3.into())),
                         }]);

        let hits = u.find(Some(&q), None);
        assert_eq!(hits.len(), 0);
    }

    #[test]
    fn it_suggests_indices() {
        use std::collections::HashMap;
        let (u, _, _) = setup();
        assert_eq!(u.suggest_indexes(1.into()), HashMap::new());
    }

    #[test]
    fn it_resolves() {
        let (u, l, r) = setup();
        let r0 = u.resolve(0);
        assert!(r0.as_ref().unwrap().iter().any(|&(n, c)| n == l && c == 0));
        assert!(r0.as_ref().unwrap().iter().any(|&(n, c)| n == r && c == 0));
        let r1 = u.resolve(1);
        assert!(r1.as_ref().unwrap().iter().any(|&(n, c)| n == l && c == 1));
        assert!(r1.as_ref().unwrap().iter().any(|&(n, c)| n == r && c == 2));
    }
}

use ops;
use flow;
use query;
use backlog;
use ops::NodeOp;
use ops::NodeType;

use std::iter;
use std::collections::HashMap;

use shortcut;

#[derive(Debug)]
struct JoinTarget {
    fields: Vec<(usize, usize)>,
    select: Vec<bool>,
    outer: bool,
}

#[derive(Debug)]
struct Join {
    against: HashMap<flow::NodeIndex, JoinTarget>,
    node: Option<ops::V>,
}

/// Convenience struct for building join nodes.
pub struct Builder {
    emit: Vec<(flow::NodeIndex, usize)>,
    join: HashMap<flow::NodeIndex, (bool, Vec<usize>)>,
}

impl Builder {
    /// Build a new join operator.
    ///
    /// `emit` dictates, for each output column, which source and column should be used.
    pub fn new(emit: Vec<(flow::NodeIndex, usize)>) -> Self {
        Builder {
            emit: emit,
            join: HashMap::new(),
        }
    }

    /// Set the source view for this join.
    ///
    /// This is semantically identical to `join`, except that it also asserts that this is the
    /// first view being added. The first view is of particular importance as it dictates the
    /// behavior of later *left* joins (when they are added).
    pub fn from(self, node: flow::NodeIndex, groups: Vec<usize>) -> Self {
        assert!(self.join.is_empty());
        self.join(node, groups)
    }

    /// Also join with the given `node`.
    ///
    /// `groups` gives the group assignments for each output column of `node`. Columns across join
    /// sources that share a group are used to join rows from those sources by equality. Thus, each
    /// group number can appear at most once for each view.
    ///
    /// Let us look at a SQL join such as
    ///
    /// ```sql
    /// SELECT a.0, b.0
    /// FROM a JOIN b USING (a.0 == b.1)
    /// ```
    ///
    /// Assuming `a` has two columns and `b` has three, the map would look like this:
    ///
    /// ```rust,ignore
    /// Builder::new(vec![(a, 0), (b, 0)]).from(a, vec![1, 0]).join(b, vec![0, 1, 0]);
    /// ```
    pub fn join(mut self, node: flow::NodeIndex, groups: Vec<usize>) -> Self {
        assert!(self.join.insert(node, (false, groups)).is_none());
        self
    }

    /// Also perform a left join against the given `node`.
    ///
    /// The semantics of this is similar to the SQL notion of a `LEFT JOIN`, namely that records
    /// from other tables that join against this table will always be present in the output,
    /// regardless of whether matching records exist in `node`. For such *zero rows*, all columns
    /// emitted from this node will be set to `DataType::None`.
    pub fn left_join(mut self, node: flow::NodeIndex, groups: Vec<usize>) -> Self {
        assert!(self.join.insert(node, (true, groups)).is_none());
        self
    }
}

impl From<Builder> for Joiner {
    fn from(b: Builder) -> Joiner {
        if b.join.len() != 2 {
            // only two-way joins are currently supported
            unimplemented!();
        }

        // we technically want this assert, but we don't have self.nodes until .prime() has been
        // called. unfortunately, at that time, we don't have .join in the original format, and so
        // the debug doesn't makes sense. it's probably not worth carrying along the original join
        // map just to verify this, but maybe...
        // assert!(self.nodes.iter().all(|(ni, n)| self.join[ni].len() == n.args().len()));

        // the format of `join` is convenient for users, but not particulary convenient for lookups
        // the particular use-case we want to be efficient is:
        //
        //  - we are given a record from `src`
        //  - for each other parent `p`, we want to know which columns of `p` to constrain, and
        //    which values in the `src` record those correspond to
        //
        // so, we construct a map of the form
        //
        //   src: NodeIndex => {
        //     p: NodeIndex => [(srci, pi), ...]
        //   }
        //
        let join = b.join
            .iter()
            .map(|(&src, &(_, ref srcg))| {
                // which groups are bound to which columns?
                let g2c = srcg.iter()
                    .enumerate()
                    .filter_map(|(c, &g)| { if g == 0 { None } else { Some((g, c)) } })
                    .collect::<HashMap<_, _>>();

                // for every other view
                let other = b.join
                    .iter()
                    .filter_map(|(&p, &(outer, ref pg))| {
                        // *other* view
                        if p == src {
                            return None;
                        }
                        // look through the group assignments for that other view
                        let pg: Vec<_> = pg.iter()
                            .enumerate()
                            .filter_map(|(pi, g)| {
                                // look for ones that share a group with us
                                g2c.get(g).map(|srci| {
                                    // and emit that mapping
                                    (*srci, pi)
                                })
                            })
                            .collect();

                        // if there are no shared columns, don't join against this view
                        if pg.is_empty() {
                            return None;
                        }
                        // but if there are, emit the mapping we found
                        Some((p,
                              JoinTarget {
                            fields: pg,
                            outer: outer,
                            select: Vec::new(),
                        }))
                    })
                    .collect();

                (src,
                 Join {
                    against: other,
                    node: None,
                })
            })
            .collect();

        Joiner {
            emit: b.emit,
            join: join,
        }
    }
}

impl From<Joiner> for NodeType {
    fn from(b: Joiner) -> NodeType {
        NodeType::Join(b)
    }
}

impl From<Builder> for NodeType {
    fn from(b: Builder) -> NodeType {
        NodeType::Join(b.into())
    }
}

/// Joiner provides a 2-way join between two views.
///
/// It shouldn't be *too* hard to extend this to `n`-way joins, but it would require restructuring
/// `.join` such that it can express "query this view first, then use one of its columns to query
/// this other view".
#[derive(Debug)]
pub struct Joiner {
    emit: Vec<(flow::NodeIndex, usize)>,
    join: HashMap<flow::NodeIndex, Join>,
}

impl Joiner {
    fn join<'a>(&'a self,
                left: (flow::NodeIndex, Vec<query::DataType>, i64),
                ts: i64)
                -> Box<Iterator<Item = (Vec<query::DataType>, i64)> + 'a> {

        // NOTE: this only works for two-way joins
        let other = *self.join.keys().find(|&other| other != &left.0).unwrap();
        let this = &self.join[&left.0];
        let target = &this.against[&other];

        // figure out the join values for this record
        let params = target.fields
            .iter()
            .map(|&(lefti, righti)| {
                shortcut::Condition {
                    column: righti,
                    cmp: shortcut::Comparison::Equal(shortcut::Value::Const(left.1[lefti].clone())),
                }
            })
            .collect();

        // TODO: technically, we only need the columns in .join and .emit
        let q = query::Query::new(&target.select[..], params);

        // send the parameters to start the query.
        let rx = self.join[&other].node.as_ref().unwrap().find(Some(&q), Some(ts));

        if rx.is_empty() && target.outer {
            return Box::new(Some((self.emit
                    .iter()
                    .map(|&(source, column)| {
                        if source == other {
                            query::DataType::None
                        } else {
                            // this clone is unnecessary
                            left.1[column].clone()
                        }
                    })
                    .collect(),
                                  left.2))
                .into_iter());
        }

        Box::new(rx.into_iter().map(move |(right, rts)| {
            use std::cmp;

            // weave together r and j according to join rules
            let r = self.emit
                .iter()
                .map(|&(source, column)| {
                    if source == other {
                        // FIXME: this clone is unnecessary.
                        // it's tricky to remove though, because it means we'd need to
                        // be removing things from right. what if a later column also needs
                        // to select from right? we'd need to keep track of which things we
                        // have removed, and subtract that many from the index of the
                        // later column. ugh.
                        right[column].clone()
                    } else {
                        left.1[column].clone()
                    }
                })
                .collect();

            // we need to be careful here.
            // we want to emit a record with the *same* timestamp regardless of which side of the
            // join is left and right. this is particularly important when the left is a negative,
            // because we want the resulting negative records to have the same timestamp as the
            // original positive we sent. however, the original positive *could* have been produced
            // by a right, not a left. in that case, the positive has the timestamp of the right!
            // we solve this by making the output timestamp always be the max of the left and
            // right, as this must be the timestamp that resulted in the join output in the first
            // place.
            (r, cmp::max(left.2, rts))
        }))
    }
}

impl NodeOp for Joiner {
    fn prime(&mut self, g: &ops::Graph) -> Vec<flow::NodeIndex> {
        for (ni, j) in &mut self.join {
            j.node = g[*ni].as_ref().cloned();

            for (t, jt) in &mut j.against {
                jt.select = iter::repeat(true)
                    .take(g[*t].as_ref().unwrap().args().len())
                    .collect::<Vec<_>>();
            }
        }
        self.join.keys().cloned().collect()
    }

    fn forward(&self,
               u: ops::Update,
               from: flow::NodeIndex,
               ts: i64,
               _: Option<&backlog::BufferedStore>)
               -> Option<ops::Update> {

        match u {
            ops::Update::Records(rs) => {
                // okay, so here's what's going on:
                // the record(s) we receive are all from one side of the join. we need to query the
                // other side(s) for records matching the incoming records on that side's join
                // fields.

                // TODO: we should be clever here, and only query once per *distinct join value*,
                // instead of once per received record.
                Some(ops::Update::Records(rs.into_iter()
                    .flat_map(|rec| {
                        let (r, pos, lts) = rec.extract();

                        self.join((from, r, lts), ts).map(move |(res, ts)| {
                            // return new row with appropriate sign
                            if pos {
                                ops::Record::Positive(res, ts)
                            } else {
                                ops::Record::Negative(res, ts)
                            }
                        })
                    })
                    .collect()))
            }
        }
    }

    fn query(&self, q: Option<&query::Query>, ts: i64) -> ops::Datas {
        use std::iter;

        // We're essentially doing nested for loops, where each loop yields rows from one "table".
        // For the case of a two-way join (which is all that's supported for now), we call the two
        // tables `left` and `right`. We're going to iterate over results from `left` in the outer
        // loop, and query `right` inside the loop for each `left`.

        // pick some view query order
        // TODO: figure out which join order is best
        let lefti = *self.join.keys().min().unwrap();
        let left = &self.join[&lefti];

        // Set up parameters for querying all rows in left.
        //
        // We find the number of parameters by looking at how many parameters the other side of the
        // join would have used if it tried to query us.
        let mut lparams = None;

        // Avoid scanning rows that wouldn't match the query anyway. We do this by finding all
        // conditions that filter over a field present in left, and use those as parameters.
        if let Some(q) = q {
            lparams = Some(q.having
                .iter()
                .filter_map(|c| {
                    let (srci, coli) = self.emit[c.column];
                    if srci != lefti {
                        return None;
                    }

                    Some(shortcut::Condition {
                        column: coli,
                        cmp: c.cmp.clone(),
                    })
                })
                .collect::<Vec<_>>());

            if lparams.as_ref().unwrap().is_empty() {
                lparams = None;
            }
        }

        // produce a left * right given a left (basically the same as forward())
        // TODO: we probably don't need to select all columns here
        let lq = lparams.map(|ps| {
            query::Query::new(&iter::repeat(true)
                                  .take(left.node.as_ref().unwrap().args().len())
                                  .collect::<Vec<_>>(),
                              ps)
        });

        left.node
            .as_ref()
            .unwrap()
            .find(lq.as_ref(), Some(ts))
            .into_iter()
            .flat_map(move |(lrec, lts)| {
                // TODO: also add constants from q to filter used to select from right
                // TODO: respect q.select
                self.join((lefti, lrec, lts), ts)
            })
            .filter_map(move |(r, ts)| {
                if let Some(q) = q {
                    q.feed(r).map(|r| (r, ts))
                } else {
                    Some((r, ts))
                }
            })
            .collect()
    }

    fn suggest_indexes(&self, this: flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>> {
        use std::collections::HashSet;

        // index all join fields
        self.join
            .iter()
            // for every left
            .flat_map(|(left, rs)| {
                // for every right
                rs.against.iter().flat_map(move |(right, rs)| {
                    // emit both the left binding
                    rs.fields.iter().map(move |&(li, _)| (left, li))
                    // and the right binding
                    .chain(rs.fields.iter().map(move |&(_, ri)| (right, ri)))
                })
            })
            // we now have (NodeIndex, usize) for every join column.
            .fold(HashMap::new(), |mut hm, (node, col)| {
                hm.entry(*node).or_insert_with(HashSet::new).insert(col);

                // if this join column is emitted, we also want an index on that output column, as
                // it's likely the user will do lookups on it.
                if let Some(outi) = self.emit.iter().position(|&(ref n, c)| n == node && c == col) {
                    hm.entry(this).or_insert_with(HashSet::new).insert(outi);
                }
                hm
            })
            // convert HashSets into Vec
            .into_iter().map(|(node, cols)| (node, cols.into_iter().collect())).collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(flow::NodeIndex, usize)>> {
        Some(vec![self.emit[col].clone()])
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

    fn setup(left: bool) -> (ops::Node, flow::NodeIndex, flow::NodeIndex) {
        use std::sync;

        let mut g = petgraph::Graph::new();
        let mut l = ops::new("left", &["l0", "l1"], true, ops::base::Base {});
        let mut r = ops::new("right", &["r0", "r1"], true, ops::base::Base {});

        l.prime(&g);
        r.prime(&g);

        let l = g.add_node(Some(sync::Arc::new(l)));
        let r = g.add_node(Some(sync::Arc::new(r)));

        g[l].as_ref().unwrap().process((vec![1.into(), "a".into()], 0).into(), l, 0);
        g[l].as_ref().unwrap().process((vec![2.into(), "b".into()], 1).into(), l, 1);
        g[l].as_ref().unwrap().process((vec![3.into(), "c".into()], 2).into(), l, 2);
        g[r].as_ref().unwrap().process((vec![1.into(), "x".into()], 0).into(), r, 0);
        g[r].as_ref().unwrap().process((vec![1.into(), "y".into()], 1).into(), r, 1);
        g[r].as_ref().unwrap().process((vec![2.into(), "z".into()], 2).into(), r, 2);

        // join on first field
        let b = Builder::new(vec![(0.into(), 0), (0.into(), 1), (1.into(), 1)]).from(l, vec![1, 0]);
        let b = if left {
            b.left_join(r, vec![1, 0])
        } else {
            b.join(r, vec![1, 0])
        };
        let mut c: Joiner = b.into();
        c.prime(&g);
        (ops::new("join", &["j0", "j1", "j2"], false, c), l, r)
    }

    fn forward_non_weird(j: ops::Node, l: flow::NodeIndex, r: flow::NodeIndex) {
        // these are the data items we have to work with
        // these are in left
        let l_a1 = vec![1.into(), "a".into()];
        let l_b2 = vec![2.into(), "b".into()];
        // let l_c3 = vec![3.into(), "c".into()]; // considered weird
        // these are in right
        let r_x1 = vec![1.into(), "x".into()];
        let r_y1 = vec![1.into(), "y".into()];
        let r_z2 = vec![2.into(), "z".into()];

        // *************************************
        // forward from the left
        // *************************************

        // forward b2 from left; should produce [b2*z2]
        match j.process(l_b2.clone().into(), l, 100).unwrap() {
            ops::Update::Records(rs) => {
                // we're expecting to only match z2
                assert_eq!(rs,
                           vec![ops::Record::Positive(vec![2.into(), "b".into(), "z".into()], 2)]);
            }
        }

        // forward a1 from left; should produce [a1*x1, a1*y1]
        match j.process(l_a1.clone().into(), l, 100).unwrap() {
            ops::Update::Records(rs) => {
                // we're expecting two results: x1 and y1
                assert_eq!(rs.len(), 2);
                // they should all be positive since input was positive
                assert!(rs.iter().all(|r| r.is_positive()));
                // they should all have the correct values from the provided left
                assert!(rs.iter().all(|r| r.rec()[0] == 1.into() && r.rec()[1] == "a".into()));
                // and both join results should be present
                // with ts set to be the max of left and right
                assert!(rs.iter().any(|r| r.rec()[2] == "x".into() && r.ts() == 0));
                assert!(rs.iter().any(|r| r.rec()[2] == "y".into() && r.ts() == 1));
            }
        }

        // *************************************
        // forward from the right
        // *************************************

        // forward x1 from right; should produce [a1*x1]
        match j.process(r_x1.clone().into(), r, 100).unwrap() {
            ops::Update::Records(rs) => {
                assert_eq!(rs,
                           vec![ops::Record::Positive(vec![1.into(), "a".into(), "x".into()], 0)]);
            }
        }

        // forward y1 from right; should produce [a1*y1]
        match j.process(r_y1.clone().into(), r, 100).unwrap() {
            ops::Update::Records(rs) => {
                // NOTE: because we use r_y1.into(), left's timestamp will be set to 0
                assert_eq!(rs,
                           vec![ops::Record::Positive(vec![1.into(), "a".into(), "y".into()], 0)]);
            }
        }

        // forward z2 from right; should produce [b2*z2]
        match j.process(r_z2.clone().into(), r, 100).unwrap() {
            ops::Update::Records(rs) => {
                // NOTE: because we use r_z2.into(), left's timestamp will be set to 0, and thus
                // right's (b2's) timestamp will be used.
                assert_eq!(rs,
                           vec![ops::Record::Positive(vec![2.into(), "b".into(), "z".into()], 1)]);
            }
        }
    }

    #[test]
    fn it_works() {
        let (j, l, r) = setup(false);
        let l_c3 = vec![3.into(), "c".into()];

        // forward c3 from left; should produce [] since no records in right are 3
        match j.process(l_c3.clone().into(), l, 100).unwrap() {
            ops::Update::Records(rs) => {
                // right has no records with value 3
                assert_eq!(rs.len(), 0);
            }
        }

        forward_non_weird(j, l, r);
    }

    #[test]
    fn it_works_left() {
        let (j, l, r) = setup(true);

        let l_c3 = vec![3.into(), "c".into()];

        // forward c3 from left; should produce [c3 + None] since no records in right are 3
        match j.process(l_c3.clone().into(), l, 100).unwrap() {
            ops::Update::Records(rs) => {
                // right has no records with value 3, so we're expecting a single record with None
                // for all columns output from the (non-existing) right record
                assert_eq!(rs.len(), 1);
                // that row should be positive
                assert!(rs.iter().all(|r| r.is_positive()));
                // and should have the correct values from the provided left
                assert!(rs.iter().all(|r| r.rec()[0] == 3.into() && r.rec()[1] == "c".into()));
                // and None for the remaining column
                assert!(rs.iter().any(|r| r.rec()[2] == query::DataType::None));
                // and finally, the timestamp of the output should be the timestamp of the left
                // (which is 0 given that we use the Vec -> ops::Update conversion)
                assert!(rs.iter().any(|r| r.ts() == 0));
            }
        }

        forward_non_weird(j, l, r);
    }

    #[test]
    fn it_queries() {
        let (j, _, _) = setup(false);

        // do a full query, which should return product of left + right:
        // [ax, ay, bz]
        let hits = j.find(None, None);
        assert_eq!(hits.len(), 3);
        assert!(hits.iter()
            .any(|&(ref r, ts)| {
                ts == 0 && r[0] == 1.into() && r[1] == "a".into() && r[2] == "x".into()
            }));
        assert!(hits.iter()
            .any(|&(ref r, ts)| {
                ts == 1 && r[0] == 1.into() && r[1] == "a".into() && r[2] == "y".into()
            }));
        assert!(hits.iter()
            .any(|&(ref r, ts)| {
                ts == 2 && r[0] == 2.into() && r[1] == "b".into() && r[2] == "z".into()
            }));

        // query using join field
        let q = query::Query::new(&[true, true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                         }]);

        let hits = j.find(Some(&q), None);
        assert_eq!(hits.len(), 1);
        assert!(hits.iter()
            .any(|&(ref r, ts)| {
                ts == 2 && r[0] == 2.into() && r[1] == "b".into() && r[2] == "z".into()
            }));

        // query using field from left
        let q = query::Query::new(&[true, true, true],
                                  vec![shortcut::Condition {
                             column: 1,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const("a".into())),
                         }]);

        let hits = j.find(Some(&q), None);
        assert_eq!(hits.len(), 2);
        assert!(hits.iter()
            .any(|&(ref r, ts)| {
                ts == 0 && r[0] == 1.into() && r[1] == "a".into() && r[2] == "x".into()
            }));
        assert!(hits.iter()
            .any(|&(ref r, ts)| {
                ts == 1 && r[0] == 1.into() && r[1] == "a".into() && r[2] == "y".into()
            }));

        // query using field from right
        let q = query::Query::new(&[true, true, true],
                                  vec![shortcut::Condition {
                             column: 2,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const("z".into())),
                         }]);

        let hits = j.find(Some(&q), None);
        assert_eq!(hits.len(), 1);
        assert!(hits.iter()
            .any(|&(ref r, ts)| {
                ts == 2 && r[0] == 2.into() && r[1] == "b".into() && r[2] == "z".into()
            }));
    }

    #[test]
    fn it_queries_left() {
        let (j, _, _) = setup(true);

        // do a full query, which should return product of left + right:
        // [ax, ay, bz, c+None]
        let hits = j.find(None, None);
        assert_eq!(hits.len(), 4);
        assert!(hits.iter()
            .any(|&(ref r, ts)| {
                ts == 0 && r[0] == 1.into() && r[1] == "a".into() && r[2] == "x".into()
            }));
        assert!(hits.iter()
            .any(|&(ref r, ts)| {
                ts == 1 && r[0] == 1.into() && r[1] == "a".into() && r[2] == "y".into()
            }));
        assert!(hits.iter()
            .any(|&(ref r, ts)| {
                ts == 2 && r[0] == 2.into() && r[1] == "b".into() && r[2] == "z".into()
            }));
        assert!(hits.iter()
            .any(|&(ref r, ts)| {
                ts == 2 && r[0] == 3.into() && r[1] == "c".into() && r[2] == query::DataType::None
            }));
    }

    #[test]
    fn it_suggests_indices() {
        use std::collections::HashMap;
        let (j, l, r) = setup(false);
        let hm: HashMap<_, _> = vec![
            (l, vec![0]), // join column for left
            (r, vec![0]), // join column for right
            (2.into(), vec![0]), // output column that is used as join column
        ]
            .into_iter()
            .collect();
        assert_eq!(j.suggest_indexes(2.into()), hm);
    }

    #[test]
    fn it_resolves() {
        let (j, _, _) = setup(false);
        assert_eq!(j.resolve(0), Some(vec![(0.into(), 0)]));
        assert_eq!(j.resolve(1), Some(vec![(0.into(), 1)]));
        assert_eq!(j.resolve(2), Some(vec![(1.into(), 1)]));
    }
}

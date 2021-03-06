use ops;

use std::collections::HashMap;
use std::collections::HashSet;

use flow::prelude::*;

/// Latest provides an operator that will maintain the last record for every group.
///
/// Whenever a new record arrives for a group, the latest operator will negative the previous
/// latest for that group.
#[derive(Debug, Clone)]
pub struct Latest {
    us: Option<NodeAddress>,
    src: NodeAddress,
    // MUST be in reverse sorted order!
    key: Vec<usize>,
    key_m: HashMap<usize, usize>,
}

impl Latest {
    /// Construct a new latest operator.
    ///
    /// `src` should be the ancestor the operation is performed over, and `keys` should be a list
    /// of fields used to group records by. The latest record *within each group* will be
    /// maintained.
    pub fn new(src: NodeAddress, mut keys: Vec<usize>) -> Latest {
        assert_eq!(keys.len(),
                   1,
                   "only latest over a single column is supported");
        keys.sort();
        let key_m = keys.clone().into_iter().enumerate().map(|(idx, col)| (col, idx)).collect();
        keys.reverse();
        Latest {
            us: None,
            src: src,
            key: keys,
            key_m: key_m,
        }
    }
}

impl Ingredient for Latest {
    fn take(&mut self) -> Box<Ingredient> {
        Box::new(Clone::clone(self))
    }

    fn ancestors(&self) -> Vec<NodeAddress> {
        vec![self.src]
    }

    fn should_materialize(&self) -> bool {
        true
    }

    fn will_query(&self, _: bool) -> bool {
        true // because the latest may be retracted
    }

    fn on_connected(&mut self, _: &Graph) {}

    fn on_commit(&mut self, us: NodeAddress, remap: &HashMap<NodeAddress, NodeAddress>) {
        self.us = Some(us);
        self.src = remap[&self.src]
    }

    fn on_input(&mut self,
                from: NodeAddress,
                rs: Records,
                _: &DomainNodes,
                state: &StateMap)
                -> Records {
        debug_assert_eq!(from, self.src);
        // We don't allow standalone negatives as input to a latest. This is because it
        // would be very computationally expensive (and currently impossible) to find what
        // the *previous* latest was if the current latest was revoked. However, if a
        // record is negated, and a positive for the same key is given in the same group,
        // then we should just emit the new record as the new latest.
        //
        // We do this by processing in two steps. We first process all positives, emitting
        // all the -/+ pairs for each one, and keeping track of which keys we have handled.
        // Then, we assert that there are no negatives whose key does not appear in the
        // list of keys that have been handled.
        let (pos, _): (Vec<_>, _) = rs.into_iter().partition(|r| r.is_positive());
        let mut handled = HashSet::new();

        // buffer emitted records
        let mut out = Vec::with_capacity(pos.len());
        for r in pos {
            let group: Vec<_> = self.key.iter().map(|&col| r[col].clone()).collect();
            handled.insert(group);

            {
                let r = r.rec();

                // find the current value for this group
                let db = state.get(self.us.as_ref().unwrap().as_local())
                    .expect("latest must have its own state materialized");
                let rs = db.lookup(&[self.key[0]], &KeyType::Single(&r[self.key[0]]));
                debug_assert!(rs.len() <= 1, "a group had more than 1 result");
                if let Some(current) = rs.get(0) {
                    out.push(ops::Record::Negative(current.clone()));
                }
            }

            // if there was a previous latest for this key, revoke old record
            out.push(r);
        }

        // TODO: check that there aren't any standalone negatives

        out.into()
    }

    fn suggest_indexes(&self, this: NodeAddress) -> HashMap<NodeAddress, Vec<usize>> {
        // index all key columns
        Some((this, self.key.clone())).into_iter().collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeAddress, usize)>> {
        Some(vec![(self.src, col)])
    }

    fn description(&self) -> String {
        let key_cols = self.key
            .iter()
            .map(|k| k.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        format!("⧖ γ[{}]", key_cols)
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeAddress, Option<usize>)> {
        vec![(self.src, Some(column))]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup(key: usize, mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);
        g.set_op("latest", &["x", "y"], Latest::new(s, vec![key]), mat);
        g
    }

    // TODO: test when last *isn't* latest!

    #[test]
    fn it_describes() {
        let c = setup(0, false);
        assert_eq!(c.node().description(), "⧖ γ[0]");
    }

    #[test]
    fn it_forwards() {
        let mut c = setup(0, true);

        let u = vec![1.into(), 1.into()];

        // first record for a group should emit just a positive
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            ops::Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 1.into());
            }
            _ => unreachable!(),
        }

        let u = vec![2.into(), 2.into()];

        // first record for a second group should also emit just a positive
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            ops::Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], 2.into());
            }
            _ => unreachable!(),
        }

        let u = vec![1.into(), 2.into()];

        // new record for existing group should revoke the old latest, and emit the new
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            ops::Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 1.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            ops::Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 2.into());
            }
            _ => unreachable!(),
        }

        let u = vec![(vec![1.into(), 1.into()], false),
                     (vec![1.into(), 2.into()], false),
                     (vec![1.into(), 3.into()], true),
                     (vec![2.into(), 2.into()], false),
                     (vec![2.into(), 4.into()], true)];

        // negatives and positives should still result in only one new current for each group
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 4); // one - and one + for each group
        // group 1 lost 2 and gained 3
        assert!(rs.iter().any(|r| if let ops::Record::Negative(ref r) = *r {
            r[0] == 1.into() && r[1] == 2.into()
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let ops::Record::Positive(ref r) = *r {
            r[0] == 1.into() && r[1] == 3.into()
        } else {
            false
        }));
        // group 2 lost 2 and gained 4
        assert!(rs.iter().any(|r| if let ops::Record::Negative(ref r) = *r {
            r[0] == 2.into() && r[1] == 2.into()
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let ops::Record::Positive(ref r) = *r {
            r[0] == 2.into() && r[1] == 4.into()
        } else {
            false
        }));
    }

    #[test]
    fn it_suggests_indices() {
        let me = NodeAddress::mock_global(1.into());
        let c = setup(1, false);
        let idx = c.node().suggest_indexes(me);

        // should only add index on own columns
        assert_eq!(idx.len(), 1);
        assert!(idx.contains_key(&me));

        // should only index on the group-by column
        assert_eq!(idx[&me], vec![1]);
    }


    #[test]
    fn it_resolves() {
        let c = setup(1, false);
        assert_eq!(c.node().resolve(0), Some(vec![(c.narrow_base_id(), 0)]));
        assert_eq!(c.node().resolve(1), Some(vec![(c.narrow_base_id(), 1)]));
        assert_eq!(c.node().resolve(2), Some(vec![(c.narrow_base_id(), 2)]));
    }
}

//! Functions for identifying which nodes should be materialized, and what indices should be used
//! for those materializations.
//!
//! This module also holds the logic for *identifying* state that must be transfered from other
//! domains, but does not perform that copying itself (that is the role of the `augmentation`
//! module).

use flow;
use flow::domain;
use flow::prelude::*;

use petgraph;
use petgraph::graph::NodeIndex;

use std::collections::{HashSet, HashMap};
use std::sync::mpsc;

use slog::Logger;

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }}
}

use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
static TAG_GENERATOR: AtomicUsize = ATOMIC_USIZE_INIT;

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Tag(u32);

impl Tag {
    pub fn id(&self) -> u32 {
        self.0
    }
}

pub fn pick(log: &Logger, graph: &Graph, nodes: &[(NodeIndex, bool)]) -> HashSet<LocalNodeIndex> {
    let nodes: Vec<_> = nodes.iter()
        .map(|&(ni, new)| (ni, &graph[ni], new))
        .collect();

    let mut materialize: HashSet<_> = nodes.iter()
        .filter_map(|&(ni, n, _)| {
            // materialized state for any nodes that need it
            // in particular, we keep state for
            //
            //  - any internal node that requires its own state to be materialized
            //  - any internal node that has an outgoing edge marked as materialized (we know
            //    that that edge has to be internal, since ingress/egress nodes have already
            //    been added, and they make sure that there are no cross-domain materialized
            //    edges).
            //  - any ingress node with children that say that they may query their ancestors
            //
            // that last point needs to be checked *after* we have determined if all internal
            // nodes should be materialized
            match **n {
                flow::node::Type::Internal(ref i) => {
                    if i.should_materialize() ||
                       graph.edges_directed(ni, petgraph::EdgeDirection::Outgoing)
                        .any(|e| *e.weight()) {
                        trace!(log, "should materialize"; "node" => format!("{}", ni.index()));
                        Some(*n.addr().as_local())
                    } else {
                        trace!(log, "not materializing"; "node" => format!("{}", ni.index()));
                        None
                    }
                }
                _ => None,
            }
        })
        .collect();

    let mut inquisitive_children = HashSet::new();
    {
        let mark_parent_inquisitive_or_materialize =
            |ni: NodeIndex,
             materialize: &mut HashSet<flow::LocalNodeIndex>,
             inquisitive_children: &mut HashSet<NodeIndex>|
             -> Option<NodeIndex> {
                let n = &graph[ni];
                if let flow::node::Type::Internal(ref nn) = **n {
                    if !materialize.contains(n.addr().as_local()) {
                        if nn.can_query_through() {
                            trace!(log, "parent can be queried through, mark it as querying";
                                   "node" => format!("{}", ni.index()));
                            inquisitive_children.insert(ni);
                            // continue backtracking
                            return Some(ni);
                        } else {
                            // we can't query through this internal node, so materialize it
                            trace!(log, "parent can't be queried through, so materialize it";
                                   "node" => format!("{}", ni.index()));
                            materialize.insert(*n.addr().as_local());
                        }
                    }
                }
                None
            };
        for &(ni, n, _) in nodes.iter() {
            if let flow::node::Type::Internal(..) = **n {
                if n.will_query(materialize.contains(n.addr().as_local())) {
                    trace!(log, "found querying child"; "node" => format!("{}", ni.index()));
                    inquisitive_children.insert(ni);
                    // track child back to an ingress, marking any unmaterialized nodes on the path as
                    // inquisitive as long as we can query through them
                    let mut q = vec![ni];
                    while !q.is_empty() {
                        let ni = q.pop().unwrap();
                        for ni in graph.neighbors_directed(ni, petgraph::EdgeDirection::Incoming) {
                            let next =
                                mark_parent_inquisitive_or_materialize(ni,
                                                                       &mut materialize,
                                                                       &mut inquisitive_children);
                            match next {
                                Some(next_ni) => q.push(next_ni),
                                None => continue,
                            }
                        }
                    }
                }
            }
        }
    }

    for &(ni, n, _) in &nodes {
        if let flow::node::Type::Ingress = **n {
            if graph.neighbors_directed(ni, petgraph::EdgeDirection::Outgoing)
                .any(|child| inquisitive_children.contains(&child)) {
                // we have children that may query us, so our output should be materialized
                trace!(log,
                       format!("querying children force materialization of node {}",
                               ni.index()));
                materialize.insert(*n.addr().as_local());
            }
        }
    }

    // find all nodes that can be queried through, and where any of its outgoing edges are
    // materialized. for those nodes, we should instead materialize the input to that node.
    for &(ni, n, _) in &nodes {
        if let flow::node::Type::Internal(..) = **n {
            if !n.can_query_through() {
                continue;
            }

            if !materialize.contains(n.addr().as_local()) {
                // we're not materialized, so no materialization shifting necessary
                continue;
            }

            if graph.edges_directed(ni, petgraph::EdgeDirection::Outgoing)
                .any(|e| *e.weight()) {
                // our output is materialized! what a waste. instead, materialize our input.
                materialize.remove(n.addr().as_local());
                trace!(log, "hoisting materialization"; "past" => ni.index());

                // TODO: unclear if we need *all* our parents to be materialized. it's
                // certainly the case for filter, which is our only use-case for now...
                for p in graph.neighbors_directed(ni, petgraph::EdgeDirection::Incoming) {
                    materialize.insert(*graph[p].addr().as_local());
                }
            }
        }
    }

    materialize
}

pub fn index(log: &Logger,
             graph: &Graph,
             nodes: &[(NodeIndex, bool)],
             materialize: HashSet<LocalNodeIndex>)
             -> HashMap<LocalNodeIndex, Vec<Vec<usize>>> {

    let map: HashMap<_, _> =
        nodes.iter().map(|&(ni, _)| (*graph[ni].addr().as_local(), ni)).collect();
    let nodes: Vec<_> = nodes.iter()
        .map(|&(ni, new)| (&graph[ni], new))
        .collect();

    let mut state: HashMap<_, Option<Vec<Vec<usize>>>> =
        materialize.into_iter().map(|n| (n, None)).collect();

    // Now let's talk indices.
    //
    // We need to query all our nodes for what indices they believe should be maintained, and
    // apply those to the stores in state. However, this is somewhat complicated by the fact
    // that we need to push indices through non-materialized views so that they end up on the
    // columns of the views that will actually query into a table of some sort.
    {
        let nodes: HashMap<_, _> = nodes.iter().map(|&(n, _)| (n.addr(), n)).collect();
        let mut indices = nodes.iter()
                .filter(|&(_, node)| node.is_internal()) // only internal nodes can suggest indices
                .filter(|&(_, node)| {
                    // under what circumstances might a node need indices to be placed?
                    // there are two cases:
                    //
                    //  - if makes queries into its ancestors regardless of whether it's
                    //    materialized or not
                    //  - if it queries its ancestors when it is *not* materialized (implying that
                    //    it queries into its own output)
                    //
                    //  unless we come up with a weird operator that *doesn't* need indices when
                    //  it is *not* materialized, but *does* when is, we can therefore just use
                    //  will_query(false) as an indicator of whether indices are necessary.
                    node.will_query(false)
                })
                .flat_map(|(ni, node)| node.suggest_indexes(*ni).into_iter())
                .filter(|&(ref node, _)| nodes.contains_key(node))
                .fold(HashMap::new(), |mut hm, (v, idx)| {
                    hm.entry(v).or_insert_with(HashSet::new).insert(idx);
                    hm
                });

        // push down indices
        let mut leftover_indices: HashMap<_, _> = indices.drain().collect();
        let mut tmp = HashMap::new();
        while !leftover_indices.is_empty() {
            for (v, idxs) in leftover_indices.drain() {
                if let Some(mut state) = state.get_mut(v.as_local()) {
                    // this node is materialized! add the indices!
                    info!(log, "adding indices"; "node" => map[v.as_local()].index(), "cols" => format!("{:?}", idxs));
                    *state = Some(idxs.into_iter().collect());
                } else if let Some(node) = nodes.get(&v) {
                    // this node is not materialized
                    // we need to push the index up to its ancestor(s)
                    if let flow::node::Type::Ingress = ***node {
                        // we can't push further up!
                        unreachable!("node suggested index outside domain, and ingress isn't \
                                      materalized");
                    }

                    assert!(node.is_internal());
                    // TODO: push indices up through views (do we even need this)?
                    // for idx in idxs {
                    //     let really = node.resolve(col);
                    //     if let Some(really) = really {
                    //         // the index should instead be placed on the corresponding
                    //         // columns of this view's inputs
                    //         for (v, col) in really {
                    //             trace!(log, "pushing up index into column {} of {}", col, v);
                    //             tmp.entry(v).or_insert_with(HashSet::new).insert(col);
                    //         }
                    //     } else {
                    //         // this view is materialized, so we should index this column
                    //         indices.entry(v).or_insert_with(HashSet::new).insert(col);
                    //     }
                    // }
                } else {
                    unreachable!("node suggested index outside domain");
                }
            }
            leftover_indices.extend(tmp.drain());
        }
    }

    state.into_iter()
        .filter_map(|(n, col)| {
            if let Some(col) = col {
                Some((n, col))
            } else {
                // this materialization doesn't have any primary key,
                // so we assume it's not in use.

                let ref node = graph[map[&n]];
                if node.is_internal() && node.is_base() {
                    // but it's a base nodes!
                    // we must *always* materialize base nodes
                    // so, just make up some column to index on
                    return Some((n, vec![vec![0]]));
                }

                info!(log, "removing unnecessary materialization"; "node" => map[&n].index());
                None
            }
        })
        .collect()
}

pub fn initialize(log: &Logger,
                  graph: &Graph,
                  source: NodeIndex,
                  new: &HashSet<NodeIndex>,
                  mut materialize: HashMap<domain::Index,
                                           HashMap<LocalNodeIndex, Vec<Vec<usize>>>>,
                  txs: &mut HashMap<domain::Index, mpsc::SyncSender<Packet>>) {
    let mut topo_list = Vec::with_capacity(new.len());
    let mut topo = petgraph::visit::Topo::new(&*graph);
    while let Some(node) = topo.next(&*graph) {
        if node == source {
            continue;
        }
        if !new.contains(&node) {
            continue;
        }
        topo_list.push(node);
    }

    // TODO: what about adding materialization to *existing* views?

    let mut empty = HashSet::new();
    for node in topo_list {
        let n = &graph[node];
        let d = n.domain();

        let index_on = materialize.get_mut(&d)
            .and_then(|ss| ss.get(n.addr().as_local()))
            .cloned()
            .map(|idxs| {
                // we've been told to materialize a node using 0 indices
                assert!(!idxs.is_empty());
                idxs
            })
            .unwrap_or_else(Vec::new);
        let mut has_state = !index_on.is_empty();

        if let flow::node::Type::Reader(_, ref r) = **n {
            if r.state.is_some() {
                has_state = true;
            }
        }

        // ready communicates to the domain in charge of a particular node that it should start
        // delivering updates to a given new node. note that we wait for the domain to acknowledge
        // the change. this is important so that we don't ready a child in a different domain
        // before the parent has been readied. it's also important to avoid us returning before the
        // graph is actually fully operational.
        let ready = |txs: &mut HashMap<_, mpsc::SyncSender<_>>, index_on: Vec<Vec<usize>>| {
            let (ack_tx, ack_rx) = mpsc::sync_channel(0);
            trace!(log, "readying node"; "node" => node.index());
            txs[&d]
                .send(Packet::Ready {
                    node: *n.addr().as_local(),
                    index: index_on,
                    ack: ack_tx,
                })
                .unwrap();
            match ack_rx.recv() {
                Err(mpsc::RecvError) => (),
                _ => unreachable!(),
            }
            trace!(log, "node ready"; "node" => node.index());
        };

        if graph.neighbors_directed(node, petgraph::EdgeDirection::Incoming)
            .filter(|&ni| ni != source)
            .all(|n| empty.contains(&n)) {
            // all parents are empty, so we can materialize it immediately
            trace!(log, "no need to replay empty view"; "node" => node.index());
            empty.insert(node);
            ready(txs, index_on);
        } else {
            // if this node doesn't need to be materialized, then we're done. note that this check
            // needs to happen *after* the empty parents check so that we keep tracking whether or
            // not nodes are empty.
            if !has_state {
                trace!(log, "no need to replay non-materialized view"; "node" => node.index());
                ready(txs, index_on);
                continue;
            }

            // we have a parent that has data, so we need to replay and reconstruct
            let start = ::std::time::Instant::now();
            let log = log.new(o!("node" => node.index()));
            info!(log, "beginning reconstruction of {:?}", *graph[node]);
            reconstruct(&log,
                        graph,
                        source,
                        &empty,
                        &materialize,
                        txs,
                        node,
                        index_on);
            debug!(log, "reconstruction started");
            // NOTE: the state has already been marked ready by the replay completing,
            // but we want to wait for the domain to finish replay, which a Ready does.
            ready(txs, vec![]);
            info!(log, "reconstruction completed"; "ms" => dur_to_ns!(start.elapsed()) / 1_000_000);
        }
    }
}

pub fn reconstruct(log: &Logger,
                   graph: &Graph,
                   source: NodeIndex,
                   empty: &HashSet<NodeIndex>,
                   materialized: &HashMap<domain::Index,
                                          HashMap<LocalNodeIndex, Vec<Vec<usize>>>>,
                   txs: &mut HashMap<domain::Index, mpsc::SyncSender<Packet>>,
                   node: NodeIndex,
                   index_on: Vec<Vec<usize>>) {

    // okay, so here's the situation: `node` is a node that
    //
    //   a) was not previously materialized, and
    //   b) now needs to be materialized, and
    //   c) at least one of node's parents has existing data
    //
    // because of the topological traversal done by `initialize`, we know that all our ancestors
    // that should be materialized have been.
    //
    // our plan is as follows:
    //
    //   1. search our ancestors for the closest materialization points along each path
    //   2. for each such path, identify the domains along that path and pause them
    //   3. construct a daisy-chain of channels, and pass them to each domain along the path
    //   4. tell the domain nearest to the root to start replaying
    //
    // so, first things first, let's find our closest materialized parents
    let paths = trace(graph, source, node, empty, materialized, vec![node]);

    if let flow::node::Type::Reader(..) = *graph[node] {
        // readers have their own internal state
    } else {
        assert!(!index_on.is_empty(),
                "all non-reader nodes must have a state key");

        // tell the domain in question to create an empty state for the node in question
        txs[&graph[node].domain()]
            .send(Packet::PrepareState {
                node: *graph[node].addr().as_local(),
                index: index_on,
            })
            .unwrap();
    }

    // TODO:
    // technically, we can be a bit smarter here. for example, a union with a 1-1 projection does
    // not need to be replayed through if it is not materialized. neither does an ingress node.
    // unfortunately, skipping things this way would make `Message::to` and `Message::from` contain
    // weird values, and cause breakage.

    // set up channels for replay along each path
    for mut path in paths {
        // we want path to have the ancestor closest to the root *first*
        path.reverse();

        let tag = Tag(TAG_GENERATOR.fetch_add(1, Ordering::SeqCst) as u32);
        trace!(log, "tag" => tag.id(); "replaying along path {:?}", path);

        // first, find out which domains we are crossing
        let mut segments = Vec::new();
        let mut last_domain = None;
        for node in path {
            let domain = graph[node].domain();
            if last_domain.is_none() || domain != last_domain.unwrap() {
                segments.push((domain, Vec::new()));
                last_domain = Some(domain);
            }

            segments.last_mut().unwrap().1.push(node);
        }

        debug!(log, "domain replay path is {:?}", segments);

        let locals = |i: usize| -> Vec<NodeAddress> {
            if i == 0 {
                // we're not replaying through the starter node
                segments[i]
                    .1
                    .iter()
                    .skip(1)
                    .map(|&ni| graph[ni].addr())
                    .collect::<Vec<_>>()
            } else {
                segments[i]
                    .1
                    .iter()
                    .map(|&ni| graph[ni].addr())
                    .collect::<Vec<_>>()
            }
        };

        let (wait_tx, wait_rx) = mpsc::sync_channel(segments.len());
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let mut main_done_tx = Some(done_tx);

        // first, tell all the domains about the replay path
        let mut seen = HashSet::new();
        for (i, &(ref domain, ref nodes)) in segments.iter().enumerate() {
            // TODO:
            //  a domain may appear multiple times in this list if a path crosses into the same
            //  domain more than once. currently, that will cause a deadlock.
            assert!(!seen.contains(domain),
                    "a-b-a domain replays are not yet supported");
            seen.insert(*domain);

            let locals = locals(i);
            if locals.is_empty() {
                // first domain may *only* have the starter state
                assert_eq!(i, 0);
                continue;
            }

            let mut setup = Packet::SetupReplayPath {
                tag: tag,
                path: locals,
                done_tx: None,
                ack: wait_tx.clone(),
            };
            if i == segments.len() - 1 {
                // last domain should report when it's done
                assert!(main_done_tx.is_some());
                if let Packet::SetupReplayPath { ref mut done_tx, .. } = setup {
                    *done_tx = main_done_tx.take();
                }
            } else {
                // the last node *must* be an egress node since there's a later domain
                if let flow::node::Type::Egress { ref tags, .. } = *graph[*nodes.last().unwrap()] {
                    let mut tags = tags.lock().unwrap();
                    tags.insert(tag, segments[i + 1].1[0].into());
                } else {
                    unreachable!();
                }
            }

            trace!(log, "telling domain about replay path"; "domain" => domain.index());
            txs[domain].send(setup).unwrap();
        }

        // wait for them all to have seen that message
        for _ in &segments {
            wait_rx.recv().unwrap();
        }
        trace!(log, "all domains ready for replay");

        // next, tell the first domain to start playing
        trace!(log, "telling root domain to start replay"; "domain" => segments[0].0.index());
        txs[&segments[0].0]
            .send(Packet::StartReplay {
                tag: tag,
                from: graph[segments[0].1[0]].addr(),
                ack: wait_tx.clone(),
            })
            .unwrap();

        // and finally, wait for the last domain to finish the replay
        trace!(log, "waiting for done message from target"; "domain" => segments.last().unwrap().0.index());
        done_rx.recv().unwrap();
    }
}

fn trace<T>(graph: &Graph,
            source: NodeIndex,
            node: NodeIndex,
            empty: &HashSet<NodeIndex>,
            materialized: &HashMap<domain::Index, HashMap<LocalNodeIndex, T>>,
            path: Vec<NodeIndex>)
            -> Vec<Vec<NodeIndex>> {

    if node == source {
        unreachable!("base node was not materialized!");
    }

    let n = &graph[node];
    let is_materialized = if path.len() == 1 {
        // the start node is the one we're trying to replay to, so while it'll be marked as
        // materialized in the map, it isn't really
        false
    } else {
        materialized.get(&n.domain())
            .map(|dm| dm.contains_key(n.addr().as_local()))
            .unwrap_or(false)
    };

    if is_materialized {
        vec![path]
    } else {
        let mut parents: Vec<_> = graph.neighbors_directed(node, petgraph::EdgeDirection::Incoming)
            .collect();
        if parents.len() != 1 {
            // there are two cases where we have multiple parents: joins and unions
            // for unions, we should replay *all* paths. for joins, we should only replay one path.
            // in particular, for a join, we should only replay the ancestor that yields the full
            // result-set (i.e., the left side of a left join).
            assert!(n.is_internal());
            // find empty parents
            let empty: HashSet<_> = parents.iter()
                .filter(|ni| empty.contains(ni))
                .map(|ni| graph[*ni].addr())
                .collect();
            if let Some(picked_ancestor) = n.replay_ancestor(&empty) {
                // join, only replay picked ancestor
                // NOTE: this is a *non-deterministic* choice
                parents.retain(|&parent| graph[parent].addr() == picked_ancestor);
            } else {
                // union; just replay all
            }
        }

        // there's no point in replaying parents that are empty
        parents.retain(|&parent| !empty.contains(&parent));

        parents.into_iter()
            .flat_map(|parent| {
                let mut path = path.clone();
                path.push(parent);
                trace(graph, source, parent, empty, materialized, path)
            })
            .collect()
    }
}

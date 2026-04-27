use crate::block::Block;
use std::collections::HashSet;

/// Tracks which blocks are ready to schedule by maintaining a "surface" of
/// completed blocks and a "boundary" of failed blocks.
///
/// A block is SURFACE if it completed successfully and has downstream
/// dependents that haven't been scheduled yet. A block is BOUNDARY if it
/// failed and has upstream dependents in the surface.
///
/// Only surface + boundary blocks are stored, keeping memory usage
/// proportional to the active frontier rather than the total block count.
pub struct ReadySurface<F, G>
where
    F: Fn(&Block) -> Vec<Block>,
    G: Fn(&Block) -> Vec<Block>,
{
    downstream: F,
    upstream: G,
    surface: HashSet<Block>,
    boundary: HashSet<Block>,
}

impl<F, G> ReadySurface<F, G>
where
    F: Fn(&Block) -> Vec<Block>,
    G: Fn(&Block) -> Vec<Block>,
{
    pub fn new(downstream: F, upstream: G) -> Self {
        Self {
            downstream,
            upstream,
            surface: HashSet::new(),
            boundary: HashSet::new(),
        }
    }

    /// Mark a block as successfully completed. Returns the list of downstream
    /// blocks that are now ready to be scheduled (all their upstream deps are
    /// in the surface).
    pub fn mark_success(&mut self, node: &Block) -> Vec<Block> {
        let up_nodes: Vec<Block> = (self.upstream)(node);

        debug_assert!(
            up_nodes.iter().all(|up| self.surface.contains(up)),
            "not all upstream deps of {node} are in the surface"
        );

        self.surface.insert(node.clone());
        let mut new_ready = Vec::new();

        // Check if any downstream blocks become ready or enter the boundary.
        for down_node in (self.downstream)(node) {
            if !self.add_to_boundary(&down_node) {
                let down_ups = (self.upstream)(&down_node);
                if down_ups.iter().all(|up| self.surface.contains(up)) {
                    new_ready.push(down_node);
                }
            }
        }

        // Check if upstream nodes can leave the surface (all their downstream
        // are now either surface or boundary — no longer OTHER).
        for up_node in &up_nodes {
            let down_nodes = (self.downstream)(up_node);
            let contained: Vec<i8> = down_nodes
                .iter()
                .map(|dn| {
                    if self.surface.contains(dn) {
                        1
                    } else if self.boundary.contains(dn) {
                        -1
                    } else {
                        0
                    }
                })
                .collect();

            if contained.iter().all(|&x| x.abs() > 0) {
                self.surface.remove(up_node);
                for (dn, &c) in down_nodes.iter().zip(contained.iter()) {
                    if c < 0 {
                        self.remove_from_boundary(dn);
                    }
                }
            }
        }

        // Leaf nodes: no downstream → leave surface immediately.
        if (self.downstream)(node).is_empty() {
            self.surface.remove(node);
        }

        new_ready
    }

    /// Mark a block as permanently failed. Propagates failure to all
    /// reachable downstream blocks via BFS. Returns the set of orphaned
    /// blocks.
    pub fn mark_failure(&mut self, node: &Block, _count_all_orphans: bool) -> HashSet<Block> {
        let up_nodes: Vec<Block> = (self.upstream)(node);

        debug_assert!(
            up_nodes.iter().all(|up| self.surface.contains(up)),
            "not all upstream deps of {node} are in the surface"
        );

        self.boundary.insert(node.clone());

        // BFS: propagate failure downstream.
        let mut to_visit: Vec<Block> = (self.downstream)(node).into_iter().collect();
        let mut orphans = HashSet::new();

        while let Some(down_node) = to_visit.pop() {
            if self.add_to_boundary(&down_node) {
                for further in (self.downstream)(&down_node) {
                    if !orphans.contains(&further) {
                        to_visit.push(further);
                    }
                }
                orphans.insert(down_node);
            }
        }

        // Clean up upstream surface nodes that have no OTHER downstream.
        for up_node in &up_nodes {
            let down_nodes = (self.downstream)(up_node);
            let contained: Vec<i8> = down_nodes
                .iter()
                .map(|dn| {
                    if self.surface.contains(dn) {
                        1
                    } else if self.boundary.contains(dn) {
                        -1
                    } else {
                        0
                    }
                })
                .collect();

            if contained.iter().all(|&x| x.abs() > 0) {
                self.surface.remove(up_node);
                for (dn, &c) in down_nodes.iter().zip(contained.iter()) {
                    if c < 0 {
                        self.remove_from_boundary(dn);
                    }
                }
            }
        }

        // Root failure: no upstream → leave boundary.
        if (self.upstream)(node).is_empty() {
            self.boundary.remove(node);
        }

        orphans
    }

    fn add_to_boundary(&mut self, node: &Block) -> bool {
        if self.boundary.contains(node) {
            return false;
        }
        let up_nodes = (self.upstream)(node);
        if up_nodes.iter().any(|up| self.boundary.contains(up)) {
            self.boundary.insert(node.clone());
            true
        } else {
            false
        }
    }

    fn remove_from_boundary(&mut self, node: &Block) -> bool {
        if !self.boundary.contains(node) {
            return false;
        }
        let up_nodes = (self.upstream)(node);
        if up_nodes.iter().all(|up| !self.surface.contains(up)) {
            self.boundary.remove(node);
            true
        } else {
            false
        }
    }
}

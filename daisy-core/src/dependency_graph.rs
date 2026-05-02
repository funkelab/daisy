use crate::block::Block;
use crate::coordinate::Coordinate;
use crate::roi::Roi;
use crate::task::{Fit, Task};
use petgraph::Direction;
use petgraph::graph::{DiGraph, NodeIndex};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::debug;

/// Dependency graph for a single task's blocks. Computes independent "levels"
/// of blocks where all blocks within a level can run in parallel without
/// read/write conflicts, and blocks in level N+1 depend on blocks in level N.
pub struct BlockwiseDependencyGraph {
    task_id: String,
    block_read_roi: Roi,
    block_write_roi: Roi,
    read_write_context: (Coordinate, Coordinate),
    total_read_roi: Roi,
    total_write_roi: Roi,
    read_write_conflict: bool,
    fit: Fit,
    rounding_term: Coordinate,
    level_stride: Coordinate,
    level_offsets: Vec<Coordinate>,
    level_conflicts: Vec<Vec<Coordinate>>,
}

impl BlockwiseDependencyGraph {
    pub fn new(
        task_id: &str,
        block_read_roi: Roi,
        block_write_roi: Roi,
        read_write_conflict: bool,
        fit: Fit,
        total_read_roi: Option<Roi>,
        total_write_roi: Option<Roi>,
    ) -> Self {
        let read_write_context = (
            block_write_roi.begin() - block_read_roi.begin(),
            block_read_roi.end() - block_write_roi.end(),
        );

        let (total_read_roi, total_write_roi) = match (total_read_roi, total_write_roi) {
            (Some(tr), Some(tw)) => {
                let tc = (
                    tw.begin() - tr.begin(),
                    tr.end() - tw.end(),
                );
                assert_eq!(
                    tc, read_write_context,
                    "total ROI context mismatch with block context"
                );
                (tr, tw)
            }
            (Some(tr), None) => {
                let tw = tr.grow(&-&read_write_context.0, &-&read_write_context.1);
                (tr, tw)
            }
            (None, Some(tw)) => {
                let tr = tw.grow(&read_write_context.0, &read_write_context.1);
                (tr, tw)
            }
            (None, None) => panic!("either total_read_roi or total_write_roi must be provided"),
        };

        let dims = block_write_roi.dims();
        let rounding_term = match fit {
            Fit::Overhang | Fit::Shrink => Coordinate::new(vec![1; dims]),
            Fit::Valid => block_write_roi.shape().clone(),
        };

        let mut graph = Self {
            task_id: task_id.to_string(),
            block_read_roi,
            block_write_roi,
            read_write_context,
            total_read_roi,
            total_write_roi,
            read_write_conflict,
            fit,
            rounding_term,
            // placeholders, computed below
            level_stride: Coordinate::zeros(dims),
            level_offsets: Vec::new(),
            level_conflicts: Vec::new(),
        };

        graph.level_stride = graph.compute_level_stride();
        graph.level_offsets = graph.compute_level_offsets();
        graph.level_conflicts = graph.compute_level_conflicts();
        graph
    }

    pub fn from_task(task: &Task) -> Self {
        Self::new(
            &task.task_id,
            task.read_roi.clone(),
            task.write_roi.clone(),
            task.read_write_conflict,
            task.fit.clone(),
            Some(task.total_roi.clone()),
            None,
        )
    }

    pub fn num_levels(&self) -> usize {
        self.level_offsets.len()
    }

    pub fn num_blocks(&self) -> i64 {
        (0..self.num_levels())
            .map(|level| self.num_level_blocks(level))
            .sum()
    }

    pub fn num_roots(&self) -> i64 {
        self.num_level_blocks(0)
    }

    fn num_level_blocks(&self, level: usize) -> i64 {
        let level_offset = &self.level_offsets[level];
        let mut count: i64 = 1;
        for i in 0..level_offset.dims() {
            let blocks_in_dim =
                (self.total_write_roi.shape()[i] - level_offset[i] + self.level_stride[i]
                    - self.rounding_term[i])
                    / self.level_stride[i];
            count *= blocks_in_dim.max(0);
        }
        debug!(
            level,
            count,
            ?level_offset,
            "computed blocks for level"
        );
        count
    }

    /// Lazily iterate blocks for a given level.
    pub fn level_blocks(&self, level: usize) -> impl Iterator<Item = Block> + '_ {
        self.compute_level_block_offsets(level)
            .filter_map(move |block_offset| {
                let block = Block::new(
                    &self.total_read_roi,
                    &self.block_read_roi + &block_offset,
                    &self.block_write_roi + &block_offset,
                    &self.task_id,
                );
                if self.inclusion_criteria(&block) {
                    Some(self.fit_block(block))
                } else {
                    None
                }
            })
    }

    /// Root generator: produces all blocks in level 0.
    pub fn root_gen(&self) -> impl Iterator<Item = Block> + '_ {
        self.level_blocks(0)
    }

    /// Get blocks directly downstream of the given block (in the next level).
    pub fn downstream(&self, block: &Block) -> Vec<Block> {
        let level = self.level(block);
        let next_level = level + 1;
        if next_level >= self.num_levels() {
            return Vec::new();
        }

        let mut conflicts = Vec::new();
        let mut seen = HashSet::new();
        for conflict_offset in &self.level_conflicts[next_level] {
            let conflict_block = Block::new(
                &self.total_read_roi,
                Roi::new(
                    block.read_roi.offset() - conflict_offset,
                    self.block_read_roi.shape().clone(),
                ),
                Roi::new(
                    block.write_roi.offset() - conflict_offset,
                    self.block_write_roi.shape().clone(),
                ),
                &self.task_id,
            );
            if self.inclusion_criteria(&conflict_block) {
                let fitted = self.fit_block(conflict_block);
                if seen.insert(fitted.block_id.clone()) {
                    conflicts.push(fitted);
                }
            }
        }
        conflicts
    }

    /// Get blocks directly upstream of the given block (in the same level's
    /// conflict set).
    pub fn upstream(&self, block: &Block) -> Vec<Block> {
        let level = self.level(block);

        let mut conflicts = Vec::new();
        let mut seen = HashSet::new();
        for conflict_offset in &self.level_conflicts[level] {
            let conflict_block = Block::new(
                &self.total_read_roi,
                Roi::new(
                    block.read_roi.offset() + conflict_offset,
                    self.block_read_roi.shape().clone(),
                ),
                Roi::new(
                    block.write_roi.offset() + conflict_offset,
                    self.block_write_roi.shape().clone(),
                ),
                &self.task_id,
            );
            if self.inclusion_criteria(&conflict_block) {
                let fitted = self.fit_block(conflict_block);
                if seen.insert(fitted.block_id.clone()) {
                    conflicts.push(fitted);
                }
            }
        }
        conflicts
    }

    /// Return blocks whose write ROIs cover `sub_roi`. When `read_roi` is
    /// true, return blocks whose read ROIs overlap with `sub_roi` instead.
    pub fn get_subgraph_blocks(&self, sub_roi: &Roi, read_roi: bool) -> Vec<Block> {
        let sub_roi = if read_roi {
            sub_roi.grow(&self.read_write_context.0, &self.read_write_context.1)
        } else {
            sub_roi.clone()
        };

        let sub_roi = sub_roi.intersect(&self.total_write_roi);

        // Get sub_roi relative to the write ROI origin.
        let begin = sub_roi.begin() - self.total_write_roi.offset();
        let end = sub_roi.end() - self.total_write_roi.offset();

        // Convert to block coordinates: floor for begin, ceil for end.
        let write_shape = self.block_write_roi.shape();
        let aligned_begin = &begin / write_shape;
        let aligned_end = &(-&(&-&end / write_shape)); // ceil division

        // Generate relative offsets of relevant write blocks.
        let ranges: Vec<Vec<i64>> = (0..write_shape.dims())
            .map(|d| {
                let lo = aligned_begin[d] * write_shape[d];
                let hi = aligned_end[d] * write_shape[d];
                let step = write_shape[d];
                (lo..hi).step_by(step as usize).collect()
            })
            .collect();

        let mut blocks = Vec::new();
        for offsets in cartesian_product(&ranges) {
            let offset =
                Coordinate::new(offsets) + self.total_read_roi.offset();
            let read_roi_offset = &offset - self.block_read_roi.offset();
            let block = Block::new(
                &self.total_read_roi,
                &self.block_read_roi + &read_roi_offset,
                &self.block_write_roi + &read_roi_offset,
                &self.task_id,
            );
            if self.inclusion_criteria(&block) {
                blocks.push(self.fit_block(block));
            }
        }
        blocks
    }

    // --- Internal computation methods ---

    fn level(&self, block: &Block) -> usize {
        let block_offset = block.read_roi.offset() - self.total_read_roi.offset();
        let level_offset = &block_offset % &self.level_stride;
        for (i, offset) in self.level_offsets.iter().enumerate() {
            if &level_offset == offset {
                return i;
            }
        }
        panic!(
            "block offset {level_offset} not found in level offsets {:?}",
            self.level_offsets
        );
    }

    fn compute_level_stride(&self) -> Coordinate {
        if !self.read_write_conflict {
            return self.block_write_roi.shape().clone();
        }

        let context_ul = self.block_write_roi.begin() - self.block_read_roi.begin();
        let context_lr = self.block_read_roi.end() - self.block_write_roi.end();
        let max_context = context_ul.max(&context_lr);

        debug!(?max_context, "max context per dimension");

        let min_level_stride = &max_context + self.block_write_roi.shape();

        debug!(?min_level_stride, "min level stride");

        // Round up to next multiple of write shape.
        let write_shape = self.block_write_roi.shape();
        let level_stride = Coordinate::new(
            min_level_stride
                .iter()
                .zip(write_shape.iter())
                .map(|(&level, &w)| ((level - 1) / w + 1) * w)
                .collect(),
        );

        // Clamp to total write ROI size to avoid empty levels.
        let mut write_roi_shape = self.total_write_roi.shape().clone();
        match self.fit {
            Fit::Valid => {
                // Round down to nearest block size.
                write_roi_shape =
                    &write_roi_shape - &(&write_roi_shape % self.block_write_roi.shape());
            }
            Fit::Overhang | Fit::Shrink => {
                // Round up to nearest block size.
                let remainder = &write_roi_shape % self.block_write_roi.shape();
                let adjustment = &(-&remainder) % self.block_write_roi.shape();
                write_roi_shape = &write_roi_shape + &adjustment;
            }
        }

        let level_stride = level_stride.min(&write_roi_shape);

        debug!(?level_stride, "final level stride");

        level_stride
    }

    fn compute_level_offsets(&self) -> Vec<Coordinate> {
        let write_stride = self.block_write_roi.shape();

        let dim_offsets: Vec<Vec<i64>> = (0..self.level_stride.dims())
            .map(|d| {
                (0..self.level_stride[d])
                    .step_by(write_stride[d] as usize)
                    .collect()
            })
            .collect();

        let mut offsets: Vec<Coordinate> = cartesian_product(&dim_offsets)
            .into_iter()
            .map(Coordinate::new)
            .collect();
        offsets.reverse();

        debug!(?offsets, "level offsets");

        offsets
    }

    fn compute_level_conflicts(&self) -> Vec<Vec<Coordinate>> {
        let mut level_conflict_offsets = Vec::new();
        let mut prev_level_offset: Option<&Coordinate> = None;

        for (_level, level_offset) in self.level_offsets.iter().enumerate() {
            let conflict_offsets = if let Some(prev) = prev_level_offset {
                if self.read_write_conflict {
                    self.get_conflict_offsets(level_offset, prev)
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };
            prev_level_offset = Some(level_offset);
            level_conflict_offsets.push(conflict_offsets);
        }

        level_conflict_offsets
    }

    fn get_conflict_offsets(
        &self,
        level_offset: &Coordinate,
        prev_level_offset: &Coordinate,
    ) -> Vec<Coordinate> {
        let offset_to_prev = prev_level_offset - level_offset;

        let conflict_dim_offsets: Vec<Vec<i64>> = offset_to_prev
            .iter()
            .zip(self.level_stride.iter())
            .map(|(&op, &ls)| {
                if op < 0 {
                    vec![op, op + ls]
                } else if op == 0 {
                    vec![op]
                } else {
                    vec![op - ls, op]
                }
            })
            .collect();

        let offsets: Vec<Coordinate> = cartesian_product(&conflict_dim_offsets)
            .into_iter()
            .map(Coordinate::new)
            .collect();

        debug!(?offsets, "conflict offsets to previous level");

        offsets
    }

    fn compute_level_block_offsets(
        &self,
        level: usize,
    ) -> impl Iterator<Item = Coordinate> + '_ {
        let level_offset = &self.level_offsets[level];

        let ranges: Vec<Vec<i64>> = (0..level_offset.dims())
            .map(|d| {
                let lo = level_offset[d];
                let hi = self.total_write_roi.shape()[d] + 1 - self.rounding_term[d];
                let step = self.level_stride[d];
                (lo..hi).step_by(step as usize).collect()
            })
            .collect();

        let global_offset =
            self.total_read_roi.offset() - self.block_read_roi.offset();

        // Lazy cartesian product — avoids allocating a Vec<Vec<i64>> of
        // length `num_blocks`, which on a 1M-block 1D task is a million
        // small allocations of upfront cost. With this iterator, blocks
        // are produced on demand as the scheduler hands them out.
        LazyCartesian::new(ranges)
            .map(move |offsets| Coordinate::new(offsets) + &global_offset)
    }

    /// Owned iterator over the blocks of `level`. Holds clones of the
    /// per-block data so it's `Send + 'static` and storable in a
    /// `Box<dyn Iterator<Item = Block> + Send>` on a `ProcessingQueue`.
    /// The hot path used by `Scheduler::new` for root-level dispatch.
    pub fn level_blocks_owned(&self, level: usize) -> LazyBlockIter {
        let level_offset = &self.level_offsets[level];
        let ranges: Vec<Vec<i64>> = (0..level_offset.dims())
            .map(|d| {
                let lo = level_offset[d];
                let hi = self.total_write_roi.shape()[d] + 1 - self.rounding_term[d];
                let step = self.level_stride[d];
                (lo..hi).step_by(step as usize).collect()
            })
            .collect();
        let global_offset =
            self.total_read_roi.offset() - self.block_read_roi.offset();
        LazyBlockIter {
            total_read_roi: self.total_read_roi.clone(),
            total_write_roi: self.total_write_roi.clone(),
            block_read_roi: self.block_read_roi.clone(),
            block_write_roi: self.block_write_roi.clone(),
            task_id: self.task_id.clone(),
            fit: self.fit.clone(),
            global_offset,
            cart: LazyCartesian::new(ranges),
        }
    }

    /// Owned root-level iterator (level 0). Same as `level_blocks_owned(0)`,
    /// matching `root_gen` semantically.
    pub fn root_iter_owned(&self) -> LazyBlockIter {
        self.level_blocks_owned(0)
    }

    fn inclusion_criteria(&self, block: &Block) -> bool {
        match self.fit {
            Fit::Valid => self.total_write_roi.contains_roi(&block.write_roi),
            Fit::Overhang => self.total_write_roi.contains_point(block.write_roi.begin()),
            Fit::Shrink => self.shrink_possible(block),
        }
    }

    fn fit_block(&self, block: Block) -> Block {
        match self.fit {
            Fit::Valid | Fit::Overhang => block,
            Fit::Shrink => self.shrink(block),
        }
    }

    fn shrink_possible(&self, block: &Block) -> bool {
        self.total_write_roi.contains_point(block.write_roi.begin())
    }

    fn shrink(&self, mut block: Block) -> Block {
        block.write_roi = self.total_write_roi.intersect(&block.write_roi);
        block.read_roi = self.total_read_roi.intersect(&block.read_roi);
        block
    }
}

/// Multi-task dependency graph. Wraps one `BlockwiseDependencyGraph`
/// per task and adds inter-task dependencies (task A's write ROI feeds
/// task B's read ROI). The task DAG itself is a `petgraph::DiGraph`
/// over task ids.
pub struct DependencyGraph {
    task_graphs: HashMap<String, BlockwiseDependencyGraph>,
    task_dag: DiGraph<String, ()>,
    task_dag_index: HashMap<String, NodeIndex>,
}

impl DependencyGraph {
    /// Build the inter-task dep graph from a `Pipeline`. Tasks no
    /// longer carry an `upstream_tasks` field; the Pipeline is the
    /// canonical dependency carrier.
    pub fn from_pipeline(pipeline: &crate::pipeline::Pipeline) -> Self {
        let mut task_dag: DiGraph<String, ()> = DiGraph::new();
        let mut task_dag_index: HashMap<String, NodeIndex> = HashMap::new();
        let mut task_graphs: HashMap<String, BlockwiseDependencyGraph> = HashMap::new();

        for task in &pipeline.tasks {
            let task_id = task.task_id.clone();
            let n = task_dag.add_node(task_id.clone());
            task_dag_index.insert(task_id.clone(), n);
            task_graphs.insert(task_id, BlockwiseDependencyGraph::from_task(task));
        }
        for (up, down) in pipeline.edges() {
            let u = task_dag_index[up];
            let d = task_dag_index[down];
            task_dag.add_edge(u, d, ());
        }

        Self { task_graphs, task_dag, task_dag_index }
    }

    pub fn num_blocks(&self, task_id: &str) -> i64 {
        self.task_graphs[task_id].num_blocks()
    }

    /// Task ids upstream of `task_id` (one hop, not transitive).
    pub fn upstream_task_ids<'a>(
        &'a self,
        task_id: &str,
    ) -> Box<dyn Iterator<Item = &'a str> + 'a> {
        match self.task_dag_index.get(task_id).copied() {
            Some(n) => Box::new(
                self.task_dag
                    .neighbors_directed(n, Direction::Incoming)
                    .map(move |nbr| self.task_dag[nbr].as_str()),
            ),
            None => Box::new(std::iter::empty()),
        }
    }

    /// Task ids downstream of `task_id` (one hop, not transitive).
    pub fn downstream_task_ids<'a>(
        &'a self,
        task_id: &str,
    ) -> Box<dyn Iterator<Item = &'a str> + 'a> {
        match self.task_dag_index.get(task_id).copied() {
            Some(n) => Box::new(
                self.task_dag
                    .neighbors_directed(n, Direction::Outgoing)
                    .map(move |nbr| self.task_dag[nbr].as_str()),
            ),
            None => Box::new(std::iter::empty()),
        }
    }

    /// Get all upstream blocks for a given block, including inter-task deps.
    pub fn upstream(&self, block: &Block) -> Vec<Block> {
        let task_id = block.task_id();
        let mut upstream = self.task_graphs[task_id].upstream(block);
        for up_task_id in self.upstream_task_ids(task_id) {
            upstream.extend(
                self.task_graphs[up_task_id].get_subgraph_blocks(&block.read_roi, false),
            );
        }
        upstream.sort_by_key(|b| b.block_id.spatial_id);
        upstream
    }

    /// Get all downstream blocks for a given block, including inter-task deps.
    pub fn downstream(&self, block: &Block) -> Vec<Block> {
        let task_id = block.task_id();
        let mut downstream = self.task_graphs[task_id].downstream(block);
        for down_task_id in self.downstream_task_ids(task_id) {
            downstream.extend(
                self.task_graphs[down_task_id].get_subgraph_blocks(&block.write_roi, true),
            );
        }
        downstream.sort_by_key(|b| b.block_id.spatial_id);
        downstream
    }

    pub fn root_tasks(&self) -> Vec<String> {
        self.task_dag
            .externals(Direction::Incoming)
            .map(|n| self.task_dag[n].clone())
            .collect()
    }

    pub fn num_roots(&self, task_id: &str) -> i64 {
        self.task_graphs[task_id].num_roots()
    }

    /// Return a mapping of root task_id → (num_roots, lazy root iterator).
    /// The iterator is owned (`Send + 'static`) and storable directly in a
    /// `ProcessingQueue` — no upfront materialization of the full block set.
    pub fn roots(&self) -> HashMap<String, (i64, Box<dyn Iterator<Item = Block> + Send>)> {
        self.root_tasks()
            .into_iter()
            .map(|task_id| {
                let num = self.num_roots(&task_id);
                let iter: Box<dyn Iterator<Item = Block> + Send> =
                    Box::new(self.task_graphs[&task_id].root_iter_owned());
                (task_id, (num, iter))
            })
            .collect()
    }
}

/// Compute the Cartesian product of a list of per-dimension value lists.
/// Returns a `Vec` of tuples represented as `Vec<i64>`. Eager — fine for
/// the small per-call uses (level offsets, sub-graph blocks). The hot
/// per-block iteration in `compute_level_block_offsets` uses the lazy
/// `LazyCartesian` instead.
fn cartesian_product(dim_values: &[Vec<i64>]) -> Vec<Vec<i64>> {
    if dim_values.is_empty() {
        return vec![vec![]];
    }
    let mut result = vec![vec![]];
    for values in dim_values {
        let mut next = Vec::with_capacity(result.len() * values.len());
        for partial in &result {
            for &v in values {
                let mut extended = partial.clone();
                extended.push(v);
                next.push(extended);
            }
        }
        result = next;
    }
    result
}

/// Owned, lazy iterator over the cartesian product of per-dimension value
/// lists. Produces tuples in the same row-major order as
/// `cartesian_product` (rightmost dimension varies fastest), so block
/// IDs derived from positions stay identical.
///
/// `Send + 'static` (no borrows), suitable for storage inside a
/// `Box<dyn Iterator + Send>` on a `ProcessingQueue`.
pub struct LazyCartesian {
    values: Vec<Vec<i64>>,
    indices: Vec<usize>,
    done: bool,
}

impl LazyCartesian {
    pub fn new(values: Vec<Vec<i64>>) -> Self {
        // Empty in any dimension → no products at all.
        let any_empty = values.iter().any(|v| v.is_empty());
        let dims = values.len();
        Self {
            values,
            indices: vec![0; dims],
            done: any_empty,
        }
    }
}

impl Iterator for LazyCartesian {
    type Item = Vec<i64>;

    fn next(&mut self) -> Option<Vec<i64>> {
        if self.done {
            return None;
        }
        // Snapshot the current tuple before advancing the counter.
        let item: Vec<i64> = self
            .indices
            .iter()
            .zip(self.values.iter())
            .map(|(&i, v)| v[i])
            .collect();
        // Multi-radix counter advance: rightmost dimension varies fastest,
        // matching `cartesian_product`'s output order.
        if self.values.is_empty() {
            self.done = true;
            return Some(item);
        }
        for i in (0..self.values.len()).rev() {
            self.indices[i] += 1;
            if self.indices[i] < self.values[i].len() {
                return Some(item);
            }
            self.indices[i] = 0;
        }
        // All dimensions wrapped — this is the last item.
        self.done = true;
        Some(item)
    }
}

/// Owned iterator yielding the blocks of one level of a single task's
/// dependency graph. Holds clones of the per-block data it needs so it
/// can be `'static` (storable in `Box<dyn Iterator<Item = Block> + Send>`).
pub struct LazyBlockIter {
    total_read_roi: Roi,
    total_write_roi: Roi,
    block_read_roi: Roi,
    block_write_roi: Roi,
    task_id: String,
    fit: Fit,
    global_offset: Coordinate,
    cart: LazyCartesian,
}

impl LazyBlockIter {
    fn inclusion_criteria(&self, block: &Block) -> bool {
        match self.fit {
            Fit::Valid => self.total_write_roi.contains_roi(&block.write_roi),
            Fit::Overhang | Fit::Shrink => {
                self.total_write_roi.contains_point(block.write_roi.begin())
            }
        }
    }

    fn fit_block(&self, mut block: Block) -> Block {
        match self.fit {
            Fit::Valid | Fit::Overhang => block,
            Fit::Shrink => {
                block.write_roi = self.total_write_roi.intersect(&block.write_roi);
                block.read_roi = self.total_read_roi.intersect(&block.read_roi);
                block
            }
        }
    }
}

impl Iterator for LazyBlockIter {
    type Item = Block;

    fn next(&mut self) -> Option<Block> {
        loop {
            let offsets = self.cart.next()?;
            let block_offset = Coordinate::new(offsets) + &self.global_offset;
            let block = Block::new(
                &self.total_read_roi,
                &self.block_read_roi + &block_offset,
                &self.block_write_roi + &block_offset,
                &self.task_id,
            );
            if self.inclusion_criteria(&block) {
                return Some(self.fit_block(block));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cartesian_product() {
        let result = cartesian_product(&[vec![0, 1], vec![0, 1, 2]]);
        assert_eq!(
            result,
            vec![
                vec![0, 0],
                vec![0, 1],
                vec![0, 2],
                vec![1, 0],
                vec![1, 1],
                vec![1, 2],
            ]
        );
    }

    #[test]
    fn test_lazy_cartesian_matches_eager() {
        // Lazy iterator must produce the same items in the same order
        // as the eager helper — block IDs are positional, so any
        // reorder would break done-marker compatibility.
        let dims = vec![vec![0, 1], vec![0, 1, 2]];
        let eager = cartesian_product(&dims);
        let lazy: Vec<Vec<i64>> = LazyCartesian::new(dims).collect();
        assert_eq!(eager, lazy);
    }

    #[test]
    fn test_lazy_cartesian_empty_dim_yields_nothing() {
        // If any dimension is empty, the product is empty.
        let lazy: Vec<Vec<i64>> =
            LazyCartesian::new(vec![vec![0, 1], vec![]]).collect();
        assert!(lazy.is_empty());
    }

    #[test]
    fn test_lazy_cartesian_no_dims_yields_one_empty() {
        // Conventional: cartesian product over zero dimensions has one
        // empty tuple. Mirror what `cartesian_product(&[])` returns so
        // any consumers using either path stay consistent.
        let eager = cartesian_product(&[]);
        let lazy: Vec<Vec<i64>> = LazyCartesian::new(vec![]).collect();
        assert_eq!(eager, lazy);
    }

    #[test]
    fn test_no_conflict_single_level() {
        let graph = BlockwiseDependencyGraph::new(
            "test",
            Roi::from_slices(&[0], &[10]),
            Roi::from_slices(&[0], &[10]),
            false,
            Fit::Valid,
            Some(Roi::from_slices(&[0], &[40])),
            None,
        );
        assert_eq!(graph.num_levels(), 1);
        assert_eq!(graph.num_blocks(), 4);
    }

    #[test]
    fn test_conflict_creates_levels() {
        // read_roi is larger than write_roi → context → multiple levels
        let graph = BlockwiseDependencyGraph::new(
            "test",
            Roi::from_slices(&[0], &[20]),
            Roi::from_slices(&[5], &[10]),
            true,
            Fit::Valid,
            Some(Roi::from_slices(&[0], &[60])),
            None,
        );
        assert!(graph.num_levels() > 1);
        assert!(graph.num_blocks() > 0);
    }

    #[test]
    fn test_block_counts_match_enumeration() {
        let graph = BlockwiseDependencyGraph::new(
            "test",
            Roi::from_slices(&[0, 0], &[10, 10]),
            Roi::from_slices(&[0, 0], &[10, 10]),
            false,
            Fit::Valid,
            Some(Roi::from_slices(&[0, 0], &[30, 30])),
            None,
        );
        let enumerated: usize = (0..graph.num_levels())
            .map(|l| graph.level_blocks(l).count())
            .sum();
        assert_eq!(graph.num_blocks(), enumerated as i64);
    }

    #[test]
    fn test_upstream_downstream_consistency() {
        let graph = BlockwiseDependencyGraph::new(
            "test",
            Roi::from_slices(&[0], &[20]),
            Roi::from_slices(&[5], &[10]),
            true,
            Fit::Valid,
            Some(Roi::from_slices(&[0], &[60])),
            None,
        );
        // For each level > 0 block, its upstream should be in the previous level.
        for level in 1..graph.num_levels() {
            for block in graph.level_blocks(level) {
                let upstream = graph.upstream(&block);
                assert!(
                    !upstream.is_empty(),
                    "block {block} in level {level} should have upstream deps"
                );
            }
        }
    }
}

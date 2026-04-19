//! Linear-chain detection over the workflow DAG.
//!
//! A *linear chain* is a maximal sequence of nodes where each interior
//! node has in-degree = 1 and out-degree = 1. The executor batches
//! such chains through
//! [`NodeDispatcher::dispatch_chain`](talos_workflow_engine_core::NodeDispatcher::dispatch_chain)
//! so the whole chain runs in a single transport round-trip on one
//! sandbox — one of the engine's main throughput optimizations.
//!
//! This module owns the pure-graph-topology detection. It has no
//! engine dependency; only `petgraph` and the `EdgeLogic` edge label.

use std::collections::HashSet;

use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::Direction;
use talos_workflow_engine_core::EdgeLogic;
use uuid::Uuid;

/// Detect all maximal linear chains in `graph`.
///
/// A linear chain is a maximal sequence of nodes `[v₀, v₁, …, vₙ]` where:
///
/// - Every interior node has in-degree = 1 and out-degree = 1.
/// - The source `v₀` can have any in-degree, but out-degree = 1.
/// - The sink `vₙ` can have any out-degree, but in-degree = 1.
///
/// Chains of length ≥ 2 benefit from pipeline dispatch: the worker
/// executes all steps in a single NATS round-trip without intermediate
/// serialisation.
///
/// Returns a `Vec` of chains, each chain being a `Vec<NodeIndex>` in
/// topological order (source → sink).
#[must_use]
pub fn detect_linear_chains(graph: &DiGraph<Uuid, EdgeLogic>) -> Vec<Vec<NodeIndex>> {
    // Find all potential chain *starts*: nodes with out-degree = 1 whose
    // predecessor either has out-degree ≠ 1 or is absent.
    let mut chain_starts: Vec<NodeIndex> = Vec::new();

    for idx in graph.node_indices() {
        let out_deg = graph.neighbors_directed(idx, Direction::Outgoing).count();
        if out_deg != 1 {
            continue; // Can't be an interior node or start of a 2+ chain.
        }
        let in_deg = graph.neighbors_directed(idx, Direction::Incoming).count();
        // A chain starts if:
        // - it has no predecessor (source), OR
        // - its predecessor has out-degree ≠ 1 (branches out, so chain starts here).
        if in_deg == 0 {
            chain_starts.push(idx);
        } else {
            let parent_out_deg = graph
                .neighbors_directed(idx, Direction::Incoming)
                .next()
                .map(|p| graph.neighbors_directed(p, Direction::Outgoing).count())
                .unwrap_or(0);
            if parent_out_deg != 1 {
                chain_starts.push(idx);
            }
        }
    }

    // Expand each start into its maximal chain.
    let mut visited: HashSet<NodeIndex> = HashSet::new();
    let mut chains: Vec<Vec<NodeIndex>> = Vec::new();

    for start in chain_starts {
        if visited.contains(&start) {
            continue;
        }

        let mut chain = vec![start];
        let mut current = start;

        loop {
            visited.insert(current);
            // Move to the single successor, if it qualifies as an interior node.
            let next = graph
                .neighbors_directed(current, Direction::Outgoing)
                .next();
            let Some(next_idx) = next else { break };

            let next_in_deg = graph
                .neighbors_directed(next_idx, Direction::Incoming)
                .count();
            let next_out_deg = graph
                .neighbors_directed(next_idx, Direction::Outgoing)
                .count();

            // The next node can continue the chain only if it has exactly one
            // incoming edge (from `current`). Out-degree can be anything for the
            // sink, but if it branches we stop — those children start new chains.
            if next_in_deg != 1 {
                break; // Fan-in: `next_idx` belongs to a different sub-graph.
            }
            chain.push(next_idx);
            current = next_idx;

            if next_out_deg != 1 {
                break; // Sink or fan-out — chain ends here.
            }
        }

        if chain.len() >= 2 {
            chains.push(chain);
        }
    }

    chains
}

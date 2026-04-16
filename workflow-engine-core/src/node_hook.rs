//! Engine hook for observing node completion.
//!
//! Implementations receive a synchronous notification each time a node
//! produces its final output, before the engine unblocks that node's
//! children. The hook is the engine's extension point for
//! cross-cutting concerns that care about per-node output: cost
//! attribution, side-effect persistence (actor-memory writes, audit
//! ledgers), metrics sampling, etc.
//!
//! The trait is deliberately **sync**. I/O-bearing impls should spawn
//! their own background tasks — this method runs inside the engine's
//! dispatch loop and blocking it would stall every downstream node.

use serde_json::Value as JsonValue;
use uuid::Uuid;

/// Called after each node's output is finalized.
///
/// # Contract
///
/// * **Impls that need async I/O MUST `tokio::spawn` (or equivalent).**
///   This method runs on the engine's dispatch loop. Awaiting a
///   database write, network call, or any other latency-bearing
///   operation inline will stall every downstream node in the workflow.
/// * Impls MUST return quickly. Synchronous work MUST be
///   side-effect-only and cheap (e.g. incrementing a counter).
/// * Impls observe output; they do not mutate it. The `output` value
///   is the exact shape that will propagate to this node's children.
/// * The engine invokes the hook at most once per node-completion
///   event. It is not called for skipped, pending, or failed nodes;
///   failures are surfaced through the consumer's event-persistence
///   path (e.g. the Talos `EventSink`). A future revision may add
///   sibling hooks for lifecycle transitions other than success.
/// * `actor_id` is the ID of the actor that owns the execution, if
///   any. Consumers that implement actor-scoped side effects (for
///   example, an actor-memory write triggered by an engine protocol
///   field in `output`) key off this.
pub trait NodeLifecycleHook: Send + Sync {
    /// Synchronous notification that `node_id` within execution
    /// `execution_id` has produced its final `output`.
    ///
    /// `node_label` is the user-defined label for the node (e.g.
    /// `"fetch-upcoming"`) when one exists, or `None` if the node is
    /// anonymous. Impls typically use it for human-readable rollups.
    fn on_node_completed(
        &self,
        execution_id: Uuid,
        node_id: Uuid,
        node_label: Option<&str>,
        actor_id: Option<Uuid>,
        output: &JsonValue,
    );
}

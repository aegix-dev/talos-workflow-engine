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

/// Identity + measurements that accompany every node-completion event.
///
/// Passed as a single struct rather than a long parameter list so the
/// call site stays readable and so adding a future field (e.g. fuel
/// consumed, wall-start timestamp) is a non-breaking change for
/// impls — they only pattern-match or field-access what they need.
#[derive(Debug, Clone, Copy)]
pub struct NodeCompletionContext<'a> {
    /// Parent workflow definition id. `Uuid::nil()` when the engine is
    /// running in a context with no durable workflow row (e.g. a
    /// one-off test harness); impls that persist per-workflow rollups
    /// should treat nil as "don't attribute".
    pub workflow_id: Uuid,
    /// Workflow execution that owns this dispatch.
    pub execution_id: Uuid,
    /// Engine-local node identifier within the graph.
    pub node_id: Uuid,
    /// User-defined label for the node (e.g. `"fetch-upcoming"`) when
    /// one exists, or `None` if the node is anonymous. Impls typically
    /// use it for human-readable rollups.
    pub node_label: Option<&'a str>,
    /// Resolved module id the node ran, if the engine has one. `None`
    /// for system nodes that don't dispatch to a wasm module
    /// (`SubWorkflow`, `FanIn`, synthetic triggers, etc.).
    pub module_id: Option<Uuid>,
    /// Actor that owns the execution. Consumers that implement
    /// actor-scoped side effects (for example, an actor-memory write
    /// triggered by an engine protocol field in `output`) key off this.
    pub actor_id: Option<Uuid>,
    /// Wall-clock execution time in milliseconds, measured from
    /// dispatch to completion. `0` when the engine didn't record a
    /// start time (some legacy paths don't); impls should treat `0`
    /// as "unknown" rather than "instantaneous".
    pub wall_time_ms: u64,
}

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
pub trait NodeLifecycleHook: Send + Sync {
    /// Synchronous notification that the node identified by
    /// `ctx.node_id` has produced its final `output`.
    fn on_node_completed(&self, ctx: NodeCompletionContext<'_>, output: &JsonValue);
}

//! Core data model + trait boundaries for a portable workflow
//! execution engine.
//!
//! This crate is **types + traits only**. It deliberately carries no
//! async runtime, no I/O, and no expression-evaluator dependency. An
//! executor crate layers the scheduling loop on top of these types
//! and plugs in consumer-supplied impls for each trait boundary.
//!
//! # What the executor does with these types
//!
//! The executor (currently `controller::engine::parallel` in the
//! Talos tree; extraction in progress) owns the scheduling loop.
//! Given a graph of [`SystemNodeKind`]-typed nodes connected by
//! [`EdgeLogic`], it:
//!
//! * **Topologically orders** the graph and detects linear chains.
//!   Maximal sequences of nodes with in-degree = out-degree = 1 are
//!   batched through [`NodeDispatcher::dispatch_chain`] as a single
//!   pipeline dispatch — one transport round-trip and one shared
//!   sandbox for the whole chain instead of per-node overhead.
//! * **Fans out** non-chain nodes via [`NodeDispatcher::dispatch`] on
//!   `tokio::spawn` with a configurable concurrency cap, and joins
//!   siblings via [`JoinMode`] (All / Any / Majority / N).
//! * **Speculatively prefetches** module artifacts for a node's
//!   downstream successors (via the `ModuleFetcher` trait, currently
//!   in the controller crate) while the parent still runs — hiding
//!   fetch latency behind execution.
//! * **Supports sub-workflow primitives**: every one of
//!   [`SystemNodeKind`]'s variants (`SubWorkflow`, `Judge`,
//!   `Ensemble`, `AgentLoop`, `ReActLoop`, `ReflectiveRetry`,
//!   `LlmDispatch`, `DynamicDispatch`, `CapabilityDispatch`,
//!   `ConfidenceGate`, `Verify`, `Synthesize`, `Collect`, `ForEach`,
//!   `FanIn`, `WhileLoop`, `RepeatLoop`, `Wait`, `ErrorHandler`) is
//!   dispatched through a matching handler that composes
//!   [`NodeDispatcher`] with engine-local state. Sub-workflow graphs
//!   are batch-prefetched at run start (one `WHERE id = ANY($1)`
//!   query via [`WorkflowGraphStore`]) to eliminate N+1 lookups.
//! * **Resumes paused runs** from a checkpoint via
//!   [`CheckpointStore`] (currently load-only; a save method will
//!   follow once the first consumer migrates to the trait).
//! * **Enforces security invariants** at every dispatch:
//!   [`SecretsResolver`] resolves per-node secrets; the executor
//!   refreshes short-lived credentials via
//!   [`SecretsResolver::refresh_vault_paths`] before handing them
//!   opaque-encrypted to the dispatcher; signed HMAC wire formats and
//!   topic-scoped queues are the dispatcher's concern.
//! * **Observes lifecycle events** via [`NodeLifecycleHook`] for
//!   per-node post-completion side effects (cost attribution, audit
//!   hooks, custom persistence).
//!
//! # Trait boundaries
//!
//! Every external-I/O concern the executor needs is behind exactly
//! one trait:
//!
//! * [`SecretsResolver`] — resolve module / vault / LLM-provider
//!   secrets; optional OAuth-style refresh hook.
//! * [`CheckpointStore`] — load a paused run's per-node outputs.
//! * [`WorkflowGraphStore`] — resolve sub-workflow graphs by id.
//! * [`NodeLifecycleHook`] — observe node completion for cross-cutting
//!   concerns.
//! * [`JobTransport`] — raw "send bytes, get bytes" channel to the
//!   worker pool (caller-owned timeout).
//! * [`NodeDispatcher`] — high-level "run this node (or chain of
//!   nodes)" primitive. Owns wire-format construction, signing,
//!   retry, and result parsing.
//!
//! Two more traits (`EventSink`, `ModuleFetcher`) live in the
//! controller today alongside the executor but have no Talos types
//! in their signatures; they will migrate into this crate in a
//! follow-up.
//!
//! # What's in this crate, what's not
//!
//! This crate is **types + trait boundaries** — it is the API the
//! executor commits to, nothing more. See the crate README for
//! non-goals. The executor implementation itself lives in the
//! downstream crate that uses these traits.

#![cfg_attr(docsrs, feature(doc_cfg))]

mod checkpoint;
mod context;
mod dispatcher;
mod edge;
mod event_sink;
mod graph_store;
mod node_hook;
mod retry;
mod secrets;
mod system_node;
mod transport;

pub use checkpoint::CheckpointStore;
pub use context::WorkflowContext;
pub use dispatcher::{
    dispatch_chain_sequential, ChainDispatchRequest, ChainDispatchResult, ChainStepResult,
    DispatchJob, DispatchResult, NodeDispatcher, StepStatus,
};
pub use edge::EdgeLogic;
pub use event_sink::{EventSink, NodeEventWrite};
pub use graph_store::WorkflowGraphStore;
pub use node_hook::NodeLifecycleHook;
pub use retry::RetryPolicy;
pub use secrets::{BoxError, SecretsResolver};
pub use system_node::{JoinMode, SystemNodeKind};
pub use transport::JobTransport;

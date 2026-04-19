//! Parallel workflow executor built on `talos-workflow-engine-core` traits.
//!
//! The engine runs a graph of system-node-typed workflow steps,
//! dispatching each through a consumer-supplied `NodeDispatcher`.
//! Every external-I/O concern (secrets, graph storage, events,
//! approvals, ...) is behind a trait boundary defined in
//! `talos-workflow-engine-core`; this crate carries only the scheduling loop,
//! sub-workflow handlers, and the primary engine type.
//!
//! See [`talos_workflow_engine_core`] for the trait surface; the
//! executor body is re-exported from this crate as
//! [`ParallelWorkflowEngine`] and [`AdapterSet`].

// Compile-time `llm-primitives` feature-coherence check.
//
// Cargo unifies features across the dependency graph, so the only
// mismatch a downstream `Cargo.toml` can actually produce is enabling
// `llm-primitives` on `talos-workflow-engine-core` while disabling it
// on `talos-workflow-engine` (the engine propagates its feature *to*
// `-core`, not the other way around). That mismatch leaves the LLM
// `SystemNodeKind` variants reachable in the type enum but never
// dispatched by the engine — a runtime-only failure with no
// compile-time signal. Catch it here.
//
// The check is a `const _: () = assert!(...)` rather than a build
// script so it fires during the normal `cargo check` cycle without
// adding a build dependency.
const _LLM_PRIMITIVES_FEATURE_COHERENCE_CHECK: () = assert!(
    talos_workflow_engine_core::HAS_LLM_PRIMITIVES == cfg!(feature = "llm-primitives"),
    "feature mismatch: `talos-workflow-engine-core` has the `llm-primitives` \
     feature enabled but `talos-workflow-engine` does not. The two crates must \
     agree. Either enable `llm-primitives` on `talos-workflow-engine` (the \
     default), or disable it on `talos-workflow-engine-core` via \
     `default-features = false`. Mismatched features produce LLM `SystemNodeKind` \
     variants that the engine cannot dispatch."
);

mod chain_detect;
mod engine;
pub mod error;
mod event_spawn;
pub mod graph_builder;
pub mod graph_json;
mod graph_parser;
mod sandbox;
mod scheduler_handlers;
mod secrets_pipeline;
mod validation;
pub mod vault_resolver;

pub use chain_detect::detect_linear_chains;
pub use engine::{
    AdapterSet, JudgeVerdict, ParallelWorkflowEngine, SubflowError, DEFAULT_NODE_TIMEOUT_SECS,
    DEFAULT_SANDBOX_ROOT,
};
pub use validation::validate_config_patterns;
pub use error::WorkflowEngineError;
pub use event_spawn::emit_event_spawn;
pub use graph_builder::{BuildError, WorkflowGraphBuilder, WorkflowGraphBuilderError};
pub use graph_json::{validate as validate_graph_json, GraphJsonError, GraphSummary, SCHEMA_DOC};
pub use vault_resolver::{
    extract_vault_refs, merge_vault_refs_into_allowlist, replace_vault_values, VaultRef,
};

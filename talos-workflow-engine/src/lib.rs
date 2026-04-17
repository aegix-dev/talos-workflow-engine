//! Parallel workflow executor built on `talos-workflow-engine-core` traits.
//!
//! The engine runs a graph of system-node-typed workflow steps,
//! dispatching each through a consumer-supplied `NodeDispatcher`.
//! Every external-I/O concern (secrets, graph storage, events,
//! approvals, ...) is behind a trait boundary defined in
//! `talos-workflow-engine-core`; this crate carries only the scheduling loop,
//! sub-workflow handlers, and the primary engine type.
//!
//! See [`talos_workflow_engine_core`] for the trait surface and
//! `crates/talos-workflow-engine/src/engine.rs` for the executor body.

mod engine;
mod event_spawn;
pub mod vault_resolver;

pub use engine::{
    detect_linear_chains, validate_config_patterns, AdapterSet, JudgeVerdict,
    ParallelWorkflowEngine, SubflowError, DEFAULT_NODE_TIMEOUT_SECS,
};
pub use event_spawn::emit_event_spawn;
pub use vault_resolver::{
    extract_vault_refs, merge_vault_refs_into_allowlist, replace_vault_values, VaultRef,
};

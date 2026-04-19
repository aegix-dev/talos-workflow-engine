//! Public error type for the engine's high-level entry points.
//!
//! Internal scheduling code returns `Result<_, String>` because the
//! engine body is large and the legacy convention pre-dates this
//! module. The public surface — [`ParallelWorkflowEngine::run_with_transport`],
//! [`ParallelWorkflowEngine::run_with_seed_with_transport`],
//! graph-loading methods, and the validators — wraps that string in
//! [`WorkflowEngineError`] so callers can match on documented failure
//! categories instead of substring-matching diagnostic text.
//!
//! # Categorized vs catch-all variants
//!
//! Variants split into three buckets:
//!
//! * **Documented failure modes** with no message body
//!   ([`SecretsResolverMissing`], [`GraphCyclic`]) — the engine
//!   commits to surfacing exactly these conditions when they happen,
//!   so consumers can branch on the variant and produce their own
//!   diagnostics.
//! * **Wrappers around lower-level errors** ([`GraphJson`], [`Subflow`])
//!   — pass through the typed inner error so callers retain its
//!   structure.
//! * **Catch-alls with a `String` payload** ([`LoadGraph`],
//!   [`Execution`]) — used when an internal site reports a problem
//!   the engine has not yet promoted to a typed variant. The variants
//!   are stable; the message bodies are not. New typed variants land
//!   in additive minor releases as more failure modes get categorized.
//!
//! [`ParallelWorkflowEngine::run_with_transport`]: crate::ParallelWorkflowEngine::run_with_transport
//! [`ParallelWorkflowEngine::run_with_seed_with_transport`]: crate::ParallelWorkflowEngine::run_with_seed_with_transport
//! [`SecretsResolverMissing`]: WorkflowEngineError::SecretsResolverMissing
//! [`GraphCyclic`]: WorkflowEngineError::GraphCyclic
//! [`GraphJson`]: WorkflowEngineError::GraphJson
//! [`Subflow`]: WorkflowEngineError::Subflow
//! [`LoadGraph`]: WorkflowEngineError::LoadGraph
//! [`Execution`]: WorkflowEngineError::Execution

use crate::engine::SubflowError;
use crate::graph_json::GraphJsonError;

/// Public error returned from [`ParallelWorkflowEngine`]'s high-level
/// entry points.
///
/// See the [module-level docs](self) for variant categories and
/// stability semantics.
///
/// [`ParallelWorkflowEngine`]: crate::ParallelWorkflowEngine
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkflowEngineError {
    /// The engine was constructed without a [`SecretsResolver`] and a
    /// run was attempted.
    ///
    /// Run-paths refuse to proceed without a resolver because every
    /// dispatch site requires one to encrypt per-node secrets; an
    /// unset resolver would silently produce empty-ciphertext
    /// dispatches. Wire one via
    /// [`set_secrets_resolver`](crate::ParallelWorkflowEngine::set_secrets_resolver)
    /// before calling a run method.
    ///
    /// [`SecretsResolver`]: talos_workflow_engine_core::SecretsResolver
    #[error(
        "ParallelWorkflowEngine has no SecretsResolver configured; \
         call `set_secrets_resolver` before invoking a run method"
    )]
    SecretsResolverMissing,

    /// The configured workflow graph contains a cycle.
    #[error("workflow graph contains a cycle")]
    GraphCyclic,

    /// Hard structural problem reading a `graph_json` payload —
    /// invalid JSON, top-level not an object, or `nodes` / `edges`
    /// fields with the wrong type. Soft issues (skipped nodes,
    /// unknown system kinds) flow through
    /// [`GraphSummary::warnings`](crate::GraphSummary) instead.
    #[error("graph JSON is malformed: {0}")]
    GraphJson(#[from] GraphJsonError),

    /// Sub-workflow execution failed. Carries the structured
    /// [`SubflowError`] from the engine's sub-workflow dispatch path.
    #[error("sub-workflow execution failed: {0:?}")]
    Subflow(SubflowError),

    /// A graph-loading method rejected its input for a reason the
    /// engine has not yet promoted to a typed variant. The message
    /// body is human-readable; do not pattern-match its text — match
    /// on the variant only.
    #[error("graph load failed: {0}")]
    LoadGraph(String),

    /// A run failed for a reason the engine has not yet promoted to
    /// a typed variant. The message body is human-readable; do not
    /// pattern-match its text — match on the variant only.
    #[error("workflow execution failed: {0}")]
    Execution(String),
}

impl WorkflowEngineError {
    /// Construct an [`Execution`](Self::Execution) variant from any
    /// `Into<String>` source. Convenience for `.map_err` chains:
    ///
    /// ```ignore
    /// some_call().await.map_err(WorkflowEngineError::execution)?;
    /// ```
    pub fn execution(message: impl Into<String>) -> Self {
        Self::Execution(message.into())
    }

    /// Construct a [`LoadGraph`](Self::LoadGraph) variant from any
    /// `Into<String>` source. Convenience for `.map_err` chains.
    pub fn load_graph(message: impl Into<String>) -> Self {
        Self::LoadGraph(message.into())
    }
}

impl From<SubflowError> for WorkflowEngineError {
    fn from(value: SubflowError) -> Self {
        Self::Subflow(value)
    }
}

impl From<crate::graph_builder::BuildError> for WorkflowEngineError {
    /// A [`WorkflowGraphBuilder`](crate::WorkflowGraphBuilder) that
    /// accumulated errors and failed at [`build`](crate::WorkflowGraphBuilder::build)
    /// is semantically a graph-load failure — the builder output is
    /// the exact artifact the engine would have otherwise loaded.
    /// Route through the [`LoadGraph`](Self::LoadGraph) catch-all so
    /// callers using `?` from a builder chain see the same variant
    /// they'd see for any other malformed input.
    fn from(value: crate::graph_builder::BuildError) -> Self {
        Self::LoadGraph(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_constructor_wraps_message() {
        let err = WorkflowEngineError::execution("boom");
        assert!(matches!(err, WorkflowEngineError::Execution(ref s) if s == "boom"));
        assert_eq!(err.to_string(), "workflow execution failed: boom");
    }

    #[test]
    fn load_graph_constructor_wraps_message() {
        let err = WorkflowEngineError::load_graph("missing nodes");
        assert!(matches!(err, WorkflowEngineError::LoadGraph(ref s) if s == "missing nodes"));
    }

    #[test]
    fn secrets_resolver_missing_has_descriptive_display() {
        let err = WorkflowEngineError::SecretsResolverMissing;
        assert!(err.to_string().contains("SecretsResolver"));
    }

    #[test]
    fn graph_cyclic_has_descriptive_display() {
        let err = WorkflowEngineError::GraphCyclic;
        assert_eq!(err.to_string(), "workflow graph contains a cycle");
    }

    #[test]
    fn from_graph_json_error_promotes() {
        let json_err: GraphJsonError = serde_json::from_str::<serde_json::Value>("{not")
            .map_err(GraphJsonError::from)
            .unwrap_err();
        let wrapped: WorkflowEngineError = json_err.into();
        assert!(matches!(wrapped, WorkflowEngineError::GraphJson(_)));
    }

    #[test]
    fn from_subflow_error_promotes() {
        let sub = SubflowError::NoUserId;
        let wrapped: WorkflowEngineError = sub.into();
        assert!(matches!(wrapped, WorkflowEngineError::Subflow(_)));
    }
}

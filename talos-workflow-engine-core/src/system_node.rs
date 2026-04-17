//! Built-in node taxonomy and fan-in join modes.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Built-in system node kinds that receive special handling by the
/// executor, distinct from user-supplied module nodes.
///
/// Consumers that need a kind not listed here should extend the executor's
/// dispatcher registry rather than forking this enum. The variants below
/// reflect a practical production set drawn from real workloads and are
/// likely to be useful to other adopters; the list may grow over time but
/// existing variants will not silently change shape.
///
/// `PartialEq` is derived but not `Eq`/`Hash`: two variants carry `f64`
/// thresholds, and `f64` is not totally ordered. Consumers that need a
/// hashable discriminator should project onto a dedicated `&'static str`
/// tag instead of hashing the whole value.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum SystemNodeKind {
    /// Iterate over an array in the parent's output and fan out a branch
    /// per element.
    ForEach {
        /// JSON pointer / path into the parent output locating the array.
        input_path: String,
        /// Handle name used for each element passed to the child branch.
        output_handle: String,
    },
    /// Pause execution until resumed externally.
    Wait {
        /// Optional human-readable message surfaced to the resumer.
        message: Option<String>,
    },
    /// Execute the body while a condition holds, up to `max_iterations`.
    WhileLoop {
        /// Expression evaluated before each iteration.
        condition: String,
        /// Hard safety cap on iteration count.
        max_iterations: u32,
    },
    /// Execute the body a fixed number of times.
    RepeatLoop {
        /// Number of iterations.
        count: u32,
    },
    /// Handle an error from upstream, optionally matching a pattern.
    ErrorHandler {
        /// Optional regex or substring the error must match to trigger.
        error_pattern: Option<String>,
    },
    /// Synchronize multiple inbound branches.
    FanIn {
        /// How many branches must complete before the join releases.
        join_mode: JoinMode,
        /// Optional expression aggregating the branch outputs.
        aggregation_expr: Option<String>,
    },
    /// Invoke another workflow by id and return its collapsed output.
    SubWorkflow {
        /// Target workflow id.
        workflow_id: Uuid,
        /// Hard timeout for the sub-workflow in seconds.
        timeout_secs: u64,
    },
    /// General loop node combining a condition with an iteration cap.
    Loop {
        /// Hard safety cap on iteration count.
        max_iterations: u32,
        /// Expression evaluated before each iteration.
        condition: String,
    },
    /// Collect branch outputs without otherwise transforming them.
    Collect,
    /// Synthesize a value from prior outputs, optionally via expression.
    Synthesize {
        /// Optional expression building the synthesized value.
        synthesis_expr: Option<String>,
    },
    /// Assert a condition; branch on failure.
    Verify {
        /// Expression that must evaluate to `true`.
        condition: String,
        /// Optional label identifying the check in output.
        check_label: Option<String>,
        /// Handle name to route down when the check fails.
        on_failure: String,
    },
    /// ReAct-style agent loop running a body workflow with sliding-window
    /// history injection.
    AgentLoop {
        /// Workflow id of the per-iteration body.
        body_workflow_id: Uuid,
        /// Hard safety cap on iteration count.
        max_iterations: u32,
        /// If `true`, inject prior iteration outputs as history.
        inject_history: bool,
        /// Hard timeout for each body invocation in seconds.
        timeout_secs: u64,
    },
    /// Run a judge workflow and parse its verdict.
    Judge {
        /// Workflow id of the judge.
        judge_workflow_id: Uuid,
        /// Rubric prompt or description passed to the judge.
        rubric: String,
        /// Optional score threshold the verdict must meet to pass.
        pass_threshold: Option<f64>,
        /// Hard timeout for the judge invocation in seconds.
        timeout_secs: u64,
    },
    /// Run N copies of a child workflow and consolidate their outputs.
    Ensemble {
        /// Workflow id of the child to replicate.
        child_workflow_id: Uuid,
        /// Number of child invocations.
        count: u32,
        /// Consensus strategy label (executor-defined).
        consensus: String,
        /// Optional judge used to score candidates.
        judge_workflow_id: Option<Uuid>,
        /// Hard timeout for each child invocation in seconds.
        timeout_secs: u64,
    },
    /// Branch when a confidence signal falls below a threshold.
    ConfidenceGate {
        /// Minimum confidence required to take the pass path.
        threshold: f64,
        /// Path into the parent output locating the confidence value.
        confidence_path: String,
        /// Handle name to route down when confidence is below threshold.
        on_low_confidence: String,
    },
    /// Dispatch to a target chosen at runtime by an expression.
    DynamicDispatch {
        /// Expression that resolves to a dispatch target.
        dispatch_expression: String,
        /// Hard timeout for the dispatched target in seconds.
        timeout_secs: u64,
    },
    /// Dispatch to any worker that advertises the required capabilities.
    CapabilityDispatch {
        /// Capability labels the target must all advertise.
        required_capabilities: Vec<String>,
        /// Hard timeout for the dispatched target in seconds.
        timeout_secs: u64,
    },
    /// Alternative agent-loop shape (reasoning + acting) with history.
    ReActLoop {
        /// Workflow id of the per-iteration body.
        body_workflow_id: Uuid,
        /// Hard safety cap on iteration count.
        max_iterations: u32,
        /// If `true`, inject prior iteration outputs as history.
        inject_history: bool,
        /// Hard timeout for each body invocation in seconds.
        timeout_secs: u64,
    },
    /// Run a child; on failure, run a reflection workflow and retry.
    ReflectiveRetry {
        /// Workflow id of the primary child.
        child_workflow_id: Uuid,
        /// Workflow id of the reflection step producing feedback.
        reflection_workflow_id: Uuid,
        /// Maximum retries after the first failure.
        max_retries: u32,
        /// Hard timeout per attempt in seconds.
        timeout_secs: u64,
    },
    /// Dispatch to one of several routes based on an LLM classifier.
    LlmDispatch {
        /// Workflow id of the classifier whose output selects the route.
        classifier_workflow_id: Uuid,
        /// Route name -> target workflow id.
        routes: HashMap<String, Uuid>,
        /// Optional fallback when no route matches.
        fallback_workflow_id: Option<Uuid>,
        /// Hard timeout for the dispatched route in seconds.
        timeout_secs: u64,
    },
}

/// Fan-in join semantics for [`SystemNodeKind::FanIn`].
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum JoinMode {
    /// Release only after every inbound branch completes.
    All,
    /// Release as soon as any inbound branch completes.
    Any,
    /// Release once a strict majority of inbound branches complete.
    Majority,
    /// Release once exactly `N` inbound branches complete.
    N(u32),
}

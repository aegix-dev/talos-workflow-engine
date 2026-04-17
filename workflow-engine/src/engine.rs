#![allow(dead_code)]

use futures::stream::{FuturesUnordered, StreamExt};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::Direction;
use serde_json::{Map, Value as JsonValue};
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{LazyLock, OnceLock};

/// Global rate-limit counter: module_id -> (window_start, request_count).
/// Shared across all concurrent engine runs so rate limits are enforced globally.
/// Eviction: entries older than 5 minutes are pruned when the map exceeds 1000 entries.
static MODULE_RATE_LIMITS: LazyLock<dashmap::DashMap<uuid::Uuid, (std::time::Instant, u32)>> =
    LazyLock::new(dashmap::DashMap::new);

/// Default per-node execution timeout (in seconds) applied when a node's
/// graph data doesn't carry an explicit `timeout_secs`.
///
/// 60s covers both simple HTTP fetches (sub-second typical) and LLM synthesis
/// against Ollama (20-45s typical) without requiring every agent-node module
/// author to set a custom timeout. Individual nodes can still raise or lower
/// via `add_node_to_workflow(timeout_secs:…)`; there is no implicit clamp.
///
/// Respects `WASM_EXECUTION_TIMEOUT_SECS` env var for operator override —
/// matches `get_wasm_config`'s default so the tool output and actual
/// runtime behavior agree. Previously these defaults were hardcoded `30` at
/// five call sites and diverged from the configurable env default.
pub static DEFAULT_NODE_TIMEOUT_SECS: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("WASM_EXECUTION_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(60)
});

/// Evict stale entries from MODULE_RATE_LIMITS when the map grows beyond the threshold.
fn evict_stale_rate_limits() {
    const MAX_ENTRIES: usize = 1000;
    const STALE_SECS: u64 = 300;
    if MODULE_RATE_LIMITS.len() > MAX_ENTRIES {
        let cutoff = std::time::Instant::now() - std::time::Duration::from_secs(STALE_SECS);
        MODULE_RATE_LIMITS.retain(|_, (window_start, _)| *window_start > cutoff);
    }
}

// Alias to silence Clippy's `type_complexity` warning and improve readability.
// Represents a boxed future that resolves to a node index and its execution result.
// Generic alias allowing the future to live for any lifetime `'a`.
type ExecFuture<'a> =
    Pin<Box<dyn Future<Output = (NodeIndex, Result<JsonValue, String>)> + Send + 'a>>;
use std::sync::Arc;
use uuid::Uuid;

/// Maximum number of successor nodes to speculatively prefetch per node.
/// Prevents fan-out DoS when a single node has many outgoing edges.
const MAX_PREFETCH_SUCCESSORS: usize = 8;

/// Maximum history entries maintained in AgentLoop's sliding window.
/// Keeps the last N iteration outputs injected as `__agent_history__`.
const AGENT_LOOP_MAX_HISTORY: usize = 20;

use crate::emit_event_spawn;
use workflow_engine_core::{
    DispatchJob, EdgeLogic, EventSink, JoinMode, ModuleFetcher, NodeEventWrite, NodeLifecycleHook,
    RetryPolicy, SecretsResolver, SystemNodeKind, WorkflowContext, WorkflowGraphStore,
};

// Checkpoint encryption + persistence live in
// `crate::engine::checkpoint_store::ControllerCheckpointStore`, which
// implements the `workflow_engine_core::CheckpointStore` trait. Engine
// code used to own `encrypt_checkpoint` / `decrypt_checkpoint` free
// functions and a `load_checkpoint` method bound to a `&PgPool`; those
// were the last raw-sqlx surface in this file. See the adapter module
// for both the storage format and the env-var fallback behavior.

/// Create a temporary sandboxed directory for a workflow execution.
/// Returns an Arc-wrapped cap-std Dir for secure file access.
/// The directory will be created under /tmp/talos-sandboxes/{execution_id}
/// and should be cleaned up after workflow execution completes.
fn create_execution_sandbox(execution_id: Uuid) -> Result<Arc<cap_std::fs::Dir>, String> {
    let sandbox_base = std::path::PathBuf::from("/tmp/talos-sandboxes");

    // Create base directory if it doesn't exist
    std::fs::create_dir_all(&sandbox_base)
        .map_err(|e| format!("Failed to create sandbox base directory: {}", e))?;

    // Create execution-specific sandbox directory
    let sandbox_path = sandbox_base.join(execution_id.to_string());
    std::fs::create_dir_all(&sandbox_path)
        .map_err(|e| format!("Failed to create execution sandbox directory: {}", e))?;

    // Open directory with cap-std for capability-based security
    cap_std::fs::Dir::open_ambient_dir(&sandbox_path, cap_std::ambient_authority())
        .map(Arc::new)
        .map_err(|e| format!("Failed to open sandbox directory with cap-std: {}", e))
}

/// RAII guard that removes the execution sandbox directory when dropped.
/// This ensures cleanup happens even if the execution task panics.
struct SandboxGuard {
    execution_id: Uuid,
}

impl Drop for SandboxGuard {
    fn drop(&mut self) {
        let sandbox_path =
            std::path::PathBuf::from("/tmp/talos-sandboxes").join(self.execution_id.to_string());
        if let Err(e) = std::fs::remove_dir_all(&sandbox_path) {
            tracing::warn!(
                "Failed to cleanup execution sandbox {}: {}",
                self.execution_id,
                e
            );
        } else {
            tracing::debug!("Cleaned up execution sandbox: {}", self.execution_id);
        }
    }
}

// ============================================================================
// LINEAR CHAIN DETECTION (Superpower 2)
// ============================================================================

/// Detect all maximal linear chains in `graph`.
///
/// A *linear chain* is a maximal sequence of nodes `[v₀, v₁, …, vₙ]` where:
/// - Every interior node has in-degree = 1 and out-degree = 1.
/// - The source `v₀` can have any in-degree, but out-degree = 1.
/// - The sink `vₙ` can have any out-degree, but in-degree = 1.
///
/// Chains of length ≥ 2 benefit from pipeline dispatch: the worker executes all
/// steps in a single NATS round-trip without intermediate serialisation.
///
/// Returns a `Vec` of chains, each chain being a `Vec<NodeIndex>` in topological
/// order (source → sink).
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
            // incoming edge (from `current`).  Out-degree can be anything for the
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

// Canonical LLM provider vault paths live in `job_protocol::LLM_PROVIDER_VAULT_PATHS`.
// Import from there directly — this crate no longer re-exports to keep one
// single source of truth discoverable by `grep LLM_PROVIDER_VAULT_PATHS`.
// LLM-key pre-fetch now flows through `SecretsResolver::resolve_llm_keys`;
// the controller's impl ultimately delegates to `SecretsManager::get_llm_vault_keys`.

/// Structured errors from [`ParallelWorkflowEngine::execute_subworkflow_graph`].
/// Callers convert these into their own error envelopes via
/// [`SubflowError::into_error_envelope`] so each system-node kind can keep its
/// own context-specific messages ("Judge workflow X not found", etc).
#[derive(Debug, Clone)]
pub enum SubflowError {
    /// Engine has no registry configured — sub-workflow execution impossible.
    NoRegistry,
    /// Engine has no user_id — all sub-workflow execution requires it.
    NoUserId,
    /// Secrets resolver not attached — sub-workflow modules couldn't fetch secrets.
    NoSecretsResolver,
    /// No workflow matching `sub_wf_id` exists (or not visible to user_id).
    GraphNotFound(Uuid),
    /// `build_engine_from_graph_json_with_resolver` failed — usually a module resolution issue.
    BuildFailed(String),
    /// `run_with_seed` returned an error — execution actually ran and failed.
    ExecutionFailed(String),
}

impl SubflowError {
    /// Canonical `{__error, error_message}` envelope with a caller-provided
    /// context label (e.g. "Judge", "Ensemble child", "Sub-workflow").
    pub fn into_error_envelope(self, context: &str) -> JsonValue {
        let msg = match self {
            SubflowError::NoRegistry => {
                format!("Registry not available for {} node", context)
            }
            SubflowError::NoUserId => {
                "user_id required for sub-workflow execution".to_string()
            }
            SubflowError::NoSecretsResolver => {
                format!("secrets resolver unavailable for {} execution", context)
            }
            SubflowError::GraphNotFound(id) => {
                format!("{} workflow {} not found", context, id)
            }
            SubflowError::BuildFailed(e) => {
                format!("Failed to build {} workflow engine: {}", context, e)
            }
            SubflowError::ExecutionFailed(e) => {
                format!("{} workflow execution failed: {}", context, e)
            }
        };
        serde_json::json!({ "__error": true, "error_message": msg })
    }
}

/// Structured judge verdict parsed from a collapsed sub-workflow output.
///
/// Downstream consumers (judge_node, ensemble best_of_n) want the same 4 fields;
/// this struct centralizes parsing and logs when fields are missing so malformed
/// judge workflows fail loudly rather than silently scoring 0.0.
#[derive(Debug, Clone)]
pub struct JudgeVerdict {
    pub score: f64,
    pub passed: bool,
    pub reasoning: String,
    pub feedback: String,
    /// Number of expected fields that were missing or wrong-typed in the
    /// sub-workflow output (0..=4). Non-zero indicates a malformed judge workflow.
    pub malformed_field_count: u8,
}

impl JudgeVerdict {
    /// Parse a verdict from a collapsed sub-workflow output. Missing/mistyped
    /// fields fall back to defaults and increment `malformed_field_count` so
    /// callers can surface the issue. Always returns a value — judge extraction
    /// must never panic at runtime.
    pub fn from_collapsed(verdict: &JsonValue) -> Self {
        let mut malformed = 0u8;
        let score = match verdict.get("score").and_then(|v| v.as_f64()) {
            Some(v) => v,
            None => { malformed += 1; 0.0 }
        };
        let passed = match verdict.get("passed").and_then(|v| v.as_bool()) {
            Some(v) => v,
            None => { malformed += 1; false }
        };
        let reasoning = match verdict.get("reasoning").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => { malformed += 1; String::new() }
        };
        let feedback = match verdict.get("feedback").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => { malformed += 1; String::new() }
        };
        if malformed > 0 {
            tracing::warn!(
                malformed_fields = malformed,
                "Judge sub-workflow returned malformed verdict — missing or wrong-typed fields. \
                 Expected {{score: f64, passed: bool, reasoning: string, feedback: string}}."
            );
        }
        Self { score, passed, reasoning, feedback, malformed_field_count: malformed }
    }
}

// Suppress dead‑code warnings to keep the CI passing.
#[allow(dead_code)]
/// Parallel execution engine based on Kahn's algorithm.
pub struct ParallelWorkflowEngine {
    pub graph: DiGraph<Uuid, EdgeLogic>,
    pub node_map: HashMap<Uuid, NodeIndex>,
    /// Maps internal node UUIDs back to user-defined node IDs (e.g., "n1", "fetch").
    /// Populated by `load_graph_from_json`. Used to label output with user-friendly keys.
    pub node_labels: HashMap<Uuid, String>,
    /// Per-node configuration from the workflow graph. Merged into the module
    /// config when dispatching jobs, so template modules receive the config
    /// the user specified at workflow creation time.
    pub node_configs: HashMap<Uuid, serde_json::Value>,
    /// Pluggable resolver for the wasm module artifact that a node dispatches.
    /// In production wraps [`ModuleRegistry`] (which owns the 4-level fallback
    /// pipeline). Tests and out-of-tree consumers plug in their own impl.
    module_fetcher: Option<Arc<dyn ModuleFetcher>>,
    /// Pluggable fire-and-forget sink for per-node execution events
    /// (`node_started`, `node_completed`, `node_failed`, retries, loop
    /// iterations, etc.). In production wraps `execution_events` table
    /// writes; tests can plug in a no-op or in-memory capture.
    event_sink: Option<Arc<dyn EventSink>>,
    /// Post-completion hook invoked after each node finalizes its
    /// output. In production drives fuel-cost attribution and the
    /// `__memory_write__` actor-memory protocol; tests can plug in a
    /// capture hook to assert per-node outputs.
    node_hook: Option<Arc<dyn NodeLifecycleHook>>,
    /// Pluggable read-only access to workflow graph definitions — used
    /// when the engine hits a sub-workflow-ish system node (sub-workflow,
    /// judge, ensemble child, agent-loop body, reflective-retry child,
    /// LLM-dispatch route, etc.) and needs to hydrate its body's
    /// `graph_json`. In production wraps `WorkflowRepository`.
    graph_store: Option<Arc<dyn WorkflowGraphStore>>,
    /// Pluggable secret resolver. All module-secret, vault-path, and LLM-key
    /// lookups — plus the pre-resolution OAuth refresh hook — flow through
    /// this trait object, which in production wraps a `SecretsManager`.
    /// Tests and out-of-tree consumers plug in their own implementation.
    secrets_resolver: Option<Arc<dyn SecretsResolver>>,
    /// Owner of the workflow execution — required to enforce module ownership
    /// when fetching WASM bytes/config from the registry. `None` means the
    /// engine is running in a test/fallback context without a real registry.
    user_id: Option<Uuid>,
    /// Per-node metadata: maps node UUID to (module_id, retry_policy, kind).
    pub node_meta: HashMap<
        Uuid,
        (
            Option<Uuid>,
            Option<workflow_engine_core::RetryPolicy>,
            Option<SystemNodeKind>,
        ),
    >,
    /// Maximum execution time for the entire workflow in seconds. Default: 300 (5 minutes).
    pub execution_timeout_secs: u64,
    /// Per-module rate limits (requests per minute), loaded at graph init time.
    rate_limits: HashMap<Uuid, i32>,
    /// Per-node execution timeout in seconds. Overrides the default 30-second timeout.
    /// Loaded from `node.data.timeout_secs` or `node.timeout_secs` in the graph JSON.
    node_timeouts: HashMap<Uuid, u64>,
    /// Actor ID that owns this execution — used for __memory_write__ write-back.
    actor_id: Option<Uuid>,
    /// Actor memory context injected into every node's input as `__actor_context__`.
    /// Populated by the scheduler or trigger_workflow when an actor owns the execution.
    /// Enables LLM nodes to reference learned_preferences, persona, and other actor
    /// state without per-workflow plumbing.
    actor_context: Option<serde_json::Value>,
    /// Speculative module prefetch cache — populated by background fetch tasks when a node
    /// has `speculative_prefetch: true`. `fetch_module` checks here first to avoid a DB
    /// round-trip when the module was pre-loaded while a slow predecessor was executing.
    module_prefetch_cache: Arc<dashmap::DashMap<Uuid, workflow_engine_core::ModuleArtifact>>,
    /// Pre-fetched sub-workflow graphs, keyed by workflow_id.
    /// Populated at execution start to avoid N+1 queries during node dispatch.
    /// Workflows referenced by SubWorkflow, AgentLoop, Ensemble, Judge,
    /// ReflectiveRetry, LlmDispatch, and ReActLoop nodes are batch-loaded
    /// in a single `WHERE id = ANY($1)` query. DynamicDispatch and
    /// CapabilityDispatch resolve workflow IDs at runtime and fall back to
    /// individual queries on cache miss.
    sub_workflow_cache: HashMap<Uuid, JsonValue>,
    /// When true, non-GET HTTP requests are mocked in the worker (returns 200 with dry_run metadata).
    /// Propagated to each JobRequest so the worker can intercept side effects.
    pub dry_run: bool,
    /// Parent workflow definition id. Threaded into the
    /// [`NodeLifecycleHook::on_node_completed`] context so per-workflow
    /// cost rollups attribute to the right workflow row, not the
    /// per-run `execution_id`. Optional because some in-tree callers
    /// (tests, one-off diagnostic runs) don't have a durable workflow —
    /// dispatch sites fall back to `execution_id` when this is unset,
    /// which matches the pre-extraction behavior.
    workflow_id: Option<Uuid>,
    /// Pluggable evaluator for edge conditions, retry-delay expressions,
    /// and `Synthesize` expressions. In production wraps a `rhai::Engine`
    /// configured with sandbox limits; tests can plug in a no-op.
    expression_evaluator: Option<Arc<dyn workflow_engine_core::ExpressionEvaluator>>,
    /// Pluggable output sanitizer — applied to node output / error
    /// strings before persistence. In production wraps `talos_dlp`.
    output_sanitizer: Option<Arc<dyn workflow_engine_core::OutputSanitizer>>,
    /// Pluggable classifier for dispatch-error strings. Tells the
    /// retry loop whether a given failure is worth retrying. In
    /// production wraps `retry_intelligence`.
    retry_classifier: Option<Arc<dyn workflow_engine_core::RetryClassifier>>,
    /// Pluggable per-dispatch audit log. Single-node + pipeline-step
    /// dispatch paths write a "running" row pre-dispatch and an
    /// "completed" / "failed" / "timeout" / "cancelled" row after the
    /// worker replies. In production writes to the `module_executions`
    /// Postgres table; tests plug in a capture impl.
    module_execution_store:
        Option<Arc<dyn workflow_engine_core::ModuleExecutionStore>>,
    /// Pluggable human-in-the-loop approval gate. Nodes whose module
    /// declares `requires_approval_for: [...]` route through this
    /// before dispatch to check / create a pending approval row.
    /// In production writes to `execution_approvals`; tests can plug
    /// in an auto-approve or auto-deny impl.
    approval_gate: Option<Arc<dyn workflow_engine_core::ApprovalGate>>,
}

impl Default for ParallelWorkflowEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Cloneable bundle of every policy-bearing adapter an engine holds.
///
/// Produced by [`ParallelWorkflowEngine::adapter_set`]; consumed by
/// [`AdapterSet::into_engine`] to produce a fresh engine with the
/// same adapters. Used by sub-workflow dispatch closures that need
/// to construct one or more child engines — the closure captures a
/// single `AdapterSet` clone and hydrates engines on demand.
///
/// Every field is an `Arc` (or `Copy`); `Clone` is a bounded number
/// of refcount bumps, not a deep copy. Cheap enough to clone
/// per-iteration inside an agent loop.
#[derive(Clone)]
pub struct AdapterSet {
    module_fetcher: Option<Arc<dyn ModuleFetcher>>,
    event_sink: Option<Arc<dyn EventSink>>,
    node_hook: Option<Arc<dyn NodeLifecycleHook>>,
    graph_store: Option<Arc<dyn WorkflowGraphStore>>,
    secrets_resolver: Option<Arc<dyn SecretsResolver>>,
    expression_evaluator: Option<Arc<dyn workflow_engine_core::ExpressionEvaluator>>,
    output_sanitizer: Option<Arc<dyn workflow_engine_core::OutputSanitizer>>,
    retry_classifier: Option<Arc<dyn workflow_engine_core::RetryClassifier>>,
    module_execution_store: Option<Arc<dyn workflow_engine_core::ModuleExecutionStore>>,
    approval_gate: Option<Arc<dyn workflow_engine_core::ApprovalGate>>,
    user_id: Option<Uuid>,
    actor_id: Option<Uuid>,
    dry_run: bool,
}

impl AdapterSet {
    /// Hydrate a fresh engine with this adapter set and populate its
    /// graph from `graph_json` — the common one-shot path for
    /// sub-workflow dispatch closures. Fails closed with a
    /// caller-provided error type via the inner
    /// [`load_from_graph_json`](ParallelWorkflowEngine::load_from_graph_json)
    /// error string.
    pub fn into_engine_with_graph(
        self,
        graph_json: &JsonValue,
    ) -> Result<ParallelWorkflowEngine, String> {
        let mut engine = self.into_engine();
        engine.load_from_graph_json(graph_json)?;
        Ok(engine)
    }

    /// Hydrate a fresh engine with this adapter set. The returned
    /// engine has an empty graph; callers follow with
    /// [`ParallelWorkflowEngine::load_from_graph_json`] to populate.
    #[must_use]
    pub fn into_engine(self) -> ParallelWorkflowEngine {
        let mut engine = ParallelWorkflowEngine::new();
        engine.module_fetcher = self.module_fetcher;
        engine.event_sink = self.event_sink;
        engine.node_hook = self.node_hook;
        engine.graph_store = self.graph_store;
        engine.secrets_resolver = self.secrets_resolver;
        engine.expression_evaluator = self.expression_evaluator;
        engine.output_sanitizer = self.output_sanitizer;
        engine.retry_classifier = self.retry_classifier;
        engine.module_execution_store = self.module_execution_store;
        engine.approval_gate = self.approval_gate;
        engine.user_id = self.user_id;
        engine.actor_id = self.actor_id;
        engine.dry_run = self.dry_run;
        engine
    }
}

/// Parse a React-Flow node's retry metadata into a [`RetryPolicy`].
///
/// Accepts either top-level fields (`retry_count`, `retry_backoff_ms`,
/// `retry_condition`, `retry_delay_expression`) or the same keys nested
/// under `data` — the RF frontend emits both shapes depending on node
/// type. Returns `None` when the node has no retry config at all; the
/// engine treats that as "use the workflow-level default."
fn read_node_retry_policy(node: &JsonValue) -> Option<RetryPolicy> {
    let retry_count = node
        .get("retry_count")
        .or_else(|| node.get("data").and_then(|d| d.get("retry_count")))
        .and_then(JsonValue::as_u64)
        .map(|v| v as u32);
    let retry_backoff = node
        .get("retry_backoff_ms")
        .or_else(|| node.get("data").and_then(|d| d.get("retry_backoff_ms")))
        .and_then(JsonValue::as_u64);
    let retry_condition = node
        .get("retry_condition")
        .or_else(|| node.get("data").and_then(|d| d.get("retry_condition")))
        .and_then(JsonValue::as_str)
        .map(String::from);
    let retry_delay_expression = node
        .get("retry_delay_expression")
        .or_else(|| {
            node.get("data")
                .and_then(|d| d.get("retry_delay_expression"))
        })
        .and_then(JsonValue::as_str)
        .map(String::from);

    let has_any = retry_count.is_some()
        || retry_backoff.is_some()
        || retry_condition.is_some()
        || retry_delay_expression.is_some();
    if !has_any {
        return None;
    }
    Some(RetryPolicy {
        max_retries: retry_count.unwrap_or(2),
        backoff_ms: retry_backoff.unwrap_or(500),
        retry_condition,
        retry_delay_expression,
    })
}

/// Extract vault:// secret paths from a node config JSON object.
/// Returns the paths with the "vault://" prefix stripped.
///
/// Thin wrapper over `crate::vault_resolver::extract_vault_refs`
/// that drops the config-key side of each tuple. The engine doesn't need
/// per-key tracking because payload substitution happens in the worker
/// (via EncryptedSecrets), not the controller — see lines 5759-5781.
fn extract_vault_paths(config: &serde_json::Value) -> Vec<String> {
    crate::vault_resolver::extract_vault_refs(config)
        .into_iter()
        .map(|(_key, path)| path)
        .collect()
}

/// Run the full node-dispatch secret pipeline and return encrypted ciphertext.
///
/// This is the **one** place the pipeline lives. It's called by
/// [`ParallelWorkflowEngine::build_encrypted_secrets`] on `&self`-bound
/// paths and directly by dispatch closures (agent-loop body, ensemble
/// child, llm-dispatch target) that run under `async move` and can't
/// borrow `self`. Previously the pipeline was duplicated at four call
/// sites, and drift between copies has already caused one production
/// bug (loop-node secrets injection gap, fixed 2026-04-16).
///
/// Pipeline order — preserved across every caller to avoid silent
/// override differences between copies:
///
/// 1. Module-grant secrets for `node_id`.
/// 2. Statically-declared `extra_paths` (from `wasm_module.allowed_secrets`).
///    Empty slice for callers without a declared set.
/// 3. OAuth refresh hook on `vault_paths`.
/// 4. Dynamic `vault_paths` (extracted from node config). Overwrites any
///    overlapping keys from steps 1-2 because later writes win in `HashMap::extend`.
/// 5. Canonical LLM-provider keys for `user_id`.
/// 6. AES-256-GCM encrypt the combined map under `worker_shared_key`.
///
/// Errors at any resolve step are logged and the offending set is
/// skipped — the node still gets whatever secrets *did* resolve. If the
/// combined map is empty, the function returns
/// `EncryptedSecrets::default()` (empty ciphertext) rather than
/// encrypting an empty map.
pub(crate) async fn build_encrypted_secrets_for(
    resolver: &dyn SecretsResolver,
    node_id: Uuid,
    user_id: Option<Uuid>,
    vault_paths: &[String],
    extra_paths: &[String],
    worker_shared_key: &[u8],
) -> job_protocol::EncryptedSecrets {
    // 1. Module-grant secrets.
    let mut secrets_map = resolver
        .resolve_module_secrets(node_id)
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, %node_id, "resolve_module_secrets failed");
            Default::default()
        });

    // 2. Statically-declared extra paths (module's `allowed_secrets` list).
    if !extra_paths.is_empty() {
        match resolver.resolve_by_paths(extra_paths, user_id).await {
            Ok(declared) => secrets_map.extend(declared),
            Err(e) => tracing::warn!(error = %e, "Failed to fetch module declared secrets"),
        }
    }

    // 3-4. OAuth refresh + dynamic vault paths.
    if !vault_paths.is_empty() {
        resolver.refresh_vault_paths(vault_paths).await;
        match resolver.resolve_by_paths(vault_paths, user_id).await {
            Ok(v) => secrets_map.extend(v),
            Err(e) => tracing::error!(
                error = %e,
                ?vault_paths,
                %node_id,
                "Failed to pre-fetch vault:// secrets — node will fail"
            ),
        }
    }

    // 5. LLM-provider keys. Errors swallowed: a missing/broken LLM-key
    // vault shouldn't fail nodes that don't use llm::*.
    match resolver.resolve_llm_keys(user_id).await {
        Ok(keys) => secrets_map.extend(keys),
        Err(e) => tracing::debug!(
            error = %e,
            "Failed to pre-fetch LLM vault keys — worker will fall back to env vars"
        ),
    }

    // 6. Encrypt.
    if secrets_map.is_empty() {
        return job_protocol::EncryptedSecrets::default();
    }
    job_protocol::EncryptedSecrets::encrypt(&secrets_map, worker_shared_key).unwrap_or_default()
}

/// Validate config values against `pattern` constraints in the config_schema.
/// Returns Ok(()) if all valid, Err(message) if any pattern match fails.
pub fn validate_config_patterns(
    schema: &serde_json::Value,
    config: &serde_json::Value,
) -> Result<(), String> {
    let properties = match schema.get("properties").and_then(|p| p.as_object()) {
        Some(p) => p,
        None => return Ok(()), // No schema or no properties — skip validation
    };
    let config_obj = match config.as_object() {
        Some(o) => o,
        None => return Ok(()),
    };

    for (key, prop_schema) in properties {
        if let Some(pattern) = prop_schema.get("pattern").and_then(|p| p.as_str()) {
            if let Some(value) = config_obj.get(key).and_then(|v| v.as_str()) {
                match regex::Regex::new(pattern) {
                    Ok(re) => {
                        if !re.is_match(value) {
                            return Err(format!(
                                "Config key '{}' value does not match required pattern '{}'",
                                key, pattern
                            ));
                        }
                    }
                    Err(_) => {
                        tracing::warn!(key, pattern, "Invalid regex pattern in config_schema — skipping validation");
                    }
                }
            }
        }
    }
    Ok(())
}

/// Sanitize node output: cap individual string field lengths to prevent
/// unbounded LLM-generated outputs from consuming excessive memory when
/// cloned into downstream node inputs and the final aggregated result.
/// NOTE: `__` prefixed keys are intentionally NOT stripped — some are
/// needed internally (`__memory_write__`, `__fuel_consumed__`, etc.).
fn sanitize_node_output(output: &mut serde_json::Value) {
    const MAX_STRING_FIELD_BYTES: usize = 10240; // 10 KB per string field
    if let Some(obj) = output.as_object_mut() {
        for val in obj.values_mut() {
            if let Some(s) = val.as_str() {
                if s.len() > MAX_STRING_FIELD_BYTES {
                    *val = serde_json::Value::String(format!(
                        "{}...[truncated at {}B]",
                        &s[..MAX_STRING_FIELD_BYTES],
                        MAX_STRING_FIELD_BYTES
                    ));
                }
            }
        }
    }
}

impl ParallelWorkflowEngine {
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            node_map: HashMap::new(),
            node_labels: HashMap::new(),
            node_configs: HashMap::new(),
            module_fetcher: None,
            event_sink: None,
            node_hook: None,
            graph_store: None,
            secrets_resolver: None,
            user_id: None,
            node_meta: HashMap::new(),
            execution_timeout_secs: 300,
            rate_limits: HashMap::new(),
            node_timeouts: HashMap::new(),
            actor_id: None,
            actor_context: None,
            module_prefetch_cache: Arc::new(dashmap::DashMap::new()),
            sub_workflow_cache: HashMap::new(),
            dry_run: false,
            workflow_id: None,
            expression_evaluator: None,
            output_sanitizer: None,
            retry_classifier: None,
            module_execution_store: None,
            approval_gate: None,
        }
    }

    /// Replace the default approval gate. Out-of-tree consumers plug
    /// in their own impl (auto-approve for tests, a remote
    /// approval service for SaaS deployments).
    pub fn set_approval_gate(
        &mut self,
        gate: Arc<dyn workflow_engine_core::ApprovalGate>,
    ) {
        self.approval_gate = Some(gate);
    }

    /// Replace the default module-execution store. Out-of-tree
    /// consumers that don't have a Talos Postgres pool plug in their
    /// own impl (capture, append log, no-op) here.
    pub fn set_module_execution_store(
        &mut self,
        store: Arc<dyn workflow_engine_core::ModuleExecutionStore>,
    ) {
        self.module_execution_store = Some(store);
    }

    /// Replace the default module fetcher. Consumers plug in whatever
    /// backing store they prefer (Postgres catalog, OCI registry,
    /// in-memory test map) behind the [`ModuleFetcher`] trait. The
    /// Talos controller ships a `ModuleRegistry`-backed default via
    /// its `build_controller_engine` builder; direct users of this
    /// crate call `set_module_fetcher` themselves.
    pub fn set_module_fetcher(&mut self, fetcher: Arc<dyn ModuleFetcher>) {
        self.module_fetcher = Some(fetcher);
    }

    /// Replace the default execution-event sink. Tests use this to
    /// inject an in-memory capture or a no-op sink so dispatch does not
    /// depend on a Postgres pool. In-tree production callers using
    /// `with_services` / `with_registry` get a Postgres-backed default.
    pub fn set_event_sink(&mut self, sink: Arc<dyn EventSink>) {
        self.event_sink = Some(sink);
    }

    /// Replace the default post-completion hook. Tests use this to
    /// capture per-node outputs without exercising fuel rollup or
    /// actor-memory persistence.
    pub fn set_node_hook(&mut self, hook: Arc<dyn NodeLifecycleHook>) {
        self.node_hook = Some(hook);
    }

    /// Snapshot of this engine's policy adapters + user/actor context.
    /// Used by sub-workflow dispatch sites — clone the snapshot into an
    /// `async move` closure, then hydrate a fresh sub-engine inside the
    /// closure via [`AdapterSet::into_engine`].
    ///
    /// Every adapter is an `Arc`; cloning the set is a bounded number
    /// of refcount bumps (12 at most), not a deep copy. The set has no
    /// graph state — that's what
    /// [`load_from_graph_json`](Self::load_from_graph_json) is for.
    #[must_use]
    pub fn adapter_set(&self) -> AdapterSet {
        AdapterSet {
            module_fetcher: self.module_fetcher.clone(),
            event_sink: self.event_sink.clone(),
            node_hook: self.node_hook.clone(),
            graph_store: self.graph_store.clone(),
            secrets_resolver: self.secrets_resolver.clone(),
            expression_evaluator: self.expression_evaluator.clone(),
            output_sanitizer: self.output_sanitizer.clone(),
            retry_classifier: self.retry_classifier.clone(),
            module_execution_store: self.module_execution_store.clone(),
            approval_gate: self.approval_gate.clone(),
            user_id: self.user_id,
            actor_id: self.actor_id,
            dry_run: self.dry_run,
        }
    }

    /// Build a fresh engine that reuses this engine's policy adapters
    /// and user/actor context — `self.adapter_set().into_engine()` in
    /// one call. Use this on `&self` paths; for async-move closures
    /// that need multiple sub-engines, capture an [`AdapterSet`] and
    /// re-hydrate each iteration instead.
    #[must_use]
    pub fn new_subengine(&self) -> Self {
        self.adapter_set().into_engine()
    }

    /// Populate this engine's graph from a parsed React-Flow JSON
    /// value. Accepts `&Value` so callers holding a pre-parsed graph
    /// (cached sub-workflow map, [`WorkflowGraphStore`] return) don't
    /// pay a second `serde_json::from_str`; callers holding a raw
    /// string parse once at their boundary before calling.
    ///
    /// Optional `execution_timeout_secs` at the graph root overrides
    /// the default 300s timeout. Nodes with no resolvable
    /// `module_id` (non-UUID `type` and no `data.moduleId`) are
    /// silently skipped — the engine treats them as presentation-
    /// only annotations, matching the React-Flow frontend's
    /// behavior.
    ///
    /// This replaced the pre-extraction `from_graph_json` associated
    /// function that took `Arc<ModuleRegistry>` directly. Call sites
    /// now chain `self.new_subengine().load_from_graph_json(&g)?;`
    /// which decouples the engine from any single concrete adapter
    /// type.
    ///
    /// [`WorkflowGraphStore`]: workflow_engine_core::WorkflowGraphStore
    pub fn load_from_graph_json(&mut self, graph: &JsonValue) -> Result<(), String> {
        let empty_vec = vec![];
        let nodes = graph
            .get("nodes")
            .and_then(|n| n.as_array())
            .unwrap_or(&empty_vec);

        if let Some(timeout) = graph.get("execution_timeout_secs").and_then(JsonValue::as_u64) {
            self.execution_timeout_secs = timeout;
        }

        // Map React Flow node id → engine node UUID (unique per node, not
        // per module). `module_id` is stored as metadata so the engine
        // can load the right wasm at dispatch time.
        let mut rf_to_node: HashMap<String, Uuid> = HashMap::new();

        for node in nodes {
            let rf_id = node.get("id").and_then(JsonValue::as_str).unwrap_or("");
            let module_id_str = node
                .get("type")
                .and_then(JsonValue::as_str)
                .filter(|s| Uuid::parse_str(s).is_ok())
                .or_else(|| {
                    node.get("data")
                        .and_then(|d| d.get("moduleId"))
                        .and_then(JsonValue::as_str)
                });
            let Some(module_id_str) = module_id_str else {
                continue;
            };
            let Ok(module_id) = Uuid::parse_str(module_id_str) else {
                continue;
            };
            // If the RF node id is already a UUID, reuse it for
            // deterministic graph identity across loads. Otherwise
            // derive a stable UUID from the string via SHA-256.
            let node_id = Uuid::parse_str(rf_id).unwrap_or_else(|_| {
                use sha2::{Digest, Sha256};
                let hash = Sha256::digest(rf_id.as_bytes());
                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(&hash[..16]);
                Uuid::from_bytes(bytes)
            });
            rf_to_node.insert(rf_id.to_string(), node_id);
            self.node_labels.insert(node_id, rf_id.to_string());

            if let Some(data) = node.get("data").cloned() {
                if data.is_object() && !data.as_object().map(|m| m.is_empty()).unwrap_or(true) {
                    self.node_configs.insert(node_id, data);
                }
            }

            let retry_policy = read_node_retry_policy(node);
            self.add_node(node_id, Some(module_id), retry_policy, None);
        }

        let empty_edges = vec![];
        let edges = graph
            .get("edges")
            .and_then(JsonValue::as_array)
            .unwrap_or(&empty_edges);
        for edge in edges {
            let src_rf = edge.get("source").and_then(JsonValue::as_str).unwrap_or("");
            let tgt_rf = edge.get("target").and_then(JsonValue::as_str).unwrap_or("");
            let (Some(&src), Some(&tgt)) = (rf_to_node.get(src_rf), rf_to_node.get(tgt_rf)) else {
                continue;
            };
            let condition = edge
                .get("condition")
                .and_then(JsonValue::as_str)
                .map(String::from);
            let edge_type = edge
                .get("edge_type")
                .and_then(JsonValue::as_str)
                .unwrap_or("default")
                .to_string();
            let _ = self.add_edge(
                src,
                tgt,
                EdgeLogic {
                    source_handle: "output".to_string(),
                    target_handle: "input".to_string(),
                    mapping: None,
                    condition,
                    edge_type,
                },
            );
        }

        Ok(())
    }

    /// Replace the default graph store. Consumers plug in whatever
    /// backing store resolves sub-workflow graph JSON — Postgres,
    /// S3, an in-memory map for tests — behind the
    /// [`WorkflowGraphStore`] trait. The Talos controller ships a
    /// Postgres-backed default via `build_controller_engine`; direct
    /// users of this crate call this themselves.
    pub fn set_graph_store(&mut self, store: Arc<dyn WorkflowGraphStore>) {
        self.graph_store = Some(store);
    }

    /// Replace the default secrets resolver. Out-of-tree consumers that
    /// don't have a Talos `SecretsManager` plug in their own impl here.
    /// In-tree callers using `with_services` / `with_services_and_resolver`
    /// already have a default and don't need this.
    pub fn set_secrets_resolver(&mut self, resolver: Arc<dyn SecretsResolver>) {
        self.secrets_resolver = Some(resolver);
    }

    /// Replace the default expression evaluator (used for edge
    /// conditions, retry-delay expressions, and `Synthesize` node
    /// expressions). In production wraps a `rhai::Engine` with sandbox
    /// limits; tests plug in a no-op or a controlled mock.
    pub fn set_expression_evaluator(
        &mut self,
        evaluator: Arc<dyn workflow_engine_core::ExpressionEvaluator>,
    ) {
        self.expression_evaluator = Some(evaluator);
    }

    /// Replace the default output sanitizer (applied to stored node
    /// outputs + error messages before DB persist). In production wraps
    /// `talos_dlp` with a `DLP_PROVIDER=builtin | external | none`
    /// policy selector; tests can opt out via a passthrough impl.
    pub fn set_output_sanitizer(
        &mut self,
        sanitizer: Arc<dyn workflow_engine_core::OutputSanitizer>,
    ) {
        self.output_sanitizer = Some(sanitizer);
    }

    /// Replace the default retry classifier (maps dispatch error
    /// strings to a class tag + transient/permanent decision). In
    /// production wraps `retry_intelligence`'s heuristics.
    pub fn set_retry_classifier(
        &mut self,
        classifier: Arc<dyn workflow_engine_core::RetryClassifier>,
    ) {
        self.retry_classifier = Some(classifier);
    }

    /// Set the actor ID for __memory_write__ protocol write-back.
    pub fn set_actor_id(&mut self, id: Uuid) {
        self.actor_id = Some(id);
    }

    /// Set the owning user ID used for per-user secret resolution and
    /// module-artifact cache scoping. Controller-side builders set this
    /// automatically; out-of-tree consumers call it directly.
    pub fn set_user_id(&mut self, id: Uuid) {
        self.user_id = Some(id);
    }

    /// Snapshot of the configured event sink. Used by controller-side
    /// `run` wrappers that build a `NodeDispatcher` on the fly and need
    /// to thread the engine's sink through it.
    #[must_use]
    pub fn event_sink_arc(&self) -> Option<Arc<dyn EventSink>> {
        self.event_sink.clone()
    }

    /// Snapshot of the configured retry classifier.
    #[must_use]
    pub fn retry_classifier_arc(&self) -> Option<Arc<dyn workflow_engine_core::RetryClassifier>> {
        self.retry_classifier.clone()
    }

    /// Snapshot of the configured expression evaluator.
    #[must_use]
    pub fn expression_evaluator_arc(
        &self,
    ) -> Option<Arc<dyn workflow_engine_core::ExpressionEvaluator>> {
        self.expression_evaluator.clone()
    }

    // ── Thin shims over the configured trait objects ──────────────────
    //
    // These exist so engine-body call sites read as `self.eval_bool(...)`
    // instead of `self.expression_evaluator.as_ref().map(|e| e.eval_bool(...)).unwrap_or(false)`.
    // Each shim falls back to a "safe default" when the trait is unset:
    // - `eval_bool` → `false` (condition not satisfied)
    // - `eval_bool_with_error` → `Err("no evaluator")` so callers surface the misconfiguration
    // - `eval_json` → `Err("no evaluator")` likewise
    // - `eval_i64` → `None`
    // - `redact_str` / `redact_json` → passthrough (no scrubbing)
    // - `classify_error` / `is_transient_error` → `"unknown"` / `false`
    // In production every constructor (`with_registry`, `with_services*`)
    // wires these via `wire_default_policy_adapters`, so the fallbacks
    // never fire on real traffic; they're only for bare `new()` test engines.

    fn eval_bool(&self, expression: &str, context: &JsonValue) -> bool {
        self.expression_evaluator
            .as_ref()
            .map(|e| e.eval_bool(expression, context))
            .unwrap_or(false)
    }

    fn try_eval_bool(&self, expression: &str, context: &JsonValue) -> Result<bool, String> {
        self.expression_evaluator
            .as_ref()
            .ok_or_else(|| "no ExpressionEvaluator configured".to_string())?
            .try_eval_bool(expression, context)
            .map_err(|e| e.to_string())
    }

    fn eval_json(&self, expression: &str, context: &JsonValue) -> Result<JsonValue, String> {
        self.expression_evaluator
            .as_ref()
            .ok_or_else(|| "no ExpressionEvaluator configured".to_string())?
            .eval_json(expression, context)
            .map_err(|e| e.to_string())
    }

    fn redact_str(&self, s: &str) -> String {
        self.output_sanitizer
            .as_ref()
            .map(|sz| sz.redact_str(s))
            .unwrap_or_else(|| s.to_string())
    }

    fn redact_json(&self, v: &JsonValue) -> JsonValue {
        self.output_sanitizer
            .as_ref()
            .map(|sz| sz.redact_json(v))
            .unwrap_or_else(|| v.clone())
    }

    /// Build a per-run [`ExecutionSanitizer`] from this engine's
    /// configured output sanitizer. Returns `None` when no sanitizer
    /// is wired (bare test engines); call sites substitute the
    /// stateless `redact_str` in that case.
    ///
    /// [`ExecutionSanitizer`]: workflow_engine_core::ExecutionSanitizer
    fn new_execution_sanitizer(&self) -> Option<Box<dyn workflow_engine_core::ExecutionSanitizer>> {
        let sanitizer = self.output_sanitizer.as_ref()?;
        let configs: Vec<JsonValue> = self.node_configs.values().cloned().collect();
        Some(sanitizer.new_execution(&configs))
    }

    /// Set the parent workflow id. Threaded into
    /// [`NodeLifecycleHook::on_node_completed`] so cost rollups and
    /// other workflow-scoped observations attribute correctly. Unset
    /// engines fall back to the execution id (pre-extraction behavior),
    /// which still works but conflates per-run and per-workflow rollups.
    pub fn set_workflow_id(&mut self, id: Uuid) {
        self.workflow_id = Some(id);
    }

    /// Set actor memory context to be injected into every node's input.
    /// Called by the scheduler and trigger_workflow after loading actor memories.
    /// The value should be a JSON object with `actor_id` and `memories` fields.
    pub fn set_actor_context(&mut self, context: serde_json::Value) {
        self.actor_context = Some(context);
    }

    /// Enable dry-run mode: non-GET HTTP requests, webhooks, and messaging calls
    /// are mocked in the worker with success responses.
    pub fn set_dry_run(&mut self, v: bool) {
        self.dry_run = v;
    }

    /// Build encrypted secrets for a node dispatch.
    ///
    /// Thin wrapper around [`build_encrypted_secrets_for`] that sources
    /// `vault_paths` from the node's own config and has no additional
    /// declared paths. Prefer this form on call sites that hold `&self`.
    async fn build_encrypted_secrets(
        &self,
        node_id: Uuid,
        worker_shared_key: &Option<Arc<Vec<u8>>>,
    ) -> job_protocol::EncryptedSecrets {
        let (Some(resolver), Some(key)) = (self.secrets_resolver.as_ref(), worker_shared_key)
        else {
            return job_protocol::EncryptedSecrets::default();
        };
        let vault_paths = self
            .node_configs
            .get(&node_id)
            .map(|cfg| extract_vault_paths(cfg))
            .unwrap_or_default();
        build_encrypted_secrets_for(
            resolver.as_ref(),
            node_id,
            self.user_id,
            &vault_paths,
            &[],
            key,
        )
        .await
    }

    /// Collect all statically-known sub-workflow IDs from `node_meta` and batch-fetch
    /// their `graph_json` in a single query. Populates `self.sub_workflow_cache`.
    ///
    /// Called once at the start of `run()` / `run_with_seed()` to eliminate N+1 queries
    /// during node dispatch. Nodes whose workflow IDs are resolved at runtime
    /// (DynamicDispatch, CapabilityDispatch) will fall back to individual queries
    /// via `get_sub_workflow_graph()` on cache miss.
    async fn populate_sub_workflow_cache(&mut self) {
        let (store, user_id) = match (self.graph_store.as_ref(), self.user_id) {
            (Some(s), Some(u)) => (s, u),
            _ => return, // No graph store or no user_id — nothing to prefetch.
        };

        // Walk all node_meta entries and collect every referenced workflow UUID.
        let mut ids: HashSet<Uuid> = HashSet::new();
        for (_, _, kind) in self.node_meta.values() {
            match kind {
                Some(SystemNodeKind::SubWorkflow { workflow_id, .. }) => {
                    ids.insert(*workflow_id);
                }
                Some(SystemNodeKind::AgentLoop { body_workflow_id, .. }) => {
                    ids.insert(*body_workflow_id);
                }
                Some(SystemNodeKind::Judge { judge_workflow_id, .. }) => {
                    ids.insert(*judge_workflow_id);
                }
                Some(SystemNodeKind::Ensemble {
                    child_workflow_id,
                    judge_workflow_id,
                    ..
                }) => {
                    ids.insert(*child_workflow_id);
                    if let Some(jid) = judge_workflow_id {
                        ids.insert(*jid);
                    }
                }
                Some(SystemNodeKind::ReflectiveRetry {
                    child_workflow_id,
                    reflection_workflow_id,
                    ..
                }) => {
                    ids.insert(*child_workflow_id);
                    ids.insert(*reflection_workflow_id);
                }
                Some(SystemNodeKind::LlmDispatch {
                    classifier_workflow_id,
                    routes,
                    fallback_workflow_id,
                    ..
                }) => {
                    ids.insert(*classifier_workflow_id);
                    for wf_id in routes.values() {
                        ids.insert(*wf_id);
                    }
                    if let Some(fb) = fallback_workflow_id {
                        ids.insert(*fb);
                    }
                }
                Some(SystemNodeKind::ReActLoop { body_workflow_id, .. }) => {
                    ids.insert(*body_workflow_id);
                }
                _ => {}
            }
        }

        // Remove nil UUIDs (used as sentinel for missing workflow_id).
        ids.remove(&Uuid::nil());

        if ids.is_empty() {
            return;
        }

        let id_vec: Vec<Uuid> = ids.into_iter().collect();
        tracing::info!(
            count = id_vec.len(),
            "Populating sub-workflow cache with batch query"
        );

        let rows = match store.get_graphs(&id_vec, user_id).await {
            Ok(map) => map,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to batch-fetch sub-workflow graphs — falling back to per-node queries"
                );
                return;
            }
        };

        for (wf_id, graph_json) in rows {
            self.sub_workflow_cache.insert(wf_id, graph_json);
        }

        tracing::info!(
            cached = self.sub_workflow_cache.len(),
            "Sub-workflow cache populated"
        );
    }

    /// Look up a sub-workflow's graph JSON, checking the pre-populated cache first.
    /// Falls back to an individual DB query on cache miss (e.g., DynamicDispatch
    /// targets that are resolved at runtime).
    async fn get_sub_workflow_graph(
        &self,
        sub_wf_id: Uuid,
        user_id: Uuid,
    ) -> Option<JsonValue> {
        // Fast path: cache hit.
        if let Some(cached) = self.sub_workflow_cache.get(&sub_wf_id) {
            return Some(cached.clone());
        }
        // Cache miss — fall back to an individual query via the trait.
        tracing::debug!(
            workflow_id = %sub_wf_id,
            "Sub-workflow cache miss — falling back to individual query"
        );
        let store = self.graph_store.as_ref()?;
        match store.get_graph(sub_wf_id, user_id).await {
            Ok(g) => g,
            Err(e) => {
                tracing::warn!(error = %e, "sub-workflow graph query failed");
                None
            }
        }
    }

    /// Load a workflow graph from a JSON string (React Flow format).
    ///
    /// Parses nodes and edges from the JSON and populates the internal graph.
    pub async fn load_graph_from_json(&mut self, graph_json: &str) -> Result<(), String> {
        let graph: serde_json::Value =
            serde_json::from_str(graph_json).map_err(|e| format!("Invalid graph JSON: {}", e))?;

        let empty_vec = vec![];
        let nodes = graph
            .get("nodes")
            .and_then(|n| n.as_array())
            .unwrap_or(&empty_vec);

        if nodes.is_empty() {
            return Err("Workflow has no nodes".to_string());
        }

        // Map RF node ID → unique engine node UUID.
        // The node_id in the engine graph MUST be unique per node (not per module)
        // to allow the same module to be used in multiple nodes without creating
        // false cycle detections.
        let mut rf_to_node: HashMap<String, Uuid> = HashMap::new();

        for node in nodes {
            let rf_id = node.get("id").and_then(|v| v.as_str()).unwrap_or("");
            let module_id_str = node
                .get("type")
                .and_then(|v| v.as_str())
                .filter(|s| Uuid::parse_str(s).is_ok())
                .or_else(|| {
                    node.get("data")
                        .and_then(|d| d.get("moduleId"))
                        .and_then(|v| v.as_str())
                });
            if let Some(module_id_str) = module_id_str {
                if let Ok(module_id) = Uuid::parse_str(module_id_str) {
                    // Generate unique node ID: reuse RF ID if it's a UUID,
                    // otherwise derive a deterministic UUID from the RF ID string.
                    let node_id = Uuid::parse_str(rf_id).unwrap_or_else(|_| {
                        use sha2::{Digest, Sha256};
                        let hash = Sha256::digest(rf_id.as_bytes());
                        let mut bytes = [0u8; 16];
                        bytes.copy_from_slice(&hash[..16]);
                        Uuid::from_bytes(bytes)
                    });
                    rf_to_node.insert(rf_id.to_string(), node_id);
                    self.node_labels.insert(node_id, rf_id.to_string());

                    // Store node config from graph_json for use at dispatch time
                    if let Some(data) = node.get("data").cloned() {
                        if data.is_object()
                            && !data.as_object().map(|m| m.is_empty()).unwrap_or(true)
                        {
                            self.node_configs.insert(node_id, data.clone());
                        }
                        // Extract skip_condition into node_configs under __skip_condition
                        // Check data, config, and top-level node (handles all graph_json formats)
                        if let Some(skip_cond) = data
                            .get("skip_condition")
                            .and_then(|v| v.as_str())
                            .or_else(|| node.get("skip_condition").and_then(|v| v.as_str()))
                            .or_else(|| {
                                node.get("config")
                                    .and_then(|c| c.get("skip_condition"))
                                    .and_then(|v| v.as_str())
                            })
                        {
                            let entry = self
                                .node_configs
                                .entry(node_id)
                                .or_insert_with(|| serde_json::json!({}));
                            entry.as_object_mut().map(|m| {
                                m.insert(
                                    "__skip_condition".to_string(),
                                    serde_json::json!(skip_cond),
                                )
                            });
                        }
                        // Extract continue_on_error into node_configs under __continue_on_error.
                        // Check inside data first, then fall back to node top-level since
                        // add_node_to_workflow stores continue_on_error at the node's top level.
                        if data.get("continue_on_error").and_then(|v| v.as_bool()).unwrap_or(false)
                            || node
                                .get("continue_on_error")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false)
                        {
                            let entry = self
                                .node_configs
                                .entry(node_id)
                                .or_insert_with(|| serde_json::json!({}));
                            entry.as_object_mut().map(|m| {
                                m.insert("__continue_on_error".to_string(), serde_json::json!(true))
                            });
                        }
                    } else {
                        // Node has no "data" field — check top-level and config.skip_condition
                        if let Some(skip_cond) = node
                            .get("skip_condition")
                            .and_then(|v| v.as_str())
                            .or_else(|| {
                                node.get("config")
                                    .and_then(|c| c.get("skip_condition"))
                                    .and_then(|v| v.as_str())
                            })
                        {
                            let entry = self
                                .node_configs
                                .entry(node_id)
                                .or_insert_with(|| serde_json::json!({}));
                            entry.as_object_mut().map(|m| {
                                m.insert(
                                    "__skip_condition".to_string(),
                                    serde_json::json!(skip_cond),
                                )
                            });
                        }
                        // Extract continue_on_error (top-level or config)
                        if let Some(true) = node
                            .get("continue_on_error")
                            .and_then(|v| v.as_bool())
                            .or_else(|| {
                                node.get("config")
                                    .and_then(|c| c.get("continue_on_error"))
                                    .and_then(|v| v.as_bool())
                            })
                        {
                            let entry = self
                                .node_configs
                                .entry(node_id)
                                .or_insert_with(|| serde_json::json!({}));
                            entry.as_object_mut().map(|m| {
                                m.insert("__continue_on_error".to_string(), serde_json::json!(true))
                            });
                        }
                    }

                    let kind = node.get("kind").and_then(|k| k.as_str()).and_then(|k| {
                        if k == "foreach" {
                            let data = node.get("data")?;
                            Some(SystemNodeKind::ForEach {
                                input_path: data.get("input_path")?.as_str()?.to_string(),
                                output_handle: data.get("output_handle")?.as_str()?.to_string(),
                            })
                        } else if k == "wait" {
                            Some(SystemNodeKind::Wait {
                                message: node
                                    .get("data")
                                    .and_then(|d| d.get("message"))
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string()),
                            })
                        } else if k == "sub_workflow" {
                            let data = node.get("data")?;
                            Some(SystemNodeKind::SubWorkflow {
                                workflow_id: data.get("sub_workflow_id")?.as_str()?.parse().ok()?,
                                timeout_secs: data
                                    .get("timeout_secs")
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(30),
                            })
                        } else if k == "loop" {
                            let data = node.get("data")?;
                            Some(SystemNodeKind::Loop {
                                max_iterations: data
                                    .get("max_iterations")
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(10)
                                    .min(100)
                                    as u32,
                                condition: data.get("condition")?.as_str()?.to_string(),
                            })
                        } else if k == "collect" {
                            Some(SystemNodeKind::Collect)
                        } else if k == "synthesize" {
                            let data = node.get("data").cloned().unwrap_or(serde_json::json!({}));
                            Some(SystemNodeKind::Synthesize {
                                synthesis_expr: data
                                    .get("synthesis_expr")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string()),
                            })
                        } else if k == "verify" {
                            let data = node.get("data")?;
                            Some(SystemNodeKind::Verify {
                                condition: data.get("condition")?.as_str()?.to_string(),
                                check_label: data
                                    .get("check_label")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string()),
                                on_failure: data
                                    .get("on_failure")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("error")
                                    .to_string(),
                            })
                        } else if k == "agent_loop" {
                            let data = node.get("data")?;
                            Some(SystemNodeKind::AgentLoop {
                                body_workflow_id: data
                                    .get("body_workflow_id")?
                                    .as_str()?
                                    .parse()
                                    .ok()?,
                                max_iterations: data
                                    .get("max_iterations")
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(10)
                                    .min(50) as u32,
                                inject_history: data
                                    .get("inject_history")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(true),
                                timeout_secs: data
                                    .get("timeout_secs")
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(60),
                            })
                        } else if k == "dispatch" {
                            let data = node.get("data")?;
                            Some(SystemNodeKind::DynamicDispatch {
                                dispatch_expression: data
                                    .get("dispatch_expression")?
                                    .as_str()?
                                    .to_string(),
                                timeout_secs: data
                                    .get("timeout_secs")
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(30),
                            })
                        } else if k == "capability_dispatch" {
                            let data = node.get("data")?;
                            let caps = data
                                .get("required_capabilities")?
                                .as_array()?
                                .iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect::<Vec<String>>();
                            if caps.is_empty() {
                                return None;
                            }
                            Some(SystemNodeKind::CapabilityDispatch {
                                required_capabilities: caps,
                                timeout_secs: data
                                    .get("timeout_secs")
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(30),
                            })
                        } else if k == "judge" {
                            let data = node.get("data").unwrap_or(&serde_json::Value::Null);
                            let judge_workflow_id = data.get("judge_workflow_id")
                                .and_then(|v| v.as_str())
                                .and_then(|s| uuid::Uuid::parse_str(s).ok())
                                .unwrap_or_else(uuid::Uuid::nil);
                            let rubric = data.get("rubric").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let pass_threshold = data.get("pass_threshold").and_then(|v| v.as_f64());
                            let timeout_secs = data.get("timeout_secs").and_then(|v| v.as_u64()).unwrap_or(60);
                            Some(SystemNodeKind::Judge { judge_workflow_id, rubric, pass_threshold, timeout_secs })
                        } else if k == "ensemble" {
                            let data = node.get("data").unwrap_or(&serde_json::Value::Null);
                            let child_workflow_id = data.get("child_workflow_id")
                                .and_then(|v| v.as_str())
                                .and_then(|s| uuid::Uuid::parse_str(s).ok())
                                .unwrap_or_else(uuid::Uuid::nil);
                            let count = data.get("count").and_then(|v| v.as_u64()).unwrap_or(3).min(10).max(2) as u32;
                            let consensus = data.get("consensus").and_then(|v| v.as_str()).unwrap_or("majority_vote").to_string();
                            let judge_workflow_id = data.get("judge_workflow_id")
                                .and_then(|v| v.as_str())
                                .and_then(|s| uuid::Uuid::parse_str(s).ok());
                            let timeout_secs = data.get("timeout_secs").and_then(|v| v.as_u64()).unwrap_or(60);
                            Some(SystemNodeKind::Ensemble { child_workflow_id, count, consensus, judge_workflow_id, timeout_secs })
                        } else if k == "confidence_gate" {
                            let data = node.get("data").unwrap_or(&serde_json::Value::Null);
                            let threshold = data.get("threshold").and_then(|v| v.as_f64()).unwrap_or(0.7).clamp(0.0, 1.0);
                            let confidence_path = data.get("confidence_path")
                                .and_then(|v| v.as_str())
                                .unwrap_or("__confidence__")
                                .to_string();
                            let on_low_confidence = data.get("on_low_confidence")
                                .and_then(|v| v.as_str())
                                .unwrap_or("pause")
                                .to_string();
                            Some(SystemNodeKind::ConfidenceGate { threshold, confidence_path, on_low_confidence })
                        } else if k == "reflective_retry" {
                            let data = node.get("data").unwrap_or(&serde_json::Value::Null);
                            let child_workflow_id = data.get("child_workflow_id")
                                .and_then(|v| v.as_str())
                                .and_then(|s| uuid::Uuid::parse_str(s).ok())
                                .unwrap_or_else(uuid::Uuid::nil);
                            let reflection_workflow_id = data.get("reflection_workflow_id")
                                .and_then(|v| v.as_str())
                                .and_then(|s| uuid::Uuid::parse_str(s).ok())
                                .unwrap_or_else(uuid::Uuid::nil);
                            let max_retries = data.get("max_retries").and_then(|v| v.as_u64()).unwrap_or(2).min(5).max(1) as u32;
                            let timeout_secs = data.get("timeout_secs").and_then(|v| v.as_u64()).unwrap_or(60);
                            Some(SystemNodeKind::ReflectiveRetry { child_workflow_id, reflection_workflow_id, max_retries, timeout_secs })
                        } else if k == "llm_dispatch" {
                            let data = node.get("data").unwrap_or(&serde_json::Value::Null);
                            let classifier_workflow_id = data.get("classifier_workflow_id")
                                .and_then(|v| v.as_str())
                                .and_then(|s| uuid::Uuid::parse_str(s).ok())
                                .unwrap_or_else(uuid::Uuid::nil);
                            let routes: std::collections::HashMap<String, uuid::Uuid> = data
                                .get("routes")
                                .and_then(|v| v.as_object())
                                .map(|map| {
                                    map.iter()
                                        .filter_map(|(k, v)| {
                                            v.as_str()
                                                .and_then(|s| uuid::Uuid::parse_str(s).ok())
                                                .map(|uid| (k.clone(), uid))
                                        })
                                        .collect()
                                })
                                .unwrap_or_default();
                            let fallback_workflow_id = data.get("fallback_workflow_id")
                                .and_then(|v| v.as_str())
                                .and_then(|s| uuid::Uuid::parse_str(s).ok());
                            let timeout_secs = data.get("timeout_secs").and_then(|v| v.as_u64()).unwrap_or(60);
                            Some(SystemNodeKind::LlmDispatch { classifier_workflow_id, routes, fallback_workflow_id, timeout_secs })
                        } else {
                            None
                        }
                    });
                    // Extract per-node retry policy from graph_json
                    let retry_policy = {
                        let retry_count = node
                            .get("retry_count")
                            .or_else(|| node.get("data").and_then(|d| d.get("retry_count")))
                            .and_then(|v| v.as_u64())
                            .map(|v| v as u32);
                        let retry_backoff = node
                            .get("retry_backoff_ms")
                            .or_else(|| node.get("data").and_then(|d| d.get("retry_backoff_ms")))
                            .and_then(|v| v.as_u64());
                        let retry_condition = node
                            .get("retry_condition")
                            .or_else(|| node.get("data").and_then(|d| d.get("retry_condition")))
                            .and_then(|v| v.as_str())
                            .map(String::from);
                        let retry_delay_expression = node
                            .get("retry_delay_expression")
                            .or_else(|| {
                                node.get("data")
                                    .and_then(|d| d.get("retry_delay_expression"))
                            })
                            .and_then(|v| v.as_str())
                            .map(String::from);
                        let has_any = retry_count.is_some()
                            || retry_backoff.is_some()
                            || retry_condition.is_some()
                            || retry_delay_expression.is_some();
                        if has_any {
                            // Cap retries for workflows not linked to an actor budget.
                            // Without a budget ceiling, a near-fuel-exhausting module with
                            // retry_count=10 can saturate workers for 15+ seconds per trigger.
                            // Actor-owned executions may set up to their budget ceiling; the
                            // platform default hard cap is 3 for all unbudgeted executions.
                            const MAX_RETRIES_UNBUDGETED: u32 = 3;
                            let requested = retry_count.unwrap_or(2);
                            let max_retries = if self.actor_id.is_none() {
                                requested.min(MAX_RETRIES_UNBUDGETED)
                            } else {
                                requested
                            };
                            Some(RetryPolicy {
                                max_retries,
                                backoff_ms: retry_backoff.unwrap_or(500),
                                retry_condition,
                                retry_delay_expression,
                            })
                        } else {
                            None
                        }
                    };
                    self.add_node(node_id, Some(module_id), retry_policy, kind);
                    // Extract per-node execution timeout from graph_json
                    let node_timeout_secs: Option<u64> = node
                        .get("data")
                        .and_then(|d| d.get("timeout_secs"))
                        .or_else(|| node.get("timeout_secs"))
                        .and_then(|v| v.as_u64());
                    if let Some(t) = node_timeout_secs {
                        self.node_timeouts.insert(node_id, t);
                    }
                }
            } else if node
                .get("type")
                .and_then(|v| v.as_str())
                .map(|s| s.starts_with("system:"))
                .unwrap_or(false)
            {
                // System node (e.g. system:sub_workflow) — no module_id, but has a kind
                let node_id = Uuid::parse_str(rf_id).unwrap_or_else(|_| {
                    use sha2::{Digest, Sha256};
                    let hash = Sha256::digest(rf_id.as_bytes());
                    let mut bytes = [0u8; 16];
                    bytes.copy_from_slice(&hash[..16]);
                    Uuid::from_bytes(bytes)
                });
                rf_to_node.insert(rf_id.to_string(), node_id);
                self.node_labels.insert(node_id, rf_id.to_string());

                if let Some(data) = node.get("data").cloned() {
                    if data.is_object() && !data.as_object().map(|m| m.is_empty()).unwrap_or(true) {
                        self.node_configs.insert(node_id, data.clone());
                    }
                    // Extract skip_condition into node_configs under __skip_condition.
                    // Check inside data first, then fall back to node top-level.
                    if let Some(skip_cond) = data
                        .get("skip_condition")
                        .and_then(|v| v.as_str())
                        .or_else(|| node.get("skip_condition").and_then(|v| v.as_str()))
                    {
                        let entry = self
                            .node_configs
                            .entry(node_id)
                            .or_insert_with(|| serde_json::json!({}));
                        entry.as_object_mut().map(|m| {
                            m.insert("__skip_condition".to_string(), serde_json::json!(skip_cond))
                        });
                    }
                    // Extract continue_on_error into node_configs under __continue_on_error.
                    // Check inside data first, then fall back to node top-level since
                    // add_node_to_workflow stores continue_on_error at the node's top level.
                    if data.get("continue_on_error").and_then(|v| v.as_bool()).unwrap_or(false)
                        || node
                            .get("continue_on_error")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false)
                    {
                        let entry = self
                            .node_configs
                            .entry(node_id)
                            .or_insert_with(|| serde_json::json!({}));
                        entry.as_object_mut().map(|m| {
                            m.insert("__continue_on_error".to_string(), serde_json::json!(true))
                        });
                    }
                }

                // Derive kind from explicit "kind" field first, then fall back to the
                // "system:" type suffix (e.g. "system:collect" → "collect").
                // This handles nodes created by fix_fan_in which omit the "kind" field.
                let kind_str: Option<&str> =
                    node.get("kind").and_then(|k| k.as_str()).or_else(|| {
                        node.get("type")
                            .and_then(|t| t.as_str())
                            .and_then(|t| t.strip_prefix("system:"))
                    });
                let kind = kind_str.and_then(|k| {
                    if k == "sub_workflow" {
                        let data = node.get("data")?;
                        Some(SystemNodeKind::SubWorkflow {
                            workflow_id: data.get("sub_workflow_id")?.as_str()?.parse().ok()?,
                            timeout_secs: data
                                .get("timeout_secs")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(30),
                        })
                    } else if k == "loop" {
                        let data = node.get("data")?;
                        Some(SystemNodeKind::Loop {
                            max_iterations: data
                                .get("max_iterations")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(10)
                                .min(100) as u32,
                            condition: data.get("condition")?.as_str()?.to_string(),
                        })
                    } else if k == "collect" {
                        Some(SystemNodeKind::Collect)
                    } else if k == "synthesize" {
                        let data = node.get("data").cloned().unwrap_or(serde_json::json!({}));
                        Some(SystemNodeKind::Synthesize {
                            synthesis_expr: data
                                .get("synthesis_expr")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                        })
                    } else if k == "verify" {
                        let data = node.get("data")?;
                        Some(SystemNodeKind::Verify {
                            condition: data.get("condition")?.as_str()?.to_string(),
                            check_label: data
                                .get("check_label")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            on_failure: data
                                .get("on_failure")
                                .and_then(|v| v.as_str())
                                .unwrap_or("error")
                                .to_string(),
                        })
                    } else if k == "agent_loop" {
                        let data = node.get("data")?;
                        Some(SystemNodeKind::AgentLoop {
                            body_workflow_id: data
                                .get("body_workflow_id")?
                                .as_str()?
                                .parse()
                                .ok()?,
                            max_iterations: data
                                .get("max_iterations")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(10)
                                .min(50) as u32,
                            inject_history: data
                                .get("inject_history")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(true),
                            timeout_secs: data
                                .get("timeout_secs")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(60),
                        })
                    } else if k == "dispatch" {
                        let data = node.get("data")?;
                        Some(SystemNodeKind::DynamicDispatch {
                            dispatch_expression: data
                                .get("dispatch_expression")?
                                .as_str()?
                                .to_string(),
                            timeout_secs: data
                                .get("timeout_secs")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(30),
                        })
                    } else if k == "capability_dispatch" {
                        let data = node.get("data")?;
                        let caps = data
                            .get("required_capabilities")?
                            .as_array()?
                            .iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect::<Vec<String>>();
                        if caps.is_empty() {
                            return None;
                        }
                        Some(SystemNodeKind::CapabilityDispatch {
                            required_capabilities: caps,
                            timeout_secs: data
                                .get("timeout_secs")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(30),
                        })
                    } else if k == "judge" {
                        let data = node.get("data").unwrap_or(&serde_json::Value::Null);
                        let judge_workflow_id = data.get("judge_workflow_id")
                            .and_then(|v| v.as_str())
                            .and_then(|s| uuid::Uuid::parse_str(s).ok())
                            .unwrap_or_else(uuid::Uuid::nil);
                        let rubric = data.get("rubric").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let pass_threshold = data.get("pass_threshold").and_then(|v| v.as_f64());
                        let timeout_secs = data.get("timeout_secs").and_then(|v| v.as_u64()).unwrap_or(60);
                        Some(SystemNodeKind::Judge { judge_workflow_id, rubric, pass_threshold, timeout_secs })
                    } else if k == "ensemble" {
                        let data = node.get("data").unwrap_or(&serde_json::Value::Null);
                        let child_workflow_id = data.get("child_workflow_id")
                            .and_then(|v| v.as_str())
                            .and_then(|s| uuid::Uuid::parse_str(s).ok())
                            .unwrap_or_else(uuid::Uuid::nil);
                        let count = data.get("count").and_then(|v| v.as_u64()).unwrap_or(3).min(10).max(2) as u32;
                        let consensus = data.get("consensus").and_then(|v| v.as_str()).unwrap_or("majority_vote").to_string();
                        let judge_workflow_id = data.get("judge_workflow_id")
                            .and_then(|v| v.as_str())
                            .and_then(|s| uuid::Uuid::parse_str(s).ok());
                        let timeout_secs = data.get("timeout_secs").and_then(|v| v.as_u64()).unwrap_or(60);
                        Some(SystemNodeKind::Ensemble { child_workflow_id, count, consensus, judge_workflow_id, timeout_secs })
                    } else if k == "confidence_gate" {
                        let data = node.get("data").unwrap_or(&serde_json::Value::Null);
                        let threshold = data.get("threshold").and_then(|v| v.as_f64()).unwrap_or(0.7).clamp(0.0, 1.0);
                        let confidence_path = data.get("confidence_path")
                            .and_then(|v| v.as_str())
                            .unwrap_or("__confidence__")
                            .to_string();
                        let on_low_confidence = data.get("on_low_confidence")
                            .and_then(|v| v.as_str())
                            .unwrap_or("pause")
                            .to_string();
                        Some(SystemNodeKind::ConfidenceGate { threshold, confidence_path, on_low_confidence })
                    } else if k == "reflective_retry" {
                        let data = node.get("data").unwrap_or(&serde_json::Value::Null);
                        let child_workflow_id = data.get("child_workflow_id")
                            .and_then(|v| v.as_str())
                            .and_then(|s| uuid::Uuid::parse_str(s).ok())
                            .unwrap_or_else(uuid::Uuid::nil);
                        let reflection_workflow_id = data.get("reflection_workflow_id")
                            .and_then(|v| v.as_str())
                            .and_then(|s| uuid::Uuid::parse_str(s).ok())
                            .unwrap_or_else(uuid::Uuid::nil);
                        let max_retries = data.get("max_retries").and_then(|v| v.as_u64()).unwrap_or(2).min(5).max(1) as u32;
                        let timeout_secs = data.get("timeout_secs").and_then(|v| v.as_u64()).unwrap_or(60);
                        Some(SystemNodeKind::ReflectiveRetry { child_workflow_id, reflection_workflow_id, max_retries, timeout_secs })
                    } else if k == "llm_dispatch" {
                        let data = node.get("data").unwrap_or(&serde_json::Value::Null);
                        let classifier_workflow_id = data.get("classifier_workflow_id")
                            .and_then(|v| v.as_str())
                            .and_then(|s| uuid::Uuid::parse_str(s).ok())
                            .unwrap_or_else(uuid::Uuid::nil);
                        let routes: std::collections::HashMap<String, uuid::Uuid> = data
                            .get("routes")
                            .and_then(|v| v.as_object())
                            .map(|map| {
                                map.iter()
                                    .filter_map(|(k, v)| {
                                        v.as_str()
                                            .and_then(|s| uuid::Uuid::parse_str(s).ok())
                                            .map(|uid| (k.clone(), uid))
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        let fallback_workflow_id = data.get("fallback_workflow_id")
                            .and_then(|v| v.as_str())
                            .and_then(|s| uuid::Uuid::parse_str(s).ok());
                        let timeout_secs = data.get("timeout_secs").and_then(|v| v.as_u64()).unwrap_or(60);
                        Some(SystemNodeKind::LlmDispatch { classifier_workflow_id, routes, fallback_workflow_id, timeout_secs })
                    } else {
                        None
                    }
                });
                self.add_node(node_id, None, None, kind);
            }
        }

        let empty_edges = vec![];
        let edges = graph
            .get("edges")
            .and_then(|e| e.as_array())
            .unwrap_or(&empty_edges);

        for edge in edges {
            let src_rf = edge.get("source").and_then(|v| v.as_str()).unwrap_or("");
            let tgt_rf = edge.get("target").and_then(|v| v.as_str()).unwrap_or("");
            if let (Some(&src), Some(&tgt)) = (rf_to_node.get(src_rf), rf_to_node.get(tgt_rf)) {
                let _ = self.add_edge(
                    src,
                    tgt,
                    EdgeLogic {
                        source_handle: edge
                            .get("sourceHandle")
                            .and_then(|v| v.as_str())
                            .unwrap_or("output")
                            .to_string(),
                        target_handle: edge
                            .get("targetHandle")
                            .and_then(|v| v.as_str())
                            .unwrap_or("input")
                            .to_string(),
                        mapping: None,
                        condition: edge
                            .get("condition")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        edge_type: edge
                            .get("edge_type")
                            .and_then(|v| v.as_str())
                            .unwrap_or("default")
                            .to_string(),
                    },
                );
            }
        }

        // Batch-load rate limits for all module IDs referenced in this graph.
        if let Some(ref fetcher) = self.module_fetcher {
            let module_ids: Vec<Uuid> = self
                .node_meta
                .values()
                .filter_map(|(mid, _, _)| *mid)
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();

            if !module_ids.is_empty() {
                let rate_limits = fetcher.load_rate_limits(&module_ids).await;
                for (id, limit) in rate_limits {
                    self.rate_limits.insert(id, limit);
                }
                if !self.rate_limits.is_empty() {
                    tracing::info!(
                        rate_limited_modules = self.rate_limits.len(),
                        "Loaded module rate limits for workflow",
                    );
                }
            }
        }

        // Batch-fetch all sub-workflow graphs referenced by system nodes.
        // This eliminates N+1 queries during node dispatch in run()/run_with_seed().
        self.populate_sub_workflow_cache().await;

        Ok(())
    }

    // Checkpoint load moved to `workflow_engine_core::CheckpointStore`.
    // Callers that used to invoke `engine.load_checkpoint(execution_id, &pool)`
    // now construct a `ControllerCheckpointStore` and call `store.load(id)`
    // directly, then feed the result into `run_with_seed`.

    /// Extract module UUIDs referenced in a graph_json string.
    ///
    /// Used to maintain the `workflow_module_refs` junction table.
    pub fn extract_module_ids(graph_json: &str) -> Vec<Uuid> {
        let graph: serde_json::Value = match serde_json::from_str(graph_json) {
            Ok(v) => v,
            Err(_) => return vec![],
        };

        let empty_vec = vec![];
        let nodes = graph
            .get("nodes")
            .and_then(|n| n.as_array())
            .unwrap_or(&empty_vec);

        let mut module_ids = Vec::new();
        for node in nodes {
            let module_id_str = node
                .get("type")
                .and_then(|v| v.as_str())
                .filter(|s| Uuid::parse_str(s).is_ok())
                .or_else(|| {
                    node.get("data")
                        .and_then(|d| d.get("moduleId"))
                        .and_then(|v| v.as_str())
                });
            if let Some(id_str) = module_id_str {
                if let Ok(uuid) = Uuid::parse_str(id_str) {
                    module_ids.push(uuid);
                }
            }
        }
        module_ids.sort();
        module_ids.dedup();
        module_ids
    }

    /// Maximum number of nodes allowed in a single workflow graph.
    /// Prevents unbounded resource consumption from malformed or adversarial workflows.
    const MAX_WORKFLOW_NODES: usize = 500;

    /// Resolve the actual module UUID for a node.
    /// Nodes have their own unique IDs in the graph; the module_id (which WASM to load)
    /// is stored in node_meta. Falls back to node_id for backwards compatibility.
    fn resolve_module_id(&self, node_id: Uuid) -> Uuid {
        self.node_meta
            .get(&node_id)
            .and_then(|(mid, _, _)| *mid)
            .unwrap_or(node_id)
    }

    pub fn add_node(
        &mut self,
        id: Uuid,
        module_id: Option<Uuid>,
        retry_policy: Option<workflow_engine_core::RetryPolicy>,
        kind: Option<SystemNodeKind>,
    ) {
        if self.graph.node_count() >= Self::MAX_WORKFLOW_NODES {
            tracing::warn!(
                node_count = self.graph.node_count(),
                max = Self::MAX_WORKFLOW_NODES,
                "Workflow graph exceeds maximum node count — ignoring add_node"
            );
            return;
        }
        let idx = self.graph.add_node(id);
        self.node_map.insert(id, idx);
        self.node_meta.insert(id, (module_id, retry_policy, kind));
    }

    #[allow(dead_code)]
    pub fn add_edge(&mut self, from: Uuid, to: Uuid, logic: EdgeLogic) -> Result<(), String> {
        let from_idx = *self
            .node_map
            .get(&from)
            .ok_or_else(|| format!("Edge source node {} not found", from))?;
        let to_idx = *self
            .node_map
            .get(&to)
            .ok_or_else(|| format!("Edge target node {} not found", to))?;
        self.graph.add_edge(from_idx, to_idx, logic);
        Ok(())
    }

    /// Unwrap engine wrapper from node output if present.
    /// Templates receive `{"config": ..., "input": ...}` and many echo it back.
    /// For inter-node data flow, we want the raw payload, not the engine wrapper.
    /// Collapse a completed sub-workflow's per-node results into a single output value.
    ///
    /// All sub-workflow invocation sites (judge, reflective-retry, ensemble, sub_workflow)
    /// need the same semantics; authoring a sub-workflow whose output is a shaped record
    /// (e.g. judge returning `{score, passed, reasoning, feedback}`) should "just work"
    /// regardless of how the sub-workflow graph is wired internally.
    ///
    /// Rules:
    /// - Nodes marked `__skipped` are dropped.
    /// - The synthetic `__trigger__` node is dropped.
    /// - Each remaining output is passed through `unwrap_output` to strip the engine
    ///   `{input, config, ...}` envelope.
    /// - If exactly one **terminal** node remains (a node with no outgoing edges inside
    ///   the sub-graph), its unwrapped output IS the collapsed value. Callers see the
    ///   record shape their sub-workflow returns, not a `{node_label: {...}}` wrap.
    /// - Otherwise (zero terminals, which means the graph is cyclic or empty, or
    ///   multiple terminals — a diamond without an explicit aggregator), fall back to a
    ///   label-keyed map so callers can still reach individual branches via
    ///   `output[label]`. Node-label collisions are deterministically resolved by
    ///   preferring terminal nodes (so shadowing a non-terminal is explicit).
    /// One-shot dispatch of an Ensemble system node.
    ///
    /// Runs `child_wf_id` `run_count` times with the same input, then applies
    /// the consensus strategy to pick a winner:
    /// - `first_pass`: first non-error result.
    /// - `best_of_n`: requires `judge_wf_id_opt`; scores each candidate via the
    ///   judge workflow and picks the highest score.
    /// - anything else ("majority_vote" / default): most common value at
    ///   `result`/`output` key (with an 8 KiB vote-key cap to bound memory).
    ///
    /// Output is enriched with `__ensemble_method__` and `__ensemble_size__`.
    pub async fn dispatch_ensemble(
        &self,
        inputs: JsonValue,
        child_wf_id: Uuid,
        run_count: u32,
        consensus_strategy: String,
        judge_wf_id_opt: Option<Uuid>,
        dispatcher: Arc<dyn workflow_engine_core::NodeDispatcher>,
        worker_shared_key: Option<Arc<Vec<u8>>>,
    ) -> JsonValue {
        let clean_input = if let Some(obj) = inputs.as_object() {
            let mut cleaned = obj.clone();
            cleaned.retain(|k, _| !k.starts_with("__"));
            serde_json::Value::Object(cleaned)
        } else {
            inputs
        };

        // 1. Run child workflow N times sequentially.
        let mut all_results: Vec<JsonValue> = Vec::with_capacity(run_count as usize);
        for _i in 0..run_count {
            let out = match self
                .execute_subworkflow_graph(
                    child_wf_id,
                    clean_input.clone(),
                    dispatcher.clone(),
                    worker_shared_key.clone(),
                )
                .await
            {
                Ok(v) => v,
                Err(e) => e.into_error_envelope("Ensemble child"),
            };
            all_results.push(out);
        }

        // 2. Pick a winner via the consensus strategy.
        let consensus_output: JsonValue = match consensus_strategy.as_str() {
            "first_pass" => all_results
                .iter()
                .find(|r| !r.get("__error").and_then(|v| v.as_bool()).unwrap_or(false))
                .cloned()
                .unwrap_or_else(|| {
                    all_results.first().cloned().unwrap_or_else(|| {
                        serde_json::json!({
                            "__error": true,
                            "error_message": "All ensemble runs failed"
                        })
                    })
                }),
            "best_of_n" if judge_wf_id_opt.is_some() => {
                let judge_wf_id = judge_wf_id_opt.unwrap();
                let mut best_result: Option<JsonValue> = None;
                let mut best_score = f64::NEG_INFINITY;
                for candidate in &all_results {
                    if candidate.get("__error").and_then(|v| v.as_bool()).unwrap_or(false) {
                        continue;
                    }
                    let judge_input = serde_json::json!({ "content": candidate, "rubric": "" });
                    if let Ok(collapsed) = self
                        .execute_subworkflow_graph(
                            judge_wf_id,
                            judge_input,
                            dispatcher.clone(),
                            worker_shared_key.clone(),
                        )
                        .await
                    {
                        let verdict = JudgeVerdict::from_collapsed(&collapsed);
                        if verdict.score > best_score {
                            best_score = verdict.score;
                            best_result = Some(candidate.clone());
                        }
                    }
                }
                let chosen = best_result.unwrap_or_else(|| {
                    all_results.first().cloned().unwrap_or_else(|| {
                        serde_json::json!({
                            "__error": true,
                            "error_message": "All best_of_n candidates failed"
                        })
                    })
                });
                Self::emit_quality_gate_event(
                    "ensemble_best_of_n",
                    best_score > f64::NEG_INFINITY,
                    if best_score > f64::NEG_INFINITY { Some(best_score) } else { None },
                    Some(run_count),
                    None,
                );
                chosen
            }
            _ => {
                // majority_vote: find most common value at result["result"] or result["output"].
                // Vote-key is capped at 8 KiB to bound memory when candidates are huge.
                let mut vote_counts: std::collections::HashMap<String, (usize, JsonValue)> =
                    std::collections::HashMap::new();
                const MAX_VOTE_KEY_BYTES: usize = 8_192;
                for r in &all_results {
                    if r.get("__error").and_then(|v| v.as_bool()).unwrap_or(false) {
                        continue;
                    }
                    let key_val = {
                        let s = r
                            .get("result")
                            .or_else(|| r.get("output"))
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| r.to_string());
                        if s.len() > MAX_VOTE_KEY_BYTES {
                            s[..MAX_VOTE_KEY_BYTES].to_string()
                        } else {
                            s
                        }
                    };
                    let entry = vote_counts.entry(key_val).or_insert((0, r.clone()));
                    entry.0 += 1;
                }
                vote_counts
                    .into_iter()
                    .max_by_key(|(_, (count, _))| *count)
                    .map(|(_, (_, best))| best)
                    .unwrap_or_else(|| {
                        all_results.first().cloned().unwrap_or_else(|| {
                            serde_json::json!({
                                "__error": true,
                                "error_message": "Ensemble majority_vote: all runs failed"
                            })
                        })
                    })
            }
        };

        // 3. Annotate with ensemble metadata.
        let mut out = if let Some(obj) = consensus_output.as_object() {
            obj.clone()
        } else {
            serde_json::Map::new()
        };
        out.insert(
            "__ensemble_method__".to_string(),
            serde_json::json!(consensus_strategy),
        );
        out.insert("__ensemble_size__".to_string(), serde_json::json!(run_count));
        serde_json::Value::Object(out)
    }

    /// One-shot dispatch of a LlmDispatch system node.
    ///
    /// Flow:
    /// 1. Run `classifier_wf_id` with the inbound inputs (stripped of `__*`).
    /// 2. Extract a class string from the classifier output (top-level
    ///    `class`, `output`, or `result` keys — whichever is present).
    /// 3. If the class matches a key in `routes`, run that route's workflow
    ///    with the same input. Otherwise run `fallback_wf_id` (if set),
    ///    passing the unmatched class as `__unmatched_class__`.
    ///
    /// The returned output always carries `__dispatched_class__` and
    /// `__dispatched_workflow_id__` for trace observability.
    pub async fn dispatch_llm_dispatch(
        &self,
        inputs: JsonValue,
        classifier_wf_id: Uuid,
        routes: std::collections::HashMap<String, Uuid>,
        fallback_wf_id: Option<Uuid>,
        dispatcher: Arc<dyn workflow_engine_core::NodeDispatcher>,
        worker_shared_key: Option<Arc<Vec<u8>>>,
    ) -> JsonValue {
        let clean_input = if let Some(obj) = inputs.as_object() {
            let mut cleaned = obj.clone();
            cleaned.retain(|k, _| !k.starts_with("__"));
            serde_json::Value::Object(cleaned)
        } else {
            inputs
        };

        // 1. Run classifier. Distinguish 3 failure modes rather than
        // collapsing them into a single "empty class" message:
        //   a) classifier sub-workflow itself failed (DB, build, exec error)
        //   b) classifier ran but returned no recognised class field
        //   c) classifier ran and returned an empty string
        let class_str = match self
            .execute_subworkflow_graph(
                classifier_wf_id,
                clean_input.clone(),
                dispatcher.clone(),
                worker_shared_key.clone(),
            )
            .await
        {
            Ok(out) => {
                let raw = out
                    .get("class")
                    .or_else(|| out.get("output"))
                    .or_else(|| out.get("result"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                match raw {
                    None => {
                        let keys: Vec<&String> = out
                            .as_object()
                            .map(|m| m.keys().collect())
                            .unwrap_or_default();
                        return serde_json::json!({
                            "__error": true,
                            "error_message": format!(
                                "LlmDispatch classifier output had no 'class', 'output', or 'result' \
                                 string field (saw keys: {:?}). The classifier sub-workflow must return \
                                 a string class label.",
                                keys
                            ),
                        });
                    }
                    Some(s) if s.is_empty() => {
                        return serde_json::json!({
                            "__error": true,
                            "error_message":
                                "LlmDispatch classifier returned an empty class string — \
                                 the classifier must produce a non-empty label.",
                        });
                    }
                    Some(s) => s,
                }
            }
            Err(e) => {
                // Preserve the classifier sub-workflow error detail under a
                // context-specific label so the caller can tell the difference
                // between "classifier failed" and "classifier returned bad data".
                return e.into_error_envelope("LlmDispatch classifier");
            }
        };

        // 2. Resolve the target workflow from routes or fallback.
        let (target_wf_id, input_for_target, is_fallback) = match routes.get(&class_str) {
            Some(&target) => (target, clean_input, false),
            None => match fallback_wf_id {
                Some(fb) => {
                    let mut fb_input = if let Some(obj) = clean_input.as_object() {
                        obj.clone()
                    } else {
                        serde_json::Map::new()
                    };
                    fb_input.insert(
                        "__unmatched_class__".to_string(),
                        serde_json::json!(class_str),
                    );
                    (fb, serde_json::Value::Object(fb_input), true)
                }
                None => {
                    let route_keys: Vec<&String> = routes.keys().collect();
                    return serde_json::json!({
                        "__error": true,
                        "error_message": format!(
                            "LLM dispatch: class '{}' not in routes {:?}",
                            class_str, route_keys
                        )
                    });
                }
            },
        };

        // 3. Execute the target workflow and annotate the result.
        let context_label = if is_fallback { "LlmDispatch fallback" } else { "LlmDispatch target" };
        match self
            .execute_subworkflow_graph(
                target_wf_id,
                input_for_target,
                dispatcher,
                worker_shared_key,
            )
            .await
        {
            Ok(target_out) => {
                let mut out = if let Some(obj) = target_out.as_object() {
                    obj.clone()
                } else {
                    let mut m = serde_json::Map::new();
                    m.insert("output".to_string(), target_out);
                    m
                };
                out.insert(
                    "__dispatched_class__".to_string(),
                    serde_json::json!(class_str),
                );
                out.insert(
                    "__dispatched_workflow_id__".to_string(),
                    serde_json::json!(target_wf_id.to_string()),
                );
                serde_json::Value::Object(out)
            }
            Err(e) => e.into_error_envelope(context_label),
        }
    }

    /// One-shot dispatch of a ReflectiveRetry system node.
    ///
    /// Runs `child_wf_id` up to `max_retries` times. After each failure,
    /// invokes `reflection_wf_id` with `{input, error, attempt}`. The
    /// reflection workflow's returned fields are merged (non-`__` keys only)
    /// back into the child's input for the next attempt — the child adapts
    /// instead of blindly re-running identical input.
    ///
    /// Returns the child's collapsed terminal output enriched with
    /// `__reflective_retry_attempts__` on success, or an error envelope on
    /// exhaustion.
    pub async fn dispatch_reflective_retry(
        &self,
        initial_input: JsonValue,
        child_wf_id: Uuid,
        reflection_wf_id: Uuid,
        max_retries: u32,
        dispatcher: Arc<dyn workflow_engine_core::NodeDispatcher>,
        worker_shared_key: Option<Arc<Vec<u8>>>,
    ) -> JsonValue {
        let mut current_input = initial_input;
        let mut last_error = String::new();

        for attempt in 1..=max_retries {
            let clean_input = if let Some(obj) = current_input.as_object() {
                let mut c = obj.clone();
                c.retain(|k, _| !k.starts_with("__"));
                serde_json::Value::Object(c)
            } else {
                current_input.clone()
            };

            let child_out = match self
                .execute_subworkflow_graph(
                    child_wf_id,
                    clean_input.clone(),
                    dispatcher.clone(),
                    worker_shared_key.clone(),
                )
                .await
            {
                Ok(v) => v,
                Err(e) => e.into_error_envelope("ReflectiveRetry child"),
            };

            if !child_out.get("__error").and_then(|v| v.as_bool()).unwrap_or(false) {
                Self::emit_quality_gate_event(
                    "reflective_retry",
                    true,
                    None,
                    Some(attempt),
                    None,
                );
                let mut out = if let Some(obj) = child_out.as_object() {
                    obj.clone()
                } else {
                    let mut m = serde_json::Map::new();
                    m.insert("output".to_string(), child_out.clone());
                    m
                };
                out.insert(
                    "__reflective_retry_attempts__".to_string(),
                    serde_json::json!(attempt),
                );
                return serde_json::Value::Object(out);
            }

            last_error = child_out
                .get("error_message")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error")
                .to_string();

            if attempt < max_retries {
                let reflect_input = serde_json::json!({
                    "input": clean_input,
                    "error": last_error,
                    "attempt": attempt,
                });
                if let Ok(reflection_out) = self
                    .execute_subworkflow_graph(
                        reflection_wf_id,
                        reflect_input,
                        dispatcher.clone(),
                        worker_shared_key.clone(),
                    )
                    .await
                {
                    let mut merged = if let Some(obj) = current_input.as_object() {
                        obj.clone()
                    } else {
                        serde_json::Map::new()
                    };
                    if let Some(obj) = reflection_out.as_object() {
                        for (k, v) in obj {
                            if !k.starts_with("__") {
                                merged.insert(k.clone(), v.clone());
                            }
                        }
                    }
                    current_input = serde_json::Value::Object(merged);
                }
            }
        }

        Self::emit_quality_gate_event(
            "reflective_retry",
            false,
            None,
            Some(max_retries),
            Some("exhausted"),
        );
        serde_json::json!({
            "__error": true,
            "error_message": format!(
                "Reflective retry exhausted {} attempts. Last error: {}",
                max_retries, last_error
            ),
        })
    }

    /// One-shot dispatch of a SubWorkflow system node.
    ///
    /// Strips engine metadata (`__*`) from the inbound parent inputs before
    /// passing as the sub-workflow trigger, then returns the collapsed
    /// terminal output (single-terminal workflows flatten to their leaf
    /// output; multi-terminal fall back to label-keyed map).
    pub async fn dispatch_subworkflow(
        &self,
        inputs: JsonValue,
        sub_wf_id: Uuid,
        dispatcher: Arc<dyn workflow_engine_core::NodeDispatcher>,
        worker_shared_key: Option<Arc<Vec<u8>>>,
    ) -> JsonValue {
        // Strip internal metadata keys so sub-workflow input doesn't carry
        // engine internals (`__trigger_input__`, `__fuel_consumed__`, …).
        let clean_input = if let Some(obj) = inputs.as_object() {
            let mut cleaned = obj.clone();
            cleaned.retain(|k, _| !k.starts_with("__"));
            serde_json::Value::Object(cleaned)
        } else {
            inputs
        };
        match self
            .execute_subworkflow_graph(sub_wf_id, clean_input, dispatcher, worker_shared_key)
            .await
        {
            Ok(collapsed) => collapsed,
            Err(e) => {
                tracing::error!(sub_workflow_id = %sub_wf_id, error = ?e, "Sub-workflow execution failed");
                e.into_error_envelope("Sub-workflow")
            }
        }
    }

    /// Emit a `target: "talos_engine"` event for a quality-gate outcome.
    ///
    /// Structured telemetry for judge / reflective-retry / ensemble so operators
    /// can answer "what's our judge pass rate?" and "how often does reflection
    /// rescue a failing child?" without plumbing custom metrics per-workflow.
    fn emit_quality_gate_event(
        kind: &'static str,
        passed: bool,
        score: Option<f64>,
        attempts: Option<u32>,
        extra: Option<&str>,
    ) {
        tracing::info!(
            target: "talos_engine",
            event_kind = "quality_gate",
            gate = kind,
            passed = passed,
            score = score,
            attempts = attempts,
            extra = extra,
            "quality gate completed"
        );
    }

    /// One-shot dispatch of a Judge system node. Builds the judge input from
    /// `parent_inputs`, runs the judge sub-workflow, parses the verdict, and
    /// returns the final output envelope that the outer loop will insert into
    /// the results map.
    ///
    /// Shared by the `run` and `run_with_seed` dispatch loops — both previously
    /// inlined ~100 lines of near-identical logic here.
    pub async fn dispatch_judge(
        &self,
        parent_inputs: JsonValue,
        judge_wf_id: Uuid,
        rubric: String,
        pass_threshold: Option<f64>,
        dispatcher: Arc<dyn workflow_engine_core::NodeDispatcher>,
        worker_shared_key: Option<Arc<Vec<u8>>>,
    ) -> JsonValue {
        let judge_input = serde_json::json!({
            "content": &parent_inputs,
            "rubric": rubric,
        });
        match self
            .execute_subworkflow_graph(judge_wf_id, judge_input, dispatcher, worker_shared_key)
            .await
        {
            Ok(collapsed) => {
                let verdict = JudgeVerdict::from_collapsed(&collapsed);
                let JudgeVerdict {
                    score, passed: passed_raw, reasoning, feedback, malformed_field_count,
                } = verdict;
                let passed = if let Some(threshold) = pass_threshold {
                    passed_raw && score >= threshold
                } else {
                    passed_raw
                };
                Self::emit_quality_gate_event(
                    "judge",
                    passed,
                    Some(score),
                    None,
                    if malformed_field_count > 0 {
                        Some("malformed_verdict")
                    } else {
                        None
                    },
                );
                if passed {
                    let mut out = if let Some(obj) = parent_inputs.as_object() {
                        obj.clone()
                    } else {
                        serde_json::Map::new()
                    };
                    out.insert("__judge_score__".to_string(), serde_json::json!(score));
                    out.insert("__judge_passed__".to_string(), serde_json::json!(true));
                    out.insert("__judge_reasoning__".to_string(), serde_json::json!(reasoning));
                    out.insert("__judge_feedback__".to_string(), serde_json::json!(feedback));
                    serde_json::Value::Object(out)
                } else {
                    serde_json::json!({
                        "__error": true,
                        "error_message": format!("Judge rejected output: {} (score: {:.2})", reasoning, score),
                        "__judge_score__": score,
                        "__judge_passed__": false,
                        "__judge_feedback__": feedback,
                    })
                }
            }
            Err(e) => e.into_error_envelope("Judge"),
        }
    }

    /// Execute a sub-workflow by ID with the given trigger input, and return
    /// the collapsed terminal output.
    ///
    /// This is the canonical sub-workflow invocation path. It encapsulates what
    /// was previously duplicated at ~10 call sites (judge, ensemble, reflective-
    /// retry, sub_workflow, llm-dispatch) across two dispatch loops:
    ///
    /// 1. Load the sub-workflow graph from the DB (via the registry's db_pool).
    /// 2. Build an engine, register a synthetic `__trigger__` node, wire it to
    ///    every root so root nodes execute instead of being pre-seeded.
    /// 3. `run_with_seed` with `trigger_input` as the trigger's output.
    /// 4. Call [`Self::collapse_subworkflow_output`] to flatten the
    ///    results into the shape callers expect (single-terminal → its
    ///    unwrapped output).
    ///
    /// Returns `Ok(JsonValue)` with the collapsed output, or [`SubflowError`]
    /// which each caller converts into their own error envelope via
    /// [`SubflowError::into_error_envelope`].
    pub async fn execute_subworkflow_graph(
        &self,
        sub_wf_id: Uuid,
        trigger_input: JsonValue,
        dispatcher: Arc<dyn workflow_engine_core::NodeDispatcher>,
        worker_shared_key: Option<Arc<Vec<u8>>>,
    ) -> Result<JsonValue, SubflowError> {
        self.module_fetcher.as_ref().ok_or(SubflowError::NoRegistry)?;
        let user_id = self.user_id.ok_or(SubflowError::NoUserId)?;
        self.secrets_resolver
            .as_ref()
            .ok_or(SubflowError::NoSecretsResolver)?;

        let graph_json = self
            .get_sub_workflow_graph(sub_wf_id, user_id)
            .await
            .ok_or_else(|| SubflowError::GraphNotFound(sub_wf_id))?;

        // Reuse the parent's adapter Arcs (Arc::clone is a refcount
        // bump per trait object — cheap) and populate the graph.
        let mut sub_engine = self.new_subengine();
        sub_engine
            .load_from_graph_json(&graph_json)
            .map_err(SubflowError::BuildFailed)?;

        // Synthetic trigger node: seeded with the caller's input, wired to
        // every root so root-level modules actually execute.
        let trigger_node_id = Uuid::new_v4();
        sub_engine.add_node(trigger_node_id, None, None, None);
        sub_engine
            .node_labels
            .insert(trigger_node_id, "__trigger__".to_string());
        let root_indices: Vec<NodeIndex> = sub_engine
            .graph
            .node_indices()
            .filter(|&idx| {
                sub_engine.graph[idx] != trigger_node_id
                    && sub_engine
                        .graph
                        .neighbors_directed(idx, Direction::Incoming)
                        .count()
                        == 0
            })
            .collect();
        for root_idx in &root_indices {
            let root_id = sub_engine.graph[*root_idx];
            let _ = sub_engine.add_edge(
                trigger_node_id,
                root_id,
                workflow_engine_core::EdgeLogic {
                    source_handle: "output".to_string(),
                    target_handle: "input".to_string(),
                    mapping: None,
                    condition: None,
                    edge_type: "default".to_string(),
                },
            );
        }
        let mut initial_results = HashMap::new();
        initial_results.insert(trigger_node_id, trigger_input);

        let ctx = sub_engine
            .run_with_seed_with_transport(dispatcher, worker_shared_key, initial_results, Uuid::new_v4())
            .await
            .map_err(|e| SubflowError::ExecutionFailed(e.to_string()))?;

        Ok(Self::collapse_subworkflow_output(&ctx.results, &sub_engine))
    }

    pub fn collapse_subworkflow_output(
        ctx_results: &HashMap<Uuid, JsonValue>,
        sub_engine: &ParallelWorkflowEngine,
    ) -> JsonValue {
        // Index uuid -> NodeIndex once (O(V)) so per-node lookups stay O(1).
        let mut uuid_to_idx: HashMap<Uuid, NodeIndex> = HashMap::with_capacity(sub_engine.graph.node_count());
        for idx in sub_engine.graph.node_indices() {
            uuid_to_idx.insert(sub_engine.graph[idx], idx);
        }

        // Partition node outputs into (terminal, non_terminal) while stripping
        // skipped + trigger + engine envelope.
        let mut terminals: Vec<(String, JsonValue)> = Vec::new();
        let mut non_terminals: Vec<(String, JsonValue)> = Vec::new();
        for (nid, output) in ctx_results {
            if output.get("__skipped").and_then(|v| v.as_bool()).unwrap_or(false) {
                continue;
            }
            let label = sub_engine
                .node_labels
                .get(nid)
                .cloned()
                .unwrap_or_else(|| nid.to_string());
            if label == "__trigger__" {
                continue;
            }
            let unwrapped = Self::unwrap_output(output).clone();
            let is_terminal = match uuid_to_idx.get(nid) {
                Some(idx) => sub_engine
                    .graph
                    .neighbors_directed(*idx, Direction::Outgoing)
                    .count()
                    == 0,
                // Node present in results but not in the graph — treat as non-terminal
                // so it can't accidentally shadow the real leaf.
                None => false,
            };
            if is_terminal {
                terminals.push((label, unwrapped));
            } else {
                non_terminals.push((label, unwrapped));
            }
        }

        // Canonical path: exactly one terminal → its output IS the sub-workflow output.
        if terminals.len() == 1 {
            return terminals.into_iter().next().unwrap().1;
        }

        // Fallback: label-keyed map. Insert non-terminals first, then terminals,
        // so a terminal's label wins any collision (stable, predictable ordering).
        let mut map = serde_json::Map::with_capacity(non_terminals.len() + terminals.len());
        for (label, output) in non_terminals {
            map.insert(label, output);
        }
        for (label, output) in terminals {
            map.insert(label, output);
        }
        JsonValue::Object(map)
    }

    pub fn unwrap_output(output: &JsonValue) -> &JsonValue {
        // If output is a JSON string that contains JSON, try to parse it
        if let JsonValue::String(_s) = output {
            // String output from WASM — try to parse as JSON
            // (handled at a higher level, just return as-is here)
            return output;
        }
        // If output looks like the engine wrapper, strip it down to clean payload.
        if let Some(obj) = output.as_object() {
            // Case 1: {"config": {...}, "input": {...}, ...fields} — extract input
            if obj.contains_key("input") {
                if let Some(inner) = obj.get("input") {
                    if let Some(inner_obj) = inner.as_object() {
                        let is_wrapper = inner_obj.keys().all(|k| obj.contains_key(k));
                        if is_wrapper && !inner_obj.is_empty() {
                            return inner;
                        }
                    }
                }
            }
            // Case 2: {"config": {...}, "input": null} — extract config (direct tool with no input)
            if obj.contains_key("config") && obj.get("input").map(|v| v.is_null()).unwrap_or(false)
            {
                if let Some(config) = obj.get("config") {
                    if config.is_object()
                        && !config.as_object().map(|m| m.is_empty()).unwrap_or(true)
                    {
                        return config;
                    }
                }
                // config is also empty — return empty object
                if obj.len() == 2 {
                    return &JsonValue::Null;
                }
            }
        }
        output
    }

    /// Gather inputs for a node based on completed parent results.
    ///
    /// - **Single parent**: passes the parent output directly (unwrapped)
    /// - **Multiple parents**: wraps outputs in an object keyed by user-defined
    ///   node label (from `node_labels`) or falling back to the internal UUID.
    fn gather_inputs(&self, node_idx: NodeIndex, results: &HashMap<Uuid, JsonValue>) -> JsonValue {
        let parents: Vec<(Uuid, &JsonValue)> = self
            .graph
            .neighbors_directed(node_idx, Direction::Incoming)
            .filter_map(|p_idx| {
                let pid = self.graph[p_idx];
                results.get(&pid).map(|out| (pid, Self::unwrap_output(out)))
            })
            .collect();

        match parents.len() {
            0 => JsonValue::Object(Map::new()),
            1 => {
                // Single parent: pass output directly — no UUID wrapping.
                parents[0].1.clone()
            }
            _ => {
                // Multiple parents: key by user-defined label or internal UUID.
                let mut map = Map::new();
                for (pid, output) in parents {
                    let key = self
                        .node_labels
                        .get(&pid)
                        .cloned()
                        .unwrap_or_else(|| pid.to_string());
                    map.insert(key, output.clone());
                }
                JsonValue::Object(map)
            }
        }
    }

    /// Load the Wasm bytecode for a given node ID (enforces user ownership).
    ///
    /// Three layers: the engine-local speculative-prefetch cache, a
    /// "no fetcher configured" MVP fallback for dev harnesses, and — in
    /// the normal case — a delegation to the configured
    /// [`ModuleFetcher`] which owns the real resolution pipeline
    /// (primary lookup, stale-ref-by-name, template fallback,
    /// precompiled-template fallback, Redis cache warm-up).
    async fn fetch_module(
        &self,
        node_id: Uuid,
    ) -> Result<workflow_engine_core::ModuleArtifact, String> {
        if let Some(cached) = self.module_prefetch_cache.remove(&node_id) {
            tracing::debug!(node_id = %node_id, "fetch_module: speculative prefetch cache hit");
            return Ok(cached.1);
        }
        let Some(fetcher) = self.module_fetcher.as_ref() else {
            // Dev / smoke-test convenience: a bare `ParallelWorkflowEngine::new()`
            // with no services wired up falls through to a local wasm artifact.
            // Gated on `debug_assertions` so release binaries never read arbitrary
            // files off disk when a caller misconfigures — they get a clear error
            // instead.
            #[cfg(debug_assertions)]
            {
                let bytes = std::fs::read(
                    "example-node/target/wasm32-wasi/release/my_first_node.wasm",
                )
                .map_err(|e| format!("failed to read wasm module: {}", e))?;
                return Ok(workflow_engine_core::ModuleArtifact {
                    module_id: self.resolve_module_id(node_id),
                    content_hash: "example".to_string(),
                    wasm_bytes: bytes,
                    oci_url: None,
                    max_fuel: 1_000_000,
                    capability_world: "unknown".to_string(),
                    allowed_hosts: vec![],
                    allowed_methods: vec![],
                    allowed_secrets: vec![],
                    requires_approval_for: vec![],
                    integration_name: None,
                    config: None,
                });
            }
            #[cfg(not(debug_assertions))]
            return Err(
                "engine has no module fetcher configured; construct with `with_services` \
                 or call `set_module_fetcher` before dispatching"
                    .to_string(),
            );
        };
        let user_id = self.user_id.ok_or_else(|| {
            "Module execution requires user context (user_id not set)".to_string()
        })?;
        let module_id = self.resolve_module_id(node_id);
        fetcher
            .fetch(module_id, user_id)
            .await
            .map_err(|e| e.to_string())
    }

    // ── Shared node-type helpers ──────────────────────────────────────────
    // The following methods extract duplicated per-node-type logic that was
    // previously inlined in both `run()` and `run_with_seed()`.  Each helper
    // performs the pure computation for a local-dispatch node kind and returns
    // the output `JsonValue` to be inserted into the results map.  The caller
    // is responsible for inserting the result, emitting lifecycle events, and
    // unblocking successors.

    /// Aggregate parent outputs for a FanIn node.
    ///
    /// Collects all incoming node outputs and combines them according to
    /// `join_mode`.  If `aggregation_expr` is provided, it is evaluated as a
    /// Rhai condition against the aggregated value — on failure the result is
    /// replaced with `{"__aggregation_failed": true}`.
    fn aggregate_fan_in(
        &self,
        node_idx: NodeIndex,
        results: &HashMap<Uuid, JsonValue>,
        join_mode: &JoinMode,
        aggregation_expr: &Option<String>,
    ) -> JsonValue {
        let node_id = self.graph[node_idx];
        let parent_outputs: Vec<&JsonValue> = self
            .graph
            .neighbors_directed(node_idx, Direction::Incoming)
            .filter_map(|p| results.get(&self.graph[p]))
            .collect();

        let aggregated = match join_mode {
            JoinMode::All => serde_json::json!(parent_outputs),
            JoinMode::Any => parent_outputs
                .first()
                .map(|v| (*v).clone())
                .unwrap_or(serde_json::json!(null)),
            JoinMode::Majority => serde_json::json!(parent_outputs),
            JoinMode::N(_) => serde_json::json!(parent_outputs),
        };

        let final_result = if let Some(expr) = aggregation_expr {
            if self.eval_bool(expr, &aggregated) {
                aggregated
            } else {
                serde_json::json!({"__aggregation_failed": true})
            }
        } else {
            aggregated
        };

        tracing::info!(
            node_id = %node_id,
            join_mode = ?join_mode,
            parent_count = parent_outputs.len(),
            "FanIn aggregation completed locally"
        );

        final_result
    }

    /// Gather and collect parent outputs for a Collect node.
    ///
    /// Strips engine-internal metadata (`__`-prefixed keys) from each branch
    /// output and wraps them in `{"items": [...], "count": N}`.
    fn collect_parent_outputs_for_node(
        &self,
        node_idx: NodeIndex,
        results: &HashMap<Uuid, JsonValue>,
    ) -> JsonValue {
        let node_id = self.graph[node_idx];
        let parent_outputs: Vec<JsonValue> = self
            .graph
            .neighbors_directed(node_idx, Direction::Incoming)
            .filter_map(|p| results.get(&self.graph[p]).cloned())
            .map(|v| {
                if let JsonValue::Object(mut obj) = v {
                    obj.retain(|k, _| !k.starts_with("__"));
                    JsonValue::Object(obj)
                } else {
                    v
                }
            })
            .collect();

        let parent_count = parent_outputs.len();
        let collected = serde_json::json!({
            "items": parent_outputs,
            "count": parent_count,
        });

        tracing::info!(
            node_id = %node_id,
            parent_count,
            "Collect node gathered all parent outputs into object"
        );

        collected
    }

    /// Build accumulated context from all completed node results so far.
    ///
    /// Returns a JSON object keyed by node label containing each prior node's
    /// output, with engine-internal `__`-prefixed keys stripped from values.
    /// Nodes whose labels start with `__` (engine internals like `__trigger__`)
    /// are omitted entirely. Returns `None` if no user-visible results exist.
    fn build_accumulated_context(
        node_labels: &HashMap<Uuid, String>,
        results: &HashMap<Uuid, JsonValue>,
    ) -> Option<serde_json::Value> {
        let accumulated: Map<String, JsonValue> = results
            .iter()
            .filter_map(|(id, val)| {
                let label = node_labels
                    .get(id)
                    .cloned()
                    .unwrap_or_else(|| id.to_string());
                // Skip engine-internal nodes (trigger, etc.)
                if label.starts_with("__") {
                    return None;
                }
                // Strip __-prefixed metadata keys from the value
                let cleaned = if let JsonValue::Object(obj) = val {
                    let mut c = obj.clone();
                    c.retain(|k, _| !k.starts_with("__"));
                    JsonValue::Object(c)
                } else {
                    val.clone()
                };
                Some((label, cleaned))
            })
            .collect();

        if accumulated.is_empty() {
            None
        } else {
            Some(JsonValue::Object(accumulated))
        }
    }

    /// Compute the Synthesize node output.
    ///
    /// Collects parent outputs (stripping `__`-prefixed metadata), optionally
    /// evaluates a Rhai `synthesis_expr`, and returns the synthesized value.
    /// Array size is capped at 500 to match Rhai limits.
    fn synthesize_parent_outputs(
        &self,
        node_idx: NodeIndex,
        results: &HashMap<Uuid, JsonValue>,
        synthesis_expr: &Option<String>,
    ) -> JsonValue {
        let node_id = self.graph[node_idx];
        let parent_outputs: Vec<JsonValue> = self
            .graph
            .neighbors_directed(node_idx, Direction::Incoming)
            .filter_map(|p| results.get(&self.graph[p]).cloned())
            .map(|v| {
                if let JsonValue::Object(mut obj) = v {
                    obj.retain(|k, _| !k.starts_with("__"));
                    JsonValue::Object(obj)
                } else {
                    v
                }
            })
            .collect();

        let parent_count = parent_outputs.len();

        if parent_count > 500 {
            tracing::warn!(
                node_id = %node_id,
                parent_count,
                "Synthesize: parent_outputs exceeds 500 items — truncating to 500"
            );
        }
        let parent_outputs: Vec<JsonValue> = parent_outputs.into_iter().take(500).collect();
        let parent_count = parent_outputs.len();

        let synthesized = if let Some(ref expr) = synthesis_expr {
            let items_json = serde_json::json!({
                "items": &parent_outputs,
                "count": parent_count,
            });
            match self.eval_json(expr, &items_json) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        node_id = %node_id,
                        error = %e,
                        "Synthesize Rhai expression failed — falling back to raw collect"
                    );
                    serde_json::json!({ "items": &parent_outputs, "count": parent_count })
                }
            }
        } else {
            serde_json::json!({ "items": &parent_outputs, "count": parent_count })
        };

        tracing::info!(
            node_id = %node_id,
            parent_count,
            has_expr = synthesis_expr.is_some(),
            "Synthesize node processed parent outputs"
        );

        synthesized
    }

    /// Evaluate a Verify node against its parent output.
    ///
    /// Returns `(result_json, passed)` where `passed` indicates whether the
    /// verification condition was satisfied.  The caller uses `passed` to
    /// select the event status string ("Completed" vs "Failed").
    fn evaluate_verify_node(
        &self,
        node_idx: NodeIndex,
        results: &HashMap<Uuid, JsonValue>,
        condition: &str,
        check_label: &str,
        on_failure: &str,
    ) -> (JsonValue, bool) {
        let node_id = self.graph[node_idx];
        let parent_output = self.gather_inputs(node_idx, results);
        let passed =
            self.eval_bool(condition, &parent_output);

        let verify_result = if passed {
            let mut out = parent_output;
            if let Some(obj) = out.as_object_mut() {
                obj.insert(
                    "__verified__".to_string(),
                    serde_json::json!(true),
                );
                obj.insert(
                    "__check_label__".to_string(),
                    serde_json::Value::String(check_label.to_string()),
                );
            }
            out
        } else if on_failure == "passthrough" {
            let mut out = parent_output;
            if let Some(obj) = out.as_object_mut() {
                obj.insert(
                    "__verified__".to_string(),
                    serde_json::json!(false),
                );
                obj.insert(
                    "__verification_failed__".to_string(),
                    serde_json::json!(true),
                );
                obj.insert(
                    "__check_label__".to_string(),
                    serde_json::Value::String(check_label.to_string()),
                );
                obj.insert(
                    "__verification_condition__".to_string(),
                    serde_json::Value::String(condition.to_string()),
                );
            }
            out
        } else {
            serde_json::json!({
                "__error": true,
                "error_message": format!(
                    "Verification failed for '{}': condition '{}' evaluated to false. \
                     Wire an error edge from this verify node to a fix-up workflow, or \
                     set on_failure: 'passthrough' to route conditionally downstream.",
                    check_label, condition
                ),
                "__verified__": false,
                "__check_label__": check_label,
            })
        };

        tracing::info!(
            node_id = %node_id,
            check_label = %check_label,
            passed,
            on_failure = %on_failure,
            "Verify node evaluated"
        );

        (verify_result, passed)
    }

    /// Evaluate a ConfidenceGate node against its parent output.
    ///
    /// Returns `Ok(result_json)` for pass/passthrough/error modes, or
    /// `Err(waiting_json)` when the gate is paused awaiting approval.
    /// The caller must handle the `Err` case by early-returning from the
    /// reactor loop with a `waiting: true` WorkflowContext.
    async fn evaluate_confidence_gate(
        &self,
        node_idx: NodeIndex,
        results: &HashMap<Uuid, JsonValue>,
        execution_id: Uuid,
        threshold: f64,
        confidence_path: &str,
        on_low_confidence: &str,
    ) -> Result<JsonValue, JsonValue> {
        let node_id = self.graph[node_idx];
        let parent_inputs = self.gather_inputs(node_idx, results);
        let confidence = parent_inputs
            .get(confidence_path)
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);

        if confidence >= threshold {
            let mut out = if let Some(obj) = parent_inputs.as_object() {
                obj.clone()
            } else {
                serde_json::Map::new()
            };
            out.insert(
                "__confidence_gate_passed__".to_string(),
                serde_json::json!(true),
            );
            out.insert(
                "__confidence_used__".to_string(),
                serde_json::json!(confidence),
            );
            return Ok(serde_json::Value::Object(out));
        }

        match on_low_confidence {
            "passthrough" => {
                let mut out = if let Some(obj) = parent_inputs.as_object() {
                    obj.clone()
                } else {
                    serde_json::Map::new()
                };
                out.insert(
                    "__confidence_gate_failed__".to_string(),
                    serde_json::json!(true),
                );
                out.insert(
                    "__confidence_used__".to_string(),
                    serde_json::json!(confidence),
                );
                Ok(serde_json::Value::Object(out))
            }
            "error" => Ok(serde_json::json!({
                "__error": true,
                "error_message": format!(
                    "Confidence gate: {:.3} < threshold {:.3}",
                    confidence, threshold
                ),
                "__confidence_used__": confidence,
            })),
            _ => {
                // "pause" — create approval request and suspend
                if let Some(ref gate) = self.approval_gate {
                    let required_for = vec!["low_confidence".to_string()];
                    match gate
                        .check_or_request(execution_id, node_id, &required_for, None)
                        .await
                    {
                        Ok(workflow_engine_core::ApprovalStatus::Approved) => {
                            let mut out = if let Some(obj) = parent_inputs.as_object() {
                                obj.clone()
                            } else {
                                serde_json::Map::new()
                            };
                            out.insert(
                                "__confidence_gate_passed__".to_string(),
                                serde_json::json!(true),
                            );
                            out.insert(
                                "__confidence_used__".to_string(),
                                serde_json::json!(confidence),
                            );
                            out.insert(
                                "__confidence_gate_approved__".to_string(),
                                serde_json::json!(true),
                            );
                            Ok(serde_json::Value::Object(out))
                        }
                        Ok(workflow_engine_core::ApprovalStatus::Pending) => {
                            Err(serde_json::json!({
                                "__waiting__": true,
                                "__confidence_used__": confidence,
                                "message": format!(
                                    "Confidence gate paused: {:.3} < threshold {:.3}. Awaiting approval.",
                                    confidence, threshold
                                ),
                            }))
                        }
                        Ok(workflow_engine_core::ApprovalStatus::Denied { reason }) => {
                            Ok(serde_json::json!({
                                "__error": true,
                                "error_message": reason,
                            }))
                        }
                        Err(e) => Ok(serde_json::json!({
                            "__error": true,
                            "error_message": format!("ConfidenceGate approval error: {}", e),
                        })),
                    }
                } else {
                    Ok(serde_json::json!({
                        "__error": true,
                        "error_message": "ConfidenceGate pause requires an approval gate",
                    }))
                }
            }
        }
    }

    /// Emit a `node_started` + `node_completed` pair through the engine's
    /// configured event sink. Fire-and-forget; no-op when no sink is
    /// configured.
    ///
    /// Both events are emitted from a **single** spawned task that
    /// awaits them sequentially, so `node_started` is guaranteed to
    /// commit before `node_completed`. This ordering matters for
    /// collapsed system nodes (Collect, Synthesize, Verify) whose
    /// downstream observers reconstruct per-node timelines from the
    /// events table.
    fn emit_node_lifecycle_events(
        &self,
        execution_id: Uuid,
        node_id: Uuid,
        status: &str,
        log_message: String,
    ) {
        let Some(sink) = self.event_sink.as_ref() else {
            return;
        };
        let sink = Arc::clone(sink);
        let status = status.to_string();
        tokio::spawn(async move {
            sink.emit(NodeEventWrite {
                execution_id,
                event_type: "node_started".to_string(),
                node_id: Some(node_id),
                status: "Running".to_string(),
                log_message: None,
                iteration_index: None,
            })
            .await;
            sink.emit(NodeEventWrite {
                execution_id,
                event_type: "node_completed".to_string(),
                node_id: Some(node_id),
                status,
                log_message: Some(log_message),
                iteration_index: None,
            })
            .await;
        });
    }

    /// Execute the graph in parallel using a caller-supplied
    /// [`NodeDispatcher`].
    ///
    /// Linear chains (maximal sequences of nodes with in-degree=1 / out-degree=1)
    /// are dispatched as a single `execute_pipeline()` call, eliminating per-node
    /// dispatcher round-trips and intermediate result serialisation.
    ///
    /// This is the engine's primary public API. In-tree callers hold an
    /// `Arc<async_nats::Client>` and build the NATS-backed dispatcher
    /// via a controller-side helper; out-of-tree consumers supply their
    /// own `NodeDispatcher` impl.
    ///
    /// [`NodeDispatcher`]: workflow_engine_core::NodeDispatcher
    pub async fn run_with_transport(
        &self,
        dispatcher: Arc<dyn workflow_engine_core::NodeDispatcher>,
        worker_shared_key: Option<Arc<Vec<u8>>>,
        execution_id: Uuid,
    ) -> Result<WorkflowContext, String> {
        // Abstract-entry guard: the engine's public execution path
        // REQUIRES a configured `SecretsResolver`. Without one, every
        // dispatch site's `(Some(resolver), Some(key))` guard silently
        // sends empty-ciphertext `encrypted_secrets` — the exact class
        // of bug that masked the 2026-04-16 loop-node secret-injection
        // regression. Fail closed at run start so a misconfigured
        // engine can never produce a silently-unsecured dispatch.
        if self.secrets_resolver.is_none() {
            return Err(
                "ParallelWorkflowEngine was constructed without a SecretsResolver. \
                 Use `with_services`, `with_services_and_resolver`, or \
                 `set_secrets_resolver` before calling run_with_transport. \
                 Running without a resolver is not permitted on the abstract \
                 entry point because every dispatch site requires one to encrypt \
                 per-node secrets; an unset resolver produces empty-ciphertext \
                 dispatches (silent security regression)."
                    .to_string(),
            );
        }

        // Build the execution-scoped DLP context once — used to value-scrub output/errors
        // before DB storage. Regex patterns are applied on top in a second pass.
        // Per-run DLP sanitizer — built once from resolved node configs
        // and used to scrub error messages before persistence. Stateless
        // regex-based scrubs (crate::dlp::redact_*) run in a second pass
        // on top via `self.redact_str` / `self.redact_json`.
        let exec_ctx = self.new_execution_sanitizer();

        // Create temporary sandboxed directory for this execution.
        // _sandbox_guard ensures the directory is removed even if this task panics.
        let (execution_sandbox, _sandbox_guard) = match create_execution_sandbox(execution_id) {
            Ok(sandbox) => {
                tracing::debug!("Created execution sandbox: {}", execution_id);
                (Some(sandbox), Some(SandboxGuard { execution_id }))
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to create execution sandbox: {}. File I/O will be unavailable.",
                    e
                );
                (None, None)
            }
        };

        // Verify DAG – simple cycle check.
        if petgraph::algo::is_cyclic_directed(&self.graph) {
            return Err("Workflow contains a cycle".into());
        }

        // Detect linear chains for pipeline optimisation.
        let chains = detect_linear_chains(&self.graph);

        // Build a lookup: NodeIndex → chain index (for O(1) chain membership check).
        let mut node_to_chain: HashMap<NodeIndex, usize> = HashMap::new();
        // Track which node is the *head* of each chain (for ready-queue dedup).
        let mut chain_heads: HashSet<NodeIndex> = HashSet::new();
        for (chain_idx, chain) in chains.iter().enumerate() {
            chain_heads.insert(chain[0]);
            for &n in chain {
                node_to_chain.insert(n, chain_idx);
            }
        }

        // In-degree counter.
        let mut pending: HashMap<NodeIndex, usize> = HashMap::new();
        let mut ready: VecDeque<NodeIndex> = VecDeque::new();
        for idx in self.graph.node_indices() {
            let deps = self
                .graph
                .neighbors_directed(idx, Direction::Incoming)
                .count();
            pending.insert(idx, deps);
            if deps == 0 {
                ready.push_back(idx);
            }
        }

        let mut results: HashMap<Uuid, JsonValue> = HashMap::new();
        // Use trait objects so we can push both pipeline-chain futures and
        // single-node futures (which are different concrete async block types).
        let mut executing: FuturesUnordered<ExecFuture<'_>> = FuturesUnordered::new();

        // Main reactor loop.
        while !ready.is_empty() || !executing.is_empty() {
            // Spawn ready nodes / chains.
            while let Some(node_idx) = ready.pop_front() {
                // ── Pipeline dispatch (chain head) ───────────────────────────
                if let Some(&chain_idx) = node_to_chain.get(&node_idx) {
                    // Only dispatch when we're at the chain head.
                    if chain_heads.contains(&node_idx) {
                        let chain = &chains[chain_idx];
                        let chain_node_ids: Vec<Uuid> =
                            chain.iter().map(|&n| self.graph[n]).collect();
                        // Pre-resolve graph node UUIDs → module UUIDs before
                        // entering the tokio::spawn block (which can't borrow
                        // self). Graph node IDs are SHA256-derived from the
                        // node label string ("fetch-upcoming" → deterministic
                        // UUID) and don't match any wasm_modules row.
                        // resolve_module_id maps them back to the template/
                        // module UUID stored in node_meta at graph load time.
                        let chain_module_ids: Vec<Uuid> = chain_node_ids
                            .iter()
                            .map(|&nid| self.resolve_module_id(nid))
                            .collect();

                        // Gather input for the chain's first node.
                        let chain_input = self.gather_inputs(node_idx, &results);
                        let chain_head_id = self.graph[node_idx];
                        let chain_retry = self
                            .node_meta
                            .get(&chain_head_id)
                            .and_then(|(_, rp, _)| rp.clone())
                            .unwrap_or_default();
                        let dispatcher_clone = dispatcher.clone();
                        let user_id_clone = self.user_id;
                        let module_fetcher_clone = self.module_fetcher.clone();
                        let approval_gate = self.approval_gate.clone();
                        let secrets_resolver = self.secrets_resolver.clone();
                        let chain_clone = chain.clone();
                        let chain_user_id = self.user_id;
                        let worker_shared_key_clone = worker_shared_key.clone();
                        let _node_configs_clone = self.node_configs.clone();
                        let node_timeouts_clone = self.node_timeouts.clone();
                        // Accumulated context snapshot for pipeline's first step.
                        let accumulated_snapshot =
                            Self::build_accumulated_context(&self.node_labels, &results);

                        let fut = async move {
                            // Resolve user_id early — required for all module-fetcher calls.
                            let uid_for_chain: Option<Uuid> = if module_fetcher_clone.is_some() {
                                match chain_user_id {
                                    Some(u) => Some(u),
                                    None => {
                                        return (
                                            chain_clone[chain_clone.len() - 1],
                                            Err("Module execution requires user context (user_id not set)".to_string()),
                                        )
                                    }
                                }
                            } else {
                                None
                            };

                            // Build DispatchJobs for every node in the chain. The
                            // dispatcher's `dispatch_chain` adapter maps these into
                            // whatever batch wire format its backing dispatcher uses
                            // (the Talos NATS dispatcher emits a signed
                            // `PipelineJobRequest`; an in-process test dispatcher
                            // might just loop `dispatch` via
                            // `workflow_engine_core::dispatch_chain_sequential`).
                            let mut step_jobs: Vec<DispatchJob> =
                                Vec::with_capacity(chain_clone.len());

                            for (i, &_step_idx) in chain_clone.iter().enumerate() {
                                let step_node_id = chain_node_ids[i];
                                // Use the resolved module UUID (template ID)
                                // for registry lookups so get_execution_info
                                // finds the correct allowed_hosts/secrets/fuel.
                                let step_module_id = chain_module_ids[i];

                                let uid = match uid_for_chain {
                                    Some(u) => u,
                                    None => {
                                        return (
                                            chain_clone[chain_clone.len() - 1],
                                            Err(format!(
                                                "Missing user ID for module {} in chain",
                                                step_node_id
                                            )),
                                        )
                                    }
                                };

                                // Fetch the pipeline step's module artifact via the
                                // trait; `ModuleArtifact.config` mirrors
                                // `wasm_modules.config`, same data the pre-extraction
                                // code read via `reg.get_execution_info`. Dropping
                                // the Redis cache warm that used to fire here —
                                // wasm_bytes is embedded in the dispatched chain,
                                // so the worker doesn't depend on the pre-warm.
                                let (artifact, module_config) =
                                    match module_fetcher_clone.as_ref() {
                                        Some(fetcher) => {
                                            match fetcher.fetch(step_module_id, uid).await {
                                                Ok(a) => {
                                                    let config = a
                                                        .config
                                                        .clone()
                                                        .unwrap_or_else(|| serde_json::json!({}));
                                                    (Some(a), config)
                                                }
                                                Err(e) => {
                                                    return (
                                                        chain_clone[chain_clone.len() - 1],
                                                        Err(format!(
                                                            "Failed to prepare module: {}",
                                                            e
                                                        )),
                                                    )
                                                }
                                            }
                                        }
                                        None => (None, serde_json::json!({})),
                                    };

                                // ── Approval gate (pipeline step) ────────────
                                let requires_approval: Vec<String> = artifact
                                    .as_ref()
                                    .map(|a| a.requires_approval_for.clone())
                                    .unwrap_or_default();
                                if !requires_approval.is_empty() {
                                    if let Some(ref gate) = approval_gate {
                                        let approval_webhook = module_config
                                            .get("NOTIFICATION_WEBHOOK")
                                            .and_then(|v| v.as_str());
                                        match gate
                                            .check_or_request(
                                                execution_id,
                                                step_node_id,
                                                &requires_approval,
                                                approval_webhook,
                                            )
                                            .await
                                        {
                                            Ok(workflow_engine_core::ApprovalStatus::Approved) => {
                                                /* proceed */
                                            }
                                            Ok(workflow_engine_core::ApprovalStatus::Pending) => {
                                                return (
                                                    chain_clone[chain_clone.len() - 1],
                                                    Err(format!(
                                                        "Execution paused: module {} requires approval for {:?}. \
                                                         An approval request has been created.",
                                                        step_node_id, requires_approval
                                                    )),
                                                );
                                            }
                                            Ok(workflow_engine_core::ApprovalStatus::Denied { reason }) => {
                                                return (
                                                    chain_clone[chain_clone.len() - 1],
                                                    Err(reason),
                                                );
                                            }
                                            Err(e) => {
                                                return (
                                                    chain_clone[chain_clone.len() - 1],
                                                    Err(format!(
                                                        "Approval gate check failed: {}",
                                                        e
                                                    )),
                                                );
                                            }
                                        }
                                    }
                                }

                                // Extract vault:// paths from module_config before it
                                // is moved into PipelineStep below.
                                let vault_paths = extract_vault_paths(&module_config);

                                // Per-node fuel limit precedence:
                                //   1. node config `max_fuel` (highest)
                                //   2. wasm_modules.max_fuel from the artifact
                                //   3. 1M default
                                // Capped at 50M. Previously hardcoded the
                                // fallback to 1M, silently discarding any
                                // DB-level bump on template-dispatched paths.
                                let module_default_fuel = artifact
                                    .as_ref()
                                    .map(|a| a.max_fuel)
                                    .filter(|f| *f > 0)
                                    .unwrap_or(1_000_000);
                                let node_max_fuel = module_config
                                    .get("max_fuel")
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(module_default_fuel)
                                    .min(50_000_000);

                                let encrypted_secrets = match (
                                    secrets_resolver.as_ref(),
                                    &worker_shared_key_clone,
                                ) {
                                    (Some(resolver), Some(key)) => {
                                        build_encrypted_secrets_for(
                                            resolver.as_ref(),
                                            step_node_id,
                                            user_id_clone,
                                            &vault_paths,
                                            &[],
                                            key,
                                        )
                                        .await
                                    }
                                    _ => Default::default(),
                                };
                                step_jobs.push(DispatchJob {
                                    execution_id,
                                    node_id: step_node_id,
                                    module_id: step_node_id,
                                    // Chain-level wire format derives a single
                                    // `job_id`; per-step ids are not correlated to
                                    // the individual `module_executions` rows
                                    // (those use `step_exec_ids` — see below).
                                    job_id: None,
                                    user_id: uid,
                                    actor_id: self.actor_id,
                                    // Match pre-extraction behavior: the redis fallback
                                    // key is `redis:wasm:{module_id}`, NOT the graph node
                                    // UUID. Before 73060b9, `ModuleExecutionInfo.module_uri`
                                    // already applied this mapping inside the registry;
                                    // now that we read `oci_url` directly, the fallback
                                    // must use `step_module_id` to match the worker's
                                    // redis-key convention.
                                    module_uri: artifact
                                        .as_ref()
                                        .and_then(|a| a.oci_url.clone())
                                        .unwrap_or_else(|| format!("redis:wasm:{}", step_module_id)),
                                    wasm_bytes: None,
                                    expected_wasm_hash: artifact
                                        .as_ref()
                                        .map(|a| a.content_hash.clone()),
                                    // Pipeline dispatch doesn't carry capability_world
                                    // per-step — the worker's pipeline executor uses the
                                    // chain-level world. DispatchJob carries it for the
                                    // single-node path; the NATS adapter drops it here.
                                    capability_world: None,
                                    integration_name: artifact
                                        .as_ref()
                                        .and_then(|a| a.integration_name.clone()),
                                    // PipelineStep calls this `config`; the
                                    // adapter will map `input_payload` to it.
                                    input_payload: module_config,
                                    timeout: std::time::Duration::from_secs(
                                        node_timeouts_clone
                                            .get(&step_node_id)
                                            .copied()
                                            .unwrap_or(30),
                                    ),
                                    max_fuel: node_max_fuel,
                                    allowed_hosts: artifact
                                        .as_ref()
                                        .map(|a| a.allowed_hosts.clone())
                                        .unwrap_or_default(),
                                    allowed_methods: artifact
                                        .as_ref()
                                        .map(|a| a.allowed_methods.clone())
                                        .unwrap_or_default(),
                                    allowed_secrets: artifact
                                        .as_ref()
                                        .map(|a| a.allowed_secrets.clone())
                                        .unwrap_or_default(),
                                    allowed_sql_operations: vec![],
                                    allow_tier2_exposure: false,
                                    encrypted_secrets_ciphertext: encrypted_secrets.ciphertext,
                                    encrypted_secrets_nonce: encrypted_secrets.nonce,
                                    priority: 100,
                                    dry_run: self.dry_run,
                                    max_retries: 0,
                                    backoff_ms: 0,
                                    retry_condition: None,
                                    retry_delay_expr: None,
                                    // Chain-level retry emits under the chain's
                                    // aggregate policy, not per-step.
                                    emit_retry_events: false,
                                });
                            }

                            // For the first step, inject the gathered inputs as
                            // initial input (wrap it the same way as single-node does).
                            // DispatchJob's `input_payload` maps to PipelineStep's `config`
                            // in the adapter.
                            if let Some(first) = step_jobs.first_mut() {
                                let mut wrapped = serde_json::json!({
                                    "pipeline_input": chain_input,
                                    "config": first.input_payload,
                                });
                                // Inject accumulated context into pipeline first step
                                if let Some(ref acc) = accumulated_snapshot {
                                    if let Some(obj) = wrapped.as_object_mut() {
                                        obj.insert("__accumulated__".to_string(), acc.clone());
                                    }
                                }
                                // Inject actor memory context so LLM nodes can
                                // reference learned_preferences, persona, etc.
                                if let Some(ref ctx) = self.actor_context {
                                    if let Some(obj) = wrapped.as_object_mut() {
                                        obj.insert("__actor_context__".to_string(), ctx.clone());
                                    }
                                }
                                first.input_payload = wrapped;
                            }

                            // Pre-INSERT `module_executions` rows for each step
                            // so observers can see the chain's in-flight state.
                            // Row ids (`step_exec_ids`) are engine-level
                            // bookkeeping — they're NOT threaded through to the
                            // wire format; the post-dispatch UPDATE below uses
                            // them to target the right row.
                            let mut step_exec_ids = Vec::new();
                            if let Some(ref store) = self.module_execution_store {
                                for (i, &step_node_id) in chain_node_ids.iter().enumerate() {
                                    let step_exec_id = Uuid::new_v4();
                                    step_exec_ids.push(step_exec_id);
                                    let input_for_db = if i == 0 {
                                        serde_json::json!({ "input": chain_input })
                                    } else {
                                        serde_json::json!(null)
                                    };
                                    let actual_mid =
                                        store.resolve_wasm_module_id(step_node_id).await;
                                    if let Err(db_err) = store
                                        .record_started(
                                            step_exec_id,
                                            actual_mid,
                                            uid_for_chain.unwrap_or_else(Uuid::new_v4),
                                            execution_id,
                                            &input_for_db,
                                            "webhook",
                                            // Pipeline steps dispatch as a unit — no
                                            // concurrent sibling to race against.
                                            false,
                                        )
                                        .await
                                    {
                                        tracing::error!(
                                            "module_execution_store.record_started failed: {}",
                                            db_err
                                        );
                                    }
                                }
                            }

                            // Aggregate timeout = sum of per-step budgets + 5s
                            // NATS overhead, clamped to the operator-configurable
                            // TALOS_NATS_TIMEOUT_SECS floor.
                            static NATS_TIMEOUT_FLOOR_SECS: OnceLock<u64> = OnceLock::new();
                            let nats_floor = *NATS_TIMEOUT_FLOOR_SECS.get_or_init(|| {
                                std::env::var("TALOS_NATS_TIMEOUT_SECS")
                                    .ok()
                                    .and_then(|v| v.parse::<u64>().ok())
                                    .unwrap_or(0)
                            });
                            let chain_computed_secs: u64 = chain_node_ids
                                .iter()
                                .map(|id| node_timeouts_clone.get(id).copied().unwrap_or(30))
                                .sum::<u64>()
                                + 5;
                            let timeout_secs = chain_computed_secs.max(nats_floor);

                            let chain_request = workflow_engine_core::ChainDispatchRequest {
                                workflow_execution_id: execution_id,
                                user_id: uid_for_chain.unwrap_or_else(Uuid::nil),
                                job_id: None,
                                steps: step_jobs,
                                share_sandbox: true,
                                total_timeout: std::time::Duration::from_secs(timeout_secs),
                                max_retries: chain_retry.max_retries,
                                backoff_ms: chain_retry.backoff_ms,
                                retry_condition: chain_retry.retry_condition.clone(),
                                retry_delay_expr: chain_retry.retry_delay_expression.clone(),
                            };

                            let chain_result = match dispatcher_clone
                                .dispatch_chain(chain_request)
                                .await
                            {
                                Ok(r) => r,
                                Err(e) => {
                                    return (chain_clone[chain_clone.len() - 1], Err(e.to_string()));
                                }
                            };

                            // Per-step post-processing: update module_executions
                            // rows with status/output/error, persist
                            // __memory_write__ payloads for successful steps.
                            if let Some(ref store) = self.module_execution_store {
                                for (i, step_result) in chain_result.steps.iter().enumerate() {
                                    if let Some(&step_exec_id) = step_exec_ids.get(i) {
                                        let status_str = match step_result.status {
                                            workflow_engine_core::StepStatus::Success => "completed",
                                            workflow_engine_core::StepStatus::TimedOut => "timeout",
                                            workflow_engine_core::StepStatus::Failed => "failed",
                                        };
                                        let error_msg = step_result
                                            .error
                                            .as_deref()
                                            .map(|s| self.redact_str(s));
                                        let duration = i32::try_from(
                                            step_result.execution_time_ms,
                                        )
                                        .unwrap_or(i32::MAX);
                                        if let Err(db_err) = store
                                            .record_completed(
                                                step_exec_id,
                                                status_str,
                                                &self.redact_json(&step_result.output),
                                                duration,
                                                error_msg.as_deref(),
                                            )
                                            .await
                                        {
                                            tracing::error!(
                                                "module_execution_store.record_completed failed: {}",
                                                db_err
                                            );
                                        }

                                        // __memory_write__ protocol for pipeline steps.
                                        // Only fire the hook for successful steps — failed
                                        // steps may have partial/corrupt output. The hook
                                        // owns extraction + spawn semantics; the engine
                                        // just forwards per-step outputs.
                                        if matches!(step_result.status, workflow_engine_core::StepStatus::Success) {
                                            if let Some(hook) = self.node_hook.as_ref() {
                                                hook.on_pipeline_step_completed(
                                                    self.actor_id,
                                                    &step_result.output,
                                                );
                                            }
                                        }
                                    }
                                }
                                // Mark any unexecuted trailing steps as aborted.
                                for i in chain_result.steps.len()..step_exec_ids.len() {
                                    if let Some(&step_exec_id) = step_exec_ids.get(i) {
                                        if let Err(db_err) = store
                                            .record_completed(
                                                step_exec_id,
                                                "failed",
                                                &serde_json::Value::Null,
                                                0,
                                                Some("Pipeline aborted before this step"),
                                            )
                                            .await
                                        {
    tracing::error!("Database operation failed in engine: {}", db_err);
}
                                    }
                                }
                            }

                            match chain_result.overall_status {
                                workflow_engine_core::StepStatus::Success => (
                                    chain_clone[chain_clone.len() - 1],
                                    Ok(chain_result.final_output),
                                ),
                                _ => (
                                    chain_clone[chain_clone.len() - 1],
                                    Err(format!(
                                        "Pipeline execution failed: {:?}",
                                        chain_result.final_output
                                    )),
                                ),
                            }
                        };
                        executing.push(Box::pin(fut)
                            as Pin<
                                Box<
                                    dyn Future<Output = (NodeIndex, Result<JsonValue, String>)>
                                        + Send,
                                >,
                            >);
                        continue;
                    }
                    // Non-head chain nodes are handled when the chain completes — skip them.
                    continue;
                }

                // ── FanIn aggregation (local computation, no NATS dispatch) ──
                let node_id = self.graph[node_idx];

                // ── Skip condition check (FIRST — applies to ALL node types including system nodes) ──
                if let Some(skip_cond) = self
                    .node_configs
                    .get(&node_id)
                    .and_then(|cfg| cfg.get("__skip_condition"))
                    .and_then(|v| v.as_str())
                {
                    let mut skip_context = self.gather_inputs(node_idx, &results);
                    if let Some(trigger_id) = self
                        .node_labels
                        .iter()
                        .find(|(_, label)| label.as_str() == "__trigger__")
                        .map(|(uuid, _)| *uuid)
                    {
                        if let Some(trigger_val) = results.get(&trigger_id) {
                            if let Some(obj) = trigger_val.as_object() {
                                if let Some(ctx_obj) = skip_context.as_object_mut() {
                                    for (k, v) in obj {
                                        ctx_obj.entry(k.clone()).or_insert(v.clone());
                                    }
                                }
                            }
                        }
                    }
                    if self.eval_bool(skip_cond, &skip_context) {
                        tracing::info!(node_id = %node_id, skip_condition = %skip_cond, "Node skipped by skip_condition");
                        results.insert(
                            node_id,
                            serde_json::json!({"__skipped": true, "reason": "skip_condition"}),
                        );
                        emit_event_spawn(
                            &self.event_sink,
                            NodeEventWrite {
                                execution_id,
                                event_type: "node_skipped".to_string(),
                                node_id: Some(node_id),
                                status: "Skipped".to_string(),
                                log_message: None,
                                iteration_index: None,
                            },
                        );
                        for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                            if let Some(cnt) = pending.get_mut(&child) {
                                if *cnt > 0 {
                                    *cnt -= 1;
                                }
                                if pending.get(&child).copied().unwrap_or(1) == 0 {
                                    ready.push_back(child);
                                }
                            }
                        }
                        continue;
                    }
                }

                if let Some((
                    _,
                    _,
                    Some(SystemNodeKind::FanIn {
                        ref join_mode,
                        ref aggregation_expr,
                    }),
                )) = self.node_meta.get(&node_id)
                {
                    let final_result = self.aggregate_fan_in(node_idx, &results, join_mode, aggregation_expr);

                    results.insert(node_id, final_result);

                    // Unblock successors of the FanIn node
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 {
                                *cnt -= 1;
                            }
                            if *cnt == 0 {
                                ready.push_back(child);
                            }
                        }
                    }
                    continue;
                }

                // ── Collect dispatch (local computation — chain reactor) ─────
                if let Some((_, _, Some(SystemNodeKind::Collect))) = self.node_meta.get(&node_id) {
                    let collected = self.collect_parent_outputs_for_node(node_idx, &results);
                    let parent_count = collected.get("count").and_then(|v| v.as_u64()).unwrap_or(0);

                    results.insert(node_id, collected);

                    self.emit_node_lifecycle_events(
                        execution_id,
                        node_id,
                        "Completed",
                        format!("collected {} branch outputs into items array", parent_count),
                    );

                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 {
                                *cnt -= 1;
                            }
                            if *cnt == 0 {
                                ready.push_back(child);
                            }
                        }
                    }
                    continue;
                }

                // ── Synthesize dispatch (collect + optional Rhai synthesis) ──
                if let Some((_, _, Some(SystemNodeKind::Synthesize { ref synthesis_expr }))) =
                    self.node_meta.get(&node_id)
                {
                    let synthesis_expr = synthesis_expr.clone();
                    let synthesized = self.synthesize_parent_outputs(node_idx, &results, &synthesis_expr);

                    // Recover parent_count for event logging from the synthesized output
                    // (it may be an object with "count" if no expression was applied, or
                    // arbitrary if a Rhai expression transformed it).
                    let parent_count = synthesized
                        .get("count")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);

                    results.insert(node_id, synthesized);

                    self.emit_node_lifecycle_events(
                        execution_id,
                        node_id,
                        "Completed",
                        format!("synthesized {} branch outputs", parent_count),
                    );

                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if *cnt == 0 { ready.push_back(child); }
                        }
                    }
                    continue;
                }

                // ── Verify dispatch (step-level output verification) ─────────
                if let Some((
                    _,
                    _,
                    Some(SystemNodeKind::Verify { ref condition, ref check_label, ref on_failure }),
                )) = self.node_meta.get(&node_id)
                {
                    let check_label = check_label.clone().unwrap_or_else(|| "output quality".to_string());
                    let (verify_result, passed) = self.evaluate_verify_node(
                        node_idx, &results, condition, &check_label, on_failure,
                    );

                    results.insert(node_id, verify_result);

                    self.emit_node_lifecycle_events(
                        execution_id,
                        node_id,
                        if passed { "Completed" } else { "Failed" },
                        format!(
                            "Verify '{}': {}",
                            check_label,
                            if passed { "PASSED" } else { "FAILED" }
                        ),
                    );

                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if *cnt == 0 { ready.push_back(child); }
                        }
                    }
                    continue;
                }

                // ── Judge dispatch (LLM-as-Judge evaluation) ─────────────────
                if let Some((_, _, Some(SystemNodeKind::Judge { judge_workflow_id, ref rubric, pass_threshold, timeout_secs: _ }))) =
                    self.node_meta.get(&node_id)
                {
                    let judge_wf_id = *judge_workflow_id;
                    let rubric = rubric.clone();
                    let pass_threshold = *pass_threshold;
                    let parent_inputs = self.gather_inputs(node_idx, &results);

                    let judge_result = self
                        .dispatch_judge(
                            parent_inputs,
                            judge_wf_id,
                            rubric,
                            pass_threshold,
                            dispatcher.clone(),
                            worker_shared_key.clone(),
                        )
                        .await;

                    results.insert(node_id, judge_result);
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if pending.get(&child).copied().unwrap_or(1) == 0 { ready.push_back(child); }
                        }
                    }
                    continue;
                }

                // ── Ensemble dispatch (self-consistency / ensemble voting) ────
                if let Some((_, _, Some(SystemNodeKind::Ensemble { child_workflow_id, count, ref consensus, judge_workflow_id, timeout_secs: _ }))) =
                    self.node_meta.get(&node_id)
                {
                    let child_wf_id = *child_workflow_id;
                    let run_count = *count;
                    let consensus_strategy = consensus.clone();
                    let judge_wf_id_opt = *judge_workflow_id;
                    let inputs = self.gather_inputs(node_idx, &results);

                    let ensemble_result = self
                        .dispatch_ensemble(
                            inputs,
                            child_wf_id,
                            run_count,
                            consensus_strategy,
                            judge_wf_id_opt,
                            dispatcher.clone(),
                            worker_shared_key.clone(),
                        )
                        .await;

                    results.insert(node_id, ensemble_result);
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if pending.get(&child).copied().unwrap_or(1) == 0 { ready.push_back(child); }
                        }
                    }
                    continue;
                }

                // ── ConfidenceGate dispatch ───────────────────────────────────
                if let Some((_, _, Some(SystemNodeKind::ConfidenceGate { threshold, ref confidence_path, ref on_low_confidence }))) =
                    self.node_meta.get(&node_id)
                {
                    match self.evaluate_confidence_gate(
                        node_idx, &results, execution_id, *threshold, confidence_path, on_low_confidence,
                    ).await {
                        Ok(gate_result) => {
                            results.insert(node_id, gate_result);
                        }
                        Err(waiting_json) => {
                            // Pending approval — pause execution
                            results.insert(node_id, waiting_json);
                            return Ok(WorkflowContext { results, waiting: true, ..Default::default() });
                        }
                    }
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if pending.get(&child).copied().unwrap_or(1) == 0 { ready.push_back(child); }
                        }
                    }
                    continue;
                }

                // ── ReflectiveRetry dispatch ──────────────────────────────────
                if let Some((_, _, Some(SystemNodeKind::ReflectiveRetry { child_workflow_id, reflection_workflow_id, max_retries, timeout_secs: _ }))) =
                    self.node_meta.get(&node_id)
                {
                    let child_wf_id = *child_workflow_id;
                    let reflection_wf_id = *reflection_workflow_id;
                    let max_retries = *max_retries;
                    let initial_input = self.gather_inputs(node_idx, &results);

                    let reflective_result = self
                        .dispatch_reflective_retry(
                            initial_input,
                            child_wf_id,
                            reflection_wf_id,
                            max_retries,
                            dispatcher.clone(),
                            worker_shared_key.clone(),
                        )
                        .await;

                    results.insert(node_id, reflective_result);
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if pending.get(&child).copied().unwrap_or(1) == 0 { ready.push_back(child); }
                        }
                    }
                    continue;
                }

                // ── LlmDispatch dispatch (LLM-based routing) ──────────────────
                if let Some((_, _, Some(SystemNodeKind::LlmDispatch { classifier_workflow_id, ref routes, fallback_workflow_id, timeout_secs: _ }))) =
                    self.node_meta.get(&node_id)
                {
                    let classifier_wf_id = *classifier_workflow_id;
                    let routes = routes.clone();
                    let fallback_wf_id = *fallback_workflow_id;
                    let inputs = self.gather_inputs(node_idx, &results);

                    let llm_dispatch_result = self
                        .dispatch_llm_dispatch(
                            inputs,
                            classifier_wf_id,
                            routes,
                            fallback_wf_id,
                            dispatcher.clone(),
                            worker_shared_key.clone(),
                        )
                        .await;

                    results.insert(node_id, llm_dispatch_result);
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if pending.get(&child).copied().unwrap_or(1) == 0 { ready.push_back(child); }
                        }
                    }
                    continue;
                }

                // ── AgentLoop dispatch (ReAct-style iterative sub-workflow execution) ──
                if let Some((
                    _,
                    _,
                    Some(SystemNodeKind::AgentLoop {
                        body_workflow_id,
                        max_iterations,
                        inject_history,
                        timeout_secs,
                    }),
                )) = self.node_meta.get(&node_id)
                {
                    let body_wf_id = *body_workflow_id;
                    let max_iters = *max_iterations;
                    let do_inject_history = *inject_history;
                    let timeout_secs = *timeout_secs;
                    let inputs = self.gather_inputs(node_idx, &results);

                    tracing::info!(
                        node_id = %node_id,
                        body_workflow_id = %body_wf_id,
                        max_iterations = max_iters,
                        "AgentLoop — starting ReAct iteration loop"
                    );

                    let agent_result = if self.module_fetcher.is_some() {
                        let user_id = match self.user_id {
                            Some(uid) => uid,
                            None => {
                                results.insert(node_id, serde_json::json!({
                                    "__error": true,
                                    "error_message": "user_id required for sub-workflow execution"
                                }));
                                for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                                    if let Some(cnt) = pending.get_mut(&child) {
                                        if *cnt > 0 { *cnt -= 1; }
                                        if pending.get(&child).copied().unwrap_or(1) == 0 { ready.push_back(child); }
                                    }
                                }
                                continue;
                            }
                        };
                        let graph_row = self.get_sub_workflow_graph(body_wf_id, user_id).await;

                        if let Some(graph_json) = graph_row {
                            let dispatcher_al = dispatcher.clone();
                            let worker_shared_key_al = worker_shared_key.clone();
                            let adapter_set_al = self.adapter_set();
                            let inputs_al = inputs.clone();
                            let agent_result_inner = match tokio::time::timeout(
                                std::time::Duration::from_secs(timeout_secs),
                                async move {
                                    let mut history: Vec<JsonValue> = Vec::new();
                                    let mut last_output = serde_json::json!({});
                                    let mut finished = false;
                                    // Track total iterations separately from history.len() —
                                    // history is capped at AGENT_LOOP_MAX_HISTORY entries (sliding window) so
                                    // history.len() would under-report when max_iters > AGENT_LOOP_MAX_HISTORY.
                                    let mut iterations_run: u32 = 0;

                                    for iteration in 1..=max_iters {
                                        // Build iteration input: start with clean parent inputs
                                        let mut iter_input = if let Some(obj) = inputs_al.as_object() {
                                            let mut cleaned = obj.clone();
                                            cleaned.retain(|k, _| !k.starts_with("__"));
                                            cleaned
                                        } else {
                                            serde_json::Map::new()
                                        };

                                        iter_input.insert(
                                            "__agent_iteration__".to_string(),
                                            serde_json::json!(iteration),
                                        );

                                        if do_inject_history && !history.is_empty() {
                                            iter_input.insert(
                                                "__agent_history__".to_string(),
                                                serde_json::Value::Array(history.clone()),
                                            );
                                        }

                                        let iter_input_value = serde_json::Value::Object(iter_input);

                                        let iter_result = match adapter_set_al
                                            .clone()
                                            .into_engine_with_graph(&graph_json)
                                        {
                                            Ok(mut sub_engine) => {
                                                let sub_execution_id = Uuid::new_v4();
                                                let trigger_node_id = Uuid::new_v4();
                                                sub_engine.add_node(trigger_node_id, None, None, None);
                                                sub_engine.node_labels.insert(trigger_node_id, "__trigger__".to_string());

                                                let root_indices: Vec<petgraph::graph::NodeIndex> = sub_engine
                                                    .graph
                                                    .node_indices()
                                                    .filter(|&idx| {
                                                        sub_engine.graph[idx] != trigger_node_id
                                                            && sub_engine
                                                                .graph
                                                                .neighbors_directed(idx, Direction::Incoming)
                                                                .count()
                                                                == 0
                                                    })
                                                    .collect();
                                                for root_idx in &root_indices {
                                                    let root_id = sub_engine.graph[*root_idx];
                                                    let _ = sub_engine.add_edge(
                                                        trigger_node_id,
                                                        root_id,
                                                        workflow_engine_core::EdgeLogic {
                                                            source_handle: "output".to_string(),
                                                            target_handle: "input".to_string(),
                                                            mapping: None,
                                                            condition: None,
                                                            edge_type: "default".to_string(),
                                                        },
                                                    );
                                                }

                                                let mut initial_results = HashMap::new();
                                                initial_results.insert(trigger_node_id, iter_input_value);

                                                let sub_labels = sub_engine.node_labels.clone();
                                                match sub_engine
                                                    .run_with_seed_with_transport(
                                                        dispatcher_al.clone(),
                                                        worker_shared_key_al.clone(),
                                                        initial_results,
                                                        sub_execution_id,
                                                    )
                                                    .await
                                                {
                                                    Ok(ctx) => {
                                                        let mut sub_outputs = serde_json::Map::new();
                                                        for (nid, output) in &ctx.results {
                                                            if output.get("__skipped").and_then(|v| v.as_bool()).unwrap_or(false) {
                                                                continue;
                                                            }
                                                            let key = sub_labels.get(nid).cloned().unwrap_or_else(|| nid.to_string());
                                                            if key == "__trigger__" { continue; }
                                                            sub_outputs.insert(key, ParallelWorkflowEngine::unwrap_output(output).clone());
                                                        }
                                                        serde_json::Value::Object(sub_outputs)
                                                    }
                                                    Err(e) => {
                                                        tracing::warn!(
                                                            iteration,
                                                            error = %e,
                                                            "AgentLoop body workflow failed on iteration"
                                                        );
                                                        serde_json::json!({"__error": true, "error_message": e.to_string()})
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                serde_json::json!({"__error": true, "error_message": format!("Failed to build agent body: {}", e)})
                                            }
                                        };

                                        // Check for finish signals in the iteration output.
                                        let iter_finished = iter_result
                                            .get("finished")
                                            .and_then(|v| v.as_bool())
                                            .unwrap_or(false)
                                            || iter_result
                                                .get("action")
                                                .and_then(|v| v.as_str())
                                                .map(|s| s.eq_ignore_ascii_case("FINISH"))
                                                .unwrap_or(false);

                                        // Cap history entries to prevent unbounded memory growth
                                        // when inject_history is true and iterations produce large outputs.
                                        // Keep the last AGENT_LOOP_MAX_HISTORY entries (sufficient for ReAct reasoning chains).
                                        iterations_run += 1;
                                        if history.len() >= AGENT_LOOP_MAX_HISTORY {
                                            history.remove(0);
                                        }
                                        history.push(iter_result.clone());
                                        last_output = iter_result;

                                        if iter_finished {
                                            finished = true;
                                            break;
                                        }
                                    }

                                    if !finished {
                                        tracing::warn!(
                                            max_iterations = max_iters,
                                            "AgentLoop reached max_iterations without finish signal"
                                        );
                                    }

                                    serde_json::json!({
                                        "iterations": iterations_run,
                                        "finished": finished,
                                        "history": history,
                                        "final_output": last_output,
                                    })
                                },
                            ).await {
                                Ok(result) => result,
                                Err(_) => {
                                    tracing::warn!(
                                        node_id = %node_id,
                                        timeout_secs = timeout_secs,
                                        "AgentLoop timed out"
                                    );
                                    serde_json::json!({
                                        "__error": true,
                                        "error_message": format!("AgentLoop timed out after {}s", timeout_secs),
                                    })
                                }
                            };
                            agent_result_inner
                        } else {
                            serde_json::json!({
                                "__error": true,
                                "error_message": format!("AgentLoop body workflow {} not found", body_wf_id),
                            })
                        }
                    } else {
                        serde_json::json!({
                            "__error": true,
                            "error_message": "Registry not available for AgentLoop execution",
                        })
                    };

                    results.insert(node_id, agent_result);

                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if pending.get(&child).copied().unwrap_or(1) == 0 {
                                ready.push_back(child);
                            }
                        }
                    }
                    continue;
                }

                // ── WhileLoop dispatch (local computation) ──────────────────
                if let Some((
                    _,
                    _,
                    Some(SystemNodeKind::WhileLoop {
                        ref condition,
                        max_iterations,
                    }),
                )) = self.node_meta.get(&node_id)
                {
                    let condition = condition.clone();
                    let max_iters = *max_iterations;
                    let inputs = self.gather_inputs(node_idx, &results);

                    // WhileLoop runs the body inline, checking the condition after each iteration.
                    let mut current_output = inputs;
                    let mut iteration = 0u32;

                    while iteration < max_iters {
                        // Evaluate condition against current output
                        if !self.eval_bool(
                            &condition,
                            &current_output,
                        ) {
                            break;
                        }
                        iteration += 1;
                        // Store iteration result (each iteration overwrites)
                        current_output = serde_json::json!({
                            "__loop_iteration": iteration,
                            "__loop_input": current_output,
                        });
                    }

                    if iteration >= max_iters {
                        tracing::warn!(
                            node_id = %node_id,
                            max_iterations = max_iters,
                            "WhileLoop reached maximum iterations"
                        );
                    }

                    results.insert(
                        node_id,
                        serde_json::json!({
                            "iterations": iteration,
                            "output": current_output,
                        }),
                    );

                    // Unblock successors
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 {
                                *cnt -= 1;
                            }
                            if pending.get(&child).copied().unwrap_or(1) == 0 {
                                ready.push_back(child);
                            }
                        }
                    }
                    continue;
                }

                // ── RepeatLoop dispatch (local computation) ─────────────────
                if let Some((_, _, Some(SystemNodeKind::RepeatLoop { count }))) =
                    self.node_meta.get(&node_id)
                {
                    let count = *count;
                    let inputs = self.gather_inputs(node_idx, &results);

                    results.insert(
                        node_id,
                        serde_json::json!({
                            "iterations": count,
                            "input": inputs,
                        }),
                    );

                    // Unblock successors
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 {
                                *cnt -= 1;
                            }
                            if pending.get(&child).copied().unwrap_or(1) == 0 {
                                ready.push_back(child);
                            }
                        }
                    }
                    continue;
                }

                // ── SubWorkflow dispatch (real execution) ─────────────────
                if let Some((
                    _,
                    _,
                    Some(SystemNodeKind::SubWorkflow {
                        workflow_id: sub_wf_id,
                        timeout_secs: _,
                    }),
                )) = self.node_meta.get(&node_id)
                {
                    let sub_wf_id = *sub_wf_id;
                    let inputs = self.gather_inputs(node_idx, &results);

                    tracing::info!(
                        node_id = %node_id,
                        sub_workflow_id = %sub_wf_id,
                        "SubWorkflow node — executing sub-workflow"
                    );

                    let sub_result = self
                        .dispatch_subworkflow(
                            inputs,
                            sub_wf_id,
                            dispatcher.clone(),
                            worker_shared_key.clone(),
                        )
                        .await;

                    results.insert(node_id, sub_result);

                    // Unblock successors
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 {
                                *cnt -= 1;
                            }
                            if pending.get(&child).copied().unwrap_or(1) == 0 {
                                ready.push_back(child);
                            }
                        }
                    }
                    continue;
                }

                // ── DynamicDispatch (evaluate Rhai expression to select sub-workflow) ──
                if let Some((
                    _,
                    _,
                    Some(SystemNodeKind::DynamicDispatch {
                        ref dispatch_expression,
                        timeout_secs: _,
                    }),
                )) = self.node_meta.get(&node_id)
                {
                    let expression = dispatch_expression.clone();
                    let inputs = self.gather_inputs(node_idx, &results);

                    tracing::info!(
                        node_id = %node_id,
                        expression = %expression,
                        "DynamicDispatch node — evaluating dispatch expression"
                    );

                    // Evaluate the Rhai expression to get the target workflow ID
                    let dispatch_target: Result<String, String> = {
                        let mut rhai_engine = rhai::Engine::new();
                        rhai_engine.set_max_operations(10_000);
                        rhai_engine.disable_symbol("eval");
                        rhai_engine.set_module_resolver(rhai::module_resolvers::DummyModuleResolver);
                        let mut scope = rhai::Scope::new();
                        if let Some(obj) = inputs.as_object() {
                            for (k, v) in obj {
                                let dyn_val: rhai::Dynamic = match v {
                                    serde_json::Value::String(s) => rhai::Dynamic::from(s.clone()),
                                    serde_json::Value::Number(n) => {
                                        if let Some(i) = n.as_i64() {
                                            rhai::Dynamic::from(i)
                                        } else if let Some(f) = n.as_f64() {
                                            rhai::Dynamic::from(f)
                                        } else {
                                            rhai::Dynamic::from(n.to_string())
                                        }
                                    }
                                    serde_json::Value::Bool(b) => rhai::Dynamic::from(*b),
                                    _ => rhai::Dynamic::from(v.to_string()),
                                };
                                scope.push(k.clone(), dyn_val);
                            }
                        }
                        match rhai_engine.eval_with_scope::<rhai::Dynamic>(&mut scope, &expression)
                        {
                            Ok(result) => {
                                let s = result.to_string();
                                if s.is_empty() {
                                    Err("Dispatch expression returned empty string".to_string())
                                } else {
                                    Ok(s)
                                }
                            }
                            Err(e) => Err(format!("Dispatch expression evaluation failed: {}", e)),
                        }
                    };

                    let dispatch_result = match dispatch_target {
                        Ok(target_id_or_name) => {
                            let target_wf_id: Option<uuid::Uuid> = if let Ok(id) =
                                uuid::Uuid::parse_str(&target_id_or_name)
                            {
                                Some(id)
                            } else if let Some(ref store) = self.graph_store {
                                store
                                    .resolve_by_name(
                                        &target_id_or_name,
                                        self.user_id.unwrap_or_else(Uuid::nil),
                                    )
                                    .await
                                    .map_err(|e| {
                                        tracing::warn!(
                                            error = %e,
                                            "DB query failed during execution",
                                        );
                                        e
                                    })
                                    .ok()
                                    .flatten()
                            } else {
                                None
                            };

                            match target_wf_id {
                                Some(sub_wf_id) => {
                                    tracing::info!(node_id = %node_id, dispatched_workflow_id = %sub_wf_id, "DynamicDispatch resolved to workflow");
                                    if self.module_fetcher.is_some() {
                                        let graph_row = self.get_sub_workflow_graph(sub_wf_id, self.user_id.unwrap_or_else(Uuid::nil)).await;

                                        if let Some(graph_json) = graph_row {
                                            match self
                                                .adapter_set()
                                                .into_engine_with_graph(&graph_json)
                                            {
                                                Ok(mut sub_engine) => {
                                                    let sub_execution_id = Uuid::new_v4();
                                                    let clean_input = if let Some(obj) = inputs.as_object() {
                                                        let mut cleaned = obj.clone(); cleaned.retain(|k, _| !k.starts_with("__")); serde_json::Value::Object(cleaned)
                                                    } else { inputs.clone() };

                                                    let trigger_node_id = Uuid::new_v4();
                                                    sub_engine.add_node(trigger_node_id, None, None, None);
                                                    sub_engine.node_labels.insert(trigger_node_id, "__trigger__".to_string());
                                                    let root_indices: Vec<petgraph::graph::NodeIndex> = sub_engine.graph.node_indices()
                                                        .filter(|&idx| sub_engine.graph[idx] != trigger_node_id && sub_engine.graph.neighbors_directed(idx, Direction::Incoming).count() == 0)
                                                        .collect();
                                                    for root_idx in &root_indices {
                                                        let root_id = sub_engine.graph[*root_idx];
                                                        let _ = sub_engine.add_edge(trigger_node_id, root_id, workflow_engine_core::EdgeLogic {
                                                            source_handle: "output".to_string(), target_handle: "input".to_string(), mapping: None, condition: None, edge_type: "default".to_string(),
                                                        });
                                                    }

                                                    let mut initial_results = HashMap::new();
                                                    initial_results.insert(trigger_node_id, clean_input);
                                                    let sub_labels = sub_engine.node_labels.clone();
                                                    match sub_engine.run_with_seed_with_transport(dispatcher.clone(), worker_shared_key.clone(), initial_results, sub_execution_id).await {
                                                        Ok(ctx) => {
                                                            let mut sub_outputs = serde_json::Map::new();
                                                            sub_outputs.insert("__dispatched_workflow_id".to_string(), serde_json::json!(sub_wf_id.to_string()));
                                                            for (nid, output) in &ctx.results {
                                                                if output.get("__skipped").and_then(|v| v.as_bool()).unwrap_or(false) { continue; }
                                                                let key = sub_labels.get(nid).cloned().unwrap_or_else(|| nid.to_string());
                                                                if key == "__trigger__" { continue; }
                                                                sub_outputs.insert(key, Self::unwrap_output(output).clone());
                                                            }
                                                            serde_json::Value::Object(sub_outputs)
                                                        }
                                                        Err(e) => { tracing::error!(dispatched_workflow_id = %sub_wf_id, error = %e, "Dispatched workflow failed"); serde_json::json!({"__error": true, "error_message": format!("Dispatched workflow failed: {}", e)}) }
                                                    }
                                                }
                                                Err(e) => serde_json::json!({"__error": true, "error_message": format!("Failed to build dispatched workflow engine: {}", e)}),
                                            }
                                        } else {
                                            serde_json::json!({"__error": true, "error_message": format!("Dispatched workflow {} not found", sub_wf_id)})
                                        }
                                    } else {
                                        serde_json::json!({"__error": true, "error_message": "Registry not available for dispatch execution"})
                                    }
                                }
                                None => {
                                    serde_json::json!({"__error": true, "error_message": format!("Could not resolve dispatch target: {}", target_id_or_name)})
                                }
                            }
                        }
                        Err(e) => serde_json::json!({"__error": true, "error_message": e}),
                    };

                    results.insert(node_id, dispatch_result);
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 {
                                *cnt -= 1;
                            }
                            if pending.get(&child).copied().unwrap_or(1) == 0 {
                                ready.push_back(child);
                            }
                        }
                    }
                    continue;
                }

                // ── CapabilityDispatch (find best workflow by capability tags) ──
                if let Some((
                    _,
                    _,
                    Some(SystemNodeKind::CapabilityDispatch {
                        ref required_capabilities,
                        timeout_secs: _,
                    }),
                )) = self.node_meta.get(&node_id)
                {
                    let caps = required_capabilities.clone();
                    let inputs = self.gather_inputs(node_idx, &results);

                    tracing::info!(
                        node_id = %node_id,
                        capabilities = ?caps,
                        "CapabilityDispatch node — finding best matching workflow"
                    );

                    let capability_result = if let Some(ref store) = self.graph_store {
                        let matching_row = store
                            .resolve_by_capabilities(
                                &caps,
                                self.user_id.unwrap_or_else(Uuid::nil),
                            )
                            .await
                            .map_err(|e| {
                                tracing::warn!(
                                    error = %e,
                                    "DB query failed during execution",
                                );
                                e
                            })
                            .ok()
                            .flatten();

                        match matching_row {
                            Some((sub_wf_id, sub_wf_name)) => {
                                tracing::info!(node_id = %node_id, dispatched_workflow_id = %sub_wf_id, dispatched_workflow_name = %sub_wf_name, "CapabilityDispatch resolved to workflow");
                                let graph_row = self.get_sub_workflow_graph(sub_wf_id, self.user_id.unwrap_or_else(Uuid::nil)).await;

                                if let Some(graph_json) = graph_row {
                                    match self
                                        .adapter_set()
                                        .into_engine_with_graph(&graph_json)
                                    {
                                        Ok(mut sub_engine) => {
                                            let sub_execution_id = Uuid::new_v4();
                                            let clean_input = if let Some(obj) = inputs.as_object() {
                                                let mut cleaned = obj.clone(); cleaned.retain(|k, _| !k.starts_with("__")); serde_json::Value::Object(cleaned)
                                            } else { inputs.clone() };

                                            let trigger_node_id = Uuid::new_v4();
                                            sub_engine.add_node(trigger_node_id, None, None, None);
                                            sub_engine.node_labels.insert(trigger_node_id, "__trigger__".to_string());
                                            let root_indices: Vec<petgraph::graph::NodeIndex> = sub_engine.graph.node_indices()
                                                .filter(|&idx| sub_engine.graph[idx] != trigger_node_id && sub_engine.graph.neighbors_directed(idx, Direction::Incoming).count() == 0)
                                                .collect();
                                            for root_idx in &root_indices {
                                                let root_id = sub_engine.graph[*root_idx];
                                                let _ = sub_engine.add_edge(trigger_node_id, root_id, workflow_engine_core::EdgeLogic {
                                                    source_handle: "output".to_string(), target_handle: "input".to_string(), mapping: None, condition: None, edge_type: "default".to_string(),
                                                });
                                            }

                                            let mut initial_results = HashMap::new();
                                            initial_results.insert(trigger_node_id, clean_input);
                                            let sub_labels = sub_engine.node_labels.clone();
                                            match sub_engine.run_with_seed_with_transport(dispatcher.clone(), worker_shared_key.clone(), initial_results, sub_execution_id).await {
                                                Ok(ctx) => {
                                                    let mut sub_outputs = serde_json::Map::new();
                                                    sub_outputs.insert("__dispatched_workflow_id".to_string(), serde_json::json!(sub_wf_id.to_string()));
                                                    sub_outputs.insert("__dispatched_by".to_string(), serde_json::json!("capability_dispatch"));
                                                    sub_outputs.insert("__dispatched_workflow_name".to_string(), serde_json::json!(sub_wf_name));
                                                    sub_outputs.insert("__matched_capabilities".to_string(), serde_json::json!(caps));
                                                    for (nid, output) in &ctx.results {
                                                        if output.get("__skipped").and_then(|v| v.as_bool()).unwrap_or(false) { continue; }
                                                        let key = sub_labels.get(nid).cloned().unwrap_or_else(|| nid.to_string());
                                                        if key == "__trigger__" { continue; }
                                                        sub_outputs.insert(key, Self::unwrap_output(output).clone());
                                                    }
                                                    serde_json::Value::Object(sub_outputs)
                                                }
                                                Err(e) => { tracing::error!(dispatched_workflow_id = %sub_wf_id, error = %e, "Capability-dispatched workflow failed"); serde_json::json!({"__error": true, "error_message": format!("Capability-dispatched workflow failed: {}", e)}) }
                                            }
                                        }
                                        Err(e) => serde_json::json!({"__error": true, "error_message": format!("Failed to build capability-dispatched engine: {}", e)}),
                                    }
                                } else {
                                    serde_json::json!({"__error": true, "error_message": format!("Capability-dispatched workflow {} graph not found", sub_wf_id)})
                                }
                            }
                            None => {
                                serde_json::json!({"__error": true, "error_message": format!("No workflow found matching capabilities: {:?}", caps)})
                            }
                        }
                    } else {
                        serde_json::json!({"__error": true, "error_message": "Registry not available for capability dispatch"})
                    };

                    // If capability dispatch failed, check continue_on_error before propagating
                    if capability_result.get("__error").and_then(|v| v.as_bool()).unwrap_or(false) {
                        let continue_on_error = self
                            .node_configs
                            .get(&node_id)
                            .and_then(|c| c.get("__continue_on_error"))
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        if !continue_on_error {
                            let err_msg = capability_result
                                .get("error_message")
                                .and_then(|v| v.as_str())
                                .unwrap_or("capability dispatch failed")
                                .to_string();
                            tracing::error!(node_id = %node_id, error = %err_msg, "Capability dispatch failed — failing workflow");
                            return Err(format!("Capability dispatch node {}: {}", node_id, err_msg));
                        }
                        tracing::info!(node_id = %node_id, "Capability dispatch failed but continue_on_error is set — continuing");
                    }

                    results.insert(node_id, capability_result);
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 {
                                *cnt -= 1;
                            }
                            if pending.get(&child).copied().unwrap_or(1) == 0 {
                                ready.push_back(child);
                            }
                        }
                    }
                    continue;
                }

                // ── Loop dispatch (re-dispatches body node while condition is true) ──
                if let Some((
                    _,
                    _,
                    Some(SystemNodeKind::Loop {
                        ref condition,
                        max_iterations,
                    }),
                )) = self.node_meta.get(&node_id)
                {
                    let condition = condition.clone();
                    let max_iters = *max_iterations;
                    let inputs = self.gather_inputs(node_idx, &results);

                    // Find the body_node_id from node config
                    let body_node_id_str = self
                        .node_configs
                        .get(&node_id)
                        .and_then(|c| c.get("body_node_id"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());

                    let loop_result = if let Some(body_rf_id) = body_node_id_str {
                        // Resolve the body node's module_id via node_labels
                        let body_uuid = self
                            .node_labels
                            .iter()
                            .find(|(_, label)| label.as_str() == body_rf_id)
                            .map(|(uuid, _)| *uuid);

                        if let Some(body_uuid) = body_uuid {
                            let body_module_id =
                                self.node_meta.get(&body_uuid).and_then(|(mid, _, _)| *mid);

                            if let Some(body_module_id) = body_module_id {
                                let mut current_input = inputs.clone();
                                let mut iteration = 0u32;
                                let mut last_output = current_input.clone();

                                // Extract __trigger_input__ to inject into every loop iteration.
                                // Search: (1) gathered inputs, (2) the __trigger__ node's output in results
                                let trigger_input_val = inputs
                                    .as_object()
                                    .and_then(|o| o.get("__trigger_input__"))
                                    .cloned()
                                    .or_else(|| {
                                        // Find the trigger node by label and use its value
                                        self.node_labels
                                            .iter()
                                            .find(|(_, label)| label.as_str() == "__trigger__")
                                            .and_then(|(uuid, _)| results.get(uuid))
                                            .cloned()
                                    });

                                while iteration < max_iters {
                                    // Evaluate condition against current output + loop metadata.
                                    // `iteration_count` is injected so conditions like
                                    // `iteration_count < 3` work without the body having
                                    // to explicitly echo the counter in its output.
                                    if iteration > 0 {
                                        let condition_ctx = if let Some(mut obj) =
                                            last_output.as_object().cloned()
                                        {
                                            obj.entry("iteration_count".to_string())
                                                .or_insert(serde_json::json!(iteration));
                                            obj.entry("iteration".to_string())
                                                .or_insert(serde_json::json!(iteration));
                                            serde_json::Value::Object(obj)
                                        } else {
                                            serde_json::json!({
                                                "iteration_count": iteration,
                                                "iteration": iteration,
                                                "output": last_output,
                                            })
                                        };
                                        if !self.eval_bool(
                                            &condition,
                                            &condition_ctx,
                                        ) {
                                            break;
                                        }
                                    }

                                    iteration += 1;

                                    // Log iteration event
                                    emit_event_spawn(
                                        &self.event_sink,
                                        NodeEventWrite {
                                            execution_id,
                                            event_type: "loop_iteration".to_string(),
                                            node_id: Some(node_id),
                                            status: "Running".to_string(),
                                            log_message: Some(format!(
                                                "Loop iteration {}/{}",
                                                iteration, max_iters
                                            )),
                                            iteration_index: Some(iteration as i32),
                                        },
                                    );

                                    // Dispatch the body node's module
                                    // Use fetch_module for full resolution (wasm_modules → template_id → node_templates)
                                    let fetch_result = self
                                        .fetch_module(body_uuid)
                                        .await
                                        .map_err(|e| anyhow::anyhow!(e));

                                    match fetch_result {
                                        Ok(wasm_module) => {
                                            // Flat-merge input + config (same pattern as regular node dispatch)
                                            let mut merged_input = serde_json::Map::new();
                                            // Spread current_input fields at root level
                                            if let Some(obj) = current_input.as_object() {
                                                for (k, v) in obj {
                                                    merged_input.insert(k.clone(), v.clone());
                                                }
                                            }
                                            // Add config sub-key if present
                                            if let Some(cfg) = self.node_configs.get(&body_uuid) {
                                                if cfg.is_object()
                                                    && !cfg
                                                        .as_object()
                                                        .map(|m| m.is_empty())
                                                        .unwrap_or(true)
                                                {
                                                    merged_input
                                                        .insert("config".to_string(), cfg.clone());
                                                    // Also spread config fields at root for templates that read them directly
                                                    if let Some(obj) = cfg.as_object() {
                                                        for (k, v) in obj {
                                                            merged_input
                                                                .entry(k.clone())
                                                                .or_insert(v.clone());
                                                        }
                                                    }
                                                }
                                            }
                                            // Include input sub-key for modules that read it explicitly
                                            if !current_input.is_null()
                                                && current_input != serde_json::json!({})
                                            {
                                                merged_input
                                                    .entry("input".to_string())
                                                    .or_insert(current_input.clone());
                                            }
                                            // Inject __trigger_input__ into each loop iteration
                                            if let Some(ref ti) = trigger_input_val {
                                                merged_input.insert(
                                                    "__trigger_input__".to_string(),
                                                    ti.clone(),
                                                );
                                            }
                                            // Inject loop counter so body modules can read it.
                                            // `iteration` is already incremented (1-based).
                                            merged_input
                                                .entry("iteration_count".to_string())
                                                .or_insert(serde_json::json!(iteration));
                                            merged_input
                                                .entry("iteration".to_string())
                                                .or_insert(serde_json::json!(iteration));
                                            let job_input = serde_json::Value::Object(merged_input);

                                            let body_timeout_secs = self
                                                .node_timeouts
                                                .get(&body_uuid)
                                                .copied()
                                                .unwrap_or(30);
                                            let encrypted_secrets = self
                                                .build_encrypted_secrets(
                                                    body_module_id,
                                                    &worker_shared_key,
                                                )
                                                .await;
                                            let body_job = DispatchJob {
                                                execution_id,
                                                node_id: body_uuid,
                                                module_id: body_module_id,
                                                // Loop-body iterations don't pre-INSERT
                                                // module_executions rows; let the adapter
                                                // mint a fresh job_id.
                                                job_id: None,
                                                user_id: self.user_id.unwrap_or_else(uuid::Uuid::nil),
                                                actor_id: self.actor_id,
                                                module_uri: wasm_module
                                                    .oci_url
                                                    .clone()
                                                    .unwrap_or_else(|| {
                                                        format!("redis:wasm:{}", body_module_id)
                                                    }),
                                                wasm_bytes: None,
                                                expected_wasm_hash: Some(
                                                    wasm_module.content_hash.clone(),
                                                ),
                                                capability_world: Some(
                                                    wasm_module.capability_world.clone(),
                                                ),
                                                integration_name: wasm_module
                                                    .integration_name
                                                    .clone(),
                                                input_payload: job_input,
                                                timeout: std::time::Duration::from_secs(
                                                    body_timeout_secs,
                                                ),
                                                max_fuel: (wasm_module.max_fuel)
                                                    .min(50_000_000),
                                                allowed_hosts: wasm_module.allowed_hosts.clone(),
                                                allowed_methods: wasm_module
                                                    .allowed_methods
                                                    .clone(),
                                                allowed_secrets: wasm_module
                                                    .allowed_secrets
                                                    .clone(),
                                                allowed_sql_operations: vec![],
                                                allow_tier2_exposure: false,
                                                encrypted_secrets_ciphertext: encrypted_secrets
                                                    .ciphertext,
                                                encrypted_secrets_nonce: encrypted_secrets.nonce,
                                                priority: 100,
                                                dry_run: self.dry_run,
                                                max_retries: 2,
                                                backoff_ms: 500,
                                                retry_condition: None,
                                                retry_delay_expr: None,
                                                // Retries inside a loop iteration are
                                                // internal to the iteration and should not
                                                // inflate workflow-level retry metrics.
                                                emit_retry_events: false,
                                            };
                                            match dispatcher.dispatch(body_job).await {
                                                Ok(result) => {
                                                    // Unwrap the engine envelope so the next iteration
                                                    // receives clean output, not double-wrapped input
                                                    let clean =
                                                        Self::unwrap_output(&result.output).clone();
                                                    last_output = clean.clone();
                                                    current_input = clean;
                                                }
                                                Err(e) => {
                                                    last_output = serde_json::json!({"__error": true, "error_message": e.to_string()});
                                                    break;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            last_output = serde_json::json!({"__error": true, "error_message": format!("Module fetch failed: {}", e)});
                                            break;
                                        }
                                    }
                                }

                                if iteration >= max_iters {
                                    tracing::warn!(
                                        node_id = %node_id,
                                        max_iterations = max_iters,
                                        "Loop reached maximum iterations"
                                    );
                                }

                                serde_json::json!({
                                    "iterations": iteration,
                                    "output": last_output,
                                })
                            } else {
                                serde_json::json!({"__error": true, "error_message": format!("Body node '{}' has no module_id", body_rf_id)})
                            }
                        } else {
                            serde_json::json!({"__error": true, "error_message": format!("Body node '{}' not found in workflow", body_rf_id)})
                        }
                    } else {
                        serde_json::json!({"__error": true, "error_message": "Loop node missing body_node_id in config"})
                    };

                    results.insert(node_id, loop_result);

                    // Unblock successors
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 {
                                *cnt -= 1;
                            }
                            if pending.get(&child).copied().unwrap_or(1) == 0 {
                                ready.push_back(child);
                            }
                        }
                    }
                    continue;
                }

                // ── ErrorHandler dispatch (pattern filtering) ───────────────
                if let Some((_, _, Some(SystemNodeKind::ErrorHandler { ref error_pattern }))) =
                    self.node_meta.get(&node_id)
                {
                    let inputs = self.gather_inputs(node_idx, &results);

                    // Check if error matches the pattern filter (if specified)
                    if let Some(pattern) = error_pattern {
                        let error_msg = inputs
                            .get("error_message")
                            .or_else(|| {
                                // Check parent outputs for __error payloads
                                inputs.as_object().and_then(|obj| {
                                    obj.values().find_map(|v| v.get("error_message"))
                                })
                            })
                            .and_then(|v| v.as_str())
                            .unwrap_or("");

                        if !error_msg.contains(pattern.as_str()) {
                            // Error doesn't match pattern — skip this handler, propagate error
                            results.insert(
                                node_id,
                                serde_json::json!({
                                    "__skipped": true,
                                    "reason": "error_pattern_mismatch",
                                }),
                            );

                            for child in
                                self.graph.neighbors_directed(node_idx, Direction::Outgoing)
                            {
                                if let Some(cnt) = pending.get_mut(&child) {
                                    if *cnt > 0 {
                                        *cnt -= 1;
                                    }
                                    if pending.get(&child).copied().unwrap_or(1) == 0 {
                                        ready.push_back(child);
                                    }
                                }
                            }
                            continue;
                        }
                    }
                    // If pattern matches (or no pattern), fall through to normal dispatch below
                }

                // ── Single-node dispatch ─────────────────────────────────────

                // ── Rate limit check ──────────────────────────────────────
                evict_stale_rate_limits();
                let module_id_resolved = self.resolve_module_id(node_id);
                if let Some(&limit) = self.rate_limits.get(&module_id_resolved) {
                    if limit > 0 {
                        let now = std::time::Instant::now();
                        let mut entry = MODULE_RATE_LIMITS
                            .entry(module_id_resolved)
                            .or_insert((now, 0));
                        if now.duration_since(entry.0) > std::time::Duration::from_secs(60) {
                            entry.0 = now;
                            entry.1 = 0;
                        }
                        entry.1 += 1;
                        if entry.1 > limit as u32 {
                            tracing::warn!(
                                node_id = %node_id,
                                module_id = %module_id_resolved,
                                rate_limit = limit,
                                "Module rate limit exceeded"
                            );
                            results.insert(node_id, serde_json::json!({
                                "__error": true,
                                "error_message": format!("Module rate limit exceeded ({}/min)", limit)
                            }));
                            for child in
                                self.graph.neighbors_directed(node_idx, Direction::Outgoing)
                            {
                                if let Some(cnt) = pending.get_mut(&child) {
                                    if *cnt > 0 {
                                        *cnt -= 1;
                                    }
                                    if pending.get(&child).copied().unwrap_or(1) == 0 {
                                        ready.push_back(child);
                                    }
                                }
                            }
                            continue;
                        }
                    }
                }

                let retry = self
                    .node_meta
                    .get(&node_id)
                    .and_then(|(_, rp, _)| rp.clone())
                    .unwrap_or_default();
                let inputs = self.gather_inputs(node_idx, &results);
                let dispatcher_clone = dispatcher.clone();
                let user_id_clone = self.user_id;
                let fetch_fut = self.fetch_module(node_id);
                let secrets_resolver = self.secrets_resolver.clone();
                let approval_gate = self.approval_gate.clone();
                let _exec_sandbox = execution_sandbox.clone();
                let single_user_id = self.user_id;
                let worker_shared_key_clone = worker_shared_key.clone();
                let node_configs_clone = self.node_configs.clone();
                let node_timeouts_clone = self.node_timeouts.clone();
                let event_sink_clone = self.event_sink.clone();
                let dry_run = self.dry_run;
                // Build accumulated context snapshot from all completed node
                // results so far, keyed by node label with __-prefixed metadata
                // stripped. Captured into the async block as a plain Option<Value>.
                let accumulated_snapshot =
                    Self::build_accumulated_context(&self.node_labels, &results);

                let fut = async move {
                    let wasm_module = match fetch_fut.await {
                        Ok(m) => m,
                        Err(e) => return (node_idx, Err(e)),
                    };

                    // ── Approval gate ───────────────────────────────────────
                    // If the module declares `requires_approval_for`, verify
                    // that an approved record exists before dispatching.
                    if !wasm_module.requires_approval_for.is_empty() {
                        if let Some(ref gate) = approval_gate {
                            let approval_webhook = node_configs_clone
                                .get(&node_id)
                                .and_then(|cfg| cfg.get("NOTIFICATION_WEBHOOK"))
                                .and_then(|v| v.as_str());
                            match gate
                                .check_or_request(
                                    execution_id, // workflow-level execution ID
                                    node_id,
                                    &wasm_module.requires_approval_for,
                                    approval_webhook,
                                )
                                .await
                            {
                                Ok(workflow_engine_core::ApprovalStatus::Approved) => {
                                    /* proceed */
                                }
                                Ok(workflow_engine_core::ApprovalStatus::Pending) => {
                                    return (
                                        node_idx,
                                        Err(format!(
                                            "Execution paused: module {} requires approval for {:?}. \
                                             An approval request has been created.",
                                            node_id, wasm_module.requires_approval_for
                                        )),
                                    );
                                }
                                Ok(workflow_engine_core::ApprovalStatus::Denied { reason }) => {
                                    return (node_idx, Err(reason));
                                }
                                Err(e) => {
                                    tracing::error!(
                                        node_id = %node_id,
                                        "Approval gate check failed: {}",
                                        e
                                    );
                                    return (
                                        node_idx,
                                        Err(format!("Approval gate check failed: {}", e)),
                                    );
                                }
                            }
                        }
                    }

                    // Read the module's compile-time config from the
                    // artifact we already fetched. `wasm_module` came from
                    // `fetch_fut` above (which hit `ModuleFetcher::fetch`);
                    // `ModuleArtifact::config` mirrors `wasm_modules.config`,
                    // so this avoids a separate `reg.get_module_config`
                    // round-trip the engine previously made.
                    //
                    // The previously-inlined `reg.ensure_module_in_cache`
                    // best-effort Redis warm is dropped — it was advisory
                    // only (its own comment flagged it as non-fatal and
                    // sometimes mis-keyed). Dispatch embeds `wasm_bytes`
                    // directly in the JobRequest, so the worker never
                    // depends on the pre-warm.
                    if single_user_id.is_none() {
                        return (
                            node_idx,
                            Err("Module execution requires user context (user_id not set)"
                                .to_string()),
                        );
                    }
                    let module_config = wasm_module
                        .config
                        .clone()
                        .unwrap_or_else(|| serde_json::json!({}));

                    // Merge node-level config from graph_json (takes precedence)
                    // Filter out internal keys (__skip_condition, skip_condition) that shouldn't be passed to modules
                    let module_config = if let Some(node_cfg) = node_configs_clone.get(&node_id) {
                        if module_config.is_object() && node_cfg.is_object() {
                            let mut merged = module_config.as_object().cloned().unwrap_or_default();
                            if let Some(node_cfg_obj) = node_cfg.as_object() {
                                for (k, v) in node_cfg_obj {
                                    if k == "__skip_condition"
                                        || k == "skip_condition"
                                        || k == "__continue_on_error"
                                        || k == "continue_on_error"
                                    {
                                        continue;
                                    }
                                    merged.insert(k.clone(), v.clone());
                                }
                            }
                            serde_json::Value::Object(merged)
                        } else if module_config == serde_json::json!({}) {
                            node_cfg.clone()
                        } else {
                            module_config
                        }
                    } else {
                        module_config
                    };

                    // Merge config and input into a flat object so templates can
                    // find their fields at the top level (e.g., "text", "URL").
                    // Also include "config" and "input" for backwards compatibility.
                    let wrapped_input = {
                        let mut merged = serde_json::Map::new();
                        // Start with config fields at top level
                        if let Some(obj) = module_config.as_object() {
                            for (k, v) in obj {
                                merged.insert(k.clone(), v.clone());
                            }
                        }
                        // Overlay input fields at top level (input takes precedence)
                        if let Some(obj) = inputs.as_object() {
                            for (k, v) in obj {
                                merged.insert(k.clone(), v.clone());
                            }
                        } else if !inputs.is_null() {
                            merged.insert("input".to_string(), inputs.clone());
                        }
                        // Always include config and input sub-objects for templates
                        // that explicitly read from these keys
                        // Only include "config" if it has actual content (skip empty {})
                        if module_config != serde_json::json!({}) {
                            merged.insert("config".to_string(), module_config.clone());
                        }
                        // Always include "input" sub-key for non-null, non-empty upstream
                        // outputs so downstream modules can access data["input"] regardless
                        // of whether the upstream returned an object or a scalar.
                        let is_empty_object = inputs.as_object().map(|m| m.is_empty()).unwrap_or(false);
                        if !inputs.is_null() && !is_empty_object {
                            merged.insert("input".to_string(), inputs.clone());
                        }
                        // Inject accumulated context: all prior nodes' outputs
                        // keyed by label, with __-prefixed metadata stripped.
                        if let Some(acc) = &accumulated_snapshot {
                            merged.insert("__accumulated__".to_string(), acc.clone());
                        }
                        // Inject actor memory context into every node.
                        if let Some(ref ctx) = self.actor_context {
                            merged.insert("__actor_context__".to_string(), ctx.clone());
                        }
                        serde_json::Value::Object(merged)
                    };

                    // Store truncated node input for debugging (node I/O inspector)
                    {
                        let input_preview = {
                            let s = serde_json::to_string(&wrapped_input).unwrap_or_default();
                            if s.len() > 4096 { format!("{}...(truncated)", &s[..4096]) } else { s }
                        };
                        emit_event_spawn(
                            &event_sink_clone,
                            NodeEventWrite {
                                execution_id,
                                event_type: "node_input".to_string(),
                                node_id: Some(node_id),
                                status: "Input".to_string(),
                                log_message: Some(input_preview),
                                iteration_index: None,
                            },
                        );
                    }

                    let job_id = Uuid::new_v4();

                    if let Some(ref store) = self.module_execution_store {
                        // Resolve the actual wasm_modules.id for the FK.
                        // `module_id_resolved` may be a node_template UUID
                        // (Fallback 2 path) not present in wasm_modules;
                        // the store's resolver maps template → wasm_modules
                        // by most-recent compile.
                        let actual_module_id =
                            store.resolve_wasm_module_id(module_id_resolved).await;
                        if let Err(db_err) = store
                            .record_started(
                                job_id,
                                actual_module_id,
                                single_user_id.unwrap_or_else(Uuid::new_v4),
                                execution_id,
                                &inputs,
                                "webhook",
                                // Race-safe: if a sibling has already failed
                                // the workflow, this row enters as
                                // 'cancelled' rather than 'running', closing
                                // the race with the failure-path UPDATE.
                                true,
                            )
                            .await
                        {
                            tracing::error!(
                                "module_execution_store.record_started failed: {}",
                                db_err
                            );
                        }
                    }

                    // Per-node fuel limit: config override > module default, capped at 50M.
                    let node_max_fuel = module_config
                        .get("max_fuel")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(wasm_module.max_fuel)
                        .min(50_000_000);

                    // Resolve encrypted secrets payload (opaque bytes at this layer).
                    let encrypted_secrets = match (
                        secrets_resolver.as_ref(),
                        &worker_shared_key_clone,
                    ) {
                        (Some(resolver), Some(key)) => {
                            let vault_paths = extract_vault_paths(&module_config);
                            build_encrypted_secrets_for(
                                resolver.as_ref(),
                                module_id_resolved,
                                single_user_id,
                                &vault_paths,
                                &wasm_module.allowed_secrets,
                                key,
                            )
                            .await
                        }
                        _ => Default::default(),
                    };

                    // Wire-format WASM budget. The dispatcher internally adds its
                    // own Tokio-outer grace on top (see TOKIO_WRAP_GRACE_SECS).
                    let node_timeout_secs =
                        node_timeouts_clone.get(&node_id).copied().unwrap_or(*DEFAULT_NODE_TIMEOUT_SECS);

                    let job = DispatchJob {
                        execution_id,
                        node_id,
                        module_id: module_id_resolved,
                        // Pre-INSERTed module_executions row is keyed by this id;
                        // thread it through so the worker's UPDATE lands on the
                        // same row and worker logs stay correlated.
                        job_id: Some(job_id),
                        user_id: user_id_clone.unwrap_or_else(uuid::Uuid::nil),
                        actor_id: self.actor_id,
                        module_uri: wasm_module
                            .oci_url
                            .clone()
                            .unwrap_or_else(|| format!("redis:wasm:{}", module_id_resolved)),
                        // Embed bytes directly: worker uses these without a Redis lookup,
                        // bypassing the "wasm:{uid}:{id}" vs "wasm:{id}" key mismatch and
                        // the template-UUID failure in ensure_module_in_cache. OCI modules
                        // have empty wasm_bytes (fetched by the worker from the registry).
                        wasm_bytes: if wasm_module.wasm_bytes.is_empty() { None } else { Some(wasm_module.wasm_bytes.clone()) },
                        // For OCI modules (wasm_bytes empty), commit the expected hash so the
                        // worker can verify the fetched content matches what we compiled.
                        expected_wasm_hash: if wasm_module.wasm_bytes.is_empty() {
                            Some(wasm_module.content_hash.clone())
                        } else {
                            None // HMAC already covers sha256(inline_bytes)
                        },
                        capability_world: Some(wasm_module.capability_world.clone()),
                        integration_name: wasm_module.integration_name.clone(),
                        input_payload: wrapped_input,
                        timeout: std::time::Duration::from_secs(node_timeout_secs),
                        max_fuel: node_max_fuel,
                        allowed_hosts: wasm_module.allowed_hosts.clone(),
                        allowed_methods: wasm_module.allowed_methods.clone(),
                        allowed_secrets: wasm_module.allowed_secrets.clone(),
                        allowed_sql_operations: vec![],
                        allow_tier2_exposure: false,
                        encrypted_secrets_ciphertext: encrypted_secrets.ciphertext,
                        encrypted_secrets_nonce: encrypted_secrets.nonce,
                        priority: 100,
                        dry_run,
                        max_retries: retry.max_retries,
                        backoff_ms: retry.backoff_ms,
                        retry_condition: retry.retry_condition.clone(),
                        retry_delay_expr: retry.retry_delay_expression.clone(),
                        emit_retry_events: true,
                    };

                    match dispatcher_clone.dispatch(job).await {
                        Ok(result) => {
                            tracing::info!(node_id = %node_id, "Node execution succeeded");
                            (node_idx, Ok(result.output))
                        }
                        Err(e) => (node_idx, Err(e.to_string())),
                    }
                };
                executing.push(Box::pin(fut));

                // ── Speculative module prefetch (P10) ────────────────────────
                // When a node has `speculative_prefetch: true`, kick off background
                // fetch tasks for all direct successors while this node executes.
                // The successor's fetch_module call will hit the cache (sub-ms) instead
                // of paying the DB round-trip latency after this node completes.
                //
                // Safety limits:
                //   - Max 8 successors prefetched per node (prevents fan-out DoS)
                //   - 5-second fetch timeout (prevents hung tasks from leaking memory)
                //   - DashMap insert is atomic; duplicate spawns are suppressed by
                //     entry().or_insert_with() semantics in the cache layer
                if self
                    .node_configs
                    .get(&node_id)
                    .and_then(|c| c.get("speculative_prefetch"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    for succ_idx in self
                        .graph
                        .neighbors_directed(node_idx, Direction::Outgoing)
                        .take(MAX_PREFETCH_SUCCESSORS)
                    {
                        let succ_id = self.graph[succ_idx];
                        // Skip system nodes — they have no module in the registry (resolve_module_id
                        // returns the node UUID as a fallback). Fetching would waste a 5-second
                        // timeout and generate noisy debug log entries for every system successor.
                        let succ_module_id = match self.node_meta.get(&succ_id)
                            .and_then(|(mid, _, _)| *mid)
                        {
                            Some(mid) => mid,
                            None => continue,
                        };
                        let prefetch_cache = Arc::clone(&self.module_prefetch_cache);
                        if let Some(ref fetcher) = self.module_fetcher {
                            let fetcher = Arc::clone(fetcher);
                            let uid = self.user_id;
                            tokio::spawn(async move {
                                // Atomic duplicate suppression via vacant-entry check:
                                // only one spawn proceeds to fetch; others see the key
                                // already present and return immediately.
                                if prefetch_cache.contains_key(&succ_id) {
                                    return;
                                }
                                if let Some(uid) = uid {
                                    // 5-second timeout: prevents hung prefetch tasks from
                                    // leaking tokio task slots if the registry is unresponsive.
                                    let fetch_result = tokio::time::timeout(
                                        std::time::Duration::from_secs(5),
                                        fetcher.fetch(succ_module_id, uid),
                                    )
                                    .await;
                                    match fetch_result {
                                        Ok(Ok(artifact)) => {
                                            // Use entry().or_insert to avoid overwriting a
                                            // result that another concurrent spawn already stored.
                                            prefetch_cache.entry(succ_id).or_insert(artifact);
                                            tracing::debug!(
                                                succ_id = %succ_id,
                                                "speculative prefetch: module cached"
                                            );
                                        }
                                        Ok(Err(e)) => {
                                            tracing::debug!(
                                                succ_id = %succ_id,
                                                error = %e,
                                                "speculative prefetch: fetch failed (normal dispatch will retry)"
                                            );
                                        }
                                        Err(_) => {
                                            tracing::debug!(
                                                succ_id = %succ_id,
                                                "speculative prefetch: timed out (normal dispatch will fetch)"
                                            );
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            }

            // Await next finished task.
            if let Some((finished_idx, exec_result)) = executing.next().await {
                let finished_id = self.graph[finished_idx];
                match exec_result {
                    Ok(output) => {
                        // Log node_completed event synchronously so child node_started
                        // events (which are fire-and-forget) are always ordered after
                        // this insert in the DB — fixes causally-inconsistent timelines.
                        if let Some(ref sink) = self.event_sink {
                            sink.emit(NodeEventWrite {
                                execution_id,
                                event_type: "node_completed".to_string(),
                                node_id: Some(finished_id),
                                status: "Completed".to_string(),
                                log_message: None,
                                iteration_index: None,
                            })
                            .await;
                        }
                        // For a pipeline result, mark ALL chain nodes as complete so
                        // their successors become ready.  The result is stored only for
                        // the last node (which is what `finished_idx` points to).
                        //
                        // Per-node output size guard: reject outputs larger than 5 MiB.
                        // Without this, a single misbehaving node can produce a multi-MB
                        // JSON value that is then cloned into every downstream node's
                        // gathered_inputs and into the final aggregated workflow output,
                        // leading to cascading memory exhaustion.
                        const MAX_NODE_OUTPUT_BYTES: usize = 5 * 1024 * 1024; // 5 MiB
                        let output = match serde_json::to_vec(&output) {
                            Ok(bytes) if bytes.len() > MAX_NODE_OUTPUT_BYTES => {
                                tracing::warn!(
                                    node_id = %finished_id,
                                    bytes = bytes.len(),
                                    limit = MAX_NODE_OUTPUT_BYTES,
                                    "Node output exceeds 5 MiB limit — replacing with error"
                                );
                                serde_json::json!({
                                    "__error": true,
                                    "error": format!(
                                        "Node output too large ({} bytes > {} byte limit). \
                                         Reduce the amount of data returned by this node.",
                                        bytes.len(), MAX_NODE_OUTPUT_BYTES
                                    )
                                })
                            }
                            _ => output,
                        };
                        let mut output = output;
                        sanitize_node_output(&mut output);
                        results.insert(finished_id, output.clone());

                        // Post-completion hook: drives fuel attribution,
                        // __memory_write__ persistence, and any future
                        // cross-cutting per-node observers. Fire-and-forget —
                        // the hook returns quickly; impls spawn internally.
                        // `run()` doesn't track per-node wall time (only
                        // `run_with_seed` does), so wall_time_ms is reported
                        // as 0 ("unknown") here per trait contract.
                        if let Some(hook) = self.node_hook.as_ref() {
                            let node_label =
                                self.node_labels.get(&finished_id).map(String::as_str);
                            let module_id = self
                                .node_meta
                                .get(&finished_id)
                                .and_then(|(m, _, _)| *m);
                            hook.on_node_completed(
                                workflow_engine_core::NodeCompletionContext {
                                    workflow_id: self.workflow_id.unwrap_or(execution_id),
                                    execution_id,
                                    node_id: finished_id,
                                    node_label,
                                    module_id,
                                    actor_id: self.actor_id,
                                    wall_time_ms: 0,
                                },
                                &output,
                            );
                        }

                        // If this was a chain execution, also clear pending for
                        // interior chain nodes (they have already run in the pipeline).
                        if let Some(&chain_idx) = node_to_chain.get(&finished_idx) {
                            for &n in &chains[chain_idx] {
                                pending.insert(n, 0); // Mark all chain nodes as done.
                            }
                        }

                        // Decrement children counters for finished_idx's successors.
                        // On SUCCESS, skip error-edge children (they only fire on failure).
                        for child in self
                            .graph
                            .neighbors_directed(finished_idx, Direction::Outgoing)
                        {
                            let is_error_edge = self
                                .graph
                                .edges_connecting(finished_idx, child)
                                .any(|e| e.weight().edge_type == "error");
                            if is_error_edge {
                                // Skip error-edge children on success
                                let child_id = self.graph[child];
                                results.insert(child_id, serde_json::json!({"__skipped": true}));
                                continue;
                            }
                            if let Some(cnt) = pending.get_mut(&child) {
                                *cnt -= 1;

                                // FanIn early-ready logic: some join modes don't
                                // require ALL parents to complete.
                                if let Some((
                                    _,
                                    _,
                                    Some(SystemNodeKind::FanIn { ref join_mode, .. }),
                                )) = self.node_meta.get(&self.graph[child])
                                {
                                    let total_parents = self
                                        .graph
                                        .neighbors_directed(child, Direction::Incoming)
                                        .count();
                                    let completed_parents = total_parents - *cnt;
                                    match join_mode {
                                        JoinMode::Any => {
                                            if *cnt > 0 {
                                                pending.insert(child, 0);
                                            }
                                        }
                                        JoinMode::Majority => {
                                            if completed_parents > total_parents / 2 && *cnt > 0 {
                                                pending.insert(child, 0);
                                            }
                                        }
                                        JoinMode::N(n) => {
                                            if completed_parents >= *n as usize && *cnt > 0 {
                                                pending.insert(child, 0);
                                            }
                                        }
                                        JoinMode::All => {} // default behavior
                                    }
                                }

                                if pending.get(&child).copied().unwrap_or(1) == 0 {
                                    // Check edge conditions before enqueuing.
                                    let child_node_id = self.graph[child];
                                    let mut condition_failed = false;
                                    for edge_ref in self.graph.edges_connecting(finished_idx, child)
                                    {
                                        tracing::debug!(
                                            condition = ?edge_ref.weight().condition,
                                            edge_type = %edge_ref.weight().edge_type,
                                            child = %child_node_id,
                                            "Evaluating edge"
                                        );
                                        if let Some(ref cond) = edge_ref.weight().condition {
                                            let unwrapped = Self::unwrap_output(&output);
                                            if !self.eval_bool(cond, unwrapped) {
                                                tracing::info!(
                                                    child_node_id = %child_node_id,
                                                    condition = %cond,
                                                    output_keys = ?unwrapped
                                                        .as_object()
                                                        .map(|m| m.keys().cloned().collect::<Vec<_>>())
                                                        .unwrap_or_default(),
                                                    "Edge condition false — child node will be skipped"
                                                );
                                                condition_failed = true;
                                                break;
                                            }
                                        }
                                    }
                                    if condition_failed {
                                        tracing::info!(
                                            node_id = %child_node_id,
                                            "Skipping node: edge condition evaluated to false"
                                        );
                                        // Store a skip marker so downstream nodes know this path was not taken.
                                        results.insert(
                                            child_node_id,
                                            serde_json::json!({"__skipped": true}),
                                        );
                                        // Cascade skip: decrement pending counts for the skipped node's children.
                                        for grandchild in self
                                            .graph
                                            .neighbors_directed(child, Direction::Outgoing)
                                        {
                                            if let Some(gc_cnt) = pending.get_mut(&grandchild) {
                                                if *gc_cnt > 0 {
                                                    *gc_cnt -= 1;
                                                }
                                                // Note: grandchild will be picked up if its
                                                // pending count reaches 0 in a future iteration.
                                            }
                                        }
                                    } else {
                                        ready.push_back(child);
                                    }
                                }
                            }
                        }
                    }
                    Err(error_msg) => {
                        // Two-pass scrub: value-based (known secrets) then regex DLP patterns.
                        let error_msg = self.redact_str(
                            &exec_ctx
                                .as_ref()
                                .map(|c| c.redact_error(&error_msg))
                                .unwrap_or_else(|| error_msg.clone()),
                        );
                        // Log node_failed event synchronously — same ordering guarantee
                        // as node_completed: child routing happens after this commit.
                        if let Some(ref sink) = self.event_sink {
                            sink.emit(NodeEventWrite {
                                execution_id,
                                event_type: "node_failed".to_string(),
                                node_id: Some(finished_id),
                                status: "Failed".to_string(),
                                log_message: Some(error_msg.clone()),
                                iteration_index: None,
                            })
                            .await;
                        }
                        // Check if this node has outgoing "error" edges
                        let error_children: Vec<NodeIndex> = self
                            .graph
                            .neighbors_directed(finished_idx, Direction::Outgoing)
                            .filter(|&child_idx| {
                                if let Some(edge_idx) =
                                    self.graph.find_edge(finished_idx, child_idx)
                                {
                                    self.graph[edge_idx].edge_type == "error"
                                } else {
                                    false
                                }
                            })
                            .collect();

                        if !error_children.is_empty() {
                            // Route error to error handler nodes instead of failing
                            let error_payload = serde_json::json!({
                                "__error": true,
                                "error_message": error_msg,
                                "failed_node": self.node_labels.get(&finished_id).cloned().unwrap_or_else(|| finished_id.to_string()),
                            });
                            results.insert(finished_id, error_payload.clone());
                            tracing::info!(
                                node_id = %finished_id,
                                error_handlers = error_children.len(),
                                "Node failed but has error handler edges — routing to error handlers"
                            );

                            // If this was a chain execution, also clear pending for
                            // interior chain nodes.
                            if let Some(&chain_idx) = node_to_chain.get(&finished_idx) {
                                for &n in &chains[chain_idx] {
                                    pending.insert(n, 0);
                                }
                            }

                            // Unblock ONLY error-edge children; skip default/conditional children.
                            // Default-edge children should NOT fire when the node fails.
                            for child in self
                                .graph
                                .neighbors_directed(finished_idx, Direction::Outgoing)
                            {
                                // Check if ANY edge to this child is an error edge
                                let has_error_edge = self
                                    .graph
                                    .edges_connecting(finished_idx, child)
                                    .any(|e| e.weight().edge_type == "error");
                                if !has_error_edge {
                                    // Skip default/conditional children — parent failed, success path is dead
                                    let child_id = self.graph[child];
                                    results
                                        .insert(child_id, serde_json::json!({"__skipped": true}));
                                    continue;
                                }

                                if let Some(cnt) = pending.get_mut(&child) {
                                    if *cnt > 0 {
                                        *cnt -= 1;
                                    }

                                    // FanIn early-ready logic
                                    if let Some((
                                        _,
                                        _,
                                        Some(SystemNodeKind::FanIn { ref join_mode, .. }),
                                    )) = self.node_meta.get(&self.graph[child])
                                    {
                                        let total_parents = self
                                            .graph
                                            .neighbors_directed(child, Direction::Incoming)
                                            .count();
                                        let completed_parents = total_parents - *cnt;
                                        match join_mode {
                                            JoinMode::Any => {
                                                if *cnt > 0 {
                                                    pending.insert(child, 0);
                                                }
                                            }
                                            JoinMode::Majority => {
                                                if completed_parents > total_parents / 2 && *cnt > 0
                                                {
                                                    pending.insert(child, 0);
                                                }
                                            }
                                            JoinMode::N(n) => {
                                                if completed_parents >= *n as usize && *cnt > 0 {
                                                    pending.insert(child, 0);
                                                }
                                            }
                                            JoinMode::All => {} // default behavior
                                        }
                                    }

                                    if pending.get(&child).copied().unwrap_or(1) == 0 {
                                        ready.push_back(child);
                                    }
                                }
                            }
                        } else if self
                            .node_configs
                            .get(&finished_id)
                            .and_then(|c| c.get("__continue_on_error"))
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false)
                        {
                            // continue_on_error: store error result but don't fail the workflow
                            tracing::info!(
                                node_id = %finished_id,
                                "Node failed but continue_on_error is set — continuing execution"
                            );
                            results.insert(
                                finished_id,
                                serde_json::json!({
                                    "__error": true,
                                    "error_message": error_msg,
                                    "__continued": true,
                                }),
                            );
                            // Unblock successors (same as success path)
                            for child in self
                                .graph
                                .neighbors_directed(finished_idx, Direction::Outgoing)
                            {
                                if let Some(cnt) = pending.get_mut(&child) {
                                    if *cnt > 0 {
                                        *cnt -= 1;
                                    }
                                    if pending.get(&child).copied().unwrap_or(1) == 0 {
                                        ready.push_back(child);
                                    }
                                }
                            }
                        } else {
                            // No error handlers — notify the lifecycle hook (DLQ +
                            // sibling-cancellation responsibility) and propagate
                            // failure. The hook spawns both SQL writes so they
                            // don't delay the abort return.
                            if let Some(hook) = self.node_hook.as_ref() {
                                let node_label =
                                    self.node_labels.get(&finished_id).map(String::as_str);
                                let module_id = self
                                    .node_meta
                                    .get(&finished_id)
                                    .and_then(|(m, _, _)| *m);
                                hook.on_node_failed(
                                    workflow_engine_core::NodeCompletionContext {
                                        workflow_id: self.workflow_id.unwrap_or(execution_id),
                                        execution_id,
                                        node_id: finished_id,
                                        node_label,
                                        module_id,
                                        actor_id: self.actor_id,
                                        wall_time_ms: 0,
                                    },
                                    &error_msg,
                                    results.get(&finished_id),
                                );
                            }
                            let node_label = self
                                .node_labels
                                .get(&finished_id)
                                .cloned()
                                .unwrap_or_else(|| finished_id.to_string());
                            // Clear prefetch cache before returning so unconsumed WASM
                            // modules (potentially MBs each) are not retained in the
                            // engine's Arc for the lifetime of the caller.
                            self.module_prefetch_cache.clear();
                            return Err(format!("node '{}' failed: {}", node_label, error_msg));
                        }
                    }
                }
            }
        }

        // Two-pass scrub: value-based then regex DLP patterns.
        // Prevents secrets from node configs being stored in the execution trace.
        let results: HashMap<Uuid, JsonValue> = results
            .into_iter()
            .map(|(k, v)| {
                let v = exec_ctx
                    .as_ref()
                    .map(|c| c.redact_output(&v))
                    .unwrap_or(v);
                (k, self.redact_json(&v))
            })
            .collect();

        // Release any unconsumed prefetch cache entries — skipped branches leave
        // stale WASM bytebuffers (potentially MBs each) in memory indefinitely.
        self.module_prefetch_cache.clear();

        Ok(WorkflowContext {
            results,
            ..Default::default()
        })
    }

    /// Execute the graph with pre-seeded node results (e.g., from a webhook trigger).
    ///
    /// `initial_results` maps node UUIDs to their pre-computed output.  Nodes in
    /// this map are treated as already completed; only their successors (and
    /// successors' successors) are executed.
    ///
    /// Uses single-node dispatch — the pipeline chain optimisation is not applied,
    /// keeping the implementation simple for trigger-based workflow runs.
    pub fn run_with_seed_with_transport(
        &self,
        dispatcher: Arc<dyn workflow_engine_core::NodeDispatcher>,
        worker_shared_key: Option<Arc<Vec<u8>>>,
        initial_results: HashMap<Uuid, JsonValue>,
        execution_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<WorkflowContext, String>> + Send + '_>> {
        // Abstract-entry guard: mirrors `run_with_transport`. See the
        // equivalent check there for the rationale; not repeating the
        // commentary here.
        if self.secrets_resolver.is_none() {
            return Box::pin(async move {
                Err(
                    "ParallelWorkflowEngine was constructed without a SecretsResolver. \
                     Use a controller-side builder or `set_secrets_resolver` \
                     before calling run_with_seed_with_transport."
                        .to_string(),
                )
            });
        }
        let timeout_secs = self.execution_timeout_secs;
        // Build the execution-scoped DLP context before the closure captures `self` by reference.
        // It is moved into the async closure and used to value-scrub output/errors before persistence.
        // Per-run DLP sanitizer — built once from resolved node configs
        // and used to scrub error messages before persistence. Stateless
        // regex-based scrubs (crate::dlp::redact_*) run in a second pass
        // on top via `self.redact_str` / `self.redact_json`.
        let exec_ctx = self.new_execution_sanitizer();
        Box::pin(async move {
            let timeout_duration = std::time::Duration::from_secs(timeout_secs);
            let result = tokio::time::timeout(timeout_duration, async {
        let (execution_sandbox, _sandbox_guard) = match create_execution_sandbox(execution_id) {
            Ok(sandbox) => {
                tracing::debug!("Created execution sandbox: {}", execution_id);
                (Some(sandbox), Some(SandboxGuard { execution_id }))
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to create execution sandbox: {}. File I/O will be unavailable.",
                    e
                );
                (None, None)
            }
        };

        if petgraph::algo::is_cyclic_directed(&self.graph) {
            return Err("Workflow contains a cycle".into());
        }

        // Initialise Kahn's in-degree counter.
        let mut pending: HashMap<NodeIndex, usize> = HashMap::new();
        for idx in self.graph.node_indices() {
            let deps = self
                .graph
                .neighbors_directed(idx, Direction::Incoming)
                .count();
            pending.insert(idx, deps);
        }

        // Pre-seed results and propagate pending counts to unblock successors.
        let mut results: HashMap<Uuid, JsonValue> = initial_results;

        // Store original trigger input for passthrough to all downstream nodes.
        // This allows any node to access the original trigger data via the
        // `__trigger_input__` key in its input payload.
        let trigger_input: JsonValue = results.values().next().cloned().unwrap_or(serde_json::json!({}));

        let seeded: HashSet<Uuid> = results.keys().cloned().collect();
        for &node_id in &seeded {
            if let Some(&node_idx) = self.node_map.get(&node_id) {
                pending.insert(node_idx, 0);
                for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                    if let Some(cnt) = pending.get_mut(&child) {
                        if *cnt > 0 {
                            *cnt -= 1;
                        }
                    }
                }
            }
        }

        // Build initial ready queue: nodes with 0 pending deps that were NOT pre-seeded.
        // Condition evaluation happens in the reactor loop AFTER nodes produce output,
        // not here at seed time (seeded nodes may be synthetic triggers whose output
        // doesn't contain the fields conditions reference).
        let mut ready: VecDeque<NodeIndex> = VecDeque::new();
        for idx in self.graph.node_indices() {
            let node_id = self.graph[idx];
            if pending.get(&idx).copied().unwrap_or(1) == 0 && !seeded.contains(&node_id) {
                ready.push_back(idx);
            }
        }

        let mut executing: FuturesUnordered<ExecFuture<'_>> = FuturesUnordered::new();
        let mut node_timings: HashMap<String, u64> = HashMap::new();
        let mut node_start_times: HashMap<NodeIndex, std::time::Instant> = HashMap::new();

        // DB pool for execution event logging (fire-and-forget)

        // Main reactor loop — single-node dispatch (no pipeline chain optimisation).
        while !ready.is_empty() || !executing.is_empty() {
            while let Some(node_idx) = ready.pop_front() {
                let node_id = self.graph[node_idx];

                // ── Skip condition check (FIRST — applies to ALL node types including system nodes) ──
                if let Some(skip_cond) = self.node_configs.get(&node_id)
                    .and_then(|cfg| cfg.get("__skip_condition"))
                    .and_then(|v| v.as_str())
                {
                    let mut skip_context = self.gather_inputs(node_idx, &results);
                    if let Some(trigger_id) = self.node_labels.iter()
                        .find(|(_, label)| label.as_str() == "__trigger__")
                        .map(|(uuid, _)| *uuid)
                    {
                        if let Some(trigger_val) = results.get(&trigger_id) {
                            if let Some(obj) = trigger_val.as_object() {
                                if let Some(ctx_obj) = skip_context.as_object_mut() {
                                    for (k, v) in obj {
                                        ctx_obj.entry(k.clone()).or_insert(v.clone());
                                    }
                                }
                            }
                        }
                    }
                    if self.eval_bool(skip_cond, &skip_context) {
                        tracing::info!(node_id = %node_id, skip_condition = %skip_cond, "Node skipped by skip_condition");
                        results.insert(node_id, serde_json::json!({"__skipped": true, "reason": "skip_condition"}));
                        emit_event_spawn(
                            &self.event_sink,
                            NodeEventWrite {
                                execution_id,
                                event_type: "node_skipped".to_string(),
                                node_id: Some(node_id),
                                status: "Skipped".to_string(),
                                log_message: None,
                                iteration_index: None,
                            },
                        );
                        for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                            if let Some(cnt) = pending.get_mut(&child) {
                                if *cnt > 0 { *cnt -= 1; }
                                if pending.get(&child).copied().unwrap_or(1) == 0 {
                                    ready.push_back(child);
                                }
                            }
                        }
                        continue;
                    }
                }

                // ── FanIn aggregation (local computation, no NATS dispatch) ──
                if let Some((_, _, Some(SystemNodeKind::FanIn { ref join_mode, ref aggregation_expr }))) =
                    self.node_meta.get(&node_id)
                {
                    let final_result = self.aggregate_fan_in(node_idx, &results, join_mode, aggregation_expr);

                    results.insert(node_id, final_result);

                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 {
                                *cnt -= 1;
                            }
                            if *cnt == 0 {
                                ready.push_back(child);
                            }
                        }
                    }
                    continue;
                }

                // ── Collect dispatch (local computation — single-node reactor) ─
                if let Some((_, _, Some(SystemNodeKind::Collect))) =
                    self.node_meta.get(&node_id)
                {
                    let collected = self.collect_parent_outputs_for_node(node_idx, &results);
                    let parent_count = collected.get("count").and_then(|v| v.as_u64()).unwrap_or(0);

                    results.insert(node_id, collected);

                    self.emit_node_lifecycle_events(
                        execution_id,
                        node_id,
                        "Completed",
                        format!("collected {} branch outputs into items array", parent_count),
                    );

                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if *cnt == 0 {
                                ready.push_back(child);
                            }
                        }
                    }
                    continue;
                }

                // ── Synthesize dispatch (collect + optional Rhai expression) ─
                if let Some((_, _, Some(SystemNodeKind::Synthesize { ref synthesis_expr }))) =
                    self.node_meta.get(&node_id)
                {
                    let synthesis_expr = synthesis_expr.clone();
                    let synthesized = self.synthesize_parent_outputs(node_idx, &results, &synthesis_expr);

                    let parent_count = synthesized
                        .get("count")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);

                    results.insert(node_id, synthesized);

                    self.emit_node_lifecycle_events(
                        execution_id,
                        node_id,
                        "Completed",
                        format!("synthesized {} branch outputs", parent_count),
                    );

                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if *cnt == 0 { ready.push_back(child); }
                        }
                    }
                    continue;
                }

                // ── Verify dispatch (step-level verification) ────────────────
                if let Some((_, _, Some(SystemNodeKind::Verify { ref condition, ref check_label, ref on_failure }))) =
                    self.node_meta.get(&node_id)
                {
                    let check_label = check_label.clone().unwrap_or_else(|| "output quality".to_string());
                    let (verify_result, passed) = self.evaluate_verify_node(
                        node_idx, &results, condition, &check_label, on_failure,
                    );

                    results.insert(node_id, verify_result);

                    self.emit_node_lifecycle_events(
                        execution_id,
                        node_id,
                        if passed { "Completed" } else { "Failed" },
                        format!(
                            "Verify '{}': {}",
                            check_label,
                            if passed { "PASSED" } else { "FAILED" }
                        ),
                    );

                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if pending.get(&child).copied().unwrap_or(1) == 0 { ready.push_back(child); }
                        }
                    }
                    continue;
                }

                // ── Judge dispatch (LLM-as-Judge evaluation) [run_with_seed] ──
                if let Some((_, _, Some(SystemNodeKind::Judge { judge_workflow_id, ref rubric, pass_threshold, timeout_secs: _ }))) =
                    self.node_meta.get(&node_id)
                {
                    let judge_wf_id = *judge_workflow_id;
                    let rubric = rubric.clone();
                    let pass_threshold = *pass_threshold;
                    let parent_inputs = self.gather_inputs(node_idx, &results);

                    let judge_result = self
                        .dispatch_judge(
                            parent_inputs,
                            judge_wf_id,
                            rubric,
                            pass_threshold,
                            dispatcher.clone(),
                            worker_shared_key.clone(),
                        )
                        .await;

                    results.insert(node_id, judge_result);
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if pending.get(&child).copied().unwrap_or(1) == 0 { ready.push_back(child); }
                        }
                    }
                    continue;
                }

                // ── Ensemble dispatch (self-consistency) [run_with_seed] ──────
                if let Some((_, _, Some(SystemNodeKind::Ensemble { child_workflow_id, count, ref consensus, judge_workflow_id, timeout_secs: _ }))) =
                    self.node_meta.get(&node_id)
                {
                    let child_wf_id = *child_workflow_id;
                    let run_count = *count;
                    let consensus_strategy = consensus.clone();
                    let judge_wf_id_opt = *judge_workflow_id;
                    let inputs = self.gather_inputs(node_idx, &results);

                    let ensemble_result = self
                        .dispatch_ensemble(
                            inputs,
                            child_wf_id,
                            run_count,
                            consensus_strategy,
                            judge_wf_id_opt,
                            dispatcher.clone(),
                            worker_shared_key.clone(),
                        )
                        .await;

                    results.insert(node_id, ensemble_result);
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if pending.get(&child).copied().unwrap_or(1) == 0 { ready.push_back(child); }
                        }
                    }
                    continue;
                }

                // ── ConfidenceGate dispatch [run_with_seed] ───────────────────
                if let Some((_, _, Some(SystemNodeKind::ConfidenceGate { threshold, ref confidence_path, ref on_low_confidence }))) =
                    self.node_meta.get(&node_id)
                {
                    match self.evaluate_confidence_gate(
                        node_idx, &results, execution_id, *threshold, confidence_path, on_low_confidence,
                    ).await {
                        Ok(gate_result) => {
                            results.insert(node_id, gate_result);
                        }
                        Err(waiting_json) => {
                            results.insert(node_id, waiting_json);
                            self.module_prefetch_cache.clear();
                            return Ok(WorkflowContext { results, waiting: true, ..Default::default() });
                        }
                    }
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if pending.get(&child).copied().unwrap_or(1) == 0 { ready.push_back(child); }
                        }
                    }
                    continue;
                }

                // ── ReflectiveRetry dispatch [run_with_seed] ──────────────────
                if let Some((_, _, Some(SystemNodeKind::ReflectiveRetry { child_workflow_id, reflection_workflow_id, max_retries, timeout_secs: _ }))) =
                    self.node_meta.get(&node_id)
                {
                    let child_wf_id = *child_workflow_id;
                    let reflection_wf_id = *reflection_workflow_id;
                    let max_retries = *max_retries;
                    let initial_input = self.gather_inputs(node_idx, &results);

                    let reflective_result = self
                        .dispatch_reflective_retry(
                            initial_input,
                            child_wf_id,
                            reflection_wf_id,
                            max_retries,
                            dispatcher.clone(),
                            worker_shared_key.clone(),
                        )
                        .await;

                    results.insert(node_id, reflective_result);
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if pending.get(&child).copied().unwrap_or(1) == 0 { ready.push_back(child); }
                        }
                    }
                    continue;
                }

                // ── LlmDispatch dispatch [run_with_seed] ──────────────────────
                if let Some((_, _, Some(SystemNodeKind::LlmDispatch { classifier_workflow_id, ref routes, fallback_workflow_id, timeout_secs: _ }))) =
                    self.node_meta.get(&node_id)
                {
                    let classifier_wf_id = *classifier_workflow_id;
                    let routes = routes.clone();
                    let fallback_wf_id = *fallback_workflow_id;
                    let inputs = self.gather_inputs(node_idx, &results);

                    let llm_dispatch_result = self
                        .dispatch_llm_dispatch(
                            inputs,
                            classifier_wf_id,
                            routes,
                            fallback_wf_id,
                            dispatcher.clone(),
                            worker_shared_key.clone(),
                        )
                        .await;

                    results.insert(node_id, llm_dispatch_result);
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if pending.get(&child).copied().unwrap_or(1) == 0 { ready.push_back(child); }
                        }
                    }
                    continue;
                }

                // ── AgentLoop dispatch (ReAct-style iterative sub-workflow) ─
                if let Some((_, _, Some(SystemNodeKind::AgentLoop { body_workflow_id, max_iterations, inject_history, timeout_secs }))) =
                    self.node_meta.get(&node_id)
                {
                    let body_wf_id = *body_workflow_id;
                    let max_iters = *max_iterations;
                    let do_inject_history = *inject_history;
                    let timeout_secs = *timeout_secs;
                    let inputs = self.gather_inputs(node_idx, &results);

                    let agent_result = if self.module_fetcher.is_some() {
                        let user_id = match self.user_id {
                            Some(uid) => uid,
                            None => {
                                results.insert(node_id, serde_json::json!({
                                    "__error": true,
                                    "error_message": "user_id required for sub-workflow execution"
                                }));
                                for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                                    if let Some(cnt) = pending.get_mut(&child) {
                                        if *cnt > 0 { *cnt -= 1; }
                                        if pending.get(&child).copied().unwrap_or(1) == 0 { ready.push_back(child); }
                                    }
                                }
                                continue;
                            }
                        };
                        let graph_row = self.get_sub_workflow_graph(body_wf_id, user_id).await;

                        if let Some(graph_json) = graph_row {
                            let dispatcher_al = dispatcher.clone();
                            let worker_shared_key_al = worker_shared_key.clone();
                            let adapter_set_al = self.adapter_set();
                            let inputs_al = inputs.clone();
                            let agent_result_inner = match tokio::time::timeout(
                                std::time::Duration::from_secs(timeout_secs),
                                async move {
                                    let mut history: Vec<JsonValue> = Vec::new();
                                    let mut last_output = serde_json::json!({});
                                    let mut finished = false;
                                    // Track total iterations separately from history.len() —
                                    // history is capped at AGENT_LOOP_MAX_HISTORY entries (sliding window) so
                                    // history.len() would under-report when max_iters > AGENT_LOOP_MAX_HISTORY.
                                    let mut iterations_run: u32 = 0;

                                    for iteration in 1..=max_iters {
                                        let mut iter_input = if let Some(obj) = inputs_al.as_object() {
                                            let mut cleaned = obj.clone();
                                            cleaned.retain(|k, _| !k.starts_with("__"));
                                            cleaned
                                        } else {
                                            serde_json::Map::new()
                                        };
                                        iter_input.insert("__agent_iteration__".to_string(), serde_json::json!(iteration));
                                        if do_inject_history && !history.is_empty() {
                                            iter_input.insert("__agent_history__".to_string(), serde_json::Value::Array(history.clone()));
                                        }

                                        let iter_result = match adapter_set_al
                                            .clone()
                                            .into_engine_with_graph(&graph_json)
                                        {
                                            Ok(mut sub_engine) => {
                                                let sub_execution_id = Uuid::new_v4();
                                                let trigger_node_id = Uuid::new_v4();
                                                sub_engine.add_node(trigger_node_id, None, None, None);
                                                sub_engine.node_labels.insert(trigger_node_id, "__trigger__".to_string());
                                                let root_indices: Vec<petgraph::graph::NodeIndex> = sub_engine.graph.node_indices()
                                                    .filter(|&idx| sub_engine.graph[idx] != trigger_node_id && sub_engine.graph.neighbors_directed(idx, Direction::Incoming).count() == 0)
                                                    .collect();
                                                for root_idx in &root_indices {
                                                    let root_id = sub_engine.graph[*root_idx];
                                                    let _ = sub_engine.add_edge(trigger_node_id, root_id, workflow_engine_core::EdgeLogic {
                                                        source_handle: "output".to_string(),
                                                        target_handle: "input".to_string(),
                                                        mapping: None, condition: None,
                                                        edge_type: "default".to_string(),
                                                    });
                                                }
                                                let mut initial_results = HashMap::new();
                                                initial_results.insert(trigger_node_id, serde_json::Value::Object(iter_input));
                                                let sub_labels = sub_engine.node_labels.clone();
                                                match sub_engine.run_with_seed_with_transport(dispatcher_al.clone(), worker_shared_key_al.clone(), initial_results, sub_execution_id).await {
                                                    Ok(ctx) => {
                                                        let mut sub_outputs = serde_json::Map::new();
                                                        for (nid, output) in &ctx.results {
                                                            if output.get("__skipped").and_then(|v| v.as_bool()).unwrap_or(false) { continue; }
                                                            let key = sub_labels.get(nid).cloned().unwrap_or_else(|| nid.to_string());
                                                            if key == "__trigger__" { continue; }
                                                            sub_outputs.insert(key, ParallelWorkflowEngine::unwrap_output(output).clone());
                                                        }
                                                        serde_json::Value::Object(sub_outputs)
                                                    }
                                                    Err(e) => serde_json::json!({"__error": true, "error_message": e.to_string()}),
                                                }
                                            }
                                            Err(e) => serde_json::json!({"__error": true, "error_message": format!("AgentLoop body build failed: {}", e)}),
                                        };

                                        let iter_finished = iter_result.get("finished").and_then(|v| v.as_bool()).unwrap_or(false)
                                            || iter_result.get("action").and_then(|v| v.as_str()).map(|s| s.eq_ignore_ascii_case("FINISH")).unwrap_or(false);

                                        // Cap history entries to prevent unbounded memory growth
                                        // when inject_history is true and iterations produce large outputs.
                                        iterations_run += 1;
                                        if history.len() >= AGENT_LOOP_MAX_HISTORY {
                                            history.remove(0);
                                        }
                                        history.push(iter_result.clone());
                                        last_output = iter_result;

                                        if iter_finished {
                                            finished = true;
                                            break;
                                        }
                                    }

                                    if !finished {
                                        tracing::warn!(
                                            max_iterations = max_iters,
                                            "AgentLoop reached max_iterations without finish signal"
                                        );
                                    }

                                    serde_json::json!({
                                        "iterations": iterations_run,
                                        "finished": finished,
                                        "history": history,
                                        "final_output": last_output,
                                    })
                                },
                            ).await {
                                Ok(result) => result,
                                Err(_) => {
                                    tracing::warn!(
                                        node_id = %node_id,
                                        timeout_secs = timeout_secs,
                                        "AgentLoop timed out"
                                    );
                                    serde_json::json!({
                                        "__error": true,
                                        "error_message": format!("AgentLoop timed out after {}s", timeout_secs),
                                    })
                                }
                            };
                            agent_result_inner
                        } else {
                            serde_json::json!({"__error": true, "error_message": format!("AgentLoop body workflow {} not found", body_wf_id)})
                        }
                    } else {
                        serde_json::json!({"__error": true, "error_message": "Registry not available for AgentLoop"})
                    };

                    results.insert(node_id, agent_result);

                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if pending.get(&child).copied().unwrap_or(1) == 0 { ready.push_back(child); }
                        }
                    }
                    continue;
                }

                // ── WhileLoop dispatch (local computation) ──────────────────
                if let Some((_, _, Some(SystemNodeKind::WhileLoop { ref condition, max_iterations }))) =
                    self.node_meta.get(&node_id)
                {
                    let condition = condition.clone();
                    let max_iters = *max_iterations;
                    let inputs = self.gather_inputs(node_idx, &results);

                    // WhileLoop runs the body inline, checking the condition after each iteration.
                    let mut current_output = inputs;
                    let mut iteration = 0u32;

                    while iteration < max_iters {
                        // Evaluate condition against current output
                        if !self.eval_bool(&condition, &current_output) {
                            break;
                        }
                        iteration += 1;
                        // Store iteration result (each iteration overwrites)
                        current_output = serde_json::json!({
                            "__loop_iteration": iteration,
                            "__loop_input": current_output,
                        });
                    }

                    if iteration >= max_iters {
                        tracing::warn!(
                            node_id = %node_id,
                            max_iterations = max_iters,
                            "WhileLoop reached maximum iterations"
                        );
                    }

                    results.insert(node_id, serde_json::json!({
                        "iterations": iteration,
                        "output": current_output,
                    }));

                    // Unblock successors
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if pending.get(&child).copied().unwrap_or(1) == 0 {
                                ready.push_back(child);
                            }
                        }
                    }
                    continue;
                }

                // ── RepeatLoop dispatch (local computation) ─────────────────
                if let Some((_, _, Some(SystemNodeKind::RepeatLoop { count }))) =
                    self.node_meta.get(&node_id)
                {
                    let count = *count;
                    let inputs = self.gather_inputs(node_idx, &results);

                    results.insert(node_id, serde_json::json!({
                        "iterations": count,
                        "input": inputs,
                    }));

                    // Unblock successors
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if pending.get(&child).copied().unwrap_or(1) == 0 {
                                ready.push_back(child);
                            }
                        }
                    }
                    continue;
                }

                // ── SubWorkflow dispatch (real execution) ─────────────────
                if let Some((_, _, Some(SystemNodeKind::SubWorkflow { workflow_id: sub_wf_id, timeout_secs: _ }))) =
                    self.node_meta.get(&node_id)
                {
                    let sub_wf_id = *sub_wf_id;
                    let inputs = self.gather_inputs(node_idx, &results);

                    tracing::info!(
                        node_id = %node_id,
                        sub_workflow_id = %sub_wf_id,
                        "SubWorkflow node — executing sub-workflow"
                    );

                    let sub_result = self
                        .dispatch_subworkflow(
                            inputs,
                            sub_wf_id,
                            dispatcher.clone(),
                            worker_shared_key.clone(),
                        )
                        .await;

                    results.insert(node_id, sub_result);

                    // Unblock successors
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if pending.get(&child).copied().unwrap_or(1) == 0 { ready.push_back(child); }
                        }
                    }
                    continue;
                }

                // ── DynamicDispatch (evaluate Rhai expression to select sub-workflow) ──
                if let Some((_, _, Some(SystemNodeKind::DynamicDispatch { ref dispatch_expression, timeout_secs: _ }))) =
                    self.node_meta.get(&node_id)
                {
                    let expression = dispatch_expression.clone();
                    let inputs = self.gather_inputs(node_idx, &results);

                    tracing::info!(
                        node_id = %node_id,
                        expression = %expression,
                        "DynamicDispatch node — evaluating dispatch expression"
                    );

                    // Evaluate the Rhai expression to get the target workflow ID
                    let dispatch_target: Result<String, String> = {
                        let mut rhai_engine = rhai::Engine::new();
                        rhai_engine.set_max_operations(10_000);
                        rhai_engine.disable_symbol("eval");
                        rhai_engine.set_module_resolver(rhai::module_resolvers::DummyModuleResolver);
                        let mut scope = rhai::Scope::new();
                        if let Some(obj) = inputs.as_object() {
                            for (k, v) in obj {
                                let dyn_val: rhai::Dynamic = match v {
                                    serde_json::Value::String(s) => rhai::Dynamic::from(s.clone()),
                                    serde_json::Value::Number(n) => {
                                        if let Some(i) = n.as_i64() { rhai::Dynamic::from(i) }
                                        else if let Some(f) = n.as_f64() { rhai::Dynamic::from(f) }
                                        else { rhai::Dynamic::from(n.to_string()) }
                                    }
                                    serde_json::Value::Bool(b) => rhai::Dynamic::from(*b),
                                    _ => rhai::Dynamic::from(v.to_string()),
                                };
                                scope.push(k.clone(), dyn_val);
                            }
                        }
                        match rhai_engine.eval_with_scope::<rhai::Dynamic>(&mut scope, &expression) {
                            Ok(result) => {
                                let s = result.to_string();
                                if s.is_empty() { Err("Dispatch expression returned empty string".to_string()) }
                                else { Ok(s) }
                            }
                            Err(e) => Err(format!("Dispatch expression evaluation failed: {}", e)),
                        }
                    };

                    let dispatch_result = match dispatch_target {
                        Ok(target_id_or_name) => {
                            let target_wf_id: Option<uuid::Uuid> = if let Ok(id) = uuid::Uuid::parse_str(&target_id_or_name) {
                                Some(id)
                            } else if let Some(ref store) = self.graph_store {
                                store
                                    .resolve_by_name(
                                        &target_id_or_name,
                                        self.user_id.unwrap_or_else(Uuid::nil),
                                    )
                                    .await
                                    .map_err(|e| {
                                        tracing::warn!(
                                            error = %e,
                                            "DB query failed during execution",
                                        );
                                        e
                                    })
                                    .ok()
                                    .flatten()
                            } else { None };

                            match target_wf_id {
                                Some(sub_wf_id) => {
                                    tracing::info!(node_id = %node_id, dispatched_workflow_id = %sub_wf_id, "DynamicDispatch resolved to workflow");
                                    if self.module_fetcher.is_some() {
                                        let graph_row = self.get_sub_workflow_graph(sub_wf_id, self.user_id.unwrap_or_else(Uuid::nil)).await;

                                        if let Some(graph_json) = graph_row {
                                            match self
                                                .adapter_set()
                                                .into_engine_with_graph(&graph_json)
                                            {
                                                Ok(mut sub_engine) => {
                                                    let sub_execution_id = Uuid::new_v4();
                                                    let clean_input = if let Some(obj) = inputs.as_object() {
                                                        let mut cleaned = obj.clone(); cleaned.retain(|k, _| !k.starts_with("__")); serde_json::Value::Object(cleaned)
                                                    } else { inputs.clone() };

                                                    let trigger_node_id = Uuid::new_v4();
                                                    sub_engine.add_node(trigger_node_id, None, None, None);
                                                    sub_engine.node_labels.insert(trigger_node_id, "__trigger__".to_string());
                                                    let root_indices: Vec<petgraph::graph::NodeIndex> = sub_engine.graph.node_indices()
                                                        .filter(|&idx| sub_engine.graph[idx] != trigger_node_id && sub_engine.graph.neighbors_directed(idx, Direction::Incoming).count() == 0)
                                                        .collect();
                                                    for root_idx in &root_indices {
                                                        let root_id = sub_engine.graph[*root_idx];
                                                        let _ = sub_engine.add_edge(trigger_node_id, root_id, workflow_engine_core::EdgeLogic {
                                                            source_handle: "output".to_string(), target_handle: "input".to_string(), mapping: None, condition: None, edge_type: "default".to_string(),
                                                        });
                                                    }

                                                    let mut initial_results = HashMap::new();
                                                    initial_results.insert(trigger_node_id, clean_input);
                                                    let sub_labels = sub_engine.node_labels.clone();
                                                    match sub_engine.run_with_seed_with_transport(dispatcher.clone(), worker_shared_key.clone(), initial_results, sub_execution_id).await {
                                                        Ok(ctx) => {
                                                            let mut sub_outputs = serde_json::Map::new();
                                                            sub_outputs.insert("__dispatched_workflow_id".to_string(), serde_json::json!(sub_wf_id.to_string()));
                                                            for (nid, output) in &ctx.results {
                                                                if output.get("__skipped").and_then(|v| v.as_bool()).unwrap_or(false) { continue; }
                                                                let key = sub_labels.get(nid).cloned().unwrap_or_else(|| nid.to_string());
                                                                if key == "__trigger__" { continue; }
                                                                sub_outputs.insert(key, Self::unwrap_output(output).clone());
                                                            }
                                                            serde_json::Value::Object(sub_outputs)
                                                        }
                                                        Err(e) => { tracing::error!(dispatched_workflow_id = %sub_wf_id, error = %e, "Dispatched workflow failed"); serde_json::json!({"__error": true, "error_message": format!("Dispatched workflow failed: {}", e)}) }
                                                    }
                                                }
                                                Err(e) => serde_json::json!({"__error": true, "error_message": format!("Failed to build dispatched workflow engine: {}", e)}),
                                            }
                                        } else { serde_json::json!({"__error": true, "error_message": format!("Dispatched workflow {} not found", sub_wf_id)}) }
                                    } else { serde_json::json!({"__error": true, "error_message": "Registry not available for dispatch execution"}) }
                                }
                                None => serde_json::json!({"__error": true, "error_message": format!("Could not resolve dispatch target: {}", target_id_or_name)}),
                            }
                        }
                        Err(e) => serde_json::json!({"__error": true, "error_message": e}),
                    };

                    results.insert(node_id, dispatch_result);
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 {
                                *cnt -= 1;
                            }
                            if pending.get(&child).copied().unwrap_or(1) == 0 {
                                ready.push_back(child);
                            }
                        }
                    }
                    continue;
                }

                // ── CapabilityDispatch (find best workflow by capability tags) [run_with_seed] ──
                if let Some((_, _, Some(SystemNodeKind::CapabilityDispatch { ref required_capabilities, timeout_secs: _ }))) =
                    self.node_meta.get(&node_id)
                {
                    let caps = required_capabilities.clone();
                    let inputs = self.gather_inputs(node_idx, &results);

                    tracing::info!(
                        node_id = %node_id,
                        capabilities = ?caps,
                        "CapabilityDispatch node — finding best matching workflow (run_with_seed)"
                    );

                    let capability_result = if let Some(ref store) = self.graph_store {
                        let matching_row = store
                            .resolve_by_capabilities(
                                &caps,
                                self.user_id.unwrap_or_else(Uuid::nil),
                            )
                            .await
                            .map_err(|e| {
                                tracing::warn!(
                                    error = %e,
                                    "DB query failed during execution",
                                );
                                e
                            })
                            .ok()
                            .flatten();

                        match matching_row {
                            Some((sub_wf_id, sub_wf_name)) => {
                                tracing::info!(node_id = %node_id, dispatched_workflow_id = %sub_wf_id, dispatched_workflow_name = %sub_wf_name, "CapabilityDispatch resolved to workflow (run_with_seed)");
                                let graph_row = self.get_sub_workflow_graph(sub_wf_id, self.user_id.unwrap_or_else(Uuid::nil)).await;

                                if let Some(graph_json) = graph_row {
                                    match self
                                        .adapter_set()
                                        .into_engine_with_graph(&graph_json)
                                    {
                                        Ok(mut sub_engine) => {
                                            let sub_execution_id = Uuid::new_v4();
                                            let clean_input = if let Some(obj) = inputs.as_object() {
                                                let mut cleaned = obj.clone(); cleaned.retain(|k, _| !k.starts_with("__")); serde_json::Value::Object(cleaned)
                                            } else { inputs.clone() };

                                            let trigger_node_id = Uuid::new_v4();
                                            sub_engine.add_node(trigger_node_id, None, None, None);
                                            sub_engine.node_labels.insert(trigger_node_id, "__trigger__".to_string());
                                            let root_indices: Vec<petgraph::graph::NodeIndex> = sub_engine.graph.node_indices()
                                                .filter(|&idx| sub_engine.graph[idx] != trigger_node_id && sub_engine.graph.neighbors_directed(idx, Direction::Incoming).count() == 0)
                                                .collect();
                                            for root_idx in &root_indices {
                                                let root_id = sub_engine.graph[*root_idx];
                                                let _ = sub_engine.add_edge(trigger_node_id, root_id, workflow_engine_core::EdgeLogic {
                                                    source_handle: "output".to_string(), target_handle: "input".to_string(), mapping: None, condition: None, edge_type: "default".to_string(),
                                                });
                                            }

                                            let mut initial_results = HashMap::new();
                                            initial_results.insert(trigger_node_id, clean_input);
                                            let sub_labels = sub_engine.node_labels.clone();
                                            match sub_engine.run_with_seed_with_transport(dispatcher.clone(), worker_shared_key.clone(), initial_results, sub_execution_id).await {
                                                Ok(ctx) => {
                                                    let mut sub_outputs = serde_json::Map::new();
                                                    sub_outputs.insert("__dispatched_workflow_id".to_string(), serde_json::json!(sub_wf_id.to_string()));
                                                    sub_outputs.insert("__dispatched_by".to_string(), serde_json::json!("capability_dispatch"));
                                                    sub_outputs.insert("__dispatched_workflow_name".to_string(), serde_json::json!(sub_wf_name));
                                                    sub_outputs.insert("__matched_capabilities".to_string(), serde_json::json!(caps));
                                                    for (nid, output) in &ctx.results {
                                                        if output.get("__skipped").and_then(|v| v.as_bool()).unwrap_or(false) { continue; }
                                                        let key = sub_labels.get(nid).cloned().unwrap_or_else(|| nid.to_string());
                                                        if key == "__trigger__" { continue; }
                                                        sub_outputs.insert(key, Self::unwrap_output(output).clone());
                                                    }
                                                    serde_json::Value::Object(sub_outputs)
                                                }
                                                Err(e) => { tracing::error!(dispatched_workflow_id = %sub_wf_id, error = %e, "Capability-dispatched workflow failed (run_with_seed)"); serde_json::json!({"__error": true, "error_message": format!("Capability-dispatched workflow failed: {}", e)}) }
                                            }
                                        }
                                        Err(e) => serde_json::json!({"__error": true, "error_message": format!("Failed to build capability-dispatched engine: {}", e)}),
                                    }
                                } else { serde_json::json!({"__error": true, "error_message": format!("Capability-dispatched workflow {} graph not found", sub_wf_id)}) }
                            }
                            None => {
                                serde_json::json!({"__error": true, "error_message": format!("No workflow found matching capabilities: {:?}", caps)})
                            }
                        }
                    } else {
                        serde_json::json!({"__error": true, "error_message": "Registry not available for capability dispatch"})
                    };

                    // If capability dispatch failed, check continue_on_error before propagating
                    if capability_result.get("__error").and_then(|v| v.as_bool()).unwrap_or(false) {
                        let continue_on_error = self
                            .node_configs
                            .get(&node_id)
                            .and_then(|c| c.get("__continue_on_error"))
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        if !continue_on_error {
                            let err_msg = capability_result
                                .get("error_message")
                                .and_then(|v| v.as_str())
                                .unwrap_or("capability dispatch failed")
                                .to_string();
                            tracing::error!(node_id = %node_id, error = %err_msg, "Capability dispatch failed — failing workflow");
                            return Err(format!("Capability dispatch node {}: {}", node_id, err_msg));
                        }
                        tracing::info!(node_id = %node_id, "Capability dispatch failed but continue_on_error is set — continuing");
                    }

                    results.insert(node_id, capability_result);
                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 {
                                *cnt -= 1;
                            }
                            if pending.get(&child).copied().unwrap_or(1) == 0 {
                                ready.push_back(child);
                            }
                        }
                    }
                    continue;
                }

                // ── Loop dispatch (re-dispatches body node while condition is true) ──
                if let Some((_, _, Some(SystemNodeKind::Loop { ref condition, max_iterations }))) =
                    self.node_meta.get(&node_id)
                {
                    let condition = condition.clone();
                    let max_iters = *max_iterations;
                    let inputs = self.gather_inputs(node_idx, &results);

                    // Find the body_node_id from node config
                    let body_node_id_str = self.node_configs.get(&node_id)
                        .and_then(|c| c.get("body_node_id"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());

                    let loop_result = if let Some(body_rf_id) = body_node_id_str {
                        let body_uuid = self.node_labels.iter()
                            .find(|(_, label)| label.as_str() == body_rf_id)
                            .map(|(uuid, _)| *uuid);

                        if let Some(body_uuid) = body_uuid {
                            let body_module_id = self.node_meta.get(&body_uuid)
                                .and_then(|(mid, _, _)| *mid);

                            if let Some(body_module_id) = body_module_id {
                                let mut current_input = inputs.clone();
                                let mut iteration = 0u32;
                                let mut last_output = current_input.clone();

                                // Extract __trigger_input__ to inject into every loop iteration.
                                // Search: (1) gathered inputs, (2) the __trigger__ node's output in results
                                let trigger_input_val = inputs.as_object()
                                    .and_then(|o| o.get("__trigger_input__"))
                                    .cloned()
                                    .or_else(|| {
                                        // Find the trigger node by label and use its value
                                        self.node_labels.iter()
                                            .find(|(_, label)| label.as_str() == "__trigger__")
                                            .and_then(|(uuid, _)| results.get(uuid))
                                            .cloned()
                                    });

                                while iteration < max_iters {
                                    // Evaluate condition with iteration_count injected so
                                    // conditions like `iteration_count < 3` work without
                                    // the body module needing to echo the counter.
                                    if iteration > 0 {
                                        let condition_ctx =
                                            if let Some(mut obj) = last_output.as_object().cloned() {
                                                obj.entry("iteration_count".to_string())
                                                    .or_insert(serde_json::json!(iteration));
                                                obj.entry("iteration".to_string())
                                                    .or_insert(serde_json::json!(iteration));
                                                serde_json::Value::Object(obj)
                                            } else {
                                                serde_json::json!({
                                                    "iteration_count": iteration,
                                                    "iteration": iteration,
                                                    "output": last_output,
                                                })
                                            };
                                        if !self.eval_bool(
                                            &condition,
                                            &condition_ctx,
                                        ) {
                                            break;
                                        }
                                    }

                                    iteration += 1;

                                    emit_event_spawn(
                                        &self.event_sink,
                                        NodeEventWrite {
                                            execution_id,
                                            event_type: "loop_iteration".to_string(),
                                            node_id: Some(node_id),
                                            status: "Running".to_string(),
                                            log_message: Some(format!(
                                                "Loop iteration {}/{}",
                                                iteration, max_iters
                                            )),
                                            iteration_index: Some(iteration as i32),
                                        },
                                    );

                                    // Use fetch_module for full resolution (wasm_modules → template_id → node_templates)
                                    let fetch_result = self.fetch_module(body_uuid).await
                                        .map_err(|e| anyhow::anyhow!(e));

                                    match fetch_result {
                                        Ok(wasm_module) => {
                                            // Flat-merge input + config (same pattern as regular node dispatch)
                                            let mut merged_input = serde_json::Map::new();
                                            // Spread current_input fields at root level
                                            if let Some(obj) = current_input.as_object() {
                                                for (k, v) in obj {
                                                    merged_input.insert(k.clone(), v.clone());
                                                }
                                            }
                                            // Add config sub-key if present
                                            if let Some(cfg) = self.node_configs.get(&body_uuid) {
                                                if cfg.is_object() && !cfg.as_object().map(|m| m.is_empty()).unwrap_or(true) {
                                                    merged_input.insert("config".to_string(), cfg.clone());
                                                    // Also spread config fields at root for templates that read them directly
                                                    if let Some(obj) = cfg.as_object() {
                                                        for (k, v) in obj {
                                                            merged_input.entry(k.clone()).or_insert(v.clone());
                                                        }
                                                    }
                                                }
                                            }
                                            // Include input sub-key for modules that read it explicitly
                                            if !current_input.is_null() && current_input != serde_json::json!({}) {
                                                merged_input.entry("input".to_string()).or_insert(current_input.clone());
                                            }
                                            // Inject __trigger_input__ into each loop iteration
                                            if let Some(ref ti) = trigger_input_val {
                                                merged_input.insert("__trigger_input__".to_string(), ti.clone());
                                            }
                                            // Inject loop counter so body modules can read it.
                                            // `iteration` is already incremented (1-based).
                                            merged_input.entry("iteration_count".to_string())
                                                .or_insert(serde_json::json!(iteration));
                                            merged_input.entry("iteration".to_string())
                                                .or_insert(serde_json::json!(iteration));
                                            let job_input = serde_json::Value::Object(merged_input);

                                            let body_timeout_secs =
                                                self.node_timeouts.get(&body_uuid).copied().unwrap_or(*DEFAULT_NODE_TIMEOUT_SECS);
                                            let encrypted_secrets = self
                                                .build_encrypted_secrets(
                                                    body_module_id,
                                                    &worker_shared_key,
                                                )
                                                .await;
                                            let body_job = DispatchJob {
                                                execution_id,
                                                node_id: body_uuid,
                                                module_id: body_module_id,
                                                job_id: None,
                                                user_id: self.user_id.unwrap_or_else(uuid::Uuid::nil),
                                                actor_id: self.actor_id,
                                                module_uri: wasm_module.oci_url.clone()
                                                    .unwrap_or_else(|| format!("redis:wasm:{}", body_module_id)),
                                                wasm_bytes: None,
                                                expected_wasm_hash: Some(wasm_module.content_hash.clone()),
                                                capability_world: Some(wasm_module.capability_world.clone()),
                                                integration_name: wasm_module.integration_name.clone(),
                                                input_payload: job_input,
                                                timeout: std::time::Duration::from_secs(body_timeout_secs),
                                                max_fuel: (wasm_module.max_fuel).min(50_000_000),
                                                allowed_hosts: wasm_module.allowed_hosts.clone(),
                                                allowed_methods: wasm_module.allowed_methods.clone(),
                                                allowed_secrets: wasm_module.allowed_secrets.clone(),
                                                allowed_sql_operations: vec![],
                                                allow_tier2_exposure: false,
                                                encrypted_secrets_ciphertext: encrypted_secrets.ciphertext,
                                                encrypted_secrets_nonce: encrypted_secrets.nonce,
                                                priority: 100,
                                                dry_run: self.dry_run,
                                                max_retries: 2,
                                                backoff_ms: 500,
                                                retry_condition: None,
                                                retry_delay_expr: None,
                                                emit_retry_events: false,
                                            };
                                            match dispatcher.dispatch(body_job).await {
                                                Ok(result) => {
                                                    let clean = Self::unwrap_output(&result.output).clone();
                                                    last_output = clean.clone();
                                                    current_input = clean;
                                                }
                                                Err(e) => {
                                                    last_output = serde_json::json!({"__error": true, "error_message": e.to_string()});
                                                    break;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            last_output = serde_json::json!({"__error": true, "error_message": format!("Module fetch failed: {}", e)});
                                            break;
                                        }
                                    }
                                }

                                if iteration >= max_iters {
                                    tracing::warn!(
                                        node_id = %node_id,
                                        max_iterations = max_iters,
                                        "Loop reached maximum iterations"
                                    );
                                }

                                serde_json::json!({
                                    "iterations": iteration,
                                    "output": last_output,
                                })
                            } else {
                                serde_json::json!({"__error": true, "error_message": format!("Body node '{}' has no module_id", body_rf_id)})
                            }
                        } else {
                            serde_json::json!({"__error": true, "error_message": format!("Body node '{}' not found in workflow", body_rf_id)})
                        }
                    } else {
                        serde_json::json!({"__error": true, "error_message": "Loop node missing body_node_id in config"})
                    };

                    results.insert(node_id, loop_result);

                    for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                        if let Some(cnt) = pending.get_mut(&child) {
                            if *cnt > 0 { *cnt -= 1; }
                            if pending.get(&child).copied().unwrap_or(1) == 0 {
                                ready.push_back(child);
                            }
                        }
                    }
                    continue;
                }

                // ── ErrorHandler dispatch (pattern filtering) ───────────────
                if let Some((_, _, Some(SystemNodeKind::ErrorHandler { ref error_pattern }))) =
                    self.node_meta.get(&node_id)
                {
                    let inputs = self.gather_inputs(node_idx, &results);

                    // Check if error matches the pattern filter (if specified)
                    if let Some(pattern) = error_pattern {
                        let error_msg = inputs.get("error_message")
                            .or_else(|| {
                                // Check parent outputs for __error payloads
                                inputs.as_object().and_then(|obj| {
                                    obj.values().find_map(|v| v.get("error_message"))
                                })
                            })
                            .and_then(|v| v.as_str())
                            .unwrap_or("");

                        if !error_msg.contains(pattern.as_str()) {
                            // Error doesn't match pattern — skip this handler, propagate error
                            results.insert(node_id, serde_json::json!({
                                "__skipped": true,
                                "reason": "error_pattern_mismatch",
                            }));

                            for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                                if let Some(cnt) = pending.get_mut(&child) {
                                    if *cnt > 0 { *cnt -= 1; }
                                    if pending.get(&child).copied().unwrap_or(1) == 0 {
                                        ready.push_back(child);
                                    }
                                }
                            }
                            continue;
                        }
                    }
                    // If pattern matches (or no pattern), fall through to normal dispatch below
                }

                // ── Rate limit check ──────────────────────────────────────
                evict_stale_rate_limits();
                let module_id_resolved = self.resolve_module_id(node_id);
                if let Some(&limit) = self.rate_limits.get(&module_id_resolved) {
                    if limit > 0 {
                        let now = std::time::Instant::now();
                        let mut entry = MODULE_RATE_LIMITS.entry(module_id_resolved).or_insert((now, 0));
                        if now.duration_since(entry.0) > std::time::Duration::from_secs(60) {
                            entry.0 = now;
                            entry.1 = 0;
                        }
                        entry.1 += 1;
                        if entry.1 > limit as u32 {
                            tracing::warn!(
                                node_id = %node_id,
                                module_id = %module_id_resolved,
                                rate_limit = limit,
                                "Module rate limit exceeded"
                            );
                            results.insert(node_id, serde_json::json!({
                                "__error": true,
                                "error_message": format!("Module rate limit exceeded ({}/min)", limit)
                            }));
                            for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
                                if let Some(cnt) = pending.get_mut(&child) {
                                    if *cnt > 0 { *cnt -= 1; }
                                    if pending.get(&child).copied().unwrap_or(1) == 0 {
                                        ready.push_back(child);
                                    }
                                }
                            }
                            continue;
                        }
                    }
                }

                let retry = self
                    .node_meta
                    .get(&node_id)
                    .and_then(|(_, rp, _)| rp.clone())
                    .unwrap_or_default();
                let inputs = self.gather_inputs(node_idx, &results);
                let dispatcher_clone = dispatcher.clone();
                let user_id_clone = self.user_id;
                let fetch_fut = self.fetch_module(node_id);
                let secrets_resolver = self.secrets_resolver.clone();
                let approval_gate = self.approval_gate.clone();
                let module_execution_store = self.module_execution_store.clone();
                let _exec_sandbox = execution_sandbox.clone();
                let seed_user_id = self.user_id;
                let worker_shared_key_clone = worker_shared_key.clone();
                let node_configs_clone = self.node_configs.clone();
                let node_timeouts_clone = self.node_timeouts.clone();
                let trigger_input_clone = trigger_input.clone();
                let event_sink_clone = self.event_sink.clone();
                let dry_run = self.dry_run;
                // Accumulated context snapshot for run_with_seed dispatch.
                let accumulated_snapshot =
                    Self::build_accumulated_context(&self.node_labels, &results);

                let fut = async move {
                    let wasm_module = match fetch_fut.await {
                        Ok(m) => m,
                        Err(e) => return (node_idx, Err(e)),
                    };

                    // ── Approval gate ───────────────────────────────────────
                    if !wasm_module.requires_approval_for.is_empty() {
                        if let Some(ref gate) = approval_gate {
                            let approval_webhook = node_configs_clone
                                .get(&node_id)
                                .and_then(|cfg| cfg.get("NOTIFICATION_WEBHOOK"))
                                .and_then(|v| v.as_str());
                            match gate
                                .check_or_request(
                                    execution_id,
                                    node_id,
                                    &wasm_module.requires_approval_for,
                                    approval_webhook,
                                )
                                .await
                            {
                                Ok(workflow_engine_core::ApprovalStatus::Approved) => {
                                    /* proceed */
                                }
                                Ok(workflow_engine_core::ApprovalStatus::Pending) => {
                                    return (
                                        node_idx,
                                        Err(format!(
                                            "Execution paused: module {} requires approval for {:?}. \
                                             An approval request has been created.",
                                            node_id, wasm_module.requires_approval_for
                                        )),
                                    );
                                }
                                Ok(workflow_engine_core::ApprovalStatus::Denied { reason }) => {
                                    return (node_idx, Err(reason));
                                }
                                Err(e) => {
                                    tracing::error!(
                                        node_id = %node_id,
                                        "Approval gate check failed: {}",
                                        e
                                    );
                                    return (
                                        node_idx,
                                        Err(format!("Approval gate check failed: {}", e)),
                                    );
                                }
                            }
                        }
                    }

                    // Read compile-time config from the already-fetched
                    // artifact. See the equivalent block in `run()` for
                    // why we dropped the best-effort Redis cache warm.
                    if seed_user_id.is_none() {
                        return (
                            node_idx,
                            Err("Module execution requires user context (user_id not set)"
                                .to_string()),
                        );
                    }
                    let module_config = wasm_module
                        .config
                        .clone()
                        .unwrap_or_else(|| serde_json::json!({}));

                    // Merge node-level config from graph_json (takes precedence)
                    // Filter out internal keys (__skip_condition, skip_condition) that shouldn't be passed to modules
                    let module_config = if let Some(node_cfg) = node_configs_clone.get(&node_id) {
                        if module_config.is_object() && node_cfg.is_object() {
                            let mut merged = module_config.as_object().cloned().unwrap_or_default();
                            if let Some(node_cfg_obj) = node_cfg.as_object() {
                                for (k, v) in node_cfg_obj {
                                    if k == "__skip_condition" || k == "skip_condition" || k == "__continue_on_error" || k == "continue_on_error" { continue; }
                                    merged.insert(k.clone(), v.clone());
                                }
                            }
                            serde_json::Value::Object(merged)
                        } else if module_config == serde_json::json!({}) {
                            node_cfg.clone()
                        } else {
                            module_config
                        }
                    } else {
                        module_config
                    };

                    // Merge config and input into a flat object so templates can
                    // find their fields at the top level (e.g., "text", "URL").
                    // Also include "config" and "input" for backwards compatibility.
                    let wrapped_input = {
                        let mut merged = serde_json::Map::new();
                        // Start with config fields at top level
                        if let Some(obj) = module_config.as_object() {
                            for (k, v) in obj {
                                merged.insert(k.clone(), v.clone());
                            }
                        }
                        // Overlay input fields at top level (input takes precedence)
                        if let Some(obj) = inputs.as_object() {
                            for (k, v) in obj {
                                merged.insert(k.clone(), v.clone());
                            }
                        } else if !inputs.is_null() {
                            merged.insert("input".to_string(), inputs.clone());
                        }
                        // Always include config and input sub-objects for templates
                        // that explicitly read from these keys
                        // Only include "config" if it has actual content (skip empty {})
                        if module_config != serde_json::json!({}) {
                            merged.insert("config".to_string(), module_config.clone());
                        }
                        // Always include "input" sub-key for non-null, non-empty upstream
                        // outputs so downstream modules can access data["input"] regardless
                        // of whether the upstream returned an object or a scalar.
                        let is_empty_object = inputs.as_object().map(|m| m.is_empty()).unwrap_or(false);
                        if !inputs.is_null() && !is_empty_object {
                            merged.insert("input".to_string(), inputs.clone());
                        }
                        // Inject original trigger input for passthrough to all nodes
                        merged.insert("__trigger_input__".to_string(), trigger_input_clone.clone());
                        // Inject accumulated context: all prior nodes' outputs
                        // keyed by label, with __-prefixed metadata stripped.
                        if let Some(acc) = &accumulated_snapshot {
                            merged.insert("__accumulated__".to_string(), acc.clone());
                        }
                        // Inject actor memory context into every node.
                        if let Some(ref ctx) = self.actor_context {
                            merged.insert("__actor_context__".to_string(), ctx.clone());
                        }
                        serde_json::Value::Object(merged)
                    };

                    // Store truncated node input for debugging (node I/O inspector)
                    {
                        let input_preview = {
                            let s = serde_json::to_string(&wrapped_input).unwrap_or_default();
                            if s.len() > 4096 { format!("{}...(truncated)", &s[..4096]) } else { s }
                        };
                        emit_event_spawn(
                            &event_sink_clone,
                            NodeEventWrite {
                                execution_id,
                                event_type: "node_input".to_string(),
                                node_id: Some(node_id),
                                status: "Input".to_string(),
                                log_message: Some(input_preview),
                                iteration_index: None,
                            },
                        );
                    }

                    let job_id = Uuid::new_v4();

                    if let Some(ref store) = module_execution_store {
                        // Race-safe INSERT via the store; see the primary
                        // dispatch path for the rationale behind
                        // race_safe_status=true.
                        let actual_module_id =
                            store.resolve_wasm_module_id(module_id_resolved).await;
                        if let Err(db_err) = store
                            .record_started(
                                job_id,
                                actual_module_id,
                                seed_user_id.unwrap_or_else(Uuid::new_v4),
                                execution_id,
                                &inputs,
                                "webhook",
                                true,
                            )
                            .await
                        {
                            tracing::error!(
                                "module_execution_store.record_started failed: {}",
                                db_err
                            );
                        }
                    }

                    // Per-node fuel limit: config override > module default, capped at 50M.
                    let node_max_fuel = module_config
                        .get("max_fuel")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(wasm_module.max_fuel)
                        .min(50_000_000);

                    // Resolve encrypted secrets payload (opaque bytes at this layer).
                    let encrypted_secrets = match (
                        secrets_resolver.as_ref(),
                        &worker_shared_key_clone,
                    ) {
                        (Some(resolver), Some(key)) => {
                            let vault_paths = extract_vault_paths(&module_config);
                            build_encrypted_secrets_for(
                                resolver.as_ref(),
                                module_id_resolved,
                                user_id_clone,
                                &vault_paths,
                                &wasm_module.allowed_secrets,
                                key,
                            )
                            .await
                        }
                        _ => Default::default(),
                    };

                    // Wire-format WASM budget. See the matching comment in run().
                    let node_timeout_secs =
                        node_timeouts_clone.get(&node_id).copied().unwrap_or(*DEFAULT_NODE_TIMEOUT_SECS);

                    let job = DispatchJob {
                        execution_id,
                        node_id,
                        module_id: module_id_resolved,
                        job_id: Some(job_id),
                        user_id: user_id_clone.unwrap_or_else(uuid::Uuid::nil),
                        actor_id: self.actor_id,
                        module_uri: wasm_module
                            .oci_url
                            .clone()
                            .unwrap_or_else(|| format!("redis:wasm:{}", module_id_resolved)),
                        wasm_bytes: if wasm_module.wasm_bytes.is_empty() { None } else { Some(wasm_module.wasm_bytes.clone()) },
                        expected_wasm_hash: if wasm_module.wasm_bytes.is_empty() {
                            Some(wasm_module.content_hash.clone())
                        } else {
                            None
                        },
                        capability_world: Some(wasm_module.capability_world.clone()),
                        integration_name: wasm_module.integration_name.clone(),
                        input_payload: wrapped_input,
                        timeout: std::time::Duration::from_secs(node_timeout_secs),
                        max_fuel: node_max_fuel,
                        allowed_hosts: wasm_module.allowed_hosts.clone(),
                        allowed_methods: wasm_module.allowed_methods.clone(),
                        allowed_secrets: wasm_module.allowed_secrets.clone(),
                        allowed_sql_operations: vec![],
                        allow_tier2_exposure: false,
                        encrypted_secrets_ciphertext: encrypted_secrets.ciphertext,
                        encrypted_secrets_nonce: encrypted_secrets.nonce,
                        priority: 100,
                        dry_run,
                        max_retries: retry.max_retries,
                        backoff_ms: retry.backoff_ms,
                        retry_condition: retry.retry_condition.clone(),
                        retry_delay_expr: retry.retry_delay_expression.clone(),
                        emit_retry_events: true,
                    };

                    match dispatcher_clone.dispatch(job).await {
                        Ok(result) => {
                            tracing::info!(node_id = %node_id, "Node execution succeeded");
                            (node_idx, Ok(result.output))
                        }
                        Err(e) => (node_idx, Err(e.to_string())),
                    }
                };
                node_start_times.insert(node_idx, std::time::Instant::now());
                // Log node_started event (fire-and-forget)
                emit_event_spawn(
                    &self.event_sink,
                    NodeEventWrite {
                        execution_id,
                        event_type: "node_started".to_string(),
                        node_id: Some(node_id),
                        status: "Running".to_string(),
                        log_message: None,
                        iteration_index: None,
                    },
                );
                executing.push(Box::pin(fut)
                    as Pin<
                        Box<dyn Future<Output = (NodeIndex, Result<JsonValue, String>)> + Send>,
                    >);

                // ── Speculative module prefetch (P10) ────────────────────────
                if self
                    .node_configs
                    .get(&node_id)
                    .and_then(|c| c.get("speculative_prefetch"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    const MAX_PREFETCH_SUCCESSORS: usize = 8;
                    for succ_idx in self
                        .graph
                        .neighbors_directed(node_idx, Direction::Outgoing)
                        .take(MAX_PREFETCH_SUCCESSORS)
                    {
                        let succ_id = self.graph[succ_idx];
                        // Skip system nodes — they have no module in the registry (resolve_module_id
                        // returns the node UUID as a fallback for system nodes). Attempting to fetch
                        // would waste a 5-second timeout and produce noisy debug log entries.
                        let succ_module_id = match self.node_meta.get(&succ_id)
                            .and_then(|(mid, _, _)| *mid)
                        {
                            Some(mid) => mid,
                            None => continue,
                        };
                        let prefetch_cache = Arc::clone(&self.module_prefetch_cache);
                        if let Some(ref fetcher) = self.module_fetcher {
                            let fetcher = Arc::clone(fetcher);
                            let uid = self.user_id;
                            tokio::spawn(async move {
                                if prefetch_cache.contains_key(&succ_id) {
                                    return;
                                }
                                if let Some(uid) = uid {
                                    let fetch_result = tokio::time::timeout(
                                        std::time::Duration::from_secs(5),
                                        fetcher.fetch(succ_module_id, uid),
                                    )
                                    .await;
                                    match fetch_result {
                                        Ok(Ok(artifact)) => {
                                            // Use entry().or_insert to match run() semantics:
                                            // if two concurrent spawns race, only the first stores.
                                            prefetch_cache.entry(succ_id).or_insert(artifact);
                                            tracing::debug!(
                                                succ_id = %succ_id,
                                                "speculative prefetch: module cached"
                                            );
                                        }
                                        Ok(Err(e)) => {
                                            tracing::debug!(
                                                succ_id = %succ_id,
                                                error = %e,
                                                "speculative prefetch: fetch failed (normal dispatch will retry)"
                                            );
                                        }
                                        Err(_) => {
                                            tracing::debug!(
                                                succ_id = %succ_id,
                                                "speculative prefetch: timed out (normal dispatch will fetch)"
                                            );
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            }

            if let Some((finished_idx, exec_result)) = executing.next().await {
                // Record per-node timing and stash the elapsed time so the
                // post-completion hook can report accurate wall_time_ms.
                let wall_time_ms = if let Some(start) = node_start_times.remove(&finished_idx) {
                    let elapsed_ms = start.elapsed().as_millis() as u64;
                    let label = self.node_labels.get(&self.graph[finished_idx])
                        .cloned()
                        .unwrap_or_else(|| self.graph[finished_idx].to_string());
                    node_timings.insert(label, elapsed_ms);
                    elapsed_ms
                } else {
                    0
                };
                let finished_id = self.graph[finished_idx];
                match exec_result {
                    Ok(output) => {
                        // Log node_completed event synchronously so child node_started
                        // events (which are fire-and-forget) are always ordered after
                        // this insert in the DB — fixes causally-inconsistent timelines.
                        if let Some(ref sink) = self.event_sink {
                            sink.emit(NodeEventWrite {
                                execution_id,
                                event_type: "node_completed".to_string(),
                                node_id: Some(finished_id),
                                status: "Completed".to_string(),
                                log_message: None,
                                iteration_index: None,
                            })
                            .await;
                        }
                        // Per-node output size guard (mirrors the same check in run()).
                        const MAX_NODE_OUTPUT_BYTES_SEED: usize = 5 * 1024 * 1024; // 5 MiB
                        let output = match serde_json::to_vec(&output) {
                            Ok(bytes) if bytes.len() > MAX_NODE_OUTPUT_BYTES_SEED => {
                                tracing::warn!(
                                    node_id = %finished_id,
                                    bytes = bytes.len(),
                                    "Node output exceeds 5 MiB limit (run_with_seed) — replacing with error"
                                );
                                serde_json::json!({
                                    "__error": true,
                                    "error": format!(
                                        "Node output too large ({} bytes > {} byte limit).",
                                        bytes.len(), MAX_NODE_OUTPUT_BYTES_SEED
                                    )
                                })
                            }
                            _ => output,
                        };
                        let mut output = output;
                        sanitize_node_output(&mut output);
                        results.insert(finished_id, output.clone());

                        // Post-completion hook: drives fuel attribution +
                        // __memory_write__ persistence. See `run()` for the
                        // matching call; shared trait keeps both loops in sync.
                        if let Some(hook) = self.node_hook.as_ref() {
                            let node_label =
                                self.node_labels.get(&finished_id).map(String::as_str);
                            let module_id = self
                                .node_meta
                                .get(&finished_id)
                                .and_then(|(m, _, _)| *m);
                            hook.on_node_completed(
                                workflow_engine_core::NodeCompletionContext {
                                    workflow_id: self.workflow_id.unwrap_or(execution_id),
                                    execution_id,
                                    node_id: finished_id,
                                    node_label,
                                    module_id,
                                    actor_id: self.actor_id,
                                    wall_time_ms,
                                },
                                &output,
                            );
                        }

                        // On SUCCESS, skip error-edge children (they only fire on failure).
                        for child in self
                            .graph
                            .neighbors_directed(finished_idx, Direction::Outgoing)
                        {
                            let is_error_edge = self.graph.edges_connecting(finished_idx, child)
                                .any(|e| e.weight().edge_type == "error");
                            if is_error_edge {
                                let child_id = self.graph[child];
                                results.insert(child_id, serde_json::json!({"__skipped": true}));
                                continue;
                            }
                            if let Some(cnt) = pending.get_mut(&child) {
                                *cnt -= 1;

                                // FanIn early-ready logic: some join modes don't
                                // require ALL parents to complete.
                                if let Some((_, _, Some(SystemNodeKind::FanIn { ref join_mode, .. }))) =
                                    self.node_meta.get(&self.graph[child])
                                {
                                    let total_parents = self.graph
                                        .neighbors_directed(child, Direction::Incoming)
                                        .count();
                                    let completed_parents = total_parents - *cnt;
                                    match join_mode {
                                        JoinMode::Any => {
                                            if *cnt > 0 {
                                                pending.insert(child, 0);
                                            }
                                        }
                                        JoinMode::Majority => {
                                            if completed_parents > total_parents / 2 && *cnt > 0 {
                                                pending.insert(child, 0);
                                            }
                                        }
                                        JoinMode::N(n) => {
                                            if completed_parents >= *n as usize && *cnt > 0 {
                                                pending.insert(child, 0);
                                            }
                                        }
                                        JoinMode::All => {} // default behavior
                                    }
                                }

                                if pending.get(&child).copied().unwrap_or(1) == 0 {
                                    // Check edge conditions before enqueuing.
                                    let child_node_id = self.graph[child];
                                    let mut condition_failed = false;
                                    for edge_ref in self.graph.edges_connecting(finished_idx, child) {
                                        tracing::debug!(
                                            condition = ?edge_ref.weight().condition,
                                            edge_type = %edge_ref.weight().edge_type,
                                            child = %child_node_id,
                                            "Evaluating edge"
                                        );
                                        if let Some(ref cond) = edge_ref.weight().condition {
                                            if !self.eval_bool(cond, Self::unwrap_output(&output)) {
                                                condition_failed = true;
                                                break;
                                            }
                                        }
                                    }
                                    if condition_failed {
                                        tracing::info!(
                                            node_id = %child_node_id,
                                            "Skipping node: edge condition evaluated to false"
                                        );
                                        // Store a skip marker so downstream nodes know this path was not taken.
                                        results.insert(child_node_id, serde_json::json!({"__skipped": true}));
                                        // Cascade skip: decrement pending counts for the skipped node's children.
                                        for grandchild in self.graph.neighbors_directed(child, Direction::Outgoing) {
                                            if let Some(gc_cnt) = pending.get_mut(&grandchild) {
                                                if *gc_cnt > 0 {
                                                    *gc_cnt -= 1;
                                                }
                                            }
                                        }
                                    } else {
                                        ready.push_back(child);
                                    }
                                }
                            }
                        }
                    }
                    Err(error_msg) => {
                        // Two-pass scrub: value-based (known secrets) then regex DLP patterns.
                        let error_msg = self.redact_str(
                            &exec_ctx
                                .as_ref()
                                .map(|c| c.redact_error(&error_msg))
                                .unwrap_or_else(|| error_msg.clone()),
                        );
                        // Log node_failed event synchronously — same ordering guarantee
                        // as node_completed: child routing happens after this commit.
                        if let Some(ref sink) = self.event_sink {
                            sink.emit(NodeEventWrite {
                                execution_id,
                                event_type: "node_failed".to_string(),
                                node_id: Some(finished_id),
                                status: "Failed".to_string(),
                                log_message: Some(error_msg.clone()),
                                iteration_index: None,
                            })
                            .await;
                        }
                        // Check if this node has outgoing "error" edges
                        let error_children: Vec<NodeIndex> = self.graph
                            .neighbors_directed(finished_idx, Direction::Outgoing)
                            .filter(|&child_idx| {
                                if let Some(edge_idx) = self.graph.find_edge(finished_idx, child_idx) {
                                    self.graph[edge_idx].edge_type == "error"
                                } else {
                                    false
                                }
                            })
                            .collect();

                        if !error_children.is_empty() {
                            // Route error to error handler nodes instead of failing
                            let error_payload = serde_json::json!({
                                "__error": true,
                                "error_message": error_msg,
                                "failed_node": self.node_labels.get(&finished_id).cloned().unwrap_or_else(|| finished_id.to_string()),
                            });
                            results.insert(finished_id, error_payload.clone());
                            tracing::info!(
                                node_id = %finished_id,
                                error_handlers = error_children.len(),
                                "Node failed but has error handler edges — routing to error handlers"
                            );

                            // Unblock ONLY error-edge children; skip default/conditional children.
                            // Default-edge children should NOT fire when the node fails.
                            for child in self
                                .graph
                                .neighbors_directed(finished_idx, Direction::Outgoing)
                            {
                                // Check if ANY edge to this child is an error edge
                                let has_error_edge = self.graph.edges_connecting(finished_idx, child)
                                    .any(|e| e.weight().edge_type == "error");
                                if !has_error_edge {
                                    // Skip default/conditional children — parent failed, success path is dead
                                    let child_id = self.graph[child];
                                    results.insert(child_id, serde_json::json!({"__skipped": true}));
                                    continue;
                                }

                                if let Some(cnt) = pending.get_mut(&child) {
                                    if *cnt > 0 {
                                        *cnt -= 1;
                                    }

                                    // FanIn early-ready logic
                                    if let Some((_, _, Some(SystemNodeKind::FanIn { ref join_mode, .. }))) =
                                        self.node_meta.get(&self.graph[child])
                                    {
                                        let total_parents = self.graph
                                            .neighbors_directed(child, Direction::Incoming)
                                            .count();
                                        let completed_parents = total_parents - *cnt;
                                        match join_mode {
                                            JoinMode::Any => {
                                                if *cnt > 0 {
                                                    pending.insert(child, 0);
                                                }
                                            }
                                            JoinMode::Majority => {
                                                if completed_parents > total_parents / 2 && *cnt > 0 {
                                                    pending.insert(child, 0);
                                                }
                                            }
                                            JoinMode::N(n) => {
                                                if completed_parents >= *n as usize && *cnt > 0 {
                                                    pending.insert(child, 0);
                                                }
                                            }
                                            JoinMode::All => {} // default behavior
                                        }
                                    }

                                    if pending.get(&child).copied().unwrap_or(1) == 0 {
                                        ready.push_back(child);
                                    }
                                }
                            }
                        } else if self.node_configs.get(&finished_id)
                            .and_then(|c| c.get("__continue_on_error"))
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false)
                        {
                            // continue_on_error: store error result but don't fail the workflow
                            tracing::info!(
                                node_id = %finished_id,
                                "Node failed but continue_on_error is set — continuing execution"
                            );
                            results.insert(finished_id, serde_json::json!({
                                "__error": true,
                                "error_message": error_msg,
                                "__continued": true,
                            }));
                            // Unblock successors (same as success path)
                            for child in self.graph.neighbors_directed(finished_idx, Direction::Outgoing) {
                                if let Some(cnt) = pending.get_mut(&child) {
                                    if *cnt > 0 {
                                        *cnt -= 1;
                                    }
                                    if pending.get(&child).copied().unwrap_or(1) == 0 {
                                        ready.push_back(child);
                                    }
                                }
                            }
                        } else {
                            // No error handlers — notify the lifecycle hook (DLQ +
                            // sibling-cancellation) and propagate failure. Matches
                            // the run() path above; shared trait keeps both loops
                            // consistent.
                            if let Some(hook) = self.node_hook.as_ref() {
                                let node_label =
                                    self.node_labels.get(&finished_id).map(String::as_str);
                                let module_id = self
                                    .node_meta
                                    .get(&finished_id)
                                    .and_then(|(m, _, _)| *m);
                                hook.on_node_failed(
                                    workflow_engine_core::NodeCompletionContext {
                                        workflow_id: self.workflow_id.unwrap_or(execution_id),
                                        execution_id,
                                        node_id: finished_id,
                                        node_label,
                                        module_id,
                                        actor_id: self.actor_id,
                                        wall_time_ms: 0,
                                    },
                                    &error_msg,
                                    results.get(&finished_id),
                                );
                            }
                            let node_label = self.node_labels.get(&finished_id)
                                .cloned()
                                .unwrap_or_else(|| finished_id.to_string());
                            // Clear prefetch cache before returning so unconsumed WASM
                            // modules are not retained beyond the failing execution.
                            self.module_prefetch_cache.clear();
                            return Err(format!("node '{}' failed: {}", node_label, error_msg));
                        }
                    }
                }
            }
        }

        // Two-pass scrub: value-based then regex DLP patterns.
        let results: HashMap<Uuid, JsonValue> = results
            .into_iter()
            .map(|(k, v)| {
                let v = exec_ctx
                    .as_ref()
                    .map(|c| c.redact_output(&v))
                    .unwrap_or(v);
                (k, self.redact_json(&v))
            })
            .collect();

        // Release unconsumed prefetch cache entries (skipped branches).
        self.module_prefetch_cache.clear();

        Ok(WorkflowContext { results, node_timings, ..Default::default() })
        }).await; // tokio::time::timeout
            match result {
                Ok(inner) => inner,
                Err(_) => Err(format!(
                    "Workflow execution timed out after {} seconds",
                    timeout_secs
                )),
            }
        }) // Box::pin(async move { ... })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use workflow_engine_core::EdgeLogic;

    fn make_graph(edges: &[(usize, usize)], num_nodes: usize) -> DiGraph<Uuid, EdgeLogic> {
        let mut g: DiGraph<Uuid, EdgeLogic> = DiGraph::new();
        let nodes: Vec<NodeIndex> = (0..num_nodes).map(|_| g.add_node(Uuid::new_v4())).collect();
        for &(from, to) in edges {
            g.add_edge(
                nodes[from],
                nodes[to],
                EdgeLogic {
                    source_handle: "output".to_string(),
                    target_handle: "input".to_string(),
                    mapping: None,
                    condition: None,
                    edge_type: Default::default(),
                },
            );
        }
        g
    }

    #[test]
    fn linear_chain_simple_3_nodes() {
        // A → B → C
        let g = make_graph(&[(0, 1), (1, 2)], 3);
        let chains = detect_linear_chains(&g);
        assert_eq!(chains.len(), 1, "should detect exactly one chain");
        assert_eq!(chains[0].len(), 3, "chain should include all 3 nodes");
    }

    #[test]
    fn no_chain_for_fork() {
        // A → B, A → C
        let g = make_graph(&[(0, 1), (0, 2)], 3);
        let chains = detect_linear_chains(&g);
        assert!(
            chains.is_empty(),
            "Fork has no 2+ linear chain: {:?}",
            chains
        );
    }

    #[test]
    fn no_chain_for_join() {
        // A → C, B → C
        let g = make_graph(&[(0, 2), (1, 2)], 3);
        let chains = detect_linear_chains(&g);
        assert!(chains.is_empty(), "Join has no 2+ linear chain");
    }

    #[test]
    fn chain_with_single_edge() {
        // A → B (trivial 2-node chain)
        let g = make_graph(&[(0, 1)], 2);
        let chains = detect_linear_chains(&g);
        assert_eq!(chains.len(), 1);
        assert_eq!(chains[0].len(), 2);
    }

    #[test]
    fn single_node_no_chain() {
        let g = make_graph(&[], 1);
        let chains = detect_linear_chains(&g);
        assert!(chains.is_empty(), "Single node produces no chain");
    }

    #[test]
    fn diamond_graph_no_full_chain() {
        // A → B → D, A → C → D
        // B and C each have in-degree=1, out-degree=1 — but D has in-degree=2
        let g = make_graph(&[(0, 1), (0, 2), (1, 3), (2, 3)], 4);
        let chains = detect_linear_chains(&g);
        // A→B could be a chain (A out-degree=2 breaks it), so no chain >= 2.
        // Actually A has out-degree=2, so neither B nor C's predecessors qualify
        // as chain starts... let's just verify no chain spans the diamond.
        for chain in &chains {
            assert!(chain.len() < 3, "No chain of length >=3 in diamond graph");
        }
    }

    #[test]
    fn parallel_chains() {
        // A → B → C and D → E (two independent chains)
        let g = make_graph(&[(0, 1), (1, 2), (3, 4)], 5);
        let chains = detect_linear_chains(&g);
        assert_eq!(chains.len(), 2, "should find exactly 2 chains");
        let lengths: Vec<usize> = chains.iter().map(|c| c.len()).collect();
        assert!(lengths.contains(&3), "one chain of length 3");
        assert!(lengths.contains(&2), "one chain of length 2");
    }

    // ── collapse_subworkflow_output tests ───────────────────────────────────
    // These tests pin the contract that judge/reflective-retry/ensemble rely on:
    // a sub-workflow with exactly one terminal node returns that node's output
    // directly; multiple terminals fall back to a label-keyed map.

    /// Build an engine where nodes are laid out in index order, labels
    /// are assigned by position, and edges are (src_label, dst_label) pairs.
    /// Returns (engine, label -> uuid).
    fn build_sub_engine(
        labels: &[&str],
        edges: &[(&str, &str)],
    ) -> (ParallelWorkflowEngine, HashMap<String, Uuid>) {
        let mut engine = ParallelWorkflowEngine::new();
        let mut label_to_uuid: HashMap<String, Uuid> = HashMap::new();
        let mut label_to_idx: HashMap<String, NodeIndex> = HashMap::new();
        for label in labels {
            let uuid = Uuid::new_v4();
            let idx = engine.graph.add_node(uuid);
            engine.node_labels.insert(uuid, label.to_string());
            label_to_uuid.insert(label.to_string(), uuid);
            label_to_idx.insert(label.to_string(), idx);
        }
        for (src, dst) in edges {
            let s = label_to_idx[*src];
            let d = label_to_idx[*dst];
            engine.graph.add_edge(
                s,
                d,
                EdgeLogic {
                    source_handle: "output".to_string(),
                    target_handle: "input".to_string(),
                    mapping: None,
                    condition: None,
                    edge_type: Default::default(),
                },
            );
        }
        (engine, label_to_uuid)
    }

    #[test]
    fn collapse_single_terminal_returns_unwrapped_output() {
        // Canonical judge case: one node, returns record shape — caller sees fields directly.
        let (engine, uuids) = build_sub_engine(&["judge"], &[]);
        let mut results = HashMap::new();
        results.insert(
            uuids["judge"],
            serde_json::json!({"score": 0.94, "passed": true, "reasoning": "ok", "feedback": "good"}),
        );
        let out = ParallelWorkflowEngine::collapse_subworkflow_output(&results, &engine);
        assert_eq!(out.get("score").and_then(|v| v.as_f64()), Some(0.94));
        assert_eq!(out.get("passed").and_then(|v| v.as_bool()), Some(true));
        assert_eq!(out.get("reasoning").and_then(|v| v.as_str()), Some("ok"));
        assert_eq!(out.get("feedback").and_then(|v| v.as_str()), Some("good"));
    }

    #[test]
    fn collapse_linear_chain_returns_only_terminal() {
        // A → B → C. Only C is terminal; its output is the sub-workflow output.
        let (engine, uuids) = build_sub_engine(&["a", "b", "c"], &[("a", "b"), ("b", "c")]);
        let mut results = HashMap::new();
        results.insert(uuids["a"], serde_json::json!({"stage": "a", "n": 1}));
        results.insert(uuids["b"], serde_json::json!({"stage": "b", "n": 2}));
        results.insert(uuids["c"], serde_json::json!({"stage": "c", "n": 3}));
        let out = ParallelWorkflowEngine::collapse_subworkflow_output(&results, &engine);
        assert_eq!(out.get("stage").and_then(|v| v.as_str()), Some("c"));
        assert_eq!(out.get("n").and_then(|v| v.as_i64()), Some(3));
    }

    #[test]
    fn collapse_multiple_terminals_returns_label_keyed_map() {
        // Two independent terminals: fallback to label-keyed map.
        let (engine, uuids) = build_sub_engine(&["alpha", "beta"], &[]);
        let mut results = HashMap::new();
        results.insert(uuids["alpha"], serde_json::json!({"v": 1}));
        results.insert(uuids["beta"], serde_json::json!({"v": 2}));
        let out = ParallelWorkflowEngine::collapse_subworkflow_output(&results, &engine);
        assert_eq!(out.get("alpha").and_then(|v| v.get("v")).and_then(|v| v.as_i64()), Some(1));
        assert_eq!(out.get("beta").and_then(|v| v.get("v")).and_then(|v| v.as_i64()), Some(2));
    }

    #[test]
    fn collapse_skips_trigger_and_skipped_nodes() {
        // Trigger + one skipped middle node + one real terminal.
        let (engine, uuids) = build_sub_engine(
            &["__trigger__", "skipped", "real"],
            &[("__trigger__", "skipped"), ("skipped", "real")],
        );
        let mut results = HashMap::new();
        results.insert(uuids["__trigger__"], serde_json::json!({"trigger": "ignored"}));
        results.insert(uuids["skipped"], serde_json::json!({"__skipped": true, "noise": "x"}));
        results.insert(uuids["real"], serde_json::json!({"answer": "42"}));
        let out = ParallelWorkflowEngine::collapse_subworkflow_output(&results, &engine);
        assert_eq!(out.get("answer").and_then(|v| v.as_str()), Some("42"));
        assert!(out.get("trigger").is_none(), "trigger must not leak");
        assert!(out.get("noise").is_none(), "skipped must not leak");
    }

    #[test]
    fn collapse_strips_engine_envelope_on_terminal() {
        // unwrap_output recognises {input: X, score: ..., passed: ...} as a wrapper
        // when every inner key is also at the outer level. Terminal node output
        // should pass through unwrap_output.
        let (engine, uuids) = build_sub_engine(&["judge"], &[]);
        let mut results = HashMap::new();
        // Real-world shape: engine-wrapped output where inner fields are also hoisted.
        results.insert(
            uuids["judge"],
            serde_json::json!({
                "input": {"score": 0.7, "passed": true},
                "score": 0.7,
                "passed": true,
            }),
        );
        let out = ParallelWorkflowEngine::collapse_subworkflow_output(&results, &engine);
        assert_eq!(out.get("score").and_then(|v| v.as_f64()), Some(0.7));
        assert_eq!(out.get("passed").and_then(|v| v.as_bool()), Some(true));
    }

    #[test]
    fn collapse_empty_results_returns_empty_object() {
        let (engine, _) = build_sub_engine(&["a"], &[]);
        let results: HashMap<Uuid, JsonValue> = HashMap::new();
        let out = ParallelWorkflowEngine::collapse_subworkflow_output(&results, &engine);
        assert_eq!(out, serde_json::Value::Object(serde_json::Map::new()));
    }

    #[test]
    fn collapse_fork_non_terminal_shadows_do_not_overwrite_terminal() {
        // A → B (terminal). A is not a terminal. Both happen to emit a "score" field.
        // Terminal's fields must win — but since only one terminal exists, the map
        // is NOT the output shape; instead B's output is returned directly.
        let (engine, uuids) = build_sub_engine(&["a", "b"], &[("a", "b")]);
        let mut results = HashMap::new();
        results.insert(uuids["a"], serde_json::json!({"score": 0.1}));
        results.insert(uuids["b"], serde_json::json!({"score": 0.9}));
        let out = ParallelWorkflowEngine::collapse_subworkflow_output(&results, &engine);
        assert_eq!(out.get("score").and_then(|v| v.as_f64()), Some(0.9));
    }

    #[test]
    fn collapse_diamond_two_terminals_returns_both_labels() {
        // A → {B, C}. Both B and C are terminals (no aggregator).
        let (engine, uuids) = build_sub_engine(&["a", "b", "c"], &[("a", "b"), ("a", "c")]);
        let mut results = HashMap::new();
        results.insert(uuids["a"], serde_json::json!({"stage": "a"}));
        results.insert(uuids["b"], serde_json::json!({"stage": "b"}));
        results.insert(uuids["c"], serde_json::json!({"stage": "c"}));
        let out = ParallelWorkflowEngine::collapse_subworkflow_output(&results, &engine);
        // Multiple terminals → label-keyed map including non-terminal a.
        assert_eq!(out.get("a").and_then(|v| v.get("stage")).and_then(|v| v.as_str()), Some("a"));
        assert_eq!(out.get("b").and_then(|v| v.get("stage")).and_then(|v| v.as_str()), Some("b"));
        assert_eq!(out.get("c").and_then(|v| v.get("stage")).and_then(|v| v.as_str()), Some("c"));
    }
}

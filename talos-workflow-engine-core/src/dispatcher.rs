//! Pluggable dispatch for a single workflow node.
//!
//! [`NodeDispatcher`] is the engine's highest-level dispatch
//! abstraction. Consumers of the engine implement this trait once,
//! telling the engine how to ship one node's configuration + input to
//! an executor and get a result back. Everything above this trait —
//! wire-format construction, signing, transport, retry, result
//! parsing — is the impl's responsibility.
//!
//! This is a layer above [`JobTransport`]. `JobTransport` is a raw
//! "send bytes, get bytes" channel; `NodeDispatcher` is the full
//! "run this node" primitive. An impl that backs onto a signed NATS
//! protocol uses `JobTransport` internally but exposes the higher-
//! level contract to the engine.
//!
//! # Timeout handling
//!
//! [`DispatchJob`] carries the timeout as part of the job. Impls are
//! expected to honor it. The engine does not re-wrap the dispatch
//! call in `tokio::time::timeout`; the impl either enforces the
//! timeout internally or returns an error. This is different from the
//! raw-transport contract (where the caller wraps) because
//! `NodeDispatcher` owns the full dispatch lifecycle including
//! retries, and retries need per-attempt timeout handling the caller
//! cannot express without understanding the retry policy.
//!
//! [`JobTransport`]: crate::JobTransport

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::BoxError;

/// The pre-signing description of one node's dispatch.
///
/// The engine assembles this struct from its own state (node config,
/// resolved module, user context, etc.) and hands it to a
/// [`NodeDispatcher`]. The dispatcher impl is responsible for
/// translating it into whatever wire format it uses, signing,
/// transport, and extracting the result.
///
/// All fields are either primitives, `Uuid`, `Vec<u8>`, or
/// `JsonValue` — no controller-specific types leak through. That
/// keeps this crate consumable by any engine adopter.
///
/// # Per-step vs chain-level fields
///
/// When a `DispatchJob` is used as a **single-node** dispatch via
/// [`NodeDispatcher::dispatch`], every field is honored.
///
/// When a `DispatchJob` is used as a **chain step** inside a
/// [`ChainDispatchRequest::steps`], several fields describe
/// properties that only make sense at the chain level and are taken
/// from the request, not from the per-step `DispatchJob`:
///
/// * `user_id` — taken from [`ChainDispatchRequest::user_id`]
/// * `actor_id` — chain-level (not carried by per-step wire format)
/// * `dry_run` — chain-level
/// * `max_retries`, `backoff_ms`, `retry_condition`,
///   `retry_delay_expr` — chain-level retry policy, set on the
///   request, not per step
///
/// Populating these on a chain step is harmless but ignored. If your
/// impl needs per-step values for any of them, it should document
/// that deviation; the reference NATS dispatcher does not honor them
/// per-step because the underlying `PipelineJobRequest` wire format
/// doesn't carry them.
///
/// # Construction
///
/// The struct exposes all fields as `pub`. For the common case of
/// dispatching a node that doesn't need WASM/HMAC-shaped fields, use
/// the functional-update syntax with [`Default`]:
///
/// ```no_run
/// # use uuid::Uuid;
/// # use serde_json::json;
/// # use std::time::Duration;
/// # use talos_workflow_engine_core::DispatchJob;
/// let job = DispatchJob {
///     execution_id: Uuid::new_v4(),
///     node_id: Uuid::new_v4(),
///     module_id: Uuid::new_v4(),
///     input_payload: json!({ "msg": "hello" }),
///     timeout: Duration::from_secs(30),
///     ..Default::default()
/// };
/// ```
///
/// The [`DispatchJob::new`] constructor is equivalent shorthand for
/// the four most-common required fields.
#[derive(Clone)]
pub struct DispatchJob {
    // ── Identity ─────────────────────────────────────────────────────
    /// Workflow execution that owns this dispatch.
    pub execution_id: Uuid,
    /// Engine-local node identifier within the graph.
    pub node_id: Uuid,
    /// Resolved module identifier — what the worker runs.
    pub module_id: Uuid,
    /// Optional stable job id for this dispatch. When present, impls
    /// should use it as the wire-format `job_id`; when `None`, impls
    /// generate a fresh UUID. Callers set this when they have
    /// pre-INSERTed a `module_executions` row (or similar side-effect
    /// keyed on job id) that the worker will later update by the same
    /// id — letting DB rows and worker log lines correlate.
    pub job_id: Option<Uuid>,
    /// Owning user for this execution. Workers use this for per-user
    /// quota enforcement and cross-tenant isolation. `Uuid::nil()`
    /// is the engine's sentinel for "no user context"; impls MUST
    /// treat it as equivalent to "no user" when routing (for example,
    /// falling back to a tenant-agnostic subject rather than a
    /// tenant-scoped one that no worker subscribes to).
    pub user_id: Uuid,
    /// Actor id that owns the execution (if actor-owned), so the
    /// worker can route agent-memory WIT calls to the right rows.
    pub actor_id: Option<Uuid>,

    // ── Module artifact ──────────────────────────────────────────────
    /// URI the worker can resolve the wasm binary from if
    /// `wasm_bytes` is empty (e.g. `oci://...` or `redis:wasm:<id>`).
    pub module_uri: String,
    /// Optional inlined wasm bytes. When present, worker uses these
    /// directly and skips the URI fetch.
    pub wasm_bytes: Option<Vec<u8>>,
    /// SHA-256 hex digest of the wasm binary at `module_uri`. Lets
    /// the worker verify a URI-fetched binary matches what the engine
    /// compiled. Ignored when `wasm_bytes` is populated (HMAC already
    /// covers the inline bytes).
    pub expected_wasm_hash: Option<String>,
    /// Capability-world hint (e.g. `"network-node"`). Opaque to the
    /// engine; interpreted by the worker's linker. Not signed.
    pub capability_world: Option<String>,
    /// Integration the module is scoped to, if any. The worker signs
    /// integration-state RPCs with this value.
    pub integration_name: Option<String>,

    // ── Per-dispatch input ───────────────────────────────────────────
    /// JSON payload the worker feeds into the module's entry point.
    pub input_payload: JsonValue,

    // ── Budgets ──────────────────────────────────────────────────────
    /// Per-node execution budget. Seconds-resolution — impls truncate
    /// sub-second values. This is the **WASM-level** budget the worker
    /// enforces; impls that wrap in an outer cancellation timer (for
    /// example, a NATS dispatcher using `tokio::time::timeout`) should
    /// add grace on top internally rather than forcing callers to
    /// pre-add it.
    pub timeout: Duration,
    /// Wasmtime fuel budget for the dispatch.
    pub max_fuel: u64,

    // ── Capability grants ────────────────────────────────────────────
    /// Hostnames the worker permits outbound HTTP to.
    pub allowed_hosts: Vec<String>,
    /// HTTP methods the worker permits. Empty means allow all.
    pub allowed_methods: Vec<String>,
    /// Secret path allowlist. Empty = deny all; `["*"]` = allow all.
    pub allowed_secrets: Vec<String>,
    /// SQL operation allowlist (`"SELECT"`, `"INSERT"`, ...). Empty
    /// means allow all.
    pub allowed_sql_operations: Vec<String>,
    /// When true, the module may call Tier-2 `expose_secret` to
    /// receive plaintext secret bytes in-guest. Default false.
    pub allow_tier2_exposure: bool,

    // ── Secrets (already encrypted) ──────────────────────────────────
    /// Ciphertext of the encrypted secrets map the worker will decrypt
    /// with its copy of the shared key. Opaque bytes at this layer.
    pub encrypted_secrets_ciphertext: Vec<u8>,
    /// AES-GCM nonce paired with `encrypted_secrets_ciphertext`.
    pub encrypted_secrets_nonce: Vec<u8>,

    // ── Dispatch policy ──────────────────────────────────────────────
    /// Priority hint (higher dequeues first). Default 100.
    pub priority: u8,
    /// When true, the worker mocks write-bearing calls (non-GET HTTP,
    /// webhooks, messaging) — used for dry-run previews.
    pub dry_run: bool,

    // ── Retry policy ─────────────────────────────────────────────────
    /// Max retries for transient failures. Timeouts do not retry.
    pub max_retries: u32,
    /// Base backoff between retries in milliseconds. Impls may add
    /// jitter and exponential growth.
    pub backoff_ms: u64,
    /// Optional expression evaluated against error output to decide
    /// whether to retry. Opaque at this layer.
    pub retry_condition: Option<String>,
    /// Optional expression returning a retry delay in ms computed from
    /// the error output. Opaque at this layer.
    pub retry_delay_expr: Option<String>,
    /// When true, the dispatcher emits per-attempt observability
    /// events (e.g. `node_retrying`, `retry_skipped` in the reference
    /// NATS impl) keyed on `execution_id` / `node_id`. Set to `false`
    /// for nested/internal dispatches (loop-body iterations,
    /// sub-workflow steps) whose retries are not visible at the
    /// workflow level and should not inflate retry-rate metrics.
    /// Default `true`.
    pub emit_retry_events: bool,
}

/// Default per-node execution budget used by [`DispatchJob::default`].
///
/// Matches the engine's out-of-box node-timeout (also 60 s). Chosen so a
/// `DispatchJob::new(...)` that doesn't override `timeout` produces a
/// positive, bounded budget; [`Duration::ZERO`] would surface as "0 s +
/// dispatcher-grace = ~5 s cancel" under the reference NATS dispatcher,
/// which is the wrong foot-gun to ship. Override explicitly when the
/// consumer has its own budget policy.
pub const DEFAULT_DISPATCH_TIMEOUT_SECS: u64 = 60;

impl Default for DispatchJob {
    /// Populates every field with its documented default:
    ///
    /// * All `Uuid` fields → [`Uuid::nil()`]
    /// * All `Option<...>` fields → `None`
    /// * All `Vec<...>` fields → empty
    /// * `input_payload` → `JsonValue::Null`
    /// * `timeout` → [`DEFAULT_DISPATCH_TIMEOUT_SECS`] (60 s). Chosen to
    ///   avoid the `Duration::ZERO` + dispatcher-grace foot-gun where
    ///   every job cancels after a few seconds because the user forgot
    ///   to set a budget.
    /// * `max_fuel` → 0 — impls that enforce fuel read this as "no
    ///   budget configured"
    /// * `priority` → 100 (documented default)
    /// * `emit_retry_events` → `true` (documented default)
    /// * Everything else → `false` / 0 / `None`
    fn default() -> Self {
        Self {
            execution_id: Uuid::nil(),
            node_id: Uuid::nil(),
            module_id: Uuid::nil(),
            job_id: None,
            user_id: Uuid::nil(),
            actor_id: None,
            module_uri: String::new(),
            wasm_bytes: None,
            expected_wasm_hash: None,
            capability_world: None,
            integration_name: None,
            input_payload: JsonValue::Null,
            timeout: Duration::from_secs(DEFAULT_DISPATCH_TIMEOUT_SECS),
            max_fuel: 0,
            allowed_hosts: Vec::new(),
            allowed_methods: Vec::new(),
            allowed_secrets: Vec::new(),
            allowed_sql_operations: Vec::new(),
            allow_tier2_exposure: false,
            encrypted_secrets_ciphertext: Vec::new(),
            encrypted_secrets_nonce: Vec::new(),
            priority: 100,
            dry_run: false,
            max_retries: 0,
            backoff_ms: 0,
            retry_condition: None,
            retry_delay_expr: None,
            emit_retry_events: true,
        }
    }
}

impl DispatchJob {
    /// Construct a [`DispatchJob`] with the four fields every dispatch
    /// needs — the identity triple plus the input payload — leaving
    /// every other field at its documented [`Default`].
    ///
    /// Callers that need WASM-flavored fields (`wasm_bytes`,
    /// `capability_world`, `allowed_hosts`, `encrypted_secrets_*`, etc.)
    /// populate them directly on the returned struct; the functional-
    /// update idiom `DispatchJob { field: value, ..Default::default() }`
    /// is equivalent when more than a handful of fields differ.
    #[must_use]
    pub fn new(
        execution_id: Uuid,
        node_id: Uuid,
        module_id: Uuid,
        input_payload: JsonValue,
    ) -> Self {
        Self {
            execution_id,
            node_id,
            module_id,
            input_payload,
            ..Self::default()
        }
    }
}

impl fmt::Debug for DispatchJob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Redact `input_payload` and the encrypted-secrets blobs so
        // `Debug` output is safe to feed into `tracing::` macros. The
        // plaintext input may contain secret values after caller-side
        // template interpolation; the ciphertext is safe but large and
        // adds no debugging value beyond its length.
        f.debug_struct("DispatchJob")
            .field("execution_id", &self.execution_id)
            .field("node_id", &self.node_id)
            .field("module_id", &self.module_id)
            .field("job_id", &self.job_id)
            .field("user_id", &self.user_id)
            .field("actor_id", &self.actor_id)
            .field("module_uri", &self.module_uri)
            .field(
                "wasm_bytes",
                &self
                    .wasm_bytes
                    .as_ref()
                    .map(|b| format!("<{} bytes>", b.len())),
            )
            .field("expected_wasm_hash", &self.expected_wasm_hash)
            .field("capability_world", &self.capability_world)
            .field("integration_name", &self.integration_name)
            .field(
                "input_payload",
                &"<redacted — may contain plaintext secrets>",
            )
            .field("timeout", &self.timeout)
            .field("max_fuel", &self.max_fuel)
            .field("allowed_hosts", &self.allowed_hosts)
            .field("allowed_methods", &self.allowed_methods)
            .field("allowed_secrets", &self.allowed_secrets)
            .field("allowed_sql_operations", &self.allowed_sql_operations)
            .field("allow_tier2_exposure", &self.allow_tier2_exposure)
            .field(
                "encrypted_secrets_ciphertext",
                &format!("<{} bytes>", self.encrypted_secrets_ciphertext.len()),
            )
            .field(
                "encrypted_secrets_nonce",
                &format!("<{} bytes>", self.encrypted_secrets_nonce.len()),
            )
            .field("priority", &self.priority)
            .field("dry_run", &self.dry_run)
            .field("max_retries", &self.max_retries)
            .field("backoff_ms", &self.backoff_ms)
            .field("retry_condition", &self.retry_condition)
            .field("retry_delay_expr", &self.retry_delay_expr)
            .field("emit_retry_events", &self.emit_retry_events)
            .finish()
    }
}

/// Output of a successful node dispatch.
#[derive(Debug, Clone)]
pub struct DispatchResult {
    /// The worker's output payload. Shape is module-defined.
    pub output: JsonValue,
}

/// Per-step outcome returned by [`NodeDispatcher::dispatch_chain`].
///
/// Every step in the chain produces one of these, regardless of
/// whether the overall chain succeeded — a failure in step `N` still
/// reports completed results for steps `0..N` and an absent (or
/// default) entry for later steps, depending on the impl.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepStatus {
    /// Step ran to completion and produced an `output`.
    Success,
    /// Step exceeded its per-step timeout.
    TimedOut,
    /// Step errored internally (WASM trap, serialization failure,
    /// worker-side validation error, etc.).
    Failed,
}

/// Result of one step inside a chain dispatch.
#[derive(Debug, Clone)]
pub struct ChainStepResult {
    /// Module id the step ran (mirrors [`DispatchJob::module_id`] for
    /// the corresponding input).
    pub module_id: Uuid,
    /// How the step ended.
    pub status: StepStatus,
    /// The step's output payload. Shape is module-defined. Present
    /// regardless of `status` — a failed step may still produce a
    /// partial error envelope useful for downstream routing.
    pub output: JsonValue,
    /// Optional error detail when `status != Success`.
    pub error: Option<String>,
    /// Wall-clock step execution time in milliseconds.
    pub execution_time_ms: u64,
}

/// Request to dispatch a chain (pipeline) of steps as a single unit.
///
/// Used when the engine has detected a linear sequence of nodes that
/// can share a sandbox and avoid per-node round-trips. The dispatcher
/// translates this into whatever batch format the backing transport
/// supports.
#[derive(Debug, Clone, Default)]
pub struct ChainDispatchRequest {
    /// Workflow execution the chain belongs to.
    pub workflow_execution_id: Uuid,
    /// User owning the execution. Routing / tenant isolation apply
    /// per the same rules as [`DispatchJob::user_id`].
    pub user_id: Uuid,
    /// Optional stable chain id. When `None`, impls generate one.
    pub job_id: Option<Uuid>,
    /// The chain's steps, in dispatch order. Each carries its own
    /// per-step config via the [`DispatchJob`] it reuses. See
    /// [`DispatchJob`]'s "per-step vs chain-level fields" section
    /// for which fields of each step are honored vs. inherited from
    /// this request.
    pub steps: Vec<DispatchJob>,
    /// When true, the transport tries to keep all steps on a single
    /// worker sandbox so filesystem / module-instance state carries
    /// across steps. Falls back to per-step isolation if the transport
    /// can't honor it.
    pub share_sandbox: bool,
    /// Aggregate budget for the whole chain (sum of per-step budgets
    /// plus any slack the caller wants).
    pub total_timeout: Duration,
    /// Chain-level retry policy — applied at the transport layer on
    /// the whole chain, not per individual step.
    pub max_retries: u32,
    /// Base backoff between chain retries in milliseconds.
    pub backoff_ms: u64,
    /// Optional expression evaluated against the chain-level error
    /// output to decide whether to retry. Opaque at this layer.
    pub retry_condition: Option<String>,
    /// Optional expression returning a retry delay in ms computed from
    /// the chain-level error output. Opaque at this layer.
    pub retry_delay_expr: Option<String>,
}

/// Aggregate result of a chain dispatch.
#[derive(Debug, Clone)]
pub struct ChainDispatchResult {
    /// Per-step outcomes, aligned with `ChainDispatchRequest.steps` by
    /// index. May be shorter than the input on early failure — later
    /// steps never ran.
    pub steps: Vec<ChainStepResult>,
    /// Consolidated final output the chain produced — typically the
    /// last successful step's output, but transport-defined.
    pub final_output: JsonValue,
    /// Chain-level aggregate status. `Success` implies every step in
    /// `steps` is also `Success`.
    pub overall_status: StepStatus,
}

/// Dispatch a single workflow node, or a chain of nodes, and return
/// their result(s).
///
/// See the module-level docs for the layer relationship to
/// [`crate::JobTransport`] and for the timeout contract.
#[async_trait]
pub trait NodeDispatcher: Send + Sync {
    /// Execute one node and return its result. Impls own the full
    /// dispatch lifecycle: wire encoding, signing, transport,
    /// per-attempt timeout, retries, result decoding.
    async fn dispatch(&self, job: DispatchJob) -> Result<DispatchResult, BoxError>;

    /// Execute a linear chain of steps as a single unit. Used for
    /// pipeline-chain optimization where the engine has detected a
    /// sequence of nodes that can share a sandbox.
    ///
    /// The default body delegates to [`dispatch_chain_sequential`],
    /// which loops over [`dispatch`](Self::dispatch) and assembles a
    /// `ChainDispatchResult`. Batch-capable transports (the reference
    /// NATS impl uses a `PipelineJobRequest` batch) should **override**
    /// this method to get the round-trip savings and, if
    /// `share_sandbox` is load-bearing for the consumer, a truly
    /// shared worker sandbox — the default impl does not provide
    /// either.
    async fn dispatch_chain(
        &self,
        request: ChainDispatchRequest,
    ) -> Result<ChainDispatchResult, BoxError> {
        dispatch_chain_sequential(self, request).await
    }
}

/// Helper for `NodeDispatcher` impls that lack a batch transport.
///
/// Dispatches each step sequentially via
/// [`NodeDispatcher::dispatch`] and assembles a `ChainDispatchResult`
/// with the step outputs. On the first `Err`, subsequent steps are
/// not attempted; the returned `ChainDispatchResult` has an
/// `overall_status` of `Failed` and truncated `steps`.
///
/// Note: this does not provide sandbox sharing. If `share_sandbox` is
/// load-bearing for a consumer, they MUST implement batch dispatch.
pub async fn dispatch_chain_sequential<D: NodeDispatcher + ?Sized>(
    dispatcher: &D,
    request: ChainDispatchRequest,
) -> Result<ChainDispatchResult, BoxError> {
    let mut steps = Vec::with_capacity(request.steps.len());
    let mut last_output = JsonValue::Null;
    for job in request.steps {
        let module_id = job.module_id;
        let started = std::time::Instant::now();
        match dispatcher.dispatch(job).await {
            Ok(result) => {
                last_output = result.output.clone();
                steps.push(ChainStepResult {
                    module_id,
                    status: StepStatus::Success,
                    output: result.output,
                    error: None,
                    execution_time_ms: u64::try_from(started.elapsed().as_millis())
                        .unwrap_or(u64::MAX),
                });
            }
            Err(e) => {
                steps.push(ChainStepResult {
                    module_id,
                    status: StepStatus::Failed,
                    output: JsonValue::Null,
                    error: Some(e.to_string()),
                    execution_time_ms: u64::try_from(started.elapsed().as_millis())
                        .unwrap_or(u64::MAX),
                });
                return Ok(ChainDispatchResult {
                    steps,
                    final_output: JsonValue::Null,
                    overall_status: StepStatus::Failed,
                });
            }
        }
    }
    Ok(ChainDispatchResult {
        steps,
        final_output: last_output,
        overall_status: StepStatus::Success,
    })
}

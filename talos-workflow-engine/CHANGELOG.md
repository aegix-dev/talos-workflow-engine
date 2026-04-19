# Changelog

All notable changes to `talos-workflow-engine` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Pre-1.0: breaking changes may occur in any minor version. Once the public
API stabilizes alongside `talos-workflow-engine-core`, the crate will move to
1.0 and normal semver applies.

## [Unreleased]

### Added

- Public `WorkflowEngineError` enum (in new `error` module) with
  documented failure-mode variants (`SecretsResolverMissing`,
  `GraphCyclic`), wrappers around lower-level errors (`GraphJson`,
  `Subflow`), and catch-alls (`LoadGraph`, `Execution`).
- New `graph_json` module exposing `SCHEMA_DOC` (the canonical schema
  reference embedded at compile time), `validate` /
  `validate_value` (structural check returning a `GraphSummary`
  without instantiating an engine), and `GraphJsonError` /
  `GraphSummary` types.
- Compile-time `llm-primitives` feature-coherence check in `lib.rs`:
  if a downstream `Cargo.toml` enables the feature on
  `talos-workflow-engine-core` while disabling it on this crate, the
  build fails with a descriptive message instead of silently
  producing un-dispatchable LLM nodes at runtime.
- Runnable end-to-end demo at `examples/hello_workflow.rs`. Wires
  every adapter via `talos-workflow-engine-test-utils`, scripts a
  `NodeDispatcher`, and prints per-node outputs without touching
  NATS or a wasm runtime.
- Graph-JSON parser accepts four new `kind` tags:
  `while_loop`, `repeat_loop`, `fan_in`, `error_handler`. These had
  `SystemNodeKind` variants and engine dispatch paths already but
  previously only round-tripped via imperative construction. See
  `docs/graph-json-schema.md` for their `data` shapes.

### Changed

- Added public accessor methods on `ParallelWorkflowEngine`:
  `graph()`, `node_map()`, `node_labels()`, `node_configs()`,
  `node_meta()`, `execution_timeout_secs()`, and `dry_run()`. These are
  the canonical public API for reading engine state. The corresponding
  struct fields are `#[doc(hidden)]` and will become private in a
  future minor release; callers should migrate to the accessors.
- Internal source reorganisation: `engine.rs` (was 8,967 lines) split
  into `chain_detect` (linear-chain detection), `graph_parser` (JSON →
  `SystemNodeKind` decoding, retry-policy parsing), `sandbox`
  (per-execution scratch dir + RAII guard), `secrets_pipeline` (node
  secret resolution + envelope sealing), `validation` (config pattern
  validator + output sanitizer), and `scheduler_handlers` (per-
  `SystemNodeKind` dispatch methods lifted from the reactor body).
  The scheduler body in `run_with_transport_inner` shrank from 3,025
  lines to ~1,713 (~44%) by extracting 18 handlers: local computation
  (Collect, Synthesize, Verify, FanIn), local iteration (WhileLoop,
  RepeatLoop), sub-workflow dispatch (SubWorkflow, Loop, AgentLoop,
  Judge, Ensemble, ReflectiveRetry, LlmDispatch, ConfidenceGate,
  DynamicDispatch, CapabilityDispatch), and the generic pre-filters
  (Skip-condition, ErrorHandler pattern-match). A shared
  `unblock_successors` helper replaces the ~15 copies of the
  decrement-and-enqueue boilerplate that had drifted between two
  formulations. `DynamicDispatch` and `CapabilityDispatch` share a new
  `run_dispatched_subworkflow` helper (seeded with a `DispatchedOrigin`
  enum) instead of open-coding the same sub-engine-build pattern
  twice.
  The two largest remaining inline blocks — single-node module
  dispatch (~370 lines) and pipeline-chain dispatch (~490 lines) —
  are now named methods on `ParallelWorkflowEngine`:
  `run_single_node_dispatch` and `run_pipeline_chain_dispatch`. Each
  is an `async fn` that the reactor hands to `executing.push` rather
  than an inline `async move` closure; state that used to be cloned
  into the closure is now accessed through `&self` directly. The
  rate-limit check (`check_rate_limit`) and speculative module
  prefetch (`maybe_speculative_prefetch`) are also separate helpers
  so the reactor flow reads as rate-limit → dispatch → prefetch →
  continue.
  Final scheduler body in `run_with_transport_inner`: 3,025 → 827
  lines (~73% reduction). The parallel
  `run_with_seed_with_transport_inner` body remains as follow-up.
  The public API is unchanged apart from the additions noted above.
- **Breaking** (behavior, not signature): `load_from_graph_json`
  (sync, `&Value`) and `load_graph_from_json` (async, `&str`) now share
  a single authoritative parser. The sync entry point previously
  accepted only module nodes and silently dropped system nodes; it now
  parses the full graph shape — system nodes, reserved-key lifts,
  full edge handles, and `execution_timeout_secs`. It also rejects
  graphs with zero nodes (previously it accepted them and produced an
  empty engine), matching the async entry point's behavior. The async
  entry point retains its rate-limit pre-load and sub-workflow graph
  prefetch as post-parse async work; callers who need those should
  keep using the async variant.
- **Breaking**: `WorkflowGraphBuilderError::UnsupportedSystemNodeKind`
  is removed. Every [`SystemNodeKind`] variant now round-trips through
  the builder and the engine's JSON parser — the parser gained
  `while_loop`, `repeat_loop`, `fan_in`, and `error_handler` branches,
  so there is no longer an "unsupported" subset. The enum is now
  `#[non_exhaustive]` with only `UnknownNodeId`; callers who matched
  the removed variant should delete that arm.
- **Breaking**: [`WorkflowGraphBuilder`] now accumulates configuration
  errors and surfaces them at [`build`]. `add_system_node` no longer
  returns `Result<Self, UnsupportedSystemNodeKind>`; it returns `Self`
  and records unsupported variants into the accumulator. The `with_*`
  mutators (`with_skip_condition`, `with_continue_on_error`, `with_retry`)
  that used to silently no-op on unknown node ids now record an
  `UnknownNodeId` error — typos in ids fail loudly at build time instead
  of silently dropping the intended configuration. [`build`] returns
  `Result<JsonValue, BuildError>`; use the new `build_partial()` helper
  to get the graph and errors side-by-side. `UnsupportedSystemNodeKind`
  is replaced by `WorkflowGraphBuilderError::UnsupportedSystemNodeKind`.
- **Breaking**: `ParallelWorkflowEngine::run_with_transport`,
  `run_with_seed_with_transport`, and the sub-workflow dispatch helpers
  now take `Option<WorkerSharedKey>` instead of `Option<Arc<Vec<u8>>>`
  for the worker shared-signing key. `WorkerSharedKey` is a newtype in
  `talos-workflow-engine-core` wrapping `Arc<[u8]>`; it is cheap to
  clone across spawned dispatch tasks, semantically typed, and redacted
  in `Debug` output. Migrate: replace
  `Some(Arc::new(key_bytes))` with
  `Some(WorkerSharedKey::new(key_bytes))`.
- **Breaking**: public methods on `ParallelWorkflowEngine` now
  return `Result<_, WorkflowEngineError>` instead of `Result<_, String>`.
  Affected: `run_with_transport`, `run_with_seed_with_transport`,
  `load_graph_from_json`, `load_from_graph_json`, `add_edge`,
  `validate_config_patterns`, `AdapterSet::into_engine_with_graph`.
  Internal scheduling code keeps its `String`-based error flow; the
  public wrappers wrap once at the boundary.
- **Breaking**: `talos-workflow-engine-nats` `run_with_nats` /
  `run_with_seed_via_nats` propagate the typed error, matching the
  engine signatures they wrap.
- `SystemNodeKind` rustdoc grouped into a "Choosing a variant"
  taxonomy table (iteration / coordination / control flow /
  sub-workflow / runtime dispatch, with LLM groups gated behind
  `llm-primitives`).

## [0.1.0] — Initial release

- `ParallelWorkflowEngine` — DAG scheduler with topological dispatch,
  linear-chain detection and pipeline batching, bounded concurrent fan-out,
  speculative module prefetching, sub-workflow primitives, checkpoint /
  resume, and retry-with-classifier integration.
- `JudgeVerdict`, `SubflowError`, `AdapterSet` — supporting types for
  sub-workflow contracts and adapter wiring.
- `detect_linear_chains` — pure graph function exposed for reuse.
- `validate_config_patterns` — pre-dispatch config validation helper.
- `emit_event_spawn` — fire-and-forget helper around `EventSink::emit`.
- `vault_resolver` — `vault://...` reference extraction + allowlist merge +
  in-place plaintext substitution for per-dispatch secret injection.
- Rhai-backed expression evaluator wired to the
  `talos-workflow-engine-core::ExpressionEvaluator` trait.

# Changelog

All notable changes to `talos-workflow-engine-core` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Pre-1.0: breaking changes may occur in any minor version. Once the trait
surface stabilizes, the crate will move to 1.0 and normal semver applies.

## [Unreleased]

### Changed

- **Breaking**: `DispatchJob::user_id` and `ChainDispatchRequest::user_id`
  are now `Option<Uuid>` instead of `Uuid`. Previously, `Uuid::nil()` was
  a documented sentinel for "no user context" — a typical footgun where a
  caller who forgot to populate the field got nil-routing silently. The
  explicit `Option` removes the sentinel and makes the two states
  unambiguous at the type level. Wire formats that still require a
  non-optional `Uuid` (e.g. `talos-workflow-job-protocol::JobRequest`)
  substitute `Uuid::nil()` at their own boundary, preserving on-the-wire
  compatibility. Migrate `user_id: uid` → `user_id: Some(uid)` (or `None`
  for unauthenticated test runs).

### Added

- `WorkerSharedKey` newtype wrapping `Arc<[u8]>` for the engine↔worker
  HMAC signing key. Replaces the raw `Arc<Vec<u8>>` used in public APIs:
  cheap to clone into spawned dispatch tasks, semantically typed so
  unrelated byte buffers cannot be passed by accident, and redacted in
  `Debug` output (logging `?key` reports only the byte length).
- `HAS_LLM_PRIMITIVES: bool` constant exposing the active state of
  this crate's `llm-primitives` feature. Sibling crates use it for
  compile-time feature-coherence checks (see
  `talos-workflow-engine`).
- `SystemNodeKind` rustdoc reorganised into a "Choosing a variant"
  taxonomy table grouping the variants by intent (iteration /
  coordination / control flow / sub-workflow / runtime dispatch /
  LLM judging / LLM agent loops / LLM dispatch). The LLM groups are
  gated by the `llm-primitives` feature, so the table renders
  cleanly in either build configuration.

## [0.1.0] — Initial release

- Core data model: `WorkflowContext`, `EdgeLogic`, `RetryPolicy`, `SystemNodeKind`, `JoinMode`.
- Trait surface for a portable workflow executor: `NodeDispatcher`, `JobTransport`,
  `EventSink`, `NodeLifecycleHook`, `ApprovalGate`, `SecretsResolver`,
  `CheckpointStore`, `ModuleFetcher`, `ModuleExecutionStore`,
  `WorkflowGraphStore`, `ExpressionEvaluator`, `OutputSanitizer` /
  `ExecutionSanitizer`, `RetryClassifier`.
- Protocol types: `DispatchJob`, `WasmModuleArtifact`, `NodeCompletionContext`,
  `ExecutionStartedContext`, `NodeEventWrite`.
- `BoxError` alias for trait-boundary error propagation.
- Dependency allowlist: `async-trait`, `serde`, `serde_json`, `uuid`. No
  async runtime. No I/O crates.

# Changelog

All notable changes to `talos-workflow-engine-core` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Pre-1.0: breaking changes may occur in any minor version. Once the trait
surface stabilizes, the crate will move to 1.0 and normal semver applies.

## [Unreleased]

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

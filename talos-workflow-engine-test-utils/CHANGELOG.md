# Changelog

All notable changes to `talos-workflow-engine-test-utils` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Pre-1.0: breaking changes may occur in any minor version. Once the public
API stabilizes alongside `talos-workflow-engine-core`, the crate will move to
1.0 and normal semver applies.

## [Unreleased]

## [0.1.0] — Initial release

- `memory` — in-memory implementations of `WorkflowGraphStore`,
  `CheckpointStore`, `ModuleFetcher`, `ModuleExecutionStore`,
  `SecretsResolver` for fast unit tests.
- `capture` — record-and-assert implementations of `EventSink`,
  `NodeLifecycleHook`, `ModuleExecutionStore` that store every call in a
  thread-safe buffer a test can drain at the end.
- `dispatch` — scriptable `NodeDispatcher` / `JobTransport` pair for
  testing engine logic without a real worker.
- `approval` — configurable `ApprovalGate` (always-approve,
  always-deny, always-pending).
- `noop` — trivial default impls (`StubExpressionEvaluator`,
  `NoopOutputSanitizer`, `NoopRetryClassifier`) for tests that don't
  exercise those paths.

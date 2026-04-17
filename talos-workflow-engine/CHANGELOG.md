# Changelog

All notable changes to `talos-workflow-engine` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Pre-1.0: breaking changes may occur in any minor version. Once the public
API stabilizes alongside `talos-workflow-engine-core`, the crate will move to
1.0 and normal semver applies.

## [Unreleased]

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

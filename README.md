# talos-workflow-engine

[![CI](https://github.com/aegix-dev/talos-workflow-engine/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/aegix-dev/talos-workflow-engine/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![MSRV: 1.88](https://img.shields.io/badge/MSRV-1.88-blue.svg)](#msrv)

A Rust-native workflow engine for **WASM-sandboxed module execution**
with **first-class AI/agent primitives** and a signed job protocol
for dispatching to remote worker pools.

## When to reach for this

- You're building a **Rust-native AI agent orchestration platform**
  and want workflow primitives like `Judge`, `Ensemble`,
  `ReActLoop`, and `ConfidenceGate` as building blocks rather than
  reimplementing them from `tokio::spawn`.
- You're running a **WASM module executor** (wasmtime or similar)
  and want a pluggable DAG scheduler that speaks your module format
  natively via `WasmModuleArtifact` + signed NATS dispatch.
- You need **checkpoint-based execution** — pause/resume on `Wait`
  nodes, retry-with-classifier, per-attempt observability — but
  don't want to run a Temporal server.

## When something else fits better

- **Plain async DAGs in-process**, no durability: reach for
  `tokio::join!` + `futures::future::join_all`. Much lower bar.
- **Background job queues** (retry-on-failure, rate-limited workers,
  no DAGs): use [`apalis`](https://crates.io/crates/apalis),
  [`sqlxmq`](https://crates.io/crates/sqlxmq), or
  [`faktory-rs`](https://crates.io/crates/faktory).
- **Enterprise durable-execution / saga orchestration** with multi-
  language SDKs: use [Temporal](https://temporal.io). Industry
  standard.
- **Python-centric LLM agent orchestration**:
  [LangGraph](https://github.com/langchain-ai/langgraph) /
  [CrewAI](https://github.com/joaomdmoura/crewai) have more mature
  ecosystems in their language.
- **Streaming dataflow or materialized views**:
  [Arroyo](https://arroyo.dev) / [Materialize](https://materialize.com).

## The crates

| Crate | What it is |
|---|---|
| [`talos-workflow-engine-core`](./talos-workflow-engine-core) | Types + trait boundaries. No I/O, no runtime. |
| [`talos-workflow-engine`](./talos-workflow-engine) | Parallel DAG scheduler. The executor. |
| [`talos-workflow-engine-nats`](./talos-workflow-engine-nats) | NATS-backed dispatcher + transport. |
| [`talos-workflow-engine-test-utils`](./talos-workflow-engine-test-utils) | In-memory + capture trait impls for tests. |
| [`talos-workflow-job-protocol`](./talos-workflow-job-protocol) | Signed HMAC wire format between engine and workers. |

### How the crates fit together

```text
                ┌──────────────────────────────────────┐
                │  talos-workflow-engine-core          │
                │  • types: SystemNodeKind, EdgeLogic, │
                │    DispatchJob, WorkflowContext, …   │
                │  • traits: NodeDispatcher,           │
                │    SecretsResolver, ModuleFetcher,   │
                │    CheckpointStore, EventSink,       │
                │    NodeLifecycleHook, ApprovalGate,  │
                │    OutputSanitizer, RetryClassifier, │
                │    ExpressionEvaluator,              │
                │    WorkflowGraphStore, RateLimitStore│
                │    SecretEnvelope, JobTransport      │
                └────────────┬─────────────────────────┘
                             │ depends on (every other crate consumes core)
       ┌─────────────────────┼─────────────────────────┬──────────────────┐
       ▼                     ▼                         ▼                  ▼
  ┌──────────────┐  ┌─────────────────────┐  ┌─────────────────────┐ ┌──────────────┐
  │ talos-       │  │ talos-workflow-     │  │ talos-workflow-     │ │ talos-       │
  │ workflow-    │  │ job-protocol        │  │ engine-test-utils   │ │ workflow-   │
  │ engine       │  │                     │  │                     │ │ engine-nats  │
  │              │  │ • JobRequest /      │  │ • InMemory* stores  │ │              │
  │ • Parallel   │  │   JobResult wire    │  │   (ModuleFetcher,   │ │ • Nats       │
  │   DAG        │  │   format (HMAC-     │  │   GraphStore,       │ │   Node       │
  │   scheduler  │  │   SHA256 + AES-GCM) │  │   Secrets, …)       │ │   Dispatcher │
  │ • Holds      │  │ • PipelineJob*      │  │ • Capture* hooks    │ │ • run_with_  │
  │   Arc<dyn    │  │   batch shape       │  │ • ScriptedDispatcher│ │   nats helpe │
  │   Trait>     │  │ • AesGcmSecret-     │  │ • CountingRate-     │ │ • Implements │
  │   for every  │  │   Envelope          │  │   LimitStore        │ │   Node-      │
  │   trait      │  │                     │  │ • minimal_engine()  │ │   Dispatcher │
  │              │  │                     │  │                     │ │   + Job-     │
  │              │  │                     │  │                     │ │   Transport  │
  └──────┬───────┘  └─────────────────────┘  └──────────┬──────────┘ └──────┬───────┘
         │                                              │                   │
         │ also depends on job-protocol for the         │ dev-dependency    │ depends on
         │ default AesGcmSecretEnvelope                 │ on -engine        │ -engine and
         │                                              │ (minimal_engine() │ job-protocol
         │                                              │ helper)           │
         └──────────────────────────────────────────────┴───────────────────┘
```

The arrows go from a crate to its dependency. Read top-to-bottom: every
crate depends on `-core`; the engine pulls in `job-protocol` for the
default `SecretEnvelope`; the NATS adapter and test-utils both pull in
the engine for runnable wiring; nothing depends on the engine *back*
except its test/transport siblings.

The thing this picture is meant to make obvious: **the engine has no
direct dependency on NATS, Postgres, Redis, or any concrete I/O.** The
only place a transport or storage backend touches engine code is
through the trait boundaries declared in `-core`. Adding a new
transport (gRPC, in-process, shell-out) means adding a sibling crate
alongside `-nats` — never editing the engine itself.

Per-crate contributor guides under `*/AGENTS.md` cover the
crate-specific invariants in detail.

## What the engine does

- **DAG topological dispatch** with bounded concurrent fan-out.
- **Linear-chain detection** → pipeline batch dispatch through one
  transport round-trip instead of per-node.
- **Speculative module prefetch** while the parent node still runs.
- **Sub-workflow primitives.** The engine's built-in
  [`SystemNodeKind`](https://docs.rs/talos-workflow-engine-core/0.2/talos_workflow_engine_core/enum.SystemNodeKind.html)
  enum covers 21 variants — from generic flow control (`ForEach`,
  `FanIn`, `WhileLoop`, `RepeatLoop`, `Wait`, `ErrorHandler`,
  `Synthesize`, `Collect`, `Verify`, `SubWorkflow`, `Loop`,
  `DynamicDispatch`, `CapabilityDispatch`) to LLM/agent shapes
  (`Judge`, `InlineJudge`, `Ensemble`, `LlmDispatch`, `AgentLoop`,
  `ReActLoop`, `ReflectiveRetry`, `ConfidenceGate`). The LLM variants
  live behind the default-on `llm-primitives` feature. Every variant
  round-trips through both paths: the React-Flow `graph_json` parser
  accepts every kind as a string tag (see
  [docs/graph-json-schema.md](./docs/graph-json-schema.md)), and the
  programmatic [`WorkflowGraphBuilder`](https://docs.rs/talos-workflow-engine/0.2/talos_workflow_engine/struct.WorkflowGraphBuilder.html)
  emits the same shape from Rust code.
- **Checkpoint / resume**: pause on `Wait` nodes (the engine
  short-circuits the reactor and returns
  `WorkflowContext { waiting: true, .. }`); resume by re-running through
  `run_with_seed_with_transport` with the paused-node id mapped to
  the external resume value. Checkpoint storage is the consumer's
  responsibility — implement
  [`CheckpointStore`](https://docs.rs/talos-workflow-engine-core/0.2/talos_workflow_engine_core/trait.CheckpointStore.html)
  against whatever backing store fits. See
  [`docs/checkpoint-lifecycle.md`](./docs/checkpoint-lifecycle.md) for
  the full walkthrough and `examples/checkpoint_resume.rs` for a
  runnable end-to-end demo.
- **Retry with classifier** → transient / permanent decisions and
  expression-driven delay.
- **Vault reference injection** (`vault://…` in node config) →
  allowlist-aware plaintext resolution per dispatch.
- **Signed-NATS job protocol** with HMAC-SHA256, canonical-bytes
  signing, AES-256-GCM envelope on secrets, and replay-resistant nonces.

## Design principles

- **Pluggable I/O boundaries.** Transport, graph storage, secrets,
  events, sanitizers, module fetch, approval gates, retry
  classification, and expression evaluation are all traits. No crate
  here opens a Postgres / Redis / S3 connection or spawns HTTP clients
  on its own — consumers wire those in via trait impls.
- **Dyn-compatible traits.** The engine holds `Arc<dyn Trait>` for
  each boundary — no generics leak through the scheduling loop.
- **No runtime lock-in in `-core`.** The types + traits crate depends
  only on `async-trait`, `serde`, `serde_json`, `uuid`. You pick the
  async runtime in the executor crate.
- **Security-by-default on the wire.** The optional
  `talos-workflow-job-protocol` wire format ships HMAC signing, fresh
  AES-GCM keys per dispatch, and a reserved vault-path deny-list for
  LLM provider keys. The engine itself is wire-format-agnostic —
  consumers who don't want the HMAC + AES envelope can implement
  `NodeDispatcher` directly without it.
- **Caveat (0.1):** the engine creates per-execution scratch
  directories under a sandbox root (default
  `<std::env::temp_dir()>/workflow-engine-sandboxes` — Linux/macOS
  `/tmp/...`, Windows `%TEMP%\...`) for modules that need a
  filesystem scratch space. The platform-appropriate default is
  resolved via
  [`default_sandbox_root()`](https://docs.rs/talos-workflow-engine/0.2/talos_workflow_engine/fn.default_sandbox_root.html);
  override or disable per-engine via
  [`ParallelWorkflowEngine::set_sandbox_root`](https://docs.rs/talos-workflow-engine/0.2/talos_workflow_engine/struct.ParallelWorkflowEngine.html#method.set_sandbox_root)
  (pass `None` to skip sandbox creation entirely).

## Quickstart

Pick the subset of crates you need:

```toml
[dependencies]
talos-workflow-engine-core = "0.2"
talos-workflow-engine      = "0.2"
talos-workflow-engine-nats = "0.2"   # if you use NATS
talos-workflow-job-protocol = "0.2"  # transitive via -nats; add directly for custom workers

[dev-dependencies]
talos-workflow-engine-test-utils = "0.2"
```

Build a graph programmatically with
[`WorkflowGraphBuilder`](https://docs.rs/talos-workflow-engine/0.2/talos_workflow_engine/struct.WorkflowGraphBuilder.html):

```rust,ignore
use std::time::Duration;
use serde_json::json;
use uuid::Uuid;
use talos_workflow_engine::{ParallelWorkflowEngine, WorkflowEngineError, WorkflowGraphBuilder};
use talos_workflow_engine_core::SystemNodeKind;

let module_id = Uuid::new_v4();
let graph = WorkflowGraphBuilder::new()
    .execution_timeout(Duration::from_secs(600))
    .add_module("fetch", module_id, Some(json!({ "url": "..." })))
    .add_system_node("split", SystemNodeKind::ForEach {
        input_path: "items".into(),
        output_handle: "element".into(),
    })
    .edge("fetch", "split")
    .build()?;

let mut engine = ParallelWorkflowEngine::new();
// ... wire adapters via engine.set_* methods ...
engine.load_graph_from_json(&serde_json::to_string(&graph)?).await?;
# Ok::<(), WorkflowEngineError>(())
```

### A runnable, end-to-end example

The fully-wired demo in
[`talos-workflow-engine/examples/hello_workflow.rs`](./talos-workflow-engine/examples/hello_workflow.rs)
builds a 3-node fan-out graph, wires every adapter via
`talos-workflow-engine-test-utils`, scripts a `NodeDispatcher`, runs
the workflow end-to-end, and prints each node's output. No NATS, no
wasm runtime, no network — everything is in-process:

```bash
cargo run --example hello_workflow -p talos-workflow-engine
```

[`minimal_engine()`](https://docs.rs/talos-workflow-engine-test-utils/0.2/talos_workflow_engine_test_utils/fn.minimal_engine.html)
from `talos-workflow-engine-test-utils` (used in that example) wires
in-memory defaults for every trait so a fresh engine can dispatch
within ten lines of code.

A second runnable example,
[`examples/checkpoint_resume.rs`](./talos-workflow-engine/examples/checkpoint_resume.rs),
demonstrates the full pause-and-resume cycle with a custom
`NodeDispatcher` and an in-memory `CheckpointStore`:

```bash
cargo run --example checkpoint_resume -p talos-workflow-engine
```

### Integration guides

Three topic-focused walkthroughs live under [`docs/`](./docs):

- [Implementing a custom `NodeDispatcher`](./docs/custom-dispatcher.md) —
  HTTP / gRPC / in-process / shell-out, with the timeout, error,
  encrypted-secrets, dry-run, and chain-batching contracts.
- [Checkpoint lifecycle](./docs/checkpoint-lifecycle.md) — when the
  engine pauses, how to implement `CheckpointStore`, and the
  `Wait` → snapshot → resume flow end-to-end.
- [Composing sub-workflows](./docs/sub-workflow-composition.md) —
  designing `Judge`, `Ensemble`, `AgentLoop`, `ReActLoop`, and
  `ReflectiveRetry` child graphs; verdict-shape contracts; when to
  reach for `InlineJudge` / `Synthesize` / `Verify` instead of a
  full sub-workflow.
- [Implementing a `WorkflowGraphStore`](./docs/workflow-graph-store.md) —
  the trait that backs every sub-workflow lookup. Covers the
  per-tenant security contract, the batch-prefetch path, and a
  Postgres-flavoured reference impl (with the load-bearing
  `get_graphs` override).
- [Production stack walkthrough](./docs/production-stack.md) — the
  end-to-end assembly of Postgres + Redis + NATS into one engine.
  Configuration knobs, security defaults, common operational
  issues. Read this when the per-trait guides have left you
  asking "how do I put it all together?"

See each crate's README for deeper context and
[docs/graph-json-schema.md](./docs/graph-json-schema.md) (or the
embedded
[`talos_workflow_engine::SCHEMA_DOC`](https://docs.rs/talos-workflow-engine/0.2/talos_workflow_engine/constant.SCHEMA_DOC.html))
for the full graph-JSON shape. The
[`validate_graph_json`](https://docs.rs/talos-workflow-engine/0.2/talos_workflow_engine/fn.validate_graph_json.html)
function checks a payload's structure, classifies its nodes, and
returns a [`GraphSummary`](https://docs.rs/talos-workflow-engine/0.2/talos_workflow_engine/struct.GraphSummary.html)
without needing an engine instance — useful for CI lints or editor
diagnostics.

### Errors

Public methods on `ParallelWorkflowEngine` return
[`Result<_, WorkflowEngineError>`](https://docs.rs/talos-workflow-engine/0.2/talos_workflow_engine/error/enum.WorkflowEngineError.html).
The variant taxonomy splits into documented failure modes
(`SecretsResolverMissing`, `GraphCyclic`, `Timeout { secs }`),
wrappers around lower-level errors (`GraphJson`, `Subflow`), and
catch-alls for failure modes the engine has not yet promoted to
typed variants (`LoadGraph`, `Execution`). The variants are stable
(the enum is `#[non_exhaustive]` so additions are non-breaking); the
message bodies on catch-all variants are not.

## Feature flags

| Flag | Default | What it gates |
|---|---|---|
| `llm-primitives` | on | LLM/agent-specific `SystemNodeKind` variants (`Judge`, `InlineJudge`, `Ensemble`, `LlmDispatch`, `AgentLoop`, `ReActLoop`, `ReflectiveRetry`, `ConfidenceGate`) and their engine-side dispatch code. Drop for a leaner build when not orchestrating LLM workflows. |
| `minimal` (test-utils only) | on | Pulls `talos-workflow-engine` into `talos-workflow-engine-test-utils` so `minimal_engine()` is available. Drop when you only need the trait stubs. |

**Set `llm-primitives` coherently across the family.** When you opt
out, do so on **every** sibling crate in your dependency tree —
`talos-workflow-engine-core`, `talos-workflow-engine`, and (if used)
`talos-workflow-engine-nats` and `talos-workflow-engine-test-utils`.
Mixing (e.g. `-core` with the feature on but `-engine` with it off)
would otherwise leave the LLM variants reachable in the type enum but
never dispatched by the engine.

`talos-workflow-engine` carries a `const _: () = assert!(...)` that
fires a compile-time error if the feature on `-core` and `-engine` is
mismatched, so the misconfiguration surfaces during `cargo check`
rather than at runtime.

## MSRV

Rust **1.88**. Pinned via `rust-toolchain.toml` and each crate's
`rust-version` field.

## Stability

Pre-1.0 across the family. Trait surface and wire format may still
move. Once stable, the crates graduate to 1.0 together and normal
semver applies. Release process and version-bump rules live in
[RELEASING.md](./RELEASING.md).

### What's stable now (would survive 1.0)

* The `talos-workflow-engine-core` trait surface — every trait is
  documented end-to-end, has at least one in-tree impl, and is
  covered by `#![deny(missing_docs)]`. Adding new methods (with
  default bodies) is non-breaking; signature changes would force a
  minor bump pre-1.0 and a major bump post-1.0.
* The `WorkflowEngineError` taxonomy — `#[non_exhaustive]`, with
  documented "stable variant, unstable message body" semantics.
  New variants are non-breaking.
* The `talos-workflow-job-protocol` wire format — covered by
  byte-level snapshot tests in
  [`talos-workflow-job-protocol/tests/wire_format_snapshots.rs`].
  Append-at-end field additions are wire-compatible during a
  coordinated controller+worker rollout; reorders / renames are not.
* The `ParallelWorkflowEngine` setter API. Each setter is
  documented; `#[deny(missing_docs)]` enforces it. Adding new
  setters is non-breaking.

### What's still in flux

* The `SystemNodeKind` enum may grow new variants (additive,
  non-breaking) and existing variants may gain new fields
  (`#[non_exhaustive]` would protect this; not yet applied).
* The internal scheduler error type (`Result<_, String>` inside
  `run_inner`) is still being incrementally promoted to typed
  variants — see the open `Execution(String)` catch-all sites.
  Public callers see typed `WorkflowEngineError` regardless.
* The `RateLimitStore` trait is new (added 2026-04). Production
  Redis-backed impls are likely to surface edge cases in the
  fail-open contract that may warrant a follow-up minor bump to
  add a `fail_closed` mode.

### Roadmap

Indicative — no commitments to dates:

* **0.2.0 — Cancellation, configurable limits, RateLimitStore.**
  Already shipped in `[Unreleased]`; cut as 0.2 once a few weeks
  of soak time have passed.
* **0.3.0 — Wire-format additions for worker-side cancellation.**
  Plumb the `DispatchJob.cancellation_token` through a coordinated
  worker upgrade so `WorkflowEngineError::Cancelled` actually
  aborts in-flight WASM, not just the engine reactor.
* **0.4.0 — Public API audit + crater run.** Use
  `cargo public-api diff` to lock down every pre-1.0 surface; run
  the workspace against the 100 most-popular reverse-dependency
  patterns to catch regressions.
* **1.0.0 — Stable.** Conditional on the above + at least one
  publicly-deployed production user willing to be a reference.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md). Each crate additionally
ships an `AGENTS.md` contributor guide covering per-crate invariants
(what goes where, wire-format discipline, security rules).

## Security

Found a vulnerability? See [SECURITY.md](./SECURITY.md) for disclosure.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  <https://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual-licensed as above, without any additional terms
or conditions.

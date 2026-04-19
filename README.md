# talos-workflow-engine

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

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

## What the engine does

- **DAG topological dispatch** with bounded concurrent fan-out.
- **Linear-chain detection** → pipeline batch dispatch through one
  transport round-trip instead of per-node.
- **Speculative module prefetch** while the parent node still runs.
- **Sub-workflow primitives.** The engine's built-in
  [`SystemNodeKind`](https://docs.rs/talos-workflow-engine-core/0.1/talos_workflow_engine_core/enum.SystemNodeKind.html)
  enum covers 19 variants — from generic flow control (`ForEach`,
  `FanIn`, `WhileLoop`, `RepeatLoop`, `Wait`, `ErrorHandler`,
  `Synthesize`, `Collect`, `Verify`, `SubWorkflow`, `Loop`,
  `DynamicDispatch`, `CapabilityDispatch`) to LLM/agent shapes
  (`Judge`, `Ensemble`, `LlmDispatch`, `AgentLoop`, `ReActLoop`,
  `ReflectiveRetry`, `ConfidenceGate`). The LLM variants live
  behind the default-on `llm-primitives` feature. The React-Flow
  `graph_json` parser accepts a subset of these kinds as string
  tags (see [docs/graph-json-schema.md](./docs/graph-json-schema.md));
  the remainder are available via programmatic graph construction.
- **Checkpoint / resume**: pause on `Wait` nodes or cancellation,
  resume later with per-node outputs hydrated.
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
- **Caveat (0.1):** the engine does create per-execution scratch
  directories under a sandbox root (default `/tmp/workflow-engine-sandboxes`)
  for modules that need a filesystem scratch space. Configure or
  disable this via
  [`ParallelWorkflowEngine::set_sandbox_root`](https://docs.rs/talos-workflow-engine)
  (pass `None` to skip sandbox creation entirely).

## Quickstart

Pick the subset of crates you need:

```toml
[dependencies]
talos-workflow-engine-core = "0.1"
talos-workflow-engine      = "0.1"
talos-workflow-engine-nats = "0.1"   # if you use NATS
talos-workflow-job-protocol = "0.1"  # transitive via -nats; add directly for custom workers

[dev-dependencies]
talos-workflow-engine-test-utils = "0.1"
```

Build a graph programmatically with
[`WorkflowGraphBuilder`](https://docs.rs/talos-workflow-engine/0.1/talos_workflow_engine/struct.WorkflowGraphBuilder.html):

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

[`minimal_engine()`](https://docs.rs/talos-workflow-engine-test-utils/0.1/talos_workflow_engine_test_utils/fn.minimal_engine.html)
from `talos-workflow-engine-test-utils` (used in that example) wires
in-memory defaults for every trait so a fresh engine can dispatch
within ten lines of code.

See each crate's README for deeper context and
[docs/graph-json-schema.md](./docs/graph-json-schema.md) (or the
embedded
[`talos_workflow_engine::SCHEMA_DOC`](https://docs.rs/talos-workflow-engine/0.1/talos_workflow_engine/constant.SCHEMA_DOC.html))
for the full graph-JSON shape. The
[`validate_graph_json`](https://docs.rs/talos-workflow-engine/0.1/talos_workflow_engine/fn.validate_graph_json.html)
function checks a payload's structure, classifies its nodes, and
returns a [`GraphSummary`](https://docs.rs/talos-workflow-engine/0.1/talos_workflow_engine/struct.GraphSummary.html)
without needing an engine instance — useful for CI lints or editor
diagnostics.

### Errors

Public methods on `ParallelWorkflowEngine` return
[`Result<_, WorkflowEngineError>`](https://docs.rs/talos-workflow-engine/0.1/talos_workflow_engine/error/enum.WorkflowEngineError.html).
The variant taxonomy splits into documented failure modes
(`SecretsResolverMissing`, `GraphCyclic`), wrappers around lower-
level errors (`GraphJson`, `Subflow`), and catch-alls for failure
modes the engine has not yet promoted to typed variants
(`LoadGraph`, `Execution`). The variants are stable; the message
bodies on catch-all variants are not.

## Feature flags

| Flag | Default | What it gates |
|---|---|---|
| `llm-primitives` | on | LLM/agent-specific `SystemNodeKind` variants (`Judge`, `Ensemble`, `LlmDispatch`, `AgentLoop`, `ReActLoop`, `ReflectiveRetry`, `ConfidenceGate`) and their engine-side dispatch code. Drop for a leaner build when not orchestrating LLM workflows. |
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
semver applies.

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

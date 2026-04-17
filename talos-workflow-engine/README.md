# talos-workflow-engine

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

Parallel DAG-based workflow executor built on the `talos-workflow-engine-core`
trait boundaries.

This crate owns the scheduling loop: it takes a graph of nodes connected
by edges, detects linear chains, fans non-chain work out across a bounded
concurrent pool, speculatively prefetches modules, resolves secrets, and
drives dispatch through a pluggable `NodeDispatcher`. Every external-I/O
boundary (transport, storage, secrets, events, sanitizers, …) is a trait
defined in `talos-workflow-engine-core` — this crate doesn't care what's
behind them.

## What you get

- **DAG topological dispatch** with in-flight concurrency cap.
- **Linear-chain detection** (`detect_linear_chains`) → pipeline batch
  dispatch through `NodeDispatcher::dispatch_chain`, one transport
  round-trip per chain instead of per node.
- **Speculative module prefetching** while the parent node still runs.
- **Sub-workflow primitives**: Judge, Ensemble, ForEach, FanIn,
  AgentLoop, ReActLoop, ReflectiveRetry, LlmDispatch, DynamicDispatch,
  CapabilityDispatch, ConfidenceGate, WhileLoop, RepeatLoop, Wait,
  ErrorHandler, Synthesize, Collect, Verify. Every kind from
  `SystemNodeKind` has a dispatcher.
- **Checkpoint / resume**: pause on `Wait` nodes or cancellation,
  resume later with per-node outputs hydrated.
- **Retry with classifier** → transient / permanent decisions and
  Rhai-expression-driven delay.
- **Vault reference injection** (`vault://...` in node config) →
  allowlist-aware plaintext resolution per dispatch.
- **Rhai-backed expression evaluator** for edge conditions, retry
  delays, and `Synthesize` output transforms.

## Non-goals

- **No storage implementation.** No Postgres, Redis, S3, filesystem.
  Plug in via the `talos-workflow-engine-core` trait impls.
- **No transport implementation.** The sibling `talos-workflow-engine-nats`
  crate ships a NATS-backed one; roll your own for HTTP, in-process,
  gRPC, etc.
- **No LLM integration.** This is a workflow executor, not a model
  runner. LLM calls happen inside worker-side module code.

## Quickstart

```toml
[dependencies]
talos-workflow-engine        = "0.1"
talos-workflow-engine-core   = "0.1"
```

```rust,ignore
use std::sync::Arc;
use talos_workflow_engine::{AdapterSet, ParallelWorkflowEngine};

let adapters = AdapterSet::builder()
    // plug in your impls of core traits:
    //   .with_event_sink(...)
    //   .with_graph_store(...)
    //   .with_checkpoint_store(...)
    //   .with_secrets_resolver(...)
    //   .with_module_fetcher(...)
    //   .with_module_execution_store(...)
    //   .with_node_dispatcher(...)
    .build();

let engine = ParallelWorkflowEngine::new(adapters);
// let result = engine.run(workflow_id, trigger_input).await?;
```

For a full worked example, see `talos-workflow-engine-nats` (NATS transport) and
`talos-workflow-engine-test-utils` (in-memory trait impls).

## Adapter wiring

`AdapterSet` bundles every trait impl the engine needs. Missing adapters
surface at engine-build time, not at the first dispatch. Use
`talos-workflow-engine-test-utils` for cheap defaults when writing unit tests
against the engine.

## Stability

Pre-1.0. The crate moves in lockstep with `talos-workflow-engine-core`. Minor
versions may contain breaking changes until the trait surface stabilizes.

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
license, shall be dual-licensed as above, without any additional terms or
conditions.

# talos-workflow-engine-test-utils

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

In-memory and capture implementations of every `talos-workflow-engine-core`
trait. Lets you unit-test executor and workflow logic without a
database, secrets manager, or transport.

## What's in the box

- **`memory`** — in-memory impls you wire into the engine when you want
  "it works" behavior:
  - `InMemoryWorkflowGraphStore` — graph lookup from a seeded map.
  - `InMemoryCheckpointStore` — per-execution checkpoint state.
  - `InMemoryModuleFetcher` — returns a preconfigured `ModuleArtifact`
    for any id; optional rate-limit map.
  - `InMemoryModuleExecutionStore` — minimal audit-log store.
  - `InMemorySecretsResolver` — layered module / path / LLM-key maps.
- **`capture`** — record-and-assert impls you use when you want to
  verify what the engine *did*:
  - `CaptureEventSink` — every emitted event in a `Vec`.
  - `CaptureLifecycleHook` — every `on_node_completed` /
    `on_node_failed` / `on_pipeline_step_completed` call.
  - `CaptureModuleExecutionStore` — every `record_started` /
    `record_completed` / `resolve_wasm_module_id` call.
- **`dispatch`** — scriptable dispatcher + transport for testing engine
  logic without a real worker:
  - `ScriptedNodeDispatcher` — returns preconfigured responses keyed
    on node or module id; optionally errors to exercise retry paths.
  - `ScriptedJobTransport` — raw bytes-in / bytes-out scripting.
- **`approval`** — `AlwaysApprove`, `AlwaysDeny`, `AlwaysPending`
  `ApprovalGate` impls for each branch.
- **`noop`** — trivial defaults for traits most tests don't care about:
  `StubExpressionEvaluator`, `NoopOutputSanitizer`,
  `NoopRetryClassifier`.

## Quickstart

```toml
[dev-dependencies]
talos-workflow-engine-test-utils = "0.1"
talos-workflow-engine-core       = "0.1"
```

```rust,ignore
use std::sync::Arc;
use talos_workflow_engine_core::{EventSink, NodeEventWrite};
use talos_workflow_engine_test_utils::capture::CaptureEventSink;

#[tokio::test]
async fn emits_expected_events() {
    let sink = CaptureEventSink::new();
    let sink_arc: Arc<dyn EventSink> = Arc::new(sink.clone());

    // ... drive the engine with sink_arc ...

    let events = sink.events();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].status, "running");
}
```

All capture types are `Clone` and share state via `Arc<Mutex<...>>`, so
you can keep a handle for assertions after handing a clone to the
engine.

## Thread-safety / runtime

All capture stores are `Send + Sync` and use `std::sync::Mutex`. The
engine may spawn `tokio::spawn` tasks that call into these types
concurrently — short lock-held critical sections keep contention low
for typical test shapes.

## Stability

Pre-1.0. The module surface moves with `talos-workflow-engine-core`. New
traits upstream → new in-memory / capture impls here in the same PR.

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

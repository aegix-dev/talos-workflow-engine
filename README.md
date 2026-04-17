# talos-workflow-engine

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

A portable, pluggable workflow execution engine for Rust. Five crates
that compose into a DAG-based executor with pluggable transport,
storage, secrets, and observability.

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
- **Sub-workflow primitives**: Judge, Ensemble, ForEach, FanIn,
  AgentLoop, ReActLoop, ReflectiveRetry, LlmDispatch, DynamicDispatch,
  CapabilityDispatch, ConfidenceGate, WhileLoop, RepeatLoop, Wait,
  ErrorHandler, Synthesize, Collect, Verify.
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

See each crate's README for a worked example.

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
leaves the LLM variants reachable in the type enum but never dispatched
by the engine — the engine parses them as `None`-kind and rejects at
run time. There is no compile-time error for the mismatch; it surfaces
only at workflow execution.

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

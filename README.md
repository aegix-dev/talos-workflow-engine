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

- **Pluggable everything.** Every external-I/O boundary (transport,
  storage, secrets, events, sanitizers) is a trait. No crate in this
  workspace talks to Postgres, Redis, S3, or the filesystem directly.
- **Dyn-compatible traits.** The engine holds `Arc<dyn Trait>` for
  each boundary — no generics leaking through the scheduling loop.
- **No runtime lock-in in `-core`.** The types + traits crate depends
  only on `async-trait`, `serde`, `serde_json`, `uuid`. You pick the
  runtime in the executor crate.
- **Security-by-default on the wire.** HMAC signing, fresh AES keys
  per dispatch, reserved vault-path deny-lists for LLM providers.

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

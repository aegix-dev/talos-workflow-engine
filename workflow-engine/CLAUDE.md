# workflow-engine — Claude guide

This crate hosts `ParallelWorkflowEngine` and the scheduling loop. Every
external-I/O concern is behind a `workflow-engine-core` trait boundary;
the engine holds `Arc<dyn Trait>` for each and calls through them.

This file captures the non-obvious rules for working in this crate —
invariants that matter for correctness, patterns that previous work
converged on, and pitfalls that have bitten production.

## Core invariant: zero concrete controller types

The engine body contains **zero** references to Talos controller types
(`ModuleRegistry`, `SecretsManager`, `WorkflowRepository`, etc.). Every
lookup, every side effect, every decision flows through a
`workflow-engine-core` trait object.

Verify before committing:

```bash
grep -c "crate::" crates/workflow-engine/src/engine.rs   # expect 0 (ignore tests)
```

`crate::` prefixes in the engine body mean we're reaching into this
crate's own modules — fine. `use crate::registry::`, `use
crate::secrets::` — that's a controller type leaking in. Block it.

If a new feature requires data the engine doesn't currently have access
to, add a method to the relevant core trait (with a default body if
possible — see `workflow-engine-core/CLAUDE.md`). Don't reach around the
trait surface by pulling in a concrete type.

## The dispatch-path rule (the 2026-04-16 loop-node lesson)

**Every dispatch path MUST populate `encrypted_secrets`.** A past
regression sent `encrypted_secrets: Default::default()` (empty
ciphertext) in loop-body dispatches, silently breaking vault-resolved
header injection and LLM calls inside loops. The symptom was "module
runs, returns error," with no obvious cause.

Rule: when adding a new dispatch path (new system-node kind, new
parallel executor, anything that constructs a `DispatchJob`), you MUST
call `self.build_encrypted_secrets(node_id, &worker_shared_key)` or
`build_encrypted_secrets_for(...)` at a non-Self-borrowing site.

**Never** write `encrypted_secrets: Default::default()` in a production
dispatch path. That's a silent security regression waiting to happen.

Grep for `encrypted_secrets:` in `engine.rs` to see the expected shape
of every dispatch site.

## The `AdapterSet` pattern for sub-workflow closures

Sub-workflow dispatch (Judge / Ensemble / AgentLoop / ReActLoop / ...)
builds child engines inside `async move` closures. The engine's `self`
can't cross the closure boundary — but the closure may need to
instantiate **multiple** child engines (agent loops iterate).

Pattern: capture `let adapter_set_al = self.adapter_set();` BEFORE the
`async move`, clone the set per iteration inside, and hydrate:

```rust
let mut sub_engine = adapter_set_al.clone().into_engine_with_graph(&graph_json)?;
sub_engine.set_actor_id(...);
sub_engine.run_with_seed_with_transport(...).await?
```

`AdapterSet::clone()` is bounded (12 `Arc::clone`s = 12 refcount bumps);
cheap per iteration. **Never** capture individual adapter `Arc`s
separately — that's error-prone and gets out of sync when a new trait
is added. `AdapterSet::clone` is the single capture point.

For `&self` paths (non-closure), use `self.adapter_set().into_engine_with_graph(&g)`
or its shortcut `self.new_subengine()`.

## Causal ordering at completion events

`node_completed` and `node_failed` emit **synchronously** (`sink.emit().await`)
before the engine unblocks child nodes. This guarantees observers see a
causally-consistent timeline in the events log — a child's `node_started`
never lands before its parent's `node_completed`.

Other emit sites (`node_started`, `node_input`, `retry_*`, loop iteration)
use `emit_event_spawn` — fire-and-forget tokio spawn.

When adding a new event type, decide deliberately which bucket it
belongs in. If an observer would be confused by this event landing
before/after a related one, use the synchronous path.

## `NodeDispatcher` is abstract; NATS is controller-side

The engine's public entry is `run_with_transport(Arc<dyn NodeDispatcher>, ...)`.
The engine has **no idea** that NATS exists. Controller-side glue lives
in `controller/src/engine/nats_run.rs` and `nats_dispatcher.rs`:

- `NatsNodeDispatcher` impls `NodeDispatcher` for a signed-NATS
  transport.
- `run_with_nats(engine, client, key, exec_id)` wraps `client` in a
  `NatsTransport`, builds a `NatsNodeDispatcher`, calls
  `engine.run_with_transport(dispatcher, ...)`.

**Do not** add `async_nats` or transport-specific logic to this crate.
It's banned at the `Cargo.toml` level for good reason — it would
recouple the engine to NATS.

New transports belong in their own crates. See `nats_dispatcher.rs` as
the reference shape.

## Engine body shims (`self.eval_bool`, `self.redact_str`, etc.)

The engine has thin `&self` helpers that wrap `Option<Arc<dyn Trait>>`
adapter access:

- `self.eval_bool(expr, ctx)` → `ExpressionEvaluator::eval_bool` with
  fallback `false` when no evaluator is wired.
- `self.try_eval_bool(...)` → hard-fail variant.
- `self.redact_str(s)` / `self.redact_json(v)` →
  `OutputSanitizer::redact_*` with passthrough fallback.
- `self.new_execution_sanitizer()` → build a per-run
  `ExecutionSanitizer` or return `None`.

Always call through the shims in the engine body. Direct access to
`self.expression_evaluator.as_ref().map(|e| e.eval_bool(...))` is verbose
and scatters the fallback logic. The shims centralize it.

When adding a new policy trait, add a matching `self.method_name` shim
in the same spot in the impl block.

## The `build_encrypted_secrets` duality

Two variants exist:

- `self.build_encrypted_secrets(node_id, &worker_shared_key)` — the
  `&self` form. Reads `self.node_configs[node_id]` for vault refs.
- `build_encrypted_secrets_for(resolver, node_id, user_id, vault_paths, extra_paths, key)` —
  free function for `async move` closures that can't hold `&self`.

**Use `&self` shim when possible.** The free fn exists only for
closures. If you're inlining the free fn into a spot where `&self` is
available, the `&self` shim is the right call.

Both share the same pipeline (extract vault paths → resolve → encrypt).
Previous work consolidated them into one code path (see commit
ca023c3 — the loop-node secrets fix). Don't re-duplicate.

## Pipeline-chain pitfalls

- **Fuel attribution: chain head only.** `on_node_completed` fires on
  the chain's head node with the chain-aggregate fuel; never on
  per-step completions. Per-step `__memory_write__` side effects fire
  through `on_pipeline_step_completed`, which is deliberately narrow
  (no fuel attribution, no `execution_events` row). Double-count
  prevention is the reason they're separate hook methods.
- **`module_uri` fallback uses `step_module_id`, not `step_node_id`.**
  See commit d43dc52 for the latent-bug fix. The redis-fallback key
  convention is `redis:wasm:{module_id}`; the engine must pass the
  resolved module id, not the graph node UUID. If you add a new
  pipeline-step builder, use `step_module_id` in the redis fallback.
- **Chain-level retry does NOT emit `node_retrying`/`retry_skipped`
  events.** Only single-node dispatch does. If a future observability
  need forces chain-level retry events, wire them through
  `execute_job_with_retry` with a synthetic per-attempt event —
  don't inline them.

## Edge-routing + nil UUID handling

`NatsNodeDispatcher::dispatch` maps `Uuid::nil()` → `None` for the
topic-routing user_id. This preserves the `ENABLE_EDGE_ROUTING=true`
behavior where unset user context routes to the tenant-agnostic subject
instead of `talos.jobs.00000000-...`.

If you add a new dispatch path, handle nil the same way:

```rust
let topic_user = if job.user_id.is_nil() { None } else { Some(job.user_id) };
```

## Retry-event gating

`DispatchJob::emit_retry_events: bool` gates per-attempt event emission
at the dispatcher layer. Loop-body / sub-workflow dispatches set it to
`false` — their retries are internal implementation details and would
pollute the workflow-level `retry_rate` metric. Top-level dispatches set
it to `true`.

When adding a new dispatch site, decide: is this a workflow-visible node
(observable from the outside) or an implementation-detail retry? The
former sets `true`, the latter `false`.

## Constructor policy

The engine struct has **no `registry` field**. All production-path
convenience constructors were deleted in commit `5272dde` — they now
live in `controller/src/engine/builder.rs` as free functions:

- `build_controller_engine(registry, secrets_manager, user_id)` — the
  common path.
- `build_controller_engine_with_resolver(...)` — pre-built resolver.
- `build_controller_engine_registry_only(registry)` — replay/resume.

Each builder sets the adapter trait objects via the engine's public
`set_*` methods. **Do not** add a constructor here that takes a
concrete controller type. If a convenience is needed, it belongs in
the controller builders, not here.

The only constructor in this crate is `ParallelWorkflowEngine::new()` —
returns a bare engine with all `Option<Arc<dyn ...>>` fields `None`.
Tests wire adapters via `workflow-engine-test-utils`.

## `secrets_resolver` is required on the abstract entry

`run_with_transport` and `run_with_seed_with_transport` fail closed
when `self.secrets_resolver` is `None`. This closes the structural
hole that let the 2026-04-16 loop-node regression ship silently — an
unset resolver no longer produces empty-ciphertext dispatches.

Don't weaken this check. If a test path needs to run without secrets,
wire an `InMemorySecretsResolver::new()` (empty) from
`workflow-engine-test-utils` — that's the sanctioned bypass, and it
goes through the same code path production does.

## Testing

- Unit tests live in `engine.rs` as `#[cfg(test)] mod tests`. The
  existing 15 tests cover graph operations (chain detection, result
  collapse, cycle detection). Keep them pure — no async, no mock
  dispatcher.
- End-to-end tests with a real engine + mock dispatcher go in
  `workflow-engine-test-utils` (or in the crate that's actually being
  tested).
- `vault_resolver.rs` has 7 tests of its own — JSON scanning
  correctness.

## Clippy allow-list

The `Cargo.toml` carries a generous `clippy::pedantic` allow-list —
`too_many_lines`, `too_many_arguments`, `cast_possible_truncation`,
etc. These are intentional carve-outs inherited from the engine's
pre-extraction style. **Do not** silence new warnings per-site with
`#[allow(...)]`. If a new warning fires in a way that the allow-list
doesn't already cover, fix the code or expand the allow-list with a
rationale comment.

`unsafe_code = "forbid"` — non-negotiable.

## Post-change checks

Before committing any change:

```
cargo check -p workflow-engine
cargo clippy -p workflow-engine --all-targets -- -D warnings
cargo doc -p workflow-engine --no-deps             # zero warnings
cargo test -p workflow-engine
```

Then verify downstream (the controller is a consumer):

```
cargo check --workspace
cargo test -p workflow-engine-test-utils
```

## When NOT to modify this crate

- If the change is Talos-controller-specific (Postgres table shape,
  NATS wire format, DLP policy) → belongs in the controller.
- If the change is to a core trait signature → belongs in
  `workflow-engine-core` (and then the matching impl here).
- If the change is a new test-double → belongs in
  `workflow-engine-test-utils`.

This crate is for the scheduling loop, the system-node handlers, and
`AdapterSet` plumbing. Nothing else.

## What good PR patterns look like here

- **Adding a system-node kind**: new variant in `SystemNodeKind` (in
  core), new match arm in `engine.rs`'s dispatch, new sub-workflow
  handler if it dispatches a child graph, tests in `engine.rs` for
  the new arm.
- **Adding a dispatch site**: verify `encrypted_secrets` is populated
  via `build_encrypted_secrets*`; verify `emit_retry_events` is set
  appropriately for the observability layer; verify nil-UUID topic
  mapping if it's a new transport adapter (though adapters belong in
  downstream crates, not here).
- **Adding engine state (a new `Option<Arc<dyn SomeTrait>>` field)**:
  add to the struct, add to `AdapterSet`, add to
  `AdapterSet::into_engine`, add to `adapter_set()`, add a
  `set_some_trait` method. Miss any one step and sub-workflow
  dispatch silently drops the adapter.

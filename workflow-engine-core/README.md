# workflow-engine-core

Core data model for a portable workflow execution engine.

This crate holds the types that describe a workflow — nodes, edges, retry
policies, fan-in join modes, the system-node taxonomy, and the runtime
context that flows through execution. It does **not** contain the executor
itself; that lives in sibling crates (`workflow-engine` for the DAG
scheduler, plus per-backend adapter crates for dispatch).

## Scope

- `WorkflowContext` — per-run state (node results, trace id, timings).
- `EdgeLogic` — typed edge metadata + Rhai condition/mapping expressions.
- `RetryPolicy` — retry count, backoff, optional Rhai expressions.
- `SystemNodeKind` — built-in node taxonomy (ForEach, Judge, Ensemble,
  sub-workflows, etc.) that the executor dispatches specially.
- `JoinMode` — fan-in aggregation (All / Any / Majority / N).

## Non-goals

- No scheduling. No NATS. No Postgres. No secrets. No LLM. Types only.
- Expression evaluation (Rhai) lives with the executor, not here.

## Status

Pre-1.0. API may still move as the executor is extracted out of the
Talos controller and the trait boundary is finalized.

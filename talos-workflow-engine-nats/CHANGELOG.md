# Changelog

All notable changes to `talos-workflow-engine-nats` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Pre-1.0: breaking changes may occur in any minor version. Once the public
API stabilizes alongside `talos-workflow-engine-core`, the crate will move to
1.0 and normal semver applies.

## [Unreleased]

## [0.1.0] — Initial release

- `NatsNodeDispatcher` — `NodeDispatcher` impl that publishes signed jobs
  via NATS request/reply and parses worker responses.
- `NatsTransport` — `JobTransport` impl wrapping an `async_nats::Client`.
- `run_with_nats`, `run_with_seed_via_nats` — convenience runners that
  wire a `ParallelWorkflowEngine` to a NATS-backed dispatcher.
- Topic-level priority lanes (jobs with priority ≥ 200 route to a
  `.priority` sub-topic).
- Optional edge routing (`ENABLE_EDGE_ROUTING=true`) that scopes subjects
  by user id for per-tenant worker subscriptions.
- Retry with exponential backoff on transient NATS delivery errors;
  timeouts are not retried.

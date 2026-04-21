# Releasing the talos-workflow-engine family

The five crates release **as a single unit** at one synchronized
version. A tagged release publishes:

```
talos-workflow-engine-core
talos-workflow-job-protocol
talos-workflow-engine
talos-workflow-engine-test-utils
talos-workflow-engine-nats
```

The dependency graph fans out from `-core`; everything else either
extends a trait declared there or builds on top. Sibling crates pin
each other by the workspace version (`{ path = "...", version = "0.1" }`)
so a single mismatch fails to publish — the version in every
`Cargo.toml` MUST match before a release goes out.

## Pre-release checklist

Before any `cargo publish`:

1. **Workspace clean**
   - `cargo fmt --all -- --check`
   - `cargo clippy --workspace --all-targets -- -D warnings`
   - `cargo doc --workspace --no-deps` (zero warnings)
   - `cargo test --workspace`
   - `cargo deny check`
2. **MSRV verification**
   - `cargo test --workspace` against the version pinned in
     `rust-toolchain.toml`. The `ci.yml` matrix already covers this;
     verify the matrix run is green on the release SHA.
3. **Bench regression check** (perf-touching releases only)
   - `./scripts/bench-check.sh` against the prior release baseline.
     Document any intentional regressions in the changelog.
4. **CHANGELOG cross-check.** Every PR since the last release must
   appear under `## [Unreleased]` in the relevant crate's
   `CHANGELOG.md`. Move that section to a dated header (e.g.
   `## [0.2.0] — 2026-05-15`) at release time.
5. **Public API audit.** Run `cargo public-api diff` (install if
   needed) against the previous release tag for `-core` and
   `-engine`. Any breaking change MUST be either:
   - documented as a "Breaking" bullet in the changelog,
   - covered by a typed `WorkflowEngineError` variant migration if
     the change touches error semantics, or
   - **rejected** if the release is a patch / minor that doesn't
     intend a break.

## Version-bump conventions (pre-1.0)

Per the README: pre-1.0 means breaking changes can land in any minor
version. We're not playing fast and loose with that — the rules
below mirror what a 1.x crate would do, just at the minor level
instead of the major:

| Change | Bump |
|---|---|
| New typed error variant (added to `#[non_exhaustive]` enum) | patch |
| New optional method on a trait with a default body | patch |
| New `#[must_use]` setter on `ParallelWorkflowEngine` | patch |
| New module / new public item / additive feature | minor |
| Trait method signature change | minor |
| Public field visibility change (e.g. `pub` → `pub(crate)`) | minor |
| Wire-format change (signing payload, JSON shape) | minor + coordinated worker upgrade required |

A patch-only release SHOULD NOT change `talos-workflow-job-protocol`
unless the change is purely additive (new field with `#[serde(default)]`)
and verified against the wire-format snapshot tests. Wire-format
changes break deployed workers — these go in a minor release with a
coordinated controller-then-worker rollout window.

## What would force 2.0

Once the family graduates to 1.0, these are the changes we'd hold
back for a 2.0:

* Removing or renaming any trait in `talos-workflow-engine-core`.
* Removing a `SystemNodeKind` variant.
* Changing the canonical signing-payload format in
  `talos-workflow-job-protocol::JobRequest::signing_payload` (or any
  sibling). New fields appended at the end remain wire-compatible
  during a coordinated rollout — reordering or removing forces a 2.0.
* Removing a `WorkflowEngineError` variant (vs. adding new ones,
  which are non-breaking under `#[non_exhaustive]`).
* Changing the engine's reactor semantics in a way that alters
  observable workflow behavior (e.g. changing how `FanIn` joins,
  or what counts as a successful `Wait` resume).

Adding a new variant to any `#[non_exhaustive]` enum is **not** a
2.0 trigger.

## Publish order

`cargo publish` walks the dependency graph from leaves up:

```
1. talos-workflow-engine-core
2. talos-workflow-job-protocol
3. talos-workflow-engine
4. talos-workflow-engine-test-utils
5. talos-workflow-engine-nats
```

Crates.io takes a few seconds to index each one. Wait for each
to be visible (`cargo search <name>` returns the new version)
before publishing the next — a downstream `cargo publish` with an
unindexed dependency will fail.

## Tagging

```bash
git tag -a v0.2.0 -m "v0.2.0"
git push origin v0.2.0
```

The tag triggers no automation today; it's a marker for the
changelog and for `cargo public-api diff` baselines. A future
release-automation workflow could key off the tag to publish.

## Yanked releases

If a release lands and a critical bug surfaces in the first 24
hours:

1. `cargo yank --vers <bad>` for every crate in the family.
2. Open an issue documenting the yank reason and the planned fix.
3. Cut a patch release with the fix.

Don't yank just for "we found a small bug" — that's what patch
releases are for. Yank only when the release is actively dangerous
(wire-format break, security regression, etc.).

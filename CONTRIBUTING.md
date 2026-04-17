# Contributing to talos-workflow-engine

Thanks for considering a contribution. This guide covers the mechanical
bits; each crate's `AGENTS.md` captures the non-obvious per-crate rules
(what belongs where, security invariants, wire-format discipline).

## Before you start

- **Read the crate's `AGENTS.md`.** Every crate has one. They describe
  what the crate is for, what shouldn't leak in, and any tricky
  correctness rules that reviewers will check against.
- **File an issue for non-trivial changes.** New trait methods, wire-
  format changes, or new crates in the family deserve a short
  discussion before you spend time on a PR.
- **Bug fixes and doc improvements** don't need pre-approval — just
  send a PR.

## Workflow

1. Fork + branch from `main`.
2. Make your change. Keep commits focused; squash noise locally.
3. Run the per-crate check gauntlet (each crate's `AGENTS.md` has a
   "Post-change checks" section); at minimum:

   ```bash
   cargo fmt --all
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   cargo doc --workspace --no-deps
   ```

4. Write a PR description that explains the **why**, not just the
   what. Reviewers can read the diff; they can't read your mind.
5. CI must pass before review. MSRV (`1.80`), stable, clippy, fmt,
   doc builds are all enforced.

## What gets merged

- Changes that fix real bugs with a regression test.
- Trait-surface additions that come with a sibling impl in
  `talos-workflow-engine-test-utils` in the same PR.
- Wire-format additions that are backwards-compatible (new optional
  fields with `#[serde(default)]` appended to the canonical signing
  order). Breaking wire changes need major-version bumps and a
  migration note.
- Docs, typos, examples, CI polish.

## What doesn't get merged (default)

- New runtime dependencies in `talos-workflow-engine-core` beyond the four
  allowlisted (`async-trait`, `serde`, `serde_json`, `uuid`).
- New I/O deps anywhere in `-core` (no `sqlx`, `reqwest`, filesystem
  crates).
- Changes that break dyn-compatibility of a trait without a very
  compelling reason.
- Removing or re-ordering entries from the reserved vault-path
  registry in `talos-workflow-job-protocol` — security invariant.
- Behavior changes bundled with a cosmetic refactor. Keep them
  separate PRs.

## Commit messages

- Imperative mood: "add", "fix", "remove" — not "adds", "added".
- Reference the affected crate in the subject when the change is
  scoped: `feat(-core): …`, `fix(-nats): …`.
- Explain the **why** in the body when it's non-obvious. A PR title
  and one-line body are fine for small changes.

## Code of conduct

This project adopts the [Contributor Covenant](./CODE_OF_CONDUCT.md).
By participating, you agree to uphold it.

## Licensing

Contributions are dual-licensed MIT OR Apache-2.0. By submitting a PR
you certify that:

- You wrote it (or have the right to contribute it).
- You agree to the project's license terms.
- You're not under a contract that assigns the code to someone else.

No CLA. Just the standard Apache-2.0 / MIT terms apply.

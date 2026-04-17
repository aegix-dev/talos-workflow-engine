# Extraction guide — Talos monorepo → aegix-dev/talos-workflow-engine

This doc is a one-time runbook for carving these five crates out of
the Talos monorepo into the standalone repo at
<https://github.com/aegix-dev/talos-workflow-engine>. **Delete this file
after the extraction is committed on the new repo.**

Everything else in this directory is already staged to be the new
repo's root: README.md, LICENSE-*, CONTRIBUTING.md, SECURITY.md,
CODE_OF_CONDUCT.md, .gitignore, .github/workflows/ci.yml, deny.toml,
and the five crates themselves.

## Step 0 — prerequisites

Install `git-filter-repo` (one-time):

```bash
# macOS
brew install git-filter-repo
# or
pip install --user git-filter-repo
```

## Step 1 — create the empty GitHub repo

Create <https://github.com/aegix-dev/talos-workflow-engine> on GitHub.
**Do not** initialize it with a README, .gitignore, or license —
we're pushing a fresh history.

## Step 2 — extract history with git-filter-repo

From a **fresh clone** of the Talos monorepo (filter-repo rewrites
history destructively; never run it on your primary working copy):

```bash
git clone <talos-remote-url> /tmp/talos-workflow-engine-extract
cd /tmp/talos-workflow-engine-extract

# Keep only the crates/ subdirectory and promote it to repo root.
# All commits touching only files outside crates/ are dropped.
git filter-repo --subdirectory-filter crates

# At this point the repo's root IS what used to be crates/ —
# README.md, LICENSE-*, the five crate dirs, .github/, etc.
```

## Step 3 — add the workspace Cargo.toml

The monorepo's root Cargo.toml was excluded by the subdirectory
filter. Create a new workspace root:

```bash
cat > Cargo.toml <<'EOF'
[workspace]
resolver = "2"
members = [
    "talos-workflow-engine-core",
    "talos-workflow-engine",
    "talos-workflow-engine-nats",
    "talos-workflow-engine-test-utils",
    "talos-workflow-job-protocol",
]

[profile.release]
opt-level = 3
lto = "thin"
codegen-units = 1
strip = "symbols"
EOF

git add Cargo.toml
git commit -m "chore: add workspace Cargo.toml"
```

## Step 4 — delete EXTRACTION.md

```bash
git rm EXTRACTION.md
git commit -m "chore: remove extraction runbook"
```

## Step 5 — verify clean build

```bash
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo doc --workspace --no-deps
```

All must pass. If anything fails it's a bug in the pre-extraction
prep — file it before pushing.

## Step 6 — push

```bash
git remote set-url origin git@github.com:aegix-dev/talos-workflow-engine.git
git push -u origin main
```

## Step 7 — cleanup in the Talos monorepo

Back in the original Talos monorepo (your normal working copy),
remove the crates from the workspace and switch consumers to the
published versions:

1. Delete the `crates/` directory: `rm -rf crates/`.
2. Remove each extracted crate from the root `Cargo.toml` `members`
   list.
3. In `controller/Cargo.toml` and `worker/Cargo.toml`, change the
   `talos-workflow-job-protocol = { path = "..." }` entries to git deps
   pinned on the commit you just pushed:

   ```toml
   talos-workflow-job-protocol = { git = "https://github.com/aegix-dev/talos-workflow-engine", rev = "<SHA>", version = "0.1" }
   ```

   (Once `talos-workflow-job-protocol 0.1.0` is published to crates.io, the
   `git` + `rev` fields can be dropped, leaving just `version = "0.1"`.)

4. `cargo check --workspace` — Talos should still build.

## Step 8 — publish to crates.io (when ready)

Publish in dependency order, with a ~5 min pause between each for
the crates.io index to catch up. From the extracted repo:

```bash
cargo publish -p talos-workflow-engine-core
sleep 300
cargo publish -p talos-workflow-job-protocol
sleep 300
cargo publish -p talos-workflow-engine-test-utils
sleep 300
cargo publish -p talos-workflow-engine
sleep 300
cargo publish -p talos-workflow-engine-nats
```

Before each publish, `cargo publish --dry-run -p <crate>` surfaces
packaging issues without actually pushing. Check crates.io for name
availability first: all five names must be available to reserve them
with the first publish.

## Step 9 — tag the release

```bash
git tag v0.1.0
git push origin v0.1.0
```

## Troubleshooting

- **`filter-repo: error: unknown option: --subdirectory-filter`** —
  You have a very old git-filter-repo. Update with `pip install -U
  git-filter-repo`.
- **`error: failed to select a version for the requirement …`** — A
  path+version dep pointed at a version that doesn't exist on
  crates.io yet. Either publish the dep first or temporarily remove
  the `version =` field (keep only `path =`) until the first publish
  round is complete, then add it back in a follow-up commit.
- **`cargo doc` warnings become errors in CI** — The `RUSTDOCFLAGS`
  env in `.github/workflows/ci.yml` sets `-D warnings`. Fix the doc
  link or drop the flag if the warning is intractable.

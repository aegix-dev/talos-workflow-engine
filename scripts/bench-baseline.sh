#!/usr/bin/env bash
# Capture a fresh baseline of the scheduler benches that
# scripts/bench-check.sh will compare against. Run this:
#
#   - Once per branch when starting a perf-sensitive change.
#   - On `main` after merging an intentional perf change.
#   - In CI on the merge queue once the change is accepted.
#
# The baseline name defaults to "main" (matches what bench-check.sh
# reads). Override with BENCH_BASELINE=feature-x.

set -euo pipefail

BASELINE="${BENCH_BASELINE:-main}"

cargo bench --workspace --bench scheduler -- --save-baseline "$BASELINE"
echo ""
echo "saved baseline '${BASELINE}' under target/criterion/"
echo "run scripts/bench-check.sh to verify a future change stays within noise"

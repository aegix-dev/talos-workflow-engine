#!/usr/bin/env bash
# Run the scheduler benches and fail on regressions vs. the saved
# baseline. Wired for CI; see ../docs/benchmarking.md for the full
# workflow.
#
# Exit codes:
#   0   benches ran cleanly and stayed within the noise threshold
#   1   one or more benches regressed past the threshold
#   2   no baseline saved yet — run scripts/bench-baseline.sh first
#
# Override the comparison baseline with BENCH_BASELINE=name (default
# "main"). Override the noise floor with BENCH_NOISE=0.10 (10%).

set -euo pipefail

BASELINE="${BENCH_BASELINE:-main}"
NOISE="${BENCH_NOISE:-0.10}"

# Criterion stores baselines under target/criterion/<bench>/<baseline>/.
# A missing top-level dir is the "never saved" case.
if [[ ! -d "target/criterion" ]]; then
    echo "no criterion baselines found under target/criterion/" >&2
    echo "run scripts/bench-baseline.sh first" >&2
    exit 2
fi

# `cargo bench --baseline NAME` exits 0 even on regression — Criterion
# only flags regressions in its summary, not via exit code. Capture
# the output and grep for the canonical "Performance has regressed"
# line. Faster than parsing the JSON output, and resilient to
# Criterion changing the JSON schema between releases.
OUT="$(cargo bench --workspace --bench scheduler -- \
    --baseline "$BASELINE" \
    --noise-threshold "$NOISE" \
    2>&1 | tee /dev/stderr)"

if echo "$OUT" | grep -q "Performance has regressed"; then
    echo "" >&2
    echo "REGRESSION: at least one benchmark exceeded the ${NOISE} noise threshold" >&2
    echo "  - investigate the line(s) above marked 'Performance has regressed'" >&2
    echo "  - if the change is intentional, re-baseline via scripts/bench-baseline.sh" >&2
    exit 1
fi
echo "OK: all benchmarks within ${NOISE} noise threshold of baseline '${BASELINE}'"

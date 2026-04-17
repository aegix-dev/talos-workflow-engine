# Security Policy

## Supported versions

Pre-1.0, only the latest `0.x` release receives security fixes. Once
the project reaches 1.0, this policy will be revised to cover the most
recent major version plus a defined window of prior majors.

## Reporting a vulnerability

**Please do not open public GitHub issues for security vulnerabilities.**

Email the maintainers at `security@aegix.dev` with:

- A description of the issue and its impact.
- Reproduction steps or a minimal proof of concept.
- The affected crate(s) and version(s).
- Any proposed mitigation, if you have one.

You should receive an acknowledgement within **72 hours**. If not,
please resend — mail filters occasionally eat legitimate reports.

## Disclosure timeline

- **Day 0**: report received.
- **Day 0–2**: initial triage and severity classification.
- **Day 2–30**: investigation, fix development, coordinated
  pre-disclosure to known downstream consumers where appropriate.
- **Release**: patched version published to crates.io.
- **Disclosure**: advisory published (CVE where applicable) no earlier
  than 7 days after the patched release, or immediately if the issue
  is already public.

If a fix takes longer than 90 days, we'll keep you updated on progress
and agree a disclosure date together.

## Scope

In-scope:

- Authentication / signing bypass in `talos-workflow-job-protocol`.
- Wire-format replay or tampering vulnerabilities.
- Secret leakage through logs, error messages, debug output, or
  sanitizer bypass.
- Sandbox escape / privilege escalation in execution paths this
  workspace controls.
- Dependency vulnerabilities that our code exposes.

Out-of-scope (report upstream):

- Vulnerabilities in third-party dependencies where our usage is
  correct and the fix belongs in the dependency (e.g. `async-nats`,
  `aes-gcm`, `rhai`). We'll still help coordinate if you want.
- Denial-of-service through legitimate features (rate-limit exhaustion
  via a flood of valid signed jobs, for example).

## What we won't do

- We won't threaten legal action against good-faith researchers.
- We won't require a CVE reservation before accepting a report.
- We won't ask you to sign an NDA as a condition of responding.

Thank you for helping keep the project secure.

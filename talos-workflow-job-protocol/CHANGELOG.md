# Changelog

All notable changes to `talos-workflow-job-protocol` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Pre-1.0: wire-format breaking changes may occur in any minor version.
Once stable, new optional fields are non-breaking; changed semantics
or new required fields bump the major version.

## [Unreleased]

## [0.1.0] — Initial release

- `JobRequest` / `JobResult` — single-node dispatch wire format with
  HMAC-SHA256 signing over the canonical byte form.
- `PipelineJobRequest` / `PipelineJobResult` / `PipelineStep` —
  batched chain dispatch.
- `EncryptedSecrets` — AES-256-GCM envelope for plaintext secret
  transport (fresh key + nonce per dispatch).
- `job_nonce` anti-replay token (ms timestamp + 16 random hex chars)
  with a ±5 s future-skew tolerance.
- Reserved vault-path registry (`LLM_PROVIDER_VAULT_PATHS`,
  `is_llm_provider_vault_path`, `vault_path_permitted`) — canonical
  list of host-reserved secret paths used for deny-listing and
  pre-injection.
- Integration-scoping field (`integration_name`) for gating
  integration-specific host functions.
- `capability_world` hint for worker-linker selection (not signed; a
  performance hint, not a capability grant).

//! Shared job protocol between Controller and Workers.
//!
//! Security model:
//! - Secrets are AES-256-GCM encrypted before transmission over NATS.
//! - Every JobRequest is HMAC-SHA256 signed using a pre-shared key
//!   (WORKER_SHARED_KEY) to prevent injection of malicious jobs.
//! - A `job_nonce` (timestamp + random hex) is included to prevent replay attacks.

use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use hmac::{Hmac, Mac};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

/// Maximum future-skew tolerance for nonce timestamps (seconds).
///
/// Controller and worker sit on the same NATS cluster and should be
/// within a few seconds via NTP. A larger tolerance would extend the
/// effective replay window (a future-dated signature stays valid for
/// `FUTURE_SKEW + max_age_secs` total). A 5 s ≈ 5000 ms asymmetric
/// window is a common choice for signed-NATS RPC.
const MAX_FUTURE_SKEW_SECS: u64 = 5;

fn default_priority() -> u8 {
    100
}

// ============================================================================
// Reserved host vault paths — LLM provider API keys
// ============================================================================
//
// These paths name the canonical LLM provider API keys that the controller
// pre-fetches into every worker job's secrets map so the host-side `llm::*`
// functions can resolve them. THE LIST HAS SECURITY IMPLICATIONS:
//
// - Controller-side (`engine::parallel::prefetch_llm_vault_keys` +
//   `secrets::SecretsManager::get_llm_vault_keys`): every job gets a snapshot
//   of these paths injected so LLM host calls can find them without the
//   module declaring them in `allowed_secrets`.
// - Worker-side (`host_impl::check_secret_allowlist`): guest-reachable
//   secret resolution MUST deny these paths even when a module has
//   `allowed_secrets: ["*"]` — otherwise a wildcard-grant module could
//   exfiltrate the user's LLM API keys via `secrets::get_secret` or a
//   `vault://anthropic/api_key` header interpolation.
//
// The list lives here, not per-crate, so adding a provider happens in one
// place and the controller prefetch + cache-invalidation + worker deny-list
// stay in lockstep. If you're adding a new provider (say, Mistral), update
// this constant and the corresponding branch in `worker::host_impl::llm_key_lookup_paths`.
//
// Rules for the list:
// - Entries are literal, case-sensitive vault paths.
// - The worker does a case-sensitive exact match; casing/prefix games can't
//   bypass the deny-list.
// - Add only paths that are genuinely host-only. User-facing secrets
//   (OAuth tokens, per-integration keys) do NOT belong here.
pub const LLM_PROVIDER_VAULT_PATHS: &[&str] =
    &["anthropic/api_key", "openai/api_key", "gemini/api_key"];

/// True iff `path` is one of the canonical LLM provider vault paths that
/// are reserved for host-internal consumption. Consumers use this as:
/// - worker: deny `secrets::get_secret` from returning these to WASM
/// - controller: trigger cache invalidation when the key is rotated
pub fn is_llm_provider_vault_path(path: &str) -> bool {
    LLM_PROVIDER_VAULT_PATHS.contains(&path)
}

/// True iff `path` is consumed by a controller-internal subsystem (LLM
/// client cache, OAuth refresh loop) rather than by any WASM module's
/// `allowed_secrets` grant. Used by the orphaned-secrets hygiene check
/// to suppress false positives — these paths are by-design absent from
/// every module's grant list.
///
/// Recognized patterns:
/// - LLM provider keys: every entry of [`LLM_PROVIDER_VAULT_PATHS`]
/// - OAuth refresh tokens:
///   `oauth/<provider>/<user_id>/<provider_key>/refresh_token`.
///   Access tokens are NOT considered host-internal because workflow
///   modules legitimately read them via `vault://` in node config.
///
/// Hygiene checks must use this rather than `is_llm_provider_vault_path`
/// alone — flagging an OAuth refresh_token as orphan would suggest an
/// operator delete it, silently breaking the next refresh cycle.
pub fn is_controller_internal_vault_path(path: &str) -> bool {
    if is_llm_provider_vault_path(path) {
        return true;
    }
    // Defensive: refuse to match shapes like "oauth/refresh_token" that
    // lack the {provider}/{user}/{key} segments — those wouldn't be
    // produced by the canonical refresh_token_path() builder, so they'd
    // be a genuine orphan worth surfacing.
    if let Some(rest) = path.strip_prefix("oauth/") {
        if let Some(prefix) = rest.strip_suffix("/refresh_token") {
            // Require at least three intermediate segments (provider /
            // user_id / provider_key) before the refresh_token suffix.
            if prefix.split('/').filter(|s| !s.is_empty()).count() >= 3 {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod llm_provider_path_tests {
    use super::{
        is_controller_internal_vault_path, is_llm_provider_vault_path, LLM_PROVIDER_VAULT_PATHS,
    };

    #[test]
    fn canonical_paths_are_recognised() {
        for p in LLM_PROVIDER_VAULT_PATHS {
            assert!(
                is_llm_provider_vault_path(p),
                "canonical path {} not recognised",
                p
            );
        }
    }

    #[test]
    fn non_llm_paths_are_not_recognised() {
        assert!(!is_llm_provider_vault_path(""));
        assert!(!is_llm_provider_vault_path("github/pat"));
        assert!(!is_llm_provider_vault_path("oauth/gmail/access_token"));
    }

    #[test]
    fn casing_and_nesting_do_not_bypass() {
        // Case-sensitive exact match only — attackers can't wrap the path
        // in a subpath or alter casing to bypass.
        assert!(!is_llm_provider_vault_path("ANTHROPIC/API_KEY"));
        assert!(!is_llm_provider_vault_path("anthropic/api_key/child"));
        assert!(!is_llm_provider_vault_path("prefix/anthropic/api_key"));
    }

    #[test]
    fn controller_internal_recognises_llm_keys() {
        for p in LLM_PROVIDER_VAULT_PATHS {
            assert!(is_controller_internal_vault_path(p));
        }
    }

    #[test]
    fn controller_internal_recognises_oauth_refresh_tokens() {
        // Canonical shape from oauth/credentials.rs::refresh_token_path:
        // oauth/{provider}/{user_id}/{provider_key}/refresh_token
        assert!(is_controller_internal_vault_path(
            "oauth/google_calendar/1a361562-e551-41aa-9cb4-6f8988b035f7/primary/refresh_token"
        ));
        assert!(is_controller_internal_vault_path(
            "oauth/atlassian/abc123/site/refresh_token"
        ));
    }

    #[test]
    fn controller_internal_rejects_oauth_access_tokens() {
        // Access tokens are consumed by sandbox modules via vault:// in node
        // config (e.g. pa-meeting-fetch). Including them would suppress
        // legitimate orphan warnings.
        assert!(!is_controller_internal_vault_path(
            "oauth/google_calendar/1a361562-e551-41aa-9cb4-6f8988b035f7/primary/access_token"
        ));
    }

    #[test]
    fn controller_internal_rejects_malformed_oauth_paths() {
        // Missing intermediate segments — these wouldn't be produced by the
        // canonical builder, so they're genuine orphans worth surfacing.
        assert!(!is_controller_internal_vault_path("oauth/refresh_token"));
        assert!(!is_controller_internal_vault_path(
            "oauth/provider/refresh_token"
        ));
        assert!(!is_controller_internal_vault_path(
            "oauth/provider/user/refresh_token"
        ));
        // Wrong prefix.
        assert!(!is_controller_internal_vault_path(
            "auth/google/user/key/refresh_token"
        ));
        // Misleading suffix.
        assert!(!is_controller_internal_vault_path(
            "oauth/google/user/key/refresh_token_backup"
        ));
    }

    #[test]
    fn controller_internal_rejects_unrelated_paths() {
        assert!(!is_controller_internal_vault_path(""));
        assert!(!is_controller_internal_vault_path("github/pat"));
        assert!(!is_controller_internal_vault_path("custom/secret"));
    }
}

// ============================================================================
// Vault path allowlist matcher — shared between controller and worker
// ============================================================================

/// Returns true if `key_path` is permitted by this module's `allowed_secrets` grant.
///
/// This is the single source of truth for vault path matching semantics. Both
/// the controller (static validation, hygiene reports, engine dispatch) and
/// the worker (runtime enforcement in `secrets::get_secret()`) call this
/// function so they agree on exactly which paths a module can access.
///
/// Semantics:
///   - `[]` (empty)  → deny all (no secret is permitted)
///   - `["*"]`       → allow any key (wildcard)
///   - `["prefix"]`  → allow exactly `"prefix"` and any `"prefix/<child>"` subpath
///   - `["pfx/*"]`   → explicit glob form, equivalent to the plain prefix form above
///
/// The separator must be `/` — `["stripe"]` grants `"stripe"` and `"stripe/key"`
/// but NOT `"stripe-live/key"` (different separator).
pub fn vault_path_permitted(allowed: &[String], key_path: &str) -> bool {
    if allowed.is_empty() {
        return false;
    }
    allowed.iter().any(|s| {
        s == "*"
            || s.as_str() == key_path
            || key_path.starts_with(&format!("{}/", s))
            || (s.ends_with("/*") && key_path.starts_with(&s[..s.len() - 1]))
    })
}

#[cfg(test)]
mod vault_matcher_tests {
    use super::vault_path_permitted;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn empty_list_denies_everything() {
        assert!(!vault_path_permitted(&[], "anthropic/api_key"));
        assert!(!vault_path_permitted(&[], ""));
    }

    #[test]
    fn wildcard_allows_anything() {
        assert!(vault_path_permitted(&s(&["*"]), "anthropic/api_key"));
        assert!(vault_path_permitted(&s(&["*"]), "oauth/gmail/user/access"));
    }

    #[test]
    fn exact_match_allowed() {
        assert!(vault_path_permitted(
            &s(&["anthropic/api_key"]),
            "anthropic/api_key"
        ));
    }

    #[test]
    fn prefix_match_allowed() {
        assert!(vault_path_permitted(
            &s(&["oauth/gmail"]),
            "oauth/gmail/user/access"
        ));
        assert!(vault_path_permitted(&s(&["oauth/gmail"]), "oauth/gmail"));
    }

    #[test]
    fn glob_suffix_allowed() {
        assert!(vault_path_permitted(
            &s(&["oauth/gmail/*"]),
            "oauth/gmail/user/access"
        ));
    }

    #[test]
    fn different_separator_denied() {
        // `stripe` should NOT match `stripe-live/key` — separator must be `/`
        assert!(!vault_path_permitted(&s(&["stripe"]), "stripe-live/key"));
    }

    #[test]
    fn partial_prefix_denied() {
        assert!(!vault_path_permitted(
            &s(&["oauth/gmail"]),
            "oauth/atlassian/token"
        ));
    }
}

// ============================================================================
// Encrypted secrets transport
// ============================================================================

/// Encrypted secret store for transit over untrusted channels (e.g. NATS).
///
/// The plaintext is JSON-serialized `HashMap<String, String>` encrypted
/// with AES-256-GCM using the pre-shared `WORKER_SHARED_KEY`.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct EncryptedSecrets {
    /// AES-256-GCM ciphertext.
    pub ciphertext: Vec<u8>,
    /// 12-byte random nonce (unique per encryption).
    pub nonce: Vec<u8>,
}

/// Reference [`SecretEnvelope`] impl backing the workspace's default
/// dispatch path. Seals the plaintext secrets map with AES-256-GCM,
/// using a caller-supplied 32-byte key as the AEAD key and a fresh
/// random 12-byte nonce per call. The AEAD tag authenticates the
/// ciphertext in-place, so callers do not need to add an outer MAC.
///
/// Construct as `AesGcmSecretEnvelope` (unit struct — no state). The
/// engine holds an `Arc<dyn SecretEnvelope>` and calls
/// [`SecretEnvelope::seal`] once per dispatch.
///
/// # Security properties
///
/// * Fresh 96-bit nonce per call (`rand::thread_rng`).
/// * Authenticated (AES-GCM's GMAC covers the ciphertext).
/// * Key length is validated — a non-32-byte key returns an error
///   rather than silently truncating.
///
/// [`SecretEnvelope`]: talos_workflow_engine_core::SecretEnvelope
/// [`SecretEnvelope::seal`]: talos_workflow_engine_core::SecretEnvelope::seal
#[derive(Debug, Clone, Copy, Default)]
pub struct AesGcmSecretEnvelope;

#[async_trait::async_trait]
impl talos_workflow_engine_core::SecretEnvelope for AesGcmSecretEnvelope {
    async fn seal(
        &self,
        secrets: &HashMap<String, String>,
        shared_key: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), talos_workflow_engine_core::BoxError> {
        // Empty map is a valid input — return the sentinel (empty
        // ciphertext + empty nonce) so the engine can short-circuit
        // without running AES on nothing.
        if secrets.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let enc = EncryptedSecrets::encrypt(secrets, shared_key)
            .map_err(|e| -> talos_workflow_engine_core::BoxError { e.into() })?;
        Ok((enc.ciphertext, enc.nonce))
    }
}

impl EncryptedSecrets {
    /// Encrypt a secrets map using AES-256-GCM.
    ///
    /// `key` must be exactly 32 bytes (256 bits).
    pub fn encrypt(secrets: &HashMap<String, String>, key: &[u8]) -> Result<Self, String> {
        if key.len() != 32 {
            return Err(format!(
                "WORKER_SHARED_KEY must be 32 bytes, got {}",
                key.len()
            ));
        }

        let plaintext =
            serde_json::to_vec(secrets).map_err(|e| format!("serialize secrets: {e}"))?;

        let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| format!("create cipher: {e}"))?;

        let nonce_bytes: [u8; 12] = rand::thread_rng().gen();
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = cipher
            .encrypt(nonce, plaintext.as_ref())
            .map_err(|e| format!("encrypt secrets: {e}"))?;

        Ok(Self {
            ciphertext,
            nonce: nonce_bytes.to_vec(),
        })
    }

    /// Decrypt back into a secrets map.
    ///
    /// `key` must be the same 32-byte key used for encryption.
    pub fn decrypt(&self, key: &[u8]) -> Result<HashMap<String, String>, String> {
        if key.len() != 32 {
            return Err(format!(
                "WORKER_SHARED_KEY must be 32 bytes, got {}",
                key.len()
            ));
        }
        if self.nonce.len() != 12 {
            return Err("invalid nonce length".to_string());
        }

        let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| format!("create cipher: {e}"))?;

        let nonce = Nonce::from_slice(&self.nonce);

        let plaintext = cipher
            .decrypt(nonce, self.ciphertext.as_ref())
            .map_err(|_| "decryption failed — wrong key or tampered ciphertext".to_string())?;

        serde_json::from_slice(&plaintext).map_err(|e| format!("deserialize secrets: {e}"))
    }

    /// Returns `true` if no secrets are stored.
    pub fn is_empty(&self) -> bool {
        self.ciphertext.is_empty()
    }
}

// ============================================================================
// Job request / result
// ============================================================================

/// A job dispatched by the Controller to a Worker via NATS.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobRequest {
    pub job_id: Uuid,
    pub workflow_execution_id: Uuid,
    pub module_uri: String,
    pub input_payload: serde_json::Value,

    /// AES-256-GCM encrypted `HashMap<String, String>` of secret values.
    /// Encrypted with the pre-shared `WORKER_SHARED_KEY`.
    /// Never log or expose directly.
    #[serde(default)]
    pub encrypted_secrets: EncryptedSecrets,

    pub timeout_ms: u64,

    /// Job priority (0 = lowest, 255 = highest). Default: 100.
    /// Higher-priority jobs are dequeued before lower-priority ones.
    #[serde(default = "default_priority")]
    pub priority: u8,

    /// Absolute deadline as Unix timestamp (seconds). If set, the job MUST
    /// complete before this time or be treated as failed.  0 = no deadline.
    #[serde(default)]
    pub deadline_unix_secs: u64,

    /// Opaque cancellation token.  If set, the worker checks this token
    /// periodically and aborts execution if the token is revoked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation_token: Option<String>,

    pub allowed_hosts: Vec<String>,
    /// HTTP method allowlist. Empty = allow all methods. Non-empty = restrict to listed methods.
    #[serde(default)]
    pub allowed_methods: Vec<String>,
    /// Secret allowlist. Empty = deny all. `["*"]` = allow all. Otherwise explicit secret names.
    #[serde(default)]
    pub allowed_secrets: Vec<String>,
    /// SQL operation allowlist. Empty = allow all. Otherwise explicit types (SELECT, INSERT, etc.).
    #[serde(default)]
    pub allowed_sql_operations: Vec<String>,
    /// When true, the module may call `expose_secret` (Tier-2) to receive
    /// raw secret plaintext in WASM guest memory. Default: false (blocked).
    #[serde(default)]
    pub allow_tier2_exposure: bool,

    /// HMAC-SHA256 over the canonical job fields (see [`JobRequest::sign`]).
    pub signature: Vec<u8>,

    /// Nonce used for replay-attack prevention: `"{unix_secs}:{random_hex}"`.
    pub job_nonce: String,

    /// Actor ID that owns this execution. When set, the worker routes
    /// WIT agent-memory get/set/search calls to the persistent actor_memory
    /// Postgres table instead of the ephemeral in-memory HashMap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_id: Option<Uuid>,

    /// Optional WASM module bytes.  When present the worker uses these
    /// directly instead of reading from `module_uri`, avoiding file-system
    /// coupling and improving performance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wasm_bytes: Option<Vec<u8>>,

    /// Capability world hint for the worker's tiered linker selection.
    ///
    /// When present and not "unknown", the worker uses this instead of
    /// re-inspecting the WASM binary.  This is critical for sandbox modules
    /// (stored in `node_templates.precompiled_wasm`) whose world name may
    /// not survive the Wizer snapshot step.
    ///
    /// Accepts both bare names ("minimal") and WIT world names ("minimal-node",
    /// "automation-node").  Not included in the HMAC signing payload — it is a
    /// performance hint, not a capability grant (the linker enforces real
    /// security at instantiation time).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_world: Option<String>,

    /// Integration name this module was compiled under, if any. When set,
    /// the module can call integration-scoped host functions (e.g. an
    /// `integration-state::*` WIT interface) and the worker signs every
    /// downstream RPC request with this value. When None, the host
    /// function returns `unauthorized` — non-integration modules cannot
    /// write to the shared integration-state table.
    ///
    /// Populated by the engine from `wasm_modules.integration_name` /
    /// `node_templates.integration_name`. Guest code has no way to
    /// supply or change this value — the worker reads it from the
    /// request, never from WIT arguments.
    ///
    /// Not part of the HMAC commitment (it's not a capability, just a
    /// scoping identifier); the RPC layer signs it separately.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integration_name: Option<String>,

    /// Expected SHA-256 hex digest of the WASM binary loaded from `module_uri`.
    ///
    /// Set by the controller from `wasm_modules.content_hash` (recorded at
    /// compile/registration time).  When present and `wasm_bytes` is absent
    /// (i.e. the worker will load the binary from the registry or Redis), the
    /// worker MUST verify that `sha256(loaded_bytes) == expected_wasm_hash`
    /// before execution.  A mismatch indicates tampering in the storage layer.
    ///
    /// Included in the HMAC signing payload so the commitment is tamper-evident.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_wasm_hash: Option<String>,

    /// Maximum fuel (WASM instructions) for this job.
    ///
    /// Set by the controller from the node's `max_fuel` config key or the
    /// module's stored `max_fuel` column.  When non-zero the worker SHOULD use
    /// this value instead of its global `WASM_FUEL_LIMIT` default.
    /// Capped at 50_000_000 (50M) by the controller to prevent abuse.
    /// Zero means "use the worker's default fuel limit".
    #[serde(default)]
    pub max_fuel: u64,

    /// User ID that owns this execution — used for ownership-scoped
    /// resources (integration_state writes, per-user rate limiting,
    /// audit trails). Populated by the controller from the workflow
    /// owner's user_id. Nil UUID indicates 'no user context' (system
    /// executions); integration_state host fns reject those.
    ///
    /// Added to JobRequest alongside `actor_id` so host fns that need
    /// user scoping (integration_state::{set,get,...}) don't have to
    /// conflate it with actor_id.
    #[serde(default)]
    pub user_id: Uuid,

    /// When true, non-GET HTTP requests are mocked (returns 200 with dry_run metadata).
    /// GET requests execute normally for data fetching.
    #[serde(default)]
    pub dry_run: bool,
}

impl JobRequest {
    /// Canonical byte string signed / verified by HMAC-SHA256.
    ///
    /// All security-sensitive fields are covered so that an attacker cannot
    /// substitute `input_payload`, secrets, WASM bytes, timeout, or allowed
    /// hosts without invalidating the signature.
    ///
    /// Format:
    /// `job_id:wex_id:module_uri:job_nonce:sha256(input):sha256(secrets_ciphertext):timeout_ms:sorted_hosts:sorted_methods:sha256(wasm_bytes)|expected_wasm_hash|none`
    ///
    /// When `wasm_bytes` is inline, the field is `sha256(wasm_bytes)`.
    /// When `wasm_bytes` is absent but `expected_wasm_hash` is set, the field is that hash
    /// (tamper-evident commitment to the content the worker will load from `module_uri`).
    /// Otherwise the sentinel "none" is used.
    fn signing_payload(&self) -> Vec<u8> {
        use sha2::Digest;

        // Hash large/variable fields to fixed-size hex representations.
        // This prevents payload-substitution attacks where an attacker could
        // replace input_payload, secrets, or wasm_bytes with malicious content.
        let input_hash = hex::encode(Sha256::digest(self.input_payload.to_string().as_bytes()));
        let secrets_hash = hex::encode(Sha256::digest(&self.encrypted_secrets.ciphertext));

        // Sort allowed_hosts so the signature is stable regardless of array order.
        let mut hosts = self.allowed_hosts.clone();
        hosts.sort_unstable();
        let hosts_str = hosts.join(",");

        // Sort allowed_methods for the same reason: order must not matter.
        let mut methods = self.allowed_methods.clone();
        methods.sort_unstable();
        let methods_str = methods.join(",");

        // Wasm integrity commitment:
        // - Inline bytes → sha256(bytes) (already covers the content)
        // - No inline bytes + expected hash → that hash (tamper-evident URI-content binding)
        // - Neither → "none"
        let wasm_hash = if let Some(b) = self.wasm_bytes.as_deref() {
            hex::encode(Sha256::digest(b))
        } else if let Some(ref h) = self.expected_wasm_hash {
            h.clone()
        } else {
            "none".to_string()
        };

        // integration_name is part of the module's identity for
        // integration-state scoping — a NATS-channel tampering attacker
        // could otherwise swap "gcal" → "gmail" in flight and redirect
        // a module's writes into a different integration's namespace
        // without invalidating the signature. The sentinel "-" is used
        // for modules that aren't integrations so an absent value is
        // still tamper-evident (distinct from the empty string).
        //
        // Wire-format stability rule: this field is appended at the END
        // of the format string — adding it here is safe during a
        // coordinated controller+worker restart; reordering the
        // existing positions would break every deployed signature.
        let integration_name = self.integration_name.as_deref().unwrap_or("-");

        format!(
            "{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
            self.job_id,
            self.workflow_execution_id,
            self.module_uri,
            self.job_nonce,
            input_hash,
            secrets_hash,
            self.timeout_ms,
            hosts_str,
            methods_str,
            wasm_hash,
            integration_name,
            // Appended AT THE END per the wire-format stability rule —
            // inserting in the middle would break every deployed
            // signature. user_id bound so an on-wire attacker can't
            // redirect a module's writes to a different user's
            // integration-state namespace.
            self.user_id,
        )
        .into_bytes()
    }

    /// Sign the request using the pre-shared `key`.
    ///
    /// Sets `self.signature` and `self.job_nonce` (timestamp + random hex).
    /// Call this after all other fields have been populated.
    pub fn sign(&mut self, key: &[u8]) -> Result<(), String> {
        // Build nonce: "<unix_seconds>:<16 random hex bytes>"
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| format!("system time error: {e}"))?
            .as_secs();
        let rand_bytes: [u8; 16] = rand::thread_rng().gen();
        self.job_nonce = format!("{}:{}", ts, hex::encode(rand_bytes));

        let mut mac =
            <HmacSha256 as Mac>::new_from_slice(key).map_err(|e| format!("HMAC key error: {e}"))?;
        mac.update(&self.signing_payload());
        self.signature = mac.finalize().into_bytes().to_vec();
        Ok(())
    }

    /// Verify the HMAC signature and nonce freshness.
    ///
    /// Returns `Err` if the signature is invalid or the nonce is older than
    /// `max_age_secs` (default recommendation: 300 s / 5 minutes).
    pub fn verify(&self, key: &[u8], max_age_secs: u64) -> Result<(), String> {
        // 1. Verify nonce freshness to prevent replay attacks.
        let parts: Vec<&str> = self.job_nonce.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err("malformed job_nonce".to_string());
        }
        let ts: u64 = parts[0]
            .parse()
            .map_err(|_| "invalid timestamp in job_nonce".to_string())?;
        if hex::decode(parts[1]).is_err() {
            return Err("invalid hex in job_nonce".to_string());
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if now.saturating_sub(ts) > max_age_secs {
            return Err(format!(
                "job_nonce is too old ({} s, max {})",
                now.saturating_sub(ts),
                max_age_secs
            ));
        }
        if ts.saturating_sub(now) > MAX_FUTURE_SKEW_SECS {
            return Err(format!(
                "job_nonce is in the future ({} s ahead, max {})",
                ts.saturating_sub(now),
                MAX_FUTURE_SKEW_SECS
            ));
        }

        // 2. Constant-time HMAC verification.
        let mut mac =
            <HmacSha256 as Mac>::new_from_slice(key).map_err(|e| format!("HMAC key error: {e}"))?;
        mac.update(&self.signing_payload());
        mac.verify_slice(&self.signature)
            .map_err(|_| "HMAC signature verification failed".to_string())
    }
}

/// Job status reported by a Worker.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum JobStatus {
    Success,
    Failed,
    TimedOut,
}

/// Result returned by a Worker to the Controller via NATS.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobResult {
    pub job_id: Uuid,
    pub status: JobStatus,
    pub output_payload: serde_json::Value,
    pub logs: Vec<String>,
    pub execution_time_ms: u64,
    /// HMAC-SHA256 signature over canonical result fields (see [`JobResult::sign`]).
    /// Allows the controller to verify the result came from a legitimate worker.
    #[serde(default)]
    pub signature: Vec<u8>,
    /// Nonce for replay prevention: `"{unix_secs}:{random_hex}"`.
    #[serde(default)]
    pub result_nonce: String,
}

impl JobResult {
    /// Canonical byte string signed / verified by HMAC-SHA256.
    ///
    /// Format:
    /// `job_id:status:result_nonce:sha256(output_payload):execution_time_ms`
    fn signing_payload(&self) -> Vec<u8> {
        use sha2::Digest;
        let status_str = match self.status {
            JobStatus::Success => "success",
            JobStatus::Failed => "failed",
            JobStatus::TimedOut => "timedout",
        };
        let output_hash = hex::encode(Sha256::digest(self.output_payload.to_string().as_bytes()));
        format!(
            "{}:{}:{}:{}:{}",
            self.job_id, status_str, self.result_nonce, output_hash, self.execution_time_ms,
        )
        .into_bytes()
    }

    /// Sign the result using the pre-shared `key`.
    ///
    /// Sets `self.signature` and `self.result_nonce`.
    /// Call this after all other fields have been populated.
    pub fn sign(&mut self, key: &[u8]) -> Result<(), String> {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| format!("system time error: {e}"))?
            .as_secs();
        let rand_bytes: [u8; 16] = rand::thread_rng().gen();
        self.result_nonce = format!("{}:{}", ts, hex::encode(rand_bytes));

        let mut mac =
            <HmacSha256 as Mac>::new_from_slice(key).map_err(|e| format!("HMAC key error: {e}"))?;
        mac.update(&self.signing_payload());
        self.signature = mac.finalize().into_bytes().to_vec();
        Ok(())
    }

    /// Verify the HMAC signature and nonce freshness.
    ///
    /// Returns `Err` if the signature is invalid or the nonce is older than
    /// `max_age_secs`.
    pub fn verify(&self, key: &[u8], max_age_secs: u64) -> Result<(), String> {
        // 1. Verify nonce freshness to prevent replay attacks.
        let parts: Vec<&str> = self.result_nonce.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err("malformed result_nonce".to_string());
        }
        let ts: u64 = parts[0]
            .parse()
            .map_err(|_| "invalid timestamp in result_nonce".to_string())?;
        if hex::decode(parts[1]).is_err() {
            return Err("invalid hex in result_nonce".to_string());
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if now.saturating_sub(ts) > max_age_secs {
            return Err(format!(
                "result_nonce is too old ({} s, max {})",
                now.saturating_sub(ts),
                max_age_secs
            ));
        }
        if ts.saturating_sub(now) > MAX_FUTURE_SKEW_SECS {
            return Err(format!(
                "result_nonce is in the future ({} s ahead, max {})",
                ts.saturating_sub(now),
                MAX_FUTURE_SKEW_SECS
            ));
        }

        // 2. Constant-time HMAC verification.
        let mut mac =
            <HmacSha256 as Mac>::new_from_slice(key).map_err(|e| format!("HMAC key error: {e}"))?;
        mac.update(&self.signing_payload());
        mac.verify_slice(&self.signature)
            .map_err(|_| "HMAC signature verification failed".to_string())
    }
}

// ============================================================================
// Pipeline job protocol
// ============================================================================

/// A single step in a pipeline job dispatched via NATS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStep {
    pub module_id: Uuid,
    /// URI for the module (e.g. "redis:wasm:uuid" or "file://...")
    pub module_uri: String,
    /// Optional WASM module bytes for this step (overrides module_uri if provided).
    pub wasm_bytes: Option<Vec<u8>>,
    /// Module configuration (merged into input as `{"config": ..., "input": ...}`).
    pub config: serde_json::Value,
    pub allowed_hosts: Vec<String>,
    pub allowed_methods: Vec<String>,
    /// Secret allowlist. Empty = deny all. `["*"]` = allow all.
    #[serde(default)]
    pub allowed_secrets: Vec<String>,
    /// SQL operation allowlist. Empty = allow all.
    #[serde(default)]
    pub allowed_sql_operations: Vec<String>,
    /// When true, expose_secret (Tier-2) is allowed. Default: false.
    #[serde(default)]
    pub allow_tier2_exposure: bool,
    /// AES-256-GCM encrypted secret map for this step.
    pub encrypted_secrets: EncryptedSecrets,
    /// Maximum fuel (WASM instructions) for this step.
    pub max_fuel: u64,
    pub max_memory_mb: usize,
    /// Per-step timeout in milliseconds.
    pub timeout_ms: u64,

    /// Step priority (inherited from JobRequest if not set). Default: 100.
    #[serde(default = "default_priority")]
    pub priority: u8,

    /// Cancellation token for this step. Checked by the worker during execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation_token: Option<String>,

    /// Expected SHA-256 hex digest of the WASM binary at `module_uri`.
    ///
    /// Set by the controller from `wasm_modules.content_hash`.  When present
    /// and `wasm_bytes` is absent, the worker verifies the loaded bytes match
    /// before execution.  Included in the pipeline HMAC signing payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_wasm_hash: Option<String>,

    /// Integration this step's module belongs to. Same semantics as
    /// `JobRequest::integration_name`. Pipeline steps may belong to
    /// different integrations within one pipeline (rare but valid),
    /// so it's per-step rather than at the pipeline level.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integration_name: Option<String>,
}

/// A pipeline job dispatched by the Controller to a Worker via NATS.
///
/// The signing payload covers the job identity, step count, WASM integrity hashes,
/// and nonce — making it impossible for an attacker to add/remove/replace steps
/// without invalidating the signature.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineJobRequest {
    pub job_id: Uuid,
    pub workflow_execution_id: Uuid,
    pub steps: Vec<PipelineStep>,
    /// Total timeout for the entire pipeline in milliseconds.
    pub total_timeout_ms: u64,
    /// If true, all steps share a single ephemeral filesystem sandbox.
    pub share_sandbox: bool,
    /// HMAC-SHA256 signature over the canonical pipeline fields.
    pub signature: Vec<u8>,
    /// Nonce for replay-attack prevention: `"{unix_secs}:{random_hex}"`.
    pub job_nonce: String,
    /// User ID for global rate limiting and audit logging.
    pub user_id: Uuid,
}

impl PipelineJobRequest {
    /// Canonical signing payload.
    ///
    /// Format:
    /// `pipeline:{job_id}:{wex_id}:{nonce}:{total_timeout_ms}:{share_sandbox}:
    ///  {num_steps}:{user_id}:{sha256(step0_wasm)}:{sha256(step1_wasm)}:...`
    fn signing_payload(&self) -> Vec<u8> {
        use sha2::Digest;

        let step_hashes: Vec<String> = self
            .steps
            .iter()
            .map(|s| {
                if let Some(b) = s.wasm_bytes.as_deref() {
                    // Inline bytes: hash the actual content.
                    hex::encode(Sha256::digest(b))
                } else if let Some(ref h) = s.expected_wasm_hash {
                    // No inline bytes but controller committed to a content hash.
                    h.clone()
                } else {
                    // No hash commitment: fall back to URI (unchanged legacy behavior).
                    hex::encode(Sha256::digest(s.module_uri.as_bytes()))
                }
            })
            .collect();

        // Per-step integration_name commitment. Same reasoning as
        // JobRequest::signing_payload — a NATS-channel tamperer could
        // otherwise swap a step's integration_name and redirect that
        // step's integration_state writes into a different namespace.
        // Sentinel "-" for non-integration steps (distinct from empty).
        //
        // Wire-format stability: appended at the END of the format
        // string — safe during coordinated deploys; reordering would
        // break every deployed pipeline signature.
        let step_integrations: Vec<&str> = self
            .steps
            .iter()
            .map(|s| s.integration_name.as_deref().unwrap_or("-"))
            .collect();

        format!(
            "pipeline:{}:{}:{}:{}:{}:{}:{}:{}:{}",
            self.job_id,
            self.workflow_execution_id,
            self.job_nonce,
            self.total_timeout_ms,
            self.share_sandbox,
            self.steps.len(),
            self.user_id,
            step_hashes.join(":"),
            step_integrations.join(","),
        )
        .into_bytes()
    }

    /// Sign the pipeline request using the pre-shared `key`.
    pub fn sign(&mut self, key: &[u8]) -> Result<(), String> {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| format!("system time error: {e}"))?
            .as_secs();
        let rand_bytes: [u8; 16] = rand::thread_rng().gen();
        self.job_nonce = format!("{}:{}", ts, hex::encode(rand_bytes));

        let mut mac =
            <HmacSha256 as Mac>::new_from_slice(key).map_err(|e| format!("HMAC key error: {e}"))?;
        mac.update(&self.signing_payload());
        self.signature = mac.finalize().into_bytes().to_vec();
        Ok(())
    }

    /// Verify the HMAC signature and nonce freshness.
    pub fn verify(&self, key: &[u8], max_age_secs: u64) -> Result<(), String> {
        let parts: Vec<&str> = self.job_nonce.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err("malformed job_nonce".to_string());
        }
        let ts: u64 = parts[0]
            .parse()
            .map_err(|_| "invalid timestamp in job_nonce".to_string())?;
        if hex::decode(parts[1]).is_err() {
            return Err("invalid hex in job_nonce".to_string());
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if now.saturating_sub(ts) > max_age_secs {
            return Err(format!(
                "job_nonce is too old ({} s, max {})",
                now.saturating_sub(ts),
                max_age_secs
            ));
        }
        if ts.saturating_sub(now) > MAX_FUTURE_SKEW_SECS {
            return Err(format!(
                "job_nonce is in the future ({} s ahead, max {})",
                ts.saturating_sub(now),
                MAX_FUTURE_SKEW_SECS
            ));
        }

        let mut mac =
            <HmacSha256 as Mac>::new_from_slice(key).map_err(|e| format!("HMAC key error: {e}"))?;
        mac.update(&self.signing_payload());
        mac.verify_slice(&self.signature)
            .map_err(|_| "HMAC signature verification failed".to_string())
    }
}

/// Per-step result within a `PipelineJobResult`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStepResult {
    pub module_id: Uuid,
    pub status: JobStatus,
    pub output: serde_json::Value,
    pub execution_time_ms: u64,
    pub error: Option<String>,
}

/// Result of a pipeline job returned by the Worker via NATS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineJobResult {
    pub job_id: Uuid,
    pub overall_status: JobStatus,
    pub step_results: Vec<PipelineStepResult>,
    pub final_output: serde_json::Value,
    pub total_time_ms: u64,
    /// HMAC-SHA256 signature over the canonical result fields.
    pub signature: Vec<u8>,
    /// Nonce for replay prevention.
    pub result_nonce: String,
}

impl PipelineJobResult {
    /// Canonical signing payload.
    ///
    /// Format:
    /// `pipeline_result:{job_id}:{overall_status}:{result_nonce}:
    ///  {total_time_ms}:{sha256(final_output_json)}`
    fn signing_payload(&self) -> Vec<u8> {
        use sha2::Digest;
        let status_str = match self.overall_status {
            JobStatus::Success => "success",
            JobStatus::Failed => "failed",
            JobStatus::TimedOut => "timedout",
        };
        let output_hash = hex::encode(Sha256::digest(self.final_output.to_string().as_bytes()));
        format!(
            "pipeline_result:{}:{}:{}:{}:{}",
            self.job_id, status_str, self.result_nonce, self.total_time_ms, output_hash,
        )
        .into_bytes()
    }

    /// Sign the pipeline result using the pre-shared `key`.
    pub fn sign(&mut self, key: &[u8]) -> Result<(), String> {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| format!("system time error: {e}"))?
            .as_secs();
        let rand_bytes: [u8; 16] = rand::thread_rng().gen();
        self.result_nonce = format!("{}:{}", ts, hex::encode(rand_bytes));

        let mut mac =
            <HmacSha256 as Mac>::new_from_slice(key).map_err(|e| format!("HMAC key error: {e}"))?;
        mac.update(&self.signing_payload());
        self.signature = mac.finalize().into_bytes().to_vec();
        Ok(())
    }

    /// Verify the HMAC signature and nonce freshness.
    pub fn verify(&self, key: &[u8], max_age_secs: u64) -> Result<(), String> {
        let parts: Vec<&str> = self.result_nonce.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err("malformed result_nonce".to_string());
        }
        // Validate hex portion to reject malformed nonces early.
        if hex::decode(parts[1]).is_err() {
            return Err("invalid hex in result_nonce".to_string());
        }
        let ts: u64 = parts[0]
            .parse()
            .map_err(|_| "invalid timestamp in result_nonce".to_string())?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if now.saturating_sub(ts) > max_age_secs {
            return Err(format!(
                "result_nonce is too old ({} s, max {})",
                now.saturating_sub(ts),
                max_age_secs
            ));
        }
        if ts.saturating_sub(now) > MAX_FUTURE_SKEW_SECS {
            return Err(format!(
                "result_nonce is in the future ({} s ahead, max {})",
                ts.saturating_sub(now),
                MAX_FUTURE_SKEW_SECS
            ));
        }

        let mut mac =
            <HmacSha256 as Mac>::new_from_slice(key).map_err(|e| format!("HMAC key error: {e}"))?;
        mac.update(&self.signing_payload());
        mac.verify_slice(&self.signature)
            .map_err(|_| "HMAC signature verification failed".to_string())
    }
}

// ============================================================================
// Worker heartbeat
// ============================================================================

/// Heartbeat message published by workers so the controller can track fleet health.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerHeartbeat {
    pub worker_id: Uuid,
    /// Self-reported capabilities (e.g. ["wasm", "gpu", "network"]).
    pub capabilities: Vec<String>,
    /// Current CPU usage as a percentage (0.0 – 100.0).
    pub cpu_usage_pct: f32,
    /// HMAC-SHA256 signature for tamper detection.
    #[serde(default)]
    pub signature: Vec<u8>,
    /// Nonce for replay prevention: `"{unix_secs}:{random_hex}"`.
    #[serde(default)]
    pub heartbeat_nonce: String,
}

impl WorkerHeartbeat {
    /// Canonical signing payload — includes capabilities to prevent forgery.
    fn signing_payload(&self) -> Vec<u8> {
        format!(
            "heartbeat:{}:{}:{}:{}",
            self.worker_id,
            self.heartbeat_nonce,
            self.cpu_usage_pct,
            self.capabilities.join(","),
        )
        .into_bytes()
    }

    /// Sign the heartbeat using the pre-shared `key`.
    pub fn sign(&mut self, key: &[u8]) -> Result<(), String> {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| format!("system time error: {e}"))?
            .as_secs();
        let rand_bytes: [u8; 16] = rand::thread_rng().gen();
        self.heartbeat_nonce = format!("{}:{}", ts, hex::encode(rand_bytes));

        let mut mac =
            <HmacSha256 as Mac>::new_from_slice(key).map_err(|e| format!("HMAC key error: {e}"))?;
        mac.update(&self.signing_payload());
        self.signature = mac.finalize().into_bytes().to_vec();
        Ok(())
    }

    /// Verify the HMAC signature and nonce freshness.
    pub fn verify(&self, key: &[u8], max_age_secs: u64) -> Result<(), String> {
        let parts: Vec<&str> = self.heartbeat_nonce.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err("malformed heartbeat_nonce".to_string());
        }
        let ts: u64 = parts[0]
            .parse()
            .map_err(|_| "invalid timestamp in heartbeat_nonce".to_string())?;
        if hex::decode(parts[1]).is_err() {
            return Err("invalid hex in heartbeat_nonce".to_string());
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if now.saturating_sub(ts) > max_age_secs {
            return Err(format!(
                "heartbeat_nonce is too old ({} s, max {})",
                now.saturating_sub(ts),
                max_age_secs
            ));
        }
        if ts.saturating_sub(now) > MAX_FUTURE_SKEW_SECS {
            return Err(format!(
                "heartbeat_nonce is in the future ({} s ahead, max {})",
                ts.saturating_sub(now),
                MAX_FUTURE_SKEW_SECS
            ));
        }

        let mut mac =
            <HmacSha256 as Mac>::new_from_slice(key).map_err(|e| format!("HMAC key error: {e}"))?;
        mac.update(&self.signing_payload());
        mac.verify_slice(&self.signature)
            .map_err(|_| "HMAC signature verification failed".to_string())
    }
}

// ============================================================================
// Shared-key helper
// ============================================================================

/// Decode the `WORKER_SHARED_KEY` environment variable (64 hex chars → 32 bytes)
/// and return it wrapped in a [`WorkerSharedKey`].
///
/// Both the controller and the worker must call this at startup and fail-fast
/// if the key is absent or malformed.
///
/// # Key rotation
///
/// The key is loaded once via `OnceLock` on both sides — subsequent calls
/// return the cached value. **Rotating this key requires restarting both
/// the controller and all workers simultaneously.** A rolling restart
/// (workers first, then controller, or vice-versa) creates a window where
/// HMAC verification fails and all NATS RPC requests are rejected.
///
/// This is intentional: live rotation of a symmetric signing key without
/// a key-ID negotiation protocol is strictly harder to get right than a
/// coordinated restart, and the failure mode of a botched live rotation
/// (silent signature bypass) is worse than the failure mode of a staggered
/// restart (loud, temporary request rejection).
///
/// [`WorkerSharedKey`]: talos_workflow_engine_core::WorkerSharedKey
pub fn load_worker_shared_key() -> Result<talos_workflow_engine_core::WorkerSharedKey, String> {
    // Support Docker secrets via WORKER_SHARED_KEY_FILE in addition to direct env var
    let hex_key = std::env::var("WORKER_SHARED_KEY")
        .ok()
        .or_else(|| {
            std::env::var("WORKER_SHARED_KEY_FILE").ok().and_then(|path| {
                std::fs::read_to_string(&path)
                    .map(|s| s.trim_end_matches('\n').trim_end_matches('\r').to_string())
                    .ok()
                    .filter(|s| !s.is_empty())
            })
        })
        .ok_or_else(|| {
            "WORKER_SHARED_KEY environment variable is not set (or WORKER_SHARED_KEY_FILE for Docker secrets). \
             Generate with: openssl rand -hex 32"
                .to_string()
        })?;

    let key = hex::decode(hex_key.trim())
        .map_err(|e| format!("WORKER_SHARED_KEY is not valid hex: {e}"))?;

    if key.len() != 32 {
        return Err(format!(
            "WORKER_SHARED_KEY must be 32 bytes (64 hex chars), got {} bytes",
            key.len()
        ));
    }

    Ok(talos_workflow_engine_core::WorkerSharedKey::new(key))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> Vec<u8> {
        vec![0x42u8; 32] // 32-byte test key
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let key = test_key();
        let mut secrets = HashMap::new();
        secrets.insert("slack/token".to_string(), "xoxb-secret".to_string());
        secrets.insert("api/key".to_string(), "sk-12345".to_string());

        let encrypted = EncryptedSecrets::encrypt(&secrets, &key).unwrap();
        assert!(!encrypted.ciphertext.is_empty());
        assert_eq!(encrypted.nonce.len(), 12);

        let decrypted = encrypted.decrypt(&key).unwrap();
        assert_eq!(decrypted, secrets);
    }

    #[test]
    fn test_wrong_key_fails_decryption() {
        let key1 = test_key();
        let key2 = vec![0xFFu8; 32];
        let mut secrets = HashMap::new();
        secrets.insert("key".to_string(), "value".to_string());

        let encrypted = EncryptedSecrets::encrypt(&secrets, &key1).unwrap();
        let result = encrypted.decrypt(&key2);
        assert!(result.is_err());
    }

    #[test]
    fn test_sign_and_verify() {
        let key = test_key();
        let mut req = JobRequest {
            job_id: Uuid::new_v4(),
            workflow_execution_id: Uuid::new_v4(),
            module_uri: "wasm://module/v1".to_string(),
            input_payload: serde_json::json!({}),
            encrypted_secrets: EncryptedSecrets::default(),
            timeout_ms: 30000,
            priority: 100,
            deadline_unix_secs: 0,
            cancellation_token: None,
            allowed_hosts: vec![],
            allowed_methods: vec![],
            allowed_secrets: vec![],
            allowed_sql_operations: vec![],
            allow_tier2_exposure: false,
            signature: vec![],
            job_nonce: String::new(),
            actor_id: None,
            wasm_bytes: None,
            capability_world: None,
            integration_name: None,
            user_id: Uuid::nil(),
            expected_wasm_hash: None,
            max_fuel: 0,
            dry_run: false,
        };

        req.sign(&key).unwrap();
        assert!(!req.signature.is_empty());
        assert!(!req.job_nonce.is_empty());

        // Verification should pass
        req.verify(&key, 300).unwrap();
    }

    #[test]
    fn test_tampered_signature_fails() {
        let key = test_key();
        let mut req = JobRequest {
            job_id: Uuid::new_v4(),
            workflow_execution_id: Uuid::new_v4(),
            module_uri: "wasm://module/v1".to_string(),
            input_payload: serde_json::json!({}),
            encrypted_secrets: EncryptedSecrets::default(),
            timeout_ms: 30000,
            priority: 100,
            deadline_unix_secs: 0,
            cancellation_token: None,
            allowed_hosts: vec![],
            allowed_methods: vec![],
            allowed_secrets: vec![],
            allowed_sql_operations: vec![],
            allow_tier2_exposure: false,
            signature: vec![],
            job_nonce: String::new(),
            actor_id: None,
            wasm_bytes: None,
            capability_world: None,
            integration_name: None,
            user_id: Uuid::nil(),
            expected_wasm_hash: None,
            max_fuel: 0,
            dry_run: false,
        };
        req.sign(&key).unwrap();
        req.module_uri = "wasm://evil-module/v1".to_string(); // tamper
        let result = req.verify(&key, 300);
        assert!(result.is_err());
    }

    #[test]
    fn test_job_result_sign_and_verify() {
        let key = test_key();
        let mut result = JobResult {
            job_id: Uuid::new_v4(),
            status: JobStatus::Success,
            output_payload: serde_json::json!({"answer": 42}),
            logs: vec![],
            execution_time_ms: 150,
            signature: vec![],
            result_nonce: String::new(),
        };

        result.sign(&key).unwrap();
        assert!(!result.signature.is_empty());
        assert!(!result.result_nonce.is_empty());

        result.verify(&key, 300).unwrap();
    }

    #[test]
    fn test_job_result_tampered_fails() {
        let key = test_key();
        let mut result = JobResult {
            job_id: Uuid::new_v4(),
            status: JobStatus::Success,
            output_payload: serde_json::json!({"answer": 42}),
            logs: vec![],
            execution_time_ms: 150,
            signature: vec![],
            result_nonce: String::new(),
        };
        result.sign(&key).unwrap();
        result.output_payload = serde_json::json!({"answer": 99}); // tamper
        assert!(result.verify(&key, 300).is_err());
    }

    #[test]
    fn test_tampered_allowed_methods_fails() {
        let key = test_key();
        let mut req = JobRequest {
            job_id: Uuid::new_v4(),
            workflow_execution_id: Uuid::new_v4(),
            module_uri: "wasm://module/v1".to_string(),
            input_payload: serde_json::json!({}),
            encrypted_secrets: EncryptedSecrets::default(),
            timeout_ms: 30000,
            priority: 100,
            deadline_unix_secs: 0,
            cancellation_token: None,
            allowed_hosts: vec!["api.example.com".to_string()],
            allowed_methods: vec!["GET".to_string()],
            allowed_secrets: vec![],
            allowed_sql_operations: vec![],
            allow_tier2_exposure: false,
            signature: vec![],
            job_nonce: String::new(),
            actor_id: None,
            wasm_bytes: None,
            capability_world: None,
            integration_name: None,
            user_id: Uuid::nil(),
            expected_wasm_hash: None,
            max_fuel: 0,
            dry_run: false,
        };
        req.sign(&key).unwrap();
        // An attacker cannot escalate from GET-only to POST by modifying the field.
        req.allowed_methods = vec!["GET".to_string(), "POST".to_string()];
        assert!(
            req.verify(&key, 300).is_err(),
            "tampered allowed_methods must fail verification"
        );
    }

    #[test]
    fn test_allowed_methods_order_independent() {
        let key = test_key();
        let mut req = JobRequest {
            job_id: Uuid::new_v4(),
            workflow_execution_id: Uuid::new_v4(),
            module_uri: "wasm://module/v1".to_string(),
            input_payload: serde_json::json!({}),
            encrypted_secrets: EncryptedSecrets::default(),
            timeout_ms: 30000,
            priority: 100,
            deadline_unix_secs: 0,
            cancellation_token: None,
            allowed_hosts: vec![],
            allowed_methods: vec!["POST".to_string(), "GET".to_string()],
            allowed_secrets: vec![],
            allowed_sql_operations: vec![],
            allow_tier2_exposure: false,
            signature: vec![],
            job_nonce: String::new(),
            actor_id: None,
            wasm_bytes: None,
            capability_world: None,
            integration_name: None,
            user_id: Uuid::nil(),
            expected_wasm_hash: None,
            max_fuel: 0,
            dry_run: false,
        };
        req.sign(&key).unwrap();
        // Reordering must not affect verification (sorted before hashing).
        req.allowed_methods = vec!["GET".to_string(), "POST".to_string()];
        req.verify(&key, 300)
            .expect("order-independent allowed_methods must still verify");
    }

    #[test]
    fn test_job_result_unsigned_fails() {
        let key = test_key();
        let result = JobResult {
            job_id: Uuid::new_v4(),
            status: JobStatus::Success,
            output_payload: serde_json::json!({}),
            logs: vec![],
            execution_time_ms: 0,
            signature: vec![],
            result_nonce: String::new(),
        };
        assert!(result.verify(&key, 300).is_err());
    }
}

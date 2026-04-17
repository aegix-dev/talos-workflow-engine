//! Pluggable encryption envelope for per-dispatch secrets.
//!
//! The executor resolves plaintext secrets through a
//! [`SecretsResolver`](crate::SecretsResolver), then hands them to a
//! [`SecretEnvelope`] to seal before wire transmission. The envelope
//! owns the algorithm choice; the engine treats its output as opaque
//! bytes.
//!
//! # Security invariants impls MUST uphold
//!
//! * Generate a fresh content-encryption key for each call. Static
//!   keys across calls enable replay if the shared key later leaks.
//! * Generate a fresh nonce for each call. A reused nonce under the
//!   same key breaks the confidentiality and integrity of AEAD schemes.
//! * Authenticate the ciphertext (AEAD or encrypt-then-MAC). Plain
//!   CTR/CBC without MAC is not acceptable — the engine does not add
//!   an outer MAC.
//! * Return an error rather than returning plaintext-in-ciphertext-
//!   field on failure. The engine's dispatch guard rails assume a
//!   non-empty ciphertext implies a real seal.
//!
//! The reference `AesGcmSecretEnvelope` shipped in the
//! `talos-workflow-job-protocol` crate satisfies all of the above.
//! Consumers whose workers speak a different wire format implement
//! this trait themselves; consumers who don't need encryption (a
//! pure in-process executor) can still prefer the default — the
//! per-call AES cost is single-digit microseconds on typical
//! workloads.

use std::collections::HashMap;

use async_trait::async_trait;

use crate::BoxError;

/// Seals a plaintext secrets map into a `(ciphertext, nonce)` pair
/// authenticated under `shared_key`.
#[async_trait]
pub trait SecretEnvelope: Send + Sync {
    /// Encrypt `secrets` under `shared_key`, returning
    /// `(ciphertext, nonce)`.
    ///
    /// * `secrets` — plaintext `key → value` map. Impls MUST NOT log
    ///   or persist the plaintext.
    /// * `shared_key` — pre-shared authentication key. Borrowed; the
    ///   impl must not retain it past the call. Typical impls use it
    ///   as both the HMAC key for an outer MAC and as an input to a
    ///   per-call KDF that derives the content-encryption key.
    ///
    /// Returns `(ciphertext_bytes, nonce_bytes)`. Both are opaque to
    /// the engine — it forwards them verbatim into the wire format.
    ///
    /// An empty `secrets` map is a valid input. Impls may return an
    /// empty `ciphertext` + empty `nonce` as a sentinel meaning
    /// "nothing to seal"; the reference impl does this.
    async fn seal(
        &self,
        secrets: &HashMap<String, String>,
        shared_key: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), BoxError>;
}

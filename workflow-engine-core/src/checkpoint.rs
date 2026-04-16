//! Pluggable checkpoint storage for paused / resumable workflows.
//!
//! When a workflow hits a `Wait` node or is cancelled mid-run, the
//! executor can persist each completed node's output so execution can
//! resume later. [`CheckpointStore`] is the trait the executor talks to
//! for resumption; the backing store (Postgres, S3, a local file, an
//! in-memory map for tests) is the consumer's choice.
//!
//! This initial version is **load-only**. A `save` method will land
//! once the first consumer migrates off its bespoke persistence path —
//! that migration will fix the trait's save semantics against a real
//! workload instead of guessing at them.

use std::collections::HashMap;

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::BoxError;

/// Retrieve per-node outputs for a paused execution.
///
/// # Semantics
///
/// * [`load`](Self::load) returns an empty map when the execution has
///   no checkpoint — a fresh run is indistinguishable from a run with
///   zero completed nodes, so `Ok(empty)` is correct for both.
/// * Whether the stored blob is encrypted, compressed, or serialized
///   differently than the returned `JsonValue` is entirely up to the
///   impl. The trait traffics in plaintext `JsonValue`.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Load the per-node output map previously persisted for
    /// `execution_id`. Returns an empty map when no checkpoint exists.
    async fn load(
        &self,
        execution_id: Uuid,
    ) -> Result<HashMap<Uuid, JsonValue>, BoxError>;
}

//! Pluggable per-node execution audit log.
//!
//! The executor writes a row for every node / pipeline-step dispatch:
//! a "running" row at dispatch time and a "completed" / "failed" /
//! "timeout" / "cancelled" row when the worker reports back. Consumers
//! use these rows for observability dashboards, per-module latency
//! histograms, and retry audit trails. Concrete storage (Postgres
//! `module_executions` table, an S3 append log, an in-memory ring
//! buffer for tests) is the impl's choice.
//!
//! # Why a separate trait
//!
//! This could plausibly fold into [`crate::NodeLifecycleHook`], but
//! `NodeLifecycleHook` fires **once per node completion**;
//! [`ModuleExecutionStore`] writes rows at **two distinct points**
//! (pre-dispatch "running" INSERT + post-dispatch UPDATE) and the
//! engine holds row ids across that boundary. Splitting keeps each
//! trait focused.

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::BoxError;

/// Record per-dispatch execution rows.
#[async_trait]
pub trait ModuleExecutionStore: Send + Sync {
    /// Insert a "running" row for a dispatched node or pipeline step.
    ///
    /// When `race_safe_status` is `true`, the row enters as
    /// `"cancelled"` if the parent workflow has already been flipped
    /// to a terminal state (`failed` / `cancelled`) by a sibling node
    /// failure, and `"running"` otherwise. Single-node dispatch paths
    /// set this true to close the race between a sibling's failure
    /// UPDATE and the current node's INSERT under concurrent load.
    /// Pipeline steps (which dispatch atomically as a unit) set this
    /// false — there's no concurrent sibling to race against.
    ///
    /// Impls SHOULD be idempotent on `id` collision (the Talos Postgres
    /// impl uses `INSERT ... ON CONFLICT DO NOTHING`). Observability
    /// readers tolerate a missing row (unknown run) better than a
    /// duplicate-key error that aborts dispatch.
    async fn record_started(
        &self,
        id: Uuid,
        module_id: Uuid,
        user_id: Uuid,
        workflow_execution_id: Uuid,
        input: &JsonValue,
        trigger_type: &str,
        race_safe_status: bool,
    ) -> Result<(), BoxError>;

    /// Update an existing row with completion state. `status` is one
    /// of `"completed"` / `"failed"` / `"timeout"` / `"cancelled"`
    /// (free-form to match the backing table's check constraint;
    /// impls that enforce an enum validate here).
    async fn record_completed(
        &self,
        id: Uuid,
        status: &str,
        output: &JsonValue,
        duration_ms: i32,
        error_message: Option<&str>,
    ) -> Result<(), BoxError>;

    /// Resolve a `module_id_or_template_id` to the actual
    /// `wasm_modules.id` used for the foreign key. Template-dispatched
    /// paths pass a `node_templates.id`; the Talos impl maps it to the
    /// matching `wasm_modules` row (most recent compile). Returns the
    /// input unchanged if no mapping exists — the engine stores that
    /// as-is and the FK may fail downstream, which is correct: a
    /// missing wasm_modules row is a legitimate DB-state error, not
    /// something the engine should paper over.
    async fn resolve_wasm_module_id(&self, id_or_template: Uuid) -> Uuid;
}

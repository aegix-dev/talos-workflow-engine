//! Convenience wrappers that bridge a pre-built `NodeDispatcher`
//! (typically a [`NatsNodeDispatcher`](crate::NatsNodeDispatcher)
//! wrapping a [`NatsTransport`](crate::NatsTransport)) to the engine's
//! abstract `run_with_transport` / `run_with_seed_with_transport`
//! entry points.
//!
//! These exist purely as thin forwards — they let callers import a
//! single "run the engine over NATS" symbol from this crate instead of
//! reaching into `workflow_engine::ParallelWorkflowEngine` directly.
//! Callers that already hold the engine + dispatcher can call
//! `engine.run_with_transport(...)` themselves and ignore this module.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value as JsonValue;
use uuid::Uuid;
use workflow_engine::ParallelWorkflowEngine;
use workflow_engine_core::{NodeDispatcher, WorkflowContext};

/// Dispatch the engine via a pre-built `NodeDispatcher`. The usual
/// caller builds a [`NatsNodeDispatcher`](crate::NatsNodeDispatcher)
/// wrapping a [`NatsTransport`](crate::NatsTransport); both live in
/// this crate.
pub async fn run_with_nats(
    engine: &ParallelWorkflowEngine,
    dispatcher: Arc<dyn NodeDispatcher>,
    worker_shared_key: Option<Arc<Vec<u8>>>,
    execution_id: Uuid,
) -> Result<WorkflowContext, String> {
    engine
        .run_with_transport(dispatcher, worker_shared_key, execution_id)
        .await
}

/// Seeded-dispatch variant. Signature mirrors
/// [`ParallelWorkflowEngine::run_with_seed_with_transport`]; the only
/// thing added over a direct call is naming symmetry with
/// [`run_with_nats`].
pub fn run_with_seed_via_nats(
    engine: &ParallelWorkflowEngine,
    dispatcher: Arc<dyn NodeDispatcher>,
    worker_shared_key: Option<Arc<Vec<u8>>>,
    initial_results: HashMap<Uuid, JsonValue>,
    execution_id: Uuid,
) -> Pin<Box<dyn Future<Output = Result<WorkflowContext, String>> + Send + '_>> {
    engine.run_with_seed_with_transport(
        dispatcher,
        worker_shared_key,
        initial_results,
        execution_id,
    )
}

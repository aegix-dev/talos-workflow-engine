//! Pluggable read-only access to workflow graph definitions.
//!
//! When a workflow graph contains a system node whose body is *another*
//! workflow (sub-workflow, judge, ensemble child, `AgentLoop`, etc.), the
//! executor needs to load that workflow's `graph_json` at dispatch time.
//! [`WorkflowGraphStore`] is the abstraction it uses — the backing store
//! can be Postgres, an in-memory map for tests, or anything else the
//! consumer wires in.
//!
//! The trait is **read-only**: callers that need to *create* workflows
//! go through a different path (e.g. a dedicated workflow-authoring
//! service). The executor's concern is hydration, not mutation.

use std::collections::HashMap;

use async_trait::async_trait;
use uuid::Uuid;

use crate::BoxError;

/// Resolve stored workflow graphs by id, scoped to a user/tenant.
///
/// # Security contract
///
/// Both methods take a `user_id` parameter and impls **MUST NOT** return
/// a graph the caller does not own — returning `None` (or an absent map
/// entry) for a workflow the caller is not authorized to read is correct
/// and indistinguishable from "no such workflow" at this layer. This is
/// a hard invariant, not a soft expectation: the executor does not
/// re-check ownership on the returned graph.
#[async_trait]
pub trait WorkflowGraphStore: Send + Sync {
    /// Fetch one workflow's `graph_json` string. Returns `Ok(None)`
    /// when no workflow with that id is visible to `user_id`.
    async fn get_graph(
        &self,
        workflow_id: Uuid,
        user_id: Uuid,
    ) -> Result<Option<String>, BoxError>;

    /// Batch-fetch `graph_json` for a set of workflow ids scoped to
    /// `user_id`. Ids that do not resolve are simply absent from the
    /// returned map — the caller is expected to tolerate partial results.
    ///
    /// # Overriding
    ///
    /// The default implementation is a serial loop over
    /// [`get_graph`](Self::get_graph). It's correct, but it's `O(N)`
    /// round-trips with no parallelism — acceptable only for in-memory
    /// test impls. **Override this method in any impl whose backing
    /// store has per-call latency greater than ~1 ms** (databases,
    /// remote caches, RPC-fronted services) with a single batch query
    /// like `WHERE id = ANY($1) AND user_id = $2`.
    async fn get_graphs(
        &self,
        ids: &[Uuid],
        user_id: Uuid,
    ) -> Result<HashMap<Uuid, String>, BoxError> {
        let mut out = HashMap::with_capacity(ids.len());
        for id in ids {
            if let Some(graph) = self.get_graph(*id, user_id).await? {
                out.insert(*id, graph);
            }
        }
        Ok(out)
    }
}

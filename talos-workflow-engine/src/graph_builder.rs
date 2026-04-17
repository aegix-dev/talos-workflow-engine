//! Typed, programmatic construction of the React-Flow `graph_json`
//! shape accepted by
//! [`ParallelWorkflowEngine::load_from_graph_json`](crate::ParallelWorkflowEngine::load_from_graph_json).
//!
//! The parser is React-Flow-shaped, which is useful when workflows are
//! authored in a visual editor but awkward when a consumer wants to
//! build a graph from Rust code. [`WorkflowGraphBuilder`] is the
//! idiomatic bridge — call `add_module` / `add_system_node` / `edge`
//! methods, then `build()` returns the exact `serde_json::Value` the
//! parser expects. Feed it into `load_from_graph_json` (or pass to a
//! [`WorkflowGraphStore`](talos_workflow_engine_core::WorkflowGraphStore)
//! impl for persistence).
//!
//! # Example
//!
//! ```
//! use std::time::Duration;
//! use serde_json::json;
//! use uuid::Uuid;
//! use talos_workflow_engine::WorkflowGraphBuilder;
//! use talos_workflow_engine_core::SystemNodeKind;
//!
//! let module_id = Uuid::new_v4();
//! let graph = WorkflowGraphBuilder::new()
//!     .execution_timeout(Duration::from_secs(600))
//!     .add_module("fetch", module_id, Some(json!({ "url": "https://example.com" })))
//!     .add_system_node(
//!         "split",
//!         SystemNodeKind::ForEach {
//!             input_path: "items".into(),
//!             output_handle: "element".into(),
//!         },
//!     )
//!     .edge("fetch", "split")
//!     .build();
//!
//! // `graph` is a JSON value with the same shape React Flow produces.
//! assert_eq!(graph["nodes"].as_array().unwrap().len(), 2);
//! assert_eq!(graph["edges"].as_array().unwrap().len(), 1);
//! ```
//!
//! # Preferred construction paths by scenario
//!
//! | Scenario | Preferred path |
//! |---|---|
//! | Workflow authored in React Flow (visual editor) | Hand-written JSON / editor output |
//! | In-process Rust consumers building graphs programmatically | [`WorkflowGraphBuilder`] |
//! | Dynamic / generated workflows with lots of variation | [`WorkflowGraphBuilder`] |
//! | Low-level edge cases (custom node types, third-party extensions) | [`WorkflowGraphBuilder::add_raw_node`] / `add_raw_edge` |
//!
//! All three produce `serde_json::Value`s with the same shape; the
//! engine's parser is the single source of truth for what's accepted.

use std::time::Duration;

use serde_json::{json, Map, Value as JsonValue};
use talos_workflow_engine_core::SystemNodeKind;
use uuid::Uuid;

/// Build a React-Flow-shaped `graph_json` programmatically.
///
/// See the [module-level docs](crate::graph_builder) for an example.
/// The builder is `#[must_use]`-friendly: every mutator returns
/// `self`, so chained calls compile to `build()` or nothing.
#[derive(Debug, Default, Clone)]
#[must_use]
pub struct WorkflowGraphBuilder {
    nodes: Vec<JsonValue>,
    edges: Vec<JsonValue>,
    execution_timeout_secs: Option<u64>,
}

impl WorkflowGraphBuilder {
    /// Build an empty graph. Call `add_module` / `add_system_node` /
    /// `edge` to populate, then `build()` to emit the JSON.
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the default workflow-level execution timeout.
    ///
    /// Sub-second values are truncated to whole seconds. Zero is
    /// accepted verbatim (the parser reads this back as "0s timeout,"
    /// which the engine treats as "use the default" at run time).
    pub fn execution_timeout(mut self, timeout: Duration) -> Self {
        self.execution_timeout_secs = Some(timeout.as_secs());
        self
    }

    /// Add a user-provided module node.
    ///
    /// * `id` — workflow-local node identifier. Accepts any
    ///   human-readable string; the engine derives a stable `Uuid`
    ///   from the string if it isn't already a UUID.
    /// * `module_id` — the module that executes at this node.
    /// * `config` — optional per-node configuration forwarded to the
    ///   worker. Shape is module-defined.
    pub fn add_module(
        mut self,
        id: impl Into<String>,
        module_id: Uuid,
        config: Option<JsonValue>,
    ) -> Self {
        let id = id.into();
        let mut node = Map::new();
        node.insert("id".to_string(), JsonValue::String(id));
        node.insert("type".to_string(), JsonValue::String(module_id.to_string()));
        if let Some(data) = config {
            node.insert("data".to_string(), data);
        }
        self.nodes.push(JsonValue::Object(node));
        self
    }

    /// Add a built-in system node for any [`SystemNodeKind`] variant.
    ///
    /// Serializes the variant into the React-Flow `kind` + `data`
    /// shape the parser accepts. LLM-flavored variants are only
    /// present when compiled with the `llm-primitives` feature;
    /// passing one without the feature is a compile-time error.
    pub fn add_system_node(mut self, id: impl Into<String>, kind: SystemNodeKind) -> Self {
        let id = id.into();
        let (kind_str, data) = serialize_system_node_kind(&kind);
        let mut node = Map::new();
        node.insert("id".to_string(), JsonValue::String(id));
        // The engine's full parser (`load_graph_from_json`) dispatches
        // system-only nodes on the `type: "system:<kind>"` prefix AND
        // reads the kind from the `kind` field. Emit both so the node
        // round-trips through either dispatch path.
        node.insert(
            "type".to_string(),
            JsonValue::String(format!("system:{kind_str}")),
        );
        node.insert("kind".to_string(), JsonValue::String(kind_str.to_string()));
        node.insert("data".to_string(), data);
        self.nodes.push(JsonValue::Object(node));
        self
    }

    /// Add a completely custom node shape. Use when a feature isn't
    /// covered by the typed helpers (e.g. experimental kinds in a
    /// fork, or node-type strings consumed by a custom parser in a
    /// downstream fork of this engine).
    ///
    /// The engine's stock parser silently skips nodes it doesn't
    /// recognize — see
    /// [`ParallelWorkflowEngine::load_from_graph_json`](crate::ParallelWorkflowEngine::load_from_graph_json).
    pub fn add_raw_node(mut self, node: JsonValue) -> Self {
        self.nodes.push(node);
        self
    }

    /// Attach a skip-condition expression to the node with `id`.
    ///
    /// The engine reads this into the node's config under the
    /// reserved `__skip_condition` key; when the expression evaluates
    /// truthy at dispatch time, the node short-circuits without
    /// running.
    ///
    /// No-op when no node with `id` has been added yet — the builder
    /// doesn't panic on missing ids to keep chains ergonomic.
    pub fn with_skip_condition(
        mut self,
        id: impl AsRef<str>,
        condition: impl Into<String>,
    ) -> Self {
        let id = id.as_ref();
        let condition: String = condition.into();
        if let Some(node) = self.find_node_mut(id) {
            if let Some(obj) = node.as_object_mut() {
                obj.insert("skip_condition".to_string(), JsonValue::String(condition));
            }
        }
        self
    }

    /// Mark the node with `id` as `continue_on_error`.
    ///
    /// When set, a dispatch failure on this node does not fail the
    /// workflow — downstream nodes still run with the failed node's
    /// error envelope as input.
    ///
    /// No-op when no node with `id` has been added yet.
    pub fn with_continue_on_error(mut self, id: impl AsRef<str>) -> Self {
        let id = id.as_ref();
        if let Some(node) = self.find_node_mut(id) {
            if let Some(obj) = node.as_object_mut() {
                obj.insert("continue_on_error".to_string(), JsonValue::Bool(true));
            }
        }
        self
    }

    /// Attach a per-node retry policy.
    ///
    /// * `max_retries` — max transient-failure retries (timeouts do
    ///   not retry; see the retry-classifier trait).
    /// * `backoff_ms` — base backoff between retries, in ms.
    /// * `condition` — optional expression evaluated against the
    ///   error output to decide whether to retry.
    /// * `delay_expression` — optional expression returning the next
    ///   retry delay in ms, computed from the error output.
    ///
    /// No-op when no node with `id` has been added yet.
    pub fn with_retry(
        mut self,
        id: impl AsRef<str>,
        max_retries: u32,
        backoff_ms: u64,
        condition: Option<String>,
        delay_expression: Option<String>,
    ) -> Self {
        let id = id.as_ref();
        if let Some(node) = self.find_node_mut(id) {
            if let Some(obj) = node.as_object_mut() {
                obj.insert("retry_count".to_string(), json!(max_retries));
                obj.insert("retry_backoff_ms".to_string(), json!(backoff_ms));
                if let Some(c) = condition {
                    obj.insert("retry_condition".to_string(), JsonValue::String(c));
                }
                if let Some(d) = delay_expression {
                    obj.insert("retry_delay_expression".to_string(), JsonValue::String(d));
                }
            }
        }
        self
    }

    /// Add an edge from `source` to `target` with the default
    /// `output → input` handle pair and no condition.
    pub fn edge(self, source: impl Into<String>, target: impl Into<String>) -> Self {
        self.edge_with_handles(source, target, "output", "input")
    }

    /// Add an edge that fires only when `condition` evaluates truthy
    /// against the source node's output.
    pub fn edge_condition(
        self,
        source: impl Into<String>,
        target: impl Into<String>,
        condition: impl Into<String>,
    ) -> Self {
        let mut builder = self.edge(source, target);
        let last = builder.edges.last_mut().and_then(|e| e.as_object_mut());
        if let Some(obj) = last {
            obj.insert("condition".to_string(), JsonValue::String(condition.into()));
        }
        builder
    }

    /// Add an edge with explicit `source`/`target` handles. Use when
    /// a node has multiple outputs (e.g. `on_failure` / `on_success`
    /// or LLM-dispatch route names).
    pub fn edge_with_handles(
        mut self,
        source: impl Into<String>,
        target: impl Into<String>,
        source_handle: impl Into<String>,
        target_handle: impl Into<String>,
    ) -> Self {
        let edge = json!({
            "source": source.into(),
            "target": target.into(),
            "sourceHandle": source_handle.into(),
            "targetHandle": target_handle.into(),
        });
        self.edges.push(edge);
        self
    }

    /// Add a completely custom edge shape. Use when a feature isn't
    /// covered by the typed helpers (e.g. non-default `edge_type`,
    /// mapping expressions, experimental keys).
    pub fn add_raw_edge(mut self, edge: JsonValue) -> Self {
        self.edges.push(edge);
        self
    }

    /// Emit the assembled graph as a JSON value ready to feed into
    /// [`ParallelWorkflowEngine::load_from_graph_json`](crate::ParallelWorkflowEngine::load_from_graph_json)
    /// or a consumer's
    /// [`WorkflowGraphStore`](talos_workflow_engine_core::WorkflowGraphStore).
    #[must_use]
    pub fn build(self) -> JsonValue {
        let mut root = Map::new();
        root.insert("nodes".to_string(), JsonValue::Array(self.nodes));
        root.insert("edges".to_string(), JsonValue::Array(self.edges));
        if let Some(secs) = self.execution_timeout_secs {
            root.insert("execution_timeout_secs".to_string(), json!(secs));
        }
        JsonValue::Object(root)
    }

    fn find_node_mut(&mut self, id: &str) -> Option<&mut JsonValue> {
        self.nodes
            .iter_mut()
            .find(|n| n.get("id").and_then(|v| v.as_str()) == Some(id))
    }
}

/// Map a [`SystemNodeKind`] back into the `(kind_string, data_json)`
/// pair the React-Flow parser reads.
///
/// Kept in one place so parser drift is easy to audit: every variant
/// here corresponds 1:1 to an `else if k == "..."` branch in
/// `engine.rs::load_from_graph_json` / `parse_llm_system_node_kind`.
#[allow(clippy::too_many_lines)]
fn serialize_system_node_kind(kind: &SystemNodeKind) -> (&'static str, JsonValue) {
    match kind {
        SystemNodeKind::ForEach {
            input_path,
            output_handle,
        } => (
            "foreach",
            json!({
                "input_path": input_path,
                "output_handle": output_handle,
            }),
        ),
        SystemNodeKind::Wait { message } => (
            "wait",
            match message {
                Some(m) => json!({ "message": m }),
                None => json!({}),
            },
        ),
        SystemNodeKind::WhileLoop {
            condition,
            max_iterations,
        } => (
            "loop",
            json!({
                "condition": condition,
                "max_iterations": max_iterations,
            }),
        ),
        SystemNodeKind::RepeatLoop { count } => ("loop", json!({ "count": count })),
        SystemNodeKind::ErrorHandler { error_pattern } => (
            "error_handler",
            match error_pattern {
                Some(p) => json!({ "error_pattern": p }),
                None => json!({}),
            },
        ),
        SystemNodeKind::FanIn {
            join_mode,
            aggregation_expr,
        } => (
            "fan_in",
            json!({
                "join_mode": format!("{join_mode:?}").to_lowercase(),
                "aggregation_expr": aggregation_expr,
            }),
        ),
        SystemNodeKind::SubWorkflow {
            workflow_id,
            timeout_secs,
        } => (
            "sub_workflow",
            json!({
                "sub_workflow_id": workflow_id.to_string(),
                "timeout_secs": timeout_secs,
            }),
        ),
        SystemNodeKind::Loop {
            max_iterations,
            condition,
        } => (
            "loop",
            json!({
                "max_iterations": max_iterations,
                "condition": condition,
            }),
        ),
        SystemNodeKind::Collect => ("collect", json!({})),
        SystemNodeKind::Synthesize { synthesis_expr } => (
            "synthesize",
            match synthesis_expr {
                Some(e) => json!({ "synthesis_expr": e }),
                None => json!({}),
            },
        ),
        SystemNodeKind::Verify {
            condition,
            check_label,
            on_failure,
        } => (
            "verify",
            json!({
                "condition": condition,
                "check_label": check_label,
                "on_failure": on_failure,
            }),
        ),
        SystemNodeKind::DynamicDispatch {
            dispatch_expression,
            timeout_secs,
        } => (
            "dispatch",
            json!({
                "dispatch_expression": dispatch_expression,
                "timeout_secs": timeout_secs,
            }),
        ),
        SystemNodeKind::CapabilityDispatch {
            required_capabilities,
            timeout_secs,
        } => (
            "capability_dispatch",
            json!({
                "required_capabilities": required_capabilities,
                "timeout_secs": timeout_secs,
            }),
        ),
        #[cfg(feature = "llm-primitives")]
        SystemNodeKind::AgentLoop {
            body_workflow_id,
            max_iterations,
            inject_history,
            timeout_secs,
        } => (
            "agent_loop",
            json!({
                "body_workflow_id": body_workflow_id.to_string(),
                "max_iterations": max_iterations,
                "inject_history": inject_history,
                "timeout_secs": timeout_secs,
            }),
        ),
        #[cfg(feature = "llm-primitives")]
        SystemNodeKind::Judge {
            judge_workflow_id,
            rubric,
            pass_threshold,
            timeout_secs,
        } => (
            "judge",
            json!({
                "judge_workflow_id": judge_workflow_id.to_string(),
                "rubric": rubric,
                "pass_threshold": pass_threshold,
                "timeout_secs": timeout_secs,
            }),
        ),
        #[cfg(feature = "llm-primitives")]
        SystemNodeKind::Ensemble {
            child_workflow_id,
            count,
            consensus,
            judge_workflow_id,
            timeout_secs,
        } => (
            "ensemble",
            json!({
                "child_workflow_id": child_workflow_id.to_string(),
                "count": count,
                "consensus": consensus,
                "judge_workflow_id": judge_workflow_id.map(|id| id.to_string()),
                "timeout_secs": timeout_secs,
            }),
        ),
        #[cfg(feature = "llm-primitives")]
        SystemNodeKind::ConfidenceGate {
            threshold,
            confidence_path,
            on_low_confidence,
        } => (
            "confidence_gate",
            json!({
                "threshold": threshold,
                "confidence_path": confidence_path,
                "on_low_confidence": on_low_confidence,
            }),
        ),
        #[cfg(feature = "llm-primitives")]
        SystemNodeKind::ReActLoop {
            body_workflow_id,
            max_iterations,
            inject_history,
            timeout_secs,
        } => (
            "react_loop",
            json!({
                "body_workflow_id": body_workflow_id.to_string(),
                "max_iterations": max_iterations,
                "inject_history": inject_history,
                "timeout_secs": timeout_secs,
            }),
        ),
        #[cfg(feature = "llm-primitives")]
        SystemNodeKind::ReflectiveRetry {
            child_workflow_id,
            reflection_workflow_id,
            max_retries,
            timeout_secs,
        } => (
            "reflective_retry",
            json!({
                "child_workflow_id": child_workflow_id.to_string(),
                "reflection_workflow_id": reflection_workflow_id.to_string(),
                "max_retries": max_retries,
                "timeout_secs": timeout_secs,
            }),
        ),
        #[cfg(feature = "llm-primitives")]
        SystemNodeKind::LlmDispatch {
            classifier_workflow_id,
            routes,
            fallback_workflow_id,
            timeout_secs,
        } => (
            "llm_dispatch",
            json!({
                "classifier_workflow_id": classifier_workflow_id.to_string(),
                "routes": routes.iter().map(|(k, v)| (k.clone(), v.to_string())).collect::<std::collections::HashMap<_, _>>(),
                "fallback_workflow_id": fallback_workflow_id.map(|id| id.to_string()),
                "timeout_secs": timeout_secs,
            }),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_builder_produces_empty_nodes_and_edges() {
        let g = WorkflowGraphBuilder::new().build();
        assert_eq!(g["nodes"].as_array().unwrap().len(), 0);
        assert_eq!(g["edges"].as_array().unwrap().len(), 0);
        assert!(g.get("execution_timeout_secs").is_none());
    }

    #[test]
    fn execution_timeout_is_rendered() {
        let g = WorkflowGraphBuilder::new()
            .execution_timeout(Duration::from_secs(123))
            .build();
        assert_eq!(g["execution_timeout_secs"].as_u64(), Some(123));
    }

    #[test]
    fn add_module_emits_react_flow_shape() {
        let module_id = Uuid::new_v4();
        let g = WorkflowGraphBuilder::new()
            .add_module("fetch", module_id, Some(json!({ "url": "x" })))
            .build();
        let node = &g["nodes"][0];
        assert_eq!(node["id"].as_str(), Some("fetch"));
        assert_eq!(node["type"].as_str(), Some(module_id.to_string().as_str()));
        assert_eq!(node["data"]["url"].as_str(), Some("x"));
    }

    #[test]
    fn add_system_node_foreach_emits_expected_shape() {
        let g = WorkflowGraphBuilder::new()
            .add_system_node(
                "split",
                SystemNodeKind::ForEach {
                    input_path: "items".into(),
                    output_handle: "element".into(),
                },
            )
            .build();
        let node = &g["nodes"][0];
        assert_eq!(node["id"].as_str(), Some("split"));
        assert_eq!(node["type"].as_str(), Some("system:foreach"));
        assert_eq!(node["kind"].as_str(), Some("foreach"));
        assert_eq!(node["data"]["input_path"].as_str(), Some("items"));
        assert_eq!(node["data"]["output_handle"].as_str(), Some("element"));
    }

    #[test]
    fn edge_default_handles() {
        let g = WorkflowGraphBuilder::new().edge("a", "b").build();
        let edge = &g["edges"][0];
        assert_eq!(edge["source"].as_str(), Some("a"));
        assert_eq!(edge["target"].as_str(), Some("b"));
        assert_eq!(edge["sourceHandle"].as_str(), Some("output"));
        assert_eq!(edge["targetHandle"].as_str(), Some("input"));
    }

    #[test]
    fn edge_condition_attaches_to_last_edge() {
        let g = WorkflowGraphBuilder::new()
            .edge("a", "b")
            .edge_condition("b", "c", "ok == true")
            .build();
        let second = &g["edges"][1];
        assert_eq!(second["source"].as_str(), Some("b"));
        assert_eq!(second["condition"].as_str(), Some("ok == true"));
        // First edge untouched.
        assert!(g["edges"][0].get("condition").is_none());
    }

    #[test]
    fn with_skip_condition_and_continue_on_error_modify_matching_node() {
        let module_id = Uuid::new_v4();
        let g = WorkflowGraphBuilder::new()
            .add_module("fetch", module_id, None)
            .with_skip_condition("fetch", "upstream.skip")
            .with_continue_on_error("fetch")
            .build();
        let node = &g["nodes"][0];
        assert_eq!(node["skip_condition"].as_str(), Some("upstream.skip"));
        assert_eq!(node["continue_on_error"].as_bool(), Some(true));
    }

    #[test]
    fn with_retry_policy_is_read_at_top_level() {
        let module_id = Uuid::new_v4();
        let g = WorkflowGraphBuilder::new()
            .add_module("fetch", module_id, None)
            .with_retry(
                "fetch",
                3,
                500,
                Some("error_code == 429".into()),
                Some("min(5000, base * 2)".into()),
            )
            .build();
        let node = &g["nodes"][0];
        assert_eq!(node["retry_count"].as_u64(), Some(3));
        assert_eq!(node["retry_backoff_ms"].as_u64(), Some(500));
        assert_eq!(node["retry_condition"].as_str(), Some("error_code == 429"));
        assert_eq!(
            node["retry_delay_expression"].as_str(),
            Some("min(5000, base * 2)")
        );
    }

    #[test]
    fn with_missing_id_is_noop() {
        let g = WorkflowGraphBuilder::new()
            .with_skip_condition("nonexistent", "something")
            .build();
        assert_eq!(g["nodes"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn raw_node_and_edge_passthrough() {
        let g = WorkflowGraphBuilder::new()
            .add_raw_node(json!({ "id": "custom", "type": "experimental" }))
            .add_raw_edge(json!({ "source": "a", "target": "b", "edge_type": "on_failure" }))
            .build();
        assert_eq!(g["nodes"][0]["type"].as_str(), Some("experimental"));
        assert_eq!(g["edges"][0]["edge_type"].as_str(), Some("on_failure"));
    }

    #[test]
    fn system_node_collect_has_empty_data() {
        let g = WorkflowGraphBuilder::new()
            .add_system_node("c", SystemNodeKind::Collect)
            .build();
        assert_eq!(g["nodes"][0]["kind"].as_str(), Some("collect"));
        assert!(g["nodes"][0]["data"].as_object().unwrap().is_empty());
    }

    #[test]
    fn system_node_capability_dispatch_emits_capabilities_array() {
        let g = WorkflowGraphBuilder::new()
            .add_system_node(
                "cap",
                SystemNodeKind::CapabilityDispatch {
                    required_capabilities: vec!["llm".into(), "rag".into()],
                    timeout_secs: 30,
                },
            )
            .build();
        let caps = &g["nodes"][0]["data"]["required_capabilities"];
        assert_eq!(caps[0].as_str(), Some("llm"));
        assert_eq!(caps[1].as_str(), Some("rag"));
    }

    #[tokio::test]
    async fn round_trip_through_load_graph_from_json() {
        // End-to-end: build a graph, serialize, parse, verify topology.
        //
        // We test the async `load_graph_from_json(&str)` because it's
        // the canonical full-feature entry point (handles system
        // nodes; the sync `load_from_graph_json(&Value)` only handles
        // module nodes — a pre-existing parser-divergence the engine
        // carries and a 0.2 unification candidate).
        use crate::ParallelWorkflowEngine;

        let module_id = Uuid::new_v4();
        let graph = WorkflowGraphBuilder::new()
            .execution_timeout(Duration::from_secs(42))
            .add_module("fetch", module_id, Some(json!({ "url": "x" })))
            .add_system_node(
                "split",
                SystemNodeKind::ForEach {
                    input_path: "items".into(),
                    output_handle: "element".into(),
                },
            )
            .add_system_node("aggregate", SystemNodeKind::Collect)
            .edge("fetch", "split")
            .edge("split", "aggregate")
            .build();

        let json_str = serde_json::to_string(&graph).unwrap();

        let mut engine = ParallelWorkflowEngine::new();
        engine
            .load_graph_from_json(&json_str)
            .await
            .expect("parser accepts builder output");

        // Both parsers read nodes + edges identically — assert on
        // those. `execution_timeout_secs` is only read by the sync
        // parser today; the builder emits it regardless so the sync
        // parser path picks it up when used.
        assert_eq!(engine.graph.node_count(), 3);
        assert_eq!(engine.graph.edge_count(), 2);
    }

    #[test]
    fn round_trip_through_load_from_graph_json_module_only() {
        // Complement to the async round-trip test: exercise the sync
        // `load_from_graph_json(&Value)` path. This parser only
        // accepts module nodes; use it here to verify the
        // execution_timeout propagation path that the async parser
        // currently skips.
        use crate::ParallelWorkflowEngine;

        let m1 = Uuid::new_v4();
        let m2 = Uuid::new_v4();
        let graph = WorkflowGraphBuilder::new()
            .execution_timeout(Duration::from_secs(42))
            .add_module("a", m1, None)
            .add_module("b", m2, None)
            .edge("a", "b")
            .build();

        let mut engine = ParallelWorkflowEngine::new();
        engine
            .load_from_graph_json(&graph)
            .expect("parser accepts builder output");

        assert_eq!(engine.graph.node_count(), 2);
        assert_eq!(engine.graph.edge_count(), 1);
        assert_eq!(engine.execution_timeout_secs, 42);
    }

    #[cfg(feature = "llm-primitives")]
    #[test]
    fn system_node_judge_emits_rubric_and_threshold() {
        let judge_wf = Uuid::new_v4();
        let g = WorkflowGraphBuilder::new()
            .add_system_node(
                "judge",
                SystemNodeKind::Judge {
                    judge_workflow_id: judge_wf,
                    rubric: "rate 0-1".into(),
                    pass_threshold: Some(0.8),
                    timeout_secs: 60,
                },
            )
            .build();
        let node = &g["nodes"][0];
        assert_eq!(node["kind"].as_str(), Some("judge"));
        assert_eq!(
            node["data"]["judge_workflow_id"].as_str(),
            Some(judge_wf.to_string().as_str())
        );
        assert_eq!(node["data"]["rubric"].as_str(), Some("rate 0-1"));
        assert_eq!(node["data"]["pass_threshold"].as_f64(), Some(0.8));
    }
}

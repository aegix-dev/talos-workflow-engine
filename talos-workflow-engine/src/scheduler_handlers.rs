//! Per-`SystemNodeKind` dispatch handlers used inside the scheduler loop.
//!
//! Each `try_dispatch_*` method short-circuits if `node_meta[node_id]`
//! does not match its specific [`SystemNodeKind`] variant, returning
//! `None`. If the variant matches, the method computes the node's
//! output (optionally awaiting sub-workflow dispatch), emits any
//! lifecycle events that belong with the handler's semantics, and
//! returns `Some(output)`. The scheduler caller then inserts the
//! output into `results` and unblocks successors uniformly via
//! [`ParallelWorkflowEngine::unblock_successors`].
//!
//! Splitting each handler out of the reactor loop keeps the scheduler
//! body focused on topology (ready queue, futures, chain routing) and
//! lets each kind's semantics stay auditable in isolation.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use petgraph::graph::NodeIndex;
use petgraph::Direction;
use serde_json::{json, Value as JsonValue};
use talos_workflow_engine_core::{DispatchJob, NodeDispatcher, SystemNodeKind, WorkerSharedKey};
use uuid::Uuid;

use crate::engine::ParallelWorkflowEngine;

/// Outcome of [`ParallelWorkflowEngine::try_dispatch_confidence_gate`].
///
/// The confidence gate is unusual among the local handlers because a
/// low-confidence signal can pause the entire workflow for human
/// approval — the scheduler needs to short-circuit the reactor and
/// return a [`WorkflowContext`] with `waiting: true`. Other handlers
/// just compute an output, so they can fit the uniform
/// `Option<JsonValue>` contract; this one can't.
///
/// The handler emits the node's output in both variants; the caller
/// inserts it into the accumulated `results` map and then either
/// continues the reactor or returns early with the fully-accumulated
/// map wrapped in a waiting-state context.
#[cfg(feature = "llm-primitives")]
pub(crate) enum ConfidenceGateOutcome {
    /// Gate's decision is a normal node output; caller inserts into
    /// `results` and unblocks successors as usual.
    Proceed(JsonValue),
    /// Low-confidence branch requested a pause. Caller inserts the
    /// output into its accumulated `results` map, then returns early
    /// with a [`WorkflowContext`] built from that map.
    Pause { waiting_output: JsonValue },
}

impl ParallelWorkflowEngine {
    /// Decrement every successor's pending-count and push nodes whose
    /// count reached zero onto the ready queue.
    ///
    /// Every local-computation handler calls this after inserting its
    /// output into `results`. The two-phase update — decrement first,
    /// then check zero — is load-bearing: a node with two pending
    /// predecessors decrements twice, and only the second decrement
    /// should enqueue it.
    pub(crate) fn unblock_successors(
        &self,
        node_idx: NodeIndex,
        pending: &mut HashMap<NodeIndex, usize>,
        ready: &mut VecDeque<NodeIndex>,
    ) {
        for child in self.graph.neighbors_directed(node_idx, Direction::Outgoing) {
            if let Some(cnt) = pending.get_mut(&child) {
                if *cnt > 0 {
                    *cnt -= 1;
                }
                if *cnt == 0 {
                    ready.push_back(child);
                }
            }
        }
    }

    /// [`SystemNodeKind::Collect`] — aggregate every parent branch's
    /// output into a single `{count, items: [...]}` envelope.
    pub(crate) fn try_dispatch_collect(
        &self,
        node_idx: NodeIndex,
        node_id: Uuid,
        execution_id: Uuid,
        results: &HashMap<Uuid, JsonValue>,
    ) -> Option<JsonValue> {
        let (_, _, Some(SystemNodeKind::Collect)) = self.node_meta.get(&node_id)? else {
            return None;
        };
        let collected = self.collect_parent_outputs_for_node(node_idx, results);
        let parent_count = collected.get("count").and_then(|v| v.as_u64()).unwrap_or(0);
        self.emit_node_lifecycle_events(
            execution_id,
            node_id,
            "Completed",
            format!("collected {parent_count} branch outputs into items array"),
        );
        Some(collected)
    }

    /// [`SystemNodeKind::Synthesize`] — collect parent outputs, then
    /// (optionally) transform the collected value through a Rhai
    /// expression.
    pub(crate) fn try_dispatch_synthesize(
        &self,
        node_idx: NodeIndex,
        node_id: Uuid,
        execution_id: Uuid,
        results: &HashMap<Uuid, JsonValue>,
    ) -> Option<JsonValue> {
        let (_, _, Some(SystemNodeKind::Synthesize { synthesis_expr })) =
            self.node_meta.get(&node_id)?
        else {
            return None;
        };
        let synthesis_expr = synthesis_expr.clone();
        let synthesized = self.synthesize_parent_outputs(node_idx, results, &synthesis_expr);

        // Recover parent_count for event logging from the synthesized output
        // (it may be an object with "count" if no expression was applied, or
        // arbitrary if a Rhai expression transformed it).
        let parent_count = synthesized
            .get("count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        self.emit_node_lifecycle_events(
            execution_id,
            node_id,
            "Completed",
            format!("synthesized {parent_count} branch outputs"),
        );

        Some(synthesized)
    }

    /// [`SystemNodeKind::Verify`] — evaluate a condition against the
    /// node's gathered input and emit a pass/fail outcome.
    pub(crate) fn try_dispatch_verify(
        &self,
        node_idx: NodeIndex,
        node_id: Uuid,
        execution_id: Uuid,
        results: &HashMap<Uuid, JsonValue>,
    ) -> Option<JsonValue> {
        let (
            _,
            _,
            Some(SystemNodeKind::Verify {
                condition,
                check_label,
                on_failure,
            }),
        ) = self.node_meta.get(&node_id)?
        else {
            return None;
        };
        let check_label = check_label
            .clone()
            .unwrap_or_else(|| "output quality".to_string());
        let (verify_result, passed) =
            self.evaluate_verify_node(node_idx, results, condition, &check_label, on_failure);

        self.emit_node_lifecycle_events(
            execution_id,
            node_id,
            if passed { "Completed" } else { "Failed" },
            format!(
                "Verify '{check_label}': {}",
                if passed { "PASSED" } else { "FAILED" }
            ),
        );
        Some(verify_result)
    }

    /// [`SystemNodeKind::WhileLoop`] — run the body *locally* (no
    /// module dispatch), re-evaluating the condition after each pass.
    /// Output records the final iteration count and last wrapped value.
    pub(crate) fn try_dispatch_while_loop(
        &self,
        node_idx: NodeIndex,
        node_id: Uuid,
        results: &HashMap<Uuid, JsonValue>,
    ) -> Option<JsonValue> {
        let (
            _,
            _,
            Some(SystemNodeKind::WhileLoop {
                condition,
                max_iterations,
            }),
        ) = self.node_meta.get(&node_id)?
        else {
            return None;
        };
        let condition = condition.clone();
        let max_iters = *max_iterations;

        let mut current_output = self.gather_inputs(node_idx, results);
        let mut iteration = 0u32;
        while iteration < max_iters {
            if !self.eval_bool(&condition, &current_output) {
                break;
            }
            iteration += 1;
            current_output = json!({
                "__loop_iteration": iteration,
                "__loop_input": current_output,
            });
        }
        if iteration >= max_iters {
            tracing::warn!(
                %node_id,
                max_iterations = max_iters,
                "WhileLoop reached maximum iterations"
            );
        }
        Some(json!({
            "iterations": iteration,
            "output": current_output,
        }))
    }

    /// [`SystemNodeKind::RepeatLoop`] — fixed-count pass-through; the
    /// output records the iteration count and the gathered input.
    pub(crate) fn try_dispatch_repeat_loop(
        &self,
        node_idx: NodeIndex,
        node_id: Uuid,
        results: &HashMap<Uuid, JsonValue>,
    ) -> Option<JsonValue> {
        let (_, _, Some(SystemNodeKind::RepeatLoop { count })) = self.node_meta.get(&node_id)?
        else {
            return None;
        };
        let count = *count;
        let inputs = self.gather_inputs(node_idx, results);
        Some(json!({
            "iterations": count,
            "input": inputs,
        }))
    }

    /// [`SystemNodeKind::Judge`] — run an LLM-as-judge sub-workflow.
    #[cfg(feature = "llm-primitives")]
    pub(crate) async fn try_dispatch_judge(
        &self,
        node_idx: NodeIndex,
        node_id: Uuid,
        dispatcher: &Arc<dyn NodeDispatcher>,
        worker_shared_key: &Option<WorkerSharedKey>,
        results: &HashMap<Uuid, JsonValue>,
    ) -> Option<JsonValue> {
        let (
            _,
            _,
            Some(SystemNodeKind::Judge {
                judge_workflow_id,
                rubric,
                pass_threshold,
                timeout_secs: _,
            }),
        ) = self.node_meta.get(&node_id)?
        else {
            return None;
        };
        let judge_wf_id = *judge_workflow_id;
        let rubric = rubric.clone();
        let pass_threshold = *pass_threshold;
        let parent_inputs = self.gather_inputs(node_idx, results);

        Some(
            self.dispatch_judge(
                parent_inputs,
                judge_wf_id,
                rubric,
                pass_threshold,
                dispatcher.clone(),
                worker_shared_key.clone(),
            )
            .await,
        )
    }

    /// [`SystemNodeKind::ReflectiveRetry`] — run a child workflow and,
    /// on failure, invoke a reflection workflow before retrying.
    #[cfg(feature = "llm-primitives")]
    pub(crate) async fn try_dispatch_reflective_retry(
        &self,
        node_idx: NodeIndex,
        node_id: Uuid,
        dispatcher: &Arc<dyn NodeDispatcher>,
        worker_shared_key: &Option<WorkerSharedKey>,
        results: &HashMap<Uuid, JsonValue>,
    ) -> Option<JsonValue> {
        let (
            _,
            _,
            Some(SystemNodeKind::ReflectiveRetry {
                child_workflow_id,
                reflection_workflow_id,
                max_retries,
                timeout_secs: _,
            }),
        ) = self.node_meta.get(&node_id)?
        else {
            return None;
        };
        let child_wf_id = *child_workflow_id;
        let reflection_wf_id = *reflection_workflow_id;
        let max_retries = *max_retries;
        let initial_input = self.gather_inputs(node_idx, results);

        Some(
            self.dispatch_reflective_retry(
                initial_input,
                child_wf_id,
                reflection_wf_id,
                max_retries,
                dispatcher.clone(),
                worker_shared_key.clone(),
            )
            .await,
        )
    }

    /// [`SystemNodeKind::LlmDispatch`] — route to one of several child
    /// workflows based on a classifier's output.
    #[cfg(feature = "llm-primitives")]
    pub(crate) async fn try_dispatch_llm_dispatch(
        &self,
        node_idx: NodeIndex,
        node_id: Uuid,
        dispatcher: &Arc<dyn NodeDispatcher>,
        worker_shared_key: &Option<WorkerSharedKey>,
        results: &HashMap<Uuid, JsonValue>,
    ) -> Option<JsonValue> {
        let (
            _,
            _,
            Some(SystemNodeKind::LlmDispatch {
                classifier_workflow_id,
                routes,
                fallback_workflow_id,
                timeout_secs: _,
            }),
        ) = self.node_meta.get(&node_id)?
        else {
            return None;
        };
        let classifier_wf_id = *classifier_workflow_id;
        let routes = routes.clone();
        let fallback_wf_id = *fallback_workflow_id;
        let inputs = self.gather_inputs(node_idx, results);

        Some(
            self.dispatch_llm_dispatch(
                inputs,
                classifier_wf_id,
                routes,
                fallback_wf_id,
                dispatcher.clone(),
                worker_shared_key.clone(),
            )
            .await,
        )
    }

    /// [`SystemNodeKind::SubWorkflow`] — invoke another workflow by
    /// id, seeded with this node's gathered input.
    pub(crate) async fn try_dispatch_sub_workflow(
        &self,
        node_idx: NodeIndex,
        node_id: Uuid,
        dispatcher: &Arc<dyn NodeDispatcher>,
        worker_shared_key: &Option<WorkerSharedKey>,
        results: &HashMap<Uuid, JsonValue>,
    ) -> Option<JsonValue> {
        let (
            _,
            _,
            Some(SystemNodeKind::SubWorkflow {
                workflow_id: sub_wf_id,
                timeout_secs: _,
            }),
        ) = self.node_meta.get(&node_id)?
        else {
            return None;
        };
        let sub_wf_id = *sub_wf_id;
        let inputs = self.gather_inputs(node_idx, results);
        tracing::info!(
            %node_id,
            sub_workflow_id = %sub_wf_id,
            "SubWorkflow node — executing sub-workflow"
        );
        Some(
            self.dispatch_subworkflow(
                inputs,
                sub_wf_id,
                dispatcher.clone(),
                worker_shared_key.clone(),
            )
            .await,
        )
    }

    /// [`SystemNodeKind::Loop`] — re-dispatch a body node until
    /// `condition` returns false or `max_iterations` is hit.
    ///
    /// The body node id is read from the loop node's `body_node_id`
    /// config key. Each iteration merges the previous iteration's
    /// output into the body's input and injects `iteration_count`
    /// / `iteration` into the evaluation context so conditions like
    /// `iteration_count < 3` work without the body echoing the counter.
    pub(crate) async fn try_dispatch_loop(
        &self,
        node_idx: NodeIndex,
        node_id: Uuid,
        execution_id: Uuid,
        dispatcher: &Arc<dyn NodeDispatcher>,
        worker_shared_key: &Option<WorkerSharedKey>,
        results: &HashMap<Uuid, JsonValue>,
    ) -> Option<JsonValue> {
        let (
            _,
            _,
            Some(SystemNodeKind::Loop {
                condition,
                max_iterations,
            }),
        ) = self.node_meta.get(&node_id)?
        else {
            return None;
        };
        let condition = condition.clone();
        let max_iters = *max_iterations;
        let inputs = self.gather_inputs(node_idx, results);

        // Find the body_node_id from node config.
        let body_node_id_str = self
            .node_configs
            .get(&node_id)
            .and_then(|c| c.get("body_node_id"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let loop_result = match body_node_id_str {
            None => serde_json::json!({
                "__error": true,
                "error_message": "Loop node missing body_node_id in config",
            }),
            Some(body_rf_id) => {
                let body_uuid = self
                    .node_labels
                    .iter()
                    .find(|(_, label)| label.as_str() == body_rf_id)
                    .map(|(uuid, _)| *uuid);
                let body_module_id = body_uuid
                    .and_then(|u| self.node_meta.get(&u))
                    .and_then(|(mid, _, _)| *mid);

                match (body_uuid, body_module_id) {
                    (None, _) => serde_json::json!({
                        "__error": true,
                        "error_message": format!("Body node '{}' not found in workflow", body_rf_id),
                    }),
                    (Some(_), None) => serde_json::json!({
                        "__error": true,
                        "error_message": format!("Body node '{}' has no module_id", body_rf_id),
                    }),
                    (Some(body_uuid), Some(body_module_id)) => {
                        self.run_loop_iterations(
                            node_id,
                            execution_id,
                            body_uuid,
                            body_module_id,
                            inputs,
                            &condition,
                            max_iters,
                            dispatcher,
                            worker_shared_key,
                            results,
                        )
                        .await
                    }
                }
            }
        };

        Some(loop_result)
    }

    /// Body of the [`SystemNodeKind::Loop`] iteration loop. Kept on
    /// its own method so the happy path in
    /// [`try_dispatch_loop`](Self::try_dispatch_loop) stays readable —
    /// this is the inner "per-iteration: evaluate condition, dispatch
    /// body, collect output" machinery.
    #[allow(clippy::too_many_arguments)]
    async fn run_loop_iterations(
        &self,
        node_id: Uuid,
        execution_id: Uuid,
        body_uuid: Uuid,
        body_module_id: Uuid,
        inputs: JsonValue,
        condition: &str,
        max_iters: u32,
        dispatcher: &Arc<dyn NodeDispatcher>,
        worker_shared_key: &Option<WorkerSharedKey>,
        results: &HashMap<Uuid, JsonValue>,
    ) -> JsonValue {
        let mut current_input = inputs.clone();
        let mut iteration = 0u32;
        let mut last_output = current_input.clone();

        // Extract `__trigger_input__` to inject into every loop iteration.
        // Source: (1) gathered inputs, (2) the `__trigger__` node's
        // output in `results`.
        let trigger_input_val = inputs
            .as_object()
            .and_then(|o| o.get("__trigger_input__"))
            .cloned()
            .or_else(|| {
                self.node_labels
                    .iter()
                    .find(|(_, label)| label.as_str() == "__trigger__")
                    .and_then(|(uuid, _)| results.get(uuid))
                    .cloned()
            });

        while iteration < max_iters {
            // Evaluate condition against current output + loop metadata.
            // `iteration_count` is injected so conditions like
            // `iteration_count < 3` work without the body having to
            // explicitly echo the counter in its output.
            if iteration > 0 {
                let condition_ctx = if let Some(mut obj) = last_output.as_object().cloned() {
                    obj.entry("iteration_count".to_string())
                        .or_insert(serde_json::json!(iteration));
                    obj.entry("iteration".to_string())
                        .or_insert(serde_json::json!(iteration));
                    serde_json::Value::Object(obj)
                } else {
                    serde_json::json!({
                        "iteration_count": iteration,
                        "iteration": iteration,
                        "output": last_output,
                    })
                };
                if !self.eval_bool(condition, &condition_ctx) {
                    break;
                }
            }

            iteration += 1;

            // Log iteration event via the engine's shared emit helper.
            self.emit_loop_iteration_event(execution_id, node_id, iteration, max_iters);

            // Dispatch the body node's module.
            let fetch_result = self
                .fetch_module(body_uuid)
                .await
                .map_err(|e| anyhow::anyhow!(e));

            let wasm_module = match fetch_result {
                Ok(m) => m,
                Err(e) => {
                    last_output = serde_json::json!({
                        "__error": true,
                        "error_message": format!("Module fetch failed: {e}"),
                    });
                    break;
                }
            };

            // Flat-merge input + config (same pattern as regular node dispatch).
            let mut merged_input = serde_json::Map::new();
            if let Some(obj) = current_input.as_object() {
                for (k, v) in obj {
                    merged_input.insert(k.clone(), v.clone());
                }
            }
            if let Some(cfg) = self.node_configs.get(&body_uuid) {
                if cfg.is_object()
                    && !cfg.as_object().map(|m| m.is_empty()).unwrap_or(true)
                {
                    merged_input.insert("config".to_string(), cfg.clone());
                    if let Some(obj) = cfg.as_object() {
                        for (k, v) in obj {
                            merged_input
                                .entry(k.clone())
                                .or_insert(v.clone());
                        }
                    }
                }
            }
            if !current_input.is_null() && current_input != serde_json::json!({}) {
                merged_input
                    .entry("input".to_string())
                    .or_insert(current_input.clone());
            }
            if let Some(ref ti) = trigger_input_val {
                merged_input.insert("__trigger_input__".to_string(), ti.clone());
            }
            merged_input
                .entry("iteration_count".to_string())
                .or_insert(serde_json::json!(iteration));
            merged_input
                .entry("iteration".to_string())
                .or_insert(serde_json::json!(iteration));
            let job_input = serde_json::Value::Object(merged_input);

            let body_timeout_secs = self
                .node_timeout_for(body_uuid)
                .unwrap_or(30);
            let encrypted_secrets = self
                .build_encrypted_secrets(body_module_id, worker_shared_key)
                .await;
            let body_job = DispatchJob {
                execution_id,
                node_id: body_uuid,
                module_id: body_module_id,
                // Loop-body iterations don't pre-INSERT
                // `module_executions` rows; let the adapter mint a
                // fresh `job_id`.
                job_id: None,
                user_id: self.user_id(),
                actor_id: self.actor_id(),
                module_uri: wasm_module
                    .oci_url
                    .clone()
                    .unwrap_or_else(|| format!("redis:wasm:{body_module_id}")),
                wasm_bytes: None,
                expected_wasm_hash: Some(wasm_module.content_hash.clone()),
                capability_world: Some(wasm_module.capability_world.clone()),
                integration_name: wasm_module.integration_name.clone(),
                input_payload: job_input,
                timeout: std::time::Duration::from_secs(body_timeout_secs),
                max_fuel: wasm_module.max_fuel.min(50_000_000),
                allowed_hosts: wasm_module.allowed_hosts.clone(),
                allowed_methods: wasm_module.allowed_methods.clone(),
                allowed_secrets: wasm_module.allowed_secrets.clone(),
                allowed_sql_operations: vec![],
                allow_tier2_exposure: false,
                encrypted_secrets_ciphertext: encrypted_secrets.ciphertext,
                encrypted_secrets_nonce: encrypted_secrets.nonce,
                priority: 100,
                dry_run: self.dry_run,
                max_retries: 2,
                backoff_ms: 500,
                retry_condition: None,
                retry_delay_expr: None,
                // Retries inside a loop iteration are internal and
                // should not inflate workflow-level retry metrics.
                emit_retry_events: false,
            };

            match dispatcher.dispatch(body_job).await {
                Ok(result) => {
                    // Unwrap the engine envelope so the next iteration
                    // receives clean output, not double-wrapped input.
                    let clean = Self::unwrap_output(&result.output).clone();
                    last_output = clean.clone();
                    current_input = clean;
                }
                Err(e) => {
                    last_output = serde_json::json!({
                        "__error": true,
                        "error_message": e.to_string(),
                    });
                    break;
                }
            }
        }

        if iteration >= max_iters {
            tracing::warn!(
                %node_id,
                max_iterations = max_iters,
                "Loop reached maximum iterations"
            );
        }

        serde_json::json!({
            "iterations": iteration,
            "output": last_output,
        })
    }

    /// [`SystemNodeKind::ConfidenceGate`] — evaluate the confidence
    /// signal and either emit a normal output or pause the workflow
    /// for approval.
    ///
    /// Returns `None` when the node is not a `ConfidenceGate`;
    /// otherwise a [`ConfidenceGateOutcome`] telling the caller
    /// whether to proceed or pause the scheduler.
    #[cfg(feature = "llm-primitives")]
    pub(crate) async fn try_dispatch_confidence_gate(
        &self,
        node_idx: NodeIndex,
        node_id: Uuid,
        execution_id: Uuid,
        results: &HashMap<Uuid, JsonValue>,
    ) -> Option<ConfidenceGateOutcome> {
        let (
            _,
            _,
            Some(SystemNodeKind::ConfidenceGate {
                threshold,
                confidence_path,
                on_low_confidence,
            }),
        ) = self.node_meta.get(&node_id)?
        else {
            return None;
        };
        match self
            .evaluate_confidence_gate(
                node_idx,
                results,
                execution_id,
                *threshold,
                confidence_path,
                on_low_confidence,
            )
            .await
        {
            Ok(gate_result) => Some(ConfidenceGateOutcome::Proceed(gate_result)),
            Err(waiting_json) => Some(ConfidenceGateOutcome::Pause {
                waiting_output: waiting_json,
            }),
        }
    }

    /// [`SystemNodeKind::Ensemble`] — run N copies of a child
    /// workflow and consolidate the outputs via a consensus strategy.
    #[cfg(feature = "llm-primitives")]
    pub(crate) async fn try_dispatch_ensemble(
        &self,
        node_idx: NodeIndex,
        node_id: Uuid,
        dispatcher: &Arc<dyn NodeDispatcher>,
        worker_shared_key: &Option<WorkerSharedKey>,
        results: &HashMap<Uuid, JsonValue>,
    ) -> Option<JsonValue> {
        let (
            _,
            _,
            Some(SystemNodeKind::Ensemble {
                child_workflow_id,
                count,
                consensus,
                judge_workflow_id,
                timeout_secs: _,
            }),
        ) = self.node_meta.get(&node_id)?
        else {
            return None;
        };
        let child_wf_id = *child_workflow_id;
        let run_count = *count;
        let consensus_strategy = consensus.clone();
        let judge_wf_id_opt = *judge_workflow_id;
        let inputs = self.gather_inputs(node_idx, results);

        Some(
            self.dispatch_ensemble(
                inputs,
                child_wf_id,
                run_count,
                consensus_strategy,
                judge_wf_id_opt,
                dispatcher.clone(),
                worker_shared_key.clone(),
            )
            .await,
        )
    }
}

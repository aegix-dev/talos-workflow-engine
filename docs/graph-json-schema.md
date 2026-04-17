# `graph_json` schema (v0)

This is the wire shape the engine accepts at
[`ParallelWorkflowEngine::load_from_graph_json`][load-fn] and
returns from [`WorkflowGraphStore::get_graph`][store-trait]. The
shape is derived from React Flow's node/edge model so workflows
authored in a visual editor can be loaded directly.

Pre-1.0, the shape is **additive-only**: new optional fields will
land in 0.x minor bumps; removing or re-typing an existing field
bumps the major. A typed Rust builder (`WorkflowGraphBuilder`) is
planned for 0.2.

[load-fn]: https://docs.rs/talos-workflow-engine/0.1/talos_workflow_engine/struct.ParallelWorkflowEngine.html#method.load_from_graph_json
[store-trait]: https://docs.rs/talos-workflow-engine-core/0.1/talos_workflow_engine_core/trait.WorkflowGraphStore.html

## Top-level shape

```jsonc
{
  "nodes": [ /* … node objects (see below) */ ],
  "edges": [ /* … edge objects (see below) */ ],

  // Optional overall cap; defaults to 300.
  "execution_timeout_secs": 300
}
```

Unknown top-level keys are ignored.

## Node object

```jsonc
{
  // Required. The engine parses this into the internal `Uuid` node
  // id. Any valid UUID v4 string works.
  "id": "550e8400-e29b-41d4-a716-446655440000",

  // Optional. When `type` is NOT a system-node kind string (see
  // below), the engine treats `type` as the module id that executes
  // at this node. Accepts either a bare UUID or the form
  // `module::<uuid>`.
  "type": "c8a7d9e4-…",

  // System-node kinds that the engine routes through built-in
  // handlers instead of dispatching to a module.
  //
  //   "foreach"       | "wait"            | "sub_workflow"   | "loop"
  //   "collect"       | "synthesize"      | "verify"         |
  //   "dispatch"      | "capability_dispatch"
  //
  // LLM-flavored kinds (gated by the `llm-primitives` feature, on by
  // default):
  //
  //   "agent_loop"    | "react_loop"      | "judge"          |
  //   "ensemble"     | "confidence_gate" | "reflective_retry"|
  //   "llm_dispatch"
  //
  // Consumers with `llm-primitives` disabled see these kinds parsed
  // to `None` and the node is rejected at dispatch time.
  "kind": "foreach",

  // Optional. Free-form per-kind configuration. Shape depends on
  // `kind`. See "Per-kind `data`" below.
  "data": { /* … */ },

  // Optional per-node retry policy. Merged with the workflow-level
  // default if both are present.
  "retry_count":        2,
  "retry_backoff_ms":   500,
  "retry_condition":    "error_code == 429",     // expression
  "retry_delay_expression": "min(5000, base * 2)",

  // Optional control-flow hints. The engine stores these as
  // `__skip_condition` / `__continue_on_error` reserved keys on the
  // node's config; see `talos_workflow_engine_core::reserved_keys`.
  "skip_condition":       "upstream.skip",
  "continue_on_error":    true
}
```

Nodes whose `type` is NOT a resolvable module id (not a UUID, no
`data.moduleId` fallback) and whose `kind` is also absent are
silently skipped — the engine treats them as presentation-only
annotations, matching the React Flow frontend's behavior.

## Edge object

```jsonc
{
  // Required. The engine parses `source` / `target` as either the
  // node's UUID or a user-friendly label that matches some node's
  // `id`/label.
  "source": "n1",
  "target": "n2",

  // Optional. The engine uses this to distinguish output handles
  // for nodes that produce multiple outputs (e.g. `on_failure` /
  // `on_success`). Defaults to the source node's primary handle.
  "sourceHandle": "on_failure",

  // Optional. The engine uses this to match to a specific input
  // handle on the target.
  "targetHandle": "error",

  // Optional edge logic. Controls whether the edge fires based on
  // the source output. Defaults to `always`.
  //
  //   "always"                   — fire unconditionally
  //   {"condition": "expr"}     — fire when `expr` is truthy
  //   {"not_condition": "expr"} — fire when `expr` is falsy
  "logic": { "condition": "ok == true" }
}
```

## Per-kind `data` — selected shapes

The engine ignores unknown `data` keys; the shapes below document
the load-bearing subset. All numeric fields clamp at documented
bounds (e.g. `max_iterations` caps at 50 for agent loops).

### `foreach`
```jsonc
{ "input_path": "items", "output_handle": "element" }
```

### `wait`
```jsonc
{ "message": "Human approval required" }
```

### `sub_workflow`
```jsonc
{ "sub_workflow_id": "uuid", "timeout_secs": 30 }
```

### `loop`
```jsonc
{ "max_iterations": 10, "condition": "iteration < 5" }
```

### `verify`
```jsonc
{ "condition": "response.status == 200",
  "check_label": "http_ok",
  "on_failure": "error" }
```

### `judge` *(llm-primitives)*
```jsonc
{ "judge_workflow_id": "uuid",
  "rubric": "rate helpfulness 0-1",
  "pass_threshold": 0.7,
  "timeout_secs": 60 }
```

### `ensemble` *(llm-primitives)*
```jsonc
{ "child_workflow_id": "uuid",
  "count": 3,
  "consensus": "majority_vote",
  "judge_workflow_id": "uuid",
  "timeout_secs": 60 }
```

### `confidence_gate` *(llm-primitives)*
```jsonc
{ "threshold": 0.7,
  "confidence_path": "__confidence__",
  "on_low_confidence": "pause" }
```

### `llm_dispatch` *(llm-primitives)*
```jsonc
{ "classifier_workflow_id": "uuid",
  "routes": { "support": "uuid", "billing": "uuid" },
  "fallback_workflow_id": "uuid",
  "timeout_secs": 60 }
```

### `reflective_retry` *(llm-primitives)*
```jsonc
{ "child_workflow_id": "uuid",
  "reflection_workflow_id": "uuid",
  "max_retries": 2,
  "timeout_secs": 60 }
```

### `agent_loop` / `react_loop` *(llm-primitives)*
```jsonc
{ "body_workflow_id": "uuid",
  "max_iterations": 10,
  "inject_history": true,
  "timeout_secs": 60 }
```

### `dispatch`
```jsonc
{ "dispatch_expression": "classifier.output.route",
  "timeout_secs": 30 }
```

### `capability_dispatch`
```jsonc
{ "required_capabilities": ["llm", "rag"], "timeout_secs": 30 }
```

## Reserved `__`-prefixed keys

The engine reads and writes a set of reserved keys on node input and
output payloads. See
[`talos_workflow_engine_core::reserved_keys`](https://docs.rs/talos-workflow-engine-core/0.1/talos_workflow_engine_core/reserved_keys/index.html)
for the authoritative list. Consumer-authored module output must
not shadow these — the engine strips them from user-visible output
where documented, and reading them back has undefined results.

## Versioning

The workspace is pre-1.0. The schema above is v0. Until 1.0:

* New optional fields are backwards-compatible.
* New system-node kinds are backwards-compatible (unknown kinds
  parse to `None` and are rejected at dispatch).
* Removing or changing a field's type bumps the major version.
* New LLM-flavored kinds ship behind the `llm-primitives` feature.

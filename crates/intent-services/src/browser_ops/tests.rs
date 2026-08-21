//! Unit tests for the `browser.exec` envelope parser + result shaper.

use super::*;
use serde_json::json;

fn params_of(v: Value) -> Map<String, Value> {
    v.as_object().cloned().unwrap_or_default()
}

#[test]
fn parse_args_requires_actions() {
    let err = parse_args(&Map::new()).expect_err("actions is required");
    assert_eq!(err.code, INVALID_PARAMS);
    assert!(err.message.contains("actions"));
}

#[test]
fn parse_args_rejects_non_array_actions() {
    let err = parse_args(&params_of(json!({ "actions": "not-an-array" })))
        .expect_err("actions must be an array");
    assert_eq!(err.code, INVALID_PARAMS);
    assert!(err.message.contains("array"));
}

#[test]
fn parse_args_rejects_empty_actions() {
    let err =
        parse_args(&params_of(json!({ "actions": [] }))).expect_err("actions must be non-empty");
    assert_eq!(err.code, INVALID_PARAMS);
    assert!(err.message.contains("empty"));
}

#[test]
fn parse_args_rejects_non_string_tab_id() {
    let err = parse_args(&params_of(json!({
        "actions": [{ "action": "listTabs" }],
        "tabId": 42,
    })))
    .expect_err("tabId must be a string");
    assert_eq!(err.code, INVALID_PARAMS);
    assert!(err.message.contains("tabId"));
}

#[test]
fn parse_args_rejects_non_string_agent_id() {
    let err = parse_args(&params_of(json!({
        "actions": [{ "action": "listTabs" }],
        "agentId": 42,
    })))
    .expect_err("agentId must be a string");
    assert_eq!(err.code, INVALID_PARAMS);
    assert!(err.message.contains("agentId"));
}

#[test]
fn parse_args_rejects_non_string_workspace_id() {
    let err = parse_args(&params_of(json!({
        "actions": [{ "action": "listTabs" }],
        "workspaceId": { "id": "ws-1" },
    })))
    .expect_err("workspaceId must be a string");
    assert_eq!(err.code, INVALID_PARAMS);
    assert!(err.message.contains("workspaceId"));
}

#[test]
fn parse_args_accepts_full_envelope() {
    let args = parse_args(&params_of(json!({
        "actions": [{ "action": "listTabs" }, { "action": "screenshot" }],
        "tabId": "tab-1",
        "agentId": "agent-1",
        "workspaceId": "ws-1",
    })))
    .expect("valid params");
    assert_eq!(args.actions.len(), 2);
    assert_eq!(args.tab_id.as_deref(), Some("tab-1"));
    assert_eq!(args.agent_id.as_deref(), Some("agent-1"));
    assert_eq!(args.workspace_id.as_deref(), Some("ws-1"));
}

#[test]
fn parse_args_trims_empty_optional_strings_to_none() {
    let args = parse_args(&params_of(json!({
        "actions": [{ "action": "listTabs" }],
        "tabId": "   ",
        "agentId": null,
    })))
    .expect("valid params");
    assert_eq!(args.tab_id, None);
    assert_eq!(args.agent_id, None);
    assert_eq!(args.workspace_id, None);
}

#[test]
fn parse_args_and_forward_pass_new_actions_through_verbatim() {
    // The daemon is a thin proxy: action shapes are opaque, so the
    // hidden-by-default `openTab { visible }` and the `showTab { tabId,
    // focus }` reveal action (monorepo#3045) forward to the FE executor
    // byte-for-byte. Per-action validation (e.g. showTab without tabId)
    // is FE-owned — the daemon must not reject such a batch.
    let actions = vec![
        json!({ "action": "openTab", "url": "http://localhost:5173", "visible": true }),
        json!({ "action": "showTab", "tabId": "tab-1", "focus": false }),
        json!({ "action": "showTab" }),
    ];
    let args = parse_args(&params_of(json!({ "actions": actions.clone() })))
        .expect("opaque action shapes pass envelope validation");
    assert_eq!(args.actions, actions);
    let forwarded = build_forward_params(&args);
    assert_eq!(forwarded["actions"], Value::Array(actions));
}

#[test]
fn build_forward_params_includes_only_supplied_fields() {
    let args = BrowserExecArgs {
        actions: vec![json!({ "action": "listTabs" })],
        tab_id: None,
        agent_id: Some("agent-1".to_string()),
        workspace_id: None,
    };
    let forwarded = build_forward_params(&args);
    let obj = forwarded.as_object().unwrap();
    assert!(obj.contains_key("actions"));
    assert!(!obj.contains_key("tabId"));
    assert_eq!(obj.get("agentId").and_then(Value::as_str), Some("agent-1"));
    assert!(!obj.contains_key("workspaceId"));
}

#[test]
fn shape_result_single_action_returns_action_envelope() {
    let fe = json!({
        "success": true,
        "results": [
            { "action": "listTabs", "success": true, "result": [{ "id": "tab-1" }] }
        ]
    });
    let shaped = shape_result(fe).expect("single action shapes to one result");
    assert_eq!(shaped["action"], "listTabs");
    assert_eq!(shaped["success"], true);
    assert_eq!(shaped["result"][0]["id"], "tab-1");
}

#[test]
fn shape_result_multiple_actions_returns_results_array() {
    let fe = json!({
        "success": true,
        "results": [
            { "action": "listTabs", "success": true, "result": [] },
            { "action": "screenshot", "success": true, "result": { "base64": "..." } }
        ]
    });
    let shaped = shape_result(fe).expect("multiple actions shape to results[]");
    let arr = shaped["results"].as_array().expect("results is an array");
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["action"], "listTabs");
    assert_eq!(arr[1]["action"], "screenshot");
}

#[test]
fn shape_result_failure_envelope_surfaces_error_context() {
    let fe = json!({
        "success": false,
        "error": "CDP not attached",
        "results": []
    });
    let err = shape_result(fe).expect_err("failure envelope surfaces as error");
    assert_eq!(err.code, INTERNAL_ERROR);
    assert!(err.message.contains("CDP not attached"));
}

#[test]
fn shape_result_missing_results_is_internal_error() {
    let fe = json!({ "success": true });
    let err = shape_result(fe).expect_err("missing results is internal error");
    assert_eq!(err.code, INTERNAL_ERROR);
    assert!(err.message.contains("results"));
}

#[test]
fn shape_result_non_object_is_internal_error() {
    let err = shape_result(json!("nope")).expect_err("non-object is internal");
    assert_eq!(err.code, INTERNAL_ERROR);
}

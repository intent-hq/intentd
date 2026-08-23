//! Unit tests for the `browser.exec` envelope parser + result shaper.

use super::*;
use serde_json::json;

fn params_of(v: &Value) -> Map<String, Value> {
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
    let err = parse_args(&params_of(&json!({ "actions": "not-an-array" })))
        .expect_err("actions must be an array");
    assert_eq!(err.code, INVALID_PARAMS);
    assert!(err.message.contains("array"));
}

#[test]
fn parse_args_rejects_empty_actions() {
    let err =
        parse_args(&params_of(&json!({ "actions": [] }))).expect_err("actions must be non-empty");
    assert_eq!(err.code, INVALID_PARAMS);
    assert!(err.message.contains("empty"));
}

#[test]
fn parse_args_rejects_non_string_tab_id() {
    let err = parse_args(&params_of(&json!({
        "actions": [{ "action": "listTabs" }],
        "tabId": 42,
    })))
    .expect_err("tabId must be a string");
    assert_eq!(err.code, INVALID_PARAMS);
    assert!(err.message.contains("tabId"));
}

#[test]
fn parse_args_rejects_non_string_agent_id() {
    let err = parse_args(&params_of(&json!({
        "actions": [{ "action": "listTabs" }],
        "agentId": 42,
    })))
    .expect_err("agentId must be a string");
    assert_eq!(err.code, INVALID_PARAMS);
    assert!(err.message.contains("agentId"));
}

#[test]
fn parse_args_rejects_non_string_workspace_id() {
    let err = parse_args(&params_of(&json!({
        "actions": [{ "action": "listTabs" }],
        "workspaceId": { "id": "ws-1" },
    })))
    .expect_err("workspaceId must be a string");
    assert_eq!(err.code, INVALID_PARAMS);
    assert!(err.message.contains("workspaceId"));
}

#[test]
fn parse_args_accepts_full_envelope() {
    let args = parse_args(&params_of(&json!({
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
    let args = parse_args(&params_of(&json!({
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
    let args = parse_args(&params_of(&json!({ "actions": actions.clone() })))
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
    let shaped = shape_result(&fe).expect("single action shapes to one result");
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
    let shaped = shape_result(&fe).expect("multiple actions shape to results[]");
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
    let err = shape_result(&fe).expect_err("failure envelope surfaces as error");
    assert_eq!(err.code, INTERNAL_ERROR);
    assert!(err.message.contains("CDP not attached"));
}

#[test]
fn shape_result_missing_results_is_internal_error() {
    let fe = json!({ "success": true });
    let err = shape_result(&fe).expect_err("missing results is internal error");
    assert_eq!(err.code, INTERNAL_ERROR);
    assert!(err.message.contains("results"));
}

#[test]
fn shape_result_non_object_is_internal_error() {
    let err = shape_result(&json!("nope")).expect_err("non-object is internal");
    assert_eq!(err.code, INTERNAL_ERROR);
}

// ================================================================
// shape_agent_result — the agent-JS (`ws.browser.exec`) shaping.
// Regression coverage for monorepo#3042: structured per-action
// errors (not-owner / already-claimed) must survive a
// `success: false` envelope instead of flattening into prose.
// ================================================================

#[test]
fn shape_agent_result_success_single_action_matches_shape_result() {
    let fe = json!({
        "success": true,
        "results": [
            { "action": "listTabs", "success": true, "result": [{ "id": "tab-1" }] }
        ]
    });
    let shaped = shape_agent_result(&fe.clone(), 1).expect("single action shapes to one result");
    assert_eq!(shaped, shape_result(&fe).unwrap());
}

#[test]
fn shape_agent_result_success_multi_action_matches_shape_result() {
    let fe = json!({
        "success": true,
        "results": [
            { "action": "listTabs", "success": true, "result": [] },
            { "action": "screenshot", "success": true, "result": { "base64": "..." } }
        ]
    });
    let shaped = shape_agent_result(&fe.clone(), 2).expect("multi action shapes to results[]");
    assert_eq!(shaped, shape_result(&fe).unwrap());
}

#[test]
fn shape_agent_result_preserves_single_action_not_owner_failure() {
    let fe = json!({
        "success": false,
        "error": "Tab tab-9 is not owned by you",
        "results": [{
            "action": "resizeTab",
            "success": false,
            "errorCode": "not-owner",
            "ownerAgentId": null,
            "error": "Tab tab-9 is not owned by you (owner: none). Claim it with claimTab first."
        }]
    });
    let shaped =
        shape_agent_result(&fe, 1).expect("ownership failure stays structured, not an error");
    assert_eq!(shaped["action"], "resizeTab");
    assert_eq!(shaped["success"], json!(false));
    assert_eq!(shaped["errorCode"], "not-owner");
    assert_eq!(shaped["ownerAgentId"], Value::Null);
    assert!(shaped["error"].as_str().unwrap().contains("not owned"));
}

#[test]
fn shape_agent_result_preserves_single_action_already_claimed_failure() {
    let fe = json!({
        "success": false,
        "error": "Tab tab-3 is owned by agent agent-42",
        "results": [{
            "action": "claimTab",
            "success": false,
            "errorCode": "already-claimed",
            "ownerAgentId": "agent-42",
            "error": "Tab tab-3 is owned by agent agent-42"
        }]
    });
    let shaped = shape_agent_result(&fe, 1).expect("claim loss stays structured");
    assert_eq!(shaped["action"], "claimTab");
    assert_eq!(shaped["errorCode"], "already-claimed");
    assert_eq!(shaped["ownerAgentId"], "agent-42");
}

#[test]
fn shape_agent_result_preserves_multi_action_partial_failure_envelope() {
    let fe = json!({
        "success": false,
        "error": "1 of 2 actions failed",
        "results": [
            { "action": "listTabs", "success": true, "result": [] },
            {
                "action": "resizeTab",
                "success": false,
                "errorCode": "not-owner",
                "ownerAgentId": "agent-7",
                "error": "Tab tab-2 is owned by agent agent-7"
            }
        ]
    });
    let shaped = shape_agent_result(&fe, 2).expect("partial failure keeps per-action results");
    assert_eq!(shaped["success"], json!(false));
    assert_eq!(shaped["error"], "1 of 2 actions failed");
    let arr = shaped["results"].as_array().expect("results preserved");
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["success"], json!(true));
    assert_eq!(arr[1]["errorCode"], "not-owner");
    assert_eq!(arr[1]["ownerAgentId"], "agent-7");
}

#[test]
fn shape_agent_result_multi_action_first_failure_keeps_envelope_shape() {
    // The FE aborts a batch on the first failing action and returns the
    // partial results collected so far: a 3-action batch failing at action 1
    // comes back with results.len() == 1. The shape must key off the
    // *request* arity, so a multi-action caller still gets the
    // `{ success, results, error }` envelope, never a bare action envelope.
    let fe = json!({
        "success": false,
        "error": "Tab tab-9 is not owned by you",
        "results": [{
            "action": "resizeTab",
            "success": false,
            "errorCode": "not-owner",
            "ownerAgentId": "agent-7",
            "error": "Tab tab-9 is owned by agent agent-7"
        }]
    });
    let shaped = shape_agent_result(&fe, 3).expect("aborted batch keeps envelope shape");
    assert_eq!(shaped["success"], json!(false));
    assert_eq!(shaped["error"], "Tab tab-9 is not owned by you");
    let arr = shaped["results"].as_array().expect("results key present");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["errorCode"], "not-owner");
    assert_eq!(arr[0]["ownerAgentId"], "agent-7");
}

#[test]
fn shape_agent_result_failure_without_results_is_internal_error() {
    let fe = json!({ "success": false, "error": "CDP not attached" });
    let err = shape_agent_result(&fe, 1).expect_err("no per-action detail to preserve");
    assert_eq!(err.code, INTERNAL_ERROR);
    assert!(err.message.contains("CDP not attached"));
}

#[test]
fn shape_agent_result_failure_with_empty_results_is_internal_error() {
    let fe = json!({ "success": false, "error": "CDP not attached", "results": [] });
    let err = shape_agent_result(&fe, 1).expect_err("empty results has nothing to preserve");
    assert_eq!(err.code, INTERNAL_ERROR);
    assert!(err.message.contains("CDP not attached"));
}

#[test]
fn shape_agent_result_failure_with_non_array_results_is_internal_error() {
    let fe = json!({ "success": false, "error": "CDP not attached", "results": "oops" });
    let err = shape_agent_result(&fe, 2).expect_err("non-array results has nothing to preserve");
    assert_eq!(err.code, INTERNAL_ERROR);
    assert!(err.message.contains("CDP not attached"));
}

#[test]
fn shape_agent_result_non_object_is_internal_error() {
    let err = shape_agent_result(&json!("nope"), 1).expect_err("non-object is internal");
    assert_eq!(err.code, INTERNAL_ERROR);
}

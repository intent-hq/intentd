//! Golden pins for the TOON text the agent-facing `workspace_api` tool emits
//! (`workspaceApi.toonOutput`, default on). Agents read this text, the
//! frontend decodes it to render tool cards (`@toon-format/toon`), and it is
//! kept in stored transcripts and `tool-outputs/*.toon` redirect files, so any
//! byte-level change from a `toon-format` bump must show up here as a reviewed
//! diff.

use super::render_workspace_api_value;
use serde_json::{json, Value};

fn cases() -> Vec<(&'static str, Value, &'static str)> {
    vec![
        (
            "flat_object",
            json!({"ok": true, "noteId": "spec", "count": 3, "ratio": 0.5, "missing": null}),
            r"ok: true
noteId: spec
count: 3
ratio: 0.5
missing: null",
        ),
        (
            "hyphenated_ids",
            json!({"agentId": "agent-9c54aaa4", "taskNoteId": "74881932-d1aa-4c27-a368-b7f0c1aa5360", "time": "2026-11-10T14:03:27Z", "branch": "chore/toon-format-0-6"}),
            r#"agentId: agent-9c54aaa4
taskNoteId: 74881932-d1aa-4c27-a368-b7f0c1aa5360
time: "2026-11-10T14:03:27Z"
branch: chore/toon-format-0-6"#,
        ),
        (
            "tabular_rows",
            json!({"agents": [{"id": "agent-1", "name": "Ada", "status": "idle"}, {"id": "agent-2", "name": "Bob", "status": "responding"}]}),
            r"agents[2]{id,name,status}:
  agent-1,Ada,idle
  agent-2,Bob,responding",
        ),
        (
            "list_rows_with_nested_object",
            json!({"tasks": [{"id": "t-1", "status": "todo", "owner": {"agentId": "agent-1"}}, {"id": "t-2", "status": "done", "owner": null}]}),
            r"tasks[2]:
  - id: t-1
    status: todo
    owner:
      agentId: agent-1
  - id: t-2
    status: done
    owner: null",
        ),
        (
            "uniform_nested_rows",
            json!({"monitors": [{"pr": 1, "checks": {"passed": 3, "failed": 0}}, {"pr": 2, "checks": {"passed": 1, "failed": 2}}]}),
            r"monitors[2]{pr,checks{passed,failed}}:
  1,3,0
  2,1,2",
        ),
        (
            "empty_collections",
            json!({"tags": [], "meta": {}, "hooks": [[], {}]}),
            r"tags: []
meta:
hooks[2]:
  - [0]:
  -",
        ),
        (
            "quoting",
            json!({"comma": "a, b", "colon": "k: v", "leadingDash": "- item", "leadingSpace": " x", "numeric": "42", "boolean": "true", "newline": "l1\nl2", "empty": "", "unicode": "café ✓", "key with space": 1}),
            r#"comma: "a, b"
colon: "k: v"
leadingDash: "- item"
leadingSpace: " x"
numeric: "42"
boolean: "true"
newline: "l1\nl2"
empty: ""
unicode: café ✓
"key with space": 1"#,
        ),
        (
            "mixed_array",
            json!({"values": [1, "two", {"three": 3}, [4, 5]]}),
            r"values[4]:
  - 1
  - two
  - three: 3
  - [2]: 4,5",
        ),
        (
            "top_level_array",
            json!([{"tabId": "tab-1", "url": "http://daemon.localhost:5173/", "mounted": true}, {"tabId": "tab-2", "url": "https://example.com/a-b", "mounted": false}]),
            r#"[2]{tabId,url,mounted}:
  tab-1,"http://daemon.localhost:5173/",true
  tab-2,"https://example.com/a-b",false"#,
        ),
        (
            "keyed_object_field",
            json!({"results": {"a": {"ok": true, "noteId": "x"}, "b": {"ok": true, "noteId": "y"}}}),
            r"results[2:]{ok,noteId}:
  a: true,x
  b: true,y",
        ),
        (
            "keyed_object_root",
            json!({"spec": {"ok": true, "lines": 10}, "plan": {"ok": false, "lines": 3}}),
            r"[2:]{ok,lines}:
  spec: true,10
  plan: false,3",
        ),
        (
            "version_and_date_strings",
            json!({"version": "0.6.1", "date": "2026-11-10", "range": "10-20", "zero": "05"}),
            r#"version: 0.6.1
date: 2026-11-10
range: 10-20
zero: "05""#,
        ),
        (
            "leading_hash_and_plus",
            json!({"title": "# Heading", "delta": "+1"}),
            r##"title: "# Heading"
delta: "+1""##,
        ),
        (
            "control_characters",
            json!({"exitCode": 0, "output": "\u{1b}[31mred\u{1b}[0m"}),
            r#"exitCode: 0
output: "\u001b[31mred\u001b[0m""#,
        ),
        (
            "non_ascii_key",
            json!({"café": 1, "plain": 2}),
            r#""café": 1
plain: 2"#,
        ),
        (
            "empty_array_list_element",
            json!({"rows": [[], [1]]}),
            r"rows[2]:
  - [0]:
  - [1]: 1",
        ),
    ]
}

#[test]
fn toon_output_matches_golden() {
    for (name, value, expected) in cases() {
        let (body, ext) = render_workspace_api_value(&value, true);
        assert_eq!(ext, "toon", "{name}");
        assert_eq!(body, expected, "{name}: TOON output changed");
    }
}

/// Self-consistency with the same crate's decoder only; the frontend decodes
/// with `@toon-format/toon`, which this test does not exercise.
#[test]
fn toon_output_round_trips_to_the_same_json() {
    for (name, value, _) in cases() {
        let (body, _) = render_workspace_api_value(&value, true);
        let decoded: Value = toon_format::decode_default(&body)
            .unwrap_or_else(|e| panic!("{name}: emitted TOON does not decode: {e}\n{body}"));
        assert_eq!(decoded, value, "{name}: round trip changed the value");
    }
}

#[test]
fn toon_disabled_and_scalar_results_stay_pretty_json() {
    let value = json!({"agentId": "agent-1", "tags": []});
    assert_eq!(
        render_workspace_api_value(&value, false),
        (
            "{\n  \"agentId\": \"agent-1\",\n  \"tags\": []\n}".to_owned(),
            "json"
        )
    );
    assert_eq!(
        render_workspace_api_value(&json!("agent-1"), true),
        ("\"agent-1\"".to_owned(), "txt")
    );
}

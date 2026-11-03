//! Catalog list paging: unchanged replies without paging params, pages that
//! fit the budget, and `nextToken` walks without gaps.

use super::*;

impl std::fmt::Debug for RpcErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RpcErr({}, {})", self.code, self.message)
    }
}

fn params(v: &Value) -> Map<String, Value> {
    v.as_object().cloned().unwrap_or_default()
}

fn ws_id() -> intent_core::WorkspaceId {
    intent_core::WorkspaceId::from("ws-1")
}

fn frame_len(v: &Value) -> usize {
    serialized_len(v)
}

fn specialists(n: usize, body: usize) -> Value {
    let rows: Vec<Value> = (0..n)
        .map(|i| json!({ "id": format!("s-{i:03}"), "name": "S", "behaviorPrompt": "p".repeat(body) }))
        .collect();
    json!({ "specialists": rows, "importDiagnostics": [{ "path": "bad.md" }] })
}

fn skills(n: usize) -> Value {
    Value::Array(
        (0..n)
            .map(|i| json!({ "name": format!("skill-{i:04}"), "description": "d".repeat(1_000) }))
            .collect(),
    )
}

fn rules(n: usize, body: usize) -> Value {
    let rows: Vec<Value> = (0..n)
        .map(|i| json!({ "source": format!(".intent/rules/rule-{i:02}.md"), "content": "r".repeat(body) }))
        .collect();
    json!({ "rules": { "workspaceId": "ws-1", "rules": rows } })
}

/// Follow `nextToken` from `first` to the end, collecting row ids.
fn walk(
    first: Map<String, Value>,
    mut call: impl FnMut(&Map<String, Value>) -> Value,
    rows_at: &str,
    id_key: &str,
) -> Vec<String> {
    let mut ids = Vec::new();
    let mut p = first;
    for _ in 0..1_000 {
        let page = call(&p);
        assert!(
            frame_len(&page) <= CATALOG_PAGE_BYTES,
            "page fits the budget"
        );
        ids.extend(
            page.pointer(rows_at)
                .and_then(Value::as_array)
                .unwrap()
                .iter()
                .map(|r| r[id_key].as_str().unwrap().to_owned()),
        );
        match page.get("nextToken").and_then(Value::as_str) {
            Some(t) => {
                p.insert("nextToken".into(), json!(t));
            }
            None => return ids,
        }
    }
    panic!("paging did not terminate");
}

#[test]
fn small_specialist_catalog_is_one_page() {
    let page = page_specialist_list(specialists(3, 10), &params(&json!({ "limit": 500 }))).unwrap();
    assert_eq!(page["specialists"].as_array().unwrap().len(), 3);
    assert_eq!(page["specialists"][0]["behaviorPrompt"], "p".repeat(10));
    assert_eq!(page["importDiagnostics"][0]["path"], "bad.md");
    assert_eq!(page["nextToken"], Value::Null);
}

#[test]
fn large_specialist_catalog_pages_under_budget_without_gaps() {
    let ids = walk(
        params(&json!({ "limit": 500 })),
        |p| page_specialist_list(specialists(150, 40 * 1024), p).unwrap(),
        "/specialists",
        "id",
    );
    let expected: Vec<String> = (0..150).map(|i| format!("s-{i:03}")).collect();
    assert_eq!(ids, expected);
}

#[test]
fn defaults_are_the_first_page_in_the_existing_shape() {
    let page = page_specialist_list(specialists(150, 40 * 1024), &Map::new()).unwrap();
    assert!(page["nextToken"].is_string());
    assert!(page["specialists"].as_array().unwrap().len() < 150);
    assert!(frame_len(&page) <= CATALOG_PAGE_BYTES);
    let page = page_rules_list(rules(60, 20 * 1024), &Map::new()).unwrap();
    assert!(page["nextToken"].is_string());
    assert!(frame_len(&page) <= CATALOG_PAGE_BYTES);
    let ws = params(&json!({ "workspaceId": "ws-1" }));
    let page = page_skill_list(skills(1_100), &ws, &ws_id()).unwrap();
    let rows = page.as_array().expect("bare array stays a bare array");
    assert!(!rows.is_empty() && rows.len() < 1_100);
    assert!(frame_len(&page) <= CATALOG_PAGE_BYTES);
    let small = page_skill_list(skills(3), &ws, &ws_id()).unwrap();
    assert_eq!(small, skills(3), "a catalog that fits is returned whole");
}

#[test]
fn import_diagnostics_ride_the_first_specialist_page_only() {
    let first = page_specialist_list(
        specialists(150, 40 * 1024),
        &params(&json!({ "limit": 500 })),
    )
    .unwrap();
    let token = first["nextToken"].as_str().expect("more pages").to_owned();
    let second = page_specialist_list(
        specialists(150, 40 * 1024),
        &params(&json!({ "nextToken": token })),
    )
    .unwrap();
    assert!(first.get("importDiagnostics").is_some());
    assert!(second.get("importDiagnostics").is_none());
}

#[test]
fn oversized_specialist_prompt_is_omitted_from_a_page() {
    let page = page_specialist_list(
        specialists(1, CATALOG_PAGE_BYTES + 1),
        &params(&json!({ "limit": 1 })),
    )
    .unwrap();
    let row = &page["specialists"][0];
    assert!(row.get("behaviorPrompt").is_none());
    assert_eq!(row["behaviorPromptOmitted"], true);
    assert_eq!(row["behaviorPromptBytes"], CATALOG_PAGE_BYTES + 1);
    assert!(frame_len(&page) <= CATALOG_PAGE_BYTES);
}

#[test]
fn limit_clamps_into_one_to_the_row_maximum() {
    let many = specialists(CATALOG_PAGE_MAX_ROWS + 10, 0);
    let page = page_specialist_list(many.clone(), &params(&json!({ "limit": 100_000 }))).unwrap();
    assert_eq!(
        page["specialists"].as_array().unwrap().len(),
        CATALOG_PAGE_MAX_ROWS
    );
    let page = page_specialist_list(many, &params(&json!({ "limit": 0 }))).unwrap();
    assert_eq!(page["specialists"].as_array().unwrap().len(), 1);
}

#[test]
fn rules_list_pages_the_rule_set() {
    let set = rules(60, 20 * 1024);
    let first = page_rules_list(set.clone(), &params(&json!({ "limit": 500 }))).unwrap();
    assert_eq!(first["rules"]["workspaceId"], "ws-1");
    assert!(first["nextToken"].is_string());
    let ids = walk(
        params(&json!({ "limit": 500 })),
        |p| page_rules_list(set.clone(), p).unwrap(),
        "/rules/rules",
        "source",
    );
    let expected: Vec<String> = (0..60)
        .map(|i| format!(".intent/rules/rule-{i:02}.md"))
        .collect();
    assert_eq!(ids, expected);
}

#[test]
fn skill_list_paging_params_return_the_envelope_and_walk_every_skill() {
    let ids = walk(
        params(&json!({ "workspaceId": "ws-1", "limit": 500 })),
        |p| page_skill_list(skills(1_100), p, &ws_id()).unwrap(),
        "/skills",
        "name",
    );
    let expected: Vec<String> = (0..1_100).map(|i| format!("skill-{i:04}")).collect();
    assert_eq!(ids, expected);
    let end = page_skill_list(skills(2), &params(&json!({ "limit": 10 })), &ws_id()).unwrap();
    assert_eq!(end["nextToken"], Value::Null);
    assert_eq!(end["skills"].as_array().unwrap().len(), 2);
}

#[test]
fn malformed_or_mismatched_tokens_are_invalid_params() {
    let bad = page_specialist_list(specialists(3, 0), &params(&json!({ "nextToken": "nope" })));
    assert_eq!(bad.unwrap_err().code, -32602);
    let other_ws = encode_token("skill.list:ws-2", 1);
    let bad = page_skill_list(
        skills(3),
        &params(&json!({ "nextToken": other_ws })),
        &ws_id(),
    );
    assert_eq!(
        bad.unwrap_err().code,
        -32602,
        "token scoped to another workspace"
    );
    let rules_token = encode_token("rules.list:ws-1", 1);
    let bad = page_specialist_list(
        specialists(3, 0),
        &params(&json!({ "nextToken": rules_token })),
    );
    assert_eq!(bad.unwrap_err().code, -32602, "token from another method");
}

#[test]
fn a_token_past_the_end_reads_an_empty_last_page() {
    let token = encode_token("specialist.list", 99);
    let page =
        page_specialist_list(specialists(3, 0), &params(&json!({ "nextToken": token }))).unwrap();
    assert!(page["specialists"].as_array().unwrap().is_empty());
    assert_eq!(page["nextToken"], Value::Null);
}

#[test]
fn replies_without_the_catalog_array_pass_through() {
    let odd = json!({ "path": null, "provider": "fixture" });
    assert_eq!(page_specialist_list(odd.clone(), &Map::new()).unwrap(), odd);
    let no_rows = json!({ "rules": { "workspaceId": "ws-1" } });
    assert_eq!(
        page_rules_list(no_rows.clone(), &Map::new()).unwrap(),
        no_rows
    );
}

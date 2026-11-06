//! The truncated-read cut rule (`truncated_content_len`, `note_bounded_row`)
//! checked against the protocol vectors in
//! `docs/protocol/fixtures/notes/truncated-read.json` (monorepo). The vectors
//! are copied to `tests/fixtures/truncated-read.json` through the monorepo's
//! shared fixture-copy tool (`scripts/note-truncated-read-contract.mjs`): the
//! lock `tests/fixtures/truncated-read.lock.json` names the monorepo commit, CI
//! verifies the copy against it, and a lagging copy only warns there, so the
//! docs can change first. Re-export the copy instead of editing it.

use intent_core::{note_bounded_row, truncated_content_len, Note};
use serde_json::{json, Value};

const FIXTURE: &str = include_str!("fixtures/truncated-read.json");

fn fixture() -> Value {
    serde_json::from_str(FIXTURE).expect("fixture json")
}

fn note(content: &str) -> Note {
    serde_json::from_value(json!({
        "id": "n1",
        "workspaceId": "ws-1",
        "title": "Large",
        "content": content,
        "contentType": "markdown",
        "tags": ["a"],
        "isPinned": false,
        "isArchived": false,
        "isDefault": false,
        "parentId": null,
        "visibility": "workspace",
        "createdAt": "t0",
        "rev": 3,
        "updatedAt": "t1"
    }))
    .expect("note")
}

#[test]
fn fixture_advertises_the_capability_version() {
    assert_eq!(fixture()["capability"]["noteTruncatedRead"], 1);
}

#[test]
fn cut_rule_matches_every_vector() {
    let fixture = fixture();
    let cases = fixture["cases"].as_array().expect("cases");
    assert!(!cases.is_empty());
    for case in cases {
        let id = case["id"].as_str().expect("id");
        let content = case["content"].as_str().expect("content");
        let limit = usize::try_from(case["maxContentBytes"].as_u64().expect("limit")).unwrap();
        let expect = &case["expect"];

        let kept = match truncated_content_len(content, limit) {
            Some(end) => &content[..end],
            None => content,
        };
        assert_eq!(kept, expect["content"].as_str().unwrap(), "{id}: prefix");

        let row = note_bounded_row(note(content), limit);
        let mut want = serde_json::to_value(note(content)).unwrap();
        let want_map = want.as_object_mut().unwrap();
        for (key, value) in expect.as_object().unwrap() {
            want_map.insert(key.clone(), value.clone());
        }
        assert_eq!(row, want, "{id}: row");
        if expect.get("contentTruncated").is_none() {
            assert_eq!(
                serde_json::to_string(&row).unwrap(),
                serde_json::to_string(&note(content)).unwrap(),
                "{id}: a note that fits is byte-identical to the unbounded row"
            );
        } else {
            assert!(kept.len() <= limit, "{id}: within the limit");
            assert!(
                (kept.len() as u64) < expect["contentBytes"].as_u64().unwrap(),
                "{id}: a truncated prefix never passes the base guard"
            );
        }
    }
}

#[test]
fn cut_never_splits_a_character_or_a_crlf() {
    let content = "ab\r\né中😀\r\nxyz\n".repeat(7);
    for limit in 1..content.len() + 2 {
        let Some(end) = truncated_content_len(&content, limit) else {
            assert!(content.len() <= limit);
            continue;
        };
        assert!(end <= limit, "limit {limit}");
        assert!(content.is_char_boundary(end), "limit {limit}");
        let bytes = content.as_bytes();
        assert!(
            !(end > 0 && end < bytes.len() && bytes[end - 1] == b'\r' && bytes[end] == b'\n'),
            "limit {limit} split a CRLF"
        );
        if let Some(lf) = bytes[..limit].iter().rposition(|&b| b == b'\n') {
            assert_eq!(end, lf + 1, "limit {limit}: last line feed wins");
        }
    }
}

#[test]
fn exactly_at_limit_is_not_truncated() {
    let content = "中文\n😀";
    assert_eq!(truncated_content_len(content, content.len()), None);
    assert_eq!(truncated_content_len(content, content.len() - 1), Some(7));
}

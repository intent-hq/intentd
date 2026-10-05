//! Controlled already-verified Context fixtures, not Store/auth/native capture proof.
use super::*;
use crate::note_stage_repo::rendered_capture::Capture;
use intent_core::{note_page::NoteScope, note_stage::NoteStageRange};
use std::collections::{BTreeMap, BTreeSet};

fn fixture(text: &str) -> Context {
    let length = units(text);
    Context {
        operation: "retained-op".into(),
        view: "frozen-view".into(),
        payload: "b".repeat(64),
        expires: "2030-01-01T00:00:00.000Z".into(),
        length: 50.max(12 + length),
        generation: 7,
        binding: [19; 32],
        key: vec![42; 32],
        query: "STRASSE".into(),
        rendered: Some(Capture {
            text: text.into(),
            source_range: NoteStageRange {
                start: 12,
                end: 12 + length,
            },
            selected_range: NoteStageRange {
                start: 0,
                end: length,
            },
            parent: json!({"version":1,"nodeType":"paragraph","parentOrdinal":null,"nativeRange":{"from":20,"to":22+length},"attributesRef":"attrs-parent"}),
            leaf: json!({"version":2,"nodeType":"text","parentOrdinal":0,"nativeRange":{"from":21,"to":21+length},"attributesRef":"attrs-leaf","renderedText":{"textId":"uploaded-rendered","length":length,"utf8Bytes":text.len(),"sha256":"d".repeat(64)}}),
        }),
    }
}
fn query(reference: &str) -> ReceiptDetailQuery {
    ReceiptDetailQuery {
        scope: NoteScope {
            backend_id: "backend".into(),
            workspace_id: "workspace".into(),
            note_id: "note".into(),
            note_instance_id: "instance".into(),
        },
        operation_id: "11111111-1111-4111-8111-111111111111".into(),
        payload_digest: None,
        header_digest: Some("a".repeat(64)),
        kind: ReceiptDetailKind::Detail,
        reference: reference.into(),
        cursor: None,
        max_items: 2,
        max_wire_bytes: 4096,
        max_source_bytes: 4,
        operation_envelope: true,
        context_envelope: false,
        text_id: None,
        offset: None,
    }
}
fn checked(context: &Context, query: &ReceiptDetailQuery, rpc: &Value) -> Value {
    let page = read(context, query, rpc).unwrap();
    assert!(fits(&page, rpc, query.max_wire_bytes).unwrap());
    assert_eq!(page["scope"], json!(query.scope));
    assert_eq!(page["headerDigest"], json!(query.header_digest));
    assert_eq!(page["payloadDigest"], context.payload);
    assert_eq!(page["viewId"], context.view);
    assert_eq!(page["expiresAt"], context.expires);
    assert_eq!(page["sourceLength"], context.length);
    assert_eq!(page["outputKind"], "detail");
    page
}
fn directory(context: &Context, reference: &str) -> Vec<Value> {
    let mut request = query(reference);
    let mut output = Vec::new();
    let mut seen = BTreeSet::new();
    loop {
        let page = checked(context, &request, &json!("rpc\"\\\n"));
        output.extend(page["items"].as_array().unwrap().iter().cloned());
        match page["nextCursor"].as_str() {
            None => break,
            Some(cursor) => {
                assert!(seen.insert(cursor.to_owned()));
                request.cursor = Some(cursor.into());
            }
        }
    }
    output
}
fn reconstruct(context: &Context, entry: &Value, seen: &mut BTreeSet<String>) -> Value {
    let id = entry["id"].as_str().unwrap();
    assert!(seen.insert(id.to_owned()));
    match entry["type"].as_str().unwrap() {
        "object" | "array" => {
            let children = directory(context, entry["childrenRef"].as_str().unwrap());
            for child in &children {
                assert_eq!(child["parentId"], id);
            }
            if entry["type"] == "array" {
                Value::Array(
                    children
                        .iter()
                        .enumerate()
                        .map(|(i, child)| {
                            assert_eq!(child["index"], i);
                            reconstruct(context, child, seen)
                        })
                        .collect(),
                )
            } else {
                let mut last = None;
                let mut object = serde_json::Map::new();
                for child in children {
                    let key = child["key"].as_str().unwrap();
                    if let Some(previous) = last.as_deref() {
                        assert!(previous < key);
                    }
                    last = Some(key.to_owned());
                    object.insert(key.into(), reconstruct(context, &child, seen));
                }
                Value::Object(object)
            }
        }
        "string" => {
            assert!(entry.get("value").is_none());
            let mut reference = entry["valueRef"].as_str().unwrap().to_owned();
            let mut output = String::new();
            let mut refs = BTreeSet::new();
            loop {
                assert!(refs.insert(reference.clone()));
                let page = checked(context, &query(&reference), &json!("rpc\"\\\n"));
                assert!(page["nextCursor"].is_null());
                let fragment = &page["items"][0];
                assert_eq!(fragment["id"], id);
                assert_eq!(fragment["offset"], units(&output));
                assert_eq!(
                    &fragment["field"],
                    entry.get("key").unwrap_or(&json!("value"))
                );
                let text = fragment["text"].as_str().unwrap();
                assert!(text.len() <= 4);
                output.push_str(text);
                match fragment["nextRef"].as_str() {
                    None => break,
                    Some(next) => {
                        assert!(!text.is_empty());
                        reference = next.into();
                    }
                }
            }
            json!(output)
        }
        _ => entry["value"].clone(),
    }
}
fn entries(context: &Context, root: &str) -> BTreeMap<String, Value> {
    let mut queue = vec![root.to_owned()];
    let mut entries = BTreeMap::new();
    while let Some(reference) = queue.pop() {
        for entry in directory(context, &reference) {
            if let Some(children) = entry["childrenRef"].as_str() {
                queue.push(children.into());
            }
            assert!(entries
                .insert(entry["id"].as_str().unwrap().into(), entry)
                .is_none());
        }
    }
    entries
}

#[test]
fn rendered_detail_reconstructs_exact_logical_context_and_whole_leaf() {
    let context = fixture(" Straße😀 ");
    let reference = hit_ref(&context, 13, 19).unwrap();
    let root = directory(&context, &reference);
    assert_eq!(root.len(), 1);
    assert!(root[0]["parentId"].is_null());
    let actual = reconstruct(&context, &root[0], &mut BTreeSet::new());
    let expected = json!({
        "kind":"stagedRenderedHit","mapping":"identity",
        "sourceRange":{"start":13,"end":19},"renderedRange":{"start":1,"end":7},
        "parent":{"ordinal":0,"sourceRange":{"start":12,"end":22},"attributes":{},
            "descriptor":{"version":1,"nodeType":"paragraph","parentOrdinal":null,"nativeRange":{"from":20,"to":32},"attributesRef":"attrs-parent"}},
        "leaf":{"ordinal":1,"sourceRange":{"start":12,"end":22},"attributes":{},"renderedText":" Straße😀 ",
            "descriptor":{"version":2,"nodeType":"text","parentOrdinal":0,"nativeRange":{"from":21,"to":31},"attributesRef":"attrs-leaf","renderedText":{"textId":"uploaded-rendered","length":10,"utf8Bytes":13,"sha256":"d".repeat(64)}}}
    });
    assert_eq!(actual, expected);
    let all = entries(&context, &reference);
    let empty: Vec<_> = all.values().filter(|e| e["key"] == "attributes").collect();
    assert_eq!(empty.len(), 2);
    for entry in empty {
        assert!(directory(&context, entry["childrenRef"].as_str().unwrap()).is_empty());
    }
    assert!(read(&context, &query("uploaded-rendered"), &json!(1)).is_err());
}

#[test]
fn rendered_refs_reject_foreign_tampered_widened_and_unreachable_resources() {
    let mut context = fixture("Straße😀");
    context.rendered.as_mut().unwrap().selected_range = NoteStageRange { start: 1, end: 8 };
    assert!(hit_ref(&context, 12, 18).is_err());
    assert!(hit_ref(&context, 18, 19).is_err()); // half supplementary scalar
    assert!(hit_ref(&context, 13, 13).is_err());
    let reference = hit_ref(&context, 13, 18).unwrap();
    let mut foreign = fixture("ignored");
    foreign.binding = [20; 32];
    assert!(read(&foreign, &query(&reference), &json!(1)).is_err());
    for claims in [
        [13, 18, 999, 1, 0],
        [13, 18, 0, 2, 0],
        [13, 18, 1, 0, 0],
        [13, 18, 0, 1, 1],
        [12, 18, 0, 0, 0],
    ] {
        let forged = sign(REF, &context.key, &context.binding, &claims).unwrap();
        assert!(read(&context, &query(&forged), &json!(1)).is_err());
    }
    for bad in [
        format!("{reference}A"),
        reference.replacen("nrd1.", "nrd2.", 1),
        "x".repeat(257),
    ] {
        assert!(read(&context, &query(&bad), &json!(1)).is_err());
    }
    let mut wrong = query(&reference);
    wrong.kind = ReceiptDetailKind::Inverse;
    assert!(read(&context, &wrong, &json!(1)).is_err());
}

#[test]
fn directory_cursor_binds_ref_and_budgets_separately_from_scalar_seek() {
    let context = fixture("Straße😀");
    let root = directory(&context, &hit_ref(&context, 12, 18).unwrap());
    let mut q = query(root[0]["childrenRef"].as_str().unwrap());
    q.max_items = 1;
    let page = checked(&context, &q, &json!(1));
    q.cursor = Some(page["nextCursor"].as_str().unwrap().into());
    checked(&context, &q, &json!(1));
    for field in 0..4 {
        let mut bad = q.clone();
        match field {
            0 => bad.max_items += 1,
            1 => bad.max_wire_bytes += 1,
            2 => bad.max_source_bytes += 1,
            _ => bad.reference = hit_ref(&context, 12, 18).unwrap(),
        }
        assert!(read(&context, &bad, &json!(1)).is_err());
    }
    let all = entries(&context, &hit_ref(&context, 12, 18).unwrap());
    let scalar = all
        .values()
        .find(|e| e["key"] == "renderedText" && e["type"] == "string")
        .unwrap();
    let reference = scalar["valueRef"].as_str().unwrap();
    let mut q = query(reference);
    q.offset = Some(6);
    let page = checked(&context, &q, &json!(1));
    assert_eq!(page["items"][0]["text"], "😀");
    assert!(page["items"][0]["nextRef"].is_null());
    for offset in [7, 8, 9] {
        q.offset = Some(offset);
        assert!(read(&context, &q, &json!(1)).is_err());
    }
    q.offset = None;
    let first = checked(&context, &q, &json!(1));
    let mut next = query(first["items"][0]["nextRef"].as_str().unwrap());
    let continued = checked(&context, &next, &json!(1));
    assert_eq!(continued["items"][0]["offset"], 4);
    next.offset = Some(0);
    let rewound = checked(&context, &next, &json!(1));
    assert_eq!(rewound, first);
    next.offset = Some(6);
    let sought = checked(&context, &next, &json!(1));
    assert_eq!(sought["items"][0]["text"], "😀");
    assert_eq!(sought["items"][0]["id"], first["items"][0]["id"]);
    assert!(sought["items"][0]["nextRef"].is_null());
    for offset in [7, 8, 9] {
        next.offset = Some(offset);
        assert!(read(&context, &next, &json!(1)).is_err());
    }
    next.offset = None;
    next.cursor = Some(page["items"][0]["id"].as_str().unwrap().into());
    assert!(read(&context, &next, &json!(1)).is_err());
}

#[test]
fn full_escaped_frame_fitting_preserves_first_scalar_and_terminal_ref_removal() {
    let context = fixture(&format!("😀{}", "\"\\\n".repeat(600)));
    let all = entries(&context, &hit_ref(&context, 12, 14).unwrap());
    let scalar = all
        .values()
        .find(|e| e["key"] == "renderedText" && e["type"] == "string")
        .unwrap();
    let mut q = query(scalar["valueRef"].as_str().unwrap());
    let first = read(&context, &q, &json!("")).unwrap();
    assert_eq!(first["items"][0]["text"], "😀");
    let size = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":"","result":first}))
        .unwrap()
        .len();
    let rpc = json!("x".repeat(4096 - size));
    q.max_source_bytes = 16384;
    let page = checked(&context, &q, &rpc);
    assert_eq!(page, first);
    assert!(matches!(
        read(&context, &q, &json!("x".repeat(4097 - size))),
        Err(Error::NoteMutation(NoteMutationError::Budget))
    ));
    let small = fixture("😀");
    let all = entries(&small, &hit_ref(&small, 12, 14).unwrap());
    let scalar = all
        .values()
        .find(|e| e["key"] == "renderedText" && e["type"] == "string")
        .unwrap();
    let page = checked(&small, &query(scalar["valueRef"].as_str().unwrap()), &rpc);
    assert_eq!(page["items"][0]["text"], "😀");
    assert!(page["items"][0]["nextRef"].is_null());
}

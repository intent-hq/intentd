//! Additional real Store captures. Empty/long cases are Store controls, not new
//! native-editor oracle evidence. Existing six mixed captures remain unchanged.
use super::*;

async fn negatives(store: &Store, sources: &[Value]) -> Vec<Value> {
    let header = &sources[0]["header"];
    let mut result =
        vec![rejected_grant(store, "different-principal", header.clone(), "bob").await];
    for (label, path, value) in [
        (
            "different-note-instance",
            "/scope/noteInstanceId",
            json!("00000000-0000-0000-0000-000000000001"),
        ),
        (
            "different-revision",
            "/source/sourceRevision",
            json!("not-the-captured-revision"),
        ),
        (
            "different-backend",
            "/scope/backendId",
            json!("different-backend"),
        ),
    ] {
        let mut changed = header.clone();
        *changed.pointer_mut(path).unwrap() = value;
        result.push(rejected_grant(store, label, changed, "alice").await);
    }
    for field in ["ownerRef", "sourceRef"] {
        let mut changed = header.clone();
        changed["source"][field] = sources.get(1).map_or_else(
            || {
                header["source"][if field == "ownerRef" {
                    "sourceRef"
                } else {
                    "ownerRef"
                }]
                .clone()
            },
            |other| other["header"]["source"][field].clone(),
        );
        result.push(rejected_grant(store, field, changed, "alice").await);
    }
    result
}

async fn invalidations(store: &Store, note: &mut intent_core::Note, header: &Value) -> Vec<Value> {
    note.title = "Diff metadata changed".into();
    store.update_note(note).await.unwrap();
    let old_read = rejected_read(store, header).await;
    let old_grant = rejected_grant(store, "after-metadata-edit", header.clone(), "alice").await;
    let (calls, sources) = capture_sources(store, true).await;
    let fresh = sources[0]["header"].clone();
    assert_ne!(
        fresh["source"]["sourceRevision"],
        header["source"]["sourceRevision"]
    );
    let mut output = vec![
        json!({"mutation":"metadata-title","oldRead":old_read,"oldGrant":old_grant,"calls":calls,"sources":sources}),
    ];

    note.content = "```diff title\n-old\n+changed😀\n```".into();
    store.update_note(note).await.unwrap();
    let old_read = rejected_read(store, &fresh).await;
    let old_grant = rejected_grant(store, "after-code-edit", fresh, "alice").await;
    let (calls, sources) = capture_sources(store, true).await;
    assert_eq!(sources[0]["code"], "-old\n+changed😀");
    let before = sources[0]["header"].clone();
    output.push(json!({"mutation":"canonical-code-value","source":note.content,"oldRead":old_read,"oldGrant":old_grant,"calls":calls,"sources":sources}));

    store
        .delete_note(&note.workspace_id, &note.id)
        .await
        .unwrap();
    store.insert_note(note).await.unwrap();
    let old_read = rejected_read(store, &before).await;
    let old_grant = rejected_grant(store, "after-note-recreation", before.clone(), "alice").await;
    let (calls, sources) = capture_sources(store, true).await;
    assert_ne!(
        sources[0]["header"]["scope"]["noteInstanceId"],
        before["scope"]["noteInstanceId"]
    );
    output.push(json!({"mutation":"delete-recreate-same-note-id","oldRead":old_read,"oldGrant":old_grant,"calls":calls,"sources":sources}));
    output
}

fn check_scalar_continuation(calls: &[Value], source: &Value) -> usize {
    let value_ref = &source["codeField"]["valueRef"];
    let mut offset = 0_u64;
    let mut fragments = 0;
    let mut assembled = String::new();
    for call in calls
        .iter()
        .filter(|call| call["request"]["contextRef"] == *value_ref)
    {
        for item in call["response"]["items"].as_array().unwrap() {
            assert_eq!(item["kind"], "fragment");
            assert_eq!(item["offset"], offset);
            let text = item["text"].as_str().unwrap();
            // Rust strings preserve Unicode scalar boundaries; offsets are UTF-16.
            offset += u64::try_from(text.encode_utf16().count()).unwrap();
            assembled.push_str(text);
            fragments += 1;
        }
    }
    assert_eq!(assembled, source["code"].as_str().unwrap());
    fragments
}

#[tokio::test]
async fn indexed_diff_source_capture_preserves_empty_titled_scalar_and_invalidation() {
    let groups: Value =
        serde_json::from_str(include_str!("../../fixtures/note_primitive_native.json")).unwrap();
    let titled = groups
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|group| group["cases"].as_array().unwrap())
        .find(|case| case["id"] == "titledDiffFence")
        .unwrap();
    let long_code = format!("-old\n+{}END", "é😀e\u{301}\"\t".repeat(1800));
    let cases = [
        ("emptyDiff", "<div data-type=\"diff-block\" data-diff-code=\"\"></div><div data-type=\"diff-block\" data-diff-code=\"-other&#10;+other\"></div>".to_owned(), String::new(), "Store-only empty attribute control; no new native oracle"),
        ("titledDiffFence", titled["source"].as_str().unwrap().to_owned(), titled["atoms"][0]["code"].as_str().unwrap().to_owned(), "Existing frozen e6d6bb999 titled fence native oracle"),
        ("longScalarDiff", format!("```diff title\n{long_code}\n```"), long_code, "Store-only long scalar continuation control; no native paint/editor oracle"),
    ];
    for (id, raw, expected, provenance) in cases {
        let (store, _temporary, mut note) = setup(&raw).await;
        let (calls, sources) = capture_sources(&store, id != "longScalarDiff").await;
        assert!(!sources.is_empty());
        assert_eq!(sources[0]["node"]["nodeType"], "diffBlock");
        assert_eq!(sources[0]["code"], expected);
        let fragments = check_scalar_continuation(&calls, &sources[0]);
        if id == "longScalarDiff" {
            assert!(fragments > 1, "real source must require continuation");
            assert!(calls[0]["response"]["nextCursor"].is_string());
        }
        let rejected = negatives(&store, &sources).await;
        let invalidated = if id == "emptyDiff" {
            invalidations(&store, &mut note, &sources[0]["header"]).await
        } else {
            Vec::new()
        };
        if let Ok(directory) = std::env::var("NOTE_PAGE_TRANSCRIPT_DIR") {
            let output = json!({"caseId":id,"source":raw,"provenance":provenance,
                "readiness":"test-only-real-store-capture","calls":calls,"sources":sources,
                "scalarFragments":fragments,"internalGrantFailures":rejected,"internalInvalidationChecks":invalidated,
                "limitations":"Actual Store before transport. Internal Diff source authorization is not a public RPC, registered renderer/font/profile, physical reservation, production provider or native editor/paint proof."});
            let path = std::path::Path::new(&directory).join(format!("diff-{id}.json"));
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .unwrap();
            serde_json::to_writer_pretty(file, &output).unwrap();
        }
        store.close().await;
    }
}

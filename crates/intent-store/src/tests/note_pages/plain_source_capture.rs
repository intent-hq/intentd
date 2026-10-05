//! Exact small Store responses for the frontend's ordinary-paragraph adapter.
//! This records lexical resources; it does not manufacture native authority.
use super::{record_source_window_closure, setup};
use serde_json::json;

#[tokio::test]
async fn indexed_selection_paragraph_capture_preserves_nonzero_unicode_source() {
    let paragraph = "a".repeat(2050);
    let source = format!("prefix😀\n\n{paragraph}");
    assert_eq!(source.encode_utf16().count(), 2060);
    let (store, _temporary, _) = setup(&source).await;
    let captured_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let mut calls = Vec::new();
    let first = record_source_window_closure(
        &store,
        json!({"kind":"source","at":10,"maxSourceBytes":4096,"maxWireBytes":8192,"maxItems":64}),
        &mut calls,
    )
    .await;
    assert_eq!(first["text"], paragraph);
    assert_eq!(first["range"], json!({"start":10,"end":2060}));
    assert_eq!(first["sourceLength"], 2060);
    assert!(first["nextCursor"].is_null());
    assert!(calls.iter().any(|call| call["response"]["items"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|item| item["construct"] == "paragraph"
            && item["sourceRange"] == json!({"start":10,"end":2060})
            && item["entryPath"] == "markdown"
            && item["detailRef"].is_string())));
    if let Ok(directory) = std::env::var("NOTE_PAGE_TRANSCRIPT_DIR") {
        // Retain the same backend/note incarnation for a later frontend-upload
        // replay; copying just response identities into a new Store is not proof.
        let database = std::path::Path::new(&directory).join("selection-source.db");
        sqlx::query("VACUUM INTO ?")
            .bind(database.to_str().unwrap())
            .execute(store.write_pool())
            .await
            .unwrap();
        std::fs::write(
            std::path::Path::new(&directory).join("plain-paragraph-selection-2050.json"),
            serde_json::to_vec_pretty(&json!({
                "backendHead":std::env::var("NOTE_PAGE_CAPTURE_HEAD").unwrap(),
                "capturedAtMs":captured_at_ms,"source":source,"at":10,
                "workspaceId":"pages","noteId":"spec","principal":"alice",
                "rpcId":1,"calls":calls,"retainedDatabase":database,
                "claim":"actual lexical Store source/context closure; native selection is captured separately by the frontend"
            }))
            .unwrap(),
        )
        .unwrap();
    }
}

#[tokio::test]
async fn indexed_plain_paragraph_capture_preserves_actual_lexical_resources() {
    for (name, source, expected_paragraphs, entry_path) in [
        ("one", "abc", 1, "markdown"),
        ("two", "abc\n\ndef", 2, "markdown"),
        ("html-tail", "<div>html</div>\n\nabc", 1, "html"),
    ] {
        let (store, _temporary, _) = setup(source).await;
        let mut calls = Vec::new();
        let first = record_source_window_closure(
            &store,
            json!({"kind":"source","at":0,"maxSourceBytes":4096,"maxWireBytes":8192,"maxItems":64}),
            &mut calls,
        )
        .await;
        assert_eq!(first["text"], source);
        assert_eq!(first["range"], json!({"start":0,"end":source.len()}));
        assert!(first["nextCursor"].is_null());
        let mut paragraphs = std::collections::BTreeSet::new();
        for call in &calls {
            for item in call["response"]["items"].as_array().into_iter().flatten() {
                if item["kind"] == "boundary" && item["construct"] == "paragraph" {
                    assert!(item["detailRef"].is_string());
                    assert_eq!(item["entryPath"], entry_path);
                    paragraphs.insert(item["id"].as_str().unwrap().to_owned());
                }
            }
        }
        assert_eq!(paragraphs.len(), expected_paragraphs);
        if let Ok(directory) = std::env::var("NOTE_PAGE_TRANSCRIPT_DIR") {
            let head = std::env::var("NOTE_PAGE_CAPTURE_HEAD").unwrap();
            std::fs::write(
                std::path::Path::new(&directory).join(format!("plain-paragraph-{name}.json")),
                serde_json::to_vec_pretty(&json!({
                    "backendHead":head,"source":source,"at":0,"calls":calls,
                    "claim":"actual lexical Store responses, not native edit authority"
                }))
                .unwrap(),
            )
            .unwrap();
        }
    }
}

#[tokio::test]
async fn indexed_plain_ab_capture_retains_original_source_context_closure() {
    use std::collections::BTreeSet;
    use std::io::Write as _;

    let (store, _temporary, note) = setup("ab").await;
    let captured_at_ms = intent_core::now_epoch_ms();
    let mut calls = Vec::new();
    let first = super::record_page(
        &store,
        json!({"kind":"source","at":0,"maxSourceBytes":4096,"maxWireBytes":8192,"maxItems":64}),
        &mut calls,
    )
    .await;
    assert_eq!(first["text"], "ab");
    assert_eq!(first["sourceLength"], 2);
    assert_eq!(first["range"], json!({"start":0,"end":2}));
    assert_eq!(first.get("nextCursor"), Some(&serde_json::Value::Null));
    let mut pending = vec![(
        "context".to_owned(),
        first["contextRef"].as_str().unwrap().to_owned(),
    )];
    let mut seen = BTreeSet::new();
    while let Some((kind, reference)) = pending.pop() {
        if !seen.insert(reference.clone()) {
            continue;
        }
        assert!(seen.len() <= 16, "small lexical fixture closure");
        let mut request = json!({"kind":kind,"maxWireBytes":8192,"maxItems":64});
        request[if kind == "metadata" {
            "ref"
        } else {
            "contextRef"
        }] = json!(reference);
        // This two-character source has no paginated resource directory.
        let response = super::record_page(&store, request, &mut calls).await;
        assert_eq!(response.get("nextCursor"), Some(&serde_json::Value::Null));
        super::resource_refs(&response["items"], &mut pending);
    }
    assert!(calls.len() > 2);
    let mut paragraphs = BTreeSet::new();
    for call in &calls {
        let response = &call["response"];
        for field in ["scope", "sourceRevision", "snapshotId", "expiresAt"] {
            assert_eq!(response[field], first[field]);
        }
        if call["request"]["kind"] == "source" {
            assert_eq!(call["request"]["maxSourceBytes"], 4096);
        } else {
            assert!(call["request"].get("maxSourceBytes").is_none());
        }
        assert_eq!(call["request"]["maxWireBytes"], 8192);
        assert_eq!(call["request"]["maxItems"], 64);
        for item in response["items"].as_array().into_iter().flatten() {
            if item["kind"] == "boundary" && item["construct"] == "paragraph" {
                assert_eq!(item["sourceRange"], json!({"start":0,"end":2}));
                assert_eq!(item["entryPath"], "markdown");
                assert!(seen.contains(item["detailRef"].as_str().unwrap()));
                paragraphs.insert(item["id"].as_str().unwrap());
            }
        }
    }
    assert_eq!(paragraphs.len(), 1);
    assert_eq!(
        store
            .get_note(&note.workspace_id, &note.id)
            .await
            .unwrap()
            .content,
        "ab"
    );
    let expiry = intent_core::parse_iso(first["expiresAt"].as_str().unwrap()).unwrap();
    assert!(expiry.unix_timestamp_nanos() / 1_000_000 > i128::from(captured_at_ms));
    if let Ok(directory) = std::env::var("NOTE_PAGE_TRANSCRIPT_DIR") {
        let output = std::path::Path::new(&directory).join("plain-paragraph-ab.json");
        let artifact = json!({
            "backendHead":std::env::var("NOTE_PAGE_CAPTURE_HEAD").unwrap(),
            "capturedAtMs":captured_at_ms,"source":"ab","at":0,
            "workspaceId":"pages","noteId":"spec","principal":"alice","rpcId":1,
            "calls":calls,
            "claim":"original Store lexical source/context/detail closure for local recorded-clock replay; not native capture or staged mutation authority"
        });
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output)
            .unwrap()
            .write_all(&serde_json::to_vec_pretty(&artifact).unwrap())
            .unwrap();
    }
}

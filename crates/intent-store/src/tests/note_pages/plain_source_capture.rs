//! Exact small Store responses for the frontend's ordinary-paragraph adapter.
//! This records lexical resources; it does not manufacture native authority.
use super::{record_source_window_closure, setup};
use serde_json::json;

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

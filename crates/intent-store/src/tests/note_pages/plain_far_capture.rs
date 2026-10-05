//! A genuine nonzero Store seek; no response is cropped from a full-note read.
use super::{record_source_window_closure, setup};
use serde_json::json;

#[tokio::test]
async fn indexed_far_paragraph_capture_retains_document_entry_mode() {
    let prefix_repeat_count = 65_536;
    let suffix = "\n\nabc";
    let source = format!("{}{suffix}", "x".repeat(prefix_repeat_count));
    let at = prefix_repeat_count + 2;
    let (store, _temporary, _) = setup(&source).await;
    let mut calls = Vec::new();
    let first = record_source_window_closure(
        &store,
        json!({"kind":"source","at":at,"maxSourceBytes":4096,"maxWireBytes":8192,"maxItems":64}),
        &mut calls,
    )
    .await;
    assert_eq!(first["text"], "abc");
    assert_eq!(first["range"], json!({"start":at,"end":at+3}));
    assert!(first["nextCursor"].is_null());
    let mut found = false;
    for call in &calls {
        for item in call["response"]["items"].as_array().into_iter().flatten() {
            if item["kind"] == "boundary" && item["construct"] == "paragraph" {
                assert_eq!(item["sourceRange"], json!({"start":at,"end":at+3}));
                assert_eq!(item["entryPath"], "markdown");
                assert!(item["detailRef"].is_string());
                found = true;
            }
        }
    }
    assert!(found);
    if let Ok(directory) = std::env::var("NOTE_PAGE_TRANSCRIPT_DIR") {
        std::fs::write(
            std::path::Path::new(&directory).join("plain-paragraph-far.json"),
            serde_json::to_vec_pretty(&json!({
                "backendHead":std::env::var("NOTE_PAGE_CAPTURE_HEAD").unwrap(),
                "sourceRecipe":{"prefix":"x","prefixRepeatCount":prefix_repeat_count,"suffix":suffix},
                "at":at,"calls":calls,
                "claim":"actual nonzero lexical Store seek, not native edit authority"
            }))
            .unwrap(),
        )
        .unwrap();
    }
}

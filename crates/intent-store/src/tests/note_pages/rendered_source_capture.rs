//! Actual lexical input for a separately captured native rendered-search view.
use super::{record_source_window_closure, setup};
use serde_json::json;

#[tokio::test]
async fn indexed_rendered_paragraph_capture_preserves_actual_unicode_leaf() {
    let paragraph = format!("{}😀", "Straße ".repeat(18));
    assert_eq!(paragraph.encode_utf16().count(), 128);
    assert_eq!(paragraph.len(), 148);
    let source = format!("prefix😀\n\n{paragraph}");
    assert_eq!(source.encode_utf16().count(), 138);
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
    assert_eq!(first["range"], json!({"start":10,"end":138}));
    assert_eq!(first["sourceLength"], 138);
    assert!(first["nextCursor"].is_null());
    assert!(calls.iter().any(|call| call["response"]["items"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|item| item["construct"] == "paragraph"
            && item["sourceRange"] == json!({"start":10,"end":138})
            && item["entryPath"] == "markdown"
            && item["detailRef"].is_string())));
    if let Ok(directory) = std::env::var("NOTE_PAGE_TRANSCRIPT_DIR") {
        // Retain the same backend/note incarnation for a later frontend-upload
        // replay; copying just response identities into a new Store is not proof.
        let database = std::path::Path::new(&directory).join("rendered-source.db");
        sqlx::query("VACUUM INTO ?")
            .bind(database.to_str().unwrap())
            .execute(store.write_pool())
            .await
            .unwrap();
        std::fs::write(
            std::path::Path::new(&directory).join("plain-paragraph-rendered-search.json"),
            serde_json::to_vec_pretty(&json!({
                "backendHead":std::env::var("NOTE_PAGE_CAPTURE_HEAD").unwrap(),
                "capturedAtMs":captured_at_ms,"source":source,"at":10,
                "workspaceId":"pages","noteId":"spec","principal":"alice",
                "rpcId":1,"calls":calls,"retainedDatabase":database,
                "claim":"actual lexical Store source/context closure; native rendered text is captured separately by the frontend"
            }))
            .unwrap(),
        )
        .unwrap();
    }
}

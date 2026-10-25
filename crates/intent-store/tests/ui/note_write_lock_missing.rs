async fn forbidden(store: &intent_store::Store, note: &intent_core::Note) {
    let author = intent_core::NoteVersionAuthor {
        id: "user".into(),
        name: "User".into(),
        author_type: "user".into(),
    };
    let _ = store
        .update_note_with_version(note, None, &author, "2026-01-01T00:00:00Z")
        .await;
}
fn main() {}

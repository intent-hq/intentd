use super::*;

struct InsertCapture {
    inserts: Arc<AtomicUsize>,
    writer_thread: Arc<std::sync::Mutex<Option<std::thread::ThreadId>>>,
}

impl tracing::Subscriber for InsertCapture {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target() == "sqlx::query"
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Visitor(bool);
        impl tracing::field::Visit for Visitor {
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "summary" && value.starts_with("INSERT INTO note_page_entry") {
                    self.0 = true;
                }
            }
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.record_str(field, format!("{value:?}").trim_matches('"'));
            }
        }
        if *self.writer_thread.lock().unwrap() != Some(std::thread::current().id()) {
            return;
        }
        let mut visitor = Visitor(false);
        event.record(&mut visitor);
        if visitor.0 {
            self.inserts.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn multiblock_source() -> String {
    "## Heading\n\nA **bold** paragraph with `code` and plain text.\n\n- first\n- second\n\n"
        .repeat(128)
}

#[tokio::test]
async fn indexed_note_write_batches_descriptor_statements_and_preserves_source() {
    let (store, _tmp, mut note) = setup("original").await;
    note.content = multiblock_source();
    let inserts = Arc::new(AtomicUsize::new(0));
    // sqlx emits its query events on the SQLite worker, not this Tokio thread.
    // Select only this store's sole writer so concurrently running tests cannot
    // contaminate the count captured by the process-wide subscriber.
    let writer_thread = Arc::new(std::sync::Mutex::new(None));
    {
        let mut connection = store.write_pool().acquire().await.unwrap();
        let thread = writer_thread.clone();
        connection
            .lock_handle()
            .await
            .unwrap()
            .set_update_hook(move |_| {
                *thread.lock().unwrap() = Some(std::thread::current().id());
            });
    }
    tracing::subscriber::set_global_default(InsertCapture {
        inserts: inserts.clone(),
        writer_thread,
    })
    .unwrap();
    store.update_note(&note).await.unwrap();
    {
        let mut connection = store.write_pool().acquire().await.unwrap();
        connection.lock_handle().await.unwrap().remove_update_hook();
    }
    let rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM note_page_entry WHERE workspace_id='pages' AND note_id='spec'",
    )
    .fetch_one(store.read_pool())
    .await
    .unwrap();
    let inserts = inserts.load(Ordering::Relaxed);
    eprintln!("note index: {inserts} INSERT statements for {rows} descriptors");
    assert!(rows > 1000, "fixture must cross many insertion batches");
    assert!(inserts > 0, "SQL tracing must observe the real writes");
    assert!(
        inserts < usize::try_from(rows).unwrap() / 32,
        "per-row index statements hold the sole writer: {inserts} statements for {rows} rows"
    );

    let mut source = String::new();
    let mut request = json!({"kind":"source"});
    loop {
        let got = page(&store, request).await;
        source.push_str(got["text"].as_str().unwrap());
        if got["nextCursor"].is_null() {
            break;
        }
        request = json!({"kind":"source","cursor":got["nextCursor"]});
    }
    assert_eq!(source, note.content);
}

#[tokio::test]
async fn indexed_note_later_batch_failure_rolls_back_source_revision_and_index() {
    let (store, _tmp, mut note) = setup("original **source**").await;
    let before = page(&store, json!({"kind":"source"})).await;
    let rows_before: i64 = sqlx::query_scalar("SELECT count(*) FROM note_page_entry")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert!(rows_before < 200);
    sqlx::query("CREATE TRIGGER fail_later_index_batch BEFORE INSERT ON note_page_entry WHEN (SELECT count(*) FROM note_page_entry) >= 200 BEGIN SELECT RAISE(ABORT, 'later index batch failed'); END")
        .execute(store.write_pool()).await.unwrap();
    note.content = multiblock_source();
    let error = store.update_note(&note).await.unwrap_err();
    assert!(error.to_string().contains("later index batch failed"));
    let persisted = store.get_note(&note.workspace_id, &note.id).await.unwrap();
    assert_eq!(persisted.content, "original **source**");
    assert_eq!(persisted.rev, note.rev);
    let after = page(&store, json!({"kind":"source","snapshotId":before["snapshotId"],"sourceRevision":before["sourceRevision"],"noteInstanceId":before["scope"]["noteInstanceId"]})).await;
    assert_eq!(
        after, before,
        "failed batch must not publish a new page identity"
    );
    let rows_after: i64 = sqlx::query_scalar("SELECT count(*) FROM note_page_entry")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(rows_after, rows_before);
}

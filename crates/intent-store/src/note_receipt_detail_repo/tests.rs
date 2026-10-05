use super::*;
use intent_core::note_page::NoteScope;

const OP: &str = "11111111-1111-4111-8111-111111111111";
const KEY: &str = "22222222-2222-4222-8222-222222222222";
async fn fixture() -> (tempfile::TempDir, Store, ReceiptDetailQuery) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("receipt.db")).await.unwrap();
    let workspace=serde_json::from_value(json!({"id":"ws","title":"Test","branch":"test","status":"Active","activity":"idle","attention":"unread","createdAt":"date","updatedAt":"date","tags":[],"skipWorktree":true,"isRemote":false,"archived":false})).unwrap();
    store.insert_workspace(&workspace).await.unwrap();
    let backend: String =
        sqlx::query_scalar("SELECT backend_id FROM note_page_backend WHERE singleton=1")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    let query = ReceiptDetailQuery {
        scope: NoteScope {
            backend_id: backend,
            workspace_id: "ws".into(),
            note_id: "deleted-note".into(),
            note_instance_id: "original".into(),
        },
        operation_id: OP.into(),
        payload_digest: Some("a".repeat(64)),
        kind: ReceiptDetailKind::Mapping,
        reference: format!("{KEY}:mapping"),
        cursor: None,
        max_items: 2,
        max_wire_bytes: 4096,
        max_source_bytes: 16384,
        operation_envelope: true,
        context_envelope: false,
        text_id: None,
        offset: None,
    };
    let deadline = i64::try_from(intent_core::now_epoch_ms() / 1000).unwrap() + 3600;
    let receipt = json!({"kind":"noteCommitReceipt","outcome":"committed","scope":query.scope,"operationId":OP,"payloadDigest":"a".repeat(64),
        "beforeRevision":"before","afterRevision":"after","sourceLength":12,"mappingRef":format!("{KEY}:mapping"),"effectsRef":format!("{KEY}:effects"),"inverseRef":format!("{KEY}:inverse"),"receiptExpiresAt":intent_core::iso_from_unix_secs(deadline),"invalidation":"all"});
    sqlx::query("INSERT INTO note_operation(operation_key,principal,backend_id,workspace_id,note_id,instance_id,operation_id,payload_digest,admission_expires,retain_until,outcome,converted_count) VALUES(?,'alice',?,'ws','deleted-note','original',?,?,0,?,?,7)")
        .bind(KEY).bind(&query.scope.backend_id).bind(OP).bind("a".repeat(64)).bind(deadline).bind(receipt.to_string()).execute(store.write_pool()).await.unwrap();
    sqlx::query("INSERT INTO note_operation_source(operation_key,phase,start,end,text) VALUES(?,'base',0,10,'0123456789')").bind(KEY).execute(store.write_pool()).await.unwrap();
    (dir, store, query)
}
async fn item(store: &Store, kind: &str, sequence: i64, value: Value) {
    sqlx::query(
        "INSERT INTO note_operation_item(operation_key,kind,sequence,value) VALUES(?,?,?,?)",
    )
    .bind(KEY)
    .bind(kind)
    .bind(sequence)
    .bind(value.to_string())
    .execute(store.write_pool())
    .await
    .unwrap();
}
async fn register_reference(store: &Store, reference: &str) {
    sqlx::query("INSERT INTO note_operation_reference(operation_key,reference) VALUES(?,?)")
        .bind(KEY)
        .bind(reference)
        .execute(store.write_pool())
        .await
        .unwrap();
}

async fn read(store: &Store, query: &ReceiptDetailQuery) -> Value {
    store
        .read_note_receipt_detail("alice", query, &json!("escaped\\\"\n😀"))
        .await
        .unwrap()
}

#[tokio::test]
async fn receipt_detail_pages_survive_restart_and_live_note_absence() {
    let (dir, store, mut query) = fixture().await;
    for i in 0..5 {
        item(
            &store,
            "mapping",
            i,
            json!({"start":i*2,"end":i*2+1,"insertedLength":3}),
        )
        .await;
    }
    let first = read(&store, &query).await;
    assert_eq!(first["kind"], "noteOperationPage");
    assert_eq!(first["sourceLength"], 10);
    assert_eq!(first["beforeRevision"], "before");
    assert_eq!(first["afterRevision"], "after");
    query.cursor = Some(first["nextCursor"].as_str().unwrap().into());
    drop(store);
    let store = Store::open(&dir.path().join("receipt.db")).await.unwrap();
    let second = read(&store, &query).await;
    assert_eq!(second["items"][0]["start"], 4);
    query.cursor = Some(second["nextCursor"].as_str().unwrap().into());
    let last = read(&store, &query).await;
    assert_eq!(last["items"].as_array().unwrap().len(), 1);
    assert!(last["nextCursor"].is_null());
    query.cursor = None;
    query.operation_envelope = false;
    query.payload_digest = None;
    assert_eq!(read(&store, &query).await["kind"], "noteMappingPage");
}

#[tokio::test]
async fn receipt_detail_cursor_binds_principal_scope_digest_kind_and_budgets() {
    let (_dir, store, query) = fixture().await;
    for i in 0..3 {
        item(
            &store,
            "mapping",
            i,
            json!({"start":i,"end":i,"insertedLength":1}),
        )
        .await;
    }
    let first = read(&store, &query).await;
    let mut continued = query.clone();
    continued.cursor = Some(first["nextCursor"].as_str().unwrap().into());
    for mutation in 0..8 {
        let mut changed = continued.clone();
        match mutation {
            0 => changed.scope.note_instance_id = "recreated".into(),
            1 => changed.operation_id = "33333333-3333-4333-8333-333333333333".into(),
            2 => changed.payload_digest = Some("b".repeat(64)),
            3 => changed.reference = format!("{KEY}:effects"),
            4 => changed.max_items = 3,
            5 => changed.max_wire_bytes = 8192,
            6 => changed.max_source_bytes = 128,
            _ => {
                changed.kind = ReceiptDetailKind::Effects;
                changed.reference = format!("{KEY}:effects");
            }
        }
        assert!(store
            .read_note_receipt_detail("alice", &changed, &json!(1))
            .await
            .is_err());
    }
    assert!(store
        .read_note_receipt_detail("bob", &continued, &json!(1))
        .await
        .is_err());
    let mut corrupt = continued.clone();
    corrupt.cursor.as_mut().unwrap().push('x');
    assert!(matches!(
        store
            .read_note_receipt_detail("alice", &corrupt, &json!(1))
            .await,
        Err(Error::NotePage(NotePageError::CursorInvalid))
    ));
    sqlx::query("UPDATE note_operation SET retain_until=0")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert!(matches!(
        store
            .read_note_receipt_detail("alice", &continued, &json!(1))
            .await,
        Err(Error::NotePage(NotePageError::Expired))
    ));
}

#[tokio::test]
async fn receipt_detail_effects_use_retained_count_and_exact_escaped_frames() {
    let (_dir, store, mut query) = fixture().await;
    query.kind = ReceiptDetailKind::Effects;
    query.reference = format!("{KEY}:effects");
    query.max_items = 128;
    for i in 0..20 {
        item(&store,"effects",i,json!({"kind":"warning","code":"test","messagePreview":"\\\"\n\u{0000}".repeat(128),"truncated":true})).await;
    }
    let mut seen = 0;
    loop {
        let page = read(&store, &query).await;
        assert_eq!(page["convertedCount"], 7);
        assert!(frame_len(&page, &json!("escaped\\\"\n😀")) <= 4096);
        seen += page["items"].as_array().unwrap().len();
        let Some(cursor) = page["nextCursor"].as_str() else {
            break;
        };
        query.cursor = Some(cursor.into());
    }
    assert_eq!(seen, 20);
}

#[tokio::test]
async fn receipt_detail_never_exposes_internal_legacy_inverse_shape() {
    let (_dir, store, mut query) = fixture().await;
    query.kind = ReceiptDetailKind::Inverse;
    query.reference = format!("{KEY}:inverse");
    item(
        &store,
        "inverse",
        0,
        json!({"start":0,"end":1,"source":{"phase":"base","range":{"start":0,"end":1}}}),
    )
    .await;
    assert!(store
        .read_note_receipt_detail("alice", &query, &json!(1))
        .await
        .is_err());
    sqlx::query("DELETE FROM note_operation_item WHERE kind='inverse'")
        .execute(store.write_pool())
        .await
        .unwrap();
    item(&store,"inverse",0,json!({"historyGroup":"0","inputState":"after","outputState":"before","ordinal":0,"start":0,"end":1,
        "replacement":{"textId":"text:0","length":1,"utf8Bytes":1,"sha256":"a".repeat(64)},"provenanceRef":"provenance:0"})).await;
    let page = read(&store, &query).await;
    assert_eq!(page["sourceLength"], 12);
    assert_eq!(page["items"][0]["replacement"]["textId"], "text:0");
}

#[tokio::test]
async fn receipt_detail_production_query_seeks_past_large_prefix() {
    let (_dir, store, _) = fixture().await;
    sqlx::query("WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<10000) INSERT INTO note_operation_item(operation_key,kind,sequence,value) SELECT ?,'mapping',x,json_object('start',x,'end',x,'insertedLength',1) FROM n")
        .bind(KEY).execute(store.write_pool()).await.unwrap();
    let plan = sqlx::query(&format!("EXPLAIN QUERY PLAN {RECORD_PAGE_SQL}"))
        .bind(KEY)
        .bind("mapping")
        .bind(9999_i64)
        .bind(3_i64)
        .fetch_all(store.read_pool())
        .await
        .unwrap();
    let details = plan
        .iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        details.contains("SEARCH") && details.contains("sequence>"),
        "{details}"
    );
    assert!(!details.contains("TEMP B-TREE"), "{details}");
    let rows = sqlx::query(RECORD_PAGE_SQL)
        .bind(KEY)
        .bind("mapping")
        .bind(9999_i64)
        .bind(3_i64)
        .fetch_all(store.read_pool())
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.get::<i64, _>("sequence"))
            .collect::<Vec<_>>(),
        [9999, 10000]
    );
}

#[tokio::test]
async fn receipt_inverse_text_reconstructs_scalar_safe_bytes_and_enforces_reachability() {
    let (_dir, store, mut query) = fixture().await;
    let text = "a😀\r\n\\\"é".repeat(2000);
    let length = i64::try_from(text.encode_utf16().count()).unwrap();
    let sha = format!("{:x}", Sha256::digest(text.as_bytes()));
    sqlx::query("DELETE FROM note_operation_source")
        .execute(store.write_pool())
        .await
        .unwrap();
    let mut piece = String::new();
    let mut start = 0_i64;
    for ch in text.chars().chain(std::iter::once('\0')) {
        if ch == '\0' || piece.len() + ch.len_utf8() > 4096 {
            let end = start + i64::try_from(piece.encode_utf16().count()).unwrap();
            sqlx::query("INSERT INTO note_operation_source(operation_key,phase,start,end,text) VALUES(?,'base',?,?,?)")
                .bind(KEY).bind(start).bind(end).bind(&piece).execute(store.write_pool()).await.unwrap();
            start = end;
            piece.clear();
        }
        if ch != '\0' {
            piece.push(ch);
        }
    }
    let text_id = format!("{KEY}:text:0");
    sqlx::query("INSERT INTO note_operation_text(operation_key,text_id,phase,start,end,length,utf8_bytes,sha256) VALUES(?,?,'base',0,?,?,?,?)")
        .bind(KEY).bind(&text_id).bind(length).bind(length).bind(i64::try_from(text.len()).unwrap()).bind(&sha).execute(store.write_pool()).await.unwrap();
    item(&store,"inverse",0,json!({"historyGroup":"0","inputState":"after","outputState":"before","ordinal":0,"start":0,"end":1,
        "replacement":{"textId":text_id,"length":length,"utf8Bytes":text.len(),"sha256":sha},"provenanceRef":format!("{KEY}:inverse-detail:0")})).await;
    query.kind = ReceiptDetailKind::InverseText;
    query.reference = format!("{KEY}:inverse");
    query.text_id = Some(text_id);
    query.max_source_bytes = 128;
    let mut restored = String::new();
    let mut units = 0;
    loop {
        let page = read(&store, &query).await;
        let fragment = &page["items"][0];
        assert_eq!(fragment["offset"], units);
        let part = fragment["text"].as_str().unwrap();
        assert!(part.len() <= 128);
        units += part.encode_utf16().count();
        restored.push_str(part);
        let Some(next) = page["nextCursor"].as_str() else {
            break;
        };
        query.cursor = Some(next.into());
    }
    assert_eq!(restored, text);
    assert_eq!(format!("{:x}", Sha256::digest(restored.as_bytes())), sha);
    query.cursor = None;
    query.offset = Some(2);
    assert!(matches!(
        store
            .read_note_receipt_detail("alice", &query, &json!(1))
            .await,
        Err(Error::NotePage(NotePageError::CursorInvalid))
    ));
    query.offset = Some(1);
    query.max_source_bytes = 4;
    assert_eq!(read(&store, &query).await["items"][0]["text"], "😀");
    query.offset = None;
    sqlx::query("UPDATE note_operation_item SET value=json_set(value,'$.replacement.textId','unreachable') WHERE kind='inverse'").execute(store.write_pool()).await.unwrap();
    assert!(store
        .read_note_receipt_detail("alice", &query, &json!(1))
        .await
        .is_err());
}

#[tokio::test]
async fn receipt_detail_pages_only_existing_scoped_metadata_or_fragments() {
    let (_dir, store, mut query) = fixture().await;
    let reference = format!("{KEY}:inverse-detail:0");
    register_reference(&store, &reference).await;
    let records = [
        json!({"id":"root","parentId":null,"type":"string","value":"original marker metadata"}),
        json!({"kind":"fragment","id":"field","field":"text","offset":0,"text":"escaped\\\"😀","nextRef":null}),
    ];
    for (sequence, value) in records.iter().enumerate() {
        sqlx::query("INSERT INTO note_operation_detail(operation_key,reference,sequence,value) VALUES(?,?,?,?)")
            .bind(KEY).bind(&reference).bind(i64::try_from(sequence).unwrap()).bind(value.to_string()).execute(store.write_pool()).await.unwrap();
    }
    query.kind = ReceiptDetailKind::Detail;
    query.reference = reference;
    query.max_items = 1;
    let first = read(&store, &query).await;
    assert_eq!(first["sourceLength"], 10); // Captured base; final receipt length is 12.
    assert_eq!(first["items"][0], records[0]);
    query.cursor = Some(first["nextCursor"].as_str().unwrap().into());
    let last = read(&store, &query).await;
    assert_eq!(last["items"][0], records[1]);
    assert!(last["nextCursor"].is_null());
    query.reference = format!("{KEY}:inverse-detail:other");
    assert!(store
        .read_note_receipt_detail("alice", &query, &json!(1))
        .await
        .is_err());
}

#[tokio::test]
async fn receipt_detail_fragment_source_budget_applies_to_whole_page() {
    let (_dir, store, mut query) = fixture().await;
    let reference = format!("{KEY}:detail:fragments");
    register_reference(&store, &reference).await;
    for sequence in 0..40 {
        let value = json!({"kind":"fragment","id":format!("field{sequence}"),"field":"text","offset":0,"text":"x".repeat(1024),"nextRef":null});
        sqlx::query("INSERT INTO note_operation_detail(operation_key,reference,sequence,value) VALUES(?,?,?,?)")
            .bind(KEY).bind(&reference).bind(sequence).bind(value.to_string()).execute(store.write_pool()).await.unwrap();
    }
    query.kind = ReceiptDetailKind::Detail;
    query.reference = reference;
    query.max_items = 128;
    query.max_wire_bytes = 65536;
    query.max_source_bytes = 4096;
    let mut seen = 0;
    loop {
        let page = read(&store, &query).await;
        let items = page["items"].as_array().unwrap();
        assert!(
            items
                .iter()
                .map(|v| v["text"].as_str().unwrap().len())
                .sum::<usize>()
                <= 4096
        );
        seen += items.len();
        let Some(next) = page["nextCursor"].as_str() else {
            break;
        };
        query.cursor = Some(next.into());
    }
    assert_eq!(seen, 40);
}

#[tokio::test]
async fn receipt_context_uses_original_scope_and_retained_revision_without_live_note() {
    use intent_core::note_receipt_detail::NoteGetReceiptContextRequest;
    let (_dir, store, query) = fixture().await;
    let reference = format!("{KEY}:inverse-detail:0");
    register_reference(&store, &reference).await;
    sqlx::query("INSERT INTO note_operation_detail(operation_key,reference,sequence,value) VALUES(?,?,0,?)")
        .bind(KEY).bind(&reference)
        .bind(json!({"kind":"fragment","id":"text","field":"value","offset":0,"text":"original detail","nextRef":null}).to_string())
        .execute(store.write_pool()).await.unwrap();
    let request: NoteGetReceiptContextRequest = serde_json::from_value(json!({
        "backendId":query.scope.backend_id,"workspaceId":"ws","noteId":"deleted-note","noteInstanceId":"original",
        "sourceRevision":"after","page":{"kind":"context","contextRef":reference}
    })).unwrap();
    let page = store
        .read_note_receipt_context("alice", &request, &json!(1))
        .await
        .unwrap();
    assert_eq!(page["kind"], "noteContextPage");
    assert_eq!(page["sourceRevision"], "after");
    assert_eq!(page["snapshotId"], KEY);
    assert_eq!(page["scope"], serde_json::to_value(&query.scope).unwrap());
    assert_eq!(page["items"][0]["text"], "original detail");
    assert!(page["nextCursor"].is_null());
    for change in 0..4 {
        let mut bad = request.clone();
        match change {
            0 => bad.source_revision = "current".into(),
            1 => bad.note_instance_id = "recreated".into(),
            2 => bad.page.context_ref = format!("{KEY}:missing"),
            _ => bad.page.context_ref = "not-an-owner:detail".into(),
        }
        assert!(store
            .read_note_receipt_context("alice", &bad, &json!(1))
            .await
            .is_err());
    }
    assert!(store
        .read_note_receipt_context("bob", &request, &json!(1))
        .await
        .is_err());
}

#[tokio::test]
async fn receipt_detail_requires_registered_reachability_and_supports_empty_references() {
    let (_dir, store, mut query) = fixture().await;
    query.kind = ReceiptDetailKind::Detail;
    query.reference = format!("{KEY}:empty-children");
    assert!(matches!(
        store
            .read_note_receipt_detail("alice", &query, &json!(1))
            .await,
        Err(Error::NotePage(NotePageError::CursorInvalid))
    ));
    register_reference(&store, &query.reference).await;
    let empty = read(&store, &query).await;
    assert_eq!(empty["items"], json!([]));
    assert!(empty["nextCursor"].is_null());

    query.reference = format!("{KEY}:orphan");
    sqlx::query(
        "INSERT INTO note_operation_detail(operation_key,reference,sequence,value) VALUES(?,?,0,?)",
    )
    .bind(KEY)
    .bind(&query.reference)
    .bind(
        json!({"id":"orphan","parentId":null,"type":"string","value":"must not leak"}).to_string(),
    )
    .execute(store.write_pool())
    .await
    .unwrap();
    assert!(matches!(
        store
            .read_note_receipt_detail("alice", &query, &json!(1))
            .await,
        Err(Error::NotePage(NotePageError::CursorInvalid))
    ));

    let foreign = "33333333-3333-4333-8333-333333333333";
    sqlx::query("INSERT INTO note_operation(operation_key,principal,backend_id,workspace_id,note_id,instance_id,operation_id,payload_digest,admission_expires,retain_until,outcome,converted_count) SELECT ?,'alice',backend_id,workspace_id,note_id,instance_id,?,payload_digest,admission_expires,retain_until,outcome,converted_count FROM note_operation WHERE operation_key=?")
        .bind(foreign).bind(foreign).bind(KEY).execute(store.write_pool()).await.unwrap();
    sqlx::query("INSERT INTO note_operation_reference(operation_key,reference) VALUES(?,?)")
        .bind(foreign)
        .bind(&query.reference)
        .execute(store.write_pool())
        .await
        .unwrap();
    assert!(matches!(
        store
            .read_note_receipt_detail("alice", &query, &json!(1))
            .await,
        Err(Error::NotePage(NotePageError::CursorInvalid))
    ));
    register_reference(&store, &query.reference).await;
    assert_eq!(
        read(&store, &query).await["items"][0]["value"],
        "must not leak"
    );
}

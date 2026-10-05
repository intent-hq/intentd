use super::{cancel_request, request, seal_request, setup, Store};
use intent_core::{
    note_page::NotePageError,
    note_stage::{NoteStageAppend, NoteStageBegin, NoteStageOutput, NoteStageSelection},
    note_stage_read::NoteStageRead,
    Error,
};
use serde_json::{json, Value};

async fn sealed(
    store: &Store,
    query: &str,
    ranges: Option<&[(u64, u64)]>,
) -> (NoteStageBegin, NoteStageRead) {
    let mut begin = request(store).await;
    begin.header.output = NoteStageOutput::Search;
    begin.header.query = Some(
        serde_json::from_value(json!({"text":query,"caseSensitive":false,"mode":"source"}))
            .unwrap(),
    );
    if ranges.is_some() {
        begin.header.selection = NoteStageSelection::Ranges;
    }
    begin.header_digest = begin.computed_digest().unwrap();
    store.begin_note_stage("alice", &begin).await.unwrap();
    if let Some(ranges) = ranges {
        if !ranges.is_empty() {
            let records:Vec<Value>=ranges.iter().enumerate().map(|(i,(start,end))|json!({"kind":"range","ordinal":i,"start":start,"end":end,"anchorAffinity":"before","headAffinity":"after","direction":"forward"})).collect();
            let mut chunk:NoteStageAppend=serde_json::from_value(json!({"backendId":begin.backend_id,"workspaceId":begin.workspace_id,"noteId":begin.note_id,"noteInstanceId":begin.note_instance_id,"operationId":begin.operation_id,"headerDigest":begin.header_digest,"stream":"selection","sequence":0,"previousDigest":null,"records":records,"chunkDigest":"0".repeat(64)})).unwrap();
            chunk.chunk_digest = chunk.computed_digest().unwrap();
            store.append_note_stage("alice", &chunk).await.unwrap();
        }
    }
    store
        .seal_note_stage("alice", &seal_request(store, &begin).await)
        .await
        .unwrap();
    let read:NoteStageRead=serde_json::from_value(json!({"backendId":begin.backend_id,"workspaceId":begin.workspace_id,"noteId":begin.note_id,"noteInstanceId":begin.note_instance_id,"operationId":begin.operation_id,"headerDigest":begin.header_digest,"kind":"search","maxItems":1,"maxSourceBytes":4,"maxWireBytes":4096})).unwrap();
    (begin, read)
}
async fn collect(store: &Store, mut read: NoteStageRead) -> (Vec<Value>, Vec<Value>) {
    let mut hits = Vec::new();
    let mut pages = Vec::new();
    let mut frontier = 0;
    for _ in 0..1000 {
        let page = store
            .read_note_stage_source("alice", &read, &json!("escaped\"id"))
            .await
            .unwrap();
        assert!(
            json!({"jsonrpc":"2.0","id":"escaped\"id","result":page})
                .to_string()
                .len()
                <= 4096
        );
        let next = page["scannedThrough"].as_u64().unwrap();
        assert!(next >= frontier);
        frontier = next;
        hits.extend(page["items"].as_array().unwrap().iter().cloned());
        assert_eq!(page["count"]["value"].as_u64().unwrap(), hits.len() as u64);
        assert_eq!(page["count"]["exact"], page["nextCursor"].is_null());
        read.cursor = page["nextCursor"].as_str().map(str::to_owned);
        assert!(read.cursor.as_ref().is_none_or(|s| s.len() <= 256));
        pages.push(page);
        if read.cursor.is_none() {
            return (hits, pages);
        }
    }
    panic!("search failed to exhaust bounded fixture");
}

#[tokio::test]
async fn staged_search_output_unicode_overlap_and_cursor_replay() {
    for (query, expected) in [
        ("aa", vec![(2, 4), (3, 5), (4, 6)]),
        ("ss", vec![(6, 7), (7, 9)]),
        ("sss", vec![(6, 8)]),
        ("s", vec![(7, 8), (8, 9)]),
    ] {
        let (store, _tmp, mut note) = setup("😀aaaaßSS").await;
        let (_, read) = sealed(&store, query, None).await;
        note.content = "changed after seal".into();
        store.update_note(&note).await.unwrap();
        let first = store
            .read_note_stage_source("alice", &read, &json!(1))
            .await
            .unwrap();
        assert_eq!(
            store
                .read_note_stage_source("alice", &read, &json!(1))
                .await
                .unwrap(),
            first
        );
        let (hits, pages) = collect(&store, read).await;
        let actual: Vec<_> = hits
            .iter()
            .map(|h| {
                (
                    h["sourceRange"]["start"].as_u64().unwrap(),
                    h["sourceRange"]["end"].as_u64().unwrap(),
                )
            })
            .collect();
        assert_eq!(actual, expected);
        assert_eq!(pages.last().unwrap()["scannedThrough"], 9);
        let ids: std::collections::HashSet<_> =
            hits.iter().map(|h| h["hitId"].as_str().unwrap()).collect();
        assert_eq!(ids.len(), hits.len());
        assert!(hits
            .iter()
            .all(|h| h["detailRef"].as_str().unwrap().starts_with("nsh1.")));
    }
}
#[tokio::test]
async fn staged_search_output_union_gaps_and_empty_domain_are_exact() {
    let (store, _tmp, _note) = setup("ab--ababa--AB").await;
    let (_, read) = sealed(
        &store,
        "aba",
        Some(&[(6, 9), (4, 7), (1, 2), (0, 1), (4, 7)]),
    )
    .await;
    let (hits, pages) = collect(&store, read).await;
    assert_eq!(
        hits.iter()
            .map(|h| h["sourceRange"].clone())
            .collect::<Vec<_>>(),
        vec![json!({"start":4,"end":7}), json!({"start":6,"end":9})]
    );
    assert_eq!(pages.last().unwrap()["scannedThrough"], 13);
    let (_, read) = sealed(&store, "b--a", Some(&[(0, 2), (4, 9)])).await;
    assert!(collect(&store, read).await.0.is_empty());
    let (_, read) = sealed(&store, "a", Some(&[])).await;
    let (hits, pages) = collect(&store, read).await;
    assert!(hits.is_empty());
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0]["count"], json!({"value":0,"exact":true}));
}
#[tokio::test]
async fn staged_search_output_binds_cursor_budgets_owner_and_deadline() {
    let (store, _tmp, _note) = setup("aaaaaaaaaaaa").await;
    let (begin, mut read) = sealed(&store, "a", None).await;
    let page = store
        .read_note_stage_source("alice", &read, &json!(1))
        .await
        .unwrap();
    read.cursor = Some(page["nextCursor"].as_str().unwrap().into());
    let mut changed = read.clone();
    changed.max_source_bytes = Some(8);
    assert!(matches!(
        store
            .read_note_stage_source("alice", &changed, &json!(1))
            .await,
        Err(Error::NotePage(NotePageError::CursorInvalid))
    ));
    assert!(matches!(
        store.read_note_stage_source("bob", &read, &json!(1)).await,
        Err(Error::NotePage(NotePageError::CursorInvalid))
    ));
    let mut changed = read.clone();
    let mut token = changed.cursor.unwrap().into_bytes();
    token[12] = if token[12] == b'A' { b'B' } else { b'A' };
    changed.cursor = Some(String::from_utf8(token).unwrap());
    assert!(matches!(
        store
            .read_note_stage_source("alice", &changed, &json!(1))
            .await,
        Err(Error::NotePage(NotePageError::CursorInvalid))
    ));
    store
        .cancel_note_stage("alice", &cancel_request(&begin))
        .await
        .unwrap();
    assert!(matches!(
        store
            .read_note_stage_source("alice", &read, &json!(1))
            .await,
        Err(Error::NotePage(NotePageError::Expired))
    ));
    let (_, read) = sealed(&store, "a", None).await;
    sqlx::query("UPDATE note_operation SET outcome=json_set(outcome,'$.expiresAt','2000-01-01T00:00:00.000Z')").execute(store.write_pool()).await.unwrap();
    assert!(matches!(
        store
            .read_note_stage_source("alice", &read, &json!(1))
            .await,
        Err(Error::NotePage(NotePageError::Expired))
    ));
}
#[tokio::test]
async fn staged_search_output_frame_cut_preserves_every_hit_and_no_hit_progress() {
    let (store, _tmp, _note) = setup(&"a".repeat(200)).await;
    let (_, mut read) = sealed(&store, "a", None).await;
    read.max_items = Some(128);
    read.max_source_bytes = Some(8192);
    let (hits, pages) = collect(&store, read).await;
    assert_eq!(hits.len(), 200);
    assert!(pages.len() > 2);
    assert!(pages
        .iter()
        .any(|p| p["items"].as_array().unwrap().len() > 1));
    for (i, hit) in hits.iter().enumerate() {
        assert_eq!(hit["sourceRange"], json!({"start":i,"end":i+1}));
    }
    let (_, read) = sealed(&store, "z", None).await;
    let (hits, pages) = collect(&store, read).await;
    assert!(hits.is_empty());
    assert!(pages.len() > 1);
    for pair in pages.windows(2) {
        assert!(
            pair[1]["scannedThrough"].as_u64().unwrap()
                > pair[0]["scannedThrough"].as_u64().unwrap()
                || pair[1]["count"]["exact"] == true
        );
    }
}

#[tokio::test]
async fn staged_search_replay_larger_than_page_budget_preserves_scalar_matches() {
    for (source, query, count) in [
        ("a".repeat(70), "a".repeat(32), 39),
        ("😀".repeat(8), "😀".repeat(4), 5),
        ("界".repeat(8), "界".repeat(4), 5),
    ] {
        let (store, _tmp, _note) = setup(&source).await;
        let (_, read) = sealed(&store, &query, None).await;
        let (hits, pages) = collect(&store, read).await;
        assert_eq!(hits.len(), count);
        assert_eq!(
            pages.last().unwrap()["scannedThrough"],
            source.encode_utf16().count()
        );
    }
}

#[tokio::test]
async fn staged_search_detail_uses_emitted_hit_ref_and_preserves_raw_source() {
    let (store, _tmp, mut note) = setup("pre 😀\n\"ßSS post").await;
    let (begin, mut read) = sealed(&store, "😀\n\"ssss", None).await;
    read.max_source_bytes = Some(8192);
    let result = store
        .read_note_stage_source("alice", &read, &json!(1))
        .await
        .unwrap();
    let hit = &result["items"][0];
    assert_eq!(hit["sourceRange"], json!({"start":4,"end":11}));
    let mut raw = serde_json::to_value(&read).unwrap();
    raw["kind"] = json!("detail");
    raw["ref"] = hit["detailRef"].clone();
    raw["maxSourceBytes"] = json!(4);
    let typed: intent_core::note_receipt_detail::NoteOperationReceiptRead =
        serde_json::from_value(raw).unwrap();
    let mut query = typed.query().unwrap();
    note.content = "new current content".into();
    store.update_note(&note).await.unwrap();
    let mut text = String::new();
    for _ in 0..20 {
        let page = store
            .read_note_stage_search_detail("alice", &query, &json!("id\""))
            .await
            .unwrap();
        assert_eq!(page["outputKind"], "detail");
        assert!(page["nextCursor"].is_null());
        assert_eq!(page["sourceLength"], result["sourceLength"]);
        assert_eq!(page["expiresAt"], begin.expires_at);
        let fragment = &page["items"][0];
        assert_eq!(fragment["id"], hit["hitId"]);
        assert_eq!(fragment["offset"], text.encode_utf16().count());
        assert_eq!(fragment["field"], "source");
        let part = fragment["text"].as_str().unwrap();
        assert!(!part.is_empty() && part.len() <= 4);
        text.push_str(part);
        if fragment["nextRef"].is_null() {
            break;
        }
        query.reference = fragment["nextRef"].as_str().unwrap().into();
    }
    assert_eq!(text, "😀\n\"ßSS");
    query.reference = hit["detailRef"].as_str().unwrap().into();
    query.offset = Some(1);
    assert!(matches!(
        store
            .read_note_stage_search_detail("alice", &query, &json!(1))
            .await,
        Err(Error::NotePage(NotePageError::CursorInvalid))
    ));
    query.offset = None;
    assert!(matches!(
        store
            .read_note_stage_search_detail("bob", &query, &json!(1))
            .await,
        Err(Error::NotePage(NotePageError::CursorInvalid))
    ));
    query.cursor = Some("arbitrary".into());
    assert!(matches!(
        store
            .read_note_stage_search_detail("alice", &query, &json!(1))
            .await,
        Err(Error::NotePage(NotePageError::CursorInvalid))
    ));
    query.cursor = None;
    store
        .cancel_note_stage("alice", &cancel_request(&begin))
        .await
        .unwrap();
    assert!(matches!(
        store
            .read_note_stage_search_detail("alice", &query, &json!(1))
            .await,
        Err(Error::NotePage(NotePageError::Expired))
    ));
}

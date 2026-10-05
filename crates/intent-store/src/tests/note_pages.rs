use super::{sample_workspace, stray_note, TempDb};
use crate::Store;
use intent_core::{
    note_page::{NotePageError, NotePageRequest},
    Error, WorkspaceId,
};
use serde_json::{json, Value};
use sqlx::Row;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

mod artifact_crash;
mod artifact_pins;
mod artifact_response;
mod canonical_source;
mod primitive_source_capture;

fn request(value: Value) -> NotePageRequest {
    serde_json::from_value(value).unwrap()
}
async fn page(store: &Store, kind: Value) -> Value {
    store
        .read_note_page("pages", "spec", "alice", request(kind), &json!(1))
        .await
        .unwrap()
}
async fn setup(text: &str) -> (Store, TempDb, intent_core::Note) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::from("pages");
    store
        .insert_workspace(&sample_workspace(&ws, "Pages", false))
        .await
        .unwrap();
    let mut note = stray_note(&ws, "spec", "Large title");
    note.content = text.into();
    store.insert_note(&note).await.unwrap();
    (store, tmp, note)
}
fn assert_error(result: intent_core::Result<Value>, kind: NotePageError) {
    match result.expect_err("page must reject invalid continuation") {
        Error::NotePage(actual) => assert_eq!(actual, kind),
        other => panic!("unexpected page error: {other:?}"),
    }
}

#[tokio::test]
async fn indexed_note_pages_reconstruct_fixture_sources_both_directions() {
    for source in [
        String::new(),
        "A😀e\u{301}\r\n中".into(),
        "**same**\n**same**".into(),
        format!("```language\r\n{}\n```", "😀\t\\\"\r\n".repeat(10000)),
    ] {
        let (store, _tmp, _) = setup(&source).await;
        for backward in [false, true] {
            let mut req = json!({"kind":"source","direction":if backward{"backward"}else{"forward"},"maxSourceBytes":37,"maxWireBytes":4096});
            let mut text = String::new();
            let mut snapshot = None;
            loop {
                let got = page(&store, req).await;
                if let Some(id) = &snapshot {
                    assert_eq!(id, &got["snapshotId"]);
                } else {
                    snapshot = Some(got["snapshotId"].clone());
                }
                let fragment = got["text"].as_str().unwrap();
                assert!(fragment.len() <= 37);
                assert!(
                    json!({"jsonrpc":"2.0","id":1,"result":got})
                        .to_string()
                        .len()
                        <= 4096
                );
                assert_eq!(
                    got["range"]["end"].as_u64().unwrap() - got["range"]["start"].as_u64().unwrap(),
                    fragment.encode_utf16().count() as u64
                );
                if backward {
                    text.insert_str(0, fragment);
                } else {
                    text.push_str(fragment);
                }
                let next = &got[if backward {
                    "previousCursor"
                } else {
                    "nextCursor"
                }];
                if next.is_null() {
                    break;
                }
                req =
                    json!({"kind":"source","cursor":next,"maxSourceBytes":37,"maxWireBytes":4096});
            }
            assert_eq!(text, source);
        }
    }
}

#[tokio::test]
async fn indexed_note_pages_reject_surrogates_scope_budgets_and_stale_metadata() {
    let (store, _tmp, mut note) = setup("A😀\r\n重复".repeat(100).as_str()).await;
    let first = page(&store, json!({"kind":"source","maxSourceBytes":4})).await;
    for at in [2, 90000] {
        assert!(matches!(
            store
                .read_note_page(
                    "pages",
                    "spec",
                    "alice",
                    request(json!({"kind":"source","at":at})),
                    &json!(1)
                )
                .await,
            Err(Error::InvalidParams(_))
        ));
    }
    let cursor = first["nextCursor"].as_str().unwrap();
    for (ws, id, who, budget) in [
        ("pages", "other", "alice", 4),
        ("other", "spec", "alice", 4),
        ("pages", "spec", "bob", 4),
        ("pages", "spec", "alice", 5),
    ] {
        assert_error(
            store
                .read_note_page(
                    ws,
                    id,
                    who,
                    request(json!({"kind":"source","cursor":cursor,"maxSourceBytes":budget})),
                    &json!(1),
                )
                .await,
            NotePageError::CursorInvalid,
        );
    }
    let mut tampered = cursor.to_string();
    tampered.replace_range(0..1, if cursor.starts_with('A') { "B" } else { "A" });
    assert_error(
        store
            .read_note_page(
                "pages",
                "spec",
                "alice",
                request(json!({"kind":"source","cursor":tampered,"maxSourceBytes":4})),
                &json!(1),
            )
            .await,
        NotePageError::CursorInvalid,
    );
    let seek=page(&store,json!({"kind":"source","snapshotId":first["snapshotId"],"sourceRevision":first["sourceRevision"],"noteInstanceId":first["scope"]["noteInstanceId"],"at":3,"maxSourceBytes":4})).await;
    assert_eq!(seek["text"], "\r\n");
    note.title = "metadata changed".into();
    store.update_note_metadata(&note).await.unwrap();
    assert_error(
        store
            .read_note_page(
                "pages",
                "spec",
                "alice",
                request(json!({"kind":"source","cursor":cursor,"maxSourceBytes":4})),
                &json!(1),
            )
            .await,
        NotePageError::Stale,
    );
    assert_eq!(
        page(&store, json!({"kind":"source","maxSourceBytes":4})).await["text"],
        "A"
    );
}

#[tokio::test]
async fn indexed_note_pages_restart_and_recreation_retire_handles() {
    let (store, tmp, note) = setup(&"same".repeat(5000)).await;
    let first = page(&store, json!({"kind":"source"})).await;
    let reopened = Store::open(&tmp.path).await.unwrap();
    assert_error(
        reopened
            .read_note_page(
                "pages",
                "spec",
                "alice",
                request(json!({"kind":"source","cursor":first["nextCursor"]})),
                &json!(1),
            )
            .await,
        NotePageError::Expired,
    );
    let fresh = page(&reopened, json!({"kind":"source"})).await;
    assert_eq!(fresh["scope"], first["scope"]);
    store
        .delete_note(&note.workspace_id, &note.id)
        .await
        .unwrap();
    store.insert_note(&note).await.unwrap();
    let recreated = page(&store, json!({"kind":"source"})).await;
    assert_ne!(
        first["scope"]["noteInstanceId"],
        recreated["scope"]["noteInstanceId"]
    );
    assert_error(
        store
            .read_note_page(
                "pages",
                "spec",
                "alice",
                request(json!({"kind":"source","cursor":first["nextCursor"]})),
                &json!(1),
            )
            .await,
        NotePageError::Stale,
    );
}

#[tokio::test]
async fn indexed_note_pages_rollback_keeps_source_index_and_metadata_together() {
    let (store, _tmp, mut note) = setup("before").await;
    let first = page(&store, json!({"kind":"source"})).await;
    sqlx::raw_sql("CREATE TRIGGER fail_page_index BEFORE INSERT ON note_page_piece BEGIN SELECT RAISE(ABORT,'injected index fault'); END;").execute(store.write_pool()).await.unwrap();
    note.content = "after".into();
    assert!(store.update_note(&note).await.is_err());
    let got = page(&store, json!({"kind":"source"})).await;
    assert_eq!(got["text"], "before");
    assert_eq!(got["sourceRevision"], first["sourceRevision"]);
    assert_eq!(
        store
            .get_note(&note.workspace_id, &note.id)
            .await
            .unwrap()
            .content,
        "before"
    );
}

#[tokio::test]
async fn indexed_note_pages_metadata_tree_and_giant_fragments_are_lossless() {
    let (store, _tmp, mut note) = setup("**same**\n\n**same**").await;
    note.title = "😀\t\"\\".repeat(12000);
    note.tags = vec!["重复".repeat(4000), "second".into()];
    store.update_note_metadata(&note).await.unwrap();
    let source = page(&store, json!({"kind":"source"})).await;
    let root = page(
        &store,
        json!({"kind":"metadata","ref":source["metadataRef"],"maxItems":1,"maxWireBytes":4096}),
    )
    .await;
    let reference = root["items"][0]["childrenRef"].clone();
    let mut req = json!({"kind":"metadata","ref":reference,"maxItems":1,"maxWireBytes":4096});
    let mut title = None;
    let mut keys = Vec::new();
    loop {
        let got = page(&store, req).await;
        for item in got["items"].as_array().unwrap() {
            keys.push(item["key"].as_str().unwrap().to_string());
            if item["key"] == "title" {
                title = Some(item["valueRef"].clone());
            }
        }
        if got["nextCursor"].is_null() {
            break;
        }
        req = json!({"kind":"metadata","ref":reference,"cursor":got["nextCursor"],"maxItems":1,"maxWireBytes":4096});
    }
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
    let mut reference = title.unwrap();
    let mut text = String::new();
    loop {
        let got = page(
            &store,
            json!({"kind":"context","contextRef":reference,"maxWireBytes":4096}),
        )
        .await;
        assert!(
            json!({"jsonrpc":"2.0","id":1,"result":got})
                .to_string()
                .len()
                <= 4096
        );
        let item = &got["items"][0];
        assert_eq!(item["offset"], text.encode_utf16().count());
        text.push_str(item["text"].as_str().unwrap());
        if item["nextRef"].is_null() {
            break;
        }
        reference = item["nextRef"].clone();
    }
    assert_eq!(text, note.title);
    let context = page(
        &store,
        json!({"kind":"context","contextRef":source["contextRef"]}),
    )
    .await;
    let marked: Vec<_> = context["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["construct"] == "strong")
        .collect();
    assert_eq!(marked.len(), 2);
    assert_ne!(marked[0]["id"], marked[1]["id"]);
}

#[tokio::test]
async fn indexed_note_seek_measures_actual_sql_work_independent_of_note_extent() {
    for bytes in [32_768, 4_194_304] {
        let started = std::time::Instant::now();
        let (mut store, tmp, _) = setup(&"x".repeat(bytes)).await;
        let construction_ms = started.elapsed().as_millis();
        // Single connection guarantees the actual production reads execute under
        // SQLite's VM progress counter, not a mocked query-cost estimate.
        store.read_pool.close().await;
        store.read_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                sqlx::sqlite::SqliteConnectOptions::new()
                    .filename(&tmp.path)
                    .read_only(true),
            )
            .await
            .unwrap();
        let steps = Arc::new(AtomicUsize::new(0));
        let counter = steps.clone();
        {
            let mut conn = store.read_pool.acquire().await.unwrap();
            conn.lock_handle()
                .await
                .unwrap()
                .set_progress_handler(1, move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                    true
                });
        }
        let _cold = page(
            &store,
            json!({"kind":"source","at":bytes-127,"maxSourceBytes":127}),
        )
        .await;
        let cold_steps = steps.swap(0, Ordering::Relaxed);
        let got = page(
            &store,
            json!({"kind":"source","at":bytes-127,"maxSourceBytes":127}),
        )
        .await;
        let measured = steps.load(Ordering::Relaxed);
        {
            let mut conn = store.read_pool.acquire().await.unwrap();
            conn.lock_handle().await.unwrap().remove_progress_handler();
        }
        assert_eq!(got["text"].as_str().unwrap().len(), 127);
        assert!(
            measured < 250,
            "{bytes} bytes took {measured} SQLite VM steps"
        );
        let rows=sqlx::query("SELECT count(*) AS pieces,max(length(CAST(text AS BLOB))) AS max_piece,sum(length(CAST(text AS BLOB))) AS stored FROM note_page_piece").fetch_one(store.read_pool()).await.unwrap();
        let plan:Vec<String>=sqlx::query("EXPLAIN QUERY PLAN SELECT start,end,text FROM note_page_piece WHERE workspace_id=? AND note_id=? AND start<=? ORDER BY start DESC LIMIT 1").bind("pages").bind("spec").bind(i64::try_from(bytes).unwrap()).fetch_all(store.read_pool()).await.unwrap().iter().map(|r|r.get::<String,_>(3)).collect();
        assert!(
            plan.iter()
                .any(|p| p.contains("SEARCH") && p.contains("start<?")),
            "{plan:?}"
        );
        assert_eq!(rows.get::<i64, _>("stored"), i64::try_from(bytes).unwrap());
        assert!(rows.get::<i64, _>("max_piece") <= 4096);
        let entries = sqlx::query("SELECT count(*) AS count,sum(length(CAST(value AS BLOB))) AS bytes FROM note_page_entry").fetch_one(store.read_pool()).await.unwrap();
        let database_pages: i64 = sqlx::query_scalar("PRAGMA page_count")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
        eprintln!("construction_with_migrations_ms={construction_ms} entry_rows={} entry_json_bytes={} database_pages={database_pages}", entries.get::<i64,_>("count"),entries.get::<i64,_>("bytes"));
        eprintln!("note_bytes={bytes} cold_vm_steps={cold_steps} vm_steps={measured} returned_source=127 piece_rows={} max_piece_bytes={} plan={plan:?}",rows.get::<i64,_>("pieces"),rows.get::<i64,_>("max_piece"));
    }
}

#[tokio::test]
async fn indexed_note_pages_migrate_existing_notes_and_preserve_legacy_text() {
    let tmp = TempDb::new();
    let pool = crate::connect_write(&tmp.path).await.unwrap();
    let old = sqlx::migrate::Migrator {
        migrations: std::borrow::Cow::Owned(
            crate::MIGRATOR
                .iter()
                .filter(|m| m.version < 148)
                .cloned()
                .collect(),
        ),
        ..sqlx::migrate::Migrator::DEFAULT
    };
    old.run(&pool).await.unwrap();
    sqlx::query("INSERT INTO workspace(id,title,branch,created_at,updated_at) VALUES ('pages','migration','main','now','now')").execute(&pool).await.unwrap();
    let text = "A😀\r\n旧".repeat(3000);
    sqlx::query("INSERT INTO note(id,workspace_id,title,content,rev,created_at,updated_at) VALUES ('spec','pages','original',?,17,'now','now')").bind(&text).execute(&pool).await.unwrap();
    pool.close().await;
    let store = Store::open(&tmp.path).await.unwrap();
    let got = page(&store, json!({"kind":"source","at":3,"maxSourceBytes":4})).await;
    assert!(got["sourceRevision"].as_str().unwrap().starts_with("r:17:"));
    assert_eq!(got["text"], "\r\n");
    assert_eq!(
        store
            .get_note(
                &WorkspaceId::from("pages"),
                &intent_core::NoteId::from("spec")
            )
            .await
            .unwrap()
            .content,
        text
    );
    let index_bytes: i64 =
        sqlx::query_scalar("SELECT sum(length(CAST(text AS BLOB))) FROM note_page_piece")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(index_bytes, i64::try_from(text.len()).unwrap());
}

#[tokio::test]
async fn indexed_note_context_preserves_fences_links_markers_and_continuation_details() {
    let source=format!("```{}\r\n{}\r\n```\n\n- **nested**\n  - list\n\n| a | b |\n|---|---|\n| c | d |\n\n[link](https://example.test/{})\n\n<!--anchor:abc:start-->marked<!--anchor:abc:end-->","language".repeat(1000),"body😀".repeat(6000),"long".repeat(2000));
    let (store, _tmp, _) = setup(&source).await;
    let source_page = page(
        &store,
        json!({"kind":"source","at":12001,"maxSourceBytes":4}),
    )
    .await;
    let context = page(
        &store,
        json!({"kind":"context","contextRef":source_page["contextRef"],"maxWireBytes":4096}),
    )
    .await;
    let fence = context["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["construct"] == "codeBlock")
        .expect("enclosing giant fence");
    assert_eq!(fence["continuationBefore"], true);
    assert_eq!(fence["continuationAfter"], true);
    let details = page(
        &store,
        json!({"kind":"context","contextRef":fence["detailRef"]}),
    )
    .await;
    let header = details["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["field"] == "openingSource")
        .unwrap();
    let mut reference = header["nextRef"].clone();
    let mut opening = String::new();
    loop {
        let got = page(
            &store,
            json!({"kind":"context","contextRef":reference,"maxWireBytes":4096}),
        )
        .await;
        let fragment = &got["items"][0];
        opening.push_str(fragment["text"].as_str().unwrap());
        if fragment["nextRef"].is_null() {
            break;
        }
        reference = fragment["nextRef"].clone();
    }
    assert_eq!(opening, format!("```{}\r\n", "language".repeat(1000)));
    let tail = page(&store, json!({"kind":"source","direction":"backward"})).await;
    let context = page(
        &store,
        json!({"kind":"context","contextRef":tail["contextRef"]}),
    )
    .await;
    assert!(context["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|i| i["role"] == "commentMarker"));
}

#[tokio::test]
async fn indexed_note_pages_count_full_escaped_frames_and_empty_exhaustion() {
    let (store, _tmp, note) = setup(&"\u{1}\t\"\\😀\r\n".repeat(8000)).await;
    let got = store
        .read_note_page(
            "pages",
            "spec",
            "alice",
            request(json!({"kind":"source","maxWireBytes":4096})),
            &json!("\u{1}".repeat(64)),
        )
        .await
        .unwrap();
    assert!(
        json!({"jsonrpc":"2.0","id":"\u{1}".repeat(64),"result":got})
            .to_string()
            .len()
            <= 4096
    );
    assert!(!got["text"].as_str().unwrap().is_empty());
    assert!(got["text"].as_str().unwrap().len() < 4096);
    let end = page(
        &store,
        json!({"kind":"source","at":note.content.encode_utf16().count()}),
    )
    .await;
    assert_eq!(end["text"], "");
    assert!(end["nextCursor"].is_null());
    let start = page(
        &store,
        json!({"kind":"source","at":0,"direction":"backward"}),
    )
    .await;
    assert_eq!(start["text"], "");
    assert!(start["previousCursor"].is_null());
    assert!(!start["nextCursor"].is_null());
}

#[tokio::test]
async fn indexed_note_task_ids_preserve_raw_legacy_membership_order_and_fragments() {
    let giant = "😀/raw%20?".repeat(1000);
    let source = format!("😀 prose [one](intent://local/task/raw%20?x) [two](intent://local/task/nonexistent)\n```\n[duplicate](intent://local/task/raw%20?x)\n```\n- [ ] [wide](intent://local/task/{giant})\n[x](intent://local/task/line\nbreak) [] (intent://local/task/nope)");
    let (store, _tmp, mut note) = setup(&source).await;
    let first = page(
        &store,
        json!({"kind":"taskIds","maxItems":1,"maxWireBytes":4096}),
    )
    .await;
    assert_eq!(first["kind"], "noteTaskIdsPage");
    assert_eq!(first["totalItems"], 4);
    assert_eq!(first["items"][0]["taskNoteId"], "raw%20?x");
    let start = source[..source.find("raw%20?x").unwrap()]
        .encode_utf16()
        .count();
    assert_eq!(
        first["items"][0]["sourceRange"],
        json!({"start":start,"end":start+8})
    );
    let mut current = first.clone();
    let mut ids = Vec::new();
    loop {
        let item = &current["items"][0];
        assert_eq!(item["index"], ids.len());
        let id = if let Some(text) = item["taskNoteId"].as_str() {
            text.to_owned()
        } else {
            let mut reference = item["taskNoteIdRef"].clone();
            let mut text = String::new();
            while !reference.is_null() {
                let fragment = page(
                    &store,
                    json!({"kind":"context","contextRef":reference,"maxWireBytes":4096}),
                )
                .await;
                assert_eq!(fragment["items"][0]["field"], "taskNoteId");
                text.push_str(fragment["items"][0]["text"].as_str().unwrap());
                reference = fragment["items"][0]["nextRef"].clone();
            }
            text
        };
        ids.push(id);
        if current["nextCursor"].is_null() {
            break;
        }
        current = page(&store,json!({"kind":"taskIds","cursor":current["nextCursor"],"maxItems":1,"maxWireBytes":4096})).await;
        assert_eq!(current["snapshotId"], first["snapshotId"]);
    }
    assert_eq!(
        ids,
        vec![
            "raw%20?x".to_owned(),
            "nonexistent".into(),
            giant,
            "line\nbreak".into()
        ]
    );
    note.content = "[new](intent://local/task/replacement)".into();
    store.update_note(&note).await.unwrap();
    assert_error(store.read_note_page("pages","spec","alice",request(json!({"kind":"taskIds","cursor":first["nextCursor"],"maxItems":1,"maxWireBytes":4096})),&json!(1)).await,NotePageError::Stale);
    let replacement = page(&store, json!({"kind":"taskIds"})).await;
    assert_eq!(replacement["totalItems"], 1);
    assert_eq!(replacement["items"][0]["taskNoteId"], "replacement");
}

#[tokio::test]
async fn indexed_note_pages_parent_deletion_refreshes_surviving_child_metadata() {
    let (store, _tmp, mut child) = setup("child text remains exact").await;
    let parent = stray_note(&WorkspaceId::from("pages"), "parent", "Parent");
    store.insert_note(&parent).await.unwrap();
    child.parent_id = Some(parent.id.clone());
    store.update_note(&child).await.unwrap();
    let before = page(&store, json!({"kind":"source","maxSourceBytes":4})).await;
    store
        .delete_note(&parent.workspace_id, &parent.id)
        .await
        .unwrap();
    let after = page(&store, json!({"kind":"source"})).await;
    assert_eq!(after["text"], child.content);
    assert_ne!(after["sourceRevision"], before["sourceRevision"]);
    let root = page(
        &store,
        json!({"kind":"metadata","ref":after["metadataRef"]}),
    )
    .await;
    let fields = page(
        &store,
        json!({"kind":"metadata","ref":root["items"][0]["childrenRef"]}),
    )
    .await;
    let parent = fields["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["key"] == "parentId")
        .unwrap();
    assert_eq!(parent["type"], "null");
    assert_error(
        store
            .read_note_page(
                "pages",
                "spec",
                "alice",
                request(json!({"kind":"source","cursor":before["nextCursor"],"maxSourceBytes":4})),
                &json!(1),
            )
            .await,
        NotePageError::Stale,
    );
}

#[tokio::test]
async fn indexed_note_pages_workspace_delete_sweeps_derived_rows_before_note_cascade() {
    let (store, _tmp, _) = setup(&"**repeated**\n\n".repeat(2000)).await;
    sqlx::raw_sql("CREATE TRIGGER reject_page_cascade BEFORE DELETE ON note WHEN EXISTS(SELECT 1 FROM note_page_piece WHERE workspace_id=old.workspace_id AND note_id=old.id) OR EXISTS(SELECT 1 FROM note_page_entry WHERE workspace_id=old.workspace_id AND note_id=old.id) BEGIN SELECT RAISE(ABORT,'unbounded page cascade'); END;").execute(store.write_pool()).await.unwrap();
    store
        .delete_workspace(&WorkspaceId::from("pages"))
        .await
        .unwrap();
    for table in ["note_page_head", "note_page_piece", "note_page_entry"] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(store.read_pool())
            .await
            .unwrap();
        assert_eq!(count, 0, "{table}");
    }
}

#[tokio::test]
async fn indexed_note_pages_ancestor_references_keep_the_source_window() {
    let text = format!("> {}\n", "long quoted paragraph ".repeat(1000));
    let (store, _tmp, _) = setup(&text).await;
    let source = page(
        &store,
        json!({"kind":"source","at":10000,"maxSourceBytes":16}),
    )
    .await;
    let context = page(
        &store,
        json!({"kind":"context","contextRef":source["contextRef"]}),
    )
    .await;
    let paragraph = context["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["construct"] == "paragraph")
        .unwrap();
    assert_eq!(paragraph["continuationBefore"], true);
    let parent = page(
        &store,
        json!({"kind":"context","contextRef":paragraph["parentRef"]}),
    )
    .await;
    assert_eq!(parent["items"][0]["construct"], "blockquote");
    assert_eq!(parent["items"][0]["continuationBefore"], true);
    assert_eq!(parent["items"][0]["continuationAfter"], true);
}

#[tokio::test]
async fn indexed_table_positions_address_far_cells_without_preceding_rows() {
    let mut text = String::from("| headA | headB | headC |\r\n| :--- | :---: | ---: |\r\n");
    text.push_str(&"| same | same | same |\r\n".repeat(1000));
    text.push_str("| ");
    text.push_str(&"😀x".repeat(420_000));
    text.push_str(" | same | target😀 |\r\n\r\n| second |\r\n| --- |\r\n| tail |\r\n");
    let (store, _tmp, _) = setup(&text).await;
    let mut first_table_id = None;
    for (needle, row, col, alignment, second) in [
        ("headC", 0, 2, "right", false),
        ("same", 1, 0, "left", false),
        ("target😀", 1001, 2, "right", false),
        ("second", 0, 0, "none", true),
        ("tail", 1, 0, "none", true),
    ] {
        let byte = text.find(needle).unwrap();
        let at = text[..byte].encode_utf16().count();
        let source = page(&store, json!({"kind":"source","at":at,"maxSourceBytes":16})).await;
        let mut request = json!({"kind":"context","contextRef":source["contextRef"],"maxWireBytes":4096,"maxItems":8});
        let cell = loop {
            let context = page(&store, request.clone()).await;
            assert!(
                json!({"jsonrpc":"2.0","id":1,"result":context})
                    .to_string()
                    .len()
                    <= 4096
            );
            if let Some(cell) = context["items"].as_array().unwrap().iter().find(|item| {
                item["construct"] == "tableCell"
                    && item["sourceRange"]["start"].as_u64().unwrap() <= at as u64
                    && item["sourceRange"]["end"].as_u64().unwrap() > at as u64
            }) {
                break cell.clone();
            }
            assert!(!context["nextCursor"].is_null(), "cell at {at}");
            request["cursor"] = context["nextCursor"].clone();
        };
        let position = &cell["tablePosition"];
        assert_eq!(position["rowIndex"], row);
        assert_eq!(position["columnIndex"], col);
        assert_eq!(position["alignment"], alignment);
        assert!(position["tableRef"].as_str().unwrap().len() <= 256);
        let owner = page(
            &store,
            json!({"kind":"context","contextRef":position["tableRef"]}),
        )
        .await;
        assert_eq!(owner["scope"], source["scope"]);
        assert_eq!(owner["sourceRevision"], source["sourceRevision"]);
        assert_eq!(owner["items"][0]["construct"], "table");
        let id = owner["items"][0]["id"].clone();
        if let Some(first) = &first_table_id {
            assert_eq!(first != &id, second);
        } else {
            first_table_id = Some(id);
        }
        let parent = page(
            &store,
            json!({"kind":"context","contextRef":cell["parentRef"]}),
        )
        .await;
        assert_eq!(parent["items"][0]["tablePosition"]["rowIndex"], row);
        assert_eq!(
            parent["items"][0]["construct"],
            if row == 0 { "tableHead" } else { "tableRow" }
        );
    }
}

#[tokio::test]
async fn indexed_html_far_cells_follow_canonical_unit_grid() {
    // The frontend production entry oracle proves that equal-length earlier
    // cells change TARGET's native column without changing its source offset.
    let opening = "<table><tbody><tr><td>";
    let extra = "PREVIOUS</td><td>";
    let closing = "</td><td>TARGET</td></tr></tbody></table>";
    let width = 2_100_000;
    let sources = [
        format!("{opening}{}{closing}", "x".repeat(width)),
        format!(
            "{opening}{extra}{}{closing}",
            "x".repeat(width - extra.len())
        ),
    ];
    assert_eq!(sources[0].find("TARGET"), sources[1].find("TARGET"));
    for (index, text) in sources.iter().enumerate() {
        let (store, _tmp, _) = setup(text).await;
        let at = text.find("TARGET").unwrap();
        let source = page(
            &store,
            json!({"kind":"source","at":at,"maxSourceBytes":16,"maxWireBytes":4096}),
        )
        .await;
        let mut request = json!({"kind":"context","contextRef":source["contextRef"],"maxWireBytes":4096,"maxItems":8});
        let cell = loop {
            let context = page(&store, request.clone()).await;
            assert!(
                serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":context}))
                    .unwrap()
                    .len()
                    <= 4096
            );
            if let Some(cell) = context["items"].as_array().unwrap().iter().find(|item| {
                item["construct"] == "htmlTableCell"
                    && item["htmlSource"]["bodyRange"]["start"] == at
            }) {
                break cell.clone();
            }
            assert!(
                !context["nextCursor"].is_null(),
                "missing canonical HTML cell at {at}"
            );
            request["cursor"] = context["nextCursor"].clone();
        };
        assert_eq!(cell["htmlPosition"]["profile"], "canonicalNote");
        assert_eq!(cell["htmlPosition"]["profileVersion"], 1);
        assert_eq!(cell["htmlPosition"]["rowIndex"], 0);
        assert_eq!(cell["htmlPosition"]["columnIndex"], index + 1);
        assert_eq!(cell["htmlPosition"]["cellRole"], "data");
        assert_eq!(cell["htmlSource"]["bodyRange"]["end"], at + 6);
        for field in ["nativeRef", "attributesRef", "sourceMapRef"] {
            assert!(cell[field].as_str().is_some_and(|s| s.len() <= 256));
        }
        let native = page(
            &store,
            json!({"kind":"context","contextRef":cell["nativeRef"],"maxWireBytes":4096}),
        )
        .await;
        assert_eq!(native["items"][0]["kind"], "nativeNode");
        assert_eq!(native["items"][0]["nodeType"], "tableCell");
        assert_eq!(native["items"][0]["childIndex"], index + 1);
        let owner = page(&store, json!({"kind":"context","contextRef":cell["htmlPosition"]["tableRef"],"maxWireBytes":4096})).await;
        assert_eq!(owner["items"][0]["construct"], "htmlTable");
    }
}

// Exact protocol9b886 invariants apply to every captured native map, including
// huge-table omitted syntax and Markdown entity/line-break endpoint probes.
fn assert_source_map_ranges(map: &Value) {
    if map["kind"] != "sourceMap" {
        return;
    }
    if map["mapping"] != "projection" {
        assert!(
            map["sourceRange"]["end"].as_u64().unwrap()
                > map["sourceRange"]["start"].as_u64().unwrap(),
            "non-projection map has empty raw range: {map}"
        );
    }
    if map["mapping"] != "omitted" {
        assert!(
            map["renderedRange"]["end"].as_u64().unwrap()
                > map["renderedRange"]["start"].as_u64().unwrap(),
            "non-omitted map has empty rendered range: {map}"
        );
        assert!(map["textRef"].is_string());
    }
}

async fn record_page(store: &Store, request: Value, transcript: &mut Vec<Value>) -> Value {
    let response = page(store, request.clone()).await;
    for item in response["items"].as_array().into_iter().flatten() {
        assert_source_map_ranges(item);
    }
    let limit = usize::try_from(request["maxWireBytes"].as_u64().unwrap_or(65_536)).unwrap();
    assert!(
        serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":response}))
            .unwrap()
            .len()
            <= limit
    );
    transcript.push(json!({"request":request,"response":response}));
    response
}

// This exhaustive traversal is a tiny-fixture capture utility, never a reader
// implementation or proof that a production consumer should drain every ref.
async fn record_fixture_closure(store: &Store, items: &Value, transcript: &mut Vec<Value>) {
    let mut queue = Vec::new();
    resource_refs(items, &mut queue);
    let mut seen = std::collections::BTreeSet::new();
    while let Some((kind, reference)) = queue.pop() {
        if !seen.insert(reference.clone()) {
            continue;
        }
        assert!(
            seen.len() < 128,
            "tiny fixture resource closure must remain finite"
        );
        let mut request = json!({"kind":kind,"maxWireBytes":8192,"maxItems":64});
        request[if kind == "metadata" {
            "ref"
        } else {
            "contextRef"
        }] = json!(reference);
        loop {
            let response = record_page(store, request.clone(), transcript).await;
            resource_refs(&response["items"], &mut queue);
            if response["nextCursor"].is_null() {
                break;
            }
            request["cursor"] = response["nextCursor"].clone();
        }
    }
}

fn resource_refs(value: &Value, refs: &mut Vec<(String, String)>) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                if key.ends_with("Ref") {
                    if let Some(reference) = value.as_str() {
                        refs.push((
                            if matches!(key.as_str(), "attributesRef" | "marksRef" | "childrenRef")
                            {
                                "metadata"
                            } else {
                                "context"
                            }
                            .into(),
                            reference.into(),
                        ));
                    }
                } else {
                    resource_refs(value, refs);
                }
            }
        }
        Value::Array(array) => {
            for value in array {
                resource_refs(value, refs);
            }
        }
        _ => {}
    }
}

#[tokio::test]
async fn indexed_html_native_resources_replay_exact_frontend_far_table_fixture() {
    for role in ["td", "th"] {
        let source=format!("<table><tr><td>{}</td><td>SECOND</td><{role}><strong>TARGET</strong></{role}></tr></table>","x".repeat(2_000_000));
        let at = source.find("TARGET").unwrap();
        let (store, _tmp, mut note) = setup(&source).await;
        let mut transcript = Vec::new();
        let first=record_page(&store,json!({"kind":"source","at":at,"maxSourceBytes":4096,"maxWireBytes":8192,"maxItems":64}),&mut transcript).await;
        assert_eq!(first["text"], source[at..]);
        let mut queue = vec![(
            "context".to_owned(),
            first["contextRef"].as_str().unwrap().to_owned(),
        )];
        let mut seen = std::collections::BTreeSet::new();
        while let Some((kind, reference)) = queue.pop() {
            if !seen.insert(reference.clone()) {
                continue;
            }
            assert!(seen.len() < 256, "bounded fixture graph");
            let mut request = json!({"kind":kind,"maxWireBytes":8192,"maxItems":64});
            request[if kind == "metadata" {
                "ref"
            } else {
                "contextRef"
            }] = json!(reference);
            loop {
                let response = record_page(&store, request.clone(), &mut transcript).await;
                assert_eq!(response["snapshotId"], first["snapshotId"]);
                if reference == first["contextRef"].as_str().unwrap() {
                    for item in response["items"].as_array().unwrap() {
                        let start = item["sourceRange"]["start"].as_u64().unwrap();
                        let end = item["sourceRange"]["end"].as_u64().unwrap();
                        assert!(
                            start < first["range"]["end"].as_u64().unwrap() && end > at as u64,
                            "window at {at} admitted prior/nonoverlapping context {item}"
                        );
                    }
                }
                resource_refs(&response["items"], &mut queue);
                if response["nextCursor"].is_null() {
                    break;
                }
                request["cursor"] = response["nextCursor"].clone();
            }
        }
        let items: Vec<&Value> = transcript
            .iter()
            .filter_map(|call| call["response"]["items"].as_array())
            .flatten()
            .collect();
        let target_map = *items
            .iter()
            .find(|item| {
                item["kind"] == "sourceMap"
                    && item["mapping"] == "identity"
                    && item["sourceRange"]["start"] == at
                    && item["sourceRange"]["end"] == at + 6
            })
            .expect("exact target map");
        let target = items
            .iter()
            .find(|item| item["kind"] == "nativeNode" && item["id"] == target_map["textNodeId"])
            .expect("resolved text node");
        assert_eq!(target["nodeType"], "text");
        let marks_call = transcript
            .iter()
            .find(|call| call["request"]["ref"] == target["marksRef"])
            .expect("resolved marks");
        assert_eq!(marks_call["response"]["items"][0]["type"], "array");
        assert!(items
            .iter()
            .any(|item| item["kind"] == "fragment" && item["text"] == "bold"));
        assert!(items.iter().any(|item| item["kind"] == "fragment"
            && item["field"] == "renderedText"
            && item["text"] == "TARGET"));
        let cell = items
            .iter()
            .find(|item| {
                item["construct"] == "htmlTableCell" && item["htmlPosition"]["columnIndex"] == 2
            })
            .expect("third cell");
        assert_eq!(
            cell["htmlPosition"]["cellRole"],
            if role == "th" { "header" } else { "data" }
        );
        let native = transcript
            .iter()
            .find(|call| call["request"]["contextRef"] == cell["nativeRef"])
            .unwrap();
        assert_eq!(native["response"]["items"][0]["childIndex"], 2);
        assert_eq!(
            native["response"]["items"][0]["nodeType"],
            if role == "th" {
                "tableHeader"
            } else {
                "tableCell"
            }
        );
        let owner = transcript
            .iter()
            .find(|call| call["request"]["contextRef"] == cell["htmlPosition"]["tableRef"])
            .unwrap();
        for field in ["sourceMapRef", "continuationBefore", "continuationAfter"] {
            assert!(owner["response"]["items"][0].get(field).is_none());
        }
        let rendered_bytes: usize = items
            .iter()
            .filter(|item| item["field"] == "renderedText")
            .map(|item| item["text"].as_str().unwrap().len())
            .sum();
        assert!(
            rendered_bytes <= 64,
            "read a preceding giant cell: {rendered_bytes}"
        );
        if let Ok(directory) = std::env::var("NOTE_PAGE_TRANSCRIPT_DIR") {
            let path =
                std::path::Path::new(&directory).join(format!("canonical-table-{role}.json"));
            std::fs::write(path,serde_json::to_vec_pretty(&json!({"fixture":{"role":role,"repeatCount":2_000_000},"sourceLength":source.len(),"at":at,"calls":transcript})).unwrap()).unwrap();
        }
        eprintln!(
            "html_role={role} source_bytes={} resource_calls={} rendered_bytes={rendered_bytes}",
            source.len(),
            transcript.len()
        );
        let stale = cell["htmlPosition"]["tableRef"].clone();
        note.title = "metadata changed".into();
        store.update_note(&note).await.unwrap();
        assert_error(
            store
                .read_note_page(
                    "pages",
                    "spec",
                    "alice",
                    request(json!({"kind":"context","contextRef":stale})),
                    &json!(1),
                )
                .await,
            NotePageError::Stale,
        );
    }
}

#[tokio::test]
async fn indexed_html_map_seek_clips_giant_identity_text_with_bounded_sql_work() {
    let mut costs = Vec::new();
    for count in [10_000, 500_000] {
        let source = format!("<table><tr><td>{}</td></tr></table>", "😀x".repeat(count));
        let started = std::time::Instant::now();
        let (mut store, tmp, _) = setup(&source).await;
        let construction_ms = started.elapsed().as_millis();
        store.read_pool.close().await;
        store.read_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                sqlx::sqlite::SqliteConnectOptions::new()
                    .filename(&tmp.path)
                    .read_only(true),
            )
            .await
            .unwrap();
        let at = 14 + (count - 5) * 3;
        let first = page(
            &store,
            json!({"kind":"source","at":at,"maxSourceBytes":10,"maxWireBytes":4096}),
        )
        .await;
        let context = page(
            &store,
            json!({"kind":"context","contextRef":first["contextRef"]}),
        )
        .await;
        let table = context["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["construct"] == "htmlTable")
            .unwrap();
        let request =
            json!({"kind":"context","contextRef":table["sourceMapRef"],"maxWireBytes":4096});
        let _warm = page(&store, request.clone()).await;
        let steps = Arc::new(AtomicUsize::new(0));
        let counter = steps.clone();
        {
            let mut connection = store.read_pool.acquire().await.unwrap();
            connection
                .lock_handle()
                .await
                .unwrap()
                .set_progress_handler(1, move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                    true
                });
        }
        let maps = page(&store, request).await;
        let cost = steps.load(Ordering::Relaxed);
        {
            let mut connection = store.read_pool.acquire().await.unwrap();
            connection
                .lock_handle()
                .await
                .unwrap()
                .remove_progress_handler();
        }
        costs.push(cost);
        assert!(cost < 500, "{count} repeats: {cost} VM steps");
        let mut reconstructed = String::new();
        for map in maps["items"].as_array().unwrap() {
            assert_eq!(map["mapping"], "identity");
            assert!(map["sourceRange"]["start"].as_u64().unwrap() >= at as u64);
            assert!(
                map["sourceRange"]["end"].as_u64().unwrap()
                    <= first["range"]["end"].as_u64().unwrap()
            );
            if map["textRef"].is_null() {
                continue;
            }
            let text = page(
                &store,
                json!({"kind":"context","contextRef":map["textRef"],"maxWireBytes":4096}),
            )
            .await;
            assert_eq!(text["items"][0]["offset"], 0);
            assert!(text["items"][0]["nextRef"].is_null());
            reconstructed.push_str(text["items"][0]["text"].as_str().unwrap());
        }
        assert_eq!(reconstructed, first["text"]);
        let rows=sqlx::query("SELECT count(*) AS count,sum(length(CAST(value AS BLOB))) AS bytes FROM note_page_entry").fetch_one(store.read_pool()).await.unwrap();
        eprintln!("html_bytes={} build_with_migrations_ms={construction_ms} map_vm_steps={cost} map_rows={} returned_rendered_bytes={} entry_rows={} entry_json_bytes={}",source.len(),maps["items"].as_array().unwrap().len(),reconstructed.len(),rows.get::<i64,_>("count"),rows.get::<i64,_>("bytes"));
    }
    assert!(
        costs[1] <= costs[0] + 30,
        "map query work grew with body extent: {costs:?}"
    );
}

#[tokio::test]
async fn indexed_html_profile_retirement_rebuilds_without_changing_raw_revision() {
    let (store, tmp, _) = setup("<table><tr><td>one &amp; two</td></tr></table>").await;
    let original = page(&store, json!({"kind":"source"})).await;
    let revision = original["sourceRevision"].clone();
    sqlx::query("UPDATE note_page_head SET profile_revision='retired-build'")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert_error(
        store
            .read_note_page(
                "pages",
                "spec",
                "alice",
                request(json!({"kind":"context","contextRef":original["contextRef"]})),
                &json!(1),
            )
            .await,
        NotePageError::Expired,
    );
    store.read_pool.close().await;
    store.write_pool.close().await;
    drop(store);
    let reopened = Store::open(&tmp.path).await.unwrap();
    assert_error(
        reopened
            .read_note_page(
                "pages",
                "spec",
                "alice",
                request(json!({"kind":"context","contextRef":original["contextRef"]})),
                &json!(1),
            )
            .await,
        NotePageError::Expired,
    );
    let current = page(&reopened, json!({"kind":"source"})).await;
    assert_eq!(current["sourceRevision"], revision);
    assert_eq!(current["text"], original["text"]);
    let profile: String = sqlx::query_scalar("SELECT profile_revision FROM note_page_head")
        .fetch_one(reopened.read_pool())
        .await
        .unwrap();
    assert_eq!(profile, crate::note_page_index::profile_revision());
}

#[tokio::test]
async fn indexed_html_window_keeps_implicit_ancestors_without_prior_cells() {
    // HTML5 inserts a row here. Its literal anchor is before TARGET, but the
    // admitted cell still requires that canonical ancestor and its direct ref.
    let source = "<table><td>earlier</td><td>TARGET</td></table>";
    let at = source.find("TARGET").unwrap();
    let (store, _tmp, _) = setup(source).await;
    let first = page(&store, json!({"kind":"source","at":at,"maxSourceBytes":6})).await;
    let context = page(
        &store,
        json!({"kind":"context","contextRef":first["contextRef"]}),
    )
    .await;
    let items = context["items"].as_array().unwrap();
    let row = items
        .iter()
        .find(|item| item["construct"] == "htmlTableRow")
        .expect("implicit row admitted");
    assert_eq!(row["htmlSource"]["provenance"], "implicit");
    assert_eq!(row["sourceRange"]["start"], row["sourceRange"]["end"]);
    assert!(row["sourceRange"]["start"].as_u64().unwrap() < (at as u64));
    assert!(items
        .iter()
        .all(|item| item.get("_admissionRange").is_none()));
    let cells: Vec<_> = items
        .iter()
        .filter(|item| item["construct"] == "htmlTableCell")
        .collect();
    assert_eq!(cells.len(), 1);
    assert_eq!(cells[0]["htmlPosition"]["columnIndex"], 1);
    let parent = page(
        &store,
        json!({"kind":"context","contextRef":cells[0]["parentRef"]}),
    )
    .await;
    assert_eq!(parent["items"][0]["id"], row["id"]);
    let native = page(
        &store,
        json!({"kind":"context","contextRef":row["nativeRef"]}),
    )
    .await;
    assert_eq!(native["items"][0]["provenance"], "implicit");
    assert_eq!(native["items"][0]["sourceRange"], row["sourceRange"]);
}

#[tokio::test]
async fn indexed_inline_code_uses_canonical_ranges_marks_and_bounded_delimiters() {
    let giant = "`".repeat(100_001);
    for (source, expected) in [
        ("before `` A ` B\r\nC `` after".to_owned(), Some("A ` B C")),
        (
            "before ```x `` y ` z``` after".to_owned(),
            Some("x `` y ` z"),
        ),
        ("before `   ` after".to_owned(), None),
        (
            "before `a \"quoted\" and 'single' & <x>` after".to_owned(),
            Some("a \"quoted\" and 'single' & <x>"),
        ),
        (format!("before {giant}TARGET{giant} after"), Some("TARGET")),
    ] {
        let (store, _tmp, _) = setup(&source).await;
        let open = source.find('`').unwrap();
        let width = source[open..]
            .bytes()
            .take_while(|byte| *byte == b'`')
            .count();
        let body = open + width;
        let at = if width > 100_000 { 50_000 } else { body };
        let first = page(&store, json!({"kind":"source","at":at,"maxSourceBytes":8})).await;
        let context = page(
            &store,
            json!({"kind":"context","contextRef":first["contextRef"]}),
        )
        .await;
        let code = context["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["role"] == "code")
            .expect("code owner");
        assert_eq!(
            code["codeSource"]["openingRange"],
            json!({"start":open,"end":body})
        );
        assert_eq!(code["codeSource"]["bodyRange"]["start"], body);
        assert_eq!(
            code["codeSource"]["bodyRange"]["end"],
            code["codeSource"]["closingRange"]["start"]
        );
        let close_start = usize::try_from(
            code["codeSource"]["closingRange"]["start"]
                .as_u64()
                .unwrap(),
        )
        .unwrap();
        let close_end =
            usize::try_from(code["codeSource"]["closingRange"]["end"].as_u64().unwrap()).unwrap();
        assert_eq!(close_end - close_start, width);
        assert_eq!(&source[close_start..close_end], &source[open..body]);
        if width > 100_000 {
            let maps = page(
                &store,
                json!({"kind":"context","contextRef":code["sourceMapRef"]}),
            )
            .await;
            assert!(maps["items"]
                .as_array()
                .unwrap()
                .iter()
                .all(|item| item["mapping"] == "omitted" && item["textRef"].is_null()));
            assert_eq!(maps["items"][0]["sourceRange"], first["range"]);
        }
        if let Some(expected) = expected {
            let leaf = page(
                &store,
                json!({"kind":"context","contextRef":code["nativeRef"]}),
            )
            .await;
            assert_eq!(leaf["items"][0]["nodeType"], "text");
            assert!(leaf["items"][0]["marksRef"].is_string());
            let body_source = page(
                &store,
                json!({"kind":"source","at":body,"maxSourceBytes":128}),
            )
            .await;
            let body_context = page(
                &store,
                json!({"kind":"context","contextRef":body_source["contextRef"]}),
            )
            .await;
            let body_code = body_context["items"]
                .as_array()
                .unwrap()
                .iter()
                .find(|item| item["role"] == "code")
                .unwrap();
            let maps = page(
                &store,
                json!({"kind":"context","contextRef":body_code["sourceMapRef"]}),
            )
            .await;
            let mut rendered = String::new();
            for map in maps["items"].as_array().unwrap() {
                if map["textRef"].is_null() {
                    continue;
                }
                let text = page(
                    &store,
                    json!({"kind":"context","contextRef":map["textRef"]}),
                )
                .await;
                rendered.push_str(text["items"][0]["text"].as_str().unwrap());
            }
            assert_eq!(rendered, expected);
        } else {
            assert!(code["nativeRef"].is_null());
        }
    }
}

#[tokio::test]
async fn indexed_inline_code_preserves_container_and_table_source_positions() {
    for source in [
        "> before `A\n> TARGET` after",
        "- before `A\n  TARGET` after",
        "| code |\n| --- |\n| `A\\|TARGET` |",
        "before `😀 &amp; TARGET` after",
    ] {
        let (store, _tmp, _) = setup(source).await;
        let at = source[..source.find("TARGET").unwrap()]
            .encode_utf16()
            .count();
        let source_page = page(&store, json!({"kind":"source","at":at,"maxSourceBytes":6})).await;
        let context = page(
            &store,
            json!({"kind":"context","contextRef":source_page["contextRef"]}),
        )
        .await;
        let code = context["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["role"] == "code")
            .unwrap();
        let maps = page(
            &store,
            json!({"kind":"context","contextRef":code["sourceMapRef"]}),
        )
        .await;
        let visible: Vec<_> = maps["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|map| {
                !map["textRef"].is_null()
                    && map["sourceRange"]["start"].as_u64().unwrap() < (at + 6) as u64
                    && map["sourceRange"]["end"].as_u64().unwrap() > at as u64
            })
            .collect();
        assert_eq!(visible.len(), 1, "{source}: {maps}");
        assert_eq!(visible[0]["mapping"], "identity", "{source}: {maps}");
        assert_eq!(
            visible[0]["sourceRange"], source_page["range"],
            "{source}: {maps}"
        );
        let text = page(
            &store,
            json!({"kind":"context","contextRef":visible[0]["textRef"]}),
        )
        .await;
        assert_eq!(text["items"][0]["text"], "TARGET", "{source}: {maps}");
    }
}

#[tokio::test]
async fn indexed_html_entry_paths_keep_heading_and_anchor_tables_literal() {
    // Exact production processMarkdownToHTML/createEditorConfig oracle: these
    // entry paths escape table tags. A lexical htmlBlock is not a native table.
    for (name, source) in [
        (
            "heading-first",
            "## Before\n\n<table><tr><td>x</td></tr></table>\n\n**After**",
        ),
        (
            "anchor-first",
            "<!--anchor:comment-a:point-->\n\n<table><tr><td>x</td></tr></table>\n\n**After**",
        ),
    ] {
        let at = source.find("<td>x").unwrap() + 4;
        let (store, _temporary, _note) = setup(source).await;
        let mut transcript = Vec::new();
        let first = record_page(&store, json!({"kind":"source","at":at,"maxSourceBytes":4096,"maxWireBytes":8192,"maxItems":64}), &mut transcript).await;
        assert_eq!(first["text"], source[at..]);
        let mut request = json!({"kind":"context","contextRef":first["contextRef"],"maxWireBytes":8192,"maxItems":64});
        let mut items = Vec::new();
        loop {
            let page = record_page(&store, request.clone(), &mut transcript).await;
            items.extend(page["items"].as_array().unwrap().iter().cloned());
            if page["nextCursor"].is_null() {
                break;
            }
            request["cursor"] = page["nextCursor"].clone();
        }
        record_fixture_closure(&store, &json!(items), &mut transcript).await;
        if let Ok(directory) = std::env::var("NOTE_PAGE_TRANSCRIPT_DIR") {
            let path = std::path::Path::new(&directory).join(format!("mixed-entry-{name}.json"));
            std::fs::write(
                path,
                serde_json::to_vec_pretty(&json!({"source":source,"at":at,"calls":transcript}))
                    .unwrap(),
            )
            .unwrap();
        }
        assert!(
            !items.iter().any(|item| matches!(
                item["construct"].as_str(),
                Some("htmlTable" | "htmlTableRow" | "htmlTableCell")
            )),
            "literal HTML must not acquire native table context: {items:?}"
        );
    }
}

#[tokio::test]
async fn indexed_html_entry_preserves_literal_markdown_tail() {
    // Frozen FE d8333fc1 canonical entry oracle: HTML-first bypasses Markdown
    // conversion. The trailing stars remain literal and never become bold.
    let source = "<table><tr><td>x</td></tr></table>\n\n**After**";
    let at = source.find("**After**").unwrap();
    let (store, _temporary, _note) = setup(source).await;
    let mut transcript = Vec::new();
    let first = record_page(
        &store,
        json!({"kind":"source","at":at,"maxSourceBytes":4096,"maxWireBytes":8192,"maxItems":64}),
        &mut transcript,
    )
    .await;
    assert_eq!(first["text"], "**After**");
    let mut queue = vec![(
        "context".to_owned(),
        first["contextRef"].as_str().unwrap().to_owned(),
    )];
    let mut seen = std::collections::BTreeSet::new();
    while let Some((kind, reference)) = queue.pop() {
        if !seen.insert(reference.clone()) {
            continue;
        }
        assert!(seen.len() < 128, "tiny canonical fixture must stay bounded");
        let mut request = json!({"kind":kind,"maxWireBytes":8192,"maxItems":64});
        request[if kind == "metadata" {
            "ref"
        } else {
            "contextRef"
        }] = json!(reference);
        loop {
            let response = record_page(&store, request.clone(), &mut transcript).await;
            resource_refs(&response["items"], &mut queue);
            if response["nextCursor"].is_null() {
                break;
            }
            request["cursor"] = response["nextCursor"].clone();
        }
    }
    if let Ok(directory) = std::env::var("NOTE_PAGE_TRANSCRIPT_DIR") {
        std::fs::write(
            std::path::Path::new(&directory).join("html-first-literal-tail.json"),
            serde_json::to_vec_pretty(&json!({"source":source,"at":at,"calls":transcript}))
                .unwrap(),
        )
        .unwrap();
    }
    let items: Vec<_> = transcript
        .iter()
        .filter_map(|call| call["response"]["items"].as_array())
        .flatten()
        .collect();
    let map = items
        .iter()
        .find(|item| {
            item["kind"] == "sourceMap"
                && item["mapping"] == "identity"
                && item["sourceRange"]["start"]
                    .as_u64()
                    .is_some_and(|start| start <= at as u64)
                && item["sourceRange"]["end"]
                    .as_u64()
                    .is_some_and(|end| end >= source.len() as u64)
        })
        .expect("HTML-entry document must expose literal tail native mapping");
    let native = items
        .iter()
        .find(|item| item["kind"] == "nativeNode" && item["id"] == map["textNodeId"])
        .unwrap();
    assert!(
        native.get("marksRef").is_none(),
        "literal tail must not acquire a bold mark"
    );
    assert!(items.iter().any(|item| item["field"] == "renderedText"
        && item["text"]
            .as_str()
            .is_some_and(|text| text.contains("**After**"))));
}

#[tokio::test]
async fn indexed_markdown_inside_escaped_html_retains_native_paragraph_semantics() {
    // Frozen production FE oracle 7d735045: tags are literal, but Markdown
    // marks/entities/escapes and LF/CRLF hard breaks inside them remain native.
    for (name, source, expected) in [
        (
            "marks",
            "## Before\n\n<div>**bold** `code` &amp; \\* </div>",
            "<div>bold code & * </div>",
        ),
        (
            "lf",
            "## Before\n\n<div>one\ntwo</div>",
            "<div>onetwo</div>",
        ),
        (
            "crlf",
            "## Before\r\n\r\n<div>one\r\ntwo</div>",
            "<div>onetwo</div>",
        ),
    ] {
        let at = source.find("<div>").unwrap();
        let (store, _temporary, _) = setup(source).await;
        let mut transcript = Vec::new();
        let first = record_page(&store, json!({"kind":"source","at":at,"maxSourceBytes":4096,"maxWireBytes":8192,"maxItems":64}), &mut transcript).await;
        let context = record_page(&store, json!({"kind":"context","contextRef":first["contextRef"],"maxWireBytes":8192,"maxItems":64}), &mut transcript).await;
        record_fixture_closure(&store, &context["items"], &mut transcript).await;
        if let Ok(directory) = std::env::var("NOTE_PAGE_TRANSCRIPT_DIR") {
            std::fs::write(
                std::path::Path::new(&directory).join(format!("markdown-html-{name}.json")),
                serde_json::to_vec_pretty(&json!({"source":source,"at":at,"calls":transcript}))
                    .unwrap(),
            )
            .unwrap();
        }
        let items: Vec<_> = transcript
            .iter()
            .filter_map(|call| call["response"]["items"].as_array())
            .flatten()
            .collect();
        let owner = context["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["construct"] == "markdownBlock")
            .expect("escaped HTML requires a canonical Markdown paragraph owner");
        assert_eq!(owner["entryPath"], "markdown");
        assert!(!items.iter().any(|item| item["construct"] == "htmlDocument"));
        let native = page(
            &store,
            json!({"kind":"context","contextRef":owner["nativeRef"]}),
        )
        .await;
        assert_eq!(native["items"][0]["nodeType"], "paragraph");
        assert_eq!(native["items"][0]["childIndex"], 1);
        let mut texts = std::collections::BTreeMap::new();
        for item in &items {
            if item["kind"] == "sourceMap" && item["textRef"].is_string() {
                let text = page(
                    &store,
                    json!({"kind":"context","contextRef":item["textRef"]}),
                )
                .await;
                texts.insert(
                    (
                        item["sourceRange"]["start"].as_u64().unwrap(),
                        item["textNodeId"].to_string(),
                        item["renderedRange"]["start"].as_u64().unwrap(),
                    ),
                    text["items"][0]["text"].as_str().unwrap().to_owned(),
                );
            }
        }
        assert_eq!(
            texts.values().cloned().collect::<String>(),
            expected,
            "{name}"
        );
        let mut identity_maps = std::collections::BTreeMap::new();
        for item in &items {
            if item["kind"] == "sourceMap" && item["mapping"] == "identity" {
                identity_maps.insert(item["id"].as_str().unwrap(), *item);
            }
        }
        let mut identity_maps: Vec<_> = identity_maps.into_values().collect();
        identity_maps.sort_by_key(|item| item["sourceRange"]["start"].as_u64().unwrap());
        for pair in identity_maps.windows(2) {
            assert!(
                pair[0]["ownerRef"] != pair[1]["ownerRef"]
                    || pair[0]["textNodeId"] != pair[1]["textNodeId"]
                    || pair[0]["sourceRange"]["end"] != pair[1]["sourceRange"]["start"]
                    || pair[0]["renderedRange"]["end"] != pair[1]["renderedRange"]["start"],
                "{name}: adjacent identity pieces in one native leaf must share one bounded map"
            );
        }

        if name == "marks" {
            for mark in ["bold", "code"] {
                assert!(items
                    .iter()
                    .any(|item| item["kind"] == "fragment" && item["text"] == mark));
            }
            assert!(
                items
                    .iter()
                    .filter(|item| item["nodeType"] == "text" && item["marksRef"].is_string())
                    .count()
                    >= 2,
                "bold and code marks survive"
            );
        } else {
            assert!(
                items
                    .iter()
                    .any(|item| item["nodeType"] == "hardBreak" && item["nodeClass"] == "atom"),
                "{name}: native hard break survives"
            );
        }
    }
}

async fn record_source_window_closure(
    store: &Store,
    request: Value,
    calls: &mut Vec<Value>,
) -> Value {
    let source = record_page(store, request, calls).await;
    let mut context = json!({"kind":"context","contextRef":source["contextRef"],"maxWireBytes":8192,"maxItems":64});
    loop {
        let result = record_page(store, context.clone(), calls).await;
        record_fixture_closure(store, &result["items"], calls).await;
        if result["nextCursor"].is_null() {
            break;
        }
        context["cursor"] = result["nextCursor"].clone();
    }
    source
}

#[tokio::test]
async fn indexed_markdown_small_windows_keep_one_snapshot_and_exact_seams() {
    for (name, source) in [
        (
            "marks",
            "## Before\n\n<div>**bold** `code` &amp; \\* </div>",
        ),
        ("lf", "## Before\n\n<div>one\ntwo</div>"),
        ("crlf", "## Before\r\n\r\n<div>one\r\ntwo</div>"),
    ] {
        // These frozen oracle sources are ASCII, so byte and UTF-16 indices agree.
        assert!(source.is_ascii());
        let at = source.find("<div>").unwrap();
        let (store, _temporary, _) = setup(source).await;
        let mut calls = Vec::new();
        let first = record_source_window_closure(&store, json!({"kind":"source","at":at,"maxSourceBytes":4096,"maxWireBytes":8192,"maxItems":64}), &mut calls).await;
        let mut request = json!({"kind":"source","at":at,"maxSourceBytes":16,"maxWireBytes":8192,"maxItems":64,"snapshotId":first["snapshotId"],"sourceRevision":first["sourceRevision"],"noteInstanceId":first["scope"]["noteInstanceId"]});
        let mut windows = Vec::new();
        let mut assembled = String::new();
        let mut position = at;
        while position < source.len() {
            request["at"] = json!(position);
            if position != at {
                // Ordinary navigation retries its default request at each new
                // position. Capture that actual response on the same snapshot,
                // not a cropped or synthesized answer from the previous page.
                let mut wide = request.clone();
                wide["maxSourceBytes"] = json!(4096);
                let next = record_source_window_closure(&store, wide, &mut calls).await;
                assert_eq!(next["snapshotId"], first["snapshotId"]);
                assert_eq!(next["sourceRevision"], first["sourceRevision"]);
                assert_eq!(next["scope"], first["scope"]);
                assert_eq!(next["range"]["start"], position);
                assert_eq!(next["text"], source[position..]);
            }
            let next = record_source_window_closure(&store, request.clone(), &mut calls).await;
            assert_eq!(next["snapshotId"], first["snapshotId"]);
            assert_eq!(next["sourceRevision"], first["sourceRevision"]);
            assert_eq!(next["scope"], first["scope"]);
            assert_eq!(next["range"]["start"], position);
            let end = usize::try_from(next["range"]["end"].as_u64().unwrap()).unwrap();
            assert!(end > position && end <= source.len());
            let text = next["text"].as_str().unwrap();
            assert!(text.len() <= 16);
            assert_eq!(text, &source[position..end]);
            assembled.push_str(text);
            windows.push(next["range"].clone());
            position = end;
        }
        assert_eq!(assembled, &source[at..]);
        let mut probes = Vec::new();
        for needle in ["&amp;", "**", "\\*", "\r\n", "\n"] {
            if let Some(offset) = source[at..].find(needle) {
                request["at"] = json!(at + offset + 1);
                request["maxSourceBytes"] = json!(4);
                let next = record_source_window_closure(&store, request.clone(), &mut calls).await;
                assert_eq!(next["snapshotId"], first["snapshotId"]);
                probes.push(json!({"needle":needle,"request":request,"range":next["range"]}));
            }
        }
        if let Ok(directory) = std::env::var("NOTE_PAGE_TRANSCRIPT_DIR") {
            std::fs::write(std::path::Path::new(&directory).join(format!("markdown-html-{name}-windows.json")), serde_json::to_vec_pretty(&json!({"source":source,"at":at,"calls":calls,"windows":windows,"probes":probes})).unwrap()).unwrap();
        }
    }
}

#[tokio::test]
async fn indexed_markdown_paragraph_far_seek_uses_bounded_native_maps() {
    let mut costs = Vec::new();
    for length in [32_768, 2_000_000] {
        let source = format!("## Before\r\n\r\n<div data-label=\"é😀\">{} **TARGET** &amp; `quoted \"value\"`\r\nEND</div>", "x".repeat(length));
        let at = source[..source.find("TARGET").unwrap()]
            .encode_utf16()
            .count();
        let started = std::time::Instant::now();
        let (mut store, temporary, _) = setup(&source).await;
        let build_ms = started.elapsed().as_millis();
        // A single connection ensures the request uses the instrumented SQLite
        // handle. A zero count is an instrumentation failure, never a proof.
        store.read_pool.close().await;
        store.read_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                sqlx::sqlite::SqliteConnectOptions::new()
                    .filename(&temporary.path)
                    .read_only(true),
            )
            .await
            .unwrap();
        let first = page(
            &store,
            json!({"kind":"source","at":at,"maxSourceBytes":6,"maxWireBytes":8192}),
        )
        .await;
        let context = page(
            &store,
            json!({"kind":"context","contextRef":first["contextRef"],"maxItems":128}),
        )
        .await;
        let owner = context["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["construct"] == "markdownBlock")
            .unwrap();
        let request =
            json!({"kind":"context","contextRef":owner["sourceMapRef"],"maxWireBytes":8192});
        let _warm = page(&store, request.clone()).await;
        let counter = Arc::new(AtomicUsize::new(0));
        let count = counter.clone();
        {
            let mut conn = store.read_pool.acquire().await.unwrap();
            conn.lock_handle()
                .await
                .unwrap()
                .set_progress_handler(1, move || {
                    count.fetch_add(1, Ordering::Relaxed);
                    true
                });
        }
        let maps = page(&store, request).await;
        let cost = counter.load(Ordering::Relaxed);
        {
            let mut conn = store.read_pool.acquire().await.unwrap();
            conn.lock_handle().await.unwrap().remove_progress_handler();
        }
        costs.push(cost);
        assert!(cost > 0 && cost < 500, "actual SQL work: {cost}");
        let visible: Vec<_> = maps["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["textRef"].is_string())
            .collect();
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0]["sourceRange"], first["range"]);
        let text = page(
            &store,
            json!({"kind":"context","contextRef":visible[0]["textRef"]}),
        )
        .await;
        assert_eq!(text["items"][0]["text"], "TARGET");
        let leaf = page(
            &store,
            json!({"kind":"context","contextRef":visible[0]["textNodeRef"]}),
        )
        .await;
        assert!(leaf["items"][0]["marksRef"].is_string());
        let direct = page(
            &store,
            json!({"kind":"context","contextRef":visible[0]["ownerRef"]}),
        )
        .await;
        for field in ["sourceMapRef", "continuationBefore", "continuationAfter"] {
            assert!(direct["items"][0].get(field).is_none());
        }
        assert_eq!(direct["items"][0]["nativeRef"], owner["nativeRef"]);
        let rows=sqlx::query("SELECT count(*) AS count,sum(length(CAST(value AS BLOB))) AS bytes FROM note_page_entry").fetch_one(store.read_pool()).await.unwrap();
        eprintln!("markdown_bytes={} build_and_migrations_ms={build_ms} warm_map_vm_steps={cost} map_rows={} entry_rows={} entry_bytes={}",source.len(),maps["items"].as_array().unwrap().len(),rows.get::<i64,_>("count"),rows.get::<i64,_>("bytes"));
    }
    assert!(
        costs[1] <= costs[0] + 30,
        "far seek grew with unloaded prefix: {costs:?}"
    );
}

#[tokio::test]
async fn indexed_native_primitive_grants_bind_exact_code_and_scope() {
    use intent_core::note_artifact::request::ArtifactHeader;
    let source = "<div data-type=\"diff-block\" data-diff-code=\"LW9sZAor\"></div><div data-type=\"mermaid-block\" data-mermaid-code=\"graph TD\n A[Alpha]\n\"></div>";
    let (store, _temporary, mut note) = setup(source).await;
    let first = page(
        &store,
        json!({"kind":"source","maxSourceBytes":4096,"maxItems":128}),
    )
    .await;
    let context = page(
        &store,
        json!({"kind":"context","contextRef":first["contextRef"],"maxItems":128}),
    )
    .await;
    let atoms: Vec<_> = context["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["nodeClass"] == "atom")
        .collect();
    assert_eq!(atoms.len(), 2);
    let mut headers = Vec::new();
    for (atom, primitive, expected) in [
        (atoms[0], "diff", "LW9sZAor"),
        (atoms[1], "mermaid", "graph TD\n A[Alpha]"),
    ] {
        let owner = page(
            &store,
            json!({"kind":"context","contextRef":atom["nativeRef"]}),
        )
        .await;
        assert_eq!(
            owner["items"][0], *atom,
            "window atom retains stable parent and self references"
        );
        let root = page(
            &store,
            json!({"kind":"metadata","ref":atom["attributesRef"]}),
        )
        .await;
        let fields = page(
            &store,
            json!({"kind":"metadata","ref":root["items"][0]["childrenRef"]}),
        )
        .await;
        let code = fields["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|field| field["key"] == "code")
            .unwrap();
        let value = page(
            &store,
            json!({"kind":"context","contextRef":code["valueRef"]}),
        )
        .await;
        assert_eq!(value["items"][0]["text"], expected);
        let header: ArtifactHeader = serde_json::from_value(json!({
            "scope":first["scope"],"source":{"kind":"snapshot","snapshotId":first["snapshotId"],"sourceRevision":first["sourceRevision"],"ownerRef":atom["nativeRef"],"sourceRef":code["valueRef"]},
            "primitive":primitive,"profile":"test-profile-not-registered","environment":{"width":800,"height":600,"theme":"light","fontRef":"test-font","fontSize":14,"devicePixelRatio":1},
            "reservation":{"payloadBytes":4096,"records":10,"indexEntries":10,"storageChargeBytes":65536}
        })).unwrap();
        let grant = store
            .authorize_note_artifact_source("pages", "alice", &header)
            .await
            .unwrap();
        assert_eq!(grant.scope, header.scope);
        assert!(store
            .authorize_note_artifact_source("pages", "bob", &header)
            .await
            .is_err());
        assert!(store
            .authorize_note_artifact_source("other", "alice", &header)
            .await
            .is_err());
        headers.push(header);
    }
    let mut swapped = serde_json::to_value(&headers[0]).unwrap();
    swapped["source"]["sourceRef"] =
        serde_json::to_value(&headers[1]).unwrap()["source"]["sourceRef"].clone();
    assert!(store
        .authorize_note_artifact_source("pages", "alice", &serde_json::from_value(swapped).unwrap())
        .await
        .is_err());
    note.title = "new metadata revision".into();
    store.update_note(&note).await.unwrap();
    assert!(store
        .authorize_note_artifact_source("pages", "alice", &headers[0])
        .await
        .is_err());
    // Content replacement deletes the canonical binding in the same writer tx.
    note.content = "plain replacement".into();
    store.update_note(&note).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM note_artifact_source")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn indexed_markdown_primitive_code_matches_predecoder_native_values() {
    let source = "## Before\n\n```diff\n-café & old\n+世界 <new>\n```\n\n```mermaid\nflowchart LR\n A[\"café & 世界\"] --> B\n```\n\n**After**";
    let (store, _temporary, _note) = setup(source).await;
    let first = page(
        &store,
        json!({"kind":"source","maxSourceBytes":4096,"maxItems":128}),
    )
    .await;
    let context = page(
        &store,
        json!({"kind":"context","contextRef":first["contextRef"],"maxItems":128}),
    )
    .await;
    let atoms: Vec<_> = context["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["nodeClass"] == "atom")
        .collect();
    assert_eq!(atoms.len(), 2);
    for (atom, index, expected) in [
        (atoms[0], 1, "LWNhZsOpICYgb2xkCivkuJbnlYwgPG5ldz4="),
        (
            atoms[1],
            2,
            "Zmxvd2NoYXJ0IExSCiBBWyJjYWbDqSAmIOS4lueVjCJdIC0tPiBC",
        ),
    ] {
        let lexical = context["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| {
                item["construct"] == "codeBlock" && item["sourceRange"] == atom["sourceRange"]
            })
            .unwrap();
        assert_eq!(lexical["nativeRef"], atom["nativeRef"]);
        let direct: String = sqlx::query_scalar("SELECT value FROM note_page_entry WHERE workspace_id='pages' AND note_id='spec' AND collection=? AND position=0")
            .bind(format!("d:{}", lexical["id"].as_str().unwrap())).fetch_one(store.read_pool()).await.unwrap();
        let direct: Value = serde_json::from_str(&direct).unwrap();
        assert_eq!(
            direct["nativeRef"],
            format!("d:{}", atom["id"].as_str().unwrap()),
            "direct lexical owner must preserve its canonical atom shortcut"
        );
        assert_eq!(atom["childIndex"], index);
        let parent = page(
            &store,
            json!({"kind":"context","contextRef":atom["parentRef"]}),
        )
        .await;
        assert_eq!(parent["items"][0]["nodeType"], "doc");
        let root = page(
            &store,
            json!({"kind":"metadata","ref":atom["attributesRef"]}),
        )
        .await;
        let fields = page(
            &store,
            json!({"kind":"metadata","ref":root["items"][0]["childrenRef"]}),
        )
        .await;
        let code = fields["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|field| field["key"] == "code")
            .unwrap();
        let value = page(
            &store,
            json!({"kind":"context","contextRef":code["valueRef"]}),
        )
        .await;
        assert_eq!(value["items"][0]["text"], expected);
    }
}

#[tokio::test]
async fn indexed_html_entry_backticks_follow_canonical_entry_mode() {
    // Frozen FE ffb1ddf7 paired oracle: only the heading-first entry has a code mark.
    for (prefix, markdown) in [("", false), ("## Before\n\n", true)] {
        let source = format!("{prefix}<table><tr><td>x</td></tr></table>\n\n`After`");
        let at = source.find("`After`").unwrap();
        let (store, _temporary, _) = setup(&source).await;
        let mut transcript = Vec::new();
        let first = record_page(
            &store,
            json!({"kind":"source","at":at,"maxSourceBytes":32,"maxWireBytes":8192,"maxItems":64}),
            &mut transcript,
        )
        .await;
        let context = record_page(&store, json!({"kind":"context","contextRef":first["contextRef"],"maxWireBytes":8192,"maxItems":64}), &mut transcript).await;
        record_fixture_closure(&store, &context["items"], &mut transcript).await;
        if let Ok(directory) = std::env::var("NOTE_PAGE_TRANSCRIPT_DIR") {
            let name = if markdown { "markdown" } else { "html" };
            std::fs::write(
                std::path::Path::new(&directory).join(format!("backtick-{name}.json")),
                serde_json::to_vec_pretty(&json!({"source":source,"at":at,"calls":transcript}))
                    .unwrap(),
            )
            .unwrap();
        }
        let items = context["items"].as_array().unwrap();
        let code = items.iter().find(|item| item["role"] == "code");
        if markdown {
            let code = code.expect("Markdown entry must retain code semantics");
            assert!(code["codeSource"].is_object());
            assert!(code["nativeRef"].is_string());
            assert!(code["sourceMapRef"].is_string());
            let maps = page(
                &store,
                json!({"kind":"context","contextRef":code["sourceMapRef"]}),
            )
            .await;
            let direct = page(
                &store,
                json!({"kind":"context","contextRef":maps["items"][0]["ownerRef"]}),
            )
            .await;
            assert_eq!(
                code["parentRef"], direct["items"][0]["parentRef"],
                "canonical code owner parent is snapshot-stable"
            );
            let second = page(
                &store,
                json!({"kind":"source","at":at+2,"maxSourceBytes":4,"snapshotId":first["snapshotId"],"sourceRevision":first["sourceRevision"],"noteInstanceId":first["scope"]["noteInstanceId"]}),
            )
            .await;
            let next = page(
                &store,
                json!({"kind":"context","contextRef":second["contextRef"],"maxItems":128}),
            )
            .await;
            let next = next["items"]
                .as_array()
                .unwrap()
                .iter()
                .find(|item| item["role"] == "code")
                .unwrap();
            for field in ["id", "parentRef", "nativeRef", "codeSource", "sourceRange"] {
                assert_eq!(
                    code[field], next[field],
                    "code owner {field} changed across source windows"
                );
            }
            assert_ne!(code["sourceMapRef"], next["sourceMapRef"]);
        } else {
            assert!(
                code.is_none(),
                "HTML-entry backticks must not advertise code semantics: {items:?}"
            );
            assert!(items.iter().any(|item| item["role"] == "literal"));
            assert!(items.iter().any(|item| item["construct"] == "htmlDocument"));
        }
    }
}

#[tokio::test]
async fn indexed_html_entry_windows_and_prefix_edit_keep_source_ownership_scoped() {
    let source = format!(
        "<table><tr><td>x</td></tr></table>\n\n{}",
        "😀literal&text ".repeat(2000)
    );
    let (store, _temporary, mut note) = setup(&source).await;
    let mut owners = Vec::new();
    for at in [100, 20100] {
        let first = page(
            &store,
            json!({"kind":"source","at":at,"maxSourceBytes":128}),
        )
        .await;
        let context = page(
            &store,
            json!({"kind":"context","contextRef":first["contextRef"],"maxItems":128}),
        )
        .await;
        let owner = context["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["construct"] == "htmlDocument")
            .unwrap()
            .clone();
        assert_eq!(
            owner["sourceRange"],
            json!({"start":0,"end":source.encode_utf16().count()})
        );
        let maps = page(
            &store,
            json!({"kind":"context","contextRef":owner["sourceMapRef"],"maxItems":128}),
        )
        .await;
        for map in maps["items"].as_array().unwrap() {
            assert!(
                map["sourceRange"]["end"].as_u64().unwrap()
                    >= first["range"]["start"].as_u64().unwrap()
            );
            assert!(
                map["sourceRange"]["start"].as_u64().unwrap()
                    <= first["range"]["end"].as_u64().unwrap()
            );
        }
        let reference = maps["items"][0]["ownerRef"].clone();
        let direct = page(&store, json!({"kind":"context","contextRef":reference})).await;
        let direct = &direct["items"][0];
        assert_eq!(direct["id"], owner["id"]);
        for field in ["sourceMapRef", "continuationBefore", "continuationAfter"] {
            assert!(
                direct.get(field).is_none(),
                "stable owner must omit {field}"
            );
        }
        owners.push(owner);
    }
    assert_eq!(owners[0]["id"], owners[1]["id"]);
    assert_ne!(owners[0]["sourceMapRef"], owners[1]["sourceMapRef"]);
    note.content = format!("## Before\n\n{source}");
    store.update_note(&note).await.unwrap();
    assert_error(
        store
            .read_note_page(
                "pages",
                "spec",
                "alice",
                request(json!({"kind":"context","contextRef":owners[0]["sourceMapRef"]})),
                &json!(1),
            )
            .await,
        NotePageError::Stale,
    );
    let first = page(
        &store,
        json!({"kind":"source","at":20111,"maxSourceBytes":128}),
    )
    .await;
    let context = page(
        &store,
        json!({"kind":"context","contextRef":first["contextRef"],"maxItems":128}),
    )
    .await;
    assert!(!context["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["construct"] == "htmlDocument"));
}

#[tokio::test]
async fn indexed_markdown_titled_primitives_preserve_raw_predecoder_values() {
    // Frozen FE e6d6bb999 titled-fence oracle: raw code, unlike exact-language base64.
    for (language, body) in [
        ("diff", "-café & old\n+世界 <new>"),
        ("mermaid", "flowchart LR\n A[\"café & 世界\"] --> B"),
    ] {
        let source = format!("```{language} title\n{body}\n```");
        let (store, _temporary, _) = setup(&source).await;
        let first = page(&store, json!({"kind":"source","maxSourceBytes":4096})).await;
        let context = page(
            &store,
            json!({"kind":"context","contextRef":first["contextRef"],"maxItems":128}),
        )
        .await;
        let atom = context["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["nodeClass"] == "atom")
            .expect("titled primitive must retain canonical native atom");
        let root = page(
            &store,
            json!({"kind":"metadata","ref":atom["attributesRef"]}),
        )
        .await;
        let fields = page(
            &store,
            json!({"kind":"metadata","ref":root["items"][0]["childrenRef"]}),
        )
        .await;
        let code = fields["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|field| field["key"] == "code")
            .unwrap();
        let value = page(
            &store,
            json!({"kind":"context","contextRef":code["valueRef"]}),
        )
        .await;
        assert_eq!(value["items"][0]["text"], body);
    }
}

#[tokio::test]
async fn indexed_native_primitives_match_all_frozen_entry_values() {
    let groups: Value =
        serde_json::from_str(include_str!("fixtures/note_primitive_native.json")).unwrap();
    for group in groups.as_array().unwrap() {
        for case in group["cases"].as_array().unwrap() {
            let source = case["source"].as_str().unwrap();
            let (store, _temporary, _) = setup(source).await;
            let first = page(&store, json!({"kind":"source","maxSourceBytes":4096})).await;
            let context = page(
                &store,
                json!({"kind":"context","contextRef":first["contextRef"],"maxItems":128}),
            )
            .await;
            assert!(context["nextCursor"].is_null());
            let atoms: Vec<_> = context["items"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|item| item["nodeClass"] == "atom")
                .collect();
            assert_eq!(
                atoms.len(),
                case["atoms"].as_array().unwrap().len(),
                "{}",
                case["id"]
            );
            for (atom, expected) in atoms.iter().zip(case["atoms"].as_array().unwrap()) {
                assert_eq!(atom["nodeType"], expected["type"], "{}", case["id"]);
                let root = page(
                    &store,
                    json!({"kind":"metadata","ref":atom["attributesRef"]}),
                )
                .await;
                let fields = page(
                    &store,
                    json!({"kind":"metadata","ref":root["items"][0]["childrenRef"]}),
                )
                .await;
                let code = fields["items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|field| field["key"] == "code")
                    .unwrap();
                let value = page(
                    &store,
                    json!({"kind":"context","contextRef":code["valueRef"]}),
                )
                .await;
                assert_eq!(
                    value["items"][0]["text"], expected["code"],
                    "{}",
                    case["id"]
                );
            }
        }
    }
}

async fn artifact_begin_fixture() -> (
    Store,
    TempDb,
    intent_core::Note,
    intent_core::note_artifact::request::ArtifactBegin,
) {
    let (store, temporary, note) = setup("```diff\n-old\n+new\n```\n").await;
    store
        .configure_test_note_artifact_arena(
            &temporary.path.with_extension("artifacts.sqlite"),
            1024,
        )
        .await
        .unwrap();
    let first = page(&store, json!({"kind":"source"})).await;
    let context = page(
        &store,
        json!({"kind":"context","contextRef":first["contextRef"]}),
    )
    .await;
    let atom = context["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["nodeType"] == "diffBlock")
        .unwrap();
    let root = page(
        &store,
        json!({"kind":"metadata","ref":atom["attributesRef"]}),
    )
    .await;
    let fields = page(
        &store,
        json!({"kind":"metadata","ref":root["items"][0]["childrenRef"]}),
    )
    .await;
    let code = fields["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["key"] == "code")
        .unwrap();
    let mut request: intent_core::note_artifact::request::ArtifactBegin = serde_json::from_value(json!({
        "jobId":"job", "expiresAt":first["expiresAt"], "headerDigest":"",
        "header":{
            "scope":first["scope"],
            "source":{"kind":"snapshot","snapshotId":first["snapshotId"],"sourceRevision":first["sourceRevision"],"ownerRef":atom["nativeRef"],"sourceRef":code["valueRef"]},
            "primitive":"diff","profile":"journal-test-not-registered",
            "environment":{"width":800.5,"height":600,"theme":"light","fontRef":"test-only","fontSize":14,"devicePixelRatio":1.25},
            "reservation":{"payloadBytes":1024,"records":2,"indexEntries":2,"storageChargeBytes":4096}
        }
    })).unwrap();
    sign_artifact_begin(&mut request);
    for (kind, id) in [
        ("global", ""),
        ("principal", "alice"),
        ("workspace", "pages"),
    ] {
        sqlx::query("INSERT INTO note_artifact_capacity(scope_kind,scope_id,payload_limit,record_limit,index_limit,storage_limit,job_limit) VALUES (?,?,1024,2,2,4096,1)")
            .bind(kind).bind(id).execute(store.artifact_pool().unwrap()).await.unwrap();
    }
    (store, temporary, note, request)
}

fn sign_artifact_begin(request: &mut intent_core::note_artifact::request::ArtifactBegin) {
    request.header_digest = intent_core::note_artifact::canonical::digest(
        &json!({
            "domain":"note.artifact.begin.v1", "jobId":request.job_id,
            "expiresAt":request.expires_at, "header":request.header
        })
        .to_string(),
    )
    .unwrap();
}

fn artifact_retention(request: &intent_core::note_artifact::request::ArtifactBegin) -> i64 {
    i64::try_from(
        intent_core::parse_iso(&request.expires_at)
            .unwrap()
            .unix_timestamp_nanos()
            / 1_000_000,
    )
    .unwrap()
        + 60_000
}

#[tokio::test]
async fn artifact_begin_reserves_once_and_preserves_retired_replay() {
    let (store, _temporary, _note, request) = artifact_begin_fixture().await;
    let retention = artifact_retention(&request);
    assert!(store
        .begin_note_artifact_journal("bob", "pages", &request, retention)
        .await
        .is_err());
    let first = store
        .begin_note_artifact_journal("alice", "pages", &request, retention)
        .await
        .unwrap();
    let second = store
        .begin_note_artifact_journal("alice", "pages", &request, retention + 1)
        .await
        .unwrap();
    assert_eq!(first.generation, second.generation);
    assert_eq!(first.job_ref, second.job_ref);
    assert_eq!(first.status_until, second.status_until);
    assert_eq!(first.state, "building");
    let mut changed = request.clone();
    changed.expires_at = intent_core::iso_ms_from_now(120_000);
    sign_artifact_begin(&mut changed);
    assert!(store
        .begin_note_artifact_journal("alice", "pages", &changed, retention)
        .await
        .is_err());
    let mut another = request.clone();
    another.job_id = "second-job".into();
    sign_artifact_begin(&mut another);
    assert!(store
        .begin_note_artifact_journal("alice", "pages", &another, retention)
        .await
        .is_err());
    store
        .abort_note_artifact_journal("alice", "pages", &first.job_ref)
        .await
        .unwrap();
    let replay = store
        .begin_note_artifact_journal("alice", "pages", &request, retention)
        .await
        .unwrap();
    assert_eq!(replay.state, "aborted");
    assert_eq!(replay.generation, first.generation);
    let rows = sqlx::query("SELECT jobs_reserved,payload_reserved,records_reserved,indexes_reserved,storage_reserved FROM note_artifact_capacity")
        .fetch_all(store.artifact_pool().unwrap()).await.unwrap();
    for row in rows {
        assert_eq!(row.get::<i64, _>("jobs_reserved"), 1);
        assert_eq!(row.get::<i64, _>("payload_reserved"), 1024);
        assert_eq!(row.get::<i64, _>("records_reserved"), 2);
        assert_eq!(row.get::<i64, _>("indexes_reserved"), 2);
        assert_eq!(row.get::<i64, _>("storage_reserved"), 4096);
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM note_artifact_job")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn artifact_begin_revalidates_source_after_waiting_for_writer() {
    use std::future::Future as _;
    let (store, _temporary, mut note, request) = artifact_begin_fixture().await;
    // A valid read grant is deliberately obtained before the source advances.
    store
        .authorize_note_artifact_source("pages", "alice", &request.header)
        .await
        .unwrap();
    let mut writer = store.write_pool().acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *writer)
        .await
        .unwrap();
    let admission =
        store.begin_note_artifact_journal("alice", "pages", &request, artifact_retention(&request));
    tokio::pin!(admission);
    // Poll to the pending writer acquisition, without a timing sleep.
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(admission.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    sqlx::query("UPDATE note_page_head SET current_rev=current_rev+1 WHERE workspace_id='pages' AND note_id='spec'")
        .execute(&mut *writer).await.unwrap();
    sqlx::query("COMMIT").execute(&mut *writer).await.unwrap();
    drop(writer);
    assert!(admission.await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM note_artifact_job")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT sum(jobs_reserved) FROM note_artifact_capacity")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap(),
        0
    );
    note.content = "replacement".into();
    store.update_note(&note).await.unwrap();
    assert!(store
        .begin_note_artifact_journal("alice", "pages", &request, artifact_retention(&request))
        .await
        .is_err());
}

#[tokio::test]
async fn artifact_arena_commit_keeps_main_source_mutation_serialized() {
    use std::future::Future as _;
    let (store, _temporary, mut note, request) = artifact_begin_fixture().await;
    let arena_io = store.artifact_pool().unwrap().acquire().await.unwrap();
    let mut admission = Box::pin(store.begin_note_artifact_journal(
        "alice",
        "pages",
        &request,
        artifact_retention(&request),
    ));
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(admission.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            if store.write_pool().num_idle() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("admission did not acquire the main source guard");
    note.title = "queued after source guard".into();
    let mut edit = Box::pin(store.update_note(&note));
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(edit.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    // Main readers retain progress while its writer guard waits for arena I/O.
    let notes: i64 = sqlx::query_scalar("SELECT count(*) FROM note")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert!(notes > 0);
    drop(arena_io);
    let (admitted, edited) = tokio::join!(admission, edit);
    assert_eq!(admitted.unwrap().state, "building");
    edited.unwrap();
    assert!(store
        .begin_note_artifact_journal("alice", "pages", &request, artifact_retention(&request))
        .await
        .is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM note_artifact_job")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn artifact_cancelled_commit_keeps_source_guard_until_worker_settles() {
    let (store, _temporary, mut note, request) = artifact_begin_fixture().await;
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let mut reached_tx = Some(reached_tx);
    let mut connection = store.artifact_pool().unwrap().acquire().await.unwrap();
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_commit_hook(move || {
            if let Some(sender) = reached_tx.take() {
                let _ = sender.send(());
                return resume_rx
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .is_ok();
            }
            true
        });
    drop(connection);
    let mut admission = Box::pin(store.begin_note_artifact_journal(
        "alice",
        "pages",
        &request,
        artifact_retention(&request),
    ));
    let reached = tokio::select! {
        result = tokio::time::timeout(std::time::Duration::from_secs(10), reached_rx) => matches!(result, Ok(Ok(()))),
        _ = &mut admission => false,
    };
    drop(admission);
    assert!(
        reached,
        "admission did not reach the actual SQLite commit hook"
    );
    note.title = "edit after caller cancellation".into();
    // COMMIT is physically paused in the SQLite worker. Cancelling its caller
    // must not let the independent main writer mutate the authorized source.
    let edit = tokio::time::timeout(
        std::time::Duration::from_millis(250),
        store.update_note(&note),
    )
    .await;
    let _ = resume_tx.send(());
    let mut connection = store.artifact_pool().unwrap().acquire().await.unwrap();
    connection.lock_handle().await.unwrap().remove_commit_hook();
    drop(connection);
    store.update_note(&note).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM note_artifact_job")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap(),
        1,
    );
    assert!(
        edit.is_err(),
        "main source changed before cancelled arena COMMIT settled"
    );
}

#[tokio::test]
async fn artifact_begin_rejects_bad_integrity_and_source_deadline_without_charging() {
    let (store, temporary, _note, request) = artifact_begin_fixture().await;
    for mode in 0..5 {
        let mut changed = request.clone();
        match mode {
            0 => changed.job_id = "changed-without-new-digest".into(),
            1 => changed.header.environment.width += 1.0,
            2 => {
                changed.expires_at = intent_core::iso_ms_from_now(600_000);
                sign_artifact_begin(&mut changed);
            }
            3 => {
                changed.expires_at = "2000-01-01T00:00:00Z".into();
                sign_artifact_begin(&mut changed);
            }
            _ => {
                changed.header.source =
                    intent_core::note_artifact::request::ArtifactSource::SessionLive {
                        frozen_view_ref: "fake".into(),
                        editor_session_id: "fake".into(),
                        local_edit_sequence: 0,
                        live_generation: "fake".into(),
                        owner_ref: "fake".into(),
                        source_ref: "fake".into(),
                    };
                sign_artifact_begin(&mut changed);
            }
        }
        assert!(
            store
                .begin_note_artifact_journal(
                    "alice",
                    "pages",
                    &changed,
                    artifact_retention(&changed)
                )
                .await
                .is_err(),
            "mode {mode}"
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM note_artifact_job")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT sum(jobs_reserved) FROM note_artifact_capacity")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap(),
        0
    );
    let admitted = store
        .begin_note_artifact_journal("alice", "pages", &request, artifact_retention(&request))
        .await
        .unwrap();
    store.close().await;
    let restarted = Store::open(&temporary.path).await.unwrap();
    restarted
        .configure_test_note_artifact_arena(
            &temporary.path.with_extension("artifacts.sqlite"),
            1024,
        )
        .await
        .unwrap();
    assert!(restarted
        .begin_note_artifact_journal("alice", "pages", &request, artifact_retention(&request))
        .await
        .is_err());
    let old = restarted
        .note_artifact_journal_status("alice", "pages", &request.job_id, &request.header_digest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.generation, admitted.generation);
    restarted
        .abort_note_artifact_journal("alice", "pages", &old.job_ref)
        .await
        .unwrap();
}

fn artifact_append_request(
    job: &crate::ArtifactJournalStatus,
    sequence: u64,
    previous: &str,
    record: &str,
) -> intent_core::note_artifact::request::ArtifactAppend {
    let digest = intent_core::note_artifact::canonical::digest(
        &json!([
            "note.artifact.append.v1",
            job.header_digest,
            sequence,
            previous,
            record
        ])
        .to_string(),
    )
    .unwrap();
    intent_core::note_artifact::request::ArtifactAppend {
        job_ref: job.job_ref.clone(),
        sequence,
        previous_digest: previous.into(),
        record: record.into(),
        digest,
    }
}

#[tokio::test]
async fn artifact_append_replays_original_ack_after_later_record_and_seal() {
    let (store, _temporary, mut note, begin) = artifact_begin_fixture().await;
    let job = store
        .begin_note_artifact_journal("alice", "pages", &begin, artifact_retention(&begin))
        .await
        .unwrap();
    // These profile-shaped values exercise only the prepared journal seam, not
    // native profile validation, index construction, or physical allocation.
    let record = r#"{"kind":"diff.row","value":{"left":1}}"#;
    let first = artifact_append_request(&job, 0, &job.header_digest, record);
    let cost = crate::ArtifactJournalRecordCost {
        index_entries: 1,
        storage_bytes: 128,
        final_manifest: false,
    };
    for (principal, workspace) in [("bob", "pages"), ("alice", "other")] {
        assert!(store
            .append_note_artifact_journal(principal, workspace, &first, &cost)
            .await
            .is_err());
    }
    let ack = store
        .append_note_artifact_journal("alice", "pages", &first, &cost)
        .await
        .unwrap();
    assert_eq!(ack.next_sequence, 1);
    assert_eq!(ack.accepted_bytes, i64::try_from(record.len()).unwrap());
    let changed = artifact_append_request(
        &job,
        0,
        &job.header_digest,
        r#"{"kind":"diff.row","value":{"left":1.0}}"#,
    );
    assert!(store
        .append_note_artifact_journal("alice", "pages", &changed, &cost)
        .await
        .is_err());
    let gap = artifact_append_request(&job, 2, &first.digest, record);
    assert!(store
        .append_note_artifact_journal("alice", "pages", &gap, &cost)
        .await
        .is_err());
    let wrong_previous = artifact_append_request(&job, 1, &job.header_digest, record);
    assert!(store
        .append_note_artifact_journal("alice", "pages", &wrong_previous, &cost)
        .await
        .is_err());
    let manifest = artifact_append_request(
        &job,
        1,
        &first.digest,
        r#"{"kind":"diff.manifest","value":{"rows":1}}"#,
    );
    let final_cost = crate::ArtifactJournalRecordCost {
        index_entries: 1,
        storage_bytes: 128,
        final_manifest: true,
    };
    let last = store
        .append_note_artifact_journal("alice", "pages", &manifest, &final_cost)
        .await
        .unwrap();
    assert_eq!(last.next_sequence, 2);
    assert_eq!(
        last.accepted_bytes,
        i64::try_from(record.len() + manifest.record.len()).unwrap()
    );
    sqlx::query("UPDATE note_artifact_job SET state='sealed'")
        .execute(store.artifact_pool().unwrap())
        .await
        .unwrap();
    let replay = store
        .append_note_artifact_journal("alice", "pages", &first, &cost)
        .await
        .unwrap();
    assert_eq!(replay.next_sequence, ack.next_sequence);
    assert_eq!(replay.accepted_bytes, ack.accepted_bytes);
    assert_eq!(replay.current_digest, ack.current_digest);
    assert_eq!(replay.state, ack.state);
    let third = artifact_append_request(&job, 2, &manifest.digest, record);
    assert!(store
        .append_note_artifact_journal("alice", "pages", &third, &cost)
        .await
        .is_err());
    let current = store
        .note_artifact_journal_status("alice", "pages", &begin.job_id, &begin.header_digest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.state, "sealed");
    assert_eq!(current.accepted_bytes, last.accepted_bytes);
    assert_eq!(current.current_digest, last.current_digest);
    let totals = sqlx::query("SELECT count(*) AS records,sum(index_charge) AS indexes,sum(storage_charge) AS storage FROM note_artifact_record").fetch_one(store.artifact_pool().unwrap()).await.unwrap();
    assert_eq!(totals.get::<i64, _>("records"), 2);
    assert_eq!(totals.get::<i64, _>("indexes"), 2);
    assert_eq!(totals.get::<i64, _>("storage"), 256);
    note.title = "advance source revision".into();
    store.update_note(&note).await.unwrap();
    assert!(store
        .append_note_artifact_journal("alice", "pages", &first, &cost)
        .await
        .is_err());
}

#[tokio::test]
async fn artifact_append_failure_cannot_mutate_accepted_prefix() {
    let (store, _temporary, _note, begin) = artifact_begin_fixture().await;
    let job = store
        .begin_note_artifact_journal("alice", "pages", &begin, artifact_retention(&begin))
        .await
        .unwrap();
    let cost = crate::ArtifactJournalRecordCost {
        index_entries: 1,
        storage_bytes: 128,
        final_manifest: false,
    };
    let record = r#"{"kind":"diff.row","value":{}}"#;
    let first = artifact_append_request(&job, 0, &job.header_digest, record);
    let mut wrong_digest = first.clone();
    wrong_digest.digest = "f".repeat(64);
    let mut oversized = first.clone();
    oversized.record = " ".repeat(16_385);
    let duplicate = artifact_append_request(
        &job,
        0,
        &job.header_digest,
        r#"{"kind":"diff.row","kind":"diff.row","value":{}}"#,
    );
    for bad in [wrong_digest, oversized, duplicate] {
        assert!(store
            .append_note_artifact_journal("alice", "pages", &bad, &cost)
            .await
            .is_err());
    }
    let over = crate::ArtifactJournalRecordCost {
        index_entries: 3,
        storage_bytes: 128,
        final_manifest: false,
    };
    assert!(store
        .append_note_artifact_journal("alice", "pages", &first, &over)
        .await
        .is_err());
    let row = sqlx::query(
        "SELECT next_sequence,accepted_bytes,index_entries,storage_charge FROM note_artifact_job",
    )
    .fetch_one(store.artifact_pool().unwrap())
    .await
    .unwrap();
    for field in [
        "next_sequence",
        "accepted_bytes",
        "index_entries",
        "storage_charge",
    ] {
        assert_eq!(row.get::<i64, _>(field), 0);
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM note_artifact_ack")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap(),
        0
    );
    // All failed transactions returned the writer credit and left the original
    // digest usable for the first valid append.
    store
        .append_note_artifact_journal("alice", "pages", &first, &cost)
        .await
        .unwrap();
}

async fn artifact_publication_requests(
    store: &Store,
    begin: &intent_core::note_artifact::request::ArtifactBegin,
) -> (
    intent_core::note_artifact::request::ArtifactSeal,
    intent_core::note_artifact::request::ArtifactAdmit,
) {
    let job = store
        .begin_note_artifact_journal("alice", "pages", begin, artifact_retention(begin))
        .await
        .unwrap();
    let manifest = artifact_append_request(
        &job,
        0,
        &job.header_digest,
        r#"{"kind":"diff.manifest","value":{}}"#,
    );
    // Journal consistency fixture only: no native profile or physical index is
    // finalized by this deliberately minimal record.
    let cost = crate::ArtifactJournalRecordCost {
        index_entries: 1,
        storage_bytes: 128,
        final_manifest: true,
    };
    let ack = store
        .append_note_artifact_journal("alice", "pages", &manifest, &cost)
        .await
        .unwrap();
    assert!(ack.private_artifact_ref.is_none());
    (
        intent_core::note_artifact::request::ArtifactSeal {
            job_ref: job.job_ref.clone(),
            expected_records: 1,
            expected_bytes: u64::try_from(ack.accepted_bytes).unwrap(),
            final_digest: manifest.digest.clone(),
        },
        intent_core::note_artifact::request::ArtifactAdmit {
            job_ref: job.job_ref,
            admission_id: "admission".into(),
            final_digest: manifest.digest,
        },
    )
}

#[tokio::test]
async fn artifact_lease_record_read_is_scoped_indexed_and_revocable() {
    let (store, _temporary, _note, begin) = artifact_begin_fixture().await;
    let (seal, admit) = artifact_publication_requests(&store, &begin).await;
    let sealed = store
        .seal_note_artifact_journal("alice", "pages", &seal)
        .await
        .unwrap();
    for reference in [&seal.job_ref, sealed.private_artifact_ref.as_ref().unwrap()] {
        assert!(store
            .read_note_artifact_journal_record("alice", "pages", reference, 0)
            .await
            .is_err());
    }
    let lease = store
        .admit_note_artifact_journal("alice", "pages", &admit)
        .await
        .unwrap();
    // Drop construction-side request objects. This checks stored record backing,
    // not physical renderer retirement or a source-lease transfer implementation.
    drop(seal);
    drop(begin);
    let record = store
        .read_note_artifact_journal_record("alice", "pages", &lease.artifact_ref, 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.sequence, 0);
    assert_eq!(record.record, r#"{"kind":"diff.manifest","value":{}}"#);
    assert_eq!(record.digest, lease.final_digest);
    assert!(store
        .read_note_artifact_journal_record("alice", "pages", &lease.artifact_ref, 1)
        .await
        .unwrap()
        .is_none());
    for (principal, workspace) in [("bob", "pages"), ("alice", "other")] {
        assert!(store
            .read_note_artifact_journal_record(principal, workspace, &lease.artifact_ref, 0)
            .await
            .is_err());
    }
    assert!(store
        .read_note_artifact_journal_record("alice", "pages", &lease.artifact_ref, u64::MAX)
        .await
        .is_err());
    let plan = sqlx::query("EXPLAIN QUERY PLAN SELECT previous_digest,digest,record FROM note_artifact_record WHERE generation=? AND sequence=?")
        .bind(&lease.generation).bind(0_i64).fetch_all(store.artifact_pool().unwrap()).await.unwrap();
    let details = plan
        .iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>();
    assert!(
        details
            .iter()
            .any(|s| s.contains("SEARCH note_artifact_record")
                && s.contains("generation=? AND sequence=?")),
        "{details:?}"
    );
    store
        .release_note_artifact_lease("alice", "pages", &lease.artifact_ref)
        .await
        .unwrap();
    assert!(store
        .read_note_artifact_journal_record("alice", "pages", &lease.artifact_ref, 0)
        .await
        .is_err());
    let replay = store
        .admit_note_artifact_journal("alice", "pages", &admit)
        .await
        .unwrap();
    assert_eq!(replay, lease);
    assert!(store
        .read_note_artifact_journal_record("alice", "pages", &replay.artifact_ref, 0)
        .await
        .is_err());
}

#[tokio::test]
async fn artifact_record_reads_reject_abort_source_change_and_restart() {
    for transition in ["abort", "source", "restart"] {
        let (store, temporary, mut note, begin) = artifact_begin_fixture().await;
        let (seal, admit) = artifact_publication_requests(&store, &begin).await;
        store
            .seal_note_artifact_journal("alice", "pages", &seal)
            .await
            .unwrap();
        let lease = store
            .admit_note_artifact_journal("alice", "pages", &admit)
            .await
            .unwrap();
        assert!(store
            .read_note_artifact_journal_record("alice", "pages", &lease.artifact_ref, 0)
            .await
            .unwrap()
            .is_some());
        match transition {
            "abort" => {
                store
                    .abort_note_artifact_journal("alice", "pages", &seal.job_ref)
                    .await
                    .unwrap();
            }
            "source" => {
                note.title = "revoked source revision".into();
                store.update_note(&note).await.unwrap();
            }
            "restart" => {
                store.close().await;
                let restarted = crate::Store::open(&temporary.path).await.unwrap();
                restarted
                    .configure_test_note_artifact_arena(
                        &temporary.path.with_extension("artifacts.sqlite"),
                        1024,
                    )
                    .await
                    .unwrap();
                assert!(restarted
                    .read_note_artifact_journal_record("alice", "pages", &lease.artifact_ref, 0)
                    .await
                    .is_err());
                continue;
            }
            _ => unreachable!(),
        }
        assert!(
            store
                .read_note_artifact_journal_record("alice", "pages", &lease.artifact_ref, 0)
                .await
                .is_err(),
            "{transition}"
        );
    }
}

#[tokio::test]
async fn artifact_seal_and_admit_replay_cannot_revive_released_lease() {
    let (store, _temporary, _note, begin) = artifact_begin_fixture().await;
    let (seal, admit) = artifact_publication_requests(&store, &begin).await;
    assert!(store
        .admit_note_artifact_journal("alice", "pages", &admit)
        .await
        .is_err());
    let mut wrong = seal.clone();
    wrong.expected_bytes += 1;
    assert!(store
        .seal_note_artifact_journal("alice", "pages", &wrong)
        .await
        .is_err());
    wrong = seal.clone();
    wrong.final_digest = "f".repeat(64);
    assert!(store
        .seal_note_artifact_journal("alice", "pages", &wrong)
        .await
        .is_err());
    assert!(store
        .seal_note_artifact_journal("bob", "pages", &seal)
        .await
        .is_err());
    let sealed = store
        .seal_note_artifact_journal("alice", "pages", &seal)
        .await
        .unwrap();
    assert_eq!(sealed.state, "sealed");
    assert!(sealed.private_artifact_ref.is_some());
    let mut private_handle = admit.clone();
    private_handle.job_ref = sealed.private_artifact_ref.clone().unwrap();
    assert!(store
        .admit_note_artifact_journal("alice", "pages", &private_handle)
        .await
        .is_err());
    let replay = store
        .seal_note_artifact_journal("alice", "pages", &seal)
        .await
        .unwrap();
    assert_eq!(replay.private_artifact_ref, sealed.private_artifact_ref);
    assert!(store
        .admit_note_artifact_journal("bob", "pages", &admit)
        .await
        .is_err());
    let lease = store
        .admit_note_artifact_journal("alice", "pages", &admit)
        .await
        .unwrap();
    assert_ne!(
        Some(&lease.artifact_ref),
        sealed.private_artifact_ref.as_ref()
    );
    assert_eq!(
        store
            .admit_note_artifact_journal("alice", "pages", &admit)
            .await
            .unwrap(),
        lease
    );
    let mut another = admit.clone();
    another.admission_id = "another".into();
    assert!(store
        .admit_note_artifact_journal("alice", "pages", &another)
        .await
        .is_err());
    store
        .release_note_artifact_lease("alice", "pages", &lease.artifact_ref)
        .await
        .unwrap();
    assert_eq!(
        store
            .admit_note_artifact_journal("alice", "pages", &admit)
            .await
            .unwrap(),
        lease
    );
    // Exercise the already-defined logical cleanup transition. This direct SQL
    // fixture does not claim actual file/arena physical reclamation.
    sqlx::query("UPDATE note_artifact_job SET cleanup_complete=1")
        .execute(store.artifact_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(
        store
            .admit_note_artifact_journal("alice", "pages", &admit)
            .await
            .unwrap(),
        lease
    );
    let status = store
        .note_artifact_journal_status("alice", "pages", &begin.job_id, &begin.header_digest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.state, "admitted");
    assert!(status.cleanup_complete);
    assert_eq!(status.private_artifact_ref, sealed.private_artifact_ref);
    let row =
        sqlx::query("SELECT count(*) AS leases,sum(released) AS released FROM note_artifact_lease")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(row.get::<i64, _>("leases"), 1);
    assert_eq!(row.get::<i64, _>("released"), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT sum(jobs_reserved) FROM note_artifact_capacity")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn artifact_abort_and_admit_serialize_both_orders_without_reviving() {
    for abort_first in [true, false] {
        let (store, _temporary, _note, begin) = artifact_begin_fixture().await;
        let (seal, admit) = artifact_publication_requests(&store, &begin).await;
        store
            .seal_note_artifact_journal("alice", "pages", &seal)
            .await
            .unwrap();
        if abort_first {
            store
                .abort_note_artifact_journal("alice", "pages", &seal.job_ref)
                .await
                .unwrap();
            assert!(store
                .admit_note_artifact_journal("alice", "pages", &admit)
                .await
                .is_err());
            assert_eq!(
                sqlx::query_scalar::<_, i64>("SELECT count(*) FROM note_artifact_lease")
                    .fetch_one(store.artifact_pool().unwrap())
                    .await
                    .unwrap(),
                0
            );
        } else {
            let lease = store
                .admit_note_artifact_journal("alice", "pages", &admit)
                .await
                .unwrap();
            let aborted = store
                .abort_note_artifact_journal("alice", "pages", &seal.job_ref)
                .await
                .unwrap();
            assert_eq!(aborted.state, "aborted");
            assert!(aborted.private_artifact_ref.is_none());
            assert_eq!(
                store
                    .admit_note_artifact_journal("alice", "pages", &admit)
                    .await
                    .unwrap(),
                lease
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>("SELECT released FROM note_artifact_lease")
                    .fetch_one(store.artifact_pool().unwrap())
                    .await
                    .unwrap(),
                1
            );
        }
        assert!(store
            .seal_note_artifact_journal("alice", "pages", &seal)
            .await
            .is_err());
        let status = store
            .note_artifact_journal_status("alice", "pages", &begin.job_id, &begin.header_digest)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.state, "aborted");
        assert!(status.private_artifact_ref.is_none());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT sum(jobs_reserved) FROM note_artifact_capacity")
                .fetch_one(store.artifact_pool().unwrap())
                .await
                .unwrap(),
            3
        );
    }
}

#[tokio::test]
async fn artifact_publication_revalidates_source_after_private_seal() {
    let (store, temporary, mut note, begin) = artifact_begin_fixture().await;
    let (seal, admit) = artifact_publication_requests(&store, &begin).await;
    store
        .seal_note_artifact_journal("alice", "pages", &seal)
        .await
        .unwrap();
    note.title = "source revision advanced before admit".into();
    store.update_note(&note).await.unwrap();
    assert!(store
        .admit_note_artifact_journal("alice", "pages", &admit)
        .await
        .is_err());
    assert!(store
        .seal_note_artifact_journal("alice", "pages", &seal)
        .await
        .is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM note_artifact_lease")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap(),
        0
    );
    store.close().await;
    let restarted = Store::open(&temporary.path).await.unwrap();
    restarted
        .configure_test_note_artifact_arena(
            &temporary.path.with_extension("artifacts.sqlite"),
            1024,
        )
        .await
        .unwrap();
    assert!(restarted
        .admit_note_artifact_journal("alice", "pages", &admit)
        .await
        .is_err());
    restarted
        .abort_note_artifact_journal("alice", "pages", &seal.job_ref)
        .await
        .unwrap();
}

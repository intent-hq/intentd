//! Agent messages, payloads, paging, projections, unread probes, search and
//! monitor provenance on both backends. Sessions are not on the Postgres
//! store yet, so every run seeds its `agent_session` rows with SQL. Bodies use
//! fixed ids, leave their rows behind and assert exact values from an empty
//! start, so a Postgres statement without its tenant filter fails the
//! subject run; the decoy snapshot catches writes that cross tenants.

use std::collections::{HashMap, HashSet};

use base64::Engine as _;
use intent_core::{AgentId, AgentListRowScope, WorkspaceId};
use intent_store::{
    PrunedToolField, ReplaceMessage, Store, UsageMessageOrigin, UserMessageIndexPageRequest,
};
use serde_json::{json, Value};

use crate::harness::{exec, open, sqlite_store, tenant_rows, test_db, TestDb};

const WS: &str = "__chief__";

/// `(id, parent, is_background, metadata, created_at)`.
type Session = (
    &'static str,
    Option<&'static str>,
    i64,
    Option<&'static str>,
    &'static str,
);

const SESSIONS: &[Session] = &[
    ("a1", None, 0, None, "2026-01-01T00:00:00Z"),
    (
        "a2",
        None,
        0,
        Some(r#"{"lastSeenMessageId":"x"}"#),
        "2026-01-02T00:00:00Z",
    ),
    ("c1", Some("a1"), 0, None, "2026-01-03T00:00:00Z"),
    ("bg", None, 1, None, "2026-01-04T00:00:00Z"),
];

fn session_values(tenant: Option<&str>) -> String {
    SESSIONS
        .iter()
        .map(|(id, parent, bg, metadata, at)| {
            let lit = |v: Option<&str>| v.map_or("NULL".to_owned(), |v| format!("'{v}'"));
            let tenant = tenant.map_or(String::new(), |t| format!("'{t}', "));
            format!(
                "({tenant}'{id}', '{WS}', 'Agent {id}', 'idle', '{at}', '{at}', 'model-{id}', \
                 {}, {bg}, {})",
                lit(*parent),
                lit(*metadata)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

const SESSION_COLUMNS: &str =
    "id, workspace_id, name, status, created_at, updated_at, model, parent_agent_id, \
     is_background, metadata";

async fn seed_sqlite(store: &Store) {
    let sql = format!(
        "INSERT INTO agent_session ({SESSION_COLUMNS}) VALUES {}",
        session_values(None)
    );
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(store.write_pool())
        .await
        .expect("seed sqlite sessions");
}

async fn seed_postgres(db: &TestDb, tenant: &str) {
    exec(
        &db.admin,
        format!(
            "INSERT INTO agent_session (tenant_id, {SESSION_COLUMNS}) VALUES {}",
            session_values(Some(tenant))
        ),
    )
    .await;
}

/// Like `parity_test!`, with the sessions seeded before each run.
macro_rules! message_test {
    ($name:ident, |$store:ident, $tenant:ident| $body:block) => {
        #[tokio::test]
        async fn $name() {
            {
                eprintln!("[{}] backend: sqlite", stringify!($name));
                let (store, _dir) = sqlite_store().await;
                seed_sqlite(&store).await;
                let $store = store;
                let $tenant: &str = "sqlite";
                $body
            }
            let Some(db) = test_db(false).await else {
                return;
            };
            eprintln!("[{}] backend: postgres, tenant decoy", stringify!($name));
            {
                let $store = open(&db, "decoy").await;
                seed_postgres(&db, "decoy").await;
                let $tenant: &str = "decoy";
                $body
            }
            let subject = open(&db, "subject").await;
            seed_postgres(&db, "subject").await;
            let before = tenant_rows(&db.admin, "decoy").await;
            eprintln!("[{}] backend: postgres, tenant subject", stringify!($name));
            {
                let $store = subject;
                let $tenant: &str = "subject";
                $body
            }
            assert!(
                before == tenant_rows(&db.admin, "decoy").await,
                "the subject tenant's run changed the decoy tenant's rows"
            );
        }
    };
}

fn agent(id: &str) -> AgentId {
    AgentId(id.into())
}

fn ws() -> WorkspaceId {
    WorkspaceId(WS.into())
}

fn text(t: &str) -> Value {
    json!([{ "type": "text", "text": t }])
}

fn ids(messages: &[intent_core::AgentMessage]) -> Vec<&str> {
    messages.iter().map(|m| m.id.as_str()).collect()
}

/// A `tool_use` block whose input is past the inline limit, so it is
/// stored in `agent_message_payload`.
fn heavy_tool_use(tenant: &str) -> Value {
    json!({
        "type": "tool_use",
        "id": "tu1",
        "name": "write",
        "input": { "body": format!("{tenant} {}", "x".repeat(20_000)) },
    })
}

/// Base64 PNG of noise, past the slim budget, so it gets a thumbnail.
fn noise_png_base64(width: u32, height: u32) -> String {
    let img = image::RgbImage::from_fn(width, height, |x, y| {
        let v = u8::try_from(x.wrapping_mul(31).wrapping_add(y.wrapping_mul(17)) % 251).unwrap();
        image::Rgb([v, v.wrapping_add(97), v.wrapping_add(193)])
    });
    let mut buf = Vec::new();
    image::DynamicImage::ImageRgb8(img)
        .write_to(&mut std::io::Cursor::new(&mut buf), image::ImageFormat::Png)
        .expect("encode test png");
    base64::engine::general_purpose::STANDARD.encode(&buf)
}

message_test!(messages_append_read_and_page, |store, tenant| {
    let a1 = agent("a1");
    assert_eq!(store.count_agent_messages(&a1).await.unwrap(), 0);
    assert!(store
        .get_agent_messages(&a1, None)
        .await
        .unwrap()
        .is_empty());
    assert!(!store.agent_has_assistant_message(&a1).await.unwrap());
    assert!(store
        .persisted_queue_message_ids(&a1)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        store.get_workspace_message_watermark(&ws()).await.unwrap(),
        0
    );

    let meta = json!({
        "type": "chat",
        "extra": tenant,
        "queueInfo": { "queuedMessageId": "q1", "waitedMs": 5 },
    });
    let m1 = store
        .append_agent_message_with_id(
            &a1,
            "m1",
            "user",
            &text(&format!("hello {tenant}")),
            Some(&meta),
            "2026-02-01T00:00:00Z",
        )
        .await
        .unwrap();
    assert_eq!((m1.seq, m1.metadata.as_ref()), (0, Some(&meta)));
    store
        .append_agent_message_with_provenance(
            &a1,
            "m2",
            "assistant",
            &text("answer"),
            None,
            "2026-02-01T00:00:01Z",
            UsageMessageOrigin::Agent,
        )
        .await
        .unwrap();
    let sys = store
        .append_agent_message_with_metadata(
            &a1,
            "system",
            &json!("note"),
            None,
            "2026-02-01T00:00:02Z",
        )
        .await
        .unwrap();
    assert_eq!(sys.seq, 2);
    assert!(store.agent_has_assistant_message(&a1).await.unwrap());
    assert!(store
        .agent_has_assistant_after_last_user(&a1)
        .await
        .unwrap());
    store
        .append_agent_message_with_id(
            &a1,
            "m4",
            "user",
            &text("again"),
            None,
            "2026-02-01T00:00:03Z",
        )
        .await
        .unwrap();
    assert!(!store
        .agent_has_assistant_after_last_user(&a1)
        .await
        .unwrap());

    let all = store.get_agent_messages(&a1, None).await.unwrap();
    assert_eq!(ids(&all), ["m1", "m2", sys.id.as_str(), "m4"]);
    assert_eq!(all[0].content, text(&format!("hello {tenant}")));
    let tail = store.get_agent_messages(&a1, Some(2)).await.unwrap();
    assert_eq!(ids(&tail), [sys.id.as_str(), "m4"]);
    assert_eq!(store.count_agent_messages(&a1).await.unwrap(), 4);
    assert_eq!(
        store
            .get_last_non_system_message(&a1)
            .await
            .unwrap()
            .unwrap()
            .id,
        "m4"
    );
    assert_eq!(
        store.persisted_queue_message_ids(&a1).await.unwrap(),
        HashSet::from(["q1".to_owned()])
    );
    assert_eq!(
        store
            .get_agent_message_by_id(&a1, "m2")
            .await
            .unwrap()
            .unwrap()
            .role,
        "assistant"
    );
    assert!(store
        .get_agent_message_by_id(&agent("a2"), "m2")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        store.get_agent_message_index(&a1, "m2").await.unwrap(),
        Some(1)
    );
    assert_eq!(
        store.get_agent_message_index(&a1, "nope").await.unwrap(),
        None
    );
    let page = store.get_agent_messages_page(&a1, 1, 2).await.unwrap();
    assert_eq!(ids(&page), ["m2", sys.id.as_str()]);
    let stored = store
        .get_agent_messages_page_as_stored(&a1, 3, 10)
        .await
        .unwrap();
    assert_eq!(ids(&stored), ["m4"]);

    let (batch, total) = store.get_agent_history_batch(&a1, None, 8).await.unwrap();
    assert_eq!(
        (ids(&batch), total),
        (vec!["m4", sys.id.as_str(), "m2", "m1"], 4)
    );
    let (batch, _) = store
        .get_agent_history_batch(&a1, Some(2), 1)
        .await
        .unwrap();
    assert_eq!(ids(&batch), ["m2"]);

    let index = store.get_agent_user_message_index(&a1, 5).await.unwrap();
    let rows: Vec<_> = index
        .iter()
        .map(|i| {
            (
                i.id.as_str(),
                i.seq,
                i.preview.as_str(),
                i.qualified_monitor,
            )
        })
        .collect();
    assert_eq!(rows, [("m1", 0, "hello", false), ("m4", 3, "again", false)]);
    assert_eq!(
        index[0].metadata,
        Some(json!({ "type": "chat", "queueInfo": { "waitedMs": 5 } }))
    );
    let request = |before_seq| UserMessageIndexPageRequest {
        limit: 1,
        before_seq,
    };
    let newest = store
        .get_agent_user_message_index_page(&a1, 50, Some(request(None)))
        .await
        .unwrap();
    assert_eq!(
        newest
            .items
            .iter()
            .map(|i| i.id.as_str())
            .collect::<Vec<_>>(),
        ["m4"]
    );
    assert_eq!(newest.next_before_seq, Some(3));
    let older = store
        .get_agent_user_message_index_page(&a1, 50, Some(request(Some(3))))
        .await
        .unwrap();
    assert_eq!(older.items[0].preview, format!("hello {tenant}"));
    assert_eq!((older.items.len(), older.next_before_seq), (1, None));

    let projection = store
        .get_agent_session_message_projection(&a1)
        .await
        .unwrap();
    assert_eq!(projection.message_count, 4);
    assert_eq!(projection.last_message_role.as_deref(), Some("user"));
    assert_eq!(projection.last_message_id.as_deref(), Some("m4"));
    assert_eq!(
        projection.last_user_text_blocks,
        Some(vec!["again".to_owned()])
    );
    assert_eq!(
        projection.last_assistant_text_blocks,
        Some(vec!["answer".to_owned()])
    );
    assert_eq!(
        store
            .get_agent_session_message_projection(&agent("none"))
            .await
            .unwrap(),
        intent_store::SessionMessageProjection::default()
    );
    let all_projections = store
        .get_agent_session_message_projections(&ws())
        .await
        .unwrap();
    assert_eq!(all_projections.len(), 4);
    assert_eq!(all_projections["a1"], projection);
    assert_eq!(all_projections["a2"].message_count, 0);
    let stats = store.get_agent_session_message_stats(&ws()).await.unwrap();
    assert_eq!(stats.len(), 4);
    assert!(matches!(stats["a1"], (4, true, bytes) if bytes > 0));
    assert_eq!(stats["a2"], (0, false, 0));
    assert_eq!(
        store.get_workspace_message_watermark(&ws()).await.unwrap(),
        4
    );
});

message_test!(scoped_session_projections, |store, _tenant| {
    let keys = |map: HashMap<String, _>| {
        let mut k: Vec<String> = map.into_keys().collect();
        k.sort();
        k
    };
    let scoped = |scope| {
        let store = &store;
        async move {
            keys(
                store
                    .get_scoped_agent_session_message_projections(&ws(), &scope)
                    .await
                    .unwrap(),
            )
        }
    };
    assert_eq!(scoped(AgentListRowScope::TopLevel).await, ["a1", "a2"]);
    assert_eq!(scoped(AgentListRowScope::Background).await, ["bg"]);
    let delegated = |parent: Option<&str>, orphaned_only| AgentListRowScope::Delegated {
        parent_agent_id: parent.map(agent),
        orphaned_only,
    };
    assert_eq!(scoped(delegated(None, false)).await, ["c1"]);
    assert_eq!(scoped(delegated(Some("a1"), false)).await, ["c1"]);
    assert!(scoped(delegated(Some("a2"), false)).await.is_empty());
    assert!(scoped(delegated(None, true)).await.is_empty());
    assert_eq!(
        keys(
            store
                .get_active_agent_session_message_projections(&ws())
                .await
                .unwrap()
        ),
        ["a1", "a2", "bg", "c1"]
    );
    assert!(store
        .get_retired_agent_session_message_projections(&ws())
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .get_agent_session_message_projections(&WorkspaceId("ws-none".into()))
        .await
        .unwrap()
        .is_empty());
});

message_test!(unread_probes_and_question_tails, |store, tenant| {
    let other = WorkspaceId("ws-none".into());
    assert!(!store
        .workspace_has_unread_top_level_session(&ws())
        .await
        .unwrap());
    assert!(store
        .workspaces_with_unread_top_level_sessions()
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .list_top_level_sessions_with_unseen_last_message(&ws())
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .list_legacy_question_tail_candidates_by_workspace(&[ws()])
        .await
        .unwrap()
        .is_empty());

    let answer = text(&format!("done {tenant}"));
    for (id, msg) in [("a1", "u1"), ("a2", "u2"), ("c1", "u3"), ("bg", "u4")] {
        store
            .append_agent_message_with_id(
                &agent(id),
                msg,
                "assistant",
                &answer,
                None,
                "2026-03-01T00:00:00Z",
            )
            .await
            .unwrap();
    }
    store
        .append_agent_message_with_id(
            &agent("a2"),
            "u5",
            "system",
            &json!("sys"),
            None,
            "2026-03-01T00:00:01Z",
        )
        .await
        .unwrap();
    assert!(store
        .workspace_has_unread_top_level_session(&ws())
        .await
        .unwrap());
    assert!(!store
        .workspace_has_unread_top_level_session(&other)
        .await
        .unwrap());
    assert_eq!(
        store
            .workspaces_with_unread_top_level_sessions()
            .await
            .unwrap(),
        HashSet::from([WS.to_owned()])
    );
    assert_eq!(
        store
            .workspaces_with_unread_top_level_sessions_by_workspace(&[ws(), other.clone()])
            .await
            .unwrap(),
        HashSet::from([ws()])
    );
    assert!(store
        .workspaces_with_unread_top_level_sessions_by_workspace(&[])
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .list_top_level_sessions_with_unseen_last_message(&ws())
            .await
            .unwrap(),
        [
            ("a1".to_owned(), "u1".to_owned()),
            ("a2".to_owned(), "u2".to_owned())
        ]
    );
    let mut tails = store
        .list_legacy_question_tail_candidates_by_workspace(&[ws(), other])
        .await
        .unwrap();
    tails.sort_by(|a, b| a.0 .0.cmp(&b.0 .0));
    assert_eq!(
        tails,
        [
            (
                agent("a1"),
                "u1".to_owned(),
                "assistant".to_owned(),
                answer.clone()
            ),
            (agent("a2"), "u2".to_owned(), "assistant".to_owned(), answer),
        ]
    );
});

message_test!(
    heavy_payloads_hydrate_compact_and_prestage,
    |store, tenant| {
        let a1 = agent("a1");
        let heavy = heavy_tool_use(tenant);
        let content = json!([heavy.clone()]);
        store
            .append_agent_message_with_id(
                &a1,
                "h1",
                "assistant",
                &content,
                None,
                "2026-04-01T00:00:00Z",
            )
            .await
            .unwrap();
        let full = store
            .get_agent_message_by_id(&a1, "h1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(full.content, content);
        let stored = store
            .get_agent_messages_page_as_stored(&a1, 0, 10)
            .await
            .unwrap();
        assert_eq!(stored[0].content[0]["inputTruncated"], json!(true));
        assert_eq!(
            store.get_agent_messages(&a1, None).await.unwrap()[0].content,
            content
        );
        let (_, pruned) = store
            .get_agent_message_by_id_with_pruned(&a1, "h1")
            .await
            .unwrap()
            .unwrap();
        assert!(pruned.is_empty());

        // Too early: nothing before the cutoff.
        assert_eq!(
            store
                .compact_tool_payloads_before("2026-03-01T00:00:00Z", 100)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .compact_tool_payloads_before("2026-05-01T00:00:00Z", 100)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .compact_tool_payloads_before("2026-05-01T00:00:00Z", 100)
                .await
                .unwrap(),
            0
        );
        let (pruned_message, pruned) = store
            .get_agent_message_by_id_with_pruned(&a1, "h1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pruned.len(), 1);
        assert_eq!(
            (pruned[0].block_ordinal, pruned[0].field),
            (0, PrunedToolField::ToolUseInput)
        );
        assert_eq!(pruned_message.content[0]["inputTruncated"], json!(true));
        let replay = store.get_agent_messages_for_replay(&a1, 100).await.unwrap();
        assert_eq!(ids(&replay), ["h1"]);
        assert_ne!(replay[0].content, content);

        // Pre-staged payload: an orphan is deleted; an adopted one stays.
        let slim = store
            .prestage_agent_message_payload(&a1, "p1", 0, &heavy)
            .await
            .unwrap()
            .expect("heavy block is staged");
        assert!(store
            .prestage_agent_message_payload(&a1, "p1", 1, &json!({"type": "text", "text": "small"}))
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            store
                .delete_prestaged_agent_message_payloads("p1")
                .await
                .unwrap(),
            1
        );
        store
            .prestage_agent_message_payload(&a1, "p2", 0, &heavy)
            .await
            .unwrap();
        store
            .append_agent_message_prestaged(
                &a1,
                "p2",
                "assistant",
                &json!([slim]),
                None,
                "2026-04-01T00:00:01Z",
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .delete_prestaged_agent_message_payloads("p2")
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .get_agent_message_by_id(&a1, "p2")
                .await
                .unwrap()
                .unwrap()
                .content,
            content
        );
        assert_eq!(
            store
                .get_agent_messages_for_replay(&a1, 100)
                .await
                .unwrap()
                .len(),
            2
        );
    }
);

message_test!(thumbnails_are_read_per_message, |store, _tenant| {
    let a1 = agent("a1");
    let image =
        json!([{ "type": "image", "mimeType": "image/png", "data": noise_png_base64(64, 64) }]);
    store
        .append_agent_message_with_id(&a1, "i1", "user", &image, None, "2026-04-02T00:00:00Z")
        .await
        .unwrap();
    store
        .append_agent_message_with_id(
            &a1,
            "i2",
            "user",
            &text("plain"),
            None,
            "2026-04-02T00:00:01Z",
        )
        .await
        .unwrap();
    assert!(store
        .get_agent_message_thumbnails(&a1, &[])
        .await
        .unwrap()
        .is_empty());
    let thumbs = store
        .get_agent_message_thumbnails(&a1, &["i1".into(), "i2".into(), "none".into()])
        .await
        .unwrap();
    assert_eq!(thumbs.keys().collect::<Vec<_>>(), ["i1"]);
    assert!(thumbs["i1"]["0"]["data"].is_string());
    assert!(store
        .get_agent_message_thumbnails(&agent("a2"), &["i1".into()])
        .await
        .unwrap()
        .is_empty());
});

message_test!(replace_and_truncate_transcripts, |store, tenant| {
    let a1 = agent("a1");
    for (i, role) in ["user", "assistant", "user"].iter().enumerate() {
        store
            .append_agent_message_with_id(
                &a1,
                &format!("r{i}"),
                role,
                &text(&format!("{role} {i}")),
                None,
                "2026-05-01T00:00:00Z",
            )
            .await
            .unwrap();
    }
    let first = text("user 0");
    let fresh = text(&format!("fresh {tenant}"));
    let replaced = store
        .replace_agent_messages(
            &a1,
            &[
                ReplaceMessage {
                    role: "user",
                    content: &first,
                    metadata: None,
                    created_at: "2026-05-01T00:00:00Z",
                },
                ReplaceMessage {
                    role: "assistant",
                    content: &fresh,
                    metadata: None,
                    created_at: "2026-05-02T00:00:00Z",
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(replaced.iter().map(|m| m.seq).collect::<Vec<_>>(), [0, 1]);
    let all = store.get_agent_messages(&a1, None).await.unwrap();
    assert_eq!(
        all.iter()
            .map(|m| (m.id.as_str(), &m.content))
            .collect::<Vec<_>>(),
        [
            (replaced[0].id.as_str(), &first),
            (replaced[1].id.as_str(), &fresh)
        ]
    );
    let projection = store
        .get_agent_session_message_projection(&a1)
        .await
        .unwrap();
    assert_eq!(projection.message_count, 2);
    assert_eq!(
        projection.last_message_id.as_deref(),
        Some(replaced[1].id.as_str())
    );
    assert_eq!(projection.last_message_role.as_deref(), Some("assistant"));

    assert_eq!(store.truncate_agent_messages_from(&a1, 1).await.unwrap(), 1);
    assert_eq!(store.truncate_agent_messages_from(&a1, 5).await.unwrap(), 0);
    assert_eq!(store.count_agent_messages(&a1).await.unwrap(), 1);
    let projection = store
        .get_agent_session_message_projection(&a1)
        .await
        .unwrap();
    assert_eq!(projection.last_message_role.as_deref(), Some("user"));
    assert_eq!(projection.last_assistant_text_blocks, None);
    assert_eq!(
        projection.last_message_id.as_deref(),
        Some(replaced[0].id.as_str())
    );
    // Decoy-only rows in the range the subject's truncate deletes.
    if tenant == "decoy" {
        store
            .append_agent_message_with_id(
                &a1,
                "keep",
                "user",
                &text("decoy only"),
                None,
                "2026-05-03T00:00:00Z",
            )
            .await
            .unwrap();
    }
});

message_test!(message_search_matches_and_filters, |store, tenant| {
    let a1 = agent("a1");
    let search =
        |expr: &'static str, agent_id: Option<&'static str>, role: Option<&'static str>| {
            let store = &store;
            async move {
                let mut hits: Vec<String> = store
                    .search_agent_messages_fts(expr, None, agent_id, role, None, None)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|h| h.message_id)
                    .collect();
                hits.sort();
                hits
            }
        };
    assert!(search("\"rocket\"", None, None).await.is_empty());
    store
        .append_agent_message_with_id(
            &a1,
            "s1",
            "user",
            &text(&format!("deploy the rockets {tenant}")),
            None,
            "2026-06-01T00:00:00Z",
        )
        .await
        .unwrap();
    store
        .append_agent_message_with_id(
            &a1,
            "s2",
            "assistant",
            &text("rocket launched"),
            None,
            "2026-06-01T00:00:01Z",
        )
        .await
        .unwrap();
    store
        .append_agent_message_with_id(
            &a1,
            "s3",
            "system",
            &text("rocket system"),
            None,
            "2026-06-01T00:00:02Z",
        )
        .await
        .unwrap();
    assert_eq!(search("\"rocket\"", None, None).await, ["s1", "s2"]);
    assert_eq!(search("\"rocket\"", None, Some("user")).await, ["s1"]);
    assert!(search("\"rocket\"", Some("a2"), None).await.is_empty());
    assert_eq!(
        search("\"deploy\" AND \"rocket\"", None, None).await,
        ["s1"]
    );
    assert_eq!(search("\"the\"", None, None).await, ["s1"]);
    let hits = store
        .search_agent_messages_fts("\"rocket\"", Some(&ws()), None, None, Some(&ws()), Some(1))
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(
        (hits[0].workspace_id.as_str(), hits[0].agent_name.as_str()),
        (WS, "Agent a1")
    );
    assert!(store
        .search_agent_messages_fts(
            "\"rocket\"",
            Some(&WorkspaceId("ws-none".into())),
            None,
            None,
            None,
            None
        )
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .search_agent_messages_fts("\"rocket\"", None, None, None, None, Some(0))
        .await
        .unwrap()
        .is_empty());
});

// Provenance rows are written only by the monitor message path (pr-monitor
// domain, not on Postgres yet); a stored row whose digest does not match
// the message, or no row at all, is not evidence.
message_test!(
    monitor_provenance_needs_matching_evidence,
    |store, _tenant| {
        let a1 = agent("a1");
        store
            .append_agent_message_with_id(
                &a1,
                "pm1",
                "user",
                &text("wake"),
                None,
                "2026-07-01T00:00:00Z",
            )
            .await
            .unwrap();
        assert!(store
            .get_monitor_message_provenance(&a1, "pm1")
            .await
            .unwrap()
            .is_none());
        assert!(store
            .get_monitor_message_provenance(&a1, "none")
            .await
            .unwrap()
            .is_none());
    }
);

// Appending to a session that does not exist fails on both backends.
message_test!(append_to_a_missing_session_fails, |store, _tenant| {
    assert!(store
        .append_agent_message(
            &agent("missing"),
            "user",
            &text("x"),
            "2026-08-01T00:00:00Z"
        )
        .await
        .is_err());
    assert!(store
        .replace_agent_messages(&agent("missing"), &[])
        .await
        .is_err());
});

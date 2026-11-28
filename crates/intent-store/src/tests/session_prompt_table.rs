//! Migration 0205 moves `agent_session.system_prompt` and its
//! `prompt_breakdown` into `agent_session_prompt`, so the ~70 KB prompt no
//! longer sits in front of the columns every summary read selects. These
//! tests pin the upgrade (existing prompts move, rows keep everything else),
//! the full-row write semantics the column had, the cascade on delete, and
//! the transfer archive format (session rows still carry both values, and
//! archives written before the move import unchanged).
use super::*;

async fn columns<'e>(
    pool: impl sqlx::Executor<'e, Database = sqlx::Sqlite>,
    table: &str,
) -> Vec<String> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT name FROM pragma_table_info('{table}') ORDER BY cid"
    )))
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn stored_prompt(store: &Store, agent: &str) -> Option<(Option<String>, Option<String>)> {
    sqlx::query_as(
        "SELECT system_prompt, prompt_breakdown FROM agent_session_prompt WHERE agent_id = ?",
    )
    .bind(agent)
    .fetch_optional(store.read_pool())
    .await
    .unwrap()
}

fn session(id: &str, ws: &WorkspaceId, prompt: Option<&str>) -> AgentSession {
    AgentSession {
        id: AgentId::from(id),
        workspace_id: ws.clone(),
        name: id.into(),
        status: AgentStatus::Idle,
        system_prompt: prompt.map(str::to_string),
        created_at: "2026-01-01T00:00:00Z".into(),
        updated_at: "2026-01-01T00:00:00Z".into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn migration_moves_prompts_out_of_the_session_row() {
    let tmp = TempDb::new();
    let pool = crate::connect_write(&tmp.path).await.unwrap();
    let prefix = sqlx::migrate::Migrator {
        migrations: std::borrow::Cow::Owned(
            crate::MIGRATOR
                .iter()
                .filter(|m| m.version <= 204)
                .cloned()
                .collect(),
        ),
        ..sqlx::migrate::Migrator::DEFAULT
    };
    prefix.run(&pool).await.unwrap();
    let prompt = "p".repeat(70_000);
    sqlx::raw_sql(
        r"
        INSERT INTO workspace(id,title,branch,created_at,updated_at)
          VALUES ('ws','W','main','c','u');
        INSERT INTO agent_session(id,workspace_id,name,status,created_at,updated_at,metadata)
          VALUES ('no-prompt','ws','Bare','idle','c','u','{}');
        ",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO agent_session(id,workspace_id,name,status,created_at,updated_at,\
         system_prompt,prompt_breakdown,metadata,goal) \
         VALUES ('prompted','ws','Prompted','idle','c','u',?,'{\"total\":1}','{\"k\":1}','ship')",
    )
    .bind(&prompt)
    .execute(&pool)
    .await
    .unwrap();

    crate::MIGRATOR.run(&pool).await.unwrap();

    let session_columns = columns(&pool, "agent_session").await;
    for gone in ["system_prompt", "prompt_breakdown"] {
        assert!(
            !session_columns.iter().any(|c| c == gone),
            "{gone} left agent_session: {session_columns:?}"
        );
    }
    let moved: Vec<(String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT agent_id, system_prompt, prompt_breakdown FROM agent_session_prompt",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        moved,
        vec![(
            "prompted".to_string(),
            Some(prompt.clone()),
            Some("{\"total\":1}".to_string())
        )],
        "only the session with a prompt gets a row, with both values"
    );
    let kept: (String, Option<String>, Option<String>) =
        sqlx::query_as("SELECT name, metadata, goal FROM agent_session WHERE id = 'prompted'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        kept,
        (
            "Prompted".to_string(),
            Some("{\"k\":1}".to_string()),
            Some("ship".to_string())
        ),
        "every other session field survives the column drops"
    );
    pool.close().await;

    let store = Store::open(&tmp.path).await.unwrap();
    let full = store
        .get_agent_session_without_messages(&AgentId::from("prompted"))
        .await
        .unwrap();
    assert_eq!(full.system_prompt.as_deref(), Some(prompt.as_str()));
    let bare = store
        .get_agent_session_without_messages(&AgentId::from("no-prompt"))
        .await
        .unwrap();
    assert_eq!(bare.system_prompt, None);
    store.close().await;
}

#[tokio::test]
async fn full_row_writes_store_the_prompt_beside_the_session_row() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.unwrap();
    let ws = WorkspaceId::from("ws-prompt");
    store
        .insert_workspace(&sample_workspace(&ws, "prompt", false))
        .await
        .unwrap();
    store
        .insert_agent_session(&session("a", &ws, Some("first")))
        .await
        .unwrap();
    store
        .insert_agent_session(&session("bare", &ws, None))
        .await
        .unwrap();
    assert_eq!(
        stored_prompt(&store, "a").await,
        Some((Some("first".into()), None))
    );
    assert_eq!(
        stored_prompt(&store, "bare").await,
        None,
        "a session without a prompt stores no prompt row"
    );

    let mut full = store
        .get_agent_session_without_messages(&AgentId::from("a"))
        .await
        .unwrap();
    assert_eq!(full.system_prompt.as_deref(), Some("first"));
    full.system_prompt = Some("second".into());
    store.update_agent_session(&ws, &full).await.unwrap();
    assert_eq!(
        stored_prompt(&store, "a").await,
        Some((Some("second".into()), None))
    );
    let listed = store.list_all_agent_sessions().await.unwrap();
    let by_id = |id: &str| {
        listed
            .iter()
            .find(|s| s.id.as_str() == id)
            .unwrap()
            .system_prompt
            .clone()
    };
    assert_eq!(by_id("a").as_deref(), Some("second"));
    assert_eq!(by_id("bare"), None);

    full.system_prompt = None;
    store.update_agent_session(&ws, &full).await.unwrap();
    assert_eq!(
        stored_prompt(&store, "a").await,
        None,
        "clearing the prompt drops its row"
    );
    assert_eq!(
        store
            .get_agent_session_without_messages(&AgentId::from("a"))
            .await
            .unwrap()
            .system_prompt,
        None
    );

    store
        .set_agent_session_system_prompt(&ws, &AgentId::from("bare"), "narrow")
        .await
        .unwrap();
    assert_eq!(
        stored_prompt(&store, "bare").await,
        Some((Some("narrow".into()), None))
    );
    assert!(matches!(
        store
            .set_agent_session_system_prompt(
                &WorkspaceId::from("other"),
                &AgentId::from("bare"),
                "x"
            )
            .await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(
        stored_prompt(&store, "bare").await,
        Some((Some("narrow".into()), None)),
        "a write scoped to another workspace changes nothing"
    );

    sqlx::query("DELETE FROM agent_session WHERE id = 'bare'")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert_eq!(
        stored_prompt(&store, "bare").await,
        None,
        "deleting the session deletes its prompt"
    );
}

#[tokio::test]
async fn transfer_rows_carry_the_prompt_and_import_old_archives() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.unwrap();
    let ws = WorkspaceId::from("ws-export");
    store
        .insert_workspace(&sample_workspace(&ws, "export", false))
        .await
        .unwrap();
    let prompt = "x".repeat(5_000);
    store
        .insert_agent_session(&session("exported", &ws, Some(&prompt)))
        .await
        .unwrap();
    store
        .insert_agent_session(&session("exported-bare", &ws, None))
        .await
        .unwrap();

    let rows = store.transfer_export_rows(&ws).await.unwrap();
    let sessions = &rows.iter().find(|(t, _)| t == "agent_session").unwrap().1;
    let exported = sessions.iter().find(|r| r["id"] == "exported").unwrap();
    assert_eq!(exported["system_prompt"], json!(prompt));
    assert_eq!(exported["prompt_breakdown"], serde_json::Value::Null);
    let bare = sessions
        .iter()
        .find(|r| r["id"] == "exported-bare")
        .unwrap();
    assert_eq!(bare["system_prompt"], serde_json::Value::Null);

    let stats = store.transfer_table_stats(&ws).await.unwrap();
    let session_bytes = stats
        .iter()
        .find(|s| s.name == "agent_session")
        .unwrap()
        .approx_bytes;
    assert!(
        session_bytes >= i64::try_from(prompt.len()).unwrap(),
        "the plan's estimate counts the prompt that rides the rows: {session_bytes}"
    );

    // An archive row as a daemon before 0205 wrote it: the prompt and its
    // breakdown are plain agent_session columns.
    let target_db = TempDb::new();
    let target = Store::open(&target_db.path).await.unwrap();
    let imported_ws = "ws-imported";
    let archive = vec![
        (
            "workspace".to_string(),
            vec![
                json!({"id": imported_ws, "title": "Imported", "branch": "main",
                        "created_at": "c", "updated_at": "u"}),
            ],
        ),
        (
            "agent_session".to_string(),
            vec![
                json!({"id": "old-archive", "workspace_id": imported_ws, "name": "Old",
                       "status": "idle", "created_at": "c", "updated_at": "u",
                       "system_prompt": "archived prompt",
                       "prompt_breakdown": "{\"total\":2}"}),
                json!({"id": "old-archive-bare", "workspace_id": imported_ws, "name": "Bare",
                       "status": "idle", "created_at": "c", "updated_at": "u",
                       "system_prompt": null, "prompt_breakdown": null}),
            ],
        ),
    ];
    target.transfer_import_rows(&archive).await.unwrap();
    assert_eq!(
        stored_prompt(&target, "old-archive").await,
        Some((Some("archived prompt".into()), Some("{\"total\":2}".into())))
    );
    assert_eq!(stored_prompt(&target, "old-archive-bare").await, None);
    assert_eq!(
        target
            .get_agent_session_without_messages(&AgentId::from("old-archive"))
            .await
            .unwrap()
            .system_prompt
            .as_deref(),
        Some("archived prompt")
    );
}

/// A hostile or corrupt archive whose session row carries a prompt but no
/// text `id` is a params error, not an internal one, and imports nothing.
#[tokio::test]
async fn transfer_import_rejects_a_prompt_on_a_session_without_a_text_id() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.unwrap();
    for id in [serde_json::Value::Null, json!(7)] {
        let archive = vec![
            (
                "workspace".to_string(),
                vec![json!({"id": "ws-bad-id", "title": "Bad", "branch": "main",
                            "created_at": "c", "updated_at": "u"})],
            ),
            (
                "agent_session".to_string(),
                vec![json!({"id": id, "workspace_id": "ws-bad-id", "name": "Bad",
                           "status": "idle", "created_at": "c", "updated_at": "u",
                           "system_prompt": "orphan prompt"})],
            ),
        ];
        let result = store.transfer_import_rows(&archive).await;
        assert!(
            matches!(result, Err(Error::InvalidParams(_))),
            "id {id}: {result:?}"
        );
        let sessions: i64 = sqlx::query_scalar("SELECT count(*) FROM agent_session")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
        assert_eq!(sessions, 0, "id {id}: the whole batch rolls back");
    }
}

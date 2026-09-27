use super::*;
use serde_json::Value;
use std::sync::Arc;

async fn seed(store: &Store) -> (WorkspaceId, AgentId) {
    let ws = WorkspaceId::new();
    let agent = AgentId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "Transfer", false))
        .await
        .unwrap();
    store
        .insert_agent_session(&sample_agent_session(&agent, &ws))
        .await
        .unwrap();
    (ws, agent)
}

#[tokio::test]
async fn transfer_human_profiles_share_the_rows_wal_snapshot() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let (ws, agent) = seed(&store).await;
    let mut owner = store.get_primary_principal().await.unwrap();
    owner.login = Some("original".into());
    owner.identity = Some(PrincipalIdentity::github(42));
    store.upsert_principal(&owner).await.unwrap();
    for (id, md) in [
        ("legacy", None),
        ("stamped", Some(json!({"fromPrincipalId":owner.id}))),
        ("missing", Some(json!({"fromPrincipalId":"absent"}))),
    ] {
        store
            .append_agent_message_with_id(
                &agent,
                id,
                "user",
                &json!([{"type":"text","text":id}]),
                md.as_ref(),
                "2020-01-01T00:00:00Z",
            )
            .await
            .unwrap();
    }
    let barrier = Arc::new(crate::transfer_authorship::ExportAuthorBarrier::default());
    *store.export_author_barrier.lock().unwrap() = Some(barrier.clone());
    let exporter = store.clone();
    let export_ws = ws.clone();
    let export = tokio::spawn(async move { exporter.transfer_export_rows(&export_ws).await });
    barrier.entered.notified().await;
    owner.login = Some("changed-after-rows".into());
    owner.identity = Some(PrincipalIdentity {
        provider: "gitlab".into(),
        host: "gitlab.example".into(),
        external_user_id: "42".into(),
    });
    store.upsert_principal(&owner).await.unwrap();
    // A concurrent message also stays outside this coherent snapshot.
    store
        .append_agent_message_with_id(
            &agent,
            "later",
            "user",
            &json!([]),
            None,
            "2020-01-02T00:00:00Z",
        )
        .await
        .unwrap();
    barrier.release.notify_one();
    let rows = export.await.unwrap().unwrap();
    let messages = &rows.iter().find(|(t, _)| t == "agent_message").unwrap().1;
    assert_eq!(messages.len(), 3);
    for row in messages {
        let md: Value = serde_json::from_str(row["metadata"].as_str().unwrap()).unwrap();
        if row["id"] == "missing" {
            assert!(md["humanAuthor"]["login"].is_null());
            assert_eq!(md["humanAuthor"]["sourcePrincipalId"], "absent");
            assert!(md["humanAuthor"].get("identity").is_none());
        } else {
            assert_eq!(md["humanAuthor"]["login"], "original");
            assert_eq!(md["humanAuthor"]["identity"]["provider"], "github");
        }
    }
    assert_eq!(
        store
            .get_primary_principal()
            .await
            .unwrap()
            .login
            .as_deref(),
        Some("changed-after-rows")
    );
    let source: Option<String> =
        sqlx::query_scalar("SELECT metadata FROM agent_message WHERE id='legacy'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert!(source.is_none(), "export must not rewrite source history");
}

fn previous_migrator() -> sqlx::migrate::Migrator {
    sqlx::migrate::Migrator {
        migrations: std::borrow::Cow::Owned(
            crate::MIGRATOR
                .iter()
                .filter(|m| m.version <= 134)
                .cloned()
                .collect(),
        ),
        ..sqlx::migrate::Migrator::DEFAULT
    }
}

#[tokio::test]
async fn transfer_human_trust_migration_cleans_legacy_keys_once_and_fences_downgrades() {
    let tmp = TempDb::new();
    let write_pool = crate::connect_write(&tmp.path).await.unwrap();
    previous_migrator().run(&write_pool).await.unwrap();
    let store = Store {
        write_pool,
        read_pool: crate::connect_read(&tmp.path).await.unwrap(),
        browser_tab_displayed: crate::browser_tab_repo::DisplayedOverlay::default(),
        export_author_barrier: Arc::default(),
    };
    let (_ws, agent) = seed(&store).await;
    let old =
        json!({"humanAuthor":{"login":"planted"},"fromPrincipalId":"real-source","keep":{"x":7}});
    store
        .append_agent_message_with_id(
            &agent,
            "old-message",
            "user",
            &json!([{"type":"text","text":"unchanged"}]),
            Some(&old),
            "2020-01-01T00:00:00Z",
        )
        .await
        .unwrap();
    store.replace_agent_queue(&agent,&[AgentQueueRow{id:"old-queue".into(),agent_id:agent.clone(),position:0,payload:json!({"id":"old-queue","content":"pending","messageMetadata":old,"other":42}),created_at:"2020-01-01T00:00:00Z".into(),turn_id:"turn-original".into()}]).await.unwrap();
    // No comment schema changed; seed arbitrary pre-reservation extras.
    sqlx::query("INSERT INTO comment (id,thread_id,workspace_id,kind,content,author,author_type,status,anchor_json,created_at,updated_at,extra_json) VALUES ('old-comment','old-comment',?,'comment','text','label','user','active','{}','2020-01-01','2020-01-01',?)")
        .bind(agent_workspace(&store,&agent).await).bind(json!({"authorPrincipalId":"forged","authorIdentity":{"provider":"github","host":"github.com","externalUserId":"7"},"sourceAuthorPrincipalId":"forged","keep":42}).to_string()).execute(store.write_pool()).await.unwrap();
    store.close().await;
    let upgraded = Store::open(&tmp.path).await.unwrap();
    let row =
        sqlx::query("SELECT metadata,created_at,content FROM agent_message WHERE id='old-message'")
            .fetch_one(upgraded.read_pool())
            .await
            .unwrap();
    let md: Value = serde_json::from_str(&row.get::<String, _>("metadata")).unwrap();
    assert!(md.get("humanAuthor").is_none());
    assert_eq!(md["fromPrincipalId"], "real-source");
    assert_eq!(md["keep"]["x"], 7);
    assert_eq!(row.get::<String, _>("created_at"), "2020-01-01T00:00:00Z");
    assert!(row.get::<String, _>("content").contains("unchanged"));
    let raw: String = sqlx::query_scalar("SELECT payload FROM agent_queue WHERE id='old-queue'")
        .fetch_one(upgraded.read_pool())
        .await
        .unwrap();
    let payload: Value = serde_json::from_str(&raw).unwrap();
    assert!(payload["messageMetadata"].get("humanAuthor").is_none());
    assert_eq!(payload["other"], 42);
    assert_eq!(payload["content"], "pending");
    let raw: String = sqlx::query_scalar("SELECT extra_json FROM comment WHERE id='old-comment'")
        .fetch_one(upgraded.read_pool())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&raw).unwrap(),
        json!({"keep":42})
    );
    // After the one-time trust boundary, legitimate v2 history survives reopen.
    let trusted = json!({"humanAuthor":{"login":"trusted","displayName":null,"avatarUrl":null}});
    sqlx::query("UPDATE agent_message SET metadata=? WHERE id='old-message'")
        .bind(trusted.to_string())
        .execute(upgraded.write_pool())
        .await
        .unwrap();
    let error = previous_migrator()
        .run(upgraded.write_pool())
        .await
        .unwrap_err();
    assert!(
        matches!(error, sqlx::migrate::MigrateError::VersionMissing(135)),
        "{error:?}"
    );
    upgraded.close().await;
    let reopened = Store::open(&tmp.path).await.unwrap();
    let raw: String =
        sqlx::query_scalar("SELECT metadata FROM agent_message WHERE id='old-message'")
            .fetch_one(reopened.read_pool())
            .await
            .unwrap();
    assert_eq!(serde_json::from_str::<Value>(&raw).unwrap(), trusted);
}

async fn agent_workspace(store: &Store, agent: &AgentId) -> String {
    sqlx::query_scalar("SELECT workspace_id FROM agent_session WHERE id=?")
        .bind(agent.as_str())
        .fetch_one(store.read_pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn qualified_comment_update_restart_and_legacy_extras_preserve_creation() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let (ws, _agent) = seed(&store).await;
    let mut comment = sample_comment(&NoteId::from("unused"), "thread", "comment");
    comment.note_id = None;
    comment.author_principal_id = Some(PrincipalId::from("original"));
    comment.author_identity = Some(PrincipalIdentity::github(42));
    store.insert_comment(&ws, &comment).await.unwrap();
    let mut edited = comment.clone();
    edited.content = "edited".into();
    edited.author_principal_id = Some(PrincipalId::from("editor"));
    edited.author_identity = None;
    store.update_comment(&ws, &edited).await.unwrap();
    let preserved = store.get_comment(&comment.id).await.unwrap();
    assert_eq!(preserved.author_principal_id, comment.author_principal_id);
    assert_eq!(preserved.author_identity, comment.author_identity);
    assert_eq!(preserved.content, "edited");
    let mut legacy = comment.clone();
    legacy.id = "legacy".into();
    legacy.author_principal_id = None;
    legacy.author_identity = None;
    store.insert_comment_with_extras(&ws,&legacy,json!({"authorPrincipalId":"forged","authorIdentity":{"provider":"github","host":"github.com","externalUserId":"7"},"sourceAuthorPrincipalId":"forged","kept":true}).as_object().unwrap()).await.unwrap();
    legacy.author_principal_id = Some(PrincipalId::from("editor"));
    legacy.author_identity = Some(PrincipalIdentity::github(7));
    store.update_comment(&ws, &legacy).await.unwrap();
    store.close().await;
    let reopened = Store::open(&tmp.path).await.unwrap();
    let preserved = reopened.get_comment(&comment.id).await.unwrap();
    assert_eq!(preserved.author_principal_id, comment.author_principal_id);
    assert_eq!(preserved.author_identity, comment.author_identity);
    let unknown = reopened.get_comment(&legacy.id).await.unwrap();
    assert!(unknown.author_principal_id.is_none());
    assert!(unknown.author_identity.is_none());
    let raw: String = sqlx::query_scalar("SELECT extra_json FROM comment WHERE id='legacy'")
        .fetch_one(reopened.read_pool())
        .await
        .unwrap();
    let extra: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(extra["kept"], true);
    assert!(extra.get("sourceAuthorPrincipalId").is_none());
}

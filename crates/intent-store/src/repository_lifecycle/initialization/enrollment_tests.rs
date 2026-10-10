//! Real `SQLite` rollback and replacement boundaries for installation-private ownership.
use super::*;

#[tokio::test]
async fn private_native_enrollment_sql_replacement_and_failed_insert() {
    let f = Fixture::new().await;
    f.initialize(first("native-original")).await.unwrap();
    let key = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO review_context_enrollment(agent_id,workspace_id,acp_session_id,owner_key,version) VALUES (?,?,?,?,1)")
        .bind(&f.agent.id.0).bind(&f.workspace.id.0).bind("native-original").bind(&key)
        .execute(f.store.write_pool()).await.unwrap();
    // A failing duplicate agent insert must roll back its BEFORE trigger.
    assert!(f.store.insert_agent_session(&f.agent).await.is_err());
    let saved: String =
        sqlx::query_scalar("SELECT owner_key FROM review_context_enrollment WHERE agent_id=?")
            .bind(&f.agent.id.0)
            .fetch_one(f.store.read_pool())
            .await
            .unwrap();
    assert_eq!(saved, key);
    sqlx::query("UPDATE agent_session SET name='Renamed',updated_at='later' WHERE id=?")
        .bind(&f.agent.id.0)
        .execute(f.store.write_pool())
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM review_context_enrollment")
        .fetch_one(f.store.read_pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
    sqlx::query("UPDATE agent_session SET acp_session_id='replacement' WHERE id=?")
        .bind(&f.agent.id.0)
        .execute(f.store.write_pool())
        .await
        .unwrap();
    sqlx::query("UPDATE agent_session SET acp_session_id='native-original' WHERE id=?")
        .bind(&f.agent.id.0)
        .execute(f.store.write_pool())
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM review_context_enrollment")
        .fetch_one(f.store.read_pool())
        .await
        .unwrap();
    assert_eq!(count, 0, "public identity ABA must not restore enrollment");
    assert!(f
        .store
        .get_review_context_enrollment(&f.workspace.id, &f.agent.id, "native-original")
        .await
        .is_err());
    assert_eq!(family_count(&f.store, "native-original").await, 1);
}

use crate::{ReviewContextEnrollmentIntent as Enrollment, ReviewContextOwnerKey};

async fn enroll(f: &Fixture, session: &str) -> String {
    let candidate = ReviewContextOwnerKey::generate();
    let key = candidate.as_str().to_owned();
    let binding = f.binding(first(session));
    let result = f
        .store
        .initialize_repository_review_session_outcome(
            f.claim(&binding),
            binding,
            Enrollment::Created(candidate),
        )
        .await;
    assert!(matches!(
        result.persistence,
        RepositoryInitializationPersistence::Committed { .. }
    ));
    assert!(f.consume(result.confirmation.unwrap()));
    key
}

#[tokio::test]
async fn private_native_enrollment_atomic_winner_and_cold_selection() {
    let f = Fixture::new().await;
    let stale_binding = f.binding(first("other"));
    let stale = f.claim(&stale_binding);
    let key = enroll(&f, "original").await;
    let loser = f
        .store
        .initialize_repository_review_session_outcome(
            stale,
            stale_binding,
            Enrollment::Created(ReviewContextOwnerKey::generate()),
        )
        .await;
    assert!(loser.confirmation.is_err());
    assert_eq!(family_count(&f.store, "other").await, 0);
    assert_eq!(family_count(&f.store, "original").await, 1);
    assert!(matches!(
        loser.persistence,
        RepositoryInitializationPersistence::NoEffect { .. }
    ));
    let original = f
        .store
        .get_review_context_enrollment(&f.workspace.id, &f.agent.id, "original")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.owner_key(), key);
    let loaded_binding = f.binding(loaded("original"));
    let loaded_result = f
        .store
        .initialize_repository_review_session_outcome(
            f.claim(&loaded_binding),
            loaded_binding,
            Enrollment::Loaded(original),
        )
        .await;
    assert!(matches!(
        loaded_result.persistence,
        RepositoryInitializationPersistence::NoEffect { .. }
    ));
    assert!(f.consume(loaded_result.confirmation.unwrap()));
    f.store.close().await;
    let reopened = Store::open(&f.dir.path().join("initialization.db"))
        .await
        .unwrap();
    assert_eq!(
        reopened
            .get_review_context_enrollment(&f.workspace.id, &f.agent.id, "original")
            .await
            .unwrap()
            .unwrap()
            .owner_key(),
        key
    );
    assert!(reopened
        .get_review_context_enrollment(&f.workspace.id, &f.agent.id, "other")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn private_native_enrollment_rollback_and_post_commit_confirmation_are_distinct() {
    let f = Fixture::new().await;
    sqlx::query("CREATE TRIGGER reject_enrollment BEFORE INSERT ON review_context_enrollment BEGIN SELECT RAISE(ABORT,'fixture rollback'); END")
        .execute(f.store.write_pool()).await.unwrap();
    let binding = f.binding(first("rejected"));
    let outcome = f
        .store
        .initialize_repository_review_session_outcome(
            f.claim(&binding),
            binding,
            Enrollment::Created(ReviewContextOwnerKey::generate()),
        )
        .await;
    assert!(outcome.confirmation.is_err());
    assert!(matches!(
        outcome.persistence,
        RepositoryInitializationPersistence::Unknown
    ));
    assert_eq!(f.id().await, None);
    assert_eq!(family_count(&f.store, "rejected").await, 0);
    assert!(f
        .store
        .get_review_context_enrollment(&f.workspace.id, &f.agent.id, "rejected")
        .await
        .unwrap()
        .is_none());
    // A separate original allocation, whose SQL commits but completion retires.
    let f = Fixture::new().await;
    f.owner.reject_completion.store(true, Ordering::SeqCst);
    let candidate = ReviewContextOwnerKey::generate();
    let key = candidate.as_str().to_owned();
    let binding = f.binding(first("committed"));
    let outcome = f
        .store
        .initialize_repository_review_session_outcome(
            f.claim(&binding),
            binding,
            Enrollment::Created(candidate),
        )
        .await;
    assert!(outcome.confirmation.is_err());
    assert!(matches!(
        outcome.persistence,
        RepositoryInitializationPersistence::Committed { .. }
    ));
    assert_eq!(
        f.store
            .get_review_context_enrollment(&f.workspace.id, &f.agent.id, "committed")
            .await
            .unwrap()
            .unwrap()
            .owner_key(),
        key
    );
}

#[tokio::test]
async fn private_native_enrollment_import_refuses_injection_and_invalidates_identical_rows() {
    let f = Fixture::new().await;
    let key = enroll(&f, "original").await;
    let rows = f.store.transfer_export_rows(&f.workspace.id).await.unwrap();
    assert!(!rows.iter().any(|(table, _)| matches!(
        table.as_str(),
        "review_context_enrollment" | "review_context_private_family"
    )));
    assert!(f
        .store
        .transfer_import_rows(&[(
            "review_context_private_family".into(),
            vec![serde_json::json!({"acp_session_id":"injected","version":1})]
        )])
        .await
        .is_err());
    assert_eq!(family_count(&f.store, "injected").await, 0);
    assert!(!serde_json::to_string(&rows).unwrap().contains(&key));
    assert!(f
        .store
        .transfer_import_rows(&[("review_context_enrollment".into(), vec![])])
        .await
        .is_err());
    // Duplicate import must roll back its attempted local invalidation.
    let agent_rows = rows
        .iter()
        .find(|(table, _)| table == "agent_session")
        .unwrap()
        .clone();
    let bad = vec![
        agent_rows.clone(),
        (
            "agent_message".into(),
            vec![serde_json::json!({"invalid_column":true})],
        ),
    ];
    assert!(f.store.transfer_import_rows(&bad).await.is_err());
    assert_eq!(
        f.store
            .get_review_context_enrollment(&f.workspace.id, &f.agent.id, "original")
            .await
            .unwrap()
            .unwrap()
            .owner_key(),
        key
    );
    // The import API deliberately refuses duplicate IDs. Exercise a genuine
    // identical-byte SQLite replacement separately, then the supported import
    // after deletion; neither operation may carry the old private enrollment.
    sqlx::query("INSERT OR REPLACE INTO agent_session SELECT * FROM agent_session WHERE id=?")
        .bind(&f.agent.id.0)
        .execute(f.store.write_pool())
        .await
        .unwrap();
    assert!(f
        .store
        .get_review_context_enrollment(&f.workspace.id, &f.agent.id, "original")
        .await
        .is_err());
    f.store
        .delete_agent_session(&f.workspace.id, &f.agent.id)
        .await
        .unwrap();
    f.store.transfer_import_rows(&[agent_rows]).await.unwrap();
    assert!(f
        .store
        .get_review_context_enrollment(&f.workspace.id, &f.agent.id, "original")
        .await
        .is_err());
}

#[tokio::test]
async fn private_native_enrollment_rewind_retires_original_and_cannot_backfill_load() {
    let f = Fixture::new().await;
    enroll(&f, "original").await;
    let original = f
        .store
        .get_review_context_enrollment(&f.workspace.id, &f.agent.id, "original")
        .await
        .unwrap()
        .unwrap();
    let leaf = f.owner.leaf();
    f.store
        .replace_agent_messages(&f.agent.id, &[])
        .await
        .unwrap();
    assert!(!leaf.load(Ordering::SeqCst));
    let binding = f.binding(loaded("original"));
    let result = f
        .store
        .initialize_repository_review_session_outcome(
            f.claim(&binding),
            binding,
            Enrollment::Loaded(original),
        )
        .await;
    assert!(result.confirmation.is_err());
    assert!(f
        .store
        .get_review_context_enrollment(&f.workspace.id, &f.agent.id, "original")
        .await
        .is_err());
}

#[tokio::test]
async fn private_native_enrollment_valid_values_immutable_and_identity_lifetime() {
    for column in ["provider", "backend_session_id", "parent_agent_id"] {
        let f = Fixture::new().await;
        enroll(&f, "original").await;
        assert!(
            sqlx::query("UPDATE review_context_enrollment SET owner_key=owner_key")
                .execute(f.store.write_pool())
                .await
                .is_err()
        );
        if column == "parent_agent_id" {
            let mut parent = f.agent.clone();
            parent.id = AgentId("parent".into());
            f.store.insert_agent_session(&parent).await.unwrap();
        }
        let value = if column == "parent_agent_id" {
            "parent"
        } else {
            "changed"
        };
        sqlx::query(&format!("UPDATE agent_session SET {column}=? WHERE id=?"))
            .bind(value)
            .bind(&f.agent.id.0)
            .execute(f.store.write_pool())
            .await
            .unwrap();
        assert!(f
            .store
            .get_review_context_enrollment(&f.workspace.id, &f.agent.id, "original")
            .await
            .is_err());
    }
    let f = Fixture::new().await;
    f.initialize(first("original")).await.unwrap();
    for key in [
        "",
        "public-agent-id",
        "00000000-0000-0000-0000-000000000000",
    ] {
        assert!(sqlx::query("INSERT INTO review_context_enrollment(agent_id,workspace_id,acp_session_id,owner_key,version) VALUES (?,?,?,?,1)")
            .bind(&f.agent.id.0).bind(&f.workspace.id.0).bind("original").bind(key)
            .execute(f.store.write_pool()).await.is_err());
    }
}

async fn family_count(store: &Store, session: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM review_context_private_family WHERE acp_session_id=?")
        .bind(session)
        .fetch_one(store.read_pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn private_native_family_denial_survives_association_loss_and_restart() {
    let f = Fixture::new().await;
    enroll(&f, "original").await;
    sqlx::query("DELETE FROM review_context_enrollment WHERE agent_id=?")
        .bind(&f.agent.id.0)
        .execute(f.store.write_pool())
        .await
        .unwrap();
    assert!(f
        .store
        .get_review_context_enrollment(&f.workspace.id, &f.agent.id, "original")
        .await
        .is_err());
    assert_eq!(family_count(&f.store, "original").await, 1);
    for sql in [
        "UPDATE review_context_private_family SET version=version",
        "DELETE FROM review_context_private_family",
    ] {
        assert!(sqlx::query(sql)
            .execute(f.store.write_pool())
            .await
            .is_err());
    }
    f.store.close().await;
    let reopened = Store::open(&f.dir.path().join("initialization.db"))
        .await
        .unwrap();
    assert!(reopened
        .get_review_context_enrollment(&f.workspace.id, &f.agent.id, "original")
        .await
        .is_err());
    assert_eq!(family_count(&reopened, "original").await, 1);
    assert!(reopened
        .get_review_context_enrollment(&f.workspace.id, &f.agent.id, "ordinary-unmarked")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn private_native_family_denial_rolls_back_trigger_writes() {
    let f = Fixture::new().await;
    f.initialize(first("rolled-back")).await.unwrap();
    let mut conn = f.store.write_pool().acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO review_context_enrollment(agent_id,workspace_id,acp_session_id,owner_key,version) VALUES (?,?,?,?,1)")
        .bind(&f.agent.id.0).bind(&f.workspace.id.0).bind("rolled-back").bind(uuid::Uuid::new_v4().to_string())
        .execute(&mut *conn).await.unwrap();
    let inside: i64 = sqlx::query_scalar("SELECT count(*) FROM review_context_private_family")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(inside, 1);
    sqlx::query("ROLLBACK").execute(&mut *conn).await.unwrap();
    drop(conn);
    assert_eq!(family_count(&f.store, "rolled-back").await, 0);
    assert!(f
        .store
        .get_review_context_enrollment(&f.workspace.id, &f.agent.id, "rolled-back")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn private_native_family_denial_unreadable_marker_refuses_fallback() {
    let f = Fixture::new().await;
    enroll(&f, "original").await;
    sqlx::query("DELETE FROM review_context_enrollment")
        .execute(f.store.write_pool())
        .await
        .unwrap();
    // Simulate unavailable backing evidence in a disposable database only.
    sqlx::query("DROP TABLE review_context_private_family")
        .execute(f.store.write_pool())
        .await
        .unwrap();
    assert!(f
        .store
        .get_review_context_enrollment(&f.workspace.id, &f.agent.id, "original")
        .await
        .is_err());
    assert!(f
        .store
        .get_review_context_enrollment(&f.workspace.id, &f.agent.id, "ordinary-unmarked")
        .await
        .is_err());
}

#[tokio::test]
async fn private_native_family_migration_seeds_only_existing_private_enrollment() {
    use sqlx::Connection as _;
    let mut conn = sqlx::SqliteConnection::connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE agent_session(acp_session_id TEXT); INSERT INTO agent_session VALUES('ordinary-public'); CREATE TABLE review_context_enrollment(acp_session_id TEXT); INSERT INTO review_context_enrollment VALUES('original-private');")
        .execute(&mut conn).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../../migrations/0151_review_context_private_families.sql"
    ))
    .execute(&mut conn)
    .await
    .unwrap();
    let rows: Vec<String> =
        sqlx::query_scalar("SELECT acp_session_id FROM review_context_private_family")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(rows, vec!["original-private"]);
    sqlx::query("DELETE FROM review_context_enrollment")
        .execute(&mut conn)
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM review_context_private_family")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn private_native_family_real_sqlx_upgrade_preserves_original_enrollment() {
    use sqlx::{migrate::Migrator, sqlite::SqliteConnectOptions, SqlitePool};
    use std::borrow::Cow;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pre-denial.db");
    let pool = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true),
    )
    .await
    .unwrap();
    Migrator {
        migrations: Cow::Owned(
            crate::MIGRATOR
                .iter()
                .filter(|migration| migration.version <= 150)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    }
    .run(&pool)
    .await
    .unwrap();
    let before: Vec<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT version,checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(before.last().unwrap().0, 150);
    sqlx::query("INSERT INTO workspace(id,title,branch,status,created_at,updated_at) VALUES('upgrade-workspace','Upgrade','main','Active','2026-01-01','2026-01-01')")
        .execute(&pool).await.unwrap();
    for (agent, session) in [
        ("private-agent", "original-private"),
        ("ordinary-agent", "ordinary-public"),
    ] {
        sqlx::query("INSERT INTO agent_session(id,workspace_id,name,status,provider,acp_session_id,created_at,updated_at) VALUES(?,'upgrade-workspace','Preserved','idle','claude-code',?,'2026-01-01','2026-01-01')")
            .bind(agent).bind(session).execute(&pool).await.unwrap();
    }
    let key = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO review_context_enrollment(agent_id,workspace_id,acp_session_id,owner_key,version) VALUES('private-agent','upgrade-workspace','original-private',?,1)")
        .bind(&key).execute(&pool).await.unwrap();
    pool.close().await;

    let store = Store::open(&path).await.unwrap();
    let after: Vec<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT version,checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(store.read_pool())
            .await
            .unwrap();
    assert_eq!(&after[..before.len()], before.as_slice());
    assert!(after.iter().any(|(version, _)| *version == 151));
    let workspace = intent_core::WorkspaceId("upgrade-workspace".into());
    let private = intent_core::AgentId("private-agent".into());
    let ordinary = intent_core::AgentId("ordinary-agent".into());
    assert_eq!(
        store
            .get_review_context_enrollment(&workspace, &private, "original-private")
            .await
            .unwrap()
            .unwrap()
            .owner_key(),
        key
    );
    assert!(store
        .get_review_context_enrollment(&workspace, &ordinary, "ordinary-public")
        .await
        .unwrap()
        .is_none());
    let families: Vec<(String, i64)> = sqlx::query_as(
        "SELECT acp_session_id,version FROM review_context_private_family ORDER BY acp_session_id",
    )
    .fetch_all(store.read_pool())
    .await
    .unwrap();
    assert_eq!(families, vec![("original-private".into(), 1)]);
    let sessions: Vec<(String,String)> = sqlx::query_as(
        "SELECT id,acp_session_id FROM agent_session WHERE workspace_id='upgrade-workspace' ORDER BY id",
    ).fetch_all(store.read_pool()).await.unwrap();
    assert_eq!(
        sessions,
        vec![
            ("ordinary-agent".into(), "ordinary-public".into()),
            ("private-agent".into(), "original-private".into())
        ]
    );
    sqlx::query("DELETE FROM review_context_enrollment WHERE agent_id='private-agent'")
        .execute(store.write_pool())
        .await
        .unwrap();
    store.close().await;
    let reopened = Store::open(&path).await.unwrap();
    assert!(reopened
        .get_review_context_enrollment(&workspace, &private, "original-private")
        .await
        .is_err());
    assert!(reopened
        .get_review_context_enrollment(&workspace, &ordinary, "ordinary-public")
        .await
        .unwrap()
        .is_none());
}

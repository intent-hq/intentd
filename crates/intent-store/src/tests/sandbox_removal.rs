//! Retirement of per-agent `CoW` sandboxes (protocol 16): migration 0186
//! drops the `sandbox` table (0038/0040) and the `agent_session` sandbox link
//! columns (0039). No live per-agent sandbox exists to carry over; the
//! migration never touches files on disk and keeps every other session field.
use super::*;

async fn session_columns<'e>(
    pool: impl sqlx::Executor<'e, Database = sqlx::Sqlite>,
) -> Vec<String> {
    sqlx::query_scalar("SELECT name FROM pragma_table_info('agent_session')")
        .fetch_all(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn migration_drops_sandbox_table_and_session_link_columns() {
    let tmp = TempDb::new();
    let pool = crate::connect_write(&tmp.path).await.unwrap();
    let prefix = sqlx::migrate::Migrator {
        migrations: std::borrow::Cow::Owned(
            crate::MIGRATOR
                .iter()
                .filter(|m| m.version <= 185)
                .cloned()
                .collect(),
        ),
        ..sqlx::migrate::Migrator::DEFAULT
    };
    prefix.run(&pool).await.unwrap();
    // What a released daemon wrote for a delegated agent in a sandbox.
    sqlx::raw_sql(
        r"
        INSERT INTO workspace(id,title,branch,created_at,updated_at)
          VALUES ('ws','W','main','c','u');
        INSERT INTO agent_session(id,workspace_id,name,status,created_at,updated_at,
                                  sandbox_id,sandbox_path,sandbox_branch)
          VALUES ('agent','ws','Sandboxed','idle','c','u',
                  'sandbox-ws-agent','/ws/sandboxes/agent/repo','sb/agent');
        INSERT INTO sandbox(id,workspace_id,agent_id,path,branch,base_commit_sha,status,
                            created_at,updated_at)
          VALUES ('sandbox-ws-agent','ws','agent','/ws/sandboxes/agent/repo','sb/agent',
                  'abc','created','c','u');
        ",
    )
    .execute(&pool)
    .await
    .unwrap();

    crate::MIGRATOR.run(&pool).await.unwrap();

    let sandbox_tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE name = 'sandbox' OR tbl_name = 'sandbox'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        sandbox_tables.is_empty(),
        "sandbox table and indexes dropped: {sandbox_tables:?}"
    );
    let session_columns = session_columns(&pool).await;
    for gone in ["sandbox_id", "sandbox_path", "sandbox_branch"] {
        assert!(
            !session_columns.iter().any(|c| c == gone),
            "{gone} dropped: {session_columns:?}"
        );
    }
    let kept: (String, String, String) =
        sqlx::query_as("SELECT workspace_id, name, status FROM agent_session WHERE id = 'agent'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        kept,
        (
            "ws".to_string(),
            "Sandboxed".to_string(),
            "idle".to_string()
        ),
        "the session itself survives"
    );
    pool.close().await;

    // The migrated database opens and reads through the store.
    let store = Store::open(&tmp.path).await.unwrap();
    let session = store
        .get_agent_session_without_messages(&AgentId::from("agent"))
        .await
        .unwrap();
    assert_eq!(session.name, "Sandboxed");
    store.close().await;
}

use super::*;

#[tokio::test]
async fn script_initialization_backfills_active_and_archived_workspaces() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let active = WorkspaceId::new();
    let archived = WorkspaceId::new();
    let untouched = WorkspaceId::new();
    for ws in [&active, &archived, &untouched] {
        store
            .insert_workspace(&sample_workspace(ws, "initialization", false))
            .await
            .unwrap();
    }
    // Recreate the previous schema, then let Store::open run the actual upgrade.
    for sql in [
        "DROP TRIGGER workspace_scripts_initialized_insert",
        "ALTER TABLE workspace DROP COLUMN scripts_initialized",
        "DELETE FROM _sqlx_migrations WHERE version = 148",
    ] {
        sqlx::query(sql).execute(store.write_pool()).await.unwrap();
    }
    for (ws, archived_at) in [(&active, None), (&archived, Some(now_iso()))] {
        sqlx::query("INSERT INTO script (id, workspace_id, name, command, mode, source, created_at, archived_at) VALUES (?, ?, 'check', 'true', 'command', 'user', ?, ?)")
            .bind(ws.as_str())
            .bind(ws.as_str())
            .bind(now_iso())
            .bind(archived_at)
            .execute(store.write_pool())
            .await
            .unwrap();
    }
    store.write_pool().close().await;
    store.read_pool().close().await;
    let store = Store::open(&tmp.path).await.unwrap();
    for ws in [&active, &archived] {
        assert!(store.workspace_scripts_initialized(ws).await.unwrap());
        store.remove_script(ws.as_str()).await.unwrap();
        assert!(store.workspace_scripts_initialized(ws).await.unwrap());
    }
    assert!(!store
        .workspace_scripts_initialized(&untouched)
        .await
        .unwrap());
    assert!(store.list_all_scripts().await.unwrap().is_empty());
    store.write_pool().close().await;
    store.read_pool().close().await;
    let store = Store::open(&tmp.path).await.unwrap();
    assert!(store.workspace_scripts_initialized(&active).await.unwrap());
    assert!(store
        .workspace_scripts_initialized(&archived)
        .await
        .unwrap());
    assert!(!store
        .workspace_scripts_initialized(&untouched)
        .await
        .unwrap());
}

#[tokio::test]
async fn script_initialization_is_atomic_and_workspace_scoped() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    let other = WorkspaceId::new();
    for id in [&ws, &other] {
        store
            .insert_workspace(&sample_workspace(id, "initialization", false))
            .await
            .unwrap();
    }
    let mut tx = store.write_pool().begin().await.unwrap();
    sqlx::query("INSERT INTO script (id, workspace_id, name, command, mode, source, created_at) VALUES ('rollback', ?, 'check', 'true', 'command', 'user', ?)")
        .bind(ws.as_str())
        .bind(now_iso())
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(!store.workspace_scripts_initialized(&ws).await.unwrap());

    let mut script = intent_core::Script {
        id: "saved".into(),
        workspace_id: ws.to_string(),
        name: "check".into(),
        command: "true".into(),
        cwd: None,
        env: None,
        mode: intent_core::ScriptMode::Command,
        category: None,
        source: "user".into(),
        auto_start: None,
        created_at: now_iso(),
        updated_at: None,
        purpose: intent_core::ScriptPurpose::Saved,
        archived_at: None,
        last_run: None,
    };
    store.upsert_script_in_workspace(&script).await.unwrap();
    assert!(store.workspace_scripts_initialized(&ws).await.unwrap());
    script.workspace_id = other.to_string();
    assert!(store.upsert_script_in_workspace(&script).await.is_err());
    assert!(store
        .remove_script_in_workspace(&other, &script.id)
        .await
        .is_err());
    assert!(!store.workspace_scripts_initialized(&other).await.unwrap());
    store
        .remove_script_in_workspace(&ws, &script.id)
        .await
        .unwrap();
    assert!(store.workspace_scripts_initialized(&ws).await.unwrap());

    store.upsert_scripts(&[script]).await.unwrap();
    assert!(store.workspace_scripts_initialized(&other).await.unwrap());
    store.remove_script("saved").await.unwrap();
    assert!(store.workspace_scripts_initialized(&other).await.unwrap());
}

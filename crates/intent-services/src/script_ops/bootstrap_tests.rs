#[intent_test_macros::daemon_test]
async fn purged_scripts_are_not_reseeded_by_an_inflight_first_list() {
    for remove_custom in [true, false] {
        let h = harness().await;
        let repo = WorktreeDir::new();
        std::fs::create_dir_all(repo.0.join(".intent")).unwrap();
        std::fs::write(
            repo.0.join(".intent/config.json"),
            r#"{"scripts":[{"name":"default","command":"true","mode":"command"}]}"#,
        )
        .unwrap();
        sqlx::query("UPDATE workspace SET repository_path = ? WHERE id = ?")
            .bind(repo.0.to_str().unwrap())
            .bind(h.ws.as_str())
            .execute(h.services.store.write_pool())
            .await
            .unwrap();
        let park = Arc::new(SupervisePark::default());
        let mut mgr = h.services.script_manager();
        mgr.parks.bootstrap_persist = Some(park.clone());
        let listing = {
            let ws = h.ws.clone();
            intent_core::spawn_daemon(async move { mgr.list(&ws).await })
        };
        tokio::time::timeout(LIVENESS, park.entered.notified())
            .await
            .expect("first list reached persistence after observing no scripts");
        let id = create_simple(&h, "custom", "true", ScriptMode::Command).await;
        if remove_custom {
            h.services
                .script_remove(h.ws.clone(), id.clone())
                .await
                .unwrap();
        }
        park.release.notify_one();
        let listed = tokio::time::timeout(LIVENESS, listing)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if remove_custom {
            assert_eq!(
                listed,
                json!({"scripts": []}),
                "stale bootstrap undid a purge"
            );
        } else {
            assert_eq!(listed["scripts"].as_array().unwrap().len(), 1);
            assert_eq!(listed["scripts"][0]["id"], id);
        }
        assert_eq!(
            h.services.store.list_all_scripts().await.unwrap().len(),
            usize::from(!remove_custom)
        );
        assert_eq!(h.services.script_list(h.ws.clone()).await.unwrap(), listed);
    }
}

#[intent_test_macros::daemon_test]
async fn purged_repository_scripts_stay_empty_across_lists_and_restart() {
    use intent_core::ScriptArchiveFilter;

    let h = harness().await;
    let repo = WorktreeDir::new();
    std::fs::create_dir_all(repo.0.join(".intent")).unwrap();
    let config = r#"{"scripts":[{"name":"check","command":"true","mode":"command"}]}"#;
    std::fs::write(repo.0.join(".intent/config.json"), config).unwrap();
    let other_ws = WorkspaceId::new();
    h.services
        .store
        .insert_workspace(&workspace(&other_ws, None))
        .await
        .unwrap();
    for ws in [&h.ws, &other_ws] {
        sqlx::query("UPDATE workspace SET repository_path = ? WHERE id = ?")
            .bind(repo.0.to_str().unwrap())
            .bind(ws.as_str())
            .execute(h.services.store.write_pool())
            .await
            .unwrap();
    }

    let seeded = h.services.script_list(h.ws.clone()).await.unwrap();
    assert_eq!(seeded["scripts"].as_array().unwrap().len(), 1);
    let id = seeded["scripts"][0]["id"].as_str().unwrap().to_owned();
    h.services
        .script_archive(h.ws.clone(), vec![id.clone()])
        .await
        .unwrap();
    h.services.script_remove(h.ws.clone(), id).await.unwrap();
    assert!(h
        .services
        .store
        .list_all_scripts()
        .await
        .unwrap()
        .is_empty());

    // The same calls made by a reconnecting client must not recreate defaults.
    for _ in 0..2 {
        for archive in [
            ScriptArchiveFilter::All,
            ScriptArchiveFilter::Active,
            ScriptArchiveFilter::Archived,
        ] {
            assert_eq!(
                h.services
                    .script_list_filtered(h.ws.clone(), archive)
                    .await
                    .unwrap(),
                json!({"scripts": []}),
                "deleting the last definition must suppress repository reseeding"
            );
        }
    }

    let restarted = Services::new(Store::open(&h.tmp.path).await.unwrap());
    restarted.script_manager().hydrate().await.unwrap();
    for archive in [
        ScriptArchiveFilter::Archived,
        ScriptArchiveFilter::Active,
        ScriptArchiveFilter::All,
    ] {
        assert_eq!(
            restarted
                .script_list_filtered(h.ws.clone(), archive)
                .await
                .unwrap(),
            json!({"scripts": []})
        );
    }
    let other = restarted.script_list(other_ws.clone()).await.unwrap();
    assert_eq!(other["scripts"].as_array().unwrap().len(), 1);
    assert_eq!(other["scripts"][0]["name"], "check");
    assert_eq!(
        restarted.script_list(other_ws).await.unwrap(),
        other,
        "first-use defaults seed exactly once in another workspace"
    );
    assert_eq!(
        std::fs::read_to_string(repo.0.join(".intent/config.json")).unwrap(),
        config
    );
}

#[intent_test_macros::daemon_test]
async fn purged_custom_scripts_stay_empty_before_first_repository_list() {
    let h = harness().await;
    let repo = WorktreeDir::new();
    std::fs::create_dir_all(repo.0.join(".intent")).unwrap();
    std::fs::write(
        repo.0.join(".intent/config.json"),
        r#"{"scripts":[{"name":"default","command":"true","mode":"command"}]}"#,
    )
    .unwrap();
    sqlx::query("UPDATE workspace SET repository_path = ? WHERE id = ?")
        .bind(repo.0.to_str().unwrap())
        .bind(h.ws.as_str())
        .execute(h.services.store.write_pool())
        .await
        .unwrap();
    let id = create_simple(&h, "custom", "true", ScriptMode::Command).await;
    h.services
        .script_manager()
        .remove_in_workspace(&h.ws, &id)
        .await
        .unwrap();
    let restarted = Services::new(Store::open(&h.tmp.path).await.unwrap());
    restarted.script_manager().hydrate().await.unwrap();
    assert_eq!(
        restarted.script_list(h.ws.clone()).await.unwrap(),
        json!({"scripts": []}),
        "scoped removal must preserve intentional emptiness even without a prior list"
    );
}

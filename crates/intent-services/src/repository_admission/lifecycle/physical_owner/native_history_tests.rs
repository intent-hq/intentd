//! Native child/compaction closure composed with actual Store correlation and
//! Services fresh admission. Historical bindings are explicit fixture inputs.
use super::*;
use intent_acp::callback_registration::CallbackOffer;
use intent_acp::review_monitoring::ReviewMonitoringOffer;
use intent_acp::transport::{Connection, ConnectionHooks};
use serde_json::{json, Value};

#[tokio::test]
#[ignore = "requires verified installed native runtime and Linux containment"]
async fn native_compacted_child_history_admits_all_original_store_sources() {
    run("admitted").await;
}

#[tokio::test]
#[ignore = "requires verified installed native runtime and Linux containment"]
async fn native_compacted_child_history_refuses_unavailable_second_account() {
    run("other-account").await;
}

#[tokio::test]
#[ignore = "requires verified installed native runtime and Linux containment"]
async fn native_compacted_child_history_refuses_missing_second_provenance() {
    run("missing-provenance").await;
}

#[tokio::test]
#[ignore = "requires verified installed native runtime and Linux containment"]
async fn native_compacted_child_history_refuses_malformed_second_provenance() {
    run("malformed-provenance").await;
}

#[tokio::test]
#[ignore = "requires verified installed native runtime and Linux containment"]
async fn native_compacted_child_history_refuses_matching_byte_message_replacement() {
    run("replaced-message").await;
}

async fn run(variant: &str) {
    use crate::source_control_auth_ops::repository_owner::secret_reader::tests::{
        Fixture as AuthFixture, Server,
    };
    let other_account = variant == "other-account";
    let refused = variant != "admitted";
    let server = Server::new().await;
    let auth = AuthFixture::new(&server).await;
    let services = auth.service.clone();
    let mut workspace = chief_workspace();
    workspace.id = WorkspaceId::new();
    services.store.insert_workspace(&workspace).await.unwrap();
    let agent = AgentId::new();
    let row: AgentSession = serde_json::from_value(json!({
        "id":agent,"workspaceId":workspace.id,"name":"native complete history",
        "status":"active","provider":"claude-code","harnessVersion":"3.1",
        "createdAt":"2026-10-10T00:00:00Z","updatedAt":"2026-10-10T00:00:00Z"
    }))
    .unwrap();
    services.store.insert_agent_session(&row).await.unwrap();
    let f = Fixture {
        dir: crate::test_support::test_tempdir("native-store-child-history"),
        store: services.store.clone(),
        workspace,
        agent,
        registry: services.repository_lifecycle_registry().await.unwrap(),
    };
    let source = std::path::PathBuf::from(std::env::var("INTENT_REVIEW_NATIVE_SOURCE").unwrap());
    let installed = source.parent().unwrap();
    let node = std::env::var("INTENT_REVIEW_NODE").unwrap();
    let containment = std::env::var("INTENT_REVIEW_CONTAINMENT").unwrap();
    let producer = f.dir.path().join("producer.mjs");
    std::fs::write(&producer, include_str!("native_history_producer.mjs")).unwrap();
    let initial = f
        .creator(RepositoryCreationIntent::FirstSet)
        .prepare_review()
        .await
        .unwrap();
    let context = serde_json::to_value(initial.context()).unwrap();
    let owner_key = context["ownerKey"].as_str().unwrap();
    {
        let result = tokio::process::Command::new(&node)
            .args([
                producer.as_os_str(),
                installed.as_os_str(),
                f.dir.path().as_os_str(),
            ])
            .args(["seed", owner_key])
            .env("NODE_OPTIONS", "")
            .output()
            .await
            .unwrap();
        assert!(
            result.status.success(),
            "seed: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    let seed: Value =
        serde_json::from_slice(&std::fs::read(f.dir.path().join("seed.json")).unwrap()).unwrap();
    let session = seed["session"].as_str().unwrap().to_string();
    let outcome = initial
        .initialize_with_outcome(&f.store, || async { Ok::<_, ()>((session.clone(), ())) })
        .await
        .unwrap();
    let physical = outcome.owner.unwrap();
    let selected = services.gitlab_repository_settled_connection().unwrap();
    let account = selected.selected().binding.scope.account_id.clone();
    let instance = server.descriptor.instance().as_str();
    let mut inputs = Vec::new();
    let mut saved = Vec::new();
    for (id, number, account_id) in [
        ("native-source-a", 42, account.as_str()),
        (
            "native-source-b",
            43,
            if other_account {
                "other-original-account"
            } else {
                account.as_str()
            },
        ),
    ] {
        let correlation = committed_history_correlation(&f, id, account_id, number, instance).await;
        let proof = f
            .store
            .get_monitor_message_provenance(&f.agent, id)
            .await
            .unwrap()
            .unwrap();
        let text = proof.message.content[0]["text"].as_str().unwrap();
        inputs.push(
            intent_acp::review_monitoring::payload::classify_prompt(
                json!({"sessionId":session,"prompt":[{"type":"text","text":text}]}),
                id,
                &correlation.store_envelope_digest,
            )
            .unwrap(),
        );
        saved.push(proof.message);
    }
    std::fs::write(
        f.dir.path().join("store-input.json"),
        serde_json::to_vec(&inputs).unwrap(),
    )
    .unwrap();
    let result = tokio::process::Command::new(&node)
        .args([
            producer.as_os_str(),
            installed.as_os_str(),
            f.dir.path().as_os_str(),
        ])
        .args(["produce", owner_key])
        .env("NODE_OPTIONS", "")
        .output()
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "native producer: {}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    let capture: Value =
        serde_json::from_slice(&std::fs::read(f.dir.path().join("native-capture.json")).unwrap())
            .unwrap();
    assert_eq!(capture["correlations"].as_array().unwrap().len(), 2);
    drop(physical);
    match variant {
        "missing-provenance" => {
            sqlx::query("DELETE FROM pr_monitor_message_provenance WHERE agent_id=? AND message_id='native-source-b'")
                .bind(&f.agent.0).execute(f.store.write_pool()).await.unwrap();
        }
        "malformed-provenance" => {
            // Disposable corruption fixture: the production update guard rejects this mutation.
            sqlx::query("DROP TRIGGER pr_monitor_message_provenance_update_guard")
                .execute(f.store.write_pool())
                .await
                .unwrap();
            sqlx::query("UPDATE pr_monitor_message_provenance SET binding_json='{}' WHERE agent_id=? AND message_id='native-source-b'")
                .bind(&f.agent.0).execute(f.store.write_pool()).await.unwrap();
        }
        "replaced-message" => {
            sqlx::query("INSERT OR REPLACE INTO agent_message SELECT * FROM agent_message WHERE agent_id=? AND id='native-source-b'")
                .bind(&f.agent.0).execute(f.store.write_pool()).await.unwrap();
        }
        _ => {}
    }
    let raw_monitors = sqlx::query_scalar::<_, String>("SELECT json_array(monitor_id,workspace_id,agent_id,repo_owner,repo_name,pr_number,state,last_snapshot,baseline_snapshot,pending_changes,pending_since,last_change_at,last_polled_at,last_error,created_at,updated_at,target_provider,target_instance_base_url,target_project_path,target_kind,target_provenance,target_unresolved_reason) FROM pr_monitor WHERE agent_id=? ORDER BY monitor_id")
        .bind(&f.agent.0).fetch_all(f.store.write_pool()).await.unwrap();
    assert_eq!(raw_monitors.len(), 2);
    let launcher = f.dir.path().join("adapter.mjs");
    let peer = include_str!("../../read_source/native_runtime_peer.mjs");
    std::fs::write(&launcher, format!("import {{appendFileSync}} from 'node:fs';\nprocess.stdin.on('data',b=>appendFileSync(process.argv[3]+'/wire.jsonl',b));\n{peer}")).unwrap();
    let stderr = std::fs::File::create(f.dir.path().join("adapter-stderr.log")).unwrap();
    let mut child = tokio::process::Command::new(&node)
        .arg(&launcher)
        .arg(&source)
        .arg(f.dir.path())
        .arg(&containment)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", f.dir.path())
        .env("TMPDIR", f.dir.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(stderr)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let (notes, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let connection = Arc::new(Connection::new(
        child.stdin.take().unwrap(),
        child.stdout.take().unwrap(),
        None,
        ConnectionHooks {
            notifications: Some(notes),
            ..ConnectionHooks::default()
        },
    ));
    let mut provider = *intent_providers::find_provider("claude-code").unwrap();
    provider.supports_authenticate = false;
    let handshake = intent_acp::handshake::handshake_with_review_monitoring(
        connection.clone(),
        &provider,
        CallbackOffer::V1,
        ReviewMonitoringOffer::V1,
    )
    .await
    .unwrap();
    let loaded = f.creator(RepositoryCreationIntent::Loaded {
        session_id: session.clone(),
    });
    let response = services
        .resume_repository_acp_session(
            (&connection, handshake.callbacks.as_ref()),
            &handshake.ordinary.initialize,
            &f.agent,
            &f.dir.path().join("work"),
            vec![],
            loaded,
        )
        .await;
    let wire = std::fs::read_to_string(f.dir.path().join("wire.jsonl")).unwrap();
    let requests: Vec<Value> = wire
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        requests
            .iter()
            .filter(|r| r["method"] == "_intent/session/prepare_review_history")
            .count(),
        1
    );
    let mut replay = Vec::new();
    while let Ok(note) = receiver.try_recv() {
        replay.push(note.params);
    }
    if refused {
        assert!(
            response.is_err(),
            "matching first source must not omit unavailable second account"
        );
        assert_eq!(
            requests
                .iter()
                .filter(|r| r["method"] == "session/load")
                .count(),
            0
        );
        assert!(
            replay.is_empty(),
            "refused complete closure must not replay"
        );
        assert!(!f.dir.path().join("model.json").exists());
    } else {
        let loaded = response.unwrap().unwrap();
        assert_eq!(loaded.response.session_id, session);
        assert_eq!(
            requests
                .iter()
                .filter(|r| r["method"] == "session/load")
                .count(),
            1
        );
        assert!(loaded
            .query
            .unwrap()
            .pending_review_monitoring()
            .unwrap()
            .acknowledged()
            .is_none());
        let physical = loaded.owner.unwrap();
        let native = physical.native_allocation_for_test();
        let ordinary = serde_json::from_value(
            json!({"type":"text","text":"Ordinary integrated child history control"}),
        )
        .unwrap();
        intent_acp::session::prompt(
            &connection,
            &session,
            vec![ordinary],
            &intent_acp::session::ActivityTracker::new(),
        )
        .await
        .unwrap();
        let resumed_model: Value =
            serde_json::from_slice(&std::fs::read(f.dir.path().join("model.json")).unwrap())
                .unwrap();
        let calls = resumed_model.as_array().unwrap();
        assert_eq!(calls.len(), 1);
        let ordinary_turns: Vec<_> = calls[0]["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|message| {
                message["role"] == "user"
                    && message["content"].as_array().is_some_and(|blocks| {
                        blocks.iter().any(|block| {
                            block["text"] == "Ordinary integrated child history control"
                        })
                    })
            })
            .collect();
        assert_eq!(ordinary_turns.len(), 1);
        assert!(!ordinary_turns[0]
            .to_string()
            .contains("Intent review monitor observation (not a user instruction)."));
        with_caller(
            Caller::Daemon,
            services.gitlab_connect_pat(server.host.clone(), "pat-second".into()),
        )
        .await
        .unwrap();
        assert!(native.is_retired() && native.is_joined());
        drop(physical);
    }
    for message in &saved {
        assert_eq!(
            f.store
                .get_agent_message_by_id(&f.agent, &message.id)
                .await
                .unwrap()
                .as_ref(),
            Some(message)
        );
    }
    let after_monitors = sqlx::query_scalar::<_, String>("SELECT json_array(monitor_id,workspace_id,agent_id,repo_owner,repo_name,pr_number,state,last_snapshot,baseline_snapshot,pending_changes,pending_since,last_change_at,last_polled_at,last_error,created_at,updated_at,target_provider,target_instance_base_url,target_project_path,target_kind,target_provenance,target_unresolved_reason) FROM pr_monitor WHERE agent_id=? ORDER BY monitor_id")
        .bind(&f.agent.0).fetch_all(f.store.write_pool()).await.unwrap();
    assert_eq!(after_monitors, raw_monitors);
    let final_requests: Vec<Value> = std::fs::read_to_string(f.dir.path().join("wire.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let resumed_model: Option<Value> = std::fs::read(f.dir.path().join("model.json"))
        .ok()
        .map(|bytes| serde_json::from_slice(&bytes).unwrap());
    assert_eq!(
        final_requests
            .iter()
            .filter(|r| r["method"] == "session/new")
            .count(),
        0
    );
    if let Ok(path) = std::env::var("INTENT_REVIEW_CAPTURE") {
        std::fs::write(format!("{path}.child-{variant}.json"), serde_json::to_vec_pretty(&json!({
            "native":capture,"requests":requests,"replay":replay,"refused":refused,"variant":variant,"finalRequests":final_requests,"resumedModel":resumed_model,"originalRawMonitors":raw_monitors,"afterRawMonitors":after_monitors,
            "scope":"actual native child/compaction plus Services admission; fixture-seeded original Store bindings, controlled forge/model, authentication disabled"
        })).unwrap()).unwrap();
    }
    drop(connection);
    child.kill().await.unwrap();
    child.wait().await.unwrap();
}

//! Optional routing metadata must not replace resource identity or connection ownership.
use super::*;
use base64::Engine as _;
use serde_json::json;

async fn client(srv: &Server) -> PresenceClient {
    PresenceClient::open(srv.port, srv.cfg.clone(), TOKEN).await
}

async fn rpc(c: &mut PresenceClient, method: &str, params: Value) -> Value {
    let v = c.call(1, method, params).await;
    assert_eq!(v["jsonrpc"], "2.0", "{method}: {v}");
    assert_eq!(v["id"], 1);
    assert!(v.get("method").is_none());
    assert!(v.get("error").is_none(), "{method}: {v}");
    v.get("result").expect("result envelope").clone()
}

async fn workspace(srv: &Server) -> WorkspaceId {
    let id = WorkspaceId::new();
    let mut ws = fixture_workspace(&id);
    let path = srv.dir.path().join("workspaces").join(id.as_str());
    std::fs::create_dir_all(&path).unwrap();
    ws.path = Some(path.to_string_lossy().into());
    ws.worktree_path = ws.path.clone();
    srv.store.insert_workspace(&ws).await.unwrap();
    id
}

fn context(mut params: Value, workspace: &WorkspaceId, routed: bool) -> Value {
    if routed {
        params["workspaceId"] = json!(workspace);
    }
    params
}

/// Preserve notifications arriving before a response (including fast export builds).
async fn notification(c: &mut PresenceClient, method: &str, key: &str, value: &Value) -> Value {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(i) = c
                .skipped
                .iter()
                .position(|v| v["method"] == method && v["params"][key] == *value)
            {
                let v = c.skipped.remove(i);
                assert_eq!(v["jsonrpc"], "2.0");
                return v["params"].clone();
            }
            let v = c
                .next_within(Duration::from_secs(10))
                .await
                .expect("notification");
            c.skipped.push(v);
        }
    })
    .await
    .expect("notification deadline")
}

async fn snapshot(c: &mut PresenceClient, sub: &Value) -> Value {
    let push = notification(c, "subscription.push", "subscriptionId", sub).await;
    assert_eq!(push["kind"], "snapshot");
    assert_eq!(push["seq"], 0);
    push["snapshot"].clone()
}

#[intent_test_macros::daemon_test]
async fn resource_context_subscription_dispatch_and_connection_cleanup() {
    let srv = start(WsOptions::default()).await;
    let ws = workspace(&srv).await;
    let other_ws = workspace(&srv).await;
    let mut c = client(&srv).await;
    let mut other = client(&srv).await;
    let agent = rpc(
        &mut c,
        "agent.create",
        json!({"workspaceId":ws,"name":"routing-agent","provider":"mock","model":"default"}),
    )
    .await["agent"]["id"]
        .clone();
    let agent_id = intent_core::AgentId::from(agent.as_str().unwrap());
    srv.store
        .append_agent_message(
            &agent_id,
            "assistant",
            &json!("retained chat text"),
            &now_iso(),
        )
        .await
        .unwrap();
    for (subscribe, unsubscribe, params) in [
        (
            "agent.subscribe",
            "events.unsubscribe",
            json!({"workspaceId":ws}),
        ),
        (
            "note.subscribe",
            "note.unsubscribe",
            json!({"workspaceId":ws}),
        ),
        (
            "task.subscribe",
            "task.unsubscribe",
            json!({"workspaceId":ws}),
        ),
        (
            "comment.subscribe",
            "comment.unsubscribe",
            json!({"workspaceId":ws,"noteId":"spec"}),
        ),
        (
            "chat.subscribe",
            "chat.unsubscribe",
            json!({"agentId":agent}),
        ),
    ] {
        for routed in [false, true] {
            let sub = rpc(&mut c, subscribe, context(params.clone(), &ws, routed)).await
                ["subscriptionId"]
                .clone();
            let snap = snapshot(&mut c, &sub).await;
            if subscribe == "chat.subscribe" {
                assert!(snap.to_string().contains("retained chat text"));
            }
            if subscribe == "agent.subscribe" {
                assert!(snap.to_string().contains(agent.as_str().unwrap()));
            }
            // Another socket cannot tear down this registration, even with its workspace.
            assert_eq!(
                rpc(
                    &mut other,
                    unsubscribe,
                    json!({"subscriptionId":sub,"workspaceId":ws})
                )
                .await,
                json!({"success":false})
            );
            if subscribe == "agent.subscribe" {
                let legacy = c
                    .call(
                        2,
                        "agent.unsubscribe",
                        json!({"subscriptionId":sub,"workspaceId":ws}),
                    )
                    .await;
                assert!(
                    legacy.get("error").is_some(),
                    "must select service path: {legacy}"
                );
            }
            // Routing metadata is not an additional registry lookup scope.
            assert_eq!(
                rpc(
                    &mut c,
                    unsubscribe,
                    context(json!({"subscriptionId":sub}), &other_ws, routed)
                )
                .await,
                json!({"success":true})
            );
            assert_eq!(
                rpc(
                    &mut c,
                    unsubscribe,
                    context(json!({"subscriptionId":sub}), &ws, routed)
                )
                .await,
                json!({"success":false})
            );
        }
    }
    let bare =
        rpc(&mut c, "agent.subscribe", json!({"workspaceId":ws})).await["subscriptionId"].clone();
    snapshot(&mut c, &bare).await;
    assert_eq!(
        rpc(&mut c, "agent.unsubscribe", json!({"subscriptionId":bare})).await,
        json!({"success":true})
    );
    let legacy = rpc(
        &mut c,
        "agent.subscribe",
        json!({"workspaceId":ws,"eventTypes":["note:*"]}),
    )
    .await["subscriptionId"]
        .clone();
    assert_eq!(
        rpc(
            &mut c,
            "events.unsubscribe",
            json!({"workspaceId":ws,"subscriptionId":legacy})
        )
        .await["success"],
        false
    );
    assert_eq!(
        rpc(
            &mut c,
            "agent.unsubscribe",
            json!({"workspaceId":ws,"subscriptionId":legacy})
        )
        .await,
        json!({"success":true,"subscriptionId":legacy})
    );

    let old = rpc(
        &mut c,
        "agent.subscribe",
        json!({"workspaceId":ws,"replaceGroup":"agents"}),
    )
    .await["subscriptionId"]
        .clone();
    snapshot(&mut c, &old).await;
    c.close().await;
    let mut c = client(&srv).await;
    let new = rpc(
        &mut c,
        "agent.subscribe",
        json!({"workspaceId":ws,"replaceGroup":"agents"}),
    )
    .await["subscriptionId"]
        .clone();
    let snap = snapshot(&mut c, &new).await;
    assert!(snap.to_string().contains(agent.as_str().unwrap()));
    assert_ne!(old, new);
    assert_eq!(
        rpc(
            &mut c,
            "events.unsubscribe",
            json!({"workspaceId":ws,"subscriptionId":old})
        )
        .await["success"],
        false
    );
    assert_eq!(
        rpc(
            &mut c,
            "events.unsubscribe",
            json!({"workspaceId":ws,"subscriptionId":new})
        )
        .await["success"],
        true
    );
    c.close().await;
    other.close().await;
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn resource_context_presence_teardown_releases_only_its_lease() {
    let srv = start(WsOptions::default()).await;
    let ws = workspace(&srv).await;
    let mut watcher = client(&srv).await;
    let mut c = client(&srv).await;
    for (client, id) in [(&mut watcher, "watcher"), (&mut c, "editor")] {
        rpc(client, "client.hello", json!({"clientId":id,"name":id})).await;
    }
    let params = json!({"workspaceId":ws,"noteId":"spec"});
    let watch = rpc(&mut watcher, "note.presence.subscribe", params.clone()).await
        ["subscriptionId"]
        .clone();
    snapshot(&mut watcher, &watch).await;
    for method in ["events.unsubscribe", "note.presence.unsubscribe"] {
        let sub =
            rpc(&mut c, "note.presence.subscribe", params.clone()).await["subscriptionId"].clone();
        let viewers = snapshot(&mut c, &sub).await;
        assert_eq!(
            viewers["viewers"].as_array().unwrap().len(),
            1,
            "same person holds two leases"
        );
        assert_eq!(
            rpc(
                &mut c,
                method,
                json!({"workspaceId":ws,"subscriptionId":sub})
            )
            .await["success"],
            true
        );
    }
    // The watcher's surviving lease still admits cursor updates.
    let update = rpc(
        &mut watcher,
        "note.presence.update",
        json!({"workspaceId":ws,"noteId":"spec","rev":1,"anchor":1,"head":1}),
    )
    .await;
    assert_eq!(update["ok"], true);
    assert_eq!(
        rpc(
            &mut watcher,
            "events.unsubscribe",
            json!({"workspaceId":ws,"subscriptionId":watch})
        )
        .await["success"],
        true
    );
    let sub = rpc(&mut c, "note.presence.subscribe", params).await["subscriptionId"].clone();
    let viewers = snapshot(&mut c, &sub).await;
    assert!(
        viewers["viewers"][0]["cursor"].is_null(),
        "last lease cleanup removes cursor: {viewers}"
    );
    rpc(
        &mut c,
        "events.unsubscribe",
        json!({"workspaceId":ws,"subscriptionId":sub}),
    )
    .await;
    c.close().await;
    watcher.close().await;
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn resource_context_agent_queue_rename_stop_and_metrics() {
    let srv = start(WsOptions::default()).await;
    let ws = workspace(&srv).await;
    let unrelated = workspace(&srv).await;
    let mut c = client(&srv).await;
    for routed in [false, true] {
        let agent = rpc(
            &mut c,
            "agent.create",
            json!({"workspaceId":ws,"name":"original","provider":"mock","model":"default"}),
        )
        .await["agent"]["id"]
            .clone();
        let p = |v| context(v, &unrelated, routed);
        let queued = rpc(
            &mut c,
            "agent.queueMessage",
            p(json!({"agentId":agent,"content":"queued text"})),
        )
        .await;
        assert_eq!(queued["success"], true);
        let message = queued["queuedMessage"]["id"].clone();
        rpc(
            &mut c,
            "agent.editQueuedMessage",
            p(json!({"agentId":agent,"messageId":message,"content":"edited text","editing":true})),
        )
        .await;
        let queue = rpc(
            &mut c,
            "agent.getQueue",
            json!({"agentId":agent,"workspaceId":ws}),
        )
        .await;
        assert!(queue.to_string().contains("edited text"), "{queue}");
        rpc(
            &mut c,
            "agent.removeQueuedMessage",
            p(json!({"agentId":agent,"messageId":message})),
        )
        .await;
        let queue = rpc(&mut c, "agent.getQueue", json!({"agentId":agent})).await;
        assert!(!queue.to_string().contains("edited text"));
        rpc(
            &mut c,
            "agent.rename",
            p(json!({"agentId":agent,"name":"renamed"})),
        )
        .await;
        assert_eq!(
            rpc(&mut c, "agent.get", json!({"agentId":agent})).await["agent"]["name"],
            "renamed"
        );
        rpc(&mut c, "agent.stop", p(json!({"agentId":agent}))).await;
        srv.store
            .upsert_agent_metrics(&ws, agent.as_str().unwrap(), 12, 3, 2)
            .await
            .unwrap();
        let stats = rpc(&mut c, "metrics.getAgentStats", p(json!({"agentId":agent}))).await;
        assert_eq!(
            stats,
            json!({"additions":12,"deletions":3,"filesChanged":2})
        );
        rpc(
            &mut c,
            "metrics.clearAgentStats",
            p(json!({"agentId":agent})),
        )
        .await;
        assert!(
            rpc(&mut c, "metrics.getAgentStats", p(json!({"agentId":agent})))
                .await
                .is_null()
        );
    }
    c.close().await;
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn resource_context_upload_replay_commit_and_abort() {
    let srv = start(WsOptions::default()).await;
    let ws = workspace(&srv).await;
    let mut c = client(&srv).await;
    for routed in [false, true] {
        let bytes = b"routing attachment bytes";
        let begin = json!({"workspaceId":ws,"fileName":format!("context-{routed}.txt"),"sizeBytes":bytes.len(),"sha256":sha256_hex(bytes)});
        let upload =
            rpc(&mut c, "file.attachmentUpload.begin", begin.clone()).await["uploadId"].clone();
        let chunk = context(
            json!({"uploadId":upload,"seq":0,"data":base64::engine::general_purpose::STANDARD.encode(bytes)}),
            &ws,
            routed,
        );
        let first = rpc(&mut c, "file.attachmentUpload.chunk", chunk.clone()).await;
        assert_eq!(
            rpc(&mut c, "file.attachmentUpload.chunk", chunk).await,
            first
        );
        let placed = rpc(
            &mut c,
            "file.attachmentUpload.commit",
            context(json!({"uploadId":upload}), &ws, routed),
        )
        .await;
        let info = rpc(
            &mut c,
            "file.getAttachmentInfo",
            json!({"attachmentId":placed["attachmentId"],"workspaceId":ws}),
        )
        .await;
        assert_eq!(info["attachmentId"], placed["attachmentId"]);
        assert_eq!(info["exists"], true);
        assert_eq!(info["size"], bytes.len());
        let root = srv.dir.path().join("workspaces").join(ws.as_str());
        assert_eq!(
            std::fs::read(root.join(placed["path"].as_str().unwrap())).unwrap(),
            bytes
        );
        let upload = rpc(&mut c, "file.attachmentUpload.begin", begin).await["uploadId"].clone();
        assert_eq!(
            rpc(
                &mut c,
                "file.attachmentUpload.abort",
                context(json!({"uploadId":upload}), &ws, routed)
            )
            .await["aborted"],
            true
        );
        assert_eq!(
            rpc(
                &mut c,
                "file.attachmentUpload.abort",
                context(json!({"uploadId":upload}), &ws, routed)
            )
            .await["aborted"],
            false
        );
    }
    c.close().await;
    srv.ws.stop().await;
}

async fn transfer_event(c: &mut PresenceClient, export: &Value, kind: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(i) = c.skipped.iter().position(|v| {
                v["method"] == "events.event"
                    && v["params"]["event"]["type"] == kind
                    && v["params"]["event"]["data"]["exportId"] == *export
            }) {
                let v = c.skipped.remove(i);
                assert_eq!(v["jsonrpc"], "2.0");
                return v["params"]["event"].clone();
            }
            let v = c
                .next_within(Duration::from_secs(10))
                .await
                .expect("export event");
            c.skipped.push(v);
        }
    })
    .await
    .expect("export event deadline")
}

#[intent_test_macros::daemon_test]
async fn resource_context_export_archive_replay_finalize_and_ready_abort() {
    let srv = start(WsOptions::default()).await;
    let mut c = client(&srv).await;
    for routed in [false, true] {
        let ws = workspace(&srv).await;
        srv.store
            .insert_note(&fixture_note(&ws, "export-note", "source note bytes"))
            .await
            .unwrap();
        let sub = rpc(
            &mut c,
            "events.subscribe",
            json!({"workspaceId":ws,"eventTypes":["workspace:transfer:*"]}),
        )
        .await["subscriptionId"]
            .clone();
        let assets = srv.dir.path().join("assets").join(ws.as_str());
        std::fs::create_dir_all(&assets).unwrap();
        std::fs::write(assets.join("source-asset"), b"source asset bytes").unwrap();
        let attachment = rpc(&mut c,"file.placeAttachment",json!({"workspaceId":ws,"fileName":"source.txt","data":base64::engine::general_purpose::STANDARD.encode(b"source attachment bytes")})).await;
        let started = rpc(&mut c, "workspace.export.start", json!({"workspaceId":ws})).await;
        let export = &started["exportId"];
        assert!(started["maxChunkBytes"].as_u64().unwrap() > 0);
        let ready = transfer_event(&mut c, export, "workspace:transfer:ready").await;
        assert_eq!(ready["workspaceId"], json!(ws));
        assert_eq!(ready["data"]["workspaceId"], json!(ws));
        let progress = transfer_event(&mut c, export, "workspace:transfer:progress").await;
        assert_eq!(progress["workspaceId"], json!(ws));
        assert_eq!(progress["data"]["workspaceId"], json!(ws));
        assert!(progress["data"]["stage"].is_string());
        let mut bytes = Vec::new();
        for seq in 0..ready["data"]["totalChunks"].as_u64().unwrap() {
            let p = json!({"exportId":export,"seq":seq});
            let direct = rpc(&mut c, "workspace.export.read", p.clone()).await;
            let replay = rpc(&mut c, "workspace.export.read", context(p, &ws, true)).await;
            assert_eq!(direct, replay, "source context must preserve chunk replay");
            assert_eq!(direct["exportId"], *export);
            assert_eq!(direct["seq"], seq);
            assert_eq!(direct["totalChunks"], ready["data"]["totalChunks"]);
            bytes.extend(
                base64::engine::general_purpose::STANDARD
                    .decode(direct["data"].as_str().unwrap())
                    .unwrap(),
            );
        }
        assert_eq!(
            bytes.len() as u64,
            ready["data"]["archiveSizeBytes"].as_u64().unwrap()
        );
        assert_eq!(sha256_hex(&bytes), ready["data"]["archiveSha256"]);
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).unwrap();
        let mut notes = String::new();
        std::io::Read::read_to_string(&mut archive.by_name("rows/note.jsonl").unwrap(), &mut notes)
            .unwrap();
        assert!(notes.contains("source note bytes"));
        for (path, expected) in [
            ("assets/source-asset".to_string(), "source asset bytes"),
            (
                format!(
                    "attachments/{}",
                    attachment["attachmentId"].as_str().unwrap()
                ),
                "source attachment bytes",
            ),
        ] {
            let mut content = String::new();
            std::io::Read::read_to_string(&mut archive.by_name(&path).unwrap(), &mut content)
                .unwrap();
            assert_eq!(content, expected);
        }
        let staging = srv
            .dir
            .path()
            .join("workspaces/.export-staging")
            .join(export.as_str().unwrap());
        assert!(staging.exists());
        let finalized = rpc(&mut c,"workspace.export.finalize",context(json!({"exportId":export,"archiveSource":routed,"finalStatusMessage":"Source export finished"}),&ws,routed)).await;
        assert_eq!(finalized["finalized"], true);
        assert_eq!(finalized["workspace"]["id"], json!(ws));
        assert_eq!(
            finalized["workspace"]["statusMessage"],
            "Source export finished"
        );
        assert_eq!(finalized["workspace"]["archived"], routed);
        assert!(!staging.exists());
        let gone = c
            .call(
                3,
                "workspace.export.read",
                context(json!({"exportId":export,"seq":0}), &ws, routed),
            )
            .await;
        assert!(gone.get("error").is_some());
        assert_eq!(
            rpc(
                &mut c,
                "workspace.export.abort",
                context(json!({"exportId":export}), &ws, routed)
            )
            .await["aborted"],
            false
        );
        assert_eq!(
            rpc(
                &mut c,
                "events.unsubscribe",
                context(json!({"subscriptionId":sub}), &ws, routed)
            )
            .await["success"],
            true
        );

        // A second session is cancelled while ready; cleanup retains the source row.
        let sub = rpc(
            &mut c,
            "events.subscribe",
            json!({"workspaceId":ws,"eventTypes":["workspace:transfer:*"]}),
        )
        .await["subscriptionId"]
            .clone();
        let export = rpc(&mut c, "workspace.export.start", json!({"workspaceId":ws})).await
            ["exportId"]
            .clone();
        transfer_event(&mut c, &export, "workspace:transfer:ready").await;
        assert_eq!(
            rpc(
                &mut c,
                "workspace.export.abort",
                context(json!({"exportId":export}), &ws, routed)
            )
            .await["aborted"],
            true
        );
        assert!(!srv
            .dir
            .path()
            .join("workspaces/.export-staging")
            .join(export.as_str().unwrap())
            .exists());
        assert!(srv.store.get_workspace(&ws).await.is_ok());
        rpc(
            &mut c,
            "events.unsubscribe",
            context(json!({"subscriptionId":sub}), &ws, routed),
        )
        .await;
    }
    c.close().await;
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn resource_context_export_failure_cleanup_and_retry() {
    let srv = start(WsOptions::default()).await;
    let ws = workspace(&srv).await;
    let mut broken = srv.store.get_workspace(&ws).await.unwrap();
    // A locked Git index prevents the WIP snapshot; exercise real build failure.
    let repo = seed_git_repo("intentd-resource-export-failure-");
    std::fs::write(
        repo.path().join("untracked.txt"),
        "uncommitted source bytes",
    )
    .unwrap();
    std::fs::write(repo.path().join(".git/index.lock"), "fixture lock").unwrap();
    broken.repository_path = Some(repo.path().to_string_lossy().into());
    broken.worktree_path = broken.repository_path.clone();
    srv.store.update_workspace(&broken).await.unwrap();
    let mut c = client(&srv).await;
    let sub = rpc(
        &mut c,
        "events.subscribe",
        json!({"workspaceId":ws,"eventTypes":["workspace:transfer:*"]}),
    )
    .await["subscriptionId"]
        .clone();
    let export =
        rpc(&mut c, "workspace.export.start", json!({"workspaceId":ws})).await["exportId"].clone();
    let failed = transfer_event(&mut c, &export, "workspace:transfer:failed").await;
    assert_eq!(failed["workspaceId"], json!(ws));
    assert_eq!(failed["data"]["workspaceId"], json!(ws));
    assert!(failed["data"]["reason"].is_string(), "{failed}");
    assert!(!srv
        .dir
        .path()
        .join("workspaces/.export-staging")
        .join(export.as_str().unwrap())
        .exists());
    assert_eq!(
        rpc(
            &mut c,
            "workspace.export.abort",
            json!({"exportId":export,"workspaceId":ws})
        )
        .await["aborted"],
        false
    );
    std::fs::remove_file(repo.path().join(".git/index.lock")).unwrap();
    let retry =
        rpc(&mut c, "workspace.export.start", json!({"workspaceId":ws})).await["exportId"].clone();
    transfer_event(&mut c, &retry, "workspace:transfer:ready").await;
    assert_eq!(
        rpc(
            &mut c,
            "workspace.export.abort",
            json!({"exportId":retry,"workspaceId":ws})
        )
        .await["aborted"],
        true
    );
    rpc(
        &mut c,
        "events.unsubscribe",
        json!({"subscriptionId":sub,"workspaceId":ws}),
    )
    .await;
    c.close().await;
    srv.ws.stop().await;
}

#[cfg(unix)]
#[intent_test_macros::daemon_test]
async fn resource_context_terminal_and_exec_stream_followups() {
    let srv = start(WsOptions::default()).await;
    let ws = workspace(&srv).await;
    let mut c = client(&srv).await;
    let sub = rpc(
        &mut c,
        "events.subscribe",
        json!({"workspaceId":ws,"eventTypes":["terminal:*","host:exec:*"]}),
    )
    .await["subscriptionId"]
        .clone();
    for routed in [false, true] {
        let term = rpc(
            &mut c,
            "terminal.create",
            json!({"workspaceId":ws,"command":"/bin/cat"}),
        )
        .await["terminalId"]
            .clone();
        assert!(term.is_string());
        rpc(
            &mut c,
            "terminal.resize",
            context(json!({"terminalId":term,"cols":101,"rows":31}), &ws, routed),
        )
        .await;
        rpc(&mut c,"terminal.write",context(json!({"terminalId":term,"data":base64::engine::general_purpose::STANDARD.encode(b"resource-routing\n")}),&ws,routed)).await;
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let buffer = rpc(
                    &mut c,
                    "terminal.getBuffer",
                    context(json!({"terminalId":term}), &ws, routed),
                )
                .await;
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(buffer["data"].as_str().unwrap())
                    .unwrap();
                if String::from_utf8_lossy(&bytes).contains("resource-routing") {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("terminal output");
        rpc(
            &mut c,
            "terminal.kill",
            context(json!({"terminalId":term}), &ws, routed),
        )
        .await;

        let request = rpc(
            &mut c,
            "host.execStream",
            json!({"workspaceId":ws,"command":"/bin/cat"}),
        )
        .await["requestId"]
            .clone();
        assert_eq!(
            rpc(
                &mut c,
                "host.execStream.write",
                context(
                    json!({"requestId":request,"stdin":"resource-stream\n"}),
                    &ws,
                    routed
                )
            )
            .await["ok"],
            true
        );
        let ev = c.event("host:exec:stdout").await;
        assert_eq!(ev["data"]["requestId"], request);
        assert_eq!(ev["workspaceId"], json!(ws));
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(ev["data"]["chunk"].as_str().unwrap())
            .unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("resource-stream"));
        assert_eq!(
            rpc(
                &mut c,
                "host.execStream.cancel",
                context(json!({"requestId":request}), &ws, routed)
            )
            .await["cancelled"],
            true
        );
        let exit = c.event("host:exec:exit").await;
        assert_eq!(exit["data"]["requestId"], request);
        assert_eq!(
            rpc(
                &mut c,
                "host.execStream.cancel",
                context(json!({"requestId":request}), &ws, routed)
            )
            .await["cancelled"],
            false
        );
    }
    rpc(
        &mut c,
        "events.unsubscribe",
        json!({"subscriptionId":sub,"workspaceId":ws}),
    )
    .await;
    c.close().await;
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn resource_context_export_building_abort_and_late_cleanup() {
    let srv = start(WsOptions::default()).await;
    let ws = workspace(&srv).await;
    let mut c = client(&srv).await;
    let sub = rpc(
        &mut c,
        "events.subscribe",
        json!({"workspaceId":ws,"eventTypes":["workspace:transfer:*"]}),
    )
    .await["subscriptionId"]
        .clone();
    // Holding the writer blocks the build's first durable progress event, not
    // the read-only start admission. This makes Building cancellation deterministic.
    let writer = srv.store.write_pool().acquire().await.unwrap();
    let export =
        rpc(&mut c, "workspace.export.start", json!({"workspaceId":ws})).await["exportId"].clone();
    for (method, params) in [
        (
            "workspace.export.read",
            json!({"exportId":export,"seq":0,"workspaceId":ws}),
        ),
        (
            "workspace.export.finalize",
            json!({"exportId":export,"archiveSource":true,"workspaceId":ws}),
        ),
    ] {
        let reply = c.call(2, method, params).await;
        assert_eq!(reply["error"]["code"], -32602, "{reply}");
        assert!(reply.to_string().contains("building"), "{reply}");
    }
    assert_eq!(
        rpc(
            &mut c,
            "workspace.export.abort",
            json!({"exportId":export,"workspaceId":ws})
        )
        .await["aborted"],
        true
    );
    drop(writer);
    transfer_event(&mut c, &export, "workspace:transfer:progress").await;
    // The build cleans asynchronously; wait on session retirement, not a sleep.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let reply = c
                .call(
                    2,
                    "workspace.export.read",
                    json!({"exportId":export,"seq":0,"workspaceId":ws}),
                )
                .await;
            if reply["error"]["data"]["code"] == "not-found" {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        rpc(&mut c, "workspace.export.abort", json!({"exportId":export})).await["aborted"],
        false
    );
    let row = srv.store.get_workspace(&ws).await.unwrap();
    assert!(!row.archived);
    assert_eq!(
        rpc(
            &mut c,
            "events.unsubscribe",
            json!({"subscriptionId":sub,"workspaceId":ws})
        )
        .await["success"],
        true
    );
    c.close().await;
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn resource_context_search_cancel_stops_workspace_stream() {
    let srv = start(WsOptions::default()).await;
    let ws = workspace(&srv).await;
    let other = workspace(&srv).await;
    let mut c = client(&srv).await;
    let agent = rpc(
        &mut c,
        "agent.create",
        json!({"workspaceId":ws,"provider":"mock","model":"default"}),
    )
    .await["agent"]["id"]
        .clone();
    let agent_id = intent_core::AgentId::from(agent.as_str().unwrap());
    for i in 0..200 {
        srv.store
            .append_agent_message(
                &agent_id,
                "assistant",
                &json!(format!("needle {i}")),
                &now_iso(),
            )
            .await
            .unwrap();
    }
    let sub = rpc(
        &mut c,
        "events.subscribe",
        json!({"workspaceId":ws,"eventTypes":["search:*"]}),
    )
    .await["subscriptionId"]
        .clone();
    for routed in [false, true] {
        let request = format!("resource-search-{routed}");
        let ack = rpc(
            &mut c,
            "search.messages",
            json!({"workspaceId":ws,"query":"needle","requestId":request}),
        )
        .await;
        assert_eq!(ack["requestId"], request);
        let batch = c.event("search:result").await;
        assert_eq!(batch["data"]["requestId"], request);
        assert_eq!(batch["workspaceId"], json!(ws));
        assert!(!batch["data"]["matches"].as_array().unwrap().is_empty());
        assert_eq!(
            rpc(
                &mut c,
                "search.cancel",
                context(json!({"requestId":request}), &other, routed)
            )
            .await,
            json!({"ok":true})
        );
        let done = c.event("search:done").await;
        assert_eq!(done["data"]["requestId"], request);
        assert_eq!(done["data"]["truncated"], true);
        assert!(done["data"]["total"].as_u64().unwrap() < 200);
        assert_eq!(
            rpc(
                &mut c,
                "search.cancel",
                context(json!({"requestId":request}), &ws, routed)
            )
            .await,
            json!({"ok":true})
        );
    }
    rpc(
        &mut c,
        "events.unsubscribe",
        json!({"subscriptionId":sub,"workspaceId":ws}),
    )
    .await;
    c.close().await;
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn resource_context_cannot_grant_access_to_another_workspaces_resources() {
    let srv = start(WsOptions::default()).await;
    let source = workspace(&srv).await;
    let allowed = workspace(&srv).await;
    let mut owner = client(&srv).await;
    let mut guest = Guest::connect(&srv, &"b3".repeat(32)).await;
    let primary = srv.store.get_primary_principal().await.unwrap();
    srv.store
        .set_workspace_member_role(
            &allowed,
            &primary.id,
            intent_core::WorkspaceRole::Collaborator,
        )
        .await
        .unwrap();
    srv.store
        .add_workspace_member(
            &allowed,
            &guest.principal.id,
            intent_core::WorkspaceRole::Owner,
        )
        .await
        .unwrap();
    let agent = rpc(
        &mut owner,
        "agent.create",
        json!({"workspaceId":source,"provider":"mock","model":"default"}),
    )
    .await["agent"]["id"]
        .clone();
    let upload = rpc(&mut owner,"file.attachmentUpload.begin",json!({"workspaceId":source,"fileName":"private.txt","sizeBytes":4,"sha256":sha256_hex(b"data")})).await["uploadId"].clone();
    let sub = rpc(
        &mut owner,
        "events.subscribe",
        json!({"workspaceId":source,"eventTypes":["workspace:transfer:*"]}),
    )
    .await["subscriptionId"]
        .clone();
    let export = rpc(
        &mut owner,
        "workspace.export.start",
        json!({"workspaceId":source}),
    )
    .await["exportId"]
        .clone();
    transfer_event(&mut owner, &export, "workspace:transfer:ready").await;
    for (method, params) in [
        (
            "agent.queueMessage",
            json!({"agentId":agent,"content":"unauthorized"}),
        ),
        ("agent.stop", json!({"agentId":agent})),
        (
            "agent.rename",
            json!({"agentId":agent,"name":"unauthorized"}),
        ),
        ("metrics.getAgentStats", json!({"agentId":agent})),
        ("metrics.clearAgentStats", json!({"agentId":agent})),
        (
            "file.attachmentUpload.chunk",
            json!({"uploadId":upload,"seq":0,"data":"ZGF0YQ=="}),
        ),
        ("file.attachmentUpload.commit", json!({"uploadId":upload})),
        ("file.attachmentUpload.abort", json!({"uploadId":upload})),
        ("workspace.export.read", json!({"exportId":export,"seq":0})),
        (
            "workspace.export.finalize",
            json!({"exportId":export,"archiveSource":true}),
        ),
        ("workspace.export.abort", json!({"exportId":export})),
    ] {
        let direct = guest.call(method, params.clone()).await;
        assert!(direct.get("error").is_some(), "{method}: {direct}");
        let mut routed = guest.call(method, context(params, &allowed, true)).await;
        routed["id"] = direct["id"].clone();
        assert_eq!(
            routed, direct,
            "{method} must authorize the resource, not the metadata"
        );
    }
    assert_eq!(
        rpc(
            &mut owner,
            "workspace.export.abort",
            json!({"exportId":export,"workspaceId":source})
        )
        .await["aborted"],
        true
    );
    assert_eq!(
        rpc(
            &mut owner,
            "file.attachmentUpload.abort",
            json!({"uploadId":upload,"workspaceId":source})
        )
        .await["aborted"],
        true
    );
    rpc(
        &mut owner,
        "events.unsubscribe",
        json!({"subscriptionId":sub,"workspaceId":source}),
    )
    .await;
    owner.close().await;
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn resource_context_event_subscription_retains_workspace_filter() {
    let srv = start(WsOptions::default()).await;
    let a = workspace(&srv).await;
    let b = workspace(&srv).await;
    let mut scoped = client(&srv).await;
    let mut global = client(&srv).await;
    let sub = rpc(
        &mut scoped,
        "events.subscribe",
        json!({"workspaceId":a,"eventTypes":["note:created"]}),
    )
    .await["subscriptionId"]
        .clone();
    let all = rpc(
        &mut global,
        "events.subscribe",
        json!({"eventTypes":["note:created"]}),
    )
    .await["subscriptionId"]
        .clone();
    // Durable publication order is the barrier: seeing A means B was processed.
    for ws in [&b, &a] {
        srv.bus
            .publish(&intent_store::NewEvent {
                workspace_id: ws.clone(),
                event_type: intent_core::events::NOTE_CREATED.into(),
                timestamp: now_iso(),
                actor: intent_core::EventActor::default(),
                session_id: None,
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data: json!({"noteId":"fixture"}),
            })
            .await
            .unwrap();
    }
    assert_eq!(scoped.event("note:created").await["workspaceId"], json!(a));
    assert_eq!(global.event("note:created").await["workspaceId"], json!(b));
    assert_eq!(global.event("note:created").await["workspaceId"], json!(a));
    rpc(
        &mut scoped,
        "events.unsubscribe",
        json!({"workspaceId":a,"subscriptionId":sub}),
    )
    .await;
    rpc(
        &mut global,
        "events.unsubscribe",
        json!({"subscriptionId":all}),
    )
    .await;
    scoped.close().await;
    global.close().await;
    srv.ws.stop().await;
}

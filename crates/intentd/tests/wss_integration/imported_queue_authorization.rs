//! Imported human input requires the exact current owner credential at dequeue.
use super::*;
use intent_core::AgentId;
use serde_json::json;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Condvar,
};

struct DurableToken(std::path::PathBuf);
impl TokenStore for DurableToken {
    fn load_token(&self) -> Option<String> {
        std::fs::read_to_string(&self.0).ok()
    }
    fn store_token(&self, token: &str) -> CoreResult<()> {
        std::fs::write(&self.0, token).map_err(|e| intent_core::Error::Internal(e.to_string()))
    }
}

/// Pause the first real SQL read after dispatch, before any dequeue. The
/// destructor always releases SQLite, even if the async assertion panics.
struct ReadBarrier {
    reached: Arc<tokio::sync::Notify>,
    release: Arc<(Mutex<bool>, Condvar)>,
    armed: Arc<AtomicBool>,
}
impl ReadBarrier {
    async fn install(store: &Store) -> Self {
        let barrier = Self {
            reached: Arc::default(),
            release: Arc::default(),
            armed: Arc::default(),
        };
        let mut connections = Vec::new();
        for _ in 0..store.read_pool().options().get_max_connections() {
            connections.push(store.read_pool().acquire().await.unwrap());
        }
        for connection in &mut connections {
            let (armed, reached, release) = (
                barrier.armed.clone(),
                barrier.reached.clone(),
                barrier.release.clone(),
            );
            connection
                .lock_handle()
                .await
                .unwrap()
                .set_progress_handler(1, move || {
                    if armed.swap(false, Ordering::SeqCst) {
                        reached.notify_one();
                        let (lock, cv) = &*release;
                        let (_released, timeout) = cv
                            .wait_timeout_while(
                                lock.lock().unwrap(),
                                Duration::from_secs(15),
                                |v| !*v,
                            )
                            .unwrap();
                        assert!(
                            !timeout.timed_out(),
                            "SQL barrier must be explicitly released"
                        );
                    }
                    true
                });
        }
        barrier
    }
    async fn after_pop(store: &Store) -> Self {
        let barrier = Self {
            reached: Arc::default(),
            release: Arc::default(),
            armed: Arc::default(),
        };
        let (armed, reached, release) = (
            barrier.armed.clone(),
            barrier.reached.clone(),
            barrier.release.clone(),
        );
        store
            .write_pool()
            .acquire()
            .await
            .unwrap()
            .lock_handle()
            .await
            .unwrap()
            .set_update_hook(move |row| {
                if row.table == "agent_queue"
                    && row.operation == sqlx::sqlite::SqliteOperation::Delete
                    && armed.swap(false, Ordering::SeqCst)
                {
                    reached.notify_one();
                    let (lock, cv) = &*release;
                    let (_released, timeout) = cv
                        .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(15), |v| !*v)
                        .unwrap();
                    assert!(
                        !timeout.timed_out(),
                        "post-pop delivery barrier must release"
                    );
                }
            });
        barrier
    }
    fn release(&self) {
        let (lock, cv) = &*self.release;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }
}
impl Drop for ReadBarrier {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Clone, Copy)]
enum Change {
    Unchanged,
    Rotate,
    Remove,
    RevokePersonal,
    RotateAfterPop,
}

async fn imported_owner_case(change: Change, runtime: bool) {
    let dir = test_tempdir("intentd-imported-owner-fence-");
    let store = Store::open(&dir.path().join("intentd.db")).await.unwrap();
    let bus = EventBus::new(store.clone());
    let services = Arc::new(
        Services::new(store.clone())
            .with_workspaces_root(dir.path().join("workspaces"))
            .with_event_bus(bus.clone()),
    );
    let manager = runtime.then(|| {
        let manager = Arc::new(intent_services::AgentManager::new(
            (*services).clone(),
            Arc::new(intent_services::BusEventSink::new(bus.clone())),
            4,
        ));
        services.attach_agent_manager(&manager);
        manager
    });
    let tls = ensure_tls_certificate(dir.path()).unwrap();
    let backing = Arc::new(DurableToken(dir.path().join("token")));
    backing.store_token(TOKEN).unwrap();
    let tokens = Arc::new(AsyncTokenStore::new(backing));
    let server = WsApiServer::new(
        services.clone(),
        bus,
        &tls,
        &tokens,
        WsOptions {
            base_port: 0,
            bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
            ..Default::default()
        },
        None,
    )
    .unwrap();
    let port = server.start().await.unwrap();
    let cfg = client_config(&tls.fingerprint256);
    let personal_token = "ef".repeat(32);
    if matches!(change, Change::RevokePersonal) {
        let owner = store.get_primary_principal().await.unwrap();
        store
            .insert_principal_credential(&owner.id, &sha256_hex(personal_token.as_bytes()))
            .await
            .unwrap();
    }
    let mut owner = PresenceClient::open(
        port,
        cfg.clone(),
        if matches!(change, Change::RevokePersonal) {
            &personal_token
        } else {
            TOKEN
        },
    )
    .await;
    let created = owner
        .call(1, "workspace.create", json!({"title":"Owner fence"}))
        .await;
    let ws = created["result"]["workspace"]["id"].as_str().unwrap();
    let agent = AgentId::new();
    // This provider deliberately cannot spawn: runtime delivery/append occurs
    // before provider startup, and no external process/account is needed.
    let session = serde_json::from_value(json!({"id":agent,"workspaceId":ws,"name":"Imported","provider":"unavailable-fence-test-provider","status":"idle","createdAt":now_iso(),"updatedAt":now_iso()})).unwrap();
    store.insert_agent_session(&session).await.unwrap();
    let payload = json!({"id":"imported-fence","content":"original input","queuedAt":now_iso(),"userOrigin":true,"messageMetadata":{"humanAuthor":{"login":"source","displayName":null,"avatarUrl":null},"humanAuthorOriginalMetadata":["preserve",null]}});
    store
        .replace_agent_queue(
            &agent,
            &[intent_store::AgentQueueRow {
                id: "imported-fence".into(),
                agent_id: agent.clone(),
                position: 0,
                payload: payload.clone(),
                created_at: now_iso(),
                turn_id: "imported-fence".into(),
            }],
        )
        .await
        .unwrap();
    assert_eq!(services.rehydrate_agent_queues().await.unwrap(), 1);
    let initial = store.load_all_agent_queues().await.unwrap();
    let after_pop = matches!(change, Change::RotateAfterPop);
    let barrier = if after_pop {
        ReadBarrier::after_pop(&store).await
    } else {
        ReadBarrier::install(&store).await
    };
    barrier.armed.store(true, Ordering::SeqCst);
    owner
        .send(
            2,
            "agent.sendQueuedMessageNow",
            json!({"workspaceId":ws,"agentId":agent,"messageId":"imported-fence"}),
        )
        .await;
    tokio::time::timeout(Duration::from_secs(10), barrier.reached.notified())
        .await
        .unwrap();
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_queue WHERE id='imported-fence'")
            .fetch_one(if after_pop {
                store.read_pool()
            } else {
                store.write_pool()
            })
            .await
            .unwrap();
    assert_eq!(
        count, 1,
        "durable entry still present before delivery commits"
    );
    let fresh_token = "cd".repeat(32);
    match change {
        Change::Unchanged => {}
        Change::Rotate | Change::RotateAfterPop => {
            tokio::time::timeout(Duration::from_secs(5), tokens.store_token(&fresh_token))
                .await
                .expect("credential lease released at pop, before persistence")
                .unwrap()
        }
        Change::Remove => tokens.store_token("").await.unwrap(),
        Change::RevokePersonal => {
            assert!(store
                .revoke_principal_credential(&sha256_hex(personal_token.as_bytes()))
                .await
                .unwrap());
        }
    }
    barrier.release();
    let response = owner.reply(2, "agent.sendQueuedMessageNow").await;
    let expected_refusal = !matches!(change, Change::Unchanged | Change::RotateAfterPop);
    let rows = store.get_agent_messages(&agent, None).await.unwrap();
    let queue = store.load_all_agent_queues().await.unwrap();
    // Clean up before asserting the red/green behavior.
    owner.close().await;
    if expected_refusal {
        tokens.store_token(&fresh_token).await.unwrap();
        let mut fresh = PresenceClient::open(port, cfg, &fresh_token).await;
        let fresh_reply = fresh
            .call(
                1,
                "agent.sendQueuedMessageNow",
                json!({"workspaceId":ws,"agentId":agent,"messageId":"imported-fence"}),
            )
            .await;
        fresh.close().await;
        if response.get("error").is_some() {
            assert_eq!(
                fresh_reply["result"]["queued"], false,
                "fresh credential must send: {fresh_reply}"
            );
            let delivered = store.get_agent_messages(&agent, None).await.unwrap();
            let message = delivered.iter().find(|m| m.id == "imported-fence").unwrap();
            assert_eq!(
                message.metadata.as_ref().unwrap()["humanAuthor"],
                payload["messageMetadata"]["humanAuthor"]
            );
            assert_eq!(
                message.metadata.as_ref().unwrap()["humanAuthorOriginalMetadata"],
                payload["messageMetadata"]["humanAuthorOriginalMetadata"]
            );
        }
    }
    if let Some(manager) = manager {
        manager.shutdown().await;
    }
    server.stop().await;
    store.close().await;
    if expected_refusal {
        assert_eq!(
            response["error"]["code"], -32003,
            "revoked credential must refuse before pop: {response}"
        );
        assert!(rows.iter().all(|m| m.id != "imported-fence"));
        assert_eq!(queue.len(), 1);
        assert_eq!(
            queue[0].payload, initial[0].payload,
            "denial preserves the full durable payload"
        );
    } else {
        assert_eq!(response["result"]["queued"], false, "{response}");
        assert!(rows.iter().any(|m| m.id == "imported-fence"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn imported_owner_credential_rotation_store_only() {
    imported_owner_case(Change::Rotate, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn imported_owner_credential_removal_store_only() {
    imported_owner_case(Change::Remove, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn imported_owner_credential_unchanged_store_only() {
    imported_owner_case(Change::Unchanged, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn imported_owner_credential_rotation_runtime() {
    imported_owner_case(Change::Rotate, true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn imported_owner_credential_removal_runtime() {
    imported_owner_case(Change::Remove, true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn imported_owner_credential_unchanged_runtime() {
    imported_owner_case(Change::Unchanged, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn imported_owner_credential_personal_revoke_store_only() {
    imported_owner_case(Change::RevokePersonal, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn imported_owner_credential_personal_revoke_runtime() {
    imported_owner_case(Change::RevokePersonal, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn imported_owner_credential_rotation_after_pop_store_only() {
    imported_owner_case(Change::RotateAfterPop, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn imported_owner_credential_rotation_after_pop_runtime() {
    imported_owner_case(Change::RotateAfterPop, true).await;
}

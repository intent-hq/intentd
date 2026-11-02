//! Quota failover for a remote agent (intent-hq/intent#8317): the switch to a
//! backup profile is announced only once the agent runs again on its node
//! under the backup; a backup the node cannot start leaves the agent on its
//! previous configuration, failed with an actionable message, and nothing
//! retries behind the user's back.

use super::*;
use std::collections::VecDeque;

/// What one `runtime()` call of [`ScriptedRemote`] hands out.
enum Launch {
    Runs(Arc<FakeRuntime>),
    Fails(&'static str),
}

/// A remote-launch port whose successive starts follow a script, recording
/// how many `provider_failover` rows the transcript held at each start.
struct ScriptedRemote {
    launches: Mutex<VecDeque<Launch>>,
    asked: AtomicUsize,
    failover_rows_at_start: Mutex<Vec<usize>>,
}

impl ScriptedRemote {
    fn new(launches: Vec<Launch>) -> Arc<Self> {
        Arc::new(Self {
            launches: Mutex::new(launches.into()),
            asked: AtomicUsize::new(0),
            failover_rows_at_start: Mutex::new(Vec::new()),
        })
    }
}

impl crate::feature_ports::RemoteLaunchPort for ScriptedRemote {
    fn placements<'a>(
        &'a self,
        _svc: &'a Services,
    ) -> BoxFuture<'a, intent_core::Result<intent_core::placement::PlatformAvailability>> {
        Box::pin(async { Ok(intent_core::placement::PlatformAvailability::none(now_iso())) })
    }
    fn placement_help<'a>(&'a self, _svc: &'a Services) -> BoxFuture<'a, Option<String>> {
        Box::pin(async { None })
    }
    fn admit<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a WorkspaceId,
        _placement: &'a intent_core::placement::AgentPlacement,
        _needs: &'a intent_core::remote_launch::RemoteLaunchNeeds,
    ) -> BoxFuture<'a, intent_core::Result<intent_core::remote_launch::RemotePlacementView>> {
        Box::pin(async { Ok(remote_view()) })
    }
    fn launch<'a>(
        &'a self,
        _svc: &'a Services,
        _request: intent_core::remote_launch::RemoteLaunchRequest,
    ) -> BoxFuture<'a, intent_core::Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn runtime<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, intent_core::Result<Arc<crate::feature_ports::RemoteRuntime>>> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let messages = svc.store.get_agent_messages(agent_id, None).await?;
            self.failover_rows_at_start
                .lock()
                .unwrap()
                .push(failover_rows(&messages).len());
            match self.launches.lock().unwrap().pop_front() {
                Some(Launch::Runs(runtime)) => {
                    Ok(runtime as Arc<crate::feature_ports::RemoteRuntime>)
                }
                Some(Launch::Fails(reason)) => Err(Error::Internal(reason.into())),
                None => Err(Error::InvalidParams(
                    "node-unavailable: no scripted launch left".into(),
                )),
            }
        })
    }
}

fn failover_rows(messages: &[intent_core::AgentMessage]) -> Vec<&intent_core::AgentMessage> {
    messages
        .iter()
        .filter(|m| {
            m.metadata.as_ref().and_then(|md| md["type"].as_str())
                == Some(crate::quota_failover::PROVIDER_FAILOVER_NOTICE_TYPE)
        })
        .collect()
}

/// A `claude-code` agent (model `opus`) placed on node "Mac", with one
/// backup profile; returns the backup's reference.
async fn remote_agent_with_backup(
    port: Arc<ScriptedRemote>,
    id: &AgentId,
    ws: &WorkspaceId,
) -> (TempDb, Arc<AgentManager>, String) {
    remote_agent_with_backup_env(port, id, ws, json!([])).await
}

/// [`remote_agent_with_backup`] with the backup's saved environment.
async fn remote_agent_with_backup_env(
    port: Arc<ScriptedRemote>,
    id: &AgentId,
    ws: &WorkspaceId,
    env: Value,
) -> (TempDb, Arc<AgentManager>, String) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new_with_file_secrets(
        store,
        intent_core::FileSecretStore::with_path(tmp.path.with_extension("secrets.json")),
    )
    .with_event_bus(bus.clone())
    .with_remote_launch_port(port);
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus));
    let mgr = Arc::new(AgentManager::new(services, sink, 8));
    seed_agent(&mgr, ws, id).await;
    let store = &mgr.services.store;
    store
        .set_agent_session_model(ws, id, "opus", Some("claude-code"), &now_iso())
        .await
        .unwrap();
    store
        .set_agent_session_metadata_key_json(
            ws,
            id,
            intent_core::remote_launch::REMOTE_PLACEMENT_METADATA_KEY,
            &serde_json::to_string(&remote_view()).unwrap(),
            &now_iso(),
        )
        .await
        .unwrap();
    let backup = mgr
        .services
        .provider_profiles_create_impl(json!({
            "baseProviderId": "claude-code",
            "name": "Backup",
            "backup": true,
            "env": env,
        }))
        .await
        .expect("create profile")["profile"]["providerRef"]
        .as_str()
        .unwrap()
        .to_owned();
    mgr.services.attach_agent_manager(&mgr);
    (tmp, mgr, backup)
}

async fn send(mgr: &Arc<AgentManager>, id: &AgentId, ws: &WorkspaceId, text: &str) {
    mgr.send_message(
        id.clone(),
        ws.clone(),
        text.into(),
        None,
        super::super::super::TurnOptions::default(),
    )
    .await
    .unwrap();
}

/// A remote agent out of quota whose backup starts on its node: the turn
/// continues there, and the failover row lands only after the node started
/// the backup run.
#[intent_test_macros::daemon_test]
async fn a_remote_agent_fails_over_once_the_backup_runs_on_its_node() {
    let ws = WorkspaceId::from("ws-remote-failover-ok");
    let id = AgentId::from("agent-remote-failover-ok");
    let primary = FakeRuntime::new();
    primary.quota_failures.store(1, Ordering::SeqCst);
    let backup_run = FakeRuntime::new();
    let port = ScriptedRemote::new(vec![
        Launch::Runs(primary.clone()),
        Launch::Runs(backup_run.clone()),
    ]);
    let (_tmp, mgr, backup) = remote_agent_with_backup(port.clone(), &id, &ws).await;

    send(&mgr, &id, &ws, "hello").await;
    backup_run.wait_prompts(1).await;
    backup_run.finish.add_permits(1);
    wait_idle(&mgr, &id).await;

    assert_eq!(primary.prompts.lock().unwrap().len(), 1);
    assert_eq!(port.asked.load(Ordering::SeqCst), 2);
    assert_eq!(
        *port.failover_rows_at_start.lock().unwrap(),
        vec![0, 0],
        "no failover row before the node started the backup"
    );
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(session.provider.as_deref(), Some(backup.as_str()));
    assert_eq!(session.model.as_deref(), Some("opus"));
    assert_ne!(session.status, AgentStatus::Error);
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .unwrap();
    let rows = failover_rows(&messages);
    assert_eq!(rows.len(), 1, "{messages:#?}");
    let md = rows[0].metadata.as_ref().unwrap();
    assert_eq!(md["fromProvider"], json!("claude-code"));
    assert_eq!(md["toProvider"], json!(backup));
    assert!(
        messages.iter().any(|m| m.role == "assistant"),
        "the backup run's reply reaches the transcript"
    );
}

/// A remote agent out of quota whose backup cannot start on its node: no
/// "switched to the backup" row, the previous configuration restored, and
/// the agent failed with a message saying what went wrong and what to do.
/// The node is asked exactly twice: no further start behind the user's back.
#[intent_test_macros::daemon_test]
async fn a_remote_backup_the_node_cannot_start_fails_with_an_actionable_message() {
    let ws = WorkspaceId::from("ws-remote-failover-ko");
    let id = AgentId::from("agent-remote-failover-ko");
    let primary = FakeRuntime::new();
    primary.quota_failures.store(1, Ordering::SeqCst);
    let port = ScriptedRemote::new(vec![
        Launch::Runs(primary.clone()),
        Launch::Fails("the account folder ~/.claude-backup does not exist on this machine"),
    ]);
    let (_tmp, mgr, _backup) = remote_agent_with_backup(port.clone(), &id, &ws).await;

    send(&mgr, &id, &ws, "hello").await;
    wait_idle(&mgr, &id).await;

    assert_eq!(port.asked.load(Ordering::SeqCst), 2, "no silent retry");
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(
        session.provider.as_deref(),
        Some("claude-code"),
        "the previous configuration is restored"
    );
    assert_eq!(session.model.as_deref(), Some("opus"));
    assert_eq!(session.status, AgentStatus::Error);
    assert!(
        crate::provider_return::read_state(&session).is_none(),
        "no return-home timer for a switch that never happened"
    );
    let reason = session.stop_reason.unwrap_or_default();
    for needle in [
        "ran out of quota on Mac",
        "Backup",
        "couldn't start there",
        "~/.claude-backup does not exist",
        "Sign in to",
        "create a new agent",
    ] {
        assert!(
            reason.contains(needle),
            "{needle:?} missing from {reason:?}"
        );
    }
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .unwrap();
    assert!(
        failover_rows(&messages).is_empty(),
        "no failover row for a switch that did not happen: {messages:#?}"
    );
}

/// A remote agent whose configuration the shared quota cache marks out of
/// quota at the start of a turn moves to the backup the same way, even with
/// its previous run still live: failover is the one provider switch a
/// remote agent allows, so the run is replaced instead of the turn being
/// refused as `remote-model-change-unsupported`.
#[intent_test_macros::daemon_test]
async fn a_remote_agent_skips_a_marked_configuration_onto_a_backup_its_node_starts() {
    let ws = WorkspaceId::from("ws-remote-failover-skip");
    let id = AgentId::from("agent-remote-failover-skip");
    let primary = FakeRuntime::new();
    let backup_run = FakeRuntime::new();
    let port = ScriptedRemote::new(vec![
        Launch::Runs(primary.clone()),
        Launch::Runs(backup_run.clone()),
    ]);
    let (_tmp, mgr, backup) = remote_agent_with_backup(port.clone(), &id, &ws).await;

    send(&mgr, &id, &ws, "first").await;
    primary.wait_prompts(1).await;
    primary.finish.add_permits(1);
    wait_idle(&mgr, &id).await;
    assert!(mgr.contains(&id), "the first run stays live");

    let now = chrono::Utc::now();
    mgr.services
        .provider_quota_cache
        .mark_exhausted("claude-code", None, now, || true);
    send(&mgr, &id, &ws, "second").await;
    backup_run.wait_prompts(1).await;
    backup_run.finish.add_permits(1);
    wait_idle(&mgr, &id).await;

    assert_eq!(primary.stops.load(Ordering::SeqCst), 1, "the old run stops");
    assert_eq!(port.asked.load(Ordering::SeqCst), 2);
    assert_eq!(*port.failover_rows_at_start.lock().unwrap(), vec![0, 0]);
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(session.provider.as_deref(), Some(backup.as_str()));
    assert_ne!(session.status, AgentStatus::Error);
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .unwrap();
    assert_eq!(failover_rows(&messages).len(), 1, "{messages:#?}");
}

/// A remote agent out of quota whose only backup uses an account folder on
/// this backend never switches to it (strict: whether or not the machine
/// has the folder): it stays on its configuration and fails with what to do,
/// and the node is never asked to start the backup.
#[intent_test_macros::daemon_test]
async fn a_remote_agent_never_fails_over_to_an_account_folder_backup() {
    let ws = WorkspaceId::from("ws-remote-failover-folder");
    let id = AgentId::from("agent-remote-failover-folder");
    let primary = FakeRuntime::new();
    primary.quota_failures.store(1, Ordering::SeqCst);
    let port = ScriptedRemote::new(vec![Launch::Runs(primary.clone())]);
    let (_tmp, mgr, _backup) = remote_agent_with_backup_env(
        port.clone(),
        &id,
        &ws,
        json!([{ "key": "CLAUDE_CONFIG_DIR", "kind": "set", "value": "~/.claude-backup" }]),
    )
    .await;

    send(&mgr, &id, &ws, "hello").await;
    wait_idle(&mgr, &id).await;

    assert_eq!(
        port.asked.load(Ordering::SeqCst),
        1,
        "the backup is never started"
    );
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(session.provider.as_deref(), Some("claude-code"));
    assert_eq!(session.status, AgentStatus::Error);
    let reason = session.stop_reason.unwrap_or_default();
    for needle in [
        "ran out of quota on Mac",
        "Backup",
        "uses an account folder on this backend",
        "can't run on other machines",
        "Create a new agent with an API-key profile or the machine's own sign-in",
    ] {
        assert!(
            reason.contains(needle),
            "{needle:?} missing from {reason:?}"
        );
    }
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .unwrap();
    assert!(failover_rows(&messages).is_empty(), "{messages:#?}");
}

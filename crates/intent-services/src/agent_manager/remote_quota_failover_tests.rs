//! Quota failover for a remote agent (intent-hq/intent#8317): the switch to a
//! backup profile is announced only once the agent runs again on its node
//! under the backup; a backup the node cannot start leaves the agent on its
//! previous configuration, failed with an actionable message, and nothing
//! retries behind the user's back.

use super::*;
use std::collections::VecDeque;

/// What one `runtime()` call of [`ScriptedRemote`] hands out.
enum Launch {
    /// A fresh launch: records its sign-in on the placement, as the real
    /// port does.
    Runs(Arc<FakeRuntime>),
    /// A run the machine kept across a restart: records nothing.
    Reattaches(Arc<FakeRuntime>),
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
    fn placement_help<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a intent_core::WorkspaceId,
        _render: intent_core::machine_selection::PlacementHelpRenderer,
    ) -> BoxFuture<'a, Option<String>> {
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
            let next = self.launches.lock().unwrap().pop_front();
            match next {
                Some(Launch::Runs(runtime)) => {
                    record_launched_sign_in(svc, agent_id).await;
                    Ok(runtime as Arc<crate::feature_ports::RemoteRuntime>)
                }
                Some(Launch::Reattaches(runtime)) => {
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

/// What the real port records at a fresh launch: whether it runs on the
/// machine's own sign-in.
async fn record_launched_sign_in(svc: &Services, agent_id: &AgentId) {
    let session = svc.store.get_agent_session_summary(agent_id).await.unwrap();
    let provider = session.provider.clone().unwrap_or_default();
    let machine_sign_in = svc.runs_on_machine_sign_in(&session, &provider).await;
    let mut view =
        intent_core::remote_launch::RemotePlacementView::from_metadata(session.metadata.as_ref())
            .unwrap();
    view.launched_machine_sign_in = Some(machine_sign_in);
    svc.store
        .set_agent_session_metadata_key_json(
            &session.workspace_id,
            agent_id,
            intent_core::remote_launch::REMOTE_PLACEMENT_METADATA_KEY,
            &serde_json::to_string(&view).unwrap(),
            &now_iso(),
        )
        .await
        .unwrap();
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
    let session = mgr
        .services
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
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
    let session = mgr
        .services
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
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

/// Mark the Default for remote machines with an override, so a remote
/// launch of plain `claude-code` sends this backend's account.
async fn send_the_default_remotely(mgr: &AgentManager) {
    mgr.services
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "claude-code",
            "env": [{ "key": "CLAUDE_CODE_OAUTH_TOKEN", "kind": "set", "value": "tok-default" }],
            "remoteCompatible": true,
        }))
        .await
        .expect("mark the Default with overrides");
}

/// A remote agent whose configuration the shared quota cache marks out of
/// quota at the start of a turn moves to the backup the same way, even with
/// its previous run still live: failover is the one provider switch a
/// remote agent allows, so the run is replaced instead of the turn being
/// refused as `remote-model-change-unsupported`. The mark applies because a
/// marked Default with overrides sends this backend's account.
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
    send_the_default_remotely(&mgr).await;

    send(&mgr, &id, &ws, "first").await;
    primary.wait_prompts(1).await;
    primary.finish.add_permits(1);
    wait_idle(&mgr, &id).await;
    assert!(mgr.contains(&id), "the first run stays live");

    let now = chrono::Utc::now();
    mgr.services.provider_quota_cache.mark_exhausted(
        &crate::profile_activation::SettingsRead::elsewhere("claude-code"),
        None,
        now,
    );
    send(&mgr, &id, &ws, "second").await;
    backup_run.wait_prompts(1).await;
    backup_run.finish.add_permits(1);
    wait_idle(&mgr, &id).await;

    assert_eq!(primary.stops.load(Ordering::SeqCst), 1, "the old run stops");
    assert_eq!(port.asked.load(Ordering::SeqCst), 2);
    assert_eq!(*port.failover_rows_at_start.lock().unwrap(), vec![0, 0]);
    let session = mgr
        .services
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
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

/// A remote launch of a Default that sends nothing runs on the machine's own
/// sign-in, another account than this backend's: the backend's out-of-quota
/// mark on `claude-code` never moves it (intent-hq/intent#9100). The turn
/// starts on the plain reference, on the same run, with no failover row.
#[intent_test_macros::daemon_test]
async fn a_machine_sign_in_launch_ignores_the_backends_quota_mark() {
    let ws = WorkspaceId::from("ws-remote-failover-machine-sign-in");
    let id = AgentId::from("agent-remote-failover-machine-sign-in");
    let primary = FakeRuntime::new();
    let port = ScriptedRemote::new(vec![Launch::Runs(primary.clone())]);
    let (_tmp, mgr, _backup) = remote_agent_with_backup(port.clone(), &id, &ws).await;
    let plan = mgr
        .services
        .remote_launch_plan("claude-code", crate::RemoteTarget::Machine)
        .await
        .unwrap();
    assert!(plan.machine_sign_in, "{plan:?}");

    mgr.services.provider_quota_cache.mark_exhausted(
        &crate::profile_activation::SettingsRead::elsewhere("claude-code"),
        None,
        chrono::Utc::now(),
    );
    send(&mgr, &id, &ws, "hello").await;
    primary.wait_prompts(1).await;
    primary.finish.add_permits(1);
    wait_idle(&mgr, &id).await;

    assert_eq!(port.asked.load(Ordering::SeqCst), 1);
    assert_eq!(primary.stops.load(Ordering::SeqCst), 0);
    let session = mgr
        .services
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    assert_eq!(session.provider.as_deref(), Some("claude-code"));
    assert_ne!(session.status, AgentStatus::Error);
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .unwrap();
    assert!(failover_rows(&messages).is_empty(), "{messages:#?}");
}

/// A remote agent's real quota failure (the backup starts on its node), then
/// a turn start of a local `claude-code` agent on this backend: whether the
/// local agent moved to the backup, the remote agent's provider, and the
/// backup's reference.
async fn remote_quota_failure_then_local_turn_start(
    ws: &WorkspaceId,
    id: &AgentId,
    send_default: bool,
) -> (bool, String, String) {
    let primary = FakeRuntime::new();
    primary.quota_failures.store(1, Ordering::SeqCst);
    let backup_run = FakeRuntime::new();
    let port = ScriptedRemote::new(vec![
        Launch::Runs(primary.clone()),
        Launch::Runs(backup_run.clone()),
    ]);
    let (_tmp, mgr, backup) = remote_agent_with_backup(port.clone(), id, ws).await;
    if send_default {
        send_the_default_remotely(&mgr).await;
    }
    send(&mgr, id, ws, "hello").await;
    backup_run.wait_prompts(1).await;
    backup_run.finish.add_permits(1);
    wait_idle(&mgr, id).await;
    let remote = mgr
        .services
        .store
        .get_agent_session_without_messages(id)
        .await
        .unwrap();

    let local = AgentId::from(format!("{}-local", id.0));
    let mut local_session = crate::test_support::fixtures::session(&local, ws);
    local_session.provider = Some("claude-code".to_owned());
    local_session.model = Some("opus".to_owned());
    mgr.services
        .store
        .insert_agent_session(&local_session)
        .await
        .unwrap();
    mgr.services.avoid_exhausted_provider(ws, &local).await;
    let local_provider = mgr
        .services
        .store
        .get_agent_session_without_messages(&local)
        .await
        .unwrap()
        .provider
        .unwrap_or_default();
    (
        local_provider == backup,
        remote.provider.unwrap_or_default(),
        backup,
    )
}

/// A remote agent on the machine's own sign-in that really runs out of quota
/// still fails over, but marks nothing on this backend: that account is not
/// this backend's, so a local agent on `claude-code` is not moved
/// (intent-hq/intent#9100).
#[intent_test_macros::daemon_test]
async fn a_machine_sign_in_quota_failure_does_not_mark_the_backends_account() {
    let ws = WorkspaceId::from("ws-remote-quota-no-mark");
    let id = AgentId::from("agent-remote-quota-no-mark");
    let (local_moved, remote_provider, backup) =
        remote_quota_failure_then_local_turn_start(&ws, &id, false).await;

    assert_eq!(remote_provider, backup, "the remote agent still fails over");
    assert!(!local_moved, "the local agent stays on claude-code");
}

/// A marked Default with overrides sends this backend's account, so its
/// quota failure on a remote agent marks it as before, and a local agent on
/// `claude-code` is moved to the backup at its next turn start.
#[intent_test_macros::daemon_test]
async fn a_sent_default_quota_failure_marks_the_backends_account() {
    let ws = WorkspaceId::from("ws-remote-quota-mark");
    let id = AgentId::from("agent-remote-quota-mark");
    let (local_moved, remote_provider, backup) =
        remote_quota_failure_then_local_turn_start(&ws, &id, true).await;

    assert_eq!(remote_provider, backup);
    assert!(local_moved, "the local agent leaves the marked claude-code");
}

/// A remote agent's turn on the machine's own sign-in says nothing about
/// this backend's account, so it never clears that account's quota mark
/// (intent-hq/intent#9100).
#[intent_test_macros::daemon_test]
async fn a_machine_sign_in_turn_does_not_clear_the_backends_mark() {
    let ws = WorkspaceId::from("ws-remote-quota-keep");
    let id = AgentId::from("agent-remote-quota-keep");
    let primary = FakeRuntime::new();
    let port = ScriptedRemote::new(vec![Launch::Runs(primary.clone())]);
    let (_tmp, mgr, _backup) = remote_agent_with_backup(port.clone(), &id, &ws).await;
    mgr.services.provider_quota_cache.mark_exhausted(
        &crate::profile_activation::SettingsRead::elsewhere("claude-code"),
        None,
        chrono::Utc::now(),
    );
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    send(&mgr, &id, &ws, "hello").await;
    primary.wait_prompts(1).await;
    primary.finish.add_permits(1);
    wait_idle(&mgr, &id).await;

    assert_eq!(
        mgr.services.exhausted_providers(),
        vec!["claude-code".to_owned()]
    );
}

/// The Default marked to be sent while a machine sign-in run is live does
/// not change that run's account: its next turn still runs on the machine's
/// own sign-in, so its quota failure does not mark this backend's account.
#[intent_test_macros::daemon_test]
async fn a_live_machine_sign_in_run_keeps_its_account_after_the_default_is_sent() {
    let ws = WorkspaceId::from("ws-remote-quota-live");
    let id = AgentId::from("agent-remote-quota-live");
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

    send_the_default_remotely(&mgr).await;
    primary.quota_failures.store(1, Ordering::SeqCst);
    send(&mgr, &id, &ws, "second").await;
    backup_run.wait_prompts(1).await;
    backup_run.finish.add_permits(1);
    wait_idle(&mgr, &id).await;

    let provider = mgr
        .services
        .store
        .get_agent_session_without_messages(&id)
        .await
        .unwrap()
        .provider
        .unwrap_or_default();
    assert_eq!(provider, backup, "the remote agent still fails over");
    assert!(
        mgr.services.exhausted_providers().is_empty(),
        "{:?}",
        mgr.services.exhausted_providers()
    );
}

/// A run this daemon reattaches to after a restart keeps the account its
/// placement says it launched with, even when the Default is sent now: its
/// quota failure does not mark this backend's account.
#[intent_test_macros::daemon_test]
async fn a_reattached_run_keeps_the_account_it_launched_with() {
    let ws = WorkspaceId::from("ws-remote-quota-reattach");
    let id = AgentId::from("agent-remote-quota-reattach");
    let primary = FakeRuntime::new();
    primary.quota_failures.store(1, Ordering::SeqCst);
    let backup_run = FakeRuntime::new();
    let port = ScriptedRemote::new(vec![
        Launch::Reattaches(primary.clone()),
        Launch::Runs(backup_run.clone()),
    ]);
    let (_tmp, mgr, _backup) = remote_agent_with_backup(port.clone(), &id, &ws).await;
    let view = intent_core::remote_launch::RemotePlacementView {
        launched_machine_sign_in: Some(true),
        ..remote_view()
    };
    mgr.services
        .store
        .set_agent_session_metadata_key_json(
            &ws,
            &id,
            intent_core::remote_launch::REMOTE_PLACEMENT_METADATA_KEY,
            &serde_json::to_string(&view).unwrap(),
            &now_iso(),
        )
        .await
        .unwrap();
    send_the_default_remotely(&mgr).await;
    send(&mgr, &id, &ws, "hello").await;
    backup_run.wait_prompts(1).await;
    backup_run.finish.add_permits(1);
    wait_idle(&mgr, &id).await;

    assert!(
        mgr.services.exhausted_providers().is_empty(),
        "{:?}",
        mgr.services.exhausted_providers()
    );
}

/// A run reattached after a restart with no recorded sign-in (launched by
/// an older daemon) runs on an unknown account: it neither marks nor
/// clears this backend's account, whatever the settings say now.
#[intent_test_macros::daemon_test]
async fn a_reattached_run_of_unknown_sign_in_leaves_the_backends_marks_alone() {
    let ws = WorkspaceId::from("ws-remote-quota-unknown");
    let id = AgentId::from("agent-remote-quota-unknown");
    let primary = FakeRuntime::new();
    primary.quota_failures.store(1, Ordering::SeqCst);
    let backup_run = FakeRuntime::new();
    let port = ScriptedRemote::new(vec![
        Launch::Reattaches(primary.clone()),
        Launch::Runs(backup_run.clone()),
    ]);
    let (_tmp, mgr, backup) = remote_agent_with_backup(port.clone(), &id, &ws).await;
    send_the_default_remotely(&mgr).await;
    send(&mgr, &id, &ws, "hello").await;
    backup_run.wait_prompts(1).await;
    backup_run.finish.add_permits(1);
    wait_idle(&mgr, &id).await;

    let provider = mgr
        .services
        .store
        .get_agent_session_without_messages(&id)
        .await
        .unwrap()
        .provider
        .unwrap_or_default();
    assert_eq!(provider, backup, "the remote agent still fails over");
    assert!(
        mgr.services.exhausted_providers().is_empty(),
        "{:?}",
        mgr.services.exhausted_providers()
    );
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
    let session = mgr
        .services
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    assert_eq!(session.provider.as_deref(), Some("claude-code"));
    assert_eq!(session.status, AgentStatus::Error);
    let reason = session.stop_reason.unwrap_or_default();
    for needle in [
        "ran out of quota on Mac",
        "Backup",
        "can't run there",
        "uses an account folder (CLAUDE_CONFIG_DIR) that only exists on this backend",
        "marked for remote machines",
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

/// A backup the user has not marked for remote machines is passed over the
/// same way: the agent stays on its configuration and the message says how
/// to mark it.
#[intent_test_macros::daemon_test]
async fn a_remote_agent_never_fails_over_to_an_unmarked_backup() {
    let ws = WorkspaceId::from("ws-remote-failover-unmarked");
    let id = AgentId::from("agent-remote-failover-unmarked");
    let primary = FakeRuntime::new();
    primary.quota_failures.store(1, Ordering::SeqCst);
    let port = ScriptedRemote::new(vec![Launch::Runs(primary.clone())]);
    let (_tmp, mgr, backup) = remote_agent_with_backup_env(
        port.clone(),
        &id,
        &ws,
        json!([{ "key": "ANTHROPIC_API_KEY", "kind": "set", "value": "sk-backup" }]),
    )
    .await;
    let profile_id = backup.split_once('@').unwrap().1.to_owned();
    mgr.services
        .provider_profiles_update_impl(profile_id, json!({ "remoteCompatible": false }))
        .await
        .expect("unmark the backup");

    send(&mgr, &id, &ws, "hello").await;
    wait_idle(&mgr, &id).await;

    assert_eq!(
        port.asked.load(Ordering::SeqCst),
        1,
        "the backup is never started"
    );
    let session = mgr
        .services
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    assert_eq!(session.provider.as_deref(), Some("claude-code"));
    let reason = session.stop_reason.unwrap_or_default();
    for needle in [
        "ran out of quota on Mac",
        "Backup isn't marked for remote machines",
    ] {
        assert!(
            reason.contains(needle),
            "{needle:?} missing from {reason:?}"
        );
    }
    assert!(!reason.contains("sk-backup"), "{reason}");
}

/// Make the agent's recorded return home (after a failover) due now, with
/// home's quota reset (the failover marked it exhausted for everyone).
async fn make_return_due(mgr: &AgentManager, id: &AgentId, ws: &WorkspaceId) {
    let later = chrono::Utc::now() + chrono::Duration::seconds(1);
    mgr.services.provider_quota_cache.mark_working(
        &crate::profile_activation::SettingsRead::elsewhere("claude-code"),
        later,
        later,
    );
    let session = mgr
        .services
        .store
        .get_agent_session_with_transcript(id)
        .await
        .unwrap();
    let mut state = crate::provider_return::read_state(&session).expect("away after failover");
    state.return_after = (chrono::Utc::now() - chrono::Duration::minutes(1))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    mgr.services
        .store
        .set_agent_session_metadata_key_json(
            ws,
            id,
            crate::provider_return::PROVIDER_RETURN_METADATA_KEY,
            &json!(state).to_string(),
            &now_iso(),
        )
        .await
        .unwrap();
}

async fn send_as_user(mgr: &Arc<AgentManager>, id: &AgentId, ws: &WorkspaceId, text: &str) {
    mgr.send_message(
        id.clone(),
        ws.clone(),
        text.into(),
        None,
        super::super::super::TurnOptions {
            origin: intent_core::MessageOrigin::User,
            ..Default::default()
        },
    )
    .await
    .unwrap();
}

/// Fail over onto the backup (live afterwards), then make the return due.
async fn failed_over_with_return_due(
    port: Arc<ScriptedRemote>,
    primary: &Arc<FakeRuntime>,
    backup_run: &Arc<FakeRuntime>,
    id: &AgentId,
    ws: &WorkspaceId,
) -> (TempDb, Arc<AgentManager>, String) {
    primary.quota_failures.store(1, Ordering::SeqCst);
    let (tmp, mgr, backup) = remote_agent_with_backup(port, id, ws).await;
    send(&mgr, id, ws, "hello").await;
    backup_run.wait_prompts(1).await;
    backup_run.finish.add_permits(1);
    wait_idle(&mgr, id).await;
    assert!(mgr.contains(id), "the backup run stays live");
    make_return_due(&mgr, id, ws).await;
    (tmp, mgr, backup)
}

/// Review of intentd#2957: a remote agent's due return home replaces its
/// live backup run with one on home instead of refusing the turn as
/// `remote-model-change-unsupported`.
#[intent_test_macros::daemon_test]
async fn a_remote_agents_due_return_home_replaces_the_live_backup_run() {
    let ws = WorkspaceId::from("ws-remote-return-ok");
    let id = AgentId::from("agent-remote-return-ok");
    let primary = FakeRuntime::new();
    let backup_run = FakeRuntime::new();
    let home_run = FakeRuntime::new();
    let port = ScriptedRemote::new(vec![
        Launch::Runs(primary.clone()),
        Launch::Runs(backup_run.clone()),
        Launch::Runs(home_run.clone()),
    ]);
    let (_tmp, mgr, _backup) =
        failed_over_with_return_due(port.clone(), &primary, &backup_run, &id, &ws).await;

    send_as_user(&mgr, &id, &ws, "back home").await;
    home_run.wait_prompts(1).await;
    home_run.finish.add_permits(1);
    wait_idle(&mgr, &id).await;

    assert_eq!(port.asked.load(Ordering::SeqCst), 3);
    assert_eq!(
        backup_run.stops.load(Ordering::SeqCst),
        1,
        "the backup run stops"
    );
    let session = mgr
        .services
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    assert_eq!(session.provider.as_deref(), Some("claude-code"));
    assert_ne!(
        session.status,
        AgentStatus::Error,
        "{:?}",
        session.stop_reason
    );
}

/// A due return home the node cannot start leaves the agent on the backup,
/// with the return backed off, and fails the turn saying so.
#[intent_test_macros::daemon_test]
async fn a_remote_return_home_the_node_cannot_start_stays_on_the_backup() {
    let ws = WorkspaceId::from("ws-remote-return-ko");
    let id = AgentId::from("agent-remote-return-ko");
    let primary = FakeRuntime::new();
    let backup_run = FakeRuntime::new();
    let backup_again = FakeRuntime::new();
    let port = ScriptedRemote::new(vec![
        Launch::Runs(primary.clone()),
        Launch::Runs(backup_run.clone()),
        Launch::Fails("the provider is not signed in on this machine"),
        Launch::Runs(backup_again.clone()),
    ]);
    let (_tmp, mgr, backup) =
        failed_over_with_return_due(port.clone(), &primary, &backup_run, &id, &ws).await;

    send_as_user(&mgr, &id, &ws, "back home").await;
    wait_idle(&mgr, &id).await;

    assert_eq!(port.asked.load(Ordering::SeqCst), 3, "no silent retry");
    let session = mgr
        .services
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    assert_eq!(session.provider.as_deref(), Some(backup.as_str()));
    assert_eq!(session.status, AgentStatus::Error);
    let state = crate::provider_return::read_state(&session).expect("still away");
    assert_eq!(state.backoff_step, 1, "the failed return backs off");
    assert!(state.returning.is_none());
    let reason = session.stop_reason.unwrap_or_default();
    for needle in [
        "couldn't start on Mac when going back",
        "not signed in on this machine",
        "The agent stays on",
    ] {
        assert!(
            reason.contains(needle),
            "{needle:?} missing from {reason:?}"
        );
    }

    // Review of intentd#2957: the next message continues on the backup
    // (home's earlier output no longer skips the backoff).
    send_as_user(&mgr, &id, &ws, "carry on").await;
    // The backup also takes the undelivered "back home" message, so it runs
    // two turns.
    backup_again.finish.add_permits(2);
    backup_again.wait_prompts(2).await;
    wait_idle(&mgr, &id).await;
    assert_eq!(port.asked.load(Ordering::SeqCst), 4, "home is not retried");
    let delivered = format!("{:?}", backup_again.prompts.lock().unwrap());
    assert!(delivered.contains("carry on"), "{delivered}");
    let session = mgr
        .services
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    assert_eq!(session.provider.as_deref(), Some(backup.as_str()));
    let state = crate::provider_return::read_state(&session).expect("still away");
    assert_eq!(
        state.backoff_step, 1,
        "home is not retried before its backoff"
    );
}

/// Review of intentd#2957: in production the node starts the agent
/// asynchronously (`runtime()` returns before the provider is up), so a
/// backup the node cannot start surfaces as the first prompt failing before
/// any output. That still undoes the switch and fails with what to do,
/// without a silent redrive or a failover row.
#[intent_test_macros::daemon_test]
async fn a_backup_that_exits_before_starting_on_the_node_undoes_the_switch() {
    let ws = WorkspaceId::from("ws-remote-failover-exit");
    let id = AgentId::from("agent-remote-failover-exit");
    let primary = FakeRuntime::new();
    primary.quota_failures.store(1, Ordering::SeqCst);
    let backup_run = FakeRuntime::new();
    *backup_run.exited.lock().unwrap() =
        Some("provider-start-failed: Claude Code is not signed in".into());
    let port = ScriptedRemote::new(vec![
        Launch::Runs(primary.clone()),
        Launch::Runs(backup_run.clone()),
    ]);
    let (_tmp, mgr, _backup) = remote_agent_with_backup(port.clone(), &id, &ws).await;

    send(&mgr, &id, &ws, "hello").await;
    wait_idle(&mgr, &id).await;

    assert_eq!(port.asked.load(Ordering::SeqCst), 2, "no silent redrive");
    assert_eq!(backup_run.prompts.lock().unwrap().len(), 1);
    let session = mgr
        .services
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    assert_eq!(session.provider.as_deref(), Some("claude-code"));
    assert_eq!(session.status, AgentStatus::Error);
    assert!(crate::provider_return::read_state(&session).is_none());
    let reason = session.stop_reason.unwrap_or_default();
    for needle in [
        "ran out of quota on Mac",
        "couldn't start there",
        "Claude Code is not signed in",
        "Sign in to",
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

/// A usable backup whose provider did not finish `initialize` in time on
/// the machine (it had just woken) fails with that cause, not a sign-in
/// request.
#[intent_test_macros::daemon_test]
async fn a_backup_whose_provider_times_out_on_the_node_does_not_ask_for_a_sign_in() {
    let ws = WorkspaceId::from("ws-remote-failover-timeout");
    let id = AgentId::from("agent-remote-failover-timeout");
    let primary = FakeRuntime::new();
    primary.quota_failures.store(1, Ordering::SeqCst);
    let backup_run = FakeRuntime::new();
    *backup_run.exited.lock().unwrap() = Some(
        "the provider did not start (node-agent-start-failed: request `initialize` timed out)"
            .into(),
    );
    let port = ScriptedRemote::new(vec![
        Launch::Runs(primary.clone()),
        Launch::Runs(backup_run.clone()),
    ]);
    let (_tmp, mgr, _backup) = remote_agent_with_backup(port.clone(), &id, &ws).await;

    send(&mgr, &id, &ws, "hello").await;
    wait_idle(&mgr, &id).await;

    let session = mgr
        .services
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    assert_eq!(session.provider.as_deref(), Some("claude-code"));
    assert_eq!(session.status, AgentStatus::Error);
    let reason = session.stop_reason.unwrap_or_default();
    for needle in [
        "ran out of quota on Mac",
        "request `initialize` timed out",
        "did not respond in time",
        "Retry the message",
    ] {
        assert!(
            reason.contains(needle),
            "{needle:?} missing from {reason:?}"
        );
    }
    assert!(!reason.contains("Sign in"), "{reason}");
}

/// Node "node-1" exposed with `claude-code` signed in and `models` listed.
async fn expose_claude_node(mgr: &AgentManager, models: &[&str]) {
    use intent_core::node_link::{
        MachineCapabilities, MachineCapacity, ProviderCapability, ProviderStatus,
    };
    let platform = intent_core::placement::AgentPlatform {
        os: intent_core::nodes::NodeOs::Macos,
        arch: intent_core::nodes::NodeArch::Aarch64,
    };
    let observed_at = now_iso();
    mgr.services
        .store
        .save_exposed_machine(&intent_core::machines::ExposedMachine {
            id: "node-1".into(),
            name: "Mac".into(),
            hosts: vec!["10.0.0.1".into()],
            port: 7443,
            tc_address: None,
            fingerprint: "ab".repeat(32),
            credential_id: intent_core::ExecutorCredentialId::from("cred-1".to_string()),
            platform: Some(platform),
            capabilities: Some(MachineCapabilities {
                platform,
                intentd_version: "0.11.87".into(),
                capacity: MachineCapacity {
                    running_agents: 1,
                    agent_cap: 4,
                    charged_memory_bytes: 0,
                    memory_budget_bytes: None,
                },
                providers: vec![ProviderCapability {
                    id: "claude-code".into(),
                    visible: true,
                    status: ProviderStatus::SignedIn,
                    models: models.iter().map(|m| (*m).to_owned()).collect(),
                    models_stale: false,
                }],
                disabled_providers: vec![],
                observed_at: observed_at.clone(),
                homes: None,
            }),
            observed_at: Some(observed_at),
            state: intent_core::machines::RemoteMachineState::Disconnected,
            added_at: now_iso(),
            last_connected_at: None,
            node_protocol: None,
            cloud: None,
        })
        .await
        .unwrap();
}

/// A model switch the user makes on a running remote agent (§5.54 "switching
/// a remote agent's model"): the next turn stops the run on the old model
/// and relaunches the agent on the same machine with the new one, and that
/// fresh provider session receives the Intent history first, as a local
/// cross-session switch does.
#[intent_test_macros::daemon_test]
async fn a_remote_agents_model_switch_relaunches_it_on_its_machine_with_history() {
    let ws = WorkspaceId::from("ws-remote-switch");
    let id = AgentId::from("agent-remote-switch");
    let first = FakeRuntime::new();
    let second = FakeRuntime::new();
    let port = ScriptedRemote::new(vec![
        Launch::Runs(first.clone()),
        Launch::Runs(second.clone()),
    ]);
    let (_tmp, mgr, _backup) = remote_agent_with_backup(port.clone(), &id, &ws).await;
    expose_claude_node(&mgr, &["opus", "sonnet"]).await;

    send(&mgr, &id, &ws, "remember the word kumquat").await;
    first.wait_prompts(1).await;
    first.finish.add_permits(1);
    wait_idle(&mgr, &id).await;

    mgr.services
        .agent_set_model_op(id.clone(), "sonnet".into(), None)
        .await
        .expect("the machine lists the model");

    send(&mgr, &id, &ws, "what was the word?").await;
    second.wait_prompts(1).await;
    second.finish.add_permits(1);
    wait_idle(&mgr, &id).await;

    assert_eq!(port.asked.load(Ordering::SeqCst), 2, "relaunched once");
    assert_eq!(
        first.stops.load(Ordering::SeqCst),
        1,
        "the old run is stopped"
    );
    assert_eq!(first.prompts.lock().unwrap().len(), 1);
    let replayed = serde_json::to_string(&second.prompts.lock().unwrap()[0].1).unwrap();
    assert!(replayed.contains("kumquat"), "history replayed: {replayed}");
    assert!(replayed.contains("what was the word?"), "{replayed}");
    let session = mgr
        .services
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    assert_eq!(session.model.as_deref(), Some("sonnet"));
    assert_ne!(session.status, AgentStatus::Error);
}

/// The relaunch after a switch fails on the node and the turn's start is
/// retried: the switch stays pending through the failure, so the retried
/// run still opens with the Intent history (review of intentd#2961).
#[intent_test_macros::daemon_test]
async fn a_switch_whose_relaunch_fails_still_replays_history_on_the_retry() {
    let _env = crate::test_support::fixtures::EnvGuard::apply(&[(
        "INTENTD_SPAWN_RETRY_BACKOFF_MS",
        Some("0,0"),
    )]);
    let ws = WorkspaceId::from("ws-remote-switch-retry");
    let id = AgentId::from("agent-remote-switch-retry");
    let first = FakeRuntime::new();
    let second = FakeRuntime::new();
    let port = ScriptedRemote::new(vec![
        Launch::Runs(first.clone()),
        Launch::Fails("the node launch timed out"),
        Launch::Runs(second.clone()),
    ]);
    let (_tmp, mgr, _backup) = remote_agent_with_backup(port.clone(), &id, &ws).await;
    expose_claude_node(&mgr, &["opus", "sonnet"]).await;

    send(&mgr, &id, &ws, "remember the word kumquat").await;
    first.wait_prompts(1).await;
    first.finish.add_permits(1);
    wait_idle(&mgr, &id).await;

    mgr.services
        .agent_set_model_op(id.clone(), "sonnet".into(), None)
        .await
        .unwrap();
    send(&mgr, &id, &ws, "what was the word?").await;
    second.wait_prompts(1).await;
    second.finish.add_permits(1);
    wait_idle(&mgr, &id).await;

    assert_eq!(
        port.asked.load(Ordering::SeqCst),
        3,
        "failed once, then started"
    );
    assert_eq!(
        first.stops.load(Ordering::SeqCst),
        1,
        "the old run is stopped"
    );
    let replayed = serde_json::to_string(&second.prompts.lock().unwrap()[0].1).unwrap();
    assert!(replayed.contains("kumquat"), "history replayed: {replayed}");
    assert!(replayed.contains("what was the word?"), "{replayed}");
}

/// A switch made while the agent has no run (stopped): its next start
/// launches on the new model and still opens with the Intent history.
#[intent_test_macros::daemon_test]
async fn a_switch_of_a_stopped_remote_agent_replays_history_at_its_next_start() {
    let ws = WorkspaceId::from("ws-remote-switch-stopped");
    let id = AgentId::from("agent-remote-switch-stopped");
    let first = FakeRuntime::new();
    let second = FakeRuntime::new();
    let port = ScriptedRemote::new(vec![
        Launch::Runs(first.clone()),
        Launch::Runs(second.clone()),
    ]);
    let (_tmp, mgr, _backup) = remote_agent_with_backup(port.clone(), &id, &ws).await;
    expose_claude_node(&mgr, &["opus", "sonnet"]).await;

    send(&mgr, &id, &ws, "remember the word kumquat").await;
    first.wait_prompts(1).await;
    first.finish.add_permits(1);
    wait_idle(&mgr, &id).await;
    mgr.stop(&id).await;

    mgr.services
        .agent_set_model_op(id.clone(), "sonnet".into(), None)
        .await
        .unwrap();
    send(&mgr, &id, &ws, "what was the word?").await;
    second.wait_prompts(1).await;
    second.finish.add_permits(1);
    wait_idle(&mgr, &id).await;

    assert_eq!(port.asked.load(Ordering::SeqCst), 2);
    let replayed = serde_json::to_string(&second.prompts.lock().unwrap()[0].1).unwrap();
    assert!(replayed.contains("kumquat"), "history replayed: {replayed}");
    let session = mgr
        .services
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    assert_eq!(session.model.as_deref(), Some("sonnet"));
}

/// In production the node starts the agent asynchronously: `runtime()`
/// returns at once, and a run that exits before starting only surfaces as
/// its first prompt failing, after which the turn is driven again on a
/// fresh launch. The switch stays pending until the new run produces output,
/// so that launch still opens with the Intent history (review of
/// intentd#2961).
#[intent_test_macros::daemon_test]
async fn a_switched_run_that_exits_before_starting_still_replays_history_next_time() {
    let ws = WorkspaceId::from("ws-remote-switch-exit");
    let id = AgentId::from("agent-remote-switch-exit");
    let first = FakeRuntime::new();
    let dead = FakeRuntime::new();
    *dead.exited.lock().unwrap() = Some("provider-start-failed: the node is restarting".into());
    let second = FakeRuntime::new();
    let port = ScriptedRemote::new(vec![
        Launch::Runs(first.clone()),
        Launch::Runs(dead.clone()),
        Launch::Runs(second.clone()),
    ]);
    let (_tmp, mgr, _backup) = remote_agent_with_backup(port.clone(), &id, &ws).await;
    expose_claude_node(&mgr, &["opus", "sonnet"]).await;

    send(&mgr, &id, &ws, "remember the word kumquat").await;
    first.wait_prompts(1).await;
    first.finish.add_permits(1);
    wait_idle(&mgr, &id).await;

    mgr.services
        .agent_set_model_op(id.clone(), "sonnet".into(), None)
        .await
        .unwrap();
    send(&mgr, &id, &ws, "what was the word?").await;
    // The dead run fails the prompt before any output; the turn is driven
    // again on a fresh launch.
    second.wait_prompts(1).await;
    second.finish.add_permits(1);
    wait_idle(&mgr, &id).await;

    assert_eq!(
        dead.prompts.lock().unwrap().len(),
        1,
        "the switched run failed first"
    );
    assert_eq!(port.asked.load(Ordering::SeqCst), 3);
    let replayed = serde_json::to_string(&second.prompts.lock().unwrap()[0].1).unwrap();
    assert!(replayed.contains("kumquat"), "history replayed: {replayed}");
    assert!(replayed.contains("what was the word?"), "{replayed}");
    let session = mgr
        .services
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    assert_eq!(session.model.as_deref(), Some("sonnet"));
}

/// A switch made while the old run's turn is still in flight: that run's
/// later output and completed turn must not confirm the switch, so the next
/// turn still relaunches the agent on the new model with the history
/// (review of intentd#2961).
#[intent_test_macros::daemon_test]
async fn a_switch_during_a_running_turn_is_not_confirmed_by_the_old_run() {
    let ws = WorkspaceId::from("ws-remote-switch-midturn");
    let id = AgentId::from("agent-remote-switch-midturn");
    let first = FakeRuntime::new();
    let second = FakeRuntime::new();
    let port = ScriptedRemote::new(vec![
        Launch::Runs(first.clone()),
        Launch::Runs(second.clone()),
    ]);
    let (_tmp, mgr, _backup) = remote_agent_with_backup(port.clone(), &id, &ws).await;
    expose_claude_node(&mgr, &["opus", "sonnet"]).await;

    send(&mgr, &id, &ws, "remember the word kumquat").await;
    first.wait_prompts(1).await;
    mgr.services
        .agent_set_model_op(id.clone(), "sonnet".into(), None)
        .await
        .expect("accepted while the old turn runs");
    // The old run keeps producing output, then completes its turn.
    let session_id = first.prompts.lock().unwrap()[0].0.clone();
    first
        .updates
        .send(IncomingNotification {
            method: "session/update".into(),
            params: json!({"sessionId": session_id, "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "still on the old model"}
            }}),
        })
        .unwrap();
    first.finish.add_permits(1);
    wait_idle(&mgr, &id).await;

    send(&mgr, &id, &ws, "what was the word?").await;
    second.wait_prompts(1).await;
    second.finish.add_permits(1);
    wait_idle(&mgr, &id).await;

    assert_eq!(port.asked.load(Ordering::SeqCst), 2, "relaunched once");
    assert_eq!(
        first.stops.load(Ordering::SeqCst),
        1,
        "the old run is stopped"
    );
    let replayed = serde_json::to_string(&second.prompts.lock().unwrap()[0].1).unwrap();
    assert!(replayed.contains("kumquat"), "history replayed: {replayed}");
    assert!(replayed.contains("what was the word?"), "{replayed}");
    let session = mgr
        .services
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    assert_eq!(session.model.as_deref(), Some("sonnet"));
    assert_ne!(session.status, AgentStatus::Error);
}

/// intent-hq/intent#8383: a remote start failure after a failover (here the
/// machine could not fetch the agent's code) leaves the failed message at
/// the front of the queue for `agent.retry` or the next send, and nothing
/// starts the agent again on its own: no redrive loop. The failure names
/// its cause instead of asking for a sign-in.
#[intent_test_macros::daemon_test]
async fn a_remote_start_failure_is_requeued_but_never_redriven_on_its_own() {
    let ws = WorkspaceId::from("ws-remote-failover-no-loop");
    let id = AgentId::from("agent-remote-failover-no-loop");
    let primary = FakeRuntime::new();
    primary.quota_failures.store(1, Ordering::SeqCst);
    let backup_run = FakeRuntime::new();
    *backup_run.exited.lock().unwrap() = Some(
        "node-agent-not-running: node-checkout-failed: fetch the agent's base commit from \
         head: head refused the session (node-git-unavailable)"
            .into(),
    );
    let port = ScriptedRemote::new(vec![
        Launch::Runs(primary.clone()),
        Launch::Runs(backup_run.clone()),
    ]);
    let (_tmp, mgr, _backup) = remote_agent_with_backup(port.clone(), &id, &ws).await;

    send(&mgr, &id, &ws, "hello").await;
    wait_idle(&mgr, &id).await;
    // timing-guard: a redrive would start the agent again well within this
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    wait_idle(&mgr, &id).await;

    assert_eq!(port.asked.load(Ordering::SeqCst), 2, "no automatic redrive");
    let session = mgr
        .services
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    assert_eq!(session.status, AgentStatus::Error);
    let reason = session.stop_reason.unwrap_or_default();
    assert!(reason.contains("node-checkout-failed"), "{reason}");
    assert!(reason.contains("not a sign-in problem"), "{reason}");
    assert!(!reason.contains("Sign in"), "{reason}");
    let queue = mgr.services.queue_snapshot(&id);
    assert_eq!(queue.len(), 1, "{queue:?}");
    assert_eq!(queue[0]["requeuedAfterFailure"], json!(true), "{queue:?}");
}

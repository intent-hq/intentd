use std::sync::{Arc, Mutex};

use base64::Engine as _;
use intent_core::{
    with_caller, AgentId, AgentLite, AgentStatus, Caller, Error, HostRole, PrincipalId, Workspace,
    WorkspaceAttention, WorkspaceDisplayStatus, WorkspaceId,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;

use super::state::{
    build_state, choose_notice, idle_alert_allowed, transition_notice, Facts, IdleAgent,
    NoticeKind, Pending, DEBOUNCE,
};
use super::{proof::DaemonKey, subject_for};
use crate::test_support::capability_fixtures::host_role::fixture;
use crate::test_support::fixtures::{session, TempDb};
use crate::Services;

#[test]
fn proof_matches_the_shared_golden_vector() {
    let v: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/push-relay-daemon-proof.json"
    ))
    .unwrap();
    let s = |k: &str| v[k].as_str().unwrap();
    let seed = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s("seed"))
        .unwrap();
    let key = DaemonKey::from_seed(&seed).unwrap();
    assert_eq!(key.public_key(), s("publicKey"));
    assert_eq!(key.daemon_id(), s("daemonId"));
    let jws = key.proof(
        s("method"),
        s("path"),
        s("body").as_bytes(),
        v["iat"].as_i64().unwrap(),
        s("jti"),
    );
    assert_eq!(jws, s("jws"));
}

#[test]
fn pending_coalesces_changes_into_one_flush_per_window() {
    let mut pending = Pending::default();
    let ws = WorkspaceId::new();
    let t0 = Instant::now();
    let idle = |id: &str| IdleAgent {
        id: AgentId::from(id),
        name: None,
    };
    pending.note(ws.clone(), t0, Some(idle("a")));
    pending.note(ws.clone(), t0 + DEBOUNCE / 2, None);
    pending.note(ws.clone(), t0 + DEBOUNCE / 2, Some(idle("b")));
    pending.note(ws.clone(), t0 + DEBOUNCE / 2, Some(idle("a")));
    assert_eq!(
        pending.next_due(),
        Some(t0 + DEBOUNCE),
        "later changes do not push the window"
    );
    assert!(pending.take_due(t0 + DEBOUNCE / 2).is_empty());
    let due = pending.take_due(t0 + DEBOUNCE);
    assert_eq!(due.len(), 1);
    let ids: Vec<_> = due[0].1.iter().map(|a| a.id.0.as_str()).collect();
    assert_eq!(ids, ["b", "a"], "one entry per agent, latest last");
    assert_eq!(pending.next_due(), None);

    pending.note(ws, t0, None);
    pending.clear();
    assert!(
        pending.take_due(t0 + DEBOUNCE).is_empty(),
        "offline drops queued work"
    );
}

#[test]
fn transitions_notify_once_and_never_on_the_baseline() {
    let facts = |attention, display_status| Facts {
        attention,
        display_status,
    };
    let idle = facts(WorkspaceAttention::None, Some(WorkspaceDisplayStatus::Idle));
    let blocked = facts(
        WorkspaceAttention::None,
        Some(WorkspaceDisplayStatus::Blocked),
    );
    let needs = facts(
        WorkspaceAttention::None,
        Some(WorkspaceDisplayStatus::NeedsAttention),
    );
    let review = facts(
        WorkspaceAttention::ReviewRequired,
        Some(WorkspaceDisplayStatus::NeedsAttention),
    );
    assert_eq!(transition_notice(None, blocked), None);
    assert_eq!(
        transition_notice(Some(idle), blocked),
        Some(NoticeKind::NeedsAttention)
    );
    assert_eq!(
        transition_notice(Some(idle), needs),
        Some(NoticeKind::NeedsAttention)
    );
    assert_eq!(transition_notice(Some(blocked), needs), None);
    assert_eq!(
        transition_notice(Some(idle), review),
        Some(NoticeKind::ReviewRequired)
    );
    assert_eq!(
        transition_notice(Some(needs), review),
        Some(NoticeKind::ReviewRequired)
    );
    assert_eq!(transition_notice(Some(review), review), None);
}

fn agents<'a>(svc: &'a Services, ws: &WorkspaceId) -> impl Fn(&str) -> AgentLite + 'a {
    let ws = ws.clone();
    move |id| svc.project_lite_with_flags(session(&AgentId::from(id), &ws))
}

#[tokio::test]
async fn idle_alerts_follow_the_app_suppression_rules() {
    let tmp = TempDb::new();
    let (svc, _, _) = fixture(&tmp).await;
    let ws = WorkspaceId::new();
    let agent = agents(&svc, &ws);
    let done = AgentId::from("done");
    let running = |id: &str, muted: bool| {
        let mut a = agent(id);
        a.status = AgentStatus::Active;
        a.notifications_muted = muted;
        a
    };

    assert!(idle_alert_allowed(&done, &[agent("done"), agent("other")]));
    assert!(idle_alert_allowed(&done, &[]), "a missing row fails open");

    let mut background = agent("done");
    background.metadata.is_background = true;
    let mut muted = agent("done");
    muted.notifications_muted = true;
    let mut waiting = agent("done");
    waiting.waiting_for_agent_ids = vec![AgentId::from("child")];
    for row in [background, muted, waiting] {
        assert!(!idle_alert_allowed(&done, &[row]));
    }

    assert!(!idle_alert_allowed(
        &done,
        &[agent("done"), running("other", false)]
    ));
    assert!(idle_alert_allowed(
        &done,
        &[agent("done"), running("other", true)]
    ));
    let mut responding = agent("other");
    responding.is_responding = true;
    assert!(!idle_alert_allowed(&done, &[agent("done"), responding]));

    let idle = [IdleAgent {
        id: done.clone(),
        name: None,
    }];
    let (kind, who) = choose_notice(None, &idle, &[agent("done")]).unwrap();
    assert_eq!(kind, NoticeKind::Done);
    assert_eq!(who.unwrap().name.as_deref(), Some("test-done"));
    assert_eq!(
        choose_notice(Some(NoticeKind::ReviewRequired), &idle, &[agent("done")]).map(|n| n.0),
        Some(NoticeKind::ReviewRequired),
        "a transition wins over a completion"
    );
    assert_eq!(
        choose_notice(None, &idle, &[agent("done"), running("other", false)]),
        None
    );
}

#[tokio::test]
async fn state_names_the_active_foreground_agent_and_hides_empty_values() {
    let tmp = TempDb::new();
    let (svc, _, _) = fixture(&tmp).await;
    let ws = Workspace {
        title: "  ".into(),
        branch: "feat/x".into(),
        status_message: Some(" ".into()),
        attention: WorkspaceAttention::None,
        display_status: Some(WorkspaceDisplayStatus::InProgress),
        last_activity: None,
        updated_at: "2026-11-12T17:00:00Z".into(),
        ..Default::default()
    };
    let agent = agents(&svc, &ws.id);
    let mut background = agent("bg");
    background.status = AgentStatus::Active;
    background.metadata.is_background = true;
    background.last_activity = Some("2026-11-12T18:00:00Z".into());
    let mut fg = agent("fg");
    fg.status = AgentStatus::Processing;
    let state = build_state(&ws, &[background, fg, agent("idle")], "now");
    assert_eq!(state.title, "feat/x");
    assert_eq!(state.status_message, None);
    assert_eq!(state.attention, None);
    assert_eq!(state.display_status.as_deref(), Some("in_progress"));
    assert_eq!(state.agent_id.as_deref(), Some("fg"));
    assert_eq!(state.last_activity, "2026-11-12T17:00:00Z");
}

/// A local stand-in for the relay: records each request after checking its
/// proof binds the method and path, and answers from a script.
#[derive(Clone, Default)]
struct FakeRelay {
    seen: Arc<Mutex<Vec<(String, String, Value)>>>,
    delete_status: Arc<Mutex<u16>>,
    forget_daemon: Arc<Mutex<bool>>,
}

impl FakeRelay {
    async fn start() -> (Self, String) {
        let relay = Self::default();
        *relay.delete_status.lock().unwrap() = 204;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let handle = relay.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(handle.clone().serve(stream));
            }
        });
        (relay, url)
    }

    async fn serve(self, mut stream: tokio::net::TcpStream) {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let head_end = loop {
            let n = stream.read(&mut chunk).await.unwrap_or(0);
            if n == 0 {
                return;
            }
            buf.extend_from_slice(&chunk[..n]);
            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break i + 4;
            }
        };
        let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
        let header = |name: &str| {
            head.lines()
                .find_map(|l| {
                    l.split_once(':')
                        .filter(|(k, _)| k.eq_ignore_ascii_case(name))
                })
                .map(|(_, v)| v.trim().to_string())
        };
        let length: usize = header("content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        while buf.len() < head_end + length {
            let n = stream.read(&mut chunk).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        let mut line = head.lines().next().unwrap_or("").split(' ');
        let method = line.next().unwrap_or("").to_string();
        let path = line.next().unwrap_or("").to_string();
        let body: Value = serde_json::from_slice(&buf[head_end..]).unwrap_or(Value::Null);
        let jws = header("authorization").unwrap_or_default();
        let payload = jws
            .strip_prefix("IntentProof ")
            .and_then(|j| j.split('.').nth(1))
            .and_then(|p| {
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(p)
                    .ok()
            })
            .and_then(|p| serde_json::from_slice::<Value>(&p).ok())
            .unwrap_or_default();
        assert_eq!(payload["htm"], method.as_str());
        assert_eq!(payload["htu"], path.as_str());
        self.seen
            .lock()
            .unwrap()
            .push((method.clone(), path.clone(), body));
        let forget =
            path != "/v1/daemons" && std::mem::take(&mut *self.forget_daemon.lock().unwrap());
        let (status, reply) = match method.as_str() {
            _ if forget => (
                401,
                json!({"error": "unknown daemon", "code": "unknown_signer"}).to_string(),
            ),
            "POST" if path == "/v1/daemons" => {
                (200, json!({"daemonId": payload["sub"]}).to_string())
            }
            "POST" => (
                200,
                json!({"linkToken": "token-1", "expiresAt": "2026-11-12T18:10:00Z"}).to_string(),
            ),
            "DELETE" => (*self.delete_status.lock().unwrap(), String::new()),
            _ => (204, String::new()),
        };
        let response = format!(
            "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
            reply.len()
        );
        let _ = stream.write_all(response.as_bytes()).await;
    }

    fn take(&self) -> Vec<(String, String, Value)> {
        std::mem::take(&mut *self.seen.lock().unwrap())
    }
}

async fn linked_services(
    tmp: &TempDb,
    enabled: bool,
) -> (Services, PrincipalId, PrincipalId, FakeRelay) {
    let (svc, owner, member) = fixture(tmp).await;
    let config = tmp.path.with_extension("toml");
    std::fs::write(&config, format!("[push]\nrelayEnabled = {enabled}\n")).unwrap();
    let svc = svc
        .with_settings_registry(Arc::new(crate::SettingsRegistry::load(config).unwrap()))
        .with_secret_store(Arc::new(crate::settings::InMemorySecretStore::default()));
    let (relay, url) = FakeRelay::start().await;
    *svc.push_relay.url.lock().unwrap() = Some(url);
    (svc, owner, member, relay)
}

fn wire(principal_id: &PrincipalId, host_role: HostRole) -> Caller {
    Caller::Wire {
        principal_id: principal_id.clone(),
        host_role,
    }
}

#[tokio::test]
async fn links_are_created_and_revoked_at_the_relay() {
    let tmp = TempDb::new();
    let (svc, owner, member, relay) = linked_services(&tmp, true).await;
    let link = with_caller(wire(&owner, HostRole::Owner), svc.push_create_link_op())
        .await
        .unwrap();
    let key = svc.push_key().await.unwrap();
    let daemon = key.daemon_id();
    assert_eq!(link["linkToken"], "token-1");
    assert_eq!(link["expiresAt"], "2026-11-12T18:10:00Z");
    assert_eq!(link["daemonId"], daemon.as_str());
    let owner_subject = subject_for(&owner);
    assert_eq!(
        relay.take(),
        [
            (
                "POST".into(),
                "/v1/daemons".into(),
                json!({"publicKey": key.public_key()})
            ),
            (
                "POST".into(),
                format!("/v1/daemons/{daemon}/links"),
                json!({"subject": owner_subject})
            ),
        ]
    );

    with_caller(wire(&member, HostRole::Member), svc.push_create_link_op())
        .await
        .unwrap();
    let calls = relay.take();
    assert_eq!(calls.len(), 1, "the daemon registers once: {calls:?}");
    assert_ne!(subject_for(&member), owner_subject);

    let status = with_caller(wire(&owner, HostRole::Owner), svc.push_status_op())
        .await
        .unwrap();
    assert_eq!(status["linked"], true);
    let revoked = with_caller(wire(&owner, HostRole::Owner), svc.push_revoke_link_op())
        .await
        .unwrap();
    assert_eq!(revoked, json!({"revoked": true}));
    assert_eq!(
        relay.take(),
        [(
            "DELETE".into(),
            format!("/v1/daemons/{daemon}/links/{owner_subject}"),
            Value::Null
        )]
    );
    let status = with_caller(wire(&owner, HostRole::Owner), svc.push_status_op())
        .await
        .unwrap();
    assert_eq!(status["linked"], false);
}

#[tokio::test]
async fn a_revoked_principal_is_unlinked_once_the_relay_answers() {
    let tmp = TempDb::new();
    let (svc, _, member, relay) = linked_services(&tmp, true).await;
    with_caller(wire(&member, HostRole::Member), svc.push_create_link_op())
        .await
        .unwrap();
    relay.take();
    *relay.delete_status.lock().unwrap() = 503;
    assert!(!svc
        .push_queue_revocation(subject_for(&member))
        .await
        .unwrap());
    assert!(
        !svc.push_flush_revocations().await,
        "still pending after a 503"
    );
    *relay.delete_status.lock().unwrap() = 204;
    assert!(svc.push_flush_revocations().await);
    let deletes = relay.take();
    assert_eq!(deletes.len(), 3);
    assert!(deletes.iter().all(|(m, ..)| m == "DELETE"));
    assert!(svc.push_flush_revocations().await);
    assert!(relay.take().is_empty(), "nothing left to retry");
}

#[tokio::test]
async fn only_people_with_host_access_can_link_and_only_when_enabled() {
    let tmp = TempDb::new();
    let (svc, owner, member, relay) = linked_services(&tmp, false).await;
    let mut guest = svc.store.get_principal(&member).await.unwrap();
    guest.id = PrincipalId::new();
    svc.store.upsert_principal(&guest).await.unwrap();
    for caller in [
        wire(&guest.id, HostRole::Guest),
        Caller::Agent {
            agent_id: AgentId::from("agent"),
        },
    ] {
        let refused = with_caller(caller, svc.push_create_link_op()).await;
        assert!(matches!(refused, Err(Error::Forbidden(_))), "{refused:?}");
    }
    let off = with_caller(wire(&owner, HostRole::Owner), svc.push_create_link_op()).await;
    assert!(matches!(off, Err(Error::InvalidParams(_))), "{off:?}");
    assert!(relay.take().is_empty());
}

#[tokio::test]
async fn an_unknown_signer_registers_again_and_retries_once() {
    let tmp = TempDb::new();
    let (svc, owner, _, relay) = linked_services(&tmp, true).await;
    with_caller(wire(&owner, HostRole::Owner), svc.push_create_link_op())
        .await
        .unwrap();
    relay.take();
    *relay.forget_daemon.lock().unwrap() = true;
    let link = with_caller(wire(&owner, HostRole::Owner), svc.push_create_link_op())
        .await
        .unwrap();
    assert_eq!(link["linkToken"], "token-1");
    let paths: Vec<_> = relay
        .take()
        .into_iter()
        .map(|(m, p, _)| format!("{m} {p}"))
        .collect();
    let daemon = svc.push_key().await.unwrap().daemon_id();
    let link_path = format!("POST /v1/daemons/{daemon}/links");
    assert_eq!(
        paths,
        [link_path.clone(), "POST /v1/daemons".into(), link_path]
    );
}

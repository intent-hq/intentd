//! Push relay publisher (the relay lives in `intent-hq/intent-push`).
//!
//! intentd signs facts about each workspace with its own Ed25519 key and
//! sends them to the relay, which keeps the iOS Live Activity current and
//! sends alerts while the app is closed. `push.createLink` mints the
//! one-time token the phone uses to link itself to this daemon.

mod client;
mod proof;
mod state;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use intent_core::events::{
    AGENT_DELETED, AGENT_IDLE, AGENT_RENAMED, AGENT_STARTED, AGENT_STATUS_CHANGED,
    WORKSPACE_ACTIVITY_CHANGED, WORKSPACE_ATTENTION_CHANGED, WORKSPACE_CREATED, WORKSPACE_DELETED,
    WORKSPACE_DISPLAY_STATUS_CHANGED, WORKSPACE_UPDATED,
};
use intent_core::{
    AgentId, Caller, Error, PrincipalId, Result, WorkspaceActivity, WorkspaceApi as _, WorkspaceId,
    WorkspaceListFilter,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::time::Instant;

use crate::{EventBus, Services, SubscriptionFilter};
use client::{RelayClient, RelayError};
use proof::DaemonKey;
use state::{Facts, IdleAgent, Notice, Pending, WorkspaceState};

const KEY_ACCOUNT: &str = "push.relay.daemonKey";
const STATE_ACCOUNT: &str = "push.relay.state";
/// The relay keeps at most this many workspace rows per daemon.
const MAX_WORKSPACES: usize = 500;
const IDLE_RECHECK: Duration = Duration::from_secs(30);
const MAX_BACKOFF: Duration = Duration::from_secs(300);

const EVENT_TYPES: &[&str] = &[
    WORKSPACE_CREATED,
    WORKSPACE_UPDATED,
    WORKSPACE_DELETED,
    WORKSPACE_ACTIVITY_CHANGED,
    WORKSPACE_ATTENTION_CHANGED,
    WORKSPACE_DISPLAY_STATUS_CHANGED,
    AGENT_IDLE,
    AGENT_STARTED,
    AGENT_STATUS_CHANGED,
    AGENT_RENAMED,
    AGENT_DELETED,
];

/// Link bookkeeping kept in the secret store next to the key.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Persisted {
    /// The relay origin `POST /v1/daemons` last succeeded against.
    registered_relay: Option<String>,
    /// Subjects with a link minted at the relay.
    links: BTreeSet<String>,
    /// Subjects whose relay revocation has not succeeded yet.
    pending_revocations: BTreeSet<String>,
}

/// Runtime state shared by every `Services` clone.
#[derive(Default)]
pub(crate) struct PushRelay {
    key: tokio::sync::Mutex<Option<Arc<DaemonKey>>>,
    persisted: tokio::sync::Mutex<Option<Persisted>>,
    wake: tokio::sync::Notify,
    #[cfg(test)]
    url: std::sync::Mutex<Option<String>>,
}

/// `subject` for a principal: the relay stores it so the daemon can revoke
/// every device a person linked, without learning device ids.
fn subject_for(principal: &PrincipalId) -> String {
    proof::sha256_b64(format!("intent-push-subject:{principal}").as_bytes())
}

fn relay_failure(e: &RelayError) -> Error {
    Error::Internal(format!("push relay request failed: {e}"))
}

fn next_backoff(current: Duration) -> Duration {
    (current * 2).clamp(Duration::from_secs(1), MAX_BACKOFF)
}

impl Services {
    #[cfg_attr(not(test), expect(clippy::unused_self))]
    fn push_relay_url(&self) -> String {
        #[cfg(test)]
        if let Some(url) = self.push_relay.url.lock().unwrap().clone() {
            return url;
        }
        client::relay_url()
    }

    fn push_relay_enabled(&self) -> bool {
        self.effective_settings().push.relay_enabled
    }

    async fn push_key(&self) -> Result<Arc<DaemonKey>> {
        let mut slot = self.push_relay.key.lock().await;
        if let Some(key) = slot.as_ref() {
            return Ok(key.clone());
        }
        let stored = self.secrets.load_fresh(KEY_ACCOUNT).await?;
        let stored = stored.as_deref().and_then(|s| {
            use base64::Engine as _;
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(s)
                .ok()
        });
        let seed = if let Some(seed) = stored {
            seed
        } else {
            let seed = DaemonKey::generate_seed();
            self.secrets.store(KEY_ACCOUNT, &proof::b64(&seed)).await?;
            seed.to_vec()
        };
        let key = DaemonKey::from_seed(&seed)
            .ok_or_else(|| Error::Internal("the push relay key is invalid".into()))?;
        let key = Arc::new(key);
        *slot = Some(key.clone());
        Ok(key)
    }

    /// Read-modify-write of the link bookkeeping; persists only on change.
    async fn push_persisted<R>(&self, f: impl FnOnce(&mut Persisted) -> R) -> Result<R> {
        let mut slot = self.push_relay.persisted.lock().await;
        if slot.is_none() {
            let raw = self.secrets.load_fresh(STATE_ACCOUNT).await?;
            *slot = Some(
                raw.and_then(|s| serde_json::from_str(&s).ok())
                    .unwrap_or_default(),
            );
        }
        let current = slot.get_or_insert_with(Persisted::default);
        let before = current.clone();
        let out = f(current);
        if *current != before {
            let text = serde_json::to_string(current)
                .map_err(|e| Error::Internal(format!("push relay state: {e}")))?;
            if let Err(e) = self.secrets.store(STATE_ACCOUNT, &text).await {
                *current = before;
                return Err(e);
            }
        }
        Ok(out)
    }

    async fn push_ensure_registered(
        &self,
        client: &RelayClient,
        key: &DaemonKey,
        force: bool,
    ) -> std::result::Result<(), RelayError> {
        let base = client.base.clone();
        let local = |e: Error| RelayError::Network(e.to_string());
        if !force
            && self
                .push_persisted(|p| p.registered_relay.as_deref() == Some(base.as_str()))
                .await
                .map_err(local)?
        {
            return Ok(());
        }
        client.register(key).await?;
        self.push_persisted(|p| p.registered_relay = Some(base))
            .await
            .map_err(local)
    }

    /// Run `op` against a registered daemon. When the relay no longer knows
    /// this daemon (`unknown_signer`), register again and retry once.
    async fn push_call<T, F, Fut>(
        &self,
        client: &RelayClient,
        key: &DaemonKey,
        op: F,
    ) -> std::result::Result<T, RelayError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, RelayError>>,
    {
        self.push_ensure_registered(client, key, false).await?;
        match op().await {
            Err(e) if e.is_unknown_signer() => {
                self.push_ensure_registered(client, key, true).await?;
                op().await
            }
            other => other,
        }
    }

    /// The caller's link subject. Owner and host members only: a link shows
    /// every ordinary workspace on this machine, which is what members can
    /// already see; guests see only the workspaces they were invited to.
    async fn push_caller_subject(&self, what: &str) -> Result<String> {
        match intent_core::current_caller() {
            Some(Caller::Wire { principal_id, .. }) => {
                self.require_host_execution(what).await?;
                Ok(subject_for(&principal_id))
            }
            _ => Err(Error::Forbidden(format!(
                "{what} is for people, not agents"
            ))),
        }
    }

    /// `push.createLink` → `{linkToken, expiresAt, daemonId, relayUrl}`.
    pub(crate) async fn push_create_link_op(&self) -> Result<Value> {
        let subject = self.push_caller_subject("push.createLink").await?;
        if !self.push_relay_enabled() {
            return Err(Error::InvalidParams(
                "the push relay is off (setting push.relayEnabled)".into(),
            ));
        }
        let key = self.push_key().await?;
        let client = RelayClient::new(self.push_relay_url());
        let (link_token, expires_at) = self
            .push_call(&client, &key, || client.create_link(&key, &subject))
            .await
            .map_err(|e| relay_failure(&e))?;
        self.push_persisted(|p| {
            p.pending_revocations.remove(&subject);
            p.links.insert(subject)
        })
        .await?;
        self.push_relay.wake.notify_one();
        Ok(json!({
            "linkToken": link_token,
            "expiresAt": expires_at,
            "daemonId": key.daemon_id(),
            "relayUrl": client.base,
        }))
    }

    /// `push.revokeLink` → `{revoked}`; `false` means the relay was not
    /// reached and the daemon keeps retrying.
    pub(crate) async fn push_revoke_link_op(&self) -> Result<Value> {
        let subject = self.push_caller_subject("push.revokeLink").await?;
        let revoked = self.push_queue_revocation(subject).await?;
        Ok(json!({ "revoked": revoked }))
    }

    /// `push.status` → `{enabled, relayUrl, daemonId, linked}`.
    pub(crate) async fn push_status_op(&self) -> Result<Value> {
        let subject = self.push_caller_subject("push.status").await?;
        let enabled = self.push_relay_enabled();
        let linked = self.push_persisted(|p| p.links.contains(&subject)).await?;
        let daemon_id = if enabled || linked {
            Some(self.push_key().await?.daemon_id())
        } else {
            None
        };
        Ok(json!({
            "enabled": enabled,
            "relayUrl": self.push_relay_url(),
            "daemonId": daemon_id,
            "linked": linked,
        }))
    }

    /// Queue `subject` for revocation and try once now. Returns whether the
    /// relay confirmed it.
    async fn push_queue_revocation(&self, subject: String) -> Result<bool> {
        let registered = self
            .push_persisted(|p| {
                let known = p.links.remove(&subject) || p.pending_revocations.contains(&subject);
                if known && p.registered_relay.is_some() {
                    p.pending_revocations.insert(subject);
                }
                p.registered_relay.is_some()
            })
            .await?;
        if !registered {
            return Ok(true);
        }
        Ok(self.push_flush_revocations().await)
    }

    /// Retry every pending revocation; `true` when none remain.
    async fn push_flush_revocations(&self) -> bool {
        let Ok((pending, registered)) = self
            .push_persisted(|p| (p.pending_revocations.clone(), p.registered_relay.clone()))
            .await
        else {
            return false;
        };
        if pending.is_empty() {
            return true;
        }
        let Some(base) = registered else {
            let _ = self.push_persisted(|p| p.pending_revocations.clear()).await;
            return true;
        };
        let Ok(key) = self.push_key().await else {
            return false;
        };
        let client = RelayClient::new(base);
        let mut all = true;
        for subject in pending {
            match self
                .push_call(&client, &key, || client.revoke_link(&key, &subject))
                .await
            {
                Ok(()) => {
                    let _ = self
                        .push_persisted(|p| p.pending_revocations.remove(&subject))
                        .await;
                }
                Err(e) => {
                    all = false;
                    tracing::debug!(error = %e, "push relay link revocation will retry");
                }
            }
        }
        all
    }

    /// Spawn the publisher. It does nothing until the relay is enabled and a
    /// link exists.
    #[must_use]
    pub fn spawn_push_publisher(&self) -> tokio::task::JoinHandle<()> {
        let Some(bus) = self.event_bus.clone() else {
            return intent_core::spawn_daemon(async {});
        };
        let services = self.clone();
        intent_core::spawn_daemon(async move { services.push_publisher_loop(&bus).await })
    }

    async fn push_publisher_loop(&self, bus: &EventBus) {
        let mut sub = bus.subscribe(SubscriptionFilter {
            event_types: EVENT_TYPES.iter().map(|t| (*t).to_string()).collect(),
            ..Default::default()
        });
        let mut revocations = Some(self.principal_revocations.subscribe());
        let mut publisher = Publisher::default();
        let mut revocation_retry: Option<(Instant, Duration)> = None;
        if !self.push_flush_revocations().await {
            revocation_retry = Some((
                Instant::now() + Duration::from_secs(1),
                Duration::from_secs(1),
            ));
        }
        loop {
            let active = self.push_relay_enabled()
                && self
                    .push_persisted(|p| !p.links.is_empty())
                    .await
                    .unwrap_or(false);
            let now = Instant::now();
            let mut wake = now + IDLE_RECHECK;
            if active {
                let due = if publisher.online {
                    publisher.pending.next_due()
                } else {
                    Some(publisher.next_sync.unwrap_or(now))
                };
                if let Some(due) = due {
                    wake = wake.min(due);
                }
            }
            if let Some((at, _)) = revocation_retry {
                wake = wake.min(at);
            }
            tokio::select! {
                biased;
                () = self.settings_tasks.closed() => break,
                events = sub.recv() => {
                    let Some(events) = events else { break };
                    if active && publisher.online {
                        for event in events {
                            publisher.note(&event, Instant::now());
                        }
                    }
                }
                revoked = async {
                    match revocations.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => match revoked {
                    Ok(r) => {
                        let subject = subject_for(&r.principal_id);
                        if !self.push_queue_revocation(subject).await.unwrap_or(false) {
                            revocation_retry.get_or_insert((Instant::now() + Duration::from_secs(1), Duration::from_secs(1)));
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => revocations = None,
                },
                () = self.push_relay.wake.notified() => {}
                () = tokio::time::sleep_until(wake) => {}
            }
            if let Some((at, backoff)) = revocation_retry {
                if Instant::now() >= at {
                    revocation_retry = if self.push_flush_revocations().await {
                        None
                    } else {
                        let backoff = next_backoff(backoff);
                        Some((Instant::now() + backoff, backoff))
                    };
                }
            }
            if !active {
                publisher.go_offline(Duration::ZERO);
                continue;
            }
            if !publisher.online {
                if publisher.next_sync.is_none_or(|at| Instant::now() >= at) {
                    match self.push_sync_all(&mut publisher).await {
                        Ok(()) => publisher.online = true,
                        Err(e) => {
                            tracing::debug!(error = %e, "push relay full publish failed");
                            publisher.go_offline(next_backoff(publisher.backoff));
                        }
                    }
                }
                continue;
            }
            for (workspace, idle) in publisher.pending.take_due(Instant::now()) {
                if let Err(e) = self
                    .push_publish_one(&mut publisher, &workspace, &idle)
                    .await
                {
                    tracing::debug!(error = %e, workspace = %workspace.0, "push relay publish failed");
                    if e.is_transient() {
                        publisher.go_offline(next_backoff(publisher.backoff));
                        break;
                    }
                }
            }
        }
    }

    /// `PUT /v1/daemons/:id/workspaces`: replaces every row, so rows that
    /// vanished while offline go too. Never carries notices.
    async fn push_sync_all(
        &self,
        publisher: &mut Publisher,
    ) -> std::result::Result<(), RelayError> {
        let key = self
            .push_key()
            .await
            .map_err(|e| RelayError::Network(e.to_string()))?;
        let client = RelayClient::new(self.push_relay_url());
        let mut rows = self
            .list_workspaces(WorkspaceListFilter::ACTIVE)
            .await
            .map_err(|e| RelayError::Network(e.to_string()))?;
        rows.retain(state::is_published);
        rows.sort_by(|a, b| b.last_activity.cmp(&a.last_activity));
        rows.truncate(MAX_WORKSPACES);
        let now = intent_core::now_iso();
        let mut states = BTreeMap::new();
        let mut facts = HashMap::new();
        for ws in &rows {
            let agents = if ws.activity == WorkspaceActivity::AgentRunning {
                self.agent_list(ws.id.clone()).await.unwrap_or_default()
            } else {
                Vec::new()
            };
            states.insert(ws.id.0.clone(), state::build_state(ws, &agents, &now));
            facts.insert(ws.id.clone(), Facts::of(ws));
        }
        self.push_call(&client, &key, || client.put_all(&key, &states))
            .await?;
        publisher.facts = facts;
        publisher.sent = states
            .into_iter()
            .map(|(id, s)| (WorkspaceId::from(id.as_str()), s))
            .collect();
        publisher.backoff = Duration::ZERO;
        Ok(())
    }

    async fn push_publish_one(
        &self,
        publisher: &mut Publisher,
        workspace: &WorkspaceId,
        idle: &[IdleAgent],
    ) -> std::result::Result<(), RelayError> {
        let key = self
            .push_key()
            .await
            .map_err(|e| RelayError::Network(e.to_string()))?;
        let client = RelayClient::new(self.push_relay_url());
        let ws = match self.get_workspace(workspace.clone()).await {
            Ok(ws) if state::is_published(&ws) => ws,
            Ok(_) | Err(Error::NotFound(_)) => {
                publisher.facts.remove(workspace);
                if publisher.sent.remove(workspace).is_some() {
                    self.push_call(&client, &key, || {
                        client.put_workspace(&key, &workspace.0, None)
                    })
                    .await?;
                }
                return Ok(());
            }
            Err(e) => return Err(RelayError::Network(e.to_string())),
        };
        let agents = self.agent_list(workspace.clone()).await.unwrap_or_default();
        let now = intent_core::now_iso();
        let mut next = state::build_state(&ws, &agents, &now);
        let facts = Facts::of(&ws);
        let transition =
            state::transition_notice(publisher.facts.insert(workspace.clone(), facts), facts);
        let notice = state::choose_notice(transition, idle, &agents);
        if notice.is_none() && publisher.sent.get(workspace) == Some(&next) {
            return Ok(());
        }
        let plain = next.clone();
        next.notice = notice.map(|(kind, agent)| Notice {
            id: uuid::Uuid::new_v4().to_string(),
            kind,
            agent_id: agent
                .as_ref()
                .map(|a| a.id.0.clone())
                .or_else(|| next.agent_id.clone()),
            agent_name: agent
                .and_then(|a| a.name)
                .or_else(|| next.agent_name.clone()),
            at: now,
        });
        self.push_call(&client, &key, || {
            client.put_workspace(&key, &workspace.0, Some(&next))
        })
        .await?;
        publisher.sent.insert(workspace.clone(), plain);
        Ok(())
    }
}

/// Publisher loop state.
#[derive(Default)]
struct Publisher {
    online: bool,
    next_sync: Option<Instant>,
    backoff: Duration,
    pending: Pending,
    /// Last observed notice inputs per workspace.
    facts: HashMap<WorkspaceId, Facts>,
    /// Last published state per workspace, without its notice.
    sent: HashMap<WorkspaceId, WorkspaceState>,
}

impl Publisher {
    fn go_offline(&mut self, backoff: Duration) {
        self.online = false;
        self.backoff = backoff;
        self.next_sync = Some(Instant::now() + backoff);
        self.pending.clear();
    }

    fn note(&mut self, event: &intent_core::Event, now: Instant) {
        let idle = (event.event_type == AGENT_IDLE)
            .then(|| event.data.get("agentId").and_then(Value::as_str))
            .flatten()
            .map(|id| IdleAgent {
                id: AgentId::from(id),
                name: event
                    .data
                    .get("agentName")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            });
        self.pending.note(event.workspace_id.clone(), now, idle);
    }
}

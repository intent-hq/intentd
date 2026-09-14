//! Workspace presence and the per-note viewer / caret channel (multiplayer
//! w5): who is online, where they are, and (per note) where their caret is.
//!
//! Everything here is **ephemeral**: one in-memory table keyed by
//! (principal, connection), fed by the transport's connection lifecycle
//! (`client.hello` → [`Services::presence_connect_op`], close / heartbeat
//! reap → [`Services::presence_disconnect_op`]) and the `presence.update` /
//! `note.presence.*` fast paths. Nothing is persisted and every event is
//! published transiently, so `event.query` never returns a presence row.
//!
//! - **Workspace presence** — `presence:changed { workspaceId, members }` is
//!   the roster of the workspace's currently-online members (a member is
//!   online while it has at least one hello'd connection), each with the
//!   focus items *in that workspace* aggregated over its connections and one
//!   `typing` entry per connection typing to an agent there — keyed by an
//!   opaque, daemon-minted per-connection *typing source* (random, never
//!   derived from the client id, host or device) so a receiver can suppress
//!   its own keystrokes and expire each source independently by its `since`
//!   stamp; the connection learns its own handle from the `presence.update`
//!   reply. Emitted per workspace on every transition that can change the
//!   roster: a principal's first hello'd connection / last connection gone
//!   (to every member workspace), and a `presence.update` (to the workspaces
//!   it left and entered). `presence.snapshot` reads the same roster on
//!   demand for a client that attached its subscription after its hello.
//! - **Note presence** — a `note.presence.subscribe` holds a *lease* on a
//!   note for its connection; a principal is a viewer while any of its
//!   leases is live. `note:presence { kind: joined | updated | left }`
//!   deltas carry the viewer's profile and caret. Caret updates run through
//!   a per-(principal, note) [`CursorThrottle`]: at most one `updated` per
//!   [`CURSOR_MIN_INTERVAL`], always ending on the latest position.
//!
//! Membership: a `presence.update` focus target, a `note.presence` subscribe
//! and every caret update are Member+ (`require_member`, `NotFound` for a
//! non-member) — a lease outlives a membership removal, so the update gate
//! runs on every call and a deferred caret is re-gated when its flush fires;
//! the events are workspace-scoped so the collaborator fan-out gate narrows
//! them like any other row. Profiles (login / avatar) are read once per
//! principal from the store and cached while the principal is present.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use intent_core::events::{NOTE_PRESENCE, PRESENCE_CHANGED};
use intent_core::{
    current_caller, now_iso, AgentId, Caller, Error, EventActor, NoteId, Principal, PrincipalId,
    Result, WorkspaceId,
};
use intent_store::NewEvent;
use serde_json::{json, Value};

use crate::{publish_event_transient, Services};

/// Floor between two `note:presence { kind: "updated" }` deliveries for one
/// (principal, note): ≤10 deliveries per second.
pub const CURSOR_MIN_INTERVAL: Duration = Duration::from_millis(100);

/// What a [`CursorThrottle::offer`] decided for the offered caret.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Offer {
    /// Publish the offered caret now.
    Publish,
    /// Hold it: the caller must schedule a [`CursorThrottle::flush`] after
    /// this delay (a trailing edge so the stream always ends on the latest
    /// position).
    Defer(Duration),
    /// A flush is already scheduled; the offered caret replaced the pending
    /// one (last writer wins) and rides that flush.
    Absorbed,
}

/// Leading-edge + trailing-edge coalescer for one caret stream. Pure (time is
/// injected) so it is unit-testable; the async driver lives in
/// [`Services::note_presence_update_op`].
#[derive(Debug, Default)]
pub struct CursorThrottle {
    last_publish: Option<Instant>,
    pending: Option<Value>,
    armed: bool,
}

impl CursorThrottle {
    /// Offer a new caret at `now`.
    pub fn offer(&mut self, cursor: Value, now: Instant) -> Offer {
        if self.armed {
            self.pending = Some(cursor);
            return Offer::Absorbed;
        }
        match self.last_publish {
            Some(last) if now.duration_since(last) < CURSOR_MIN_INTERVAL => {
                self.pending = Some(cursor);
                self.armed = true;
                Offer::Defer(CURSOR_MIN_INTERVAL.saturating_sub(now.duration_since(last)))
            }
            _ => {
                self.last_publish = Some(now);
                Offer::Publish
            }
        }
    }

    /// The trailing flush: the latest pending caret (marked published at
    /// `now`), or `None` when nothing was absorbed since the deferral.
    pub fn flush(&mut self, now: Instant) -> Option<Value> {
        self.armed = false;
        let pending = self.pending.take();
        if pending.is_some() {
            self.last_publish = Some(now);
        }
        pending
    }
}

/// The cached profile fields of a present principal (what
/// `workspace.members.list` already exposes).
#[derive(Debug, Clone, Default)]
struct Profile {
    login: Option<String>,
    display_name: Option<String>,
    avatar_url: Option<String>,
}

impl From<Principal> for Profile {
    fn from(p: Principal) -> Self {
        Self {
            login: p.login,
            display_name: p.display_name,
            avatar_url: p.avatar_url,
        }
    }
}

/// One `presence.update` focus item: `{ workspaceId, agentId?, noteId? }`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Focus {
    workspace: String,
    agent: Option<String>,
    note: Option<String>,
}

impl Focus {
    fn to_json(&self) -> Value {
        let mut v = json!({ "workspaceId": self.workspace });
        if let Some(a) = &self.agent {
            v["agentId"] = json!(a);
        }
        if let Some(n) = &self.note {
            v["noteId"] = json!(n);
        }
        v
    }
}

/// The connection's typing target: `agent` (in `workspace`) since `since`
/// (ISO-8601; kept across repeated `presence.update`s naming the same agent
/// so an unrelated roster refresh never restarts a receiver's expiry timer).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Typing {
    agent: String,
    workspace: String,
    since: String,
}

/// One live connection's presence row.
#[derive(Debug)]
struct Conn {
    principal: PrincipalId,
    /// `client.hello` completed: the connection counts towards "online".
    hello: bool,
    /// The connection's opaque typing source handle: a fresh random id per
    /// connection, unrelated to the client id, host or device.
    typing_source: String,
    focus: Vec<Focus>,
    typing: Option<Typing>,
    /// Note-presence leases held by this connection: lease id → note.
    leases: HashMap<String, (WorkspaceId, NoteId)>,
}

impl Conn {
    fn new(principal: PrincipalId) -> Self {
        Self {
            principal,
            hello: false,
            typing_source: format!("ts-{}", uuid::Uuid::new_v4().simple()),
            focus: Vec::new(),
            typing: None,
            leases: HashMap::new(),
        }
    }

    /// The workspaces this connection's focus / typing state touches.
    fn workspaces(&self) -> HashSet<String> {
        self.focus
            .iter()
            .map(|f| f.workspace.clone())
            .chain(self.typing.iter().map(|t| t.workspace.clone()))
            .collect()
    }

    /// Whether the row carries nothing worth keeping once its last lease
    /// is gone (an anonymous, never-hello'd connection).
    fn is_empty(&self) -> bool {
        !self.hello && self.focus.is_empty() && self.typing.is_none() && self.leases.is_empty()
    }
}

/// One principal's viewer entry on a note.
#[derive(Debug)]
struct Viewer {
    /// Fences the coalescer's trailing-flush tasks: a task armed for one
    /// viewer never flushes its replacement (the principal left and rejoined
    /// before the old deadline), whose own timer runs on its own schedule.
    generation: u64,
    cursor: Option<Value>,
    throttle: CursorThrottle,
    /// `(connection id, lease id)` pairs keeping the principal on the note.
    leases: HashSet<(String, String)>,
}

impl Default for Viewer {
    fn default() -> Self {
        static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
        Self {
            generation: NEXT_GENERATION.fetch_add(1, Ordering::Relaxed),
            cursor: None,
            throttle: CursorThrottle::default(),
            leases: HashSet::new(),
        }
    }
}

#[derive(Debug, Default)]
struct State {
    conns: HashMap<String, Conn>,
    profiles: HashMap<PrincipalId, Profile>,
    viewers: HashMap<(WorkspaceId, NoteId), HashMap<PrincipalId, Viewer>>,
}

impl State {
    fn is_online(&self, principal: &PrincipalId) -> bool {
        self.conns
            .values()
            .any(|c| c.hello && &c.principal == principal)
    }

    fn profile(&self, principal: &PrincipalId) -> Profile {
        self.profiles.get(principal).cloned().unwrap_or_default()
    }

    /// Drop the cached profile once the principal is present nowhere.
    fn gc_profile(&mut self, principal: &PrincipalId) {
        let present = self.conns.values().any(|c| &c.principal == principal)
            || self
                .viewers
                .values()
                .any(|viewers| viewers.contains_key(principal));
        if !present {
            self.profiles.remove(principal);
        }
    }

    /// The `presence:changed` roster of `workspace_id`: its online members
    /// (`members` is the store's membership list) with their focus items in
    /// this workspace aggregated over connections and one typing entry
    /// `{ source, agentId, since }` per typing connection (never merged:
    /// two clients of one person stay two sources).
    fn roster(&self, workspace_id: &str, members: &[PrincipalId]) -> Vec<Value> {
        members
            .iter()
            .filter(|p| self.is_online(p))
            .map(|p| {
                let mut focus: Vec<Focus> = Vec::new();
                let mut typing: Vec<(&str, &Typing)> = Vec::new();
                for c in self.conns.values().filter(|c| &c.principal == p) {
                    for f in c.focus.iter().filter(|f| f.workspace == workspace_id) {
                        if !focus.contains(f) {
                            focus.push(f.clone());
                        }
                    }
                    if let Some(t) = c.typing.as_ref().filter(|t| t.workspace == workspace_id) {
                        typing.push((c.typing_source.as_str(), t));
                    }
                }
                typing.sort_by(|a, b| a.0.cmp(b.0));
                let typing: Vec<Value> = typing
                    .into_iter()
                    .map(|(source, t)| {
                        json!({ "source": source, "agentId": t.agent, "since": t.since })
                    })
                    .collect();
                let profile = self.profile(p);
                json!({
                    "principalId": p,
                    "login": profile.login,
                    "displayName": profile.display_name,
                    "avatarUrl": profile.avatar_url,
                    "focus": focus.iter().map(Focus::to_json).collect::<Vec<_>>(),
                    "typing": typing,
                })
            })
            .collect()
    }

    /// Release lease `(conn, lease)` of `principal` on a note; `true` when it
    /// was the principal's last lease there (the viewer left).
    fn release_lease(
        &mut self,
        key: &(WorkspaceId, NoteId),
        principal: &PrincipalId,
        conn: &str,
        lease: &str,
    ) -> bool {
        let Some(viewers) = self.viewers.get_mut(key) else {
            return false;
        };
        let Some(viewer) = viewers.get_mut(principal) else {
            return false;
        };
        viewer.leases.remove(&(conn.to_string(), lease.to_string()));
        if !viewer.leases.is_empty() {
            return false;
        }
        viewers.remove(principal);
        if viewers.is_empty() {
            self.viewers.remove(key);
        }
        true
    }
}

/// The daemon-wide presence table. Shared by every `Services` clone and by
/// the trailing-flush tasks the caret coalescer spawns.
#[derive(Debug, Default)]
pub struct PresenceRegistry {
    state: Mutex<State>,
}

impl PresenceRegistry {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// The wire principal of the current request. Presence is a client-connection
/// concern: agent / daemon callers and unbound requests are refused.
fn wire_principal(what: &str) -> Result<PrincipalId> {
    match current_caller() {
        Some(Caller::Wire { principal_id, .. }) => Ok(principal_id),
        Some(_) => Err(Error::Forbidden(format!(
            "{what} is only available to client connections"
        ))),
        None => Err(Error::Forbidden(format!("{what}: no caller is bound"))),
    }
}

fn presence_changed_event(workspace_id: &str, members: &[Value]) -> NewEvent {
    NewEvent {
        workspace_id: WorkspaceId::from(workspace_id),
        timestamp: now_iso(),
        event_type: PRESENCE_CHANGED.to_string(),
        actor: EventActor::default(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({ "workspaceId": workspace_id, "members": members }),
    }
}

fn viewer_row(principal: &PrincipalId, profile: &Profile, cursor: Option<&Value>) -> Value {
    json!({
        "principalId": principal,
        "login": profile.login,
        "displayName": profile.display_name,
        "avatarUrl": profile.avatar_url,
        "cursor": cursor,
    })
}

fn note_presence_event(
    key: &(WorkspaceId, NoteId),
    principal: &PrincipalId,
    profile: &Profile,
    kind: &str,
    cursor: Option<&Value>,
) -> NewEvent {
    let (workspace_id, note_id) = key;
    let mut data = viewer_row(principal, profile, cursor);
    data["workspaceId"] = json!(workspace_id);
    data["noteId"] = json!(note_id);
    data["kind"] = json!(kind);
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: NOTE_PRESENCE.to_string(),
        actor: EventActor::default(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    }
}

/// Parse `presence.update` params: `focus` (required array of
/// `{ workspaceId, agentId?, noteId? }`) and `typing` (optional
/// `{ agentId }` or `null`).
fn parse_update(params: &Value) -> Result<(Vec<Focus>, Option<String>)> {
    let invalid = |m: &str| Error::InvalidParams(format!("presence.update: {m}"));
    let obj = params
        .as_object()
        .ok_or_else(|| invalid("params must be an object"))?;
    let focus = obj
        .get("focus")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("focus must be an array"))?;
    let non_empty_str = |v: Option<&Value>, what: &str| -> Result<Option<String>> {
        match v {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(s)) if !s.is_empty() => Ok(Some(s.clone())),
            Some(_) => Err(invalid(&format!("{what} must be a non-empty string"))),
        }
    };
    let mut items = Vec::with_capacity(focus.len());
    for item in focus {
        let item = item
            .as_object()
            .ok_or_else(|| invalid("focus items must be objects"))?;
        let workspace_id = non_empty_str(item.get("workspaceId"), "focus[].workspaceId")?
            .ok_or_else(|| invalid("focus[].workspaceId is required"))?;
        let parsed = Focus {
            workspace: workspace_id,
            agent: non_empty_str(item.get("agentId"), "focus[].agentId")?,
            note: non_empty_str(item.get("noteId"), "focus[].noteId")?,
        };
        if !items.contains(&parsed) {
            items.push(parsed);
        }
    }
    let typing = match obj.get("typing") {
        None | Some(Value::Null) => None,
        Some(Value::Object(t)) => Some(
            non_empty_str(t.get("agentId"), "typing.agentId")?
                .ok_or_else(|| invalid("typing.agentId is required"))?,
        ),
        Some(_) => return Err(invalid("typing must be an object or null")),
    };
    Ok((items, typing))
}

/// Parse a `note.presence.update` caret: `{ rev, anchor, head }`, each a
/// non-negative integer.
fn parse_cursor(params: &Value) -> Result<Value> {
    let field = |name: &str| -> Result<u64> {
        params.get(name).and_then(Value::as_u64).ok_or_else(|| {
            Error::InvalidParams(format!(
                "note.presence.update: {name} must be a non-negative integer"
            ))
        })
    };
    Ok(json!({ "rev": field("rev")?, "anchor": field("anchor")?, "head": field("head")? }))
}

impl Services {
    fn publish_presence(&self, event: &NewEvent) {
        publish_event_transient(self.event_bus.as_ref(), event);
    }

    /// Cache `principal`'s profile on first sight (one store read per present
    /// principal).
    async fn ensure_profile(&self, principal: &PrincipalId) -> Result<()> {
        if self.presence.lock().profiles.contains_key(principal) {
            return Ok(());
        }
        let profile = Profile::from(self.store.get_principal(principal).await?);
        self.presence
            .lock()
            .profiles
            .entry(principal.clone())
            .or_insert(profile);
        Ok(())
    }

    /// Publish `presence:changed` for `workspace_id` from the current table
    /// (one membership read).
    async fn emit_presence_changed(&self, workspace_id: &str) {
        let members: Vec<PrincipalId> = match self
            .store
            .list_workspace_members(&WorkspaceId::from(workspace_id))
            .await
        {
            Ok(members) => members.into_iter().map(|m| m.principal_id).collect(),
            Err(e) => {
                tracing::warn!(workspace_id, error = %e, "presence: membership read failed");
                return;
            }
        };
        let roster = self.presence.lock().roster(workspace_id, &members);
        self.publish_presence(&presence_changed_event(workspace_id, &roster));
    }

    /// Publish `presence:changed` to every workspace `principal` belongs to
    /// (its online / offline transition).
    async fn emit_presence_for_memberships(&self, principal: &PrincipalId) {
        let memberships = match self.store.list_principal_memberships(principal).await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(%principal, error = %e, "presence: memberships read failed");
                return;
            }
        };
        for m in memberships {
            self.emit_presence_changed(m.workspace_id.as_str()).await;
        }
    }

    /// See [`intent_core::WorkspaceApi::presence_connect`].
    pub(crate) async fn presence_connect_op(&self, connection_id: String) -> Result<()> {
        let principal = wire_principal("presence")?;
        self.ensure_profile(&principal).await?;
        let newly_online = {
            let mut state = self.presence.lock();
            let was_online = state.is_online(&principal);
            let conn = state
                .conns
                .entry(connection_id)
                .or_insert_with(|| Conn::new(principal.clone()));
            conn.hello = true;
            !was_online
        };
        if newly_online {
            self.emit_presence_for_memberships(&principal).await;
        }
        Ok(())
    }

    /// See [`intent_core::WorkspaceApi::presence_update`].
    pub(crate) async fn presence_update_op(
        &self,
        connection_id: String,
        params: Value,
    ) -> Result<Value> {
        let principal = wire_principal("presence.update")?;
        let (focus, typing) = parse_update(&params)?;
        let hello = self
            .presence
            .lock()
            .conns
            .get(&connection_id)
            .is_some_and(|c| c.hello && c.principal == principal);
        if !hello {
            return Err(Error::InvalidParams(
                "presence.update requires a completed client.hello on this connection".to_string(),
            ));
        }
        let mut targets: HashSet<String> = focus.iter().map(|f| f.workspace.clone()).collect();
        let typing = match typing {
            Some(agent_id) => {
                let workspace_id = self
                    .agent_workspace(&AgentId::from(agent_id.as_str()))
                    .await?;
                targets.insert(workspace_id.as_str().to_string());
                Some((agent_id, workspace_id.as_str().to_string()))
            }
            None => None,
        };
        for workspace_id in &targets {
            self.require_member(&WorkspaceId::from(workspace_id.as_str()))
                .await?;
        }
        let (affected, typing_source): (HashSet<String>, String) = {
            let mut state = self.presence.lock();
            let Some(conn) = state.conns.get_mut(&connection_id) else {
                return Err(Error::InvalidParams(
                    "presence.update: the connection is no longer registered".to_string(),
                ));
            };
            let before = conn.workspaces();
            conn.focus = focus;
            conn.typing = typing.map(|(agent, workspace)| {
                // The same target keeps its `since`: a receiver's expiry
                // timer only restarts on a genuinely new typing episode.
                let since = match &conn.typing {
                    Some(t) if t.agent == agent => t.since.clone(),
                    _ => now_iso(),
                };
                Typing {
                    agent,
                    workspace,
                    since,
                }
            });
            (
                before.union(&conn.workspaces()).cloned().collect(),
                conn.typing_source.clone(),
            )
        };
        for workspace_id in affected {
            self.emit_presence_changed(&workspace_id).await;
        }
        Ok(json!({ "ok": true, "typingSource": typing_source }))
    }

    /// See [`intent_core::WorkspaceApi::presence_snapshot`].
    pub(crate) async fn presence_snapshot_op(&self, workspace_id: WorkspaceId) -> Result<Value> {
        self.require_member(&workspace_id).await?;
        self.store.get_workspace(&workspace_id).await?;
        let members: Vec<PrincipalId> = self
            .store
            .list_workspace_members(&workspace_id)
            .await?
            .into_iter()
            .map(|m| m.principal_id)
            .collect();
        let roster = self.presence.lock().roster(workspace_id.as_str(), &members);
        Ok(json!({ "workspaceId": workspace_id.as_str(), "members": roster }))
    }

    /// See [`intent_core::WorkspaceApi::presence_disconnect`].
    pub(crate) async fn presence_disconnect_op(&self, connection_id: String) {
        let (conn, left, went_offline) = {
            let mut state = self.presence.lock();
            let Some(conn) = state.conns.remove(&connection_id) else {
                return;
            };
            let mut left = Vec::new();
            for (lease, key) in &conn.leases {
                if state.release_lease(key, &conn.principal, &connection_id, lease) {
                    let profile = state.profile(&conn.principal);
                    left.push(note_presence_event(
                        key,
                        &conn.principal,
                        &profile,
                        "left",
                        None,
                    ));
                }
            }
            let went_offline = conn.hello && !state.is_online(&conn.principal);
            state.gc_profile(&conn.principal);
            (conn, left, went_offline)
        };
        for event in &left {
            self.publish_presence(event);
        }
        if went_offline {
            self.emit_presence_for_memberships(&conn.principal).await;
        } else {
            for workspace_id in conn.workspaces() {
                self.emit_presence_changed(&workspace_id).await;
            }
        }
    }

    /// See [`intent_core::WorkspaceApi::note_presence_join`].
    pub(crate) async fn note_presence_join_op(
        &self,
        connection_id: String,
        lease_id: String,
        workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> Result<Value> {
        let principal = wire_principal("note.presence.subscribe")?;
        self.require_member(&workspace_id).await?;
        self.ensure_profile(&principal).await?;
        let key = (workspace_id, note_id);
        let (snapshot, joined) = {
            let mut guard = self.presence.lock();
            let state = &mut *guard;
            let conn = state
                .conns
                .entry(connection_id.clone())
                .or_insert_with(|| Conn::new(principal.clone()));
            conn.leases.insert(lease_id.clone(), key.clone());
            let viewers = state.viewers.entry(key.clone()).or_default();
            let joined = !viewers.contains_key(&principal);
            viewers
                .entry(principal.clone())
                .or_default()
                .leases
                .insert((connection_id, lease_id));
            let mut present: Vec<(&PrincipalId, &Viewer)> = viewers.iter().collect();
            present.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
            let rows: Vec<Value> = present
                .into_iter()
                .map(|(p, v)| {
                    let profile = state.profiles.get(p).cloned().unwrap_or_default();
                    viewer_row(p, &profile, v.cursor.as_ref())
                })
                .collect();
            let joined = joined.then(|| {
                note_presence_event(&key, &principal, &state.profile(&principal), "joined", None)
            });
            (rows, joined)
        };
        if let Some(event) = &joined {
            self.publish_presence(event);
        }
        Ok(json!({ "viewers": snapshot }))
    }

    /// See [`intent_core::WorkspaceApi::note_presence_leave`].
    pub(crate) fn note_presence_leave_op(&self, connection_id: &str, lease_id: &str) {
        let left = {
            let mut state = self.presence.lock();
            let Some(conn) = state.conns.get_mut(connection_id) else {
                return;
            };
            let Some(key) = conn.leases.remove(lease_id) else {
                return;
            };
            let principal = conn.principal.clone();
            if conn.is_empty() {
                state.conns.remove(connection_id);
            }
            let left = state
                .release_lease(&key, &principal, connection_id, lease_id)
                .then(|| {
                    note_presence_event(&key, &principal, &state.profile(&principal), "left", None)
                });
            state.gc_profile(&principal);
            left
        };
        if let Some(event) = &left {
            self.publish_presence(event);
        }
    }

    /// See [`intent_core::WorkspaceApi::note_presence_update`].
    pub(crate) async fn note_presence_update_op(
        &self,
        connection_id: &str,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        cursor: &Value,
    ) -> Result<Value> {
        let principal = wire_principal("note.presence.update")?;
        self.require_member(&workspace_id).await?;
        let cursor = parse_cursor(cursor)?;
        let key = (workspace_id, note_id);
        let publish = {
            let mut state = self.presence.lock();
            let subscribed = state
                .conns
                .get(connection_id)
                .is_some_and(|c| c.principal == principal && c.leases.values().any(|k| *k == key));
            if !subscribed {
                return Err(Error::InvalidParams(
                    "note.presence.update: not subscribed to this note (note.presence.subscribe first)"
                        .to_string(),
                ));
            }
            let profile = state.profile(&principal);
            let Some(viewer) = state
                .viewers
                .get_mut(&key)
                .and_then(|v| v.get_mut(&principal))
            else {
                return Err(Error::InvalidParams(
                    "note.presence.update: not subscribed to this note (note.presence.subscribe first)"
                        .to_string(),
                ));
            };
            viewer.cursor = Some(cursor.clone());
            match viewer.throttle.offer(cursor.clone(), Instant::now()) {
                Offer::Publish => Some(note_presence_event(
                    &key,
                    &principal,
                    &profile,
                    "updated",
                    Some(&cursor),
                )),
                Offer::Defer(delay) => {
                    spawn_trailing_flush(
                        self.clone(),
                        current_caller(),
                        key,
                        principal,
                        viewer.generation,
                        delay,
                    );
                    None
                }
                Offer::Absorbed => None,
            }
        };
        if let Some(event) = &publish {
            self.publish_presence(event);
        }
        Ok(json!({ "ok": true }))
    }
}

/// The coalescer's trailing edge: after `delay`, publish the latest caret
/// absorbed for `(principal, note)` — if the viewer of `generation` is still
/// there and `caller` (the request that armed the flush) is still a member:
/// a removal that lands inside the window drops the pending caret instead of
/// letting it out after the gate closed.
fn spawn_trailing_flush(
    services: Services,
    caller: Option<Caller>,
    key: (WorkspaceId, NoteId),
    principal: PrincipalId,
    generation: u64,
    delay: Duration,
) {
    intent_core::spawn_daemon(async move {
        tokio::time::sleep(delay).await;
        let member = match caller {
            Some(caller) => intent_core::with_caller(caller, services.require_member(&key.0))
                .await
                .is_ok(),
            None => false,
        };
        let event = {
            let mut state = services.presence.lock();
            let profile = state.profile(&principal);
            state
                .viewers
                .get_mut(&key)
                .and_then(|v| v.get_mut(&principal))
                .filter(|viewer| viewer.generation == generation)
                .and_then(|viewer| viewer.throttle.flush(Instant::now()))
                .filter(|_| member)
                .map(|cursor| {
                    note_presence_event(&key, &principal, &profile, "updated", Some(&cursor))
                })
        };
        if let Some(event) = &event {
            publish_event_transient(services.event_bus.as_ref(), event);
        }
    });
}

#[cfg(test)]
mod tests;

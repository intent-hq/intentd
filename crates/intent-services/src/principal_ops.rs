//! `principal.*` service logic and caller resolution (multiplayer w1).
//!
//! Every request reaches the service layer with a [`Caller`] bound by its
//! entry point (`intent_core::caller`). This module turns that binding into
//! a principal: a wire caller IS its principal; agents and hook runs act
//! for the daemon and resolve to the primary principal. An absent caller is
//! forbidden — never the primary user.
//!
//! The GitHub identity of the primary principal is attached lazily and off
//! the read path (stale-while-revalidate): a `principal.me` read for the
//! primary user serves the cached `principal` row and, at most once per
//! [`IDENTITY_REFRESH_INTERVAL`] per process, spawns a bounded background
//! refresh from `GET /user` when the source-control auth is configured. An
//! offline daemon keeps serving the cache.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use intent_core::{
    current_caller, lift_from_principal_id, now_iso, Caller, Error, Principal, PrincipalId, Result,
    Workspace, WorkspaceId, FROM_PRINCIPAL_ID_KEY,
};
use intent_store::Store;
use serde_json::{json, Value};
use tokio::sync::OnceCell;
use tokio::time::Instant;

use crate::{pr_ops, Services};

/// Minimum spacing between two GitHub profile refreshes of the primary
/// principal within one daemon process.
const IDENTITY_REFRESH_INTERVAL: Duration = Duration::from_secs(10 * 60);
/// Bound on the network round trip; an offline daemon serves the cache.
const IDENTITY_REFRESH_TIMEOUT: Duration = Duration::from_secs(5);

/// Last successful/attempted identity refresh instant, shared across clones.
pub(crate) type IdentityRefreshState = Arc<tokio::sync::Mutex<Option<Instant>>>;

/// The forbidden error for a request with no bound caller.
pub(crate) fn no_caller() -> Error {
    Error::Internal("forbidden: request is not bound to a principal".to_string())
}

/// Resolve the current request's caller to a principal id. `None` when no
/// caller is bound.
pub(crate) async fn caller_principal_id(store: &Store) -> Result<Option<PrincipalId>> {
    match current_caller() {
        None => Ok(None),
        Some(Caller::Wire { principal_id, .. }) => Ok(Some(principal_id)),
        Some(Caller::Agent { .. } | Caller::Daemon) => {
            Ok(Some(store.get_primary_principal().await?.id))
        }
    }
}

/// The principal a human-authored message is stamped with: the bound wire
/// caller's principal. Agents and the daemon are not people and stamp
/// nothing; so does an absent caller.
fn stamping_principal_id() -> Option<PrincipalId> {
    match current_caller() {
        Some(Caller::Wire { principal_id, .. }) => Some(principal_id),
        Some(Caller::Agent { .. } | Caller::Daemon) | None => None,
    }
}

/// Daemon-authoritative principal stamp on a user-origin message payload
/// (multiplayer w2). Applied at every user-origin entry point BEFORE the
/// payload is persisted or enqueued, so direct persists, queue entries and
/// their drain/redrive all carry the same [`FROM_PRINCIPAL_ID_KEY`]: a wire
/// caller's principal overwrites whatever the client supplied; an agent /
/// daemon / absent caller strips the key instead. Every other field passes
/// through untouched. A non-object payload cannot carry the stamp and is
/// rejected with `InvalidParams` (the same rule `agent.queueMessage` and
/// `userAppMessageId` already apply) — a human send must never be credited
/// to the workspace fallback because its metadata had the wrong shape.
/// Metadata only — the content is never annotated, so prompts stay
/// byte-identical.
pub(crate) fn stamp_principal_attribution(
    message_metadata: Option<Value>,
) -> Result<Option<Value>> {
    let metadata = match message_metadata {
        None => None,
        Some(Value::Object(obj)) => Some(obj),
        Some(_) => {
            return Err(Error::InvalidParams(
                "messageMetadata must be an object".to_string(),
            ))
        }
    };
    Ok(match (metadata, stamping_principal_id()) {
        (Some(mut obj), Some(principal_id)) => {
            obj.insert(
                FROM_PRINCIPAL_ID_KEY.to_string(),
                Value::String(principal_id.0),
            );
            Some(Value::Object(obj))
        }
        (Some(mut obj), None) => {
            obj.remove(FROM_PRINCIPAL_ID_KEY);
            Some(Value::Object(obj))
        }
        (None, Some(principal_id)) => Some(json!({ FROM_PRINCIPAL_ID_KEY: principal_id.0 })),
        (None, None) => None,
    })
}

/// `true` when a queue entry / message payload carries the daemon's human
/// principal stamp — the authoritative "a person wrote this" marker,
/// independent of the entry's lifecycle origin (a human wake delivered
/// through `deliver_wake_message` is enqueued as `Automatic`, yet stamped).
pub(crate) fn carries_principal_stamp(message_metadata: Option<&Value>) -> bool {
    lift_from_principal_id(message_metadata).is_some()
}

/// `true` when an unstamped queue entry's `messageMetadata` still reads as
/// human-authored — the same rule the fe applies to transcript rows: an
/// entry is agent/automatic origin iff its metadata is an object with a
/// string `type` (other than the user-authored `question_answers` wizard
/// tag), a non-empty `fromAgentId`, or `source == "system"`. Absent or
/// non-object metadata fails open (human).
fn is_human_authored_metadata(message_metadata: Option<&Value>) -> bool {
    let Some(Value::Object(obj)) = message_metadata else {
        return true;
    };
    match obj.get("type").and_then(Value::as_str) {
        Some("question_answers") => return true,
        Some(_) => return false,
        None => {}
    }
    if obj
        .get("fromAgentId")
        .and_then(Value::as_str)
        .is_some_and(|id| !id.trim().is_empty())
    {
        return false;
    }
    obj.get("source").and_then(Value::as_str) != Some("system")
}

/// Strip a client-supplied [`FROM_PRINCIPAL_ID_KEY`] without stamping — for
/// payloads that are not human-authored regardless of who submitted them
/// (a non-user-origin send, a non-`user` transcript row).
pub(crate) fn strip_principal_attribution(message_metadata: Option<Value>) -> Option<Value> {
    match message_metadata {
        Some(Value::Object(mut obj)) => {
            obj.remove(FROM_PRINCIPAL_ID_KEY);
            Some(Value::Object(obj))
        }
        other => other,
    }
}

/// The serve-time `author` projection of a user message:
/// `{ principalId, login, displayName, avatarUrl }`. Profile fields are
/// `null` when the principal row is gone (the id is still what the row
/// says).
fn author_to_wire(principal_id: &PrincipalId, principal: Option<&Principal>) -> Value {
    json!({
        "principalId": principal_id,
        "login": principal.and_then(|p| p.login.clone()),
        "displayName": principal.and_then(|p| p.display_name.clone()),
        "avatarUrl": principal.and_then(|p| p.avatar_url.clone()),
    })
}

/// Serve-time author resolution for the user messages of one workspace
/// (multiplayer w2): the stamped [`FROM_PRINCIPAL_ID_KEY`], else the
/// workspace's `legacy_author_principal_id`, else its current owner. One
/// resolver per read: the workspace fallback is loaded at most once and a
/// page's distinct authors are fetched in ONE batched statement by
/// [`Self::prefetch`] (RPC cost contract: a transcript page costs a bounded
/// number of statements, never one per distinct author).
pub(crate) struct MessageAuthorResolver<'a> {
    services: &'a Services,
    workspace_id: &'a WorkspaceId,
    /// Memoized workspace fallback (`legacy_author_principal_id`, else
    /// `owner_principal_id`); the inner `None` means the workspace resolves
    /// no fallback author.
    fallback: OnceCell<Option<PrincipalId>>,
    principals: HashMap<PrincipalId, Option<Principal>>,
    /// Principal-table round trips issued so far (batched or single).
    #[cfg(test)]
    principal_lookups: usize,
}

impl<'a> MessageAuthorResolver<'a> {
    pub(crate) fn new(services: &'a Services, workspace_id: &'a WorkspaceId) -> Self {
        Self {
            services,
            workspace_id,
            fallback: OnceCell::new(),
            principals: HashMap::new(),
            #[cfg(test)]
            principal_lookups: 0,
        }
    }

    /// Warm the cache for one page: every distinct stamped principal in
    /// `stamps` (the lifted stamp of each user row, `None` when unstamped)
    /// plus the workspace fallback when any row is unstamped, in a single
    /// `get_principals` statement. Only ids not already cached are fetched;
    /// a failed batch leaves the cache untouched and [`Self::resolve`] falls
    /// back to single lookups.
    async fn prefetch(&mut self, stamps: Vec<Option<PrincipalId>>) {
        let mut wanted: Vec<PrincipalId> = Vec::new();
        let mut needs_fallback = false;
        for stamp in stamps {
            match stamp {
                Some(id) => {
                    if !self.principals.contains_key(&id) && !wanted.contains(&id) {
                        wanted.push(id);
                    }
                }
                None => needs_fallback = true,
            }
        }
        if needs_fallback {
            if let Some(id) = self.fallback_principal_id().await {
                if !self.principals.contains_key(&id) && !wanted.contains(&id) {
                    wanted.push(id);
                }
            }
        }
        if wanted.is_empty() {
            return;
        }
        #[cfg(test)]
        {
            self.principal_lookups += 1;
        }
        match self.services.store.get_principals(&wanted).await {
            Ok(found) => {
                for p in found {
                    self.principals.insert(p.id.clone(), Some(p));
                }
                for id in wanted {
                    self.principals.entry(id).or_insert(None);
                }
            }
            Err(e) => {
                tracing::debug!(error = %e, "message author: batched principal read failed");
            }
        }
    }

    async fn fallback_principal_id(&self) -> Option<PrincipalId> {
        self.fallback
            .get_or_init(|| async {
                match self
                    .services
                    .store
                    .get_workspace_author_fallback(self.workspace_id)
                    .await
                {
                    Ok(Some(fb)) => fb.legacy_author_principal_id.or(fb.owner_principal_id),
                    Ok(None) => None,
                    Err(e) => {
                        tracing::debug!(error = %e, "message author: workspace fallback read failed");
                        None
                    }
                }
            })
            .await
            .clone()
    }

    /// The `author` value for a user row with `metadata`; `None` when nothing
    /// resolves (no stamp and a workspace without principal columns).
    pub(crate) async fn resolve(&mut self, metadata: Option<&Value>) -> Option<Value> {
        let principal_id = match lift_from_principal_id(metadata) {
            Some(id) => id,
            None => self.fallback_principal_id().await?,
        };
        if !self.principals.contains_key(&principal_id) {
            #[cfg(test)]
            {
                self.principal_lookups += 1;
            }
            let loaded = match self.services.store.get_principal(&principal_id).await {
                Ok(p) => Some(p),
                Err(Error::NotFound(_)) => None,
                Err(e) => {
                    tracing::debug!(error = %e, principal = %principal_id, "message author: principal read failed");
                    return None;
                }
            };
            self.principals.insert(principal_id.clone(), loaded);
        }
        Some(author_to_wire(
            &principal_id,
            self.principals.get(&principal_id).and_then(Option::as_ref),
        ))
    }

    /// Set `author` on EVERY entry of a queue snapshot (`agent.getQueue` /
    /// `agent:queue:updated`): the key is always present so a client can
    /// treat it as authoritative — the projection resolved from the entry's
    /// `messageMetadata` in the same order as a transcript user row (stamp,
    /// else workspace fallback) and the same batched shape, or an explicit
    /// `null`. A stamped entry always resolves (the stamp is the daemon's own
    /// "a person submitted this" marker); an unstamped entry gets the
    /// workspace fallback only when its metadata still reads as
    /// human-authored ([`is_human_authored_metadata`]) — agent-sent and
    /// automatic (hook / monitor / system) entries are `null`, as is anything
    /// the workspace cannot resolve.
    pub(crate) async fn attach_queue(&mut self, entries: &mut [Value]) {
        let candidates: Vec<(usize, Option<PrincipalId>)> = entries
            .iter()
            .enumerate()
            .filter_map(|(i, e)| {
                let md = e.get("messageMetadata");
                let stamp = lift_from_principal_id(md);
                (stamp.is_some() || is_human_authored_metadata(md)).then_some((i, stamp))
            })
            .collect();
        self.prefetch(candidates.iter().map(|(_, s)| s.clone()).collect())
            .await;
        for entry in entries.iter_mut() {
            if let Some(obj) = entry.as_object_mut() {
                obj.insert("author".to_string(), Value::Null);
            }
        }
        for (i, _) in candidates {
            let metadata = entries[i].get("messageMetadata").cloned();
            if let Some(author) = self.resolve(metadata.as_ref()).await {
                entries[i]["author"] = author;
            }
        }
    }

    /// Attach `author` to every `user`-role message object in `messages`
    /// (the serialized transcript page). Non-user rows are untouched.
    pub(crate) async fn attach(&mut self, messages: &mut [Value]) {
        let stamps = messages
            .iter()
            .filter(|m| m.get("role").and_then(Value::as_str) == Some("user"))
            .map(|m| lift_from_principal_id(m.get("metadata")))
            .collect();
        self.prefetch(stamps).await;
        for message in messages.iter_mut() {
            if message.get("role").and_then(Value::as_str) != Some("user") {
                continue;
            }
            let Some(author) = self.resolve(message.get("metadata")).await else {
                continue;
            };
            if let Some(obj) = message.as_object_mut() {
                obj.insert("author".to_string(), author);
            }
        }
    }

    /// Typed twin of [`Self::attach`] for reads that serve
    /// [`intent_core::AgentMessage`] rows directly (`agent.getSession`):
    /// same resolution, same `author` shape, so both hydration routes agree.
    pub(crate) async fn attach_typed(&mut self, messages: &mut [intent_core::AgentMessage]) {
        let stamps = messages
            .iter()
            .filter(|m| m.role == "user")
            .map(|m| lift_from_principal_id(m.metadata.as_ref()))
            .collect();
        self.prefetch(stamps).await;
        for message in messages.iter_mut() {
            if message.role != "user" {
                continue;
            }
            message.author = self.resolve(message.metadata.as_ref()).await;
        }
    }
}

/// `principal.me` wire shape.
pub(crate) fn principal_to_wire(p: &Principal, is_administrator: bool) -> Value {
    json!({
        "id": p.id,
        "login": p.login,
        "displayName": p.display_name,
        "avatarUrl": p.avatar_url,
        "isAdministrator": is_administrator,
    })
}

impl Services {
    /// `principal.me`: see [`intent_core::WorkspaceApi::principal_me`].
    pub(crate) async fn principal_me_op(&self) -> Result<Value> {
        let caller = current_caller().ok_or_else(no_caller)?;
        let principal = match &caller {
            Caller::Wire { principal_id, .. } => self.store.get_principal(principal_id).await?,
            Caller::Agent { .. } | Caller::Daemon => self.store.get_primary_principal().await?,
        };
        if principal.is_primary {
            self.spawn_primary_identity_refresh(principal.clone()).await;
        }
        let is_administrator = match caller {
            Caller::Wire {
                is_administrator, ..
            } => is_administrator,
            Caller::Agent { .. } | Caller::Daemon => principal.is_primary,
        };
        Ok(principal_to_wire(&principal, is_administrator))
    }

    /// Spawn a rate-limited, bounded background refresh of the primary
    /// principal's GitHub identity from `GET /user`. Detached: the read that
    /// triggered it never waits, and any failure (not configured, offline,
    /// timeout) leaves the cached row untouched.
    async fn spawn_primary_identity_refresh(&self, principal: Principal) {
        {
            let mut last = self.principal_identity_refreshed_at.lock().await;
            if last.is_some_and(|at| at.elapsed() < IDENTITY_REFRESH_INTERVAL) {
                return;
            }
            *last = Some(Instant::now());
        }
        let this = self.clone();
        tokio::spawn(async move {
            this.refresh_primary_identity(principal).await;
        });
    }

    async fn refresh_primary_identity(&self, principal: Principal) {
        let fetched = tokio::time::timeout(IDENTITY_REFRESH_TIMEOUT, async {
            let sc = pr_ops::resolve_source_control(self.source_control.clone()).await?;
            if !sc.check_auth().await.is_ok_and(|s| s.authenticated) {
                return Err(Error::Internal("github auth not configured".to_string()));
            }
            sc.get_user().await.map_err(pr_ops::map_sc_err)
        })
        .await;
        let user = match fetched {
            Ok(Ok(user)) => user,
            Ok(Err(e)) => {
                tracing::debug!(error = %e, "principal.me: github identity refresh skipped");
                return;
            }
            Err(_) => {
                tracing::debug!("principal.me: github identity refresh timed out");
                return;
            }
        };
        let mut updated = principal.clone();
        updated.github_user_id = user.id.and_then(|id| i64::try_from(id).ok());
        updated.login = Some(user.login);
        updated.display_name = user.name;
        updated.avatar_url = user.avatar_url;
        if updated == principal {
            return;
        }
        updated.updated_at = now_iso();
        if let Err(e) = self.store.upsert_principal(&updated).await {
            tracing::warn!(error = %e, "principal.me: github identity persist failed");
        }
    }

    /// Attach the membership summary to one `workspace.get` row, relative
    /// to the current caller (one SQL query; a failure omits the fields).
    pub(crate) async fn attach_workspace_membership(&self, ws: &mut Workspace) {
        let viewer = caller_principal_id(&self.store).await.ok().flatten();
        match self
            .store
            .workspace_membership_summaries(viewer.as_ref(), std::slice::from_ref(&ws.id))
            .await
        {
            Ok(mut map) => ws.membership = map.remove(&ws.id),
            Err(e) => tracing::debug!(error = %e, "workspace.get: membership summary failed"),
        }
    }

    /// Attach membership summaries to `workspace.list` rows in one query
    /// scoped to exactly the rows being returned (an empty list issues none).
    pub(crate) async fn attach_workspace_memberships(&self, list: &mut [Workspace]) {
        if list.is_empty() {
            return;
        }
        let viewer = caller_principal_id(&self.store).await.ok().flatten();
        let ids: Vec<WorkspaceId> = list.iter().map(|ws| ws.id.clone()).collect();
        match self
            .store
            .workspace_membership_summaries(viewer.as_ref(), &ids)
            .await
        {
            Ok(mut map) => {
                for ws in list.iter_mut() {
                    ws.membership = map.remove(&ws.id);
                }
            }
            Err(e) => tracing::debug!(error = %e, "workspace.list: membership summary failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{workspace, TempDb};
    use intent_core::{with_caller, AgentId};

    fn wire(principal_id: &PrincipalId) -> Caller {
        Caller::Wire {
            principal_id: principal_id.clone(),
            is_administrator: false,
        }
    }

    fn principal(login: &str) -> Principal {
        Principal {
            id: PrincipalId::new(),
            github_user_id: None,
            login: Some(login.to_string()),
            display_name: Some(format!("{login} name")),
            avatar_url: Some(format!("https://example.test/{login}.png")),
            is_primary: false,
            created_at: now_iso(),
            updated_at: now_iso(),
        }
    }

    /// A wire caller's principal overwrites a client-supplied stamp, is
    /// added to a bare payload, and materializes an absent one; every other
    /// key passes through.
    #[tokio::test]
    async fn stamp_overwrites_client_value_under_wire_caller() {
        let me = PrincipalId::new();
        let (spoofed, bare, absent) = with_caller(wire(&me), async {
            (
                stamp_principal_attribution(Some(
                    json!({ "fromPrincipalId": "someone-else", "kind": "reply" }),
                ))
                .unwrap(),
                stamp_principal_attribution(Some(json!({ "kind": "reply" }))).unwrap(),
                stamp_principal_attribution(None).unwrap(),
            )
        })
        .await;
        assert_eq!(
            spoofed,
            Some(json!({ "fromPrincipalId": me.0, "kind": "reply" }))
        );
        assert_eq!(
            bare,
            Some(json!({ "fromPrincipalId": me.0, "kind": "reply" }))
        );
        assert_eq!(absent, Some(json!({ "fromPrincipalId": me.0 })));
    }

    /// A non-object payload cannot carry the stamp: it is rejected as
    /// `InvalidParams` for every caller kind instead of slipping past the
    /// stamp (which would credit a human send to the workspace fallback).
    #[tokio::test]
    async fn stamp_rejects_non_object_metadata_for_every_caller() {
        let me = PrincipalId::new();
        for payload in [json!("x"), json!([]), json!(7), json!(true)] {
            let wire_err = with_caller(wire(&me), {
                let payload = payload.clone();
                async move { stamp_principal_attribution(Some(payload)) }
            })
            .await;
            assert!(
                matches!(wire_err, Err(Error::InvalidParams(ref m)) if m == "messageMetadata must be an object"),
                "wire caller, payload {payload}: {wire_err:?}"
            );
            let daemon_err = with_caller(Caller::Daemon, {
                let payload = payload.clone();
                async move { stamp_principal_attribution(Some(payload)) }
            })
            .await;
            assert!(
                matches!(daemon_err, Err(Error::InvalidParams(_))),
                "daemon caller, payload {payload}: {daemon_err:?}"
            );
            assert!(
                matches!(
                    stamp_principal_attribution(Some(payload.clone())),
                    Err(Error::InvalidParams(_))
                ),
                "unbound caller, payload {payload}"
            );
        }
        assert!(carries_principal_stamp(Some(
            &json!({ "fromPrincipalId": me.0 })
        )));
        assert!(!carries_principal_stamp(Some(
            &json!({ "fromAgentId": "a" })
        )));
        assert!(!carries_principal_stamp(None));
    }

    /// Agents, the daemon and an unbound context are not people: a
    /// client-supplied stamp is stripped and nothing is added.
    #[tokio::test]
    async fn stamp_strips_for_agent_daemon_and_unbound_callers() {
        let spoofed = || Some(json!({ "fromPrincipalId": "someone-else", "kind": "reply" }));
        let agent = with_caller(
            Caller::Agent {
                agent_id: AgentId::new(),
            },
            async { stamp_principal_attribution(spoofed()).unwrap() },
        )
        .await;
        let daemon = with_caller(Caller::Daemon, async {
            stamp_principal_attribution(spoofed()).unwrap()
        })
        .await;
        let unbound = stamp_principal_attribution(spoofed()).unwrap();
        for (label, got) in [("agent", agent), ("daemon", daemon), ("unbound", unbound)] {
            assert_eq!(got, Some(json!({ "kind": "reply" })), "{label}");
        }
        assert_eq!(
            with_caller(Caller::Daemon, async {
                stamp_principal_attribution(None).unwrap()
            })
            .await,
            None
        );
        assert_eq!(
            strip_principal_attribution(spoofed()),
            Some(json!({ "kind": "reply" }))
        );
    }

    /// Serve-time resolution: a stamped row credits the stamped principal
    /// (profile attached, `null`s for a vanished principal); an unstamped
    /// row credits the workspace's `legacy_author_principal_id`, else its
    /// owner. Non-user rows are never annotated.
    #[tokio::test]
    async fn resolver_prefers_stamp_then_legacy_author_then_owner() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        let primary = store.get_primary_principal().await.expect("primary");
        let guest = principal("guest");
        store.upsert_principal(&guest).await.expect("guest");
        let services = Services::new(store);

        let stamped = json!({ "fromPrincipalId": guest.id.0 });
        let mut resolver = MessageAuthorResolver::new(&services, &ws);
        assert_eq!(
            resolver.resolve(Some(&stamped)).await,
            Some(json!({
                "principalId": guest.id.0,
                "login": "guest",
                "displayName": "guest name",
                "avatarUrl": "https://example.test/guest.png",
            }))
        );
        // Unstamped, no legacy author: the owner (primary by trigger).
        let owner = resolver.resolve(None).await.expect("owner fallback");
        assert_eq!(owner["principalId"], primary.id.0);
        // A stamp naming a vanished principal keeps the id, nulls the profile.
        let gone = resolver
            .resolve(Some(&json!({ "fromPrincipalId": "gone" })))
            .await
            .expect("vanished stamp still resolves");
        assert_eq!(
            gone,
            json!({ "principalId": "gone", "login": null, "displayName": null, "avatarUrl": null })
        );

        // Legacy author wins over the owner for unstamped rows.
        services
            .store
            .set_workspace_legacy_author_principal_id(&ws, Some(&guest.id))
            .await
            .expect("set legacy author");
        let mut resolver = MessageAuthorResolver::new(&services, &ws);
        let legacy = resolver.resolve(None).await.expect("legacy fallback");
        assert_eq!(legacy["principalId"], guest.id.0);

        // `attach` annotates user rows only.
        let mut page = vec![
            json!({ "role": "user", "metadata": { "fromPrincipalId": primary.id.0 } }),
            json!({ "role": "assistant" }),
            json!({ "role": "user" }),
        ];
        resolver.attach(&mut page).await;
        assert_eq!(page[0]["author"]["principalId"], primary.id.0);
        assert!(page[1].get("author").is_none());
        assert_eq!(page[2]["author"]["principalId"], guest.id.0);

        // The typed twin (`agent.getSession`) produces the identical `author`
        // per row — stamped, legacy-fallback and non-user alike.
        let typed_row = |role: &str, metadata: Option<Value>| intent_core::AgentMessage {
            id: format!("m-{role}"),
            agent_id: AgentId::from("agent-typed"),
            seq: 0,
            role: role.to_string(),
            content: json!([]),
            metadata,
            app_message_id: None,
            author: None,
            created_at: now_iso(),
        };
        let mut typed = vec![
            typed_row("user", Some(json!({ "fromPrincipalId": primary.id.0 }))),
            typed_row("assistant", None),
            typed_row("user", None),
        ];
        let mut resolver = MessageAuthorResolver::new(&services, &ws);
        resolver.attach_typed(&mut typed).await;
        assert_eq!(typed[0].author, page[0].get("author").cloned());
        assert_eq!(typed[1].author, None);
        assert_eq!(typed[2].author, page[2].get("author").cloned());
        let serialized = serde_json::to_value(&typed[1]).expect("serialize");
        assert!(
            serialized.get("author").is_none(),
            "an unresolved author is omitted from the wire shape: {serialized}"
        );
    }

    /// RPC cost contract: a transcript page with D distinct authors (stamped
    /// and unstamped, known and vanished) costs ONE principal statement on
    /// both hydration routes, not D — and rows the batch already answered
    /// never trigger a single lookup.
    #[tokio::test]
    async fn resolver_batches_a_page_into_one_principal_lookup() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        let primary = store.get_primary_principal().await.expect("primary");
        let guests: Vec<Principal> = (0..3).map(|i| principal(&format!("g{i}"))).collect();
        for g in &guests {
            store.upsert_principal(g).await.expect("guest");
        }
        let services = Services::new(store);

        let mut page = Vec::new();
        for g in &guests {
            for _ in 0..2 {
                page.push(json!({ "role": "user", "metadata": { "fromPrincipalId": g.id.0 } }));
            }
        }
        page.push(json!({ "role": "user", "metadata": { "fromPrincipalId": "gone" } }));
        page.push(json!({ "role": "user" }));
        page.push(json!({ "role": "assistant" }));
        page.push(json!({ "role": "user", "metadata": { "fromPrincipalId": primary.id.0 } }));

        let mut resolver = MessageAuthorResolver::new(&services, &ws);
        resolver.attach(&mut page).await;
        assert_eq!(
            resolver.principal_lookups, 1,
            "one batched statement for the whole page"
        );
        for (i, g) in guests.iter().enumerate() {
            assert_eq!(page[2 * i]["author"]["principalId"], g.id.0);
            assert_eq!(page[2 * i]["author"]["login"], format!("g{i}"));
        }
        assert_eq!(
            page[6]["author"],
            json!({ "principalId": "gone", "login": null, "displayName": null, "avatarUrl": null })
        );
        assert_eq!(page[7]["author"]["principalId"], primary.id.0);
        assert!(page[8].get("author").is_none());
        assert_eq!(page[9]["author"]["principalId"], primary.id.0);

        // The typed route batches identically.
        let typed_row = |metadata: Option<Value>| intent_core::AgentMessage {
            id: "m".to_string(),
            agent_id: AgentId::from("agent-typed"),
            seq: 0,
            role: "user".to_string(),
            content: json!([]),
            metadata,
            app_message_id: None,
            author: None,
            created_at: now_iso(),
        };
        let mut typed: Vec<_> = guests
            .iter()
            .map(|g| typed_row(Some(json!({ "fromPrincipalId": g.id.0 }))))
            .chain(std::iter::once(typed_row(None)))
            .collect();
        let mut resolver = MessageAuthorResolver::new(&services, &ws);
        resolver.attach_typed(&mut typed).await;
        assert_eq!(resolver.principal_lookups, 1);
        assert!(typed.iter().all(|m| m.author.is_some()));

        // An empty / non-user page issues no principal statement at all.
        let mut resolver = MessageAuthorResolver::new(&services, &ws);
        resolver.attach(&mut [json!({ "role": "assistant" })]).await;
        assert_eq!(resolver.principal_lookups, 0);
    }

    /// Queue snapshots carry `author` on EVERY entry, resolved from
    /// `messageMetadata` with the transcript's order and shape in one batched
    /// statement: a stamped entry is its principal, an unstamped human entry
    /// (no metadata, or the user-authored `question_answers` tag) falls back
    /// to the workspace author, and agent-sent (`fromAgentId`), automatic
    /// (string `type`) and `source: "system"` entries are an explicit `null`
    /// — never an absent key.
    #[tokio::test]
    async fn resolver_attaches_author_to_queue_entries() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        let primary = store.get_primary_principal().await.expect("primary");
        let guest = principal("guest");
        store.upsert_principal(&guest).await.expect("guest");
        let services = Services::new(store);

        let mut queue = vec![
            json!({ "id": "q1", "content": "hi", "queuedAt": now_iso(), "position": 0,
                    "messageMetadata": { "fromPrincipalId": guest.id.0 } }),
            json!({ "id": "q2", "content": "legacy", "queuedAt": now_iso(), "position": 1 }),
            json!({ "id": "q3", "content": "from agent", "queuedAt": now_iso(), "position": 2,
                    "messageMetadata": { "fromAgentId": "agent-x", "fromAgentName": "X" } }),
            json!({ "id": "q4", "content": "wake", "queuedAt": now_iso(), "position": 3,
                    "messageMetadata": { "type": "hook_dispatch" } }),
            json!({ "id": "q5", "content": "answers", "queuedAt": now_iso(), "position": 4,
                    "messageMetadata": { "type": "question_answers" } }),
            json!({ "id": "q6", "content": "dismissed", "queuedAt": now_iso(), "position": 5,
                    "messageMetadata": { "source": "system" } }),
        ];
        let mut resolver = MessageAuthorResolver::new(&services, &ws);
        resolver.attach_queue(&mut queue).await;
        assert_eq!(resolver.principal_lookups, 1, "one batched statement");
        assert!(
            queue.iter().all(|e| e.get("author").is_some()),
            "every entry carries the key: {}",
            json!(queue)
        );
        assert_eq!(
            queue[0]["author"],
            json!({ "principalId": guest.id.0, "login": "guest", "displayName": "guest name",
                    "avatarUrl": "https://example.test/guest.png" })
        );
        assert_eq!(queue[1]["author"]["principalId"], primary.id.0);
        assert_eq!(
            queue[2]["author"],
            Value::Null,
            "agent-sent entry is an explicit null: {}",
            queue[2]
        );
        assert_eq!(
            queue[3]["author"],
            Value::Null,
            "automatic wake is an explicit null: {}",
            queue[3]
        );
        assert_eq!(queue[4]["author"]["principalId"], primary.id.0);
        assert_eq!(queue[5]["author"], Value::Null, "{}", queue[5]);
        assert_eq!(
            queue[0]["messageMetadata"]["fromPrincipalId"], guest.id.0,
            "the stamp itself is untouched"
        );

        // A workspace that resolves no fallback (unknown row: no owner, no
        // legacy author) still emits the key.
        let unknown_ws = WorkspaceId::new();
        let mut queue = vec![
            json!({ "id": "q7", "content": "legacy", "queuedAt": now_iso(),
                                     "position": 0 }),
        ];
        let mut resolver = MessageAuthorResolver::new(&services, &unknown_ws);
        resolver.attach_queue(&mut queue).await;
        assert_eq!(
            queue[0]["author"],
            Value::Null,
            "unresolvable: {}",
            queue[0]
        );
    }
}

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
/// through untouched; a non-object payload cannot carry the stamp and is
/// returned as-is (the row then resolves through the workspace fallback at
/// serve time). Metadata only — the content is never annotated, so prompts
/// stay byte-identical.
pub(crate) fn stamp_principal_attribution(message_metadata: Option<Value>) -> Option<Value> {
    match (message_metadata, stamping_principal_id()) {
        (Some(Value::Object(mut obj)), Some(principal_id)) => {
            obj.insert(
                FROM_PRINCIPAL_ID_KEY.to_string(),
                Value::String(principal_id.0),
            );
            Some(Value::Object(obj))
        }
        (Some(Value::Object(mut obj)), None) => {
            obj.remove(FROM_PRINCIPAL_ID_KEY);
            Some(Value::Object(obj))
        }
        (None, Some(principal_id)) => Some(json!({ FROM_PRINCIPAL_ID_KEY: principal_id.0 })),
        (None, None) => None,
        (other, _) => other,
    }
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
/// resolver per read so the workspace fallback is loaded at most once and
/// each distinct principal at most once (RPC cost contract: bounded by the
/// distinct authors on the page, never by its length).
pub(crate) struct MessageAuthorResolver<'a> {
    services: &'a Services,
    workspace_id: &'a WorkspaceId,
    /// Memoized workspace fallback (`legacy_author_principal_id`, else
    /// `owner_principal_id`); the inner `None` means the workspace resolves
    /// no fallback author.
    fallback: OnceCell<Option<PrincipalId>>,
    principals: HashMap<PrincipalId, Option<Principal>>,
}

impl<'a> MessageAuthorResolver<'a> {
    pub(crate) fn new(services: &'a Services, workspace_id: &'a WorkspaceId) -> Self {
        Self {
            services,
            workspace_id,
            fallback: OnceCell::new(),
            principals: HashMap::new(),
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

    /// Attach `author` to every `user`-role message object in `messages`
    /// (the serialized transcript page). Non-user rows are untouched.
    pub(crate) async fn attach(&mut self, messages: &mut [Value]) {
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
                )),
                stamp_principal_attribution(Some(json!({ "kind": "reply" }))),
                stamp_principal_attribution(None),
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

    /// Agents, the daemon and an unbound context are not people: a
    /// client-supplied stamp is stripped and nothing is added.
    #[tokio::test]
    async fn stamp_strips_for_agent_daemon_and_unbound_callers() {
        let spoofed = || Some(json!({ "fromPrincipalId": "someone-else", "kind": "reply" }));
        let agent = with_caller(
            Caller::Agent {
                agent_id: AgentId::new(),
            },
            async { stamp_principal_attribution(spoofed()) },
        )
        .await;
        let daemon = with_caller(Caller::Daemon, async {
            stamp_principal_attribution(spoofed())
        })
        .await;
        let unbound = stamp_principal_attribution(spoofed());
        for (label, got) in [("agent", agent), ("daemon", daemon), ("unbound", unbound)] {
            assert_eq!(got, Some(json!({ "kind": "reply" })), "{label}");
        }
        assert_eq!(
            with_caller(Caller::Daemon, async { stamp_principal_attribution(None) }).await,
            None
        );
        assert_eq!(
            strip_principal_attribution(spoofed()),
            Some(json!({ "kind": "reply" }))
        );
        // A non-object payload cannot carry the stamp and is left alone.
        let me = PrincipalId::new();
        assert_eq!(
            with_caller(wire(&me), async {
                stamp_principal_attribution(Some(json!("x")))
            })
            .await,
            Some(json!("x"))
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
    }
}

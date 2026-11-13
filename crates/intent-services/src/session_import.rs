//! Import a provider's existing sessions into Intent (PROTOCOL §5.5
//! `agent.importableSessions.list` / `agent.importSession`).
//!
//! Both methods talk to the provider's own ACP adapter through a scratch
//! process ([`crate::agent_manager::scratch_adapter`]), never to an agent:
//!
//! - a provider is importable when its `initialize` advertises
//!   `sessionCapabilities.list` and `loadSession` (`sessionCapabilities.fork`
//!   is optional);
//! - the list is `session/list` on every importable, enabled provider in
//!   parallel, merged newest first. Each provider has one deadline for its
//!   start-up and pages; the rows read before it are kept (`truncated`).
//!   Without a `query` only the pages the requested window needs are read,
//!   and a listing serves later calls for [`LISTING_TTL`];
//! - the import brings the session into the workspace's checkout, then loads
//!   it once and stores the `session/load` replay as the agent's chat
//!   history. The agent's next turn resumes it with the ordinary
//!   `session/load` path (its stored ACP session id).
//!
//! How the session reaches the checkout, by preference:
//!
//! 1. Claude Code (`~/.claude/projects/<cwd slug>/<id>.jsonl`, keyed by the
//!    session's folder): copy the transcript into the checkout's project
//!    folder under a new id, rewriting `sessionId` and the `cwd` values under
//!    the source folder. The source file is never written.
//! 2. `session/fork { sessionId, cwd }` when advertised.
//! 3. Otherwise the original id is loaded (`forked: false`): the provider's
//!    session continues in Intent.

use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, PoisonError};
use std::time::Duration;

use intent_acp::transport::IncomingNotification;
use intent_core::{AgentCreateExtra, AgentId, Error, Result, WorkspaceId};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::agent_manager::scratch_adapter::ScratchAdapter;
use crate::Services;

/// Default page size of `agent.importableSessions.list`.
pub const DEFAULT_LIMIT: usize = 50;
/// Largest accepted `limit`.
pub const MAX_LIMIT: usize = 200;
/// Pages read from one adapter's `session/list` at most.
const MAX_PAGES_PER_PROVIDER: usize = 40;
/// How long one provider's listing answers later calls (paging, search)
/// before its adapter is started again.
const LISTING_TTL: Duration = Duration::from_secs(30);
/// Settle window and cap for replay notifications arriving after the
/// `session/load` response.
const REPLAY_SETTLE: Duration = Duration::from_millis(100);
const REPLAY_CAP: Duration = Duration::from_secs(2);

/// Per-provider budget for starting the adapter and listing its sessions.
/// `INTENTD_SESSION_IMPORT_TIMEOUT_MS` overrides it (tests).
fn provider_timeout() -> Duration {
    std::env::var("INTENTD_SESSION_IMPORT_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(Duration::from_secs(15), Duration::from_millis)
}

/// `agent.importableSessions.list` params.
#[derive(Debug, Clone, Default)]
pub struct ImportableListParams {
    pub provider: Option<String>,
    pub query: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
}

/// `agent.importSession` params.
#[derive(Debug, Clone)]
pub struct ImportSessionParams {
    pub workspace_id: WorkspaceId,
    pub provider: String,
    pub session_id: String,
    pub name: Option<String>,
}

/// The session store layout a provider keeps on disk, when Intent relocates
/// sessions itself instead of asking the adapter to fork.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoreLayout {
    /// `<claude config dir>/projects/<slug(cwd)>/<sessionId>.jsonl`.
    Claude,
}

fn store_layout(provider_id: &str) -> Option<StoreLayout> {
    if provider_id == "claude-code" {
        return Some(StoreLayout::Claude);
    }
    // End-to-end seam: the mock ACP agent keeps a Claude-style store.
    if provider_id == "mock"
        && std::env::var("MOCK_AGENT_SESSION_STORE").is_ok_and(|v| v == "claude")
    {
        return Some(StoreLayout::Claude);
    }
    None
}

/// Whether the mock provider is wired (end-to-end tests only).
fn mock_available() -> bool {
    std::env::var_os("MOCK_AGENT_SCRIPT_PATH").is_some()
}

/// Claude Code's project folder name for `cwd`: every character other than
/// an ASCII letter or digit becomes `-`; a name over 200 characters keeps its
/// first 200 and appends `-<base36 hash>` (the Claude Agent SDK's rule).
pub(crate) fn claude_project_slug(cwd: &str) -> String {
    const MAX: usize = 200;
    let slug: String = cwd
        .encode_utf16()
        .map(|unit| {
            let c = char::from_u32(u32::from(unit)).unwrap_or('-');
            if c.is_ascii_alphanumeric() {
                c
            } else {
                '-'
            }
        })
        .collect();
    if slug.len() <= MAX {
        return slug;
    }
    let mut hash: i32 = 0;
    for unit in cwd.encode_utf16() {
        hash = hash
            .wrapping_shl(5)
            .wrapping_sub(hash)
            .wrapping_add(i32::from(unit));
    }
    format!("{}-{}", &slug[..MAX], base36(hash.unsigned_abs()))
}

fn base36(mut n: u32) -> String {
    const DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if n == 0 {
        return "0".into();
    }
    let mut out = Vec::new();
    while n > 0 {
        out.push(DIGITS[(n % 36) as usize]);
        n /= 36;
    }
    out.reverse();
    String::from_utf8(out).unwrap_or_default()
}

/// A provider session id that is safe as a file name.
fn safe_session_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 200
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Find `<projects>/*/<session_id>.jsonl`.
fn find_claude_session(projects: &Path, session_id: &str) -> Option<PathBuf> {
    let file = format!("{session_id}.jsonl");
    std::fs::read_dir(projects)
        .ok()?
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path().join(&file))
        .find(|path| path.is_file())
}

/// The folder a session started in: the first row carrying a `cwd`.
fn claude_session_cwd(path: &Path) -> Option<String> {
    let file = std::fs::File::open(path).ok()?;
    std::io::BufReader::new(file)
        .lines()
        .map_while(std::result::Result::ok)
        .filter_map(|line| serde_json::from_str::<Value>(&line).ok())
        .find_map(|row| row["cwd"].as_str().map(str::to_owned))
}

/// `value` with the folder prefix `from` replaced by `to`, when `value` is
/// `from` or a path under it.
fn rebase(value: &str, from: &str, to: &str) -> Option<String> {
    let from = from.trim_end_matches('/');
    let rest = value.strip_prefix(from)?;
    (rest.is_empty() || rest.starts_with('/')).then(|| format!("{to}{rest}"))
}

/// Copy the Claude transcript `source` to `target`, rewriting each row's
/// top-level `sessionId` to `new_id` and each top-level `cwd` under
/// `source_cwd` to the same place under `target_cwd`. Lines that are not JSON
/// objects are copied as they are. The source is only read.
pub(crate) fn fork_claude_transcript(
    source: &Path,
    target: &Path,
    new_id: &str,
    source_cwd: &str,
    target_cwd: &str,
) -> std::io::Result<()> {
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let input = std::io::BufReader::new(std::fs::File::open(source)?);
    let mut output = std::io::BufWriter::new(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(target)?,
    );
    for line in input.split(b'\n') {
        let line = line?;
        let rewritten = serde_json::from_slice::<Value>(&line)
            .ok()
            .and_then(|mut row| {
                let obj = row.as_object_mut()?;
                if obj.contains_key("sessionId") {
                    obj.insert("sessionId".into(), json!(new_id));
                }
                if let Some(cwd) = obj.get("cwd").and_then(Value::as_str) {
                    if let Some(moved) = rebase(cwd, source_cwd, target_cwd) {
                        obj.insert("cwd".into(), json!(moved));
                    }
                }
                serde_json::to_vec(&row).ok()
            });
        match rewritten {
            Some(bytes) => output.write_all(&bytes)?,
            None => output.write_all(&line)?,
        }
        output.write_all(b"\n")?;
    }
    output.flush()
}

/// One row of the merged list.
#[derive(Debug, Clone)]
struct Listed {
    provider: String,
    raw: Value,
    updated_at: Option<String>,
}

impl Listed {
    fn to_wire(&self) -> Value {
        let session_id = self.raw["sessionId"].as_str().unwrap_or_default();
        let title = self.raw["title"]
            .as_str()
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .unwrap_or(session_id);
        let mut out = json!({
            "provider": self.provider,
            "sessionId": session_id,
            "title": title,
            "cwd": self.raw["cwd"].as_str().unwrap_or_default(),
            "updatedAt": self.updated_at,
        });
        if let Some(meta) = self.raw.get("_meta").and_then(Value::as_object) {
            if let Some(branch) = meta.get("gitBranch").and_then(Value::as_str) {
                out["gitBranch"] = json!(branch);
            }
            if let Some(count) = meta.get("messageCount").and_then(Value::as_u64) {
                out["messageCount"] = json!(count);
            }
        }
        out
    }

    fn matches(&self, needle: &str) -> bool {
        ["title", "cwd", "sessionId"].iter().any(|key| {
            self.raw[*key]
                .as_str()
                .is_some_and(|v| v.to_lowercase().contains(needle))
        })
    }
}

/// The opaque combined cursor: the offset into the merged, filtered list.
fn decode_cursor(cursor: Option<&str>) -> Result<usize> {
    match cursor {
        None => Ok(0),
        Some(c) => c
            .strip_prefix("o:")
            .and_then(|n| n.parse().ok())
            .ok_or_else(|| Error::InvalidParams("cursor: not a cursor from this method".into())),
    }
}

/// Whether `provider`'s adapter pages `session/list` newest `updatedAt`
/// first, so that its first pages hold its newest sessions. Codex lists by
/// creation time: a resumed old thread can sit on a late page.
fn lists_newest_first(provider: &str) -> bool {
    matches!(provider, "claude-code" | "pi" | "mock")
}

/// The sessions read from one provider's adapter.
#[derive(Debug)]
struct ProviderSessions {
    sessions: Vec<Value>,
    /// The adapter has pages this listing did not read: the rows already
    /// covered the requested window.
    more: bool,
    /// The deadline, the page cap or a page failure ended the listing.
    truncated: bool,
}

impl ProviderSessions {
    /// Whether this listing answers a call that needs the newest `need` rows.
    fn covers(&self, need: usize) -> bool {
        !self.more || self.sessions.len() >= need
    }
}

/// Outcome of listing one provider.
enum ProviderListing {
    Ok(Arc<ProviderSessions>),
    Unsupported,
    Error(String),
}

struct CachedListing {
    listing: Arc<ProviderSessions>,
    at: tokio::time::Instant,
}

/// One provider's last listing and last failure.
#[derive(Default)]
struct SlotState {
    listing: Option<CachedListing>,
    failed: Option<(String, tokio::time::Instant)>,
}

/// The listing state of each provider. A provider's slot is held while it
/// is listed, so concurrent calls wait for one adapter instead of each
/// starting their own.
type ListingSlot = Arc<tokio::sync::Mutex<SlotState>>;

static LISTINGS: LazyLock<std::sync::Mutex<HashMap<String, ListingSlot>>> =
    LazyLock::new(Default::default);

fn listing_slot(provider: &str) -> ListingSlot {
    LISTINGS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(provider.to_owned())
        .or_default()
        .clone()
}

/// `provider`'s sessions: the cached listing when it is fresh and covers
/// `need` rows, else a new one (cached when it lists anything). Waiting for
/// another call's listing counts against this call's deadline, and a listing
/// that failed while this call waited fails it too.
async fn cached_or_list(
    manager: &crate::AgentManager,
    provider: &str,
    cwd: &Path,
    need: usize,
) -> ProviderListing {
    let started = tokio::time::Instant::now();
    let deadline = started + provider_timeout();
    let need = if lists_newest_first(provider) {
        need
    } else {
        usize::MAX
    };
    let slot = listing_slot(provider);
    let Ok(mut state) = tokio::time::timeout_at(deadline, slot.lock()).await else {
        tracing::warn!(
            provider,
            "importable sessions listing timed out waiting for another listing"
        );
        return ProviderListing::Error("timed out".into());
    };
    if let Some(entry) = state.listing.as_ref() {
        if entry.at.elapsed() < LISTING_TTL && entry.listing.covers(need) {
            return ProviderListing::Ok(entry.listing.clone());
        }
    }
    if let Some((error, at)) = state.failed.as_ref() {
        if *at >= started {
            return ProviderListing::Error(error.clone());
        }
    }
    let listing = list_one_provider(manager, provider, cwd, need, deadline).await;
    match &listing {
        ProviderListing::Ok(sessions) => {
            state.listing = Some(CachedListing {
                listing: sessions.clone(),
                at: tokio::time::Instant::now(),
            });
            state.failed = None;
        }
        ProviderListing::Error(error) => {
            state.failed = Some((error.clone(), tokio::time::Instant::now()));
        }
        ProviderListing::Unsupported => {}
    }
    listing
}

/// The title and folder of `session_id` in `provider`'s cached listing,
/// when a fresh listing holds it.
fn cached_listed(provider: &str, session_id: &str) -> Option<(Option<String>, Option<String>)> {
    let slot = listing_slot(provider);
    let state = slot.try_lock().ok()?;
    let entry = state
        .listing
        .as_ref()
        .filter(|e| e.at.elapsed() < LISTING_TTL)?;
    entry
        .listing
        .sessions
        .iter()
        .find(|s| s["sessionId"].as_str() == Some(session_id))
        .map(listed_fields)
}

/// The trimmed `title` and `cwd` of a `session/list` row.
fn listed_fields(row: &Value) -> (Option<String>, Option<String>) {
    let title = row["title"]
        .as_str()
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(str::to_owned);
    let cwd = row["cwd"]
        .as_str()
        .filter(|c| !c.is_empty())
        .map(str::to_owned);
    (title, cwd)
}

impl Services {
    /// The bare provider ids `agent.importableSessions.list` /
    /// `agent.importSession` may use: `provider` when given (unknown is
    /// `-32602`), otherwise every registered provider that is enabled and
    /// runnable here.
    fn import_candidates(&self, provider: Option<&str>) -> Result<Vec<String>> {
        let settings = self.effective_settings();
        if let Some(provider) = provider {
            let known = intent_providers::find_provider(provider).is_some()
                && intent_providers::split_profile_ref(provider).is_none();
            if !known || (provider == "mock" && !mock_available()) {
                return Err(Error::InvalidParams(format!(
                    "provider: unknown provider \"{provider}\""
                )));
            }
            return Ok(vec![provider.to_owned()]);
        }
        Ok(intent_providers::all_provider_ids()
            .into_iter()
            .filter(|id| *id != "mock" || mock_available())
            .filter(|id| {
                *id == "mock"
                    || crate::agent_ops::ensure_provider_available_checked(
                        "agent.importableSessions.list",
                        id,
                        &settings.providers,
                        false,
                    )
                    .is_ok()
            })
            .map(str::to_owned)
            .collect())
    }

    fn require_import_manager(&self) -> Result<std::sync::Arc<crate::AgentManager>> {
        self.agent_manager()
            .ok_or_else(|| Error::Internal("agent manager unavailable".into()))
    }

    /// `agent.importableSessions.list`.
    ///
    /// # Errors
    /// `-32602` for an unknown provider, a bad cursor or limit.
    pub async fn agent_importable_sessions_list_op(
        &self,
        params: ImportableListParams,
    ) -> Result<Value> {
        // The sessions are the host account's files: administrator only.
        Self::require_administrator("agent.importableSessions.list")?;
        self.refuse_in_orchestrator_mode("agent.importableSessions.list")?;
        let limit = params.limit.unwrap_or(DEFAULT_LIMIT);
        if limit == 0 || limit > MAX_LIMIT {
            return Err(Error::InvalidParams(format!(
                "limit: must be between 1 and {MAX_LIMIT}"
            )));
        }
        let offset = decode_cursor(params.cursor.as_deref())?;
        let query = params
            .query
            .as_deref()
            .map(str::trim)
            .filter(|q| !q.is_empty());
        // A search filters every row; a plain page needs each provider's
        // newest `offset + limit` only.
        let need = if query.is_some() {
            usize::MAX
        } else {
            offset.saturating_add(limit)
        };
        let candidates = self.import_candidates(params.provider.as_deref())?;
        let manager = self.require_import_manager()?;
        let cwd = std::env::temp_dir();
        let mut tasks = tokio::task::JoinSet::new();
        for (index, provider) in candidates.iter().cloned().enumerate() {
            let manager = manager.clone();
            let cwd = cwd.clone();
            let services = self.clone();
            tasks.spawn(async move {
                let settings = services.effective_settings();
                if crate::agent_ops::provider_is_disabled(
                    &provider,
                    settings.providers.enabled.as_ref(),
                ) {
                    return (
                        index,
                        ProviderListing::Error("provider is not enabled".into()),
                    );
                }
                let listing = cached_or_list(&manager, &provider, &cwd, need).await;
                (index, listing)
            });
        }
        let mut listings: Vec<Option<ProviderListing>> = candidates.iter().map(|_| None).collect();
        while let Some(joined) = tasks.join_next().await {
            if let Ok((index, listing)) = joined {
                listings[index] = Some(listing);
            }
        }
        let listings = listings
            .into_iter()
            .map(|l| l.unwrap_or_else(|| ProviderListing::Error("listing task failed".into())));
        let mut rows = Vec::new();
        let mut providers = Vec::new();
        let mut more = false;
        for (provider, listing) in candidates.iter().zip(listings) {
            let label = intent_providers::provider_config(provider).display_name;
            match listing {
                ProviderListing::Ok(listed) => {
                    let mut status = json!({ "id": provider, "label": label, "status": "ok" });
                    if listed.truncated {
                        status["truncated"] = json!(true);
                    }
                    providers.push(status);
                    more |= listed.more;
                    rows.extend(listed.sessions.iter().filter_map(|raw| {
                        raw["sessionId"].as_str().filter(|id| !id.is_empty())?;
                        let updated_at = raw["updatedAt"].as_str().map(str::to_owned);
                        Some(Listed {
                            provider: provider.clone(),
                            raw: raw.clone(),
                            updated_at,
                        })
                    }));
                }
                ProviderListing::Unsupported => providers
                    .push(json!({ "id": provider, "label": label, "status": "unsupported" })),
                ProviderListing::Error(error) => providers.push(
                    json!({ "id": provider, "label": label, "status": "error", "error": error }),
                ),
            }
        }
        if let Some(query) = query {
            let needle = query.to_lowercase();
            rows.retain(|row| row.matches(&needle));
        }
        // Newest first; ISO-8601 UTC strings order lexically. Undated last.
        rows.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        let mut page: Vec<Value> = rows
            .iter()
            .skip(offset)
            .take(limit)
            .map(Listed::to_wire)
            .collect();
        attach_repo_roots(&mut page).await;
        let mut out = json!({ "sessions": page, "providers": providers });
        if offset + limit < rows.len() || more {
            out["nextCursor"] = json!(format!("o:{}", offset + limit));
        }
        Ok(out)
    }

    /// `agent.importSession`.
    ///
    /// # Errors
    /// `-32602` for an unknown provider or a workspace without a checkout,
    /// `-32602 not-found` for a session the provider does not have.
    pub async fn agent_import_session_op(&self, params: ImportSessionParams) -> Result<Value> {
        self.refuse_in_orchestrator_mode("agent.importSession")?;
        let ImportSessionParams {
            workspace_id,
            provider,
            session_id,
            name,
        } = params;
        Self::require_administrator("agent.importSession")?;
        self.require_workspace_operator(&workspace_id, "agent.importSession")
            .await?;
        if session_id.trim().is_empty() {
            return Err(Error::InvalidParams("sessionId: required".into()));
        }
        self.import_candidates(Some(&provider))?;
        if provider != "mock" {
            crate::agent_ops::ensure_provider_available_checked(
                "agent.importSession",
                &provider,
                &self.effective_settings().providers,
                false,
            )?;
        }
        let workspace = self.store.get_workspace(&workspace_id).await?;
        let worktree = crate::git_ops::worktree_path(&workspace)
            .filter(|p| p.is_dir())
            .ok_or_else(|| {
                Error::InvalidParams(format!(
                    "workspaceId: workspace {workspace_id} has no checkout folder to import into"
                ))
            })?;
        let manager = self.require_import_manager()?;
        let mut adapter = manager.open_scratch_adapter(&provider, &worktree).await?;
        let outcome = self
            .import_on_adapter(&mut adapter, &provider, &session_id, &worktree)
            .await;
        adapter.close().await;
        let imported = outcome?;
        let title = name
            .map(|n| n.trim().to_owned())
            .filter(|n| !n.is_empty())
            .or_else(|| imported.title.clone())
            .unwrap_or_else(|| "Imported session".to_owned());
        let created = self
            .agent_create_op(
                workspace_id.clone(),
                Some(title),
                None,
                None,
                None,
                None,
                !self.effective_auto_commit(&workspace_id).await,
                AgentCreateExtra {
                    provider: Some(provider.clone()),
                    name_explicitly_set: Some(true),
                    metadata: Some(json!({ "importedSession": {
                        "provider": provider,
                        "sourceSessionId": session_id,
                        "sessionId": imported.session_id,
                        "forked": imported.forked,
                        "sourceCwd": imported.source_cwd,
                        // The first turn tells the agent it was moved into
                        // Intent (`AgentManager::apply_import_handoff`).
                        "handoffPending": true,
                    }})),
                    ..AgentCreateExtra::default()
                },
            )
            .await?;
        let agent_id = AgentId::from(
            created["agent"]["id"]
                .as_str()
                .ok_or_else(|| Error::Internal("agent.create returned no id".into()))?,
        );
        self.store
            .set_acp_session_id(&workspace_id, &agent_id, &imported.session_id)
            .await?;
        // The session belongs to this provider's Default configuration: the
        // resume guards (cross-provider and settings binding) then load it.
        self.store
            .set_agent_session_last_turn_model(&workspace_id, &agent_id, None, &provider)
            .await?;
        if let Some(fingerprint) = imported.fingerprint.as_deref().filter(|f| !f.is_empty()) {
            self.bind_native_session_settings(&workspace_id, &agent_id, fingerprint)
                .await;
        }
        let now = intent_core::now_iso();
        let batch: Vec<intent_store::ReplaceMessage<'_>> = imported
            .messages
            .iter()
            .map(|(role, content)| intent_store::ReplaceMessage {
                role,
                content,
                metadata: None,
                created_at: now.as_str(),
            })
            .collect();
        let inserted = self.store.replace_agent_messages(&agent_id, &batch).await?;
        self.invalidate_agent_list_cache(&workspace_id);
        self.publish_agent_mutation_event(
            &workspace_id,
            &agent_id,
            intent_core::events::AGENT_UPDATED,
            json!({ "agentId": agent_id.0, "replacedCount": inserted.len() }),
        )
        .await;
        let agent = self.agent_get_op(agent_id, Some(workspace_id)).await?;
        Ok(json!({
            "agent": agent,
            "sessionId": imported.session_id,
            "forked": imported.forked,
            "messageCount": inserted.len(),
        }))
    }

    async fn import_on_adapter(
        &self,
        adapter: &mut ScratchAdapter,
        provider: &str,
        source_id: &str,
        worktree: &Path,
    ) -> Result<Imported> {
        let caps = adapter.capabilities;
        // Only a loadable session can be replayed into the chat and resumed
        // by the agent's first turn (`session/load` of the stored id).
        if !caps.load {
            return Err(Error::InvalidParams(format!(
                "provider: \"{provider}\" cannot load its sessions"
            )));
        }
        let (title, listed_cwd) = match cached_listed(provider, source_id) {
            Some(found) => found,
            None => tokio::time::timeout(provider_timeout(), find_listed(adapter, source_id))
                .await
                .unwrap_or_default(),
        };
        let mut source_cwd = listed_cwd;
        let timeout = provider_timeout().max(Duration::from_secs(60));
        let mut relocated: Option<PathBuf> = None;
        let (session_id, forked) = match store_layout(provider) {
            Some(StoreLayout::Claude) => {
                if !safe_session_id(source_id) {
                    return Err(Error::NotFound(format!("session {source_id}")));
                }
                let projects = claude_config_dir(adapter.overlay.as_deref()).join("projects");
                let source = find_claude_session(&projects, source_id)
                    .ok_or_else(|| Error::NotFound(format!("session {source_id}")))?;
                let first_cwd = claude_session_cwd(&source).unwrap_or_default();
                if !first_cwd.is_empty() {
                    source_cwd = Some(first_cwd.clone());
                }
                let new_id = Uuid::new_v4().to_string();
                let target_cwd = worktree.to_string_lossy().into_owned();
                let target = projects
                    .join(claude_project_slug(&target_cwd))
                    .join(format!("{new_id}.jsonl"));
                let (src, dst, id) = (source.clone(), target.clone(), new_id.clone());
                tokio::task::spawn_blocking(move || {
                    fork_claude_transcript(&src, &dst, &id, &first_cwd, &target_cwd)
                })
                .await
                .map_err(|e| Error::Internal(format!("session copy task failed: {e}")))?
                .map_err(|e| Error::Internal(format!("copy session {source_id}: {e}")))?;
                relocated = Some(target);
                (new_id, true)
            }
            _ if caps.fork => {
                let new_id = intent_acp::session_import::fork_session(
                    adapter.connection(),
                    source_id,
                    worktree,
                    timeout,
                )
                .await
                .map_err(|e| session_error(source_id, &e.to_string()))?;
                (new_id, true)
            }
            _ => (source_id.to_owned(), false),
        };
        drain_now(&mut adapter.notifications);
        let loaded = intent_acp::session::load_session(
            adapter.connection(),
            &session_id,
            worktree,
            Vec::new(),
            None,
        )
        .await;
        if let Err(error) = loaded {
            if let Some(path) = relocated {
                let _ = std::fs::remove_file(path);
            }
            return Err(session_error(source_id, &error.to_string()));
        }
        let notes = collect_replay(&mut adapter.notifications).await;
        let messages = crate::agent_session::replay_to_messages(&notes)
            .into_iter()
            .map(|m| (m.role.to_owned(), Value::Array(m.blocks)))
            .collect();
        Ok(Imported {
            session_id,
            source_cwd,
            forked,
            title,
            messages,
            fingerprint: adapter.fingerprint.clone(),
        })
    }
}

/// The import note an imported agent's first turn carries
/// (`AgentManager::apply_import_handoff`).
pub(crate) fn import_handoff_note(
    provider: &str,
    source_cwd: Option<&str>,
    checkout: Option<&str>,
    workspace_title: Option<&str>,
) -> String {
    let mut lines = vec![format!(
        "This conversation was started in the {provider} CLI and has been imported into Intent. \
         You are now an agent in an Intent workspace{}.",
        workspace_title
            .filter(|t| !t.trim().is_empty())
            .map(|t| format!(" (\"{t}\")"))
            .unwrap_or_default()
    )];
    match (source_cwd.filter(|c| !c.is_empty()), checkout) {
        (Some(from), Some(to)) if from != to => lines.push(format!(
            "Paths in the earlier conversation under {from} now live under {to}; work in {to}."
        )),
        (_, Some(to)) => lines.push(format!("Work in {to}.")),
        _ => {}
    }
    lines.push(
        "Follow Intent's instructions where they conflict with earlier ones. Use the ws.* tools \
         for notes, tasks, other agents and pull requests. Tools named earlier in the \
         conversation may not exist here. Uncommitted changes from the original folder were \
         not copied."
            .to_owned(),
    );
    lines.join("\n")
}

/// What the adapter step produced for the agent.
struct Imported {
    session_id: String,
    source_cwd: Option<String>,
    forked: bool,
    title: Option<String>,
    messages: Vec<(String, Value)>,
    fingerprint: Option<String>,
}

/// A provider error on a session request: "not found"-like messages are
/// `-32602 not-found`, others internal.
fn session_error(session_id: &str, message: &str) -> Error {
    let lower = message.to_lowercase();
    if lower.contains("not found") || lower.contains("no such") || lower.contains("does not exist")
    {
        Error::NotFound(format!("session {session_id}"))
    } else {
        Error::Internal(format!("session {session_id}: {message}"))
    }
}

/// The Claude config folder of a launch: the overlay's `CLAUDE_CONFIG_DIR`,
/// else the daemon's, else `~/.claude`.
fn claude_config_dir(overlay: Option<&intent_acp::ProfileEnv>) -> PathBuf {
    if let Some(overlay) = overlay {
        for (key, value) in overlay.entries() {
            if key == "CLAUDE_CONFIG_DIR" {
                if let Some(value) = value.filter(|v| !v.is_empty()) {
                    return PathBuf::from(value);
                }
            }
        }
    }
    std::env::var_os("CLAUDE_CONFIG_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .filter(|v| !v.is_empty())
                .map(|home| PathBuf::from(home).join(".claude"))
        })
        .unwrap_or_else(|| PathBuf::from(".claude"))
}

/// How one provider's paging ended.
enum ListEnd {
    /// The adapter has no more pages.
    Done,
    /// The rows cover the requested window; the adapter has more.
    Enough,
    PageCap,
    TimedOut,
    Failed(String),
}

/// Start `provider`'s adapter and read `session/list` pages until it has no
/// more, the rows cover `need`, or `deadline` passes. The rows read before a
/// deadline or a failure are kept.
async fn list_one_provider(
    manager: &crate::AgentManager,
    provider: &str,
    cwd: &Path,
    need: usize,
    deadline: tokio::time::Instant,
) -> ProviderListing {
    let budget = provider_timeout();
    let adapter = match tokio::time::timeout_at(
        deadline,
        manager.open_scratch_adapter(provider, cwd),
    )
    .await
    {
        Ok(Ok(adapter)) => adapter,
        Ok(Err(error)) => return ProviderListing::Error(error.to_string()),
        Err(_) => {
            tracing::warn!(
                provider,
                budget_ms = budget.as_millis(),
                "importable sessions listing timed out starting the adapter"
            );
            return ProviderListing::Error("timed out".into());
        }
    };
    if !adapter.capabilities.importable() {
        adapter.close().await;
        return ProviderListing::Unsupported;
    }
    let mut sessions = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    let end = loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let page = tokio::time::timeout(
            remaining,
            intent_acp::session_import::list_sessions(
                adapter.connection(),
                cursor.as_deref(),
                remaining,
            ),
        )
        .await;
        match page {
            Ok(Ok(page)) => {
                pages += 1;
                sessions.extend(page.sessions);
                match page.next_cursor {
                    None => break ListEnd::Done,
                    Some(_) if sessions.len() >= need => break ListEnd::Enough,
                    Some(_) if pages >= MAX_PAGES_PER_PROVIDER => break ListEnd::PageCap,
                    Some(next) => cursor = Some(next),
                }
            }
            Ok(Err(error)) if tokio::time::Instant::now() < deadline => {
                break ListEnd::Failed(error.to_string())
            }
            _ => break ListEnd::TimedOut,
        }
    };
    adapter.close().await;
    let rows = sessions.len();
    match &end {
        ListEnd::Done | ListEnd::Enough => {}
        ListEnd::PageCap => tracing::warn!(
            provider,
            pages,
            rows,
            "importable sessions listing stopped at the page cap"
        ),
        ListEnd::TimedOut => tracing::warn!(
            provider,
            pages,
            rows,
            budget_ms = budget.as_millis(),
            "importable sessions listing timed out; keeping the rows read"
        ),
        ListEnd::Failed(error) => {
            tracing::warn!(provider, pages, rows, %error, "importable sessions listing failed");
        }
    }
    match end {
        ListEnd::TimedOut if sessions.is_empty() => ProviderListing::Error("timed out".into()),
        ListEnd::Failed(error) if sessions.is_empty() => ProviderListing::Error(error),
        end => ProviderListing::Ok(Arc::new(ProviderSessions {
            sessions,
            more: matches!(end, ListEnd::Enough),
            truncated: !matches!(end, ListEnd::Done | ListEnd::Enough),
        })),
    }
}

/// The listed title and folder of `session_id`, best effort (a few pages
/// at most).
async fn find_listed(
    adapter: &ScratchAdapter,
    session_id: &str,
) -> (Option<String>, Option<String>) {
    if !adapter.capabilities.list {
        return (None, None);
    }
    let mut cursor: Option<String> = None;
    for _ in 0..MAX_PAGES_PER_PROVIDER {
        let Ok(page) = intent_acp::session_import::list_sessions(
            adapter.connection(),
            cursor.as_deref(),
            provider_timeout(),
        )
        .await
        else {
            break;
        };
        if let Some(found) = page
            .sessions
            .iter()
            .find(|s| s["sessionId"].as_str() == Some(session_id))
        {
            return listed_fields(found);
        }
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    (None, None)
}

fn drain_now(rx: &mut mpsc::UnboundedReceiver<IncomingNotification>) {
    while rx.try_recv().is_ok() {}
}

/// Every notification already buffered, then stragglers until the channel
/// stays quiet for [`REPLAY_SETTLE`] (at most [`REPLAY_CAP`]).
async fn collect_replay(
    rx: &mut mpsc::UnboundedReceiver<IncomingNotification>,
) -> Vec<IncomingNotification> {
    let mut notes = Vec::new();
    let deadline = tokio::time::Instant::now() + REPLAY_CAP;
    loop {
        while let Ok(note) = rx.try_recv() {
            notes.push(note);
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(REPLAY_SETTLE.min(remaining), rx.recv()).await {
            Ok(Some(note)) => notes.push(note),
            Ok(None) | Err(_) => break,
        }
    }
    notes
}

/// Bound on one `git rev-parse --show-toplevel` for a listed session's cwd.
const REPO_ROOT_TIMEOUT: Duration = Duration::from_secs(2);

/// The git top-level of `cwd` on this host, or `None` when `cwd` is not an
/// absolute existing folder, is not inside a git work tree, or git fails or
/// exceeds [`REPO_ROOT_TIMEOUT`]. Runs `git` directly (no shell) with the
/// caller's `GIT_*` location variables removed so they cannot redirect it.
pub(crate) async fn resolve_repo_root(cwd: &str) -> Option<String> {
    let path = Path::new(cwd);
    if cwd.is_empty() || !path.is_absolute() || !path.is_dir() {
        return None;
    }
    let mut command = tokio::process::Command::new("git");
    command
        .arg("-C")
        .arg(path)
        .args(["rev-parse", "--show-toplevel"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("LC_ALL", "C")
        .kill_on_drop(true);
    for key in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_COMMON_DIR",
        "GIT_INDEX_FILE",
        "GIT_CONFIG",
        "GIT_CEILING_DIRECTORIES",
        "GIT_DISCOVERY_ACROSS_FILESYSTEM",
    ] {
        command.env_remove(key);
    }
    let output = tokio::time::timeout(REPO_ROOT_TIMEOUT, command.output())
        .await
        .ok()?
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let top = String::from_utf8(output.stdout).ok()?;
    let top = top.strip_suffix('\n').unwrap_or(&top);
    (!top.is_empty() && !top.contains(['\n', '\r', '\0']) && Path::new(top).is_absolute())
        .then(|| top.to_owned())
}

/// Sets `repoRoot` on each wire row whose `cwd` resolves to a git work
/// tree. Each distinct cwd is resolved once, all in parallel.
async fn attach_repo_roots(page: &mut [Value]) {
    let mut distinct: Vec<String> = page
        .iter()
        .filter_map(|row| row["cwd"].as_str().filter(|c| !c.is_empty()))
        .map(str::to_owned)
        .collect();
    distinct.sort();
    distinct.dedup();
    let mut tasks = tokio::task::JoinSet::new();
    for cwd in distinct {
        tasks.spawn(async move {
            let root = resolve_repo_root(&cwd).await;
            (cwd, root)
        });
    }
    let mut roots = std::collections::HashMap::new();
    while let Some(joined) = tasks.join_next().await {
        if let Ok((cwd, Some(root))) = joined {
            roots.insert(cwd, root);
        }
    }
    for row in page.iter_mut() {
        if let Some(root) = row["cwd"].as_str().and_then(|c| roots.get(c)) {
            row["repoRoot"] = json!(root);
        }
    }
}

/// Shared by the unit tests.
#[cfg(test)]
pub(crate) fn wire_row(provider: &str, raw: Value) -> Value {
    let updated_at = raw["updatedAt"].as_str().map(str::to_owned);
    Listed {
        provider: provider.into(),
        raw,
        updated_at,
    }
    .to_wire()
}

#[cfg(test)]
mod tests;

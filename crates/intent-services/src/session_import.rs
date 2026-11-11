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
//!   parallel (each bounded by a timeout), merged newest first;
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

use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
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

/// Outcome of listing one provider.
enum ProviderListing {
    Ok(Vec<Value>),
    Unsupported,
    Error(String),
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
        let limit = params.limit.unwrap_or(DEFAULT_LIMIT);
        if limit == 0 || limit > MAX_LIMIT {
            return Err(Error::InvalidParams(format!(
                "limit: must be between 1 and {MAX_LIMIT}"
            )));
        }
        let offset = decode_cursor(params.cursor.as_deref())?;
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
                let listing = match tokio::time::timeout(
                    provider_timeout(),
                    list_one_provider(&manager, &provider, &cwd),
                )
                .await
                {
                    Ok(listing) => listing,
                    Err(_) => ProviderListing::Error("timed out".into()),
                };
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
        for (provider, listing) in candidates.iter().zip(listings) {
            let label = intent_providers::provider_config(provider).display_name;
            match listing {
                ProviderListing::Ok(sessions) => {
                    providers.push(json!({ "id": provider, "label": label, "status": "ok" }));
                    rows.extend(sessions.into_iter().filter_map(|raw| {
                        raw["sessionId"].as_str().filter(|id| !id.is_empty())?;
                        let updated_at = raw["updatedAt"].as_str().map(str::to_owned);
                        Some(Listed {
                            provider: provider.clone(),
                            raw,
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
        if let Some(query) = params
            .query
            .as_deref()
            .map(str::trim)
            .filter(|q| !q.is_empty())
        {
            let needle = query.to_lowercase();
            rows.retain(|row| row.matches(&needle));
        }
        // Newest first; ISO-8601 UTC strings order lexically. Undated last.
        rows.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        let page: Vec<Value> = rows
            .iter()
            .skip(offset)
            .take(limit)
            .map(Listed::to_wire)
            .collect();
        let mut out = json!({ "sessions": page, "providers": providers });
        if offset + limit < rows.len() {
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
        let (title, listed_cwd) = find_listed(adapter, source_id).await;
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

async fn list_one_provider(
    manager: &crate::AgentManager,
    provider: &str,
    cwd: &Path,
) -> ProviderListing {
    let adapter = match manager.open_scratch_adapter(provider, cwd).await {
        Ok(adapter) => adapter,
        Err(error) => return ProviderListing::Error(error.to_string()),
    };
    let listing = if adapter.capabilities.importable() {
        let mut sessions = Vec::new();
        let mut cursor: Option<String> = None;
        let mut failure = None;
        for _ in 0..MAX_PAGES_PER_PROVIDER {
            match intent_acp::session_import::list_sessions(
                adapter.connection(),
                cursor.as_deref(),
                provider_timeout(),
            )
            .await
            {
                Ok(page) => {
                    sessions.extend(page.sessions);
                    match page.next_cursor {
                        Some(next) => cursor = Some(next),
                        None => break,
                    }
                }
                Err(error) => {
                    failure = Some(error.to_string());
                    break;
                }
            }
        }
        match failure {
            Some(error) if sessions.is_empty() => ProviderListing::Error(error),
            _ => ProviderListing::Ok(sessions),
        }
    } else {
        ProviderListing::Unsupported
    };
    adapter.close().await;
    listing
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
            let title = found["title"]
                .as_str()
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .map(str::to_owned);
            let cwd = found["cwd"]
                .as_str()
                .filter(|c| !c.is_empty())
                .map(str::to_owned);
            return (title, cwd);
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

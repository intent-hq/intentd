//! Agent secret requests (PROTOCOL §5.56).
//!
//! A top-level agent asks the user for a secret with `ws.secret.request`; the
//! user answers through a masked card that calls `secret.provide` straight to
//! the daemon, so the value never enters the transcript or the model. The
//! value then reaches only that agent's `ws.host.exec` calls through
//! `secretEnv`, with exact matches redacted from their output.
//!
//! **Memory only.** [`SecretVault`] is the single owner of every value and
//! pending request. Nothing here is persisted, logged or put into an event:
//! values live in [`Zeroizing`] buffers and are wiped when dropped (forget,
//! agent delete/retire, workspace archive/delete, daemon restart).
//!
//! **Owned by the requesting agent.** Entries are keyed by
//! `(workspaceId, agentId)`; another agent — including the owner's own
//! sub-agents — can neither list nor use them, and gets the same
//! `secret-unavailable` error as for a secret that was never provided.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Mutex, MutexGuard, PoisonError};

use intent_core::{AgentId, Error, MessageOrigin, Result, WorkspaceApi, WorkspaceId};
use serde_json::{json, Map, Value};
use zeroize::Zeroizing;

use crate::Services;

/// Most pending (unanswered) requests one agent may hold.
pub(crate) const MAX_PENDING_PER_AGENT: usize = 8;
/// Largest accepted secret value, in UTF-8 bytes.
pub(crate) const MAX_VALUE_BYTES: usize = 16 * 1024;
/// Longest accepted request reason, in characters.
pub(crate) const MAX_REASON_CHARS: usize = 500;
/// Longest accepted secret name.
const MAX_NAME_LEN: usize = 64;
/// Most `secretEnv` entries one `ws.host.exec` call may name.
const MAX_SECRET_ENV_ENTRIES: usize = 32;
/// Answered outcomes remembered per agent for `secret.list` (oldest dropped).
const MAX_ANSWERED_PER_AGENT: usize = 32;

/// `messageMetadata.type` of the provided notice.
pub(crate) const SECRET_PROVIDED_METADATA_TYPE: &str = "secret_provided";
/// `messageMetadata.type` of the declined notice.
pub(crate) const SECRET_DECLINED_METADATA_TYPE: &str = "secret_declined";

/// `error.data.code` for an unknown, answered, lost or foreign request.
pub(crate) const CODE_REQUEST_UNKNOWN: &str = "secret-request-unknown";
/// `error.data.code` for malformed input.
pub(crate) const CODE_INVALID_PARAMS: &str = "invalid-params";
/// Prefix and code of the agent-facing missing-secret error.
pub(crate) const CODE_UNAVAILABLE: &str = "secret-unavailable";

/// Validate a secret name: 1–64 characters of `A`–`Z`, `0`–`9` and `_`,
/// starting with a letter. Never trimmed or case-folded.
pub(crate) fn validate_secret_name(name: &str) -> std::result::Result<(), String> {
    let mut chars = name.chars();
    let valid = name.len() <= MAX_NAME_LEN
        && chars.next().is_some_and(|c| c.is_ascii_uppercase())
        && chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
    if valid {
        Ok(())
    } else {
        Err(format!(
            "invalid secret name {name:?}: use 1-{MAX_NAME_LEN} characters of A-Z, 0-9 and _, starting with a letter (for example OPENROUTER_API_KEY)"
        ))
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::SecretRequest {
        code: CODE_INVALID_PARAMS,
        message: message.into(),
    }
}

fn unknown_request(request_id: &str) -> Error {
    Error::SecretRequest {
        code: CODE_REQUEST_UNKNOWN,
        message: format!(
            "secret request {request_id} is not pending: it was already answered, or it was lost when Intent restarted. Ask the agent to request the secret again."
        ),
    }
}

/// The agent-facing error for a secret this agent does not hold. Identical
/// whether the secret was never provided, forgotten, lost on restart, or
/// belongs to another agent.
pub(crate) fn unavailable(name: &str) -> Error {
    Error::SecretRequest {
        code: CODE_UNAVAILABLE,
        message: format!(
            "{CODE_UNAVAILABLE}: secret {name} is not available to this agent. Secrets are held in memory and are lost when Intent restarts. Call ws.secret.request({{ name: \"{name}\", reason }}) and end your turn."
        ),
    }
}

/// How a request was answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    Provided,
    Declined,
}

impl Outcome {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Provided => "provided",
            Self::Declined => "declined",
        }
    }
}

struct Pending {
    request_id: String,
    name: String,
}

struct Answered {
    request_id: String,
    name: String,
    outcome: Outcome,
}

#[derive(Default)]
struct AgentEntry {
    values: BTreeMap<String, Zeroizing<String>>,
    pending: Vec<Pending>,
    answered: VecDeque<Answered>,
}

impl AgentEntry {
    fn is_empty(&self) -> bool {
        self.values.is_empty() && self.pending.is_empty() && self.answered.is_empty()
    }
}

type Key = (WorkspaceId, AgentId);

#[derive(Default)]
struct VaultState {
    agents: HashMap<Key, AgentEntry>,
    /// Pending request id → owner, for `secret.provide` / `secret.dismiss`.
    requests: HashMap<String, Key>,
}

/// Result of [`SecretVault::request`].
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RequestTicket {
    pub(crate) request_id: String,
    /// `false` when an existing pending request for the same name was reused
    /// (no new card should be shown).
    pub(crate) created: bool,
}

/// Why [`SecretVault::request`] refused.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RequestRefusal {
    TooManyPending,
}

/// The owner of an answered request.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Answer {
    pub(crate) agent_id: AgentId,
    pub(crate) name: String,
}

/// Names-only view of one agent's vault entry.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Listing {
    pub(crate) secrets: Vec<String>,
    pub(crate) pending: Vec<(String, String)>,
    pub(crate) answered: Vec<(String, String, Outcome)>,
}

impl Listing {
    pub(crate) fn to_json(&self) -> Value {
        json!({
            "secrets": self.secrets.iter().map(|name| json!({ "name": name })).collect::<Vec<_>>(),
            "pendingRequests": self
                .pending
                .iter()
                .map(|(request_id, name)| json!({ "requestId": request_id, "name": name }))
                .collect::<Vec<_>>(),
            "answeredRequests": self
                .answered
                .iter()
                .map(|(request_id, name, outcome)| {
                    json!({ "requestId": request_id, "name": name, "outcome": outcome.as_str() })
                })
                .collect::<Vec<_>>(),
        })
    }
}

/// In-memory store of agent-owned secrets and pending requests.
#[derive(Default)]
pub(crate) struct SecretVault {
    state: Mutex<VaultState>,
}

fn mint_request_id() -> String {
    let hex = uuid::Uuid::new_v4().simple().to_string();
    format!("sec-{}", &hex[..16])
}

impl SecretVault {
    fn lock(&self) -> MutexGuard<'_, VaultState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Record a pending request for `name`, or reuse the agent's existing
    /// pending request for the same name. The name must already be valid.
    pub(crate) fn request(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        name: &str,
    ) -> std::result::Result<RequestTicket, RequestRefusal> {
        let mut state = self.lock();
        let key = (workspace_id.clone(), agent_id.clone());
        let entry = state.agents.entry(key.clone()).or_default();
        if let Some(existing) = entry.pending.iter().find(|p| p.name == name) {
            return Ok(RequestTicket {
                request_id: existing.request_id.clone(),
                created: false,
            });
        }
        if entry.pending.len() >= MAX_PENDING_PER_AGENT {
            return Err(RequestRefusal::TooManyPending);
        }
        let request_id = mint_request_id();
        entry.pending.push(Pending {
            request_id: request_id.clone(),
            name: name.to_string(),
        });
        state.requests.insert(request_id.clone(), key);
        Ok(RequestTicket {
            request_id,
            created: true,
        })
    }

    /// The owner of a pending request in `workspace_id`, without answering it.
    pub(crate) fn pending_owner(
        &self,
        workspace_id: &WorkspaceId,
        request_id: &str,
    ) -> Option<Answer> {
        let state = self.lock();
        let key = state.requests.get(request_id)?;
        if &key.0 != workspace_id {
            return None;
        }
        let entry = state.agents.get(key)?;
        let pending = entry.pending.iter().find(|p| p.request_id == request_id)?;
        Some(Answer {
            agent_id: key.1.clone(),
            name: pending.name.clone(),
        })
    }

    /// Answer a pending request exactly once. `value` is stored for
    /// [`Outcome::Provided`]. A request from another workspace, or one that
    /// is no longer pending, returns `None` and changes nothing.
    pub(crate) fn answer(
        &self,
        workspace_id: &WorkspaceId,
        request_id: &str,
        outcome: Outcome,
        value: Option<Zeroizing<String>>,
    ) -> Option<Answer> {
        let mut state = self.lock();
        let key = state.requests.get(request_id)?.clone();
        if key.0 != *workspace_id {
            return None;
        }
        state.requests.remove(request_id);
        let entry = state.agents.get_mut(&key)?;
        let index = entry
            .pending
            .iter()
            .position(|p| p.request_id == request_id)?;
        let pending = entry.pending.remove(index);
        if let (Outcome::Provided, Some(value)) = (outcome, value) {
            entry.values.insert(pending.name.clone(), value);
        }
        if entry.answered.len() >= MAX_ANSWERED_PER_AGENT {
            entry.answered.pop_front();
        }
        entry.answered.push_back(Answered {
            request_id: pending.request_id,
            name: pending.name.clone(),
            outcome,
        });
        Some(Answer {
            agent_id: key.1,
            name: pending.name,
        })
    }

    /// Names-only listing of one agent's entry.
    pub(crate) fn list(&self, workspace_id: &WorkspaceId, agent_id: &AgentId) -> Listing {
        let state = self.lock();
        let Some(entry) = state.agents.get(&(workspace_id.clone(), agent_id.clone())) else {
            return Listing::default();
        };
        Listing {
            secrets: entry.values.keys().cloned().collect(),
            pending: entry
                .pending
                .iter()
                .map(|p| (p.request_id.clone(), p.name.clone()))
                .collect(),
            answered: entry
                .answered
                .iter()
                .map(|a| (a.request_id.clone(), a.name.clone(), a.outcome))
                .collect(),
        }
    }

    /// Drop one held secret. Returns whether it existed.
    pub(crate) fn forget(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        name: &str,
    ) -> bool {
        let mut state = self.lock();
        let key = (workspace_id.clone(), agent_id.clone());
        let Some(entry) = state.agents.get_mut(&key) else {
            return false;
        };
        let forgotten = entry.values.remove(name).is_some();
        if entry.is_empty() {
            state.agents.remove(&key);
        }
        forgotten
    }

    /// Resolve every named secret for the agent, or report the first missing
    /// name. Returned values are copies in wiping buffers.
    pub(crate) fn resolve(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        names: &[&str],
    ) -> std::result::Result<Vec<(String, Zeroizing<String>)>, String> {
        let state = self.lock();
        let entry = state.agents.get(&(workspace_id.clone(), agent_id.clone()));
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            match entry.and_then(|e| e.values.get(*name)) {
                Some(value) => out.push(((*name).to_string(), value.clone())),
                None => return Err((*name).to_string()),
            }
        }
        Ok(out)
    }

    /// Drop everything one agent holds, in every workspace.
    pub(crate) fn clear_agent(&self, agent_id: &AgentId) {
        let mut state = self.lock();
        state.agents.retain(|key, _| &key.1 != agent_id);
        state.requests.retain(|_, key| &key.1 != agent_id);
    }

    /// Drop everything held in one workspace.
    pub(crate) fn clear_workspace(&self, workspace_id: &WorkspaceId) {
        let mut state = self.lock();
        state.agents.retain(|key, _| &key.0 != workspace_id);
        state.requests.retain(|_, key| &key.0 != workspace_id);
    }
}

/// Replace every exact occurrence of a secret value with `[secret:NAME]` in
/// one left-to-right pass. At each position the **longest** matching value
/// wins, and replaced text is never scanned again, so a value containing
/// another is hidden whole. Empty values are ignored.
pub(crate) fn redact(text: &str, secrets: &[(String, Zeroizing<String>)]) -> String {
    let (names, values): (Vec<&str>, Vec<&str>) = secrets
        .iter()
        .filter(|(_, value)| !value.is_empty())
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .unzip();
    if values.is_empty() {
        return text.to_string();
    }
    // Leftmost-longest, non-overlapping: at each position the longest value
    // wins and replaced text is never rescanned — one linear pass.
    let Ok(matcher) = aho_corasick::AhoCorasick::builder()
        .match_kind(aho_corasick::MatchKind::LeftmostLongest)
        .build(&values)
    else {
        // Unreachable for these bounded inputs; fail closed rather than
        // return unredacted output.
        return "[secret output withheld: redaction unavailable]".to_string();
    };
    let mut out = String::with_capacity(text.len());
    let mut last = 0;
    for found in matcher.find_iter(text) {
        out.push_str(&text[last..found.start()]);
        out.push_str("[secret:");
        out.push_str(names[found.pattern().as_usize()]);
        out.push(']');
        last = found.end();
    }
    out.push_str(&text[last..]);
    out
}

/// Parse a `secretEnv` option: an array of names (each variable is named
/// like its secret) or an object mapping a variable name to a secret name.
/// Returns `(variable, secret name)` pairs.
pub(crate) fn parse_secret_env(value: &Value) -> Result<Vec<(String, String)>> {
    let pairs: Vec<(String, String)> = match value {
        Value::Array(items) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(|name| (name.to_string(), name.to_string()))
                    .ok_or_else(|| invalid("secretEnv must be an array of secret names or an object mapping variable names to secret names"))
            })
            .collect::<Result<_>>()?,
        Value::Object(map) => map
            .iter()
            .map(|(variable, name)| {
                name.as_str()
                    .map(|name| (variable.clone(), name.to_string()))
                    .ok_or_else(|| invalid(format!("secretEnv.{variable} must be a secret name string")))
            })
            .collect::<Result<_>>()?,
        _ => {
            return Err(invalid(
                "secretEnv must be an array of secret names or an object mapping variable names to secret names",
            ))
        }
    };
    if pairs.len() > MAX_SECRET_ENV_ENTRIES {
        return Err(invalid(format!(
            "secretEnv may name at most {MAX_SECRET_ENV_ENTRIES} secrets"
        )));
    }
    for (variable, name) in &pairs {
        if variable.is_empty() || variable.contains(['=', '\0']) {
            return Err(invalid(format!(
                "secretEnv variable name {variable:?} is not a valid environment variable name"
            )));
        }
        validate_secret_name(name).map_err(invalid)?;
    }
    Ok(pairs)
}

fn validate_reason(reason: &str) -> Result<String> {
    let reason = reason.trim();
    if reason.is_empty() {
        return Err(invalid(
            "reason is required: say why the agent needs the secret",
        ));
    }
    if reason.chars().count() > MAX_REASON_CHARS {
        return Err(invalid(format!(
            "reason must be at most {MAX_REASON_CHARS} characters"
        )));
    }
    Ok(reason.to_string())
}

fn provided_notice(name: &str) -> String {
    format!(
        "The user provided secret {name}. Use it with ws.host.exec and secretEnv: [\"{name}\"]; its value is never shown to you."
    )
}

fn declined_notice(name: &str) -> String {
    format!("The user declined to provide secret {name}.")
}

impl Services {
    /// Resolve `agent_id`'s session and require it to live in `workspace_id`.
    async fn secret_agent_session(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
    ) -> Result<intent_core::AgentSession> {
        let session = self.store.get_agent_session_summary(agent_id).await?;
        if &session.workspace_id != workspace_id {
            return Err(Error::NotFound(format!("agent session {agent_id}")));
        }
        Ok(session)
    }

    /// `ws.secret.request` (MCP-only): validate and record a pending request
    /// for the calling top-level agent. The binding registers the turn-end
    /// card only when `created` is true.
    pub(crate) async fn secret_request_op(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        name: String,
        reason: String,
    ) -> Result<Value> {
        validate_secret_name(&name).map_err(invalid)?;
        let reason = validate_reason(&reason)?;
        let session = self.secret_agent_session(&workspace_id, &agent_id).await?;
        if session.parent_agent_id.is_some() || session.is_background {
            return Err(invalid(
                "only top-level agents can request secrets; report the need to your parent instead",
            ));
        }
        let ticket = self
            .secret_vault
            .request(&workspace_id, &agent_id, &name)
            .map_err(|RequestRefusal::TooManyPending| {
                invalid(format!(
                    "this agent already has {MAX_PENDING_PER_AGENT} unanswered secret requests; end your turn and wait for the user to answer them"
                ))
            })?;
        // A delete or retire that cleared the vault between the session read
        // above and the insert would orphan this request: re-check and drop
        // it so nothing outlives the agent.
        let alive = matches!(
            self.store.get_agent_session_summary(&agent_id).await,
            Ok(session) if session.retired_at.is_none()
        );
        if !alive {
            self.secret_vault.clear_agent(&agent_id);
            return Err(Error::NotFound(format!("agent session {agent_id}")));
        }
        Ok(json!({
            "requestId": ticket.request_id,
            "agentId": agent_id.0,
            "name": name,
            "reason": reason,
            "created": ticket.created,
        }))
    }

    /// `secret.list` / `ws.secret.list`: names only.
    pub(crate) async fn secret_list_op(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> Result<Value> {
        self.secret_agent_session(&workspace_id, &agent_id).await?;
        Ok(self.secret_vault.list(&workspace_id, &agent_id).to_json())
    }

    /// `secret.forget` / `ws.secret.forget`.
    pub(crate) async fn secret_forget_op(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        name: String,
    ) -> Result<Value> {
        validate_secret_name(&name).map_err(invalid)?;
        self.secret_agent_session(&workspace_id, &agent_id).await?;
        let forgotten = self.secret_vault.forget(&workspace_id, &agent_id, &name);
        Ok(json!({ "ok": true, "forgotten": forgotten }))
    }

    /// `secret.provide` / `secret.dismiss`: answer a pending request once,
    /// after checking the caller may message the owning agent, then deliver
    /// the user-origin notice (never the value).
    pub(crate) async fn secret_answer_op(
        &self,
        workspace_id: WorkspaceId,
        request_id: String,
        outcome: Outcome,
        value: Option<Zeroizing<String>>,
    ) -> Result<Value> {
        if let Some(value) = value.as_ref() {
            if value.is_empty() {
                return Err(invalid("value must be a non-empty string"));
            }
            if value.len() > MAX_VALUE_BYTES {
                return Err(invalid(format!(
                    "value must be at most {MAX_VALUE_BYTES} bytes"
                )));
            }
        }
        let owner = self
            .secret_vault
            .pending_owner(&workspace_id, &request_id)
            .ok_or_else(|| unknown_request(&request_id))?;
        self.require_agent_member_in(&owner.agent_id, &workspace_id)
            .await?;
        // Never store a value for an agent that is gone or retired: drop its
        // leftovers and report the request as unknown.
        let alive = matches!(
            self.secret_agent_session(&workspace_id, &owner.agent_id).await,
            Ok(session) if session.retired_at.is_none()
        );
        if !alive {
            self.secret_vault.clear_agent(&owner.agent_id);
            return Err(unknown_request(&request_id));
        }
        let answer = self
            .secret_vault
            .answer(&workspace_id, &request_id, outcome, value)
            .ok_or_else(|| unknown_request(&request_id))?;
        let (content, metadata_type) = match outcome {
            Outcome::Provided => (provided_notice(&answer.name), SECRET_PROVIDED_METADATA_TYPE),
            Outcome::Declined => (declined_notice(&answer.name), SECRET_DECLINED_METADATA_TYPE),
        };
        let metadata = json!({
            "type": metadata_type,
            "source": "system",
            "requestId": request_id,
            "name": answer.name,
        });
        if let Err(error) = WorkspaceApi::agent_send_message(
            self,
            workspace_id,
            answer.agent_id.clone(),
            content,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(metadata),
            MessageOrigin::User,
        )
        .await
        {
            tracing::warn!(
                agent = %answer.agent_id,
                request = %request_id,
                error = %error,
                "secret request notice delivery failed"
            );
        }
        Ok(json!({ "ok": true, "requestId": request_id, "name": answer.name }))
    }

    /// `ws.host.exec` from an agent's own turn: resolve `secretEnv` from the
    /// caller's own secrets, overlay them last on `env`, run, and redact the
    /// resolved values from `stdout` / `stderr`. Without `secretEnv` (or with
    /// `secretEnv: null`) this is the plain workspace `host.exec`. Wiping is
    /// best-effort: the transient copies handed to the child's environment
    /// are ordinary strings, freed when the call returns.
    pub(crate) async fn agent_host_exec_op(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        params: Value,
    ) -> Result<Value> {
        let mut map = match params {
            Value::Object(map) => map,
            Value::Null => Map::new(),
            _ => {
                return Err(Error::InvalidParams(
                    "host.exec args must be an object".to_string(),
                ))
            }
        };
        let spec = match map.remove("secretEnv") {
            None | Some(Value::Null) => {
                return WorkspaceApi::host_exec(self, workspace_id, Value::Object(map)).await
            }
            Some(spec) => spec,
        };
        let pairs = parse_secret_env(&spec)?;
        let names: Vec<&str> = pairs.iter().map(|(_, name)| name.as_str()).collect();
        let resolved = self
            .secret_vault
            .resolve(&workspace_id, &agent_id, &names)
            .map_err(|missing| unavailable(&missing))?;
        let mut env = match map.remove("env") {
            None | Some(Value::Null) => Map::new(),
            Some(Value::Object(env)) => env,
            Some(_) => {
                return Err(Error::InvalidParams(
                    "Invalid parameter: env must be an object of string values".to_string(),
                ))
            }
        };
        let by_name: HashMap<&str, &Zeroizing<String>> = resolved
            .iter()
            .map(|(name, value)| (name.as_str(), value))
            .collect();
        for (variable, name) in &pairs {
            env.insert(
                variable.clone(),
                Value::String(by_name[name.as_str()].as_str().to_string()),
            );
        }
        map.insert("env".to_string(), Value::Object(env));
        let mut result = WorkspaceApi::host_exec(self, workspace_id, Value::Object(map)).await?;
        for stream in ["stdout", "stderr"] {
            if let Some(text) = result.get(stream).and_then(Value::as_str) {
                let redacted = redact(text, &resolved);
                result[stream] = Value::String(redacted);
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws(id: &str) -> WorkspaceId {
        WorkspaceId::from_string(id)
    }

    fn agent(id: &str) -> AgentId {
        AgentId::from_string(id)
    }

    fn secret(value: &str) -> Zeroizing<String> {
        Zeroizing::new(value.to_string())
    }

    #[test]
    fn names_are_validated_strictly() {
        for good in ["A", "OPENROUTER_API_KEY", "K9", &"A".repeat(64)] {
            assert!(validate_secret_name(good).is_ok(), "{good}");
        }
        for bad in [
            "",
            "lower",
            "9START",
            "_LEADING",
            " KEY",
            "KEY ",
            "KEY-DASH",
            "KÉY",
            &"A".repeat(65),
        ] {
            assert!(validate_secret_name(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn request_reuses_pending_name_and_caps_open_requests() {
        let vault = SecretVault::default();
        let (w, a) = (ws("w"), agent("a"));
        let first = vault.request(&w, &a, "KEY").unwrap();
        assert!(first.created);
        assert!(first.request_id.starts_with("sec-"));
        let again = vault.request(&w, &a, "KEY").unwrap();
        assert_eq!(again.request_id, first.request_id);
        assert!(!again.created);
        for i in 1..MAX_PENDING_PER_AGENT {
            assert!(vault.request(&w, &a, &format!("K{i}")).unwrap().created);
        }
        assert_eq!(
            vault.request(&w, &a, "ONE_TOO_MANY"),
            Err(RequestRefusal::TooManyPending)
        );
        assert!(vault.request(&w, &agent("b"), "KEY").unwrap().created);
    }

    #[test]
    fn answer_is_single_use_and_workspace_bound() {
        let vault = SecretVault::default();
        let (w, a) = (ws("w"), agent("a"));
        let ticket = vault.request(&w, &a, "KEY").unwrap();
        assert!(vault
            .answer(
                &ws("other"),
                &ticket.request_id,
                Outcome::Provided,
                Some(secret("v"))
            )
            .is_none());
        assert!(vault
            .pending_owner(&ws("other"), &ticket.request_id)
            .is_none());
        assert_eq!(
            vault.pending_owner(&w, &ticket.request_id),
            Some(Answer {
                agent_id: a.clone(),
                name: "KEY".into()
            })
        );
        let answer = vault
            .answer(&w, &ticket.request_id, Outcome::Provided, Some(secret("v")))
            .unwrap();
        assert_eq!(
            answer,
            Answer {
                agent_id: a.clone(),
                name: "KEY".into()
            }
        );
        assert!(vault
            .answer(
                &w,
                &ticket.request_id,
                Outcome::Provided,
                Some(secret("v2"))
            )
            .is_none());
        let listing = vault.list(&w, &a);
        assert_eq!(listing.secrets, vec!["KEY".to_string()]);
        assert!(listing.pending.is_empty());
        assert_eq!(
            listing.answered,
            vec![(
                ticket.request_id.clone(),
                "KEY".to_string(),
                Outcome::Provided
            )]
        );
        let resolved = vault.resolve(&w, &a, &["KEY"]).unwrap();
        assert_eq!(resolved[0].1.as_str(), "v");
    }

    #[test]
    fn declined_requests_hold_no_value() {
        let vault = SecretVault::default();
        let (w, a) = (ws("w"), agent("a"));
        let ticket = vault.request(&w, &a, "KEY").unwrap();
        vault
            .answer(&w, &ticket.request_id, Outcome::Declined, None)
            .unwrap();
        let listing = vault.list(&w, &a);
        assert!(listing.secrets.is_empty());
        assert_eq!(listing.answered[0].2, Outcome::Declined);
        assert_eq!(vault.resolve(&w, &a, &["KEY"]), Err("KEY".to_string()));
    }

    #[test]
    fn secrets_belong_to_the_requesting_agent_only() {
        let vault = SecretVault::default();
        let (w, a, b) = (ws("w"), agent("a"), agent("b"));
        let ticket = vault.request(&w, &a, "KEY").unwrap();
        vault
            .answer(&w, &ticket.request_id, Outcome::Provided, Some(secret("v")))
            .unwrap();
        assert_eq!(vault.resolve(&w, &b, &["KEY"]), Err("KEY".to_string()));
        assert_eq!(vault.list(&w, &b), Listing::default());
        assert!(!vault.forget(&w, &b, "KEY"));
        assert_eq!(
            vault.resolve(&ws("w2"), &a, &["KEY"]),
            Err("KEY".to_string())
        );
        assert!(vault.resolve(&w, &a, &["KEY"]).is_ok());
    }

    #[test]
    fn forget_and_clears_drop_values_and_requests() {
        let vault = SecretVault::default();
        let (w, a, b) = (ws("w"), agent("a"), agent("b"));
        let ta = vault.request(&w, &a, "KEY").unwrap();
        vault
            .answer(&w, &ta.request_id, Outcome::Provided, Some(secret("v")))
            .unwrap();
        assert!(vault.forget(&w, &a, "KEY"));
        assert!(!vault.forget(&w, &a, "KEY"));
        assert!(vault.resolve(&w, &a, &["KEY"]).is_err());

        let pending_a = vault.request(&w, &a, "OTHER").unwrap();
        let tb = vault.request(&w, &b, "KEY").unwrap();
        vault.clear_agent(&a);
        assert!(vault.pending_owner(&w, &pending_a.request_id).is_none());
        assert_eq!(vault.list(&w, &a), Listing::default());
        assert!(vault.pending_owner(&w, &tb.request_id).is_some());

        vault.clear_workspace(&w);
        assert!(vault.pending_owner(&w, &tb.request_id).is_none());
        assert_eq!(vault.list(&w, &b), Listing::default());
    }

    #[test]
    fn answered_history_is_bounded() {
        let vault = SecretVault::default();
        let (w, a) = (ws("w"), agent("a"));
        for i in 0..(MAX_ANSWERED_PER_AGENT + 5) {
            let ticket = vault.request(&w, &a, &format!("K{i}")).unwrap();
            vault
                .answer(&w, &ticket.request_id, Outcome::Declined, None)
                .unwrap();
        }
        let listing = vault.list(&w, &a);
        assert_eq!(listing.answered.len(), MAX_ANSWERED_PER_AGENT);
        assert_eq!(listing.answered[0].1, "K5");
    }

    #[test]
    fn redaction_hides_exact_values_longest_first_in_one_pass() {
        let secrets = vec![
            ("A".to_string(), secret("abc123")),
            ("B".to_string(), secret("abc123xyz")),
        ];
        assert_eq!(
            redact("x=abc123xyz y=abc123 z=abc12", &secrets),
            "x=[secret:B] y=[secret:A] z=abc12"
        );
        let placeholder_like = vec![
            ("P".to_string(), secret("[secret:A]")),
            ("A".to_string(), secret("tok")),
        ];
        assert_eq!(
            redact("tok [secret:A]", &placeholder_like),
            "[secret:A] [secret:P]"
        );
        assert_eq!(
            redact("héllo wörld", &[("W".to_string(), secret("wö"))]),
            "héllo [secret:W]rld"
        );
        assert_eq!(redact("abc", &[("E".to_string(), secret(""))]), "abc");
        assert_eq!(
            redact("aaaa", &[("A".to_string(), secret("aa"))]),
            "[secret:A][secret:A]"
        );
        assert_eq!(redact("", &secrets), "");
    }

    #[test]
    fn secret_env_accepts_names_or_variable_maps() {
        assert_eq!(
            parse_secret_env(&json!(["KEY", "OTHER"])).unwrap(),
            vec![
                ("KEY".into(), "KEY".into()),
                ("OTHER".into(), "OTHER".into())
            ]
        );
        assert_eq!(
            parse_secret_env(&json!({ "OPENAI_API_KEY": "MY_KEY" })).unwrap(),
            vec![("OPENAI_API_KEY".into(), "MY_KEY".into())]
        );
        for bad in [
            json!("KEY"),
            json!([1]),
            json!(["lower"]),
            json!({ "": "KEY" }),
            json!({ "A=B": "KEY" }),
            json!({ "VAR": 1 }),
        ] {
            let err = parse_secret_env(&bad).unwrap_err();
            assert!(
                matches!(
                    err,
                    Error::SecretRequest {
                        code: CODE_INVALID_PARAMS,
                        ..
                    }
                ),
                "{bad}: {err}"
            );
        }
        let too_many: Vec<String> = (0..=MAX_SECRET_ENV_ENTRIES)
            .map(|i| format!("K{i}"))
            .collect();
        assert!(parse_secret_env(&json!(too_many)).is_err());
    }

    #[test]
    fn reasons_are_trimmed_and_bounded() {
        assert_eq!(
            validate_reason("  needed for tests ").unwrap(),
            "needed for tests"
        );
        assert!(validate_reason("   ").is_err());
        assert!(validate_reason(&"x".repeat(MAX_REASON_CHARS)).is_ok());
        assert!(validate_reason(&"x".repeat(MAX_REASON_CHARS + 1)).is_err());
    }

    #[test]
    fn feature_snapshots_carry_secret_requests_only_when_on() {
        use intent_core::settings_file::AgentFeaturesSettings;
        let off =
            crate::agent_ops::agent_features_snapshot(&AgentFeaturesSettings::default()).unwrap();
        assert!(off.get("secretRequests").is_none(), "{off}");
        let decoded: AgentFeaturesSettings = serde_json::from_value(off).unwrap();
        assert!(!decoded.secret_requests);
        let on = crate::agent_ops::agent_features_snapshot(&AgentFeaturesSettings {
            secret_requests: true,
            ..AgentFeaturesSettings::default()
        })
        .unwrap();
        assert_eq!(on["secretRequests"], json!(true));
        let decoded: AgentFeaturesSettings = serde_json::from_value(on).unwrap();
        assert!(decoded.secret_requests);
    }

    #[test]
    fn unavailable_error_names_the_secret_and_the_retry() {
        let err = unavailable("KEY");
        assert!(matches!(
            err,
            Error::SecretRequest {
                code: CODE_UNAVAILABLE,
                ..
            }
        ));
        let text = err.to_string();
        assert!(
            text.starts_with("secret-unavailable: secret KEY "),
            "{text}"
        );
        assert!(
            text.contains("ws.secret.request({ name: \"KEY\", reason })"),
            "{text}"
        );
        assert!(text.contains("end your turn"), "{text}");
    }
}

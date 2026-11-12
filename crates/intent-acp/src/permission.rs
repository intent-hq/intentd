//! Client-served permission prompts for `session/request_permission` (§6.7,
//! PROTOCOL §8).
//!
//! The agent asks the backend to approve a tool call. The backend either
//! auto-resolves it under a headless [`PermissionPolicy`] or surfaces the
//! normalized [`PermissionRequestData`] to clients and blocks until an answer
//! arrives. Outstanding prompts live in a [`PermissionRegistry`] keyed by
//! `requestId`: a resolving RPC (M3.8, `agent.respondPermission`) calls
//! [`PermissionRegistry::resolve`]; a reconnecting client refetches pending
//! prompts via [`PermissionRegistry::pending`] (M3.8, `agent.pendingPermissions`).
//! Unanswered prompts time out after [`DEFAULT_PERMISSION_TIMEOUT`] and resolve
//! as `cancelled`, unblocking the agent.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    PermissionOption, PermissionOptionKind, RequestPermissionRequest,
};
use intent_providers::AllowAlwaysScope;
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::oneshot;

/// Unanswered permission prompts resolve as `cancelled` after this long
/// (parity: TS 5-minute auto-cancel).
pub(crate) const DEFAULT_PERMISSION_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Heuristic risk class derived from the tool-call title (PROTOCOL §8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RiskLevel {
    /// Read-only / informational (read|view|list|get).
    Low,
    /// Anything not clearly safe or destructive.
    Medium,
    /// Mutating / destructive (delete|remove|execute|write|modify|create|launch).
    High,
}

/// Derive a [`RiskLevel`] from a tool-call title (parity: TS `assessRiskLevel`):
/// high-risk patterns win over low-risk; everything else is medium.
pub(crate) fn assess_risk_level(title: &str) -> RiskLevel {
    const HIGH: [&str; 7] = [
        "delete", "remove", "execute", "write", "modify", "create", "launch",
    ];
    const LOW: [&str; 4] = ["read", "view", "list", "get"];
    let lower = title.to_lowercase();
    if HIGH.iter().any(|p| lower.contains(p)) {
        RiskLevel::High
    } else if LOW.iter().any(|p| lower.contains(p)) {
        RiskLevel::Low
    } else {
        RiskLevel::Medium
    }
}

/// A permission option normalized to the PROTOCOL §8 client shape
/// (`{ id, label, description, destructive }`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionOptionView {
    /// Stable option id echoed back in the chosen outcome.
    pub id: String,
    /// Human-readable button label.
    pub label: String,
    /// Optional longer description (`null` when absent).
    pub description: Option<String>,
    /// Whether choosing this option rejects the operation.
    pub destructive: bool,
    /// The ACP option kind (`allow_once`, `allow_always`, `reject_once`,
    /// `reject_always`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<&'static str>,
    /// For `allow_always`: how far the approval reaches (`chat`,
    /// `workspace`, `allWorkspaces`); omitted when unknown.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<&'static str>,
}

/// The normalized permission request pushed to clients and stored for recovery
/// (PROTOCOL §8). `session_id` equals the intentd `agentId` so a client can
/// route the prompt to the right agent view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionRequestData {
    /// Unique id (`perm_<millis>_<n>`); the key the resolving RPC answers.
    pub request_id: String,
    /// The intentd agent id this prompt belongs to.
    pub session_id: String,
    /// Short title (the tool-call title).
    pub title: String,
    /// Longer description (rendered tool input), `null` when absent.
    pub description: Option<String>,
    /// Options the user chooses from (never empty — defaults applied).
    pub options: Vec<PermissionOptionView>,
    /// Provider/agent display name.
    pub agent_name: String,
    /// Heuristic risk class.
    pub risk_level: RiskLevel,
    /// Creation time in epoch milliseconds.
    pub timestamp: u64,
}

/// The user's (or policy's) decision on a permission prompt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionOutcome {
    /// One of the offered options was chosen.
    Selected {
        /// The chosen [`PermissionOptionView::id`].
        option_id: String,
    },
    /// The prompt was cancelled (timeout or explicit cancellation).
    Cancelled,
}

impl PermissionOutcome {
    /// The `RequestPermissionResponse` body returned to the agent over ACP
    /// (`{ outcome: { outcome: "selected", optionId } }` or `{ outcome: "cancelled" }`).
    #[must_use]
    pub fn to_response_value(&self) -> Value {
        json!({ "outcome": self.to_event_value() })
    }

    /// The `outcome` object carried on `agent:permission:resolved` and embedded
    /// in the ACP response (PROTOCOL §8 outcome shape).
    pub(crate) fn to_event_value(&self) -> Value {
        match self {
            PermissionOutcome::Selected { option_id } => {
                json!({ "outcome": "selected", "optionId": option_id })
            }
            PermissionOutcome::Cancelled => json!({ "outcome": "cancelled" }),
        }
    }

    /// Parse the §8 wire `outcome` object (`{ outcome: "selected", optionId }`
    /// or `{ outcome: "cancelled" }`) into a [`PermissionOutcome`]; the inverse
    /// of [`to_event_value`](Self::to_event_value). Returns `None` for a
    /// malformed shape (missing/non-string `outcome`, an unknown discriminant,
    /// or `selected` without a string `optionId`) so the resolving RPC can
    /// reject it as invalid params.
    pub fn from_wire(value: &Value) -> Option<PermissionOutcome> {
        match value.get("outcome").and_then(Value::as_str)? {
            "selected" => {
                let option_id = value.get("optionId").and_then(Value::as_str)?;
                Some(PermissionOutcome::Selected {
                    option_id: option_id.to_string(),
                })
            }
            "cancelled" => Some(PermissionOutcome::Cancelled),
            _ => None,
        }
    }
}

/// How the backend resolves prompts when no interactive client mediates them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PermissionPolicy {
    /// Surface every prompt to a client and await an answer (no auto-resolve).
    #[default]
    Interactive,
    /// Headless default: auto-allow low-risk (reads), auto-deny medium/high
    /// (potentially destructive) prompts.
    AutoByRisk,
    /// Headless: auto-allow every prompt.
    AllowAll,
    /// Headless: auto-deny every prompt.
    DenyAll,
}

impl PermissionPolicy {
    /// Wire values shared by `INTENTD_PERMISSION_POLICY` and the per-agent
    /// `agent.update { permissionPolicy }` override (PROTOCOL §8).
    pub const WIRE_VALUES: [&'static str; 4] = ["interactive", "allow", "auto", "deny"];

    /// Parse a wire value (case-insensitive, trimmed); `None` when unknown.
    #[must_use]
    pub fn from_wire(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "interactive" => Some(Self::Interactive),
            "allow" => Some(Self::AllowAll),
            "auto" => Some(Self::AutoByRisk),
            "deny" => Some(Self::DenyAll),
            _ => None,
        }
    }

    /// The canonical wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::AllowAll => "allow",
            Self::AutoByRisk => "auto",
            Self::DenyAll => "deny",
        }
    }

    /// The auto-decision for `risk`, or `None` when the prompt must be surfaced
    /// to a client. `true` selects an allow option, `false` a deny option.
    pub(crate) fn auto_allow(self, risk: RiskLevel) -> Option<bool> {
        match self {
            PermissionPolicy::Interactive => None,
            PermissionPolicy::AllowAll => Some(true),
            PermissionPolicy::DenyAll => Some(false),
            PermissionPolicy::AutoByRisk => Some(matches!(risk, RiskLevel::Low)),
        }
    }
}

/// One outstanding prompt: the channel that delivers its outcome plus the data
/// kept for client recovery.
struct Pending {
    sender: oneshot::Sender<PermissionOutcome>,
    data: PermissionRequestData,
}

/// Registry of outstanding permission prompts, keyed by `requestId`.
pub struct PermissionRegistry {
    inner: Mutex<HashMap<String, Pending>>,
    counter: AtomicU64,
    timeout: Duration,
}

impl Default for PermissionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PermissionRegistry {
    /// A registry with the default 5-minute prompt timeout.
    #[must_use]
    pub fn new() -> Self {
        Self::with_timeout(DEFAULT_PERMISSION_TIMEOUT)
    }

    /// A registry with an explicit timeout (used by tests to exercise the
    /// timeout path without waiting five minutes).
    pub(crate) fn with_timeout(timeout: Duration) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            counter: AtomicU64::new(0),
            timeout,
        }
    }

    /// The configured auto-cancel timeout.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Mint the next `perm_<millis>_<n>` request id.
    pub(crate) fn next_request_id(&self) -> String {
        let n = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        format!("perm_{}_{}", now_millis(), n)
    }

    /// Register `data` as outstanding and return the receiver its outcome will
    /// arrive on (from [`resolve`](Self::resolve) or a timeout-driven removal).
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub fn register(&self, data: PermissionRequestData) -> oneshot::Receiver<PermissionOutcome> {
        let (sender, receiver) = oneshot::channel();
        self.inner
            .lock()
            .unwrap()
            .insert(data.request_id.clone(), Pending { sender, data });
        receiver
    }

    /// Deliver `outcome` to the waiter for `request_id`. Returns `false` when no
    /// such prompt is outstanding (already resolved or timed out).
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub fn resolve(&self, request_id: &str, outcome: PermissionOutcome) -> bool {
        match self.inner.lock().unwrap().remove(request_id) {
            Some(pending) => pending.sender.send(outcome).is_ok(),
            None => false,
        }
    }

    /// Drop a prompt without delivering an outcome (timeout cleanup).
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub fn remove(&self, request_id: &str) {
        self.inner.lock().unwrap().remove(request_id);
    }

    /// Snapshot of every outstanding prompt, for client reconnect recovery.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub fn pending(&self) -> Vec<PermissionRequestData> {
        self.inner
            .lock()
            .unwrap()
            .values()
            .map(|p| p.data.clone())
            .collect()
    }
}

/// The default options when a provider offers none (PROTOCOL §8).
pub(crate) fn default_options() -> Vec<PermissionOptionView> {
    vec![
        PermissionOptionView {
            id: "allow_once".to_string(),
            label: "Allow".to_string(),
            description: None,
            destructive: false,
            kind: Some("allow_once"),
            scope: None,
        },
        PermissionOptionView {
            id: "reject_once".to_string(),
            label: "Deny".to_string(),
            description: None,
            destructive: true,
            kind: Some("reject_once"),
            scope: None,
        },
    ]
}

/// Normalize ACP [`PermissionOption`]s to the §8 client shape; an empty list
/// falls back to [`default_options`]. `reject_*` kinds are marked destructive.
pub(crate) fn normalize_options(
    options: &[PermissionOption],
    allow_always_scopes: &[(&str, AllowAlwaysScope)],
) -> Vec<PermissionOptionView> {
    if options.is_empty() {
        return default_options();
    }
    options
        .iter()
        .map(|opt| {
            let kind = match opt.kind {
                PermissionOptionKind::AllowOnce => Some("allow_once"),
                PermissionOptionKind::AllowAlways => Some("allow_always"),
                PermissionOptionKind::RejectOnce => Some("reject_once"),
                PermissionOptionKind::RejectAlways => Some("reject_always"),
                _ => None,
            };
            let scope = (opt.kind == PermissionOptionKind::AllowAlways)
                .then(|| always_scope(&opt.option_id.0, &opt.name, allow_always_scopes))
                .flatten()
                .map(AllowAlwaysScope::as_wire);
            PermissionOptionView {
                id: opt.option_id.0.to_string(),
                label: opt.name.clone(),
                description: None,
                destructive: matches!(
                    opt.kind,
                    PermissionOptionKind::RejectOnce | PermissionOptionKind::RejectAlways
                ),
                kind,
                scope,
            }
        })
        .collect()
}

/// Session-bound labels ("during this session", "for this conversation")
/// reach the chat only; otherwise the provider's option-id prefixes decide.
fn always_scope(
    id: &str,
    label: &str,
    scopes: &[(&str, AllowAlwaysScope)],
) -> Option<AllowAlwaysScope> {
    let label = label.to_ascii_lowercase();
    if label.contains("this session") || label.contains("this conversation") {
        return Some(AllowAlwaysScope::Chat);
    }
    scopes
        .iter()
        .find(|(prefix, _)| id.starts_with(prefix))
        .map(|(_, scope)| *scope)
}

/// Build the normalized [`PermissionRequestData`] for an incoming ACP
/// `session/request_permission`. `session_id` is the intentd agent id (so a
/// client routes the prompt to the agent view); `request_id` is minted by the
/// registry. Title/description are derived from the tool-call fields.
pub(crate) fn normalize_request(
    request_id: String,
    session_id: String,
    agent_name: String,
    req: &RequestPermissionRequest,
    allow_always_scopes: &[(&str, AllowAlwaysScope)],
) -> PermissionRequestData {
    let title = req
        .tool_call
        .fields
        .title
        .clone()
        .unwrap_or_else(|| "Permission required".to_string());
    let description = req
        .tool_call
        .fields
        .raw_input
        .as_ref()
        .map(|input| format!("Tool input: {input}"));
    let risk_level = assess_risk_level(&title);
    PermissionRequestData {
        request_id,
        session_id,
        title,
        description,
        options: normalize_options(&req.options, allow_always_scopes),
        agent_name,
        risk_level,
        timestamp: now_millis(),
    }
}

/// Pick the option id to auto-select for a policy decision: the first
/// non-destructive option when allowing, otherwise the first destructive one.
/// Falls back to the first option so a decision can always be made.
pub(crate) fn select_option(options: &[PermissionOptionView], allow: bool) -> Option<String> {
    options
        .iter()
        .find(|o| o.destructive != allow)
        .or_else(|| options.first())
        .map(|o| o.id.clone())
}

/// The first `allow_once` option's id, if the provider offered one.
pub(crate) fn allow_once_option(options: &[PermissionOption]) -> Option<String> {
    options
        .iter()
        .find(|o| o.kind == PermissionOptionKind::AllowOnce)
        .map(|o| o.option_id.0.to_string())
}

/// Epoch milliseconds (timestamp on [`PermissionRequestData`]).
fn now_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_providers::AllowAlwaysScope;

    fn options(raw: Value) -> Vec<PermissionOption> {
        serde_json::from_value(raw).unwrap()
    }

    fn scopes(provider: &str) -> &'static [(&'static str, AllowAlwaysScope)] {
        intent_providers::find_provider(provider)
            .unwrap()
            .allow_always_scopes
    }

    #[test]
    fn options_carry_kind_and_destructive() {
        let claude = options(json!([
            { "optionId": "allow-once", "name": "Yes", "kind": "allow_once" },
            { "optionId": "reject", "name": "No", "kind": "reject_once" }
        ]));
        let wire = serde_json::to_value(normalize_options(&claude, scopes("claude-code"))).unwrap();
        assert_eq!(wire[0]["kind"], "allow_once");
        assert!(wire[0].get("scope").is_none(), "{wire}");
        assert_eq!(wire[1]["kind"], "reject_once");
        assert_eq!(wire[1]["destructive"], true);
    }

    /// Every `allow_always` option id and label in the pinned adapters
    /// (claude-agent-acp 0.95.0, codex-acp 2.6.0, cursor-agent 2026.11.05),
    /// with where the approval is kept. Unknown options get no scope.
    #[test]
    fn allow_always_scope_matches_where_each_adapter_keeps_the_approval() {
        for (provider, id, name, expected) in [
            (
                "claude-code",
                "allow-with-updates",
                "Yes, and don't ask again for Bash commands",
                Some("workspace"),
            ),
            (
                "claude-code",
                "allow-with-updates",
                "Yes, allow all edits during this session",
                Some("chat"),
            ),
            (
                "claude-code",
                "allow-skill-exact",
                "Yes, and don't ask again for this skill",
                Some("workspace"),
            ),
            (
                "claude-code",
                "exit-plan-auto",
                "Yes, and use auto mode",
                Some("chat"),
            ),
            (
                "claude-code",
                "exit-plan-bypass",
                "Yes, and bypass permissions",
                Some("chat"),
            ),
            (
                "codex",
                "allow_for_session",
                "Yes, and don't ask again for this command in this session",
                Some("chat"),
            ),
            (
                "codex",
                "allow_permissions_session",
                "Yes, grant these permissions for this session",
                Some("chat"),
            ),
            (
                "codex",
                "allow_session",
                "Allow for this session",
                Some("chat"),
            ),
            (
                "codex",
                "accept_execpolicy_amendment",
                "Yes, and don't ask again for commands that start with `npm`",
                Some("allWorkspaces"),
            ),
            (
                "codex",
                "apply_network_policy_amendment:0",
                "Yes, and allow this host in the future",
                None,
            ),
            ("codex", "allow_always", "Always allow", None),
            (
                "cursor",
                "allow-always",
                "Allow always",
                Some("allWorkspaces"),
            ),
            ("mock", "allow_always", "Always allow", None),
        ] {
            let opts = options(json!([{ "optionId": id, "name": name, "kind": "allow_always" }]));
            let wire = serde_json::to_value(normalize_options(&opts, scopes(provider))).unwrap();
            assert_eq!(wire[0]["kind"], "allow_always");
            assert_eq!(
                wire[0]["scope"].as_str(),
                expected,
                "{provider} {id}: {name}"
            );
        }
    }

    #[test]
    fn from_wire_parses_selected_and_cancelled() {
        assert_eq!(
            PermissionOutcome::from_wire(
                &json!({ "outcome": "selected", "optionId": "allow_once" })
            ),
            Some(PermissionOutcome::Selected {
                option_id: "allow_once".to_string()
            })
        );
        assert_eq!(
            PermissionOutcome::from_wire(&json!({ "outcome": "cancelled" })),
            Some(PermissionOutcome::Cancelled)
        );
    }

    #[test]
    fn from_wire_rejects_malformed_shapes() {
        // `selected` without an `optionId`, an unknown discriminant, a missing
        // `outcome`, and a non-object value all parse to `None`.
        assert_eq!(
            PermissionOutcome::from_wire(&json!({ "outcome": "selected" })),
            None
        );
        assert_eq!(
            PermissionOutcome::from_wire(&json!({ "outcome": "approved" })),
            None
        );
        assert_eq!(PermissionOutcome::from_wire(&json!({})), None);
        assert_eq!(PermissionOutcome::from_wire(&json!("cancelled")), None);
    }

    #[test]
    fn from_wire_round_trips_to_event_value() {
        for outcome in [
            PermissionOutcome::Selected {
                option_id: "reject_once".to_string(),
            },
            PermissionOutcome::Cancelled,
        ] {
            assert_eq!(
                PermissionOutcome::from_wire(&outcome.to_event_value()),
                Some(outcome)
            );
        }
    }
}

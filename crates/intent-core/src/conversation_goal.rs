//! Conversation goal (PROTOCOL §5.5 "Conversation goal", intent-hq/intent#9646):
//! one user-set statement pinned to an agent conversation. This module owns
//! the transport-independent rules every layer shares: `/goal` command
//! recognition, validation, the action table and the `goal_command` notice
//! row. Storage lives in `intent-store` (`Store::set_agent_goal`), the write
//! path in `intent-services`, and the per-turn prompt wording in the
//! services prompt harness.

use serde_json::{json, Value};

use crate::{Error, Principal, Result};

/// The command token. Case-sensitive.
pub const COMMAND: &str = "/goal";

/// Longest accepted goal, in characters (Unicode scalar values), after
/// trimming.
pub const MAX_CHARS: usize = 1000;

/// `metadata.type` of the transcript-only notice row every command (and
/// every writing `agent.setGoal`) persists.
pub const NOTICE_TYPE: &str = "goal_command";

/// `-32602` message for a `/goal` command that carries image or file blocks.
pub const ATTACHMENTS_ERROR: &str = "/goal does not take attachments";

/// `-32602` message for a goal over [`MAX_CHARS`].
pub const TOO_LONG_ERROR: &str = "goal is too long (max 1000 characters)";

/// Notice text for `/goal` with no goal set.
pub const NO_GOAL_NOTICE: &str = "No goal set. Send /goal followed by what done looks like.";

/// Notice text for a removed goal.
pub const REMOVED_NOTICE: &str = "Goal removed.";

/// Longest stored `goalSetBy.displayName` / `goalSetBy.login` snapshot, in
/// bytes (cut back to a character boundary). Profile fields are not capped
/// at the source, and the attribution rides every `agent.list` row, so the
/// snapshot is bounded to keep the row inside
/// [`crate::AGENT_LIST_ROW_BUDGET_BYTES`]. Real logins and display names are
/// far shorter.
pub const SET_BY_NAME_CAP_BYTES: usize = 128;

/// Who wrote the current goal (PROTOCOL §5.5 "Conversation goal" →
/// "Attribution"): the writer's principal id plus a `displayName` / `login`
/// snapshot of its profile at write time. Stored as JSON in
/// `agent_session.goal_set_by` and served as `goalSetBy`. Both name keys are
/// always serialized (`null` when the profile had none).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalSetBy {
    pub principal_id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub login: Option<String>,
}

impl GoalSetBy {
    /// The attribution snapshot of `principal`'s current profile, each name
    /// capped at [`SET_BY_NAME_CAP_BYTES`].
    #[must_use]
    pub fn snapshot(principal: &Principal) -> Self {
        Self {
            principal_id: principal.id.0.clone(),
            display_name: principal.display_name.as_deref().map(cap_name),
            login: principal.login.as_deref().map(cap_name),
        }
    }
}

/// `name` cut to at most [`SET_BY_NAME_CAP_BYTES`] bytes on a character
/// boundary.
fn cap_name(name: &str) -> String {
    if name.len() <= SET_BY_NAME_CAP_BYTES {
        return name.to_string();
    }
    let mut end = SET_BY_NAME_CAP_BYTES;
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    name[..end].to_string()
}

/// A documented conversation-goal refusal (`-32602`, nothing changed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalError {
    /// The trimmed goal is longer than [`MAX_CHARS`].
    TooLong,
    /// A `/goal` command carried image or file blocks.
    Attachments,
    /// The target agent does not exist (or was deleted).
    AgentNotFound,
    /// The target agent is retired.
    AgentRetired,
}

impl GoalError {
    /// The wire `error.message`.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::TooLong => TOO_LONG_ERROR,
            Self::Attachments => ATTACHMENTS_ERROR,
            Self::AgentNotFound => "agent not found",
            Self::AgentRetired => "agent is retired",
        }
    }

    /// The wire `error.data`, `None` for a refusal without a machine code.
    #[must_use]
    pub fn data(self) -> Option<Value> {
        match self {
            Self::TooLong => Some(json!({ "code": "goal-too-long", "maxLength": MAX_CHARS })),
            Self::Attachments => Some(json!({ "code": "goal-attachments-not-allowed" })),
            Self::AgentNotFound => None,
            Self::AgentRetired => Some(json!({ "code": "agent-retired" })),
        }
    }
}

/// What a goal command or `agent.setGoal` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalAction {
    /// A goal was stored where none was set.
    Set,
    /// A different goal replaced the stored one.
    Updated,
    /// The requested goal equals the stored one (or a remove found none):
    /// nothing was written.
    Unchanged,
    /// `/goal` with no statement: the current goal was shown.
    Shown,
    /// `agent.setGoal` removed the stored goal.
    Cleared,
}

impl GoalAction {
    /// The wire spelling (`command.action`, `agent.setGoal` `action`, notice
    /// `metadata.action`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Set => "set",
            Self::Updated => "updated",
            Self::Unchanged => "unchanged",
            Self::Shown => "shown",
            Self::Cleared => "cleared",
        }
    }

    /// Whether this action changes the stored goal (and so emits
    /// `agent:updated`).
    #[must_use]
    pub const fn writes(self) -> bool {
        matches!(self, Self::Set | Self::Updated | Self::Cleared)
    }
}

/// The statement of a `/goal` command in `content`, or `None` when `content`
/// is an ordinary message. Recognized when the trimmed content is exactly
/// [`COMMAND`] or starts with it followed by whitespace; the statement is
/// everything after that first whitespace, trimmed (inner newlines kept), and
/// is empty for a bare `/goal`.
#[must_use]
pub fn parse_command(content: &str) -> Option<&str> {
    let rest = content.trim().strip_prefix(COMMAND)?;
    if rest.is_empty() {
        return Some("");
    }
    rest.starts_with(char::is_whitespace).then(|| rest.trim())
}

/// Whether a send's `messageMetadata` leaves it a plain typed send that may
/// be read as a command: absent, `null`, or an object with no `type` key.
/// Other keys (the desktop's `mentionTokens`, …) do not stop recognition and
/// are dropped with the command; typed metadata (`question_answers`, wizard
/// submissions, every other `type`) is delivered as text.
#[must_use]
pub fn metadata_is_plain(metadata: Option<&Value>) -> bool {
    match metadata {
        None | Some(Value::Null) => true,
        Some(Value::Object(obj)) => !obj.contains_key("type"),
        Some(_) => false,
    }
}

/// Trim `goal` and check its length: `Ok(None)` for an empty (or absent)
/// goal, [`GoalError::TooLong`] over [`MAX_CHARS`].
///
/// # Errors
///
/// Returns `Error::ConversationGoal(GoalError::TooLong)` when the trimmed
/// goal is longer than [`MAX_CHARS`] characters.
pub fn normalize(goal: Option<&str>) -> Result<Option<String>> {
    let trimmed = goal.map(str::trim).unwrap_or_default();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.chars().count() > MAX_CHARS {
        return Err(Error::ConversationGoal(GoalError::TooLong));
    }
    Ok(Some(trimmed.to_string()))
}

/// The action that moves the stored goal `current` to `next`; `None` for
/// `next` removes it.
#[must_use]
pub fn write_action(current: Option<&str>, next: Option<&str>) -> GoalAction {
    match (current, next) {
        (Some(current), Some(next)) if current == next => GoalAction::Unchanged,
        (None, None) => GoalAction::Unchanged,
        (None, Some(_)) => GoalAction::Set,
        (Some(_), Some(_)) => GoalAction::Updated,
        (Some(_), None) => GoalAction::Cleared,
    }
}

/// The human-readable notice text for `action`, `goal` being the goal after
/// it (the fallback clients without `goal_command` support render).
#[must_use]
pub fn notice_text(action: GoalAction, goal: Option<&str>) -> String {
    match (action, goal) {
        (GoalAction::Set, Some(goal)) => format!("Goal set: {goal}"),
        (GoalAction::Updated, Some(goal)) => format!("Goal updated: {goal}"),
        (GoalAction::Unchanged, Some(goal)) => format!("Goal unchanged: {goal}"),
        (GoalAction::Shown, Some(goal)) => format!("Current goal: {goal}"),
        (GoalAction::Cleared, _) | (GoalAction::Unchanged, None) => REMOVED_NOTICE.to_string(),
        (GoalAction::Shown | GoalAction::Set | GoalAction::Updated, None) => {
            NO_GOAL_NOTICE.to_string()
        }
    }
}

/// The notice row's `content` blocks: one text block.
#[must_use]
pub fn notice_content(action: GoalAction, goal: Option<&str>) -> Value {
    json!([{ "type": "text", "text": notice_text(action, goal) }])
}

/// The notice row's `metadata`:
/// `{ type: "goal_command", action, goal, fromPrincipalId? }`.
#[must_use]
pub fn notice_metadata(
    action: GoalAction,
    goal: Option<&str>,
    from_principal_id: Option<&str>,
) -> Value {
    let mut metadata = json!({
        "type": NOTICE_TYPE,
        "action": action.as_str(),
        "goal": goal,
    });
    if let Some(principal) = from_principal_id {
        metadata[crate::FROM_PRINCIPAL_ID_KEY] = json!(principal);
    }
    metadata
}

#[cfg(test)]
mod tests;

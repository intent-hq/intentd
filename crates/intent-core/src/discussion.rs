//! Workspace discussions, separate from agent sessions; agent access requires explicit addressing.
use serde::{Deserialize, Serialize};

use crate::{AgentId, PrincipalId, WorkspaceId};

pub const DEFAULT_CONVERSATION: &str = "default";
pub const DISCUSSION_RECIPIENT_MAX: usize = 31;

pub const DISCUSSION_BODY_MAX_BYTES: usize = 16 * 1024;
pub const DISCUSSION_PAGE_MAX: u32 = 100;
pub const DISCUSSION_PAGE_DEFAULT: u32 = 50;
pub const DISCUSSION_REACTIONS: [&str; 6] = ["👍", "👎", "👀", "✅", "🙈", "🙂"];
pub const DISCUSSION_REACTOR_MAX: u32 = 20;

/// Server-bound local identity, never deserialized from discussion arguments.
#[derive(Debug, Clone)]
pub enum DiscussionActor {
    Human(PrincipalId),
    Agent(AgentId),
}
impl DiscussionActor {
    #[must_use]
    pub fn principal_id(&self) -> Option<&str> {
        match self {
            Self::Human(id) => Some(id.as_str()),
            Self::Agent(_) => None,
        }
    }
    #[must_use]
    pub fn agent_id(&self) -> Option<&str> {
        match self {
            Self::Agent(id) => Some(id.as_str()),
            Self::Human(_) => None,
        }
    }
}

/// Profile snapshot captured from the authenticated human or bound agent at send time.
/// After workspace transfer the principal id is inert source attribution;
/// never resolve it against the destination registry or use it as authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscussionAuthor {
    pub principal_id: Option<PrincipalId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    pub login: Option<String>,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
}

/// Durable state of one recipient's explicitly addressed invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscussionAgentActivity {
    pub agent_id: AgentId,
    pub attempt: u32,
    pub version: i64,
    pub state: String,
    pub updated_at: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscussionMessage {
    pub id: String,
    pub workspace_id: WorkspaceId,
    #[serde(default = "default_conversation_id")]
    pub conversation_id: String,
    pub sequence: i64,
    pub body: String,
    #[serde(default)]
    pub recipient_agent_ids: Vec<AgentId>,
    #[serde(default)]
    pub agent_activity: Vec<DiscussionAgentActivity>,
    #[serde(default)]
    pub agent_mentions: Vec<DiscussionAgentMention>,
    pub author: DiscussionAuthor,
    pub created_at: String,
    #[serde(default)]
    pub reply_to_message_id: Option<String>,
    #[serde(default)]
    pub reply_to: Option<DiscussionReply>,
    #[serde(default)]
    pub reactions: Vec<DiscussionReaction>,
    #[serde(default)]
    pub reaction_version: i64,
}

/// Immutable same-conversation quote; source identity remains inert on import.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscussionReply {
    pub id: String,
    pub author: DiscussionAuthor,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscussionReaction {
    pub reaction: String,
    pub count: i64,
    pub reacted: bool,
    /// First-added immutable snapshots, bounded independently of the total count.
    #[serde(default)]
    pub reactors: Vec<DiscussionAuthor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscussionReactionState {
    pub message_id: String,
    pub reactions: Vec<DiscussionReaction>,
    pub reaction_version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscussionPage {
    pub messages: Vec<DiscussionMessage>,
    pub has_more: bool,
}

fn default_conversation_id() -> String {
    DEFAULT_CONVERSATION.into()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscussionConversation {
    pub id: String,
    pub workspace_id: WorkspaceId,
    pub kind: String,
    pub participants: Vec<DiscussionAuthor>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscussionSendResult {
    pub message: DiscussionMessage,
    pub conversation: DiscussionConversation,
}

#[derive(Debug, Clone, Default)]
pub struct DiscussionTarget {
    pub agent_mentions: Vec<DiscussionAgentMention>,
    pub recipient_agent_ids: Vec<AgentId>,
    pub conversation_id: Option<String>,
    pub recipient_principal_ids: Option<Vec<PrincipalId>>,
    pub reply_to_message_id: Option<String>,
}

/// Exact invocation observed by a human control. Attempt is a compare-and-swap fence.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscussionAgentControl {
    pub workspace_id: WorkspaceId,
    #[serde(default = "default_conversation_id")]
    pub conversation_id: String,
    pub message_id: String,
    pub agent_id: AgentId,
    pub attempt: u32,
}

/// Immutable selected span; offsets are UTF-16 code units, as in browser strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiscussionAgentMention {
    pub agent_id: AgentId,
    pub name: String,
    pub start: u32,
    pub end: u32,
}

/// Validate presentation metadata without deriving any delivery authority from text.
/// # Errors
/// Rejects malformed, overlapping, non-selected or altered mention spans.
pub fn validate_agent_mentions(body: &str, target: &DiscussionTarget) -> crate::Result<()> {
    let units: Vec<u16> = body.encode_utf16().collect();
    let mut end = 0usize;
    if target.agent_mentions.len() > 100 {
        return Err(crate::Error::InvalidParams(
            "too many agent mentions".into(),
        ));
    }
    for mention in &target.agent_mentions {
        let start = mention.start as usize;
        let next = mention.end as usize;
        if mention.name.trim().is_empty()
            || mention.name.len() > 256
            || start < end
            || start >= next
            || !target.recipient_agent_ids.contains(&mention.agent_id)
            || units
                .get(start..next)
                .and_then(|span| String::from_utf16(span).ok())
                .as_deref()
                != Some(format!("@{}", mention.name).as_str())
        {
            return Err(crate::Error::InvalidParams(
                "invalid selected agent mention".into(),
            ));
        }
        end = next;
    }
    Ok(())
}

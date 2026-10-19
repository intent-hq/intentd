//! Workspace discussions, separate from agent sessions; agent access requires explicit addressing.
use serde::{Deserialize, Serialize};

use crate::{AgentId, PrincipalId, WorkspaceId};

pub const DEFAULT_CONVERSATION: &str = "default";
pub const DISCUSSION_RECIPIENT_MAX: usize = 31;

pub const DISCUSSION_BODY_MAX_BYTES: usize = 16 * 1024;
pub const DISCUSSION_PAGE_MAX: u32 = 100;
pub const DISCUSSION_PAGE_DEFAULT: u32 = 50;
pub const DISCUSSION_REACTIONS: [&str; 2] = ["👍", "🙂"];

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
    pub recipient_agent_ids: Vec<AgentId>,
    pub conversation_id: Option<String>,
    pub recipient_principal_ids: Option<Vec<PrincipalId>>,
    pub reply_to_message_id: Option<String>,
}

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
    #[serde(default)]
    pub principal_mentions: Vec<DiscussionPrincipalMention>,
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
    pub principal_mentions: Vec<DiscussionPrincipalMention>,
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

/// Explicit human notification identity with an immutable selected UTF-16 span.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiscussionPrincipalMention {
    pub principal_id: PrincipalId,
    pub name: String,
    pub start: u32,
    pub end: u32,
}

/// Validate selected person spans; membership is checked in the write transaction.
/// # Errors
/// Rejects malformed, overlapping, or altered mention spans.
pub fn validate_principal_mentions(body: &str, target: &DiscussionTarget) -> crate::Result<()> {
    let units: Vec<u16> = body.encode_utf16().collect();
    let mut end = 0usize;
    if target.principal_mentions.len() > 100 {
        return Err(crate::Error::InvalidParams(
            "too many principal mentions".into(),
        ));
    }
    for mention in &target.principal_mentions {
        let start = mention.start as usize;
        let next = mention.end as usize;
        if mention.principal_id.as_str().trim().is_empty()
            || mention.name.trim().is_empty()
            || mention.name.len() > 256
            || start < end
            || start >= next
            || units
                .get(start..next)
                .and_then(|span| String::from_utf16(span).ok())
                .as_deref()
                != Some(format!("@{}", mention.name).as_str())
            || target
                .agent_mentions
                .iter()
                .any(|agent| mention.start < agent.end && agent.start < mention.end)
        {
            return Err(crate::Error::InvalidParams(
                "invalid selected principal mention".into(),
            ));
        }
        end = next;
    }
    Ok(())
}

#[cfg(test)]
mod principal_mention_tests {
    use super::*;

    fn target() -> DiscussionTarget {
        DiscussionTarget {
            principal_mentions: vec![DiscussionPrincipalMention {
                principal_id: PrincipalId::from("person"),
                name: "Bob".into(),
                start: 3,
                end: 7,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn principal_mentions_validate_utf16_selection_and_bounds() {
        let body = "😀 @Bob @Bob";
        assert!(validate_principal_mentions(body, &target()).is_ok());
        let mut repeated = target();
        repeated
            .principal_mentions
            .push(DiscussionPrincipalMention {
                start: 8,
                end: 12,
                ..repeated.principal_mentions[0].clone()
            });
        assert!(validate_principal_mentions(body, &repeated).is_ok());
        repeated.principal_mentions.reverse();
        assert!(validate_principal_mentions(body, &repeated).is_err());
        for (start, end) in [(0, 1), (2, 6), (3, 3), (7, 3), (3, 99), (4, 7)] {
            let mut invalid = target();
            invalid.principal_mentions[0].start = start;
            invalid.principal_mentions[0].end = end;
            assert!(
                validate_principal_mentions(body, &invalid).is_err(),
                "{start}..{end}"
            );
        }
        for name in ["", " ", "Alice"] {
            let mut invalid = target();
            invalid.principal_mentions[0].name = name.into();
            assert!(validate_principal_mentions(body, &invalid).is_err());
        }
        let mut invalid = target();
        invalid.principal_mentions[0].principal_id = PrincipalId::from(" ");
        assert!(validate_principal_mentions(body, &invalid).is_err());
        let mut overlap = target();
        overlap
            .principal_mentions
            .push(overlap.principal_mentions[0].clone());
        assert!(validate_principal_mentions(body, &overlap).is_err());
        let mut cross_overlap = target();
        cross_overlap.agent_mentions.push(DiscussionAgentMention {
            agent_id: AgentId::from("agent"),
            name: "Bob".into(),
            start: 3,
            end: 7,
        });
        assert!(validate_principal_mentions(body, &cross_overlap).is_err());
        for count in [100, 101] {
            let body = "@B ".repeat(count);
            let mut many = target();
            many.principal_mentions = (0..count)
                .map(|i| DiscussionPrincipalMention {
                    principal_id: PrincipalId::from("person"),
                    name: "B".into(),
                    start: u32::try_from(i * 3).unwrap(),
                    end: u32::try_from(i * 3 + 2).unwrap(),
                })
                .collect();
            assert_eq!(
                validate_principal_mentions(&body, &many).is_ok(),
                count == 100
            );
        }
        for bytes in [256, 257] {
            let name = "B".repeat(bytes);
            let body = format!("@{name}");
            let mut large = target();
            large.principal_mentions[0].name = name;
            large.principal_mentions[0].start = 0;
            large.principal_mentions[0].end = u32::try_from(bytes + 1).unwrap();
            assert_eq!(
                validate_principal_mentions(&body, &large).is_ok(),
                bytes == 256
            );
        }
    }
}

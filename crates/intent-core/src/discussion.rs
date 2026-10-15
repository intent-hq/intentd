//! One human discussion per workspace, separate from agent conversations.
use serde::{Deserialize, Serialize};

use crate::{PrincipalId, WorkspaceId};

pub const DISCUSSION_BODY_MAX_BYTES: usize = 16 * 1024;
pub const DISCUSSION_PAGE_MAX: u32 = 100;
pub const DISCUSSION_PAGE_DEFAULT: u32 = 50;

/// Profile snapshot captured from the authenticated human at send time.
/// After workspace transfer the principal id is inert source attribution;
/// never resolve it against the destination registry or use it as authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscussionAuthor {
    pub principal_id: PrincipalId,
    pub login: Option<String>,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscussionMessage {
    pub id: String,
    pub workspace_id: WorkspaceId,
    pub sequence: i64,
    pub body: String,
    pub author: DiscussionAuthor,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscussionPage {
    pub messages: Vec<DiscussionMessage>,
    pub has_more: bool,
}

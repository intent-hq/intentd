//! Pre-workspace repository browsing and checkout on one original native socket.
//!
//! The opaque binding is correlation, not permission. Services revalidate the
//! original host caller, connection and credential at each consuming boundary.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CheckoutCaptureQuery {
    pub provider: String,
    pub instance_base_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_owner_avatar: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CheckoutBinding {
    pub checkout_id: String,
    pub revision: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CheckoutProjectsQuery {
    pub checkout_id: String,
    pub revision: String,
    pub query: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CheckoutProjectQuery {
    pub checkout_id: String,
    pub revision: String,
    pub project_path: Option<String>,
    pub url: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CheckoutBranchesQuery {
    pub checkout_id: String,
    pub revision: String,
    pub project_path: String,
    pub query: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    pub cached: Option<bool>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckoutMode {
    Direct,
    Cached,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CheckoutSelection {
    pub checkout_id: String,
    pub revision: String,
    pub project_path: String,
    pub branch: String,
    pub commit_sha: String,
    pub mode: CheckoutMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckoutFrame {
    Capture(CheckoutCaptureQuery),
    Projects(CheckoutProjectsQuery),
    Project(CheckoutProjectQuery),
    Branches(CheckoutBranchesQuery),
    Warm(CheckoutSelection),
    Create(CheckoutSelection),
    Release(CheckoutBinding),
    Fetch(crate::WorkspaceId),
    Push {
        workspace_id: crate::WorkspaceId,
        force: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckoutCapture {
    pub checkout_id: String,
    pub revision: String,
    pub provider: String,
    pub instance_base_url: String,
    pub expires_after_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckoutProject {
    pub project_path: String,
    pub name: String,
    pub namespace: String,
    pub web_url: String,
    pub clone_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_avatar_url: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckoutProjectDetail {
    pub project: CheckoutProject,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_url: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckoutProjects {
    pub items: Vec<CheckoutProject>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckoutBranch {
    pub name: String,
    pub commit_sha: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protected: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckoutBranches {
    pub items: Vec<CheckoutBranch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_branch: Option<String>,
    pub cached: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckoutWarm {
    pub project_path: String,
    pub branch: String,
    pub commit_sha: String,
    pub cached: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckoutReleased {
    pub released: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CheckoutUnavailable {
    Disabled,
    NotConnected,
    AccessDenied,
    RateLimited,
    Unreachable,
    Retired,
    NotFound,
    InvalidTarget,
    EmptyRepository,
    BranchChanged,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum CheckoutResult<T> {
    Ready {
        value: T,
    },
    Unavailable {
        reason: CheckoutUnavailable,
        #[serde(rename = "retryAfterMs", skip_serializing_if = "Option::is_none")]
        retry_after_ms: Option<u64>,
    },
}

impl<T> CheckoutResult<T> {
    #[must_use]
    pub fn unavailable(reason: CheckoutUnavailable) -> Self {
        Self::Unavailable {
            reason,
            retry_after_ms: None,
        }
    }
}

#[cfg(test)]
mod tests;

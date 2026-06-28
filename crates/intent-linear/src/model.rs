//! Data models for the Linear engine.
//!
//! Field names serialize as `camelCase` to match the FE wire shapes
//! (`src/features/linear-auth/types.ts`). Timestamps are kept as RFC-3339
//! strings (parity with the TS ground truth) rather than a datetime type.

use serde::{Deserialize, Serialize};

/// Which set of issues to list (mirrors the FE `fetchMyIssues` filter enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IssueFilter {
    /// Issues assigned to the viewer (the FE default).
    #[default]
    Assigned,
    /// Issues created by the viewer.
    Created,
    /// Issues the viewer is subscribed to.
    Subscribed,
    /// Issues in teams the viewer belongs to.
    Team,
    /// All issues accessible to the viewer.
    All,
}

/// Auth / connectivity probe result (`linear.authStatus`).
///
/// Mirrors the source-control `AuthStatus` shape. Linear personal-key scopes
/// are not returned by `viewer`, so [`Self::scopes`] is always empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthStatus {
    pub authenticated: bool,
    pub login: Option<String>,
    pub scopes: Vec<String>,
}

/// The flattened issue shape returned by `linear.listIssues` /
/// `linear.searchIssues`.
///
/// Mirrors the FE `LinearIssueResult` (`linear-auth.client.ts` /
/// `linear-auth.service.ts`) field-for-field so existing consumers need zero
/// changes. Relations are flattened to their human-readable names.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinearIssueResult {
    pub id: String,
    pub identifier: String,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub team_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub team_key: Option<String>,
    /// Workflow-state name (e.g. "In Progress").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<f64>,
    /// Assignee display name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,
    /// Label names.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<Vec<String>>,
    /// Project name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    /// Creator display name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creator: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issue_filter_deserializes_lowercase() {
        let f: IssueFilter = serde_json::from_str("\"assigned\"").unwrap();
        assert_eq!(f, IssueFilter::Assigned);
        assert_eq!(IssueFilter::default(), IssueFilter::Assigned);
        let f: IssueFilter = serde_json::from_str("\"all\"").unwrap();
        assert_eq!(f, IssueFilter::All);
    }

    #[test]
    fn issue_result_serializes_camel_case_and_omits_none() {
        let issue = LinearIssueResult {
            id: "uuid-1".into(),
            identifier: "ENG-42".into(),
            title: "Fix the thing".into(),
            description: None,
            url: Some("https://linear.app/x/issue/ENG-42".into()),
            team_name: Some("Engineering".into()),
            team_key: Some("ENG".into()),
            state: Some("In Progress".into()),
            priority: Some(2.0),
            assignee: Some("Ada".into()),
            labels: Some(vec!["bug".into()]),
            project: None,
            creator: Some("Grace".into()),
            created_at: Some("2026-01-01T00:00:00.000Z".into()),
            updated_at: None,
        };
        let v = serde_json::to_value(&issue).unwrap();
        assert_eq!(v["identifier"], "ENG-42");
        assert_eq!(v["teamKey"], "ENG");
        assert_eq!(v["teamName"], "Engineering");
        assert_eq!(v["createdAt"], "2026-01-01T00:00:00.000Z");
        assert!(v.get("description").is_none());
        assert!(v.get("project").is_none());
        assert!(v.get("updatedAt").is_none());
    }

    #[test]
    fn auth_status_serializes_camel_case() {
        let s = AuthStatus {
            authenticated: true,
            login: Some("Ada Lovelace".into()),
            scopes: vec![],
        };
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["authenticated"], true);
        assert_eq!(v["login"], "Ada Lovelace");
        assert!(v["scopes"].as_array().unwrap().is_empty());
    }
}

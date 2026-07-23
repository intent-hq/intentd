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

/// One engine-level page of issues backing `linear.listIssues` /
/// `linear.searchIssues`.
///
/// `next_token` carries the **raw** Linear GraphQL `pageInfo.endCursor` and is
/// present only when `pageInfo.hasNextPage` is true. The services layer wraps
/// it into the opaque base64 wire `nextToken` (and emits explicit `null` on
/// the last page) per the §5.5 uniform-pagination conventions — this struct
/// never crosses the wire directly.
#[derive(Debug, Clone, PartialEq)]
pub struct LinearIssuePage {
    pub issues: Vec<LinearIssueResult>,
    pub next_token: Option<String>,
}

/// A Linear user (`linear.viewer`).
///
/// Mirrors the FE `LinearUser` (`linear-auth/types.ts`) field-for-field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinearUser {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar_url: Option<String>,
}

/// A Linear team (`linear.listTeams`).
///
/// Mirrors the FE `LinearTeam` (`linear-auth/types.ts`) field-for-field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinearTeam {
    pub id: String,
    /// Team key like "ENG".
    pub key: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// A Linear workflow state (`linear.listWorkflowStates`).
///
/// Mirrors the FE `LinearWorkflowState` (`linear-auth/types.ts`) field-for-field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinearWorkflowState {
    pub id: String,
    pub name: String,
    /// "backlog", "unstarted", "started", "completed", "canceled".
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
}

/// A Linear project (`linear.listProjects`).
///
/// Mirrors the FE `LinearProject` (`linear-auth/types.ts`) field-for-field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinearProject {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// "backlog", "planned", "started", "paused", "completed", "canceled".
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

/// A Linear label (`linear.listLabels`).
///
/// Mirrors the FE `LinearLabel` (`linear-auth/types.ts`) field-for-field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinearLabel {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
}

/// Request body for `linear.createIssue` (P2 write, §5.28).
///
/// Mirrors the FE `CreateIssueRequest` (`linear-auth/types.ts`) field-for-field.
/// `title` and `teamId` are required; everything else is optional.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CreateIssueRequest {
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub team_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assignee_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label_ids: Option<Vec<String>>,
}

/// Request body for `linear.updateIssue` (P2 write, §5.28).
///
/// Mirrors the FE `UpdateIssueRequest` (`linear-auth/types.ts`) field-for-field.
/// `issueId` is required; every other field is optional and only included in the
/// GraphQL `IssueUpdateInput` when present.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UpdateIssueRequest {
    pub issue_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assignee_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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

    #[test]
    fn user_serializes_camel_case_and_omits_none() {
        let u = LinearUser {
            id: "u1".into(),
            name: "Ada".into(),
            display_name: Some("ada".into()),
            email: None,
            avatar_url: Some("https://x/a.png".into()),
        };
        let v = serde_json::to_value(&u).unwrap();
        assert_eq!(v["id"], "u1");
        assert_eq!(v["displayName"], "ada");
        assert_eq!(v["avatarUrl"], "https://x/a.png");
        assert!(v.get("email").is_none());
    }

    #[test]
    fn team_serializes_camel_case_and_omits_none() {
        let t = LinearTeam {
            id: "t1".into(),
            key: "ENG".into(),
            name: "Engineering".into(),
            description: None,
        };
        let v = serde_json::to_value(&t).unwrap();
        assert_eq!(v["key"], "ENG");
        assert_eq!(v["name"], "Engineering");
        assert!(v.get("description").is_none());
    }

    #[test]
    fn workflow_state_serializes_type_field_and_omits_none() {
        let s = LinearWorkflowState {
            id: "s1".into(),
            name: "In Progress".into(),
            r#type: "started".into(),
            description: None,
            color: Some("#abc".into()),
        };
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["type"], "started");
        assert_eq!(v["color"], "#abc");
        assert!(v.get("description").is_none());
    }

    #[test]
    fn project_serializes_camel_case_and_omits_none() {
        let p = LinearProject {
            id: "p1".into(),
            name: "Apollo".into(),
            description: None,
            state: "started".into(),
            url: Some("https://linear.app/x/project/apollo".into()),
        };
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["state"], "started");
        assert_eq!(v["url"], "https://linear.app/x/project/apollo");
        assert!(v.get("description").is_none());
    }

    #[test]
    fn label_serializes_camel_case_and_omits_none() {
        let l = LinearLabel {
            id: "l1".into(),
            name: "bug".into(),
            description: Some("a bug".into()),
            color: None,
        };
        let v = serde_json::to_value(&l).unwrap();
        assert_eq!(v["name"], "bug");
        assert_eq!(v["description"], "a bug");
        assert!(v.get("color").is_none());
    }

    #[test]
    fn create_issue_request_deserializes_camel_case_and_omits_none() {
        let req: CreateIssueRequest = serde_json::from_value(json!({
            "title": "Do it",
            "teamId": "team-uuid",
            "assigneeId": "u1",
            "labelIds": ["l1", "l2"],
        }))
        .unwrap();
        assert_eq!(req.title, "Do it");
        assert_eq!(req.team_id, "team-uuid");
        assert_eq!(req.assignee_id.as_deref(), Some("u1"));
        assert!(req.description.is_none());
        assert!(req.state_id.is_none());
        assert!(req.priority.is_none());
        assert_eq!(req.label_ids.as_deref().unwrap().len(), 2);

        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v["teamId"], "team-uuid");
        assert_eq!(v["assigneeId"], "u1");
        assert_eq!(v["labelIds"], json!(["l1", "l2"]));
        assert!(v.get("description").is_none());
        assert!(v.get("priority").is_none());
        assert!(v.get("stateId").is_none());
    }

    #[test]
    fn update_issue_request_deserializes_camel_case_and_omits_none() {
        let req: UpdateIssueRequest = serde_json::from_value(json!({
            "issueId": "uuid-1",
            "title": "New title",
            "priority": 2,
        }))
        .unwrap();
        assert_eq!(req.issue_id, "uuid-1");
        assert_eq!(req.title.as_deref(), Some("New title"));
        assert_eq!(req.priority, Some(2.0));
        assert!(req.description.is_none());
        assert!(req.assignee_id.is_none());
        assert!(req.state_id.is_none());

        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v["issueId"], "uuid-1");
        assert_eq!(v["title"], "New title");
        assert!(v.get("assigneeId").is_none());
        assert!(v.get("stateId").is_none());
        assert!(v.get("description").is_none());
    }
}

//! `LinearEngine` — the P0 read surface backing `linear.*`.
//!
//! Three methods mirror the FE calls that exist today: `authStatus`,
//! `listIssues`, `searchIssues`. The `filter` enum maps to **typed Linear
//! GraphQL filters server-side** (replacing the old natural-language prompts).

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::client::LinearClient;
use crate::error::Result;
use crate::model::{AuthStatus, IssueFilter, LinearIssueResult};

/// Default page size when the caller does not specify one.
const DEFAULT_LIMIT: u32 = 50;
/// Linear caps `first` at 250.
const MAX_LIMIT: u32 = 250;

/// GraphQL selection shared by list/search issue queries.
const ISSUE_FIELDS: &str = r#"
    id
    identifier
    title
    description
    url
    priority
    createdAt
    updatedAt
    team { key name }
    state { name }
    assignee { name }
    creator { name }
    project { name }
    labels { nodes { name } }
"#;

/// The P0 Linear read API consumed by `linear.*` wire methods.
#[async_trait]
pub trait LinearEngine: Send + Sync {
    /// Auth / connectivity probe via `viewer { id name email }`.
    async fn auth_status(&self) -> Result<AuthStatus>;

    /// List issues matching `filter` (typed server-side filters).
    async fn list_issues(
        &self,
        filter: IssueFilter,
        limit: Option<u32>,
    ) -> Result<Vec<LinearIssueResult>>;

    /// Full-text search issues by `query`.
    async fn search_issues(
        &self,
        query: &str,
        limit: Option<u32>,
    ) -> Result<Vec<LinearIssueResult>>;
}

/// GraphQL-backed [`LinearEngine`] over [`LinearClient`].
#[derive(Debug)]
pub struct LinearEngineImpl {
    client: LinearClient,
}

impl LinearEngineImpl {
    /// Wrap an existing [`LinearClient`].
    pub fn new(client: LinearClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl LinearEngine for LinearEngineImpl {
    async fn auth_status(&self) -> Result<AuthStatus> {
        let query = "query { viewer { id name email } }";
        match self.client.graphql(query, json!({})).await {
            Ok(data) => Ok(map_auth_status(&data)),
            Err(crate::error::Error::Auth(_)) => Ok(AuthStatus {
                authenticated: false,
                login: None,
                scopes: vec![],
            }),
            Err(e) => Err(e),
        }
    }

    async fn list_issues(
        &self,
        filter: IssueFilter,
        limit: Option<u32>,
    ) -> Result<Vec<LinearIssueResult>> {
        let query = format!(
            "query Issues($first: Int, $filter: IssueFilter) {{ \
                issues(first: $first, filter: $filter) {{ nodes {{ {ISSUE_FIELDS} }} }} \
            }}"
        );
        let variables = json!({
            "first": clamp_limit(limit),
            "filter": build_issue_filter(filter),
        });
        let data = self.client.graphql(&query, variables).await?;
        Ok(map_issue_nodes(data.pointer("/issues/nodes")))
    }

    async fn search_issues(
        &self,
        query: &str,
        limit: Option<u32>,
    ) -> Result<Vec<LinearIssueResult>> {
        let gql = format!(
            "query Search($term: String!, $first: Int) {{ \
                searchIssues(term: $term, first: $first) {{ nodes {{ {ISSUE_FIELDS} }} }} \
            }}"
        );
        let variables = json!({ "term": query, "first": clamp_limit(limit) });
        let data = self.client.graphql(&gql, variables).await?;
        Ok(map_issue_nodes(data.pointer("/searchIssues/nodes")))
    }
}

/// Clamp an optional limit into `[1, MAX_LIMIT]`, defaulting when absent.
fn clamp_limit(limit: Option<u32>) -> u32 {
    limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

/// Map the FE filter enum to a typed Linear `IssueFilter`. `isMe` comparators
/// scope to the viewer without needing a separate id lookup.
pub(crate) fn build_issue_filter(filter: IssueFilter) -> Value {
    match filter {
        IssueFilter::Assigned => json!({ "assignee": { "isMe": { "eq": true } } }),
        IssueFilter::Created => json!({ "creator": { "isMe": { "eq": true } } }),
        IssueFilter::Subscribed => json!({ "subscribers": { "isMe": { "eq": true } } }),
        IssueFilter::Team => json!({ "team": { "members": { "isMe": { "eq": true } } } }),
        IssueFilter::All => json!({}),
    }
}

/// Map a `viewer` payload to an [`AuthStatus`]. A present `viewer` ⇒
/// authenticated; `login` prefers `name`, falling back to `email`.
pub(crate) fn map_auth_status(data: &Value) -> AuthStatus {
    let viewer = data.get("viewer");
    let authenticated = viewer.map(|v| !v.is_null()).unwrap_or(false);
    let login = viewer.and_then(|v| str_field(v, "name").or_else(|| str_field(v, "email")));
    AuthStatus {
        authenticated,
        login,
        scopes: vec![],
    }
}

/// Map a `nodes` array (if present) to flattened issue results.
pub(crate) fn map_issue_nodes(nodes: Option<&Value>) -> Vec<LinearIssueResult> {
    nodes
        .and_then(Value::as_array)
        .map(|arr| arr.iter().map(map_issue).collect())
        .unwrap_or_default()
}

/// Flatten a single Linear issue node into [`LinearIssueResult`].
pub(crate) fn map_issue(node: &Value) -> LinearIssueResult {
    let labels = node
        .pointer("/labels/nodes")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|l| str_field(l, "name"))
                .collect::<Vec<_>>()
        });
    LinearIssueResult {
        id: str_field(node, "id").unwrap_or_default(),
        identifier: str_field(node, "identifier").unwrap_or_default(),
        title: str_field(node, "title").unwrap_or_default(),
        description: str_field(node, "description"),
        url: str_field(node, "url"),
        team_name: node.get("team").and_then(|t| str_field(t, "name")),
        team_key: node.get("team").and_then(|t| str_field(t, "key")),
        state: node.get("state").and_then(|s| str_field(s, "name")),
        priority: node.get("priority").and_then(Value::as_f64),
        assignee: node.get("assignee").and_then(|a| str_field(a, "name")),
        labels,
        project: node.get("project").and_then(|p| str_field(p, "name")),
        creator: node.get("creator").and_then(|c| str_field(c, "name")),
        created_at: str_field(node, "createdAt"),
        updated_at: str_field(node, "updatedAt"),
    }
}

/// Read a non-null string field as an owned `String`.
fn str_field(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(Value::as_str).map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamps_limit() {
        assert_eq!(clamp_limit(None), DEFAULT_LIMIT);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(10)), 10);
        assert_eq!(clamp_limit(Some(9999)), MAX_LIMIT);
    }

    #[test]
    fn filter_maps_to_typed_graphql() {
        assert_eq!(
            build_issue_filter(IssueFilter::Assigned),
            json!({ "assignee": { "isMe": { "eq": true } } })
        );
        assert_eq!(
            build_issue_filter(IssueFilter::Created),
            json!({ "creator": { "isMe": { "eq": true } } })
        );
        assert_eq!(
            build_issue_filter(IssueFilter::Subscribed),
            json!({ "subscribers": { "isMe": { "eq": true } } })
        );
        assert_eq!(
            build_issue_filter(IssueFilter::Team),
            json!({ "team": { "members": { "isMe": { "eq": true } } } })
        );
        assert_eq!(build_issue_filter(IssueFilter::All), json!({}));
    }

    #[test]
    fn maps_auth_status_from_viewer() {
        let s =
            map_auth_status(&json!({ "viewer": { "id": "u1", "name": "Ada", "email": "a@x.io" } }));
        assert!(s.authenticated);
        assert_eq!(s.login.as_deref(), Some("Ada"));
        assert!(s.scopes.is_empty());

        let s = map_auth_status(&json!({ "viewer": { "id": "u1", "email": "a@x.io" } }));
        assert_eq!(s.login.as_deref(), Some("a@x.io"));

        let s = map_auth_status(&json!({ "viewer": null }));
        assert!(!s.authenticated);
        assert!(s.login.is_none());
    }

    #[test]
    fn flattens_issue_node() {
        let node = json!({
            "id": "uuid-1",
            "identifier": "ENG-7",
            "title": "Do it",
            "description": "desc",
            "url": "https://linear.app/x/issue/ENG-7",
            "priority": 2,
            "createdAt": "2026-01-01T00:00:00.000Z",
            "updatedAt": "2026-01-02T00:00:00.000Z",
            "team": { "key": "ENG", "name": "Engineering" },
            "state": { "name": "In Progress" },
            "assignee": { "name": "Ada" },
            "creator": { "name": "Grace" },
            "project": { "name": "Apollo" },
            "labels": { "nodes": [ { "name": "bug" }, { "name": "p1" } ] }
        });
        let r = map_issue(&node);
        assert_eq!(r.identifier, "ENG-7");
        assert_eq!(r.team_key.as_deref(), Some("ENG"));
        assert_eq!(r.team_name.as_deref(), Some("Engineering"));
        assert_eq!(r.state.as_deref(), Some("In Progress"));
        assert_eq!(r.priority, Some(2.0));
        assert_eq!(r.assignee.as_deref(), Some("Ada"));
        assert_eq!(r.creator.as_deref(), Some("Grace"));
        assert_eq!(r.project.as_deref(), Some("Apollo"));
        assert_eq!(r.labels, Some(vec!["bug".to_string(), "p1".to_string()]));
    }

    #[test]
    fn maps_empty_and_missing_nodes() {
        assert!(map_issue_nodes(None).is_empty());
        assert!(map_issue_nodes(Some(&json!(null))).is_empty());
        let one = map_issue_nodes(Some(
            &json!([{ "id": "x", "identifier": "A-1", "title": "t" }]),
        ));
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].labels, None);
    }
}

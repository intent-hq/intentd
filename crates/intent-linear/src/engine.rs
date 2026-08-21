//! `LinearEngine` — the P0 read surface backing `linear.*`.
//!
//! Three methods mirror the FE calls that exist today: `authStatus`,
//! `listIssues`, `searchIssues`. The `filter` enum maps to **typed Linear
//! GraphQL filters server-side** (replacing the old natural-language prompts).

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::client::LinearClient;
use crate::error::{Error, Result};
use crate::model::{
    AuthStatus, CreateIssueRequest, IssueFilter, LinearIssuePage, LinearIssueResult, LinearLabel,
    LinearProject, LinearTeam, LinearUser, LinearWorkflowState, UpdateIssueRequest,
};

/// Default page size when the caller does not specify one.
const DEFAULT_LIMIT: u32 = 50;
/// Linear caps `first` at 250.
const MAX_LIMIT: u32 = 250;

/// GraphQL selection shared by list/search issue queries.
const ISSUE_FIELDS: &str = r"
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
";

/// The P0 Linear read API consumed by `linear.*` wire methods.
#[async_trait]
pub trait LinearEngine: Send + Sync {
    /// Auth / connectivity probe via `viewer { id name email }`.
    async fn auth_status(&self) -> Result<AuthStatus>;

    /// List issues matching `filter` (typed server-side filters). `next_token`
    /// is the opaque cursor from a previous page (threaded as the GraphQL
    /// `after` cursor); absent → first page.
    async fn list_issues(
        &self,
        filter: IssueFilter,
        limit: Option<u32>,
        next_token: Option<&str>,
    ) -> Result<LinearIssuePage>;

    /// Full-text search issues by `query`. `next_token` paginates like
    /// [`Self::list_issues`].
    async fn search_issues(
        &self,
        query: &str,
        limit: Option<u32>,
        next_token: Option<&str>,
    ) -> Result<LinearIssuePage>;

    /// Fetch a single issue by UUID `id` or `ENG-123` `identifier`.
    async fn get_issue(&self, id_or_identifier: &str) -> Result<LinearIssueResult>;

    /// The authenticated user (`viewer`).
    async fn viewer(&self) -> Result<LinearUser>;

    /// List teams.
    async fn list_teams(&self, limit: Option<u32>) -> Result<Vec<LinearTeam>>;

    /// List workflow states.
    async fn list_workflow_states(&self, limit: Option<u32>) -> Result<Vec<LinearWorkflowState>>;

    /// List projects.
    async fn list_projects(&self, limit: Option<u32>) -> Result<Vec<LinearProject>>;

    /// List issue labels.
    async fn list_labels(&self, limit: Option<u32>) -> Result<Vec<LinearLabel>>;

    /// Create a new issue (`issueCreate` mutation). Returns the flattened
    /// issue mirroring the read shape.
    async fn create_issue(&self, req: CreateIssueRequest) -> Result<LinearIssueResult>;

    /// Update an existing issue (`issueUpdate` mutation). Only the fields
    /// present in `req` are sent through `IssueUpdateInput`.
    async fn update_issue(&self, req: UpdateIssueRequest) -> Result<LinearIssueResult>;
}

/// GraphQL-backed [`LinearEngine`] over [`LinearClient`].
#[derive(Debug)]
pub(crate) struct LinearEngineImpl {
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
        next_token: Option<&str>,
    ) -> Result<LinearIssuePage> {
        let query = format!(
            "query Issues($first: Int, $after: String, $filter: IssueFilter) {{ \
                issues(first: $first, after: $after, filter: $filter) \
                {{ nodes {{ {ISSUE_FIELDS} }} pageInfo {{ endCursor hasNextPage }} }} \
            }}"
        );
        let variables = list_issues_variables(filter, limit, next_token);
        let data = self.client.graphql(&query, variables).await?;
        Ok(map_issue_page(data.get("issues")))
    }

    async fn search_issues(
        &self,
        query: &str,
        limit: Option<u32>,
        next_token: Option<&str>,
    ) -> Result<LinearIssuePage> {
        let gql = format!(
            "query Search($term: String!, $first: Int, $after: String) {{ \
                searchIssues(term: $term, first: $first, after: $after) \
                {{ nodes {{ {ISSUE_FIELDS} }} pageInfo {{ endCursor hasNextPage }} }} \
            }}"
        );
        let variables = search_issues_variables(query, limit, next_token);
        let data = self.client.graphql(&gql, variables).await?;
        Ok(map_issue_page(data.get("searchIssues")))
    }

    async fn get_issue(&self, id_or_identifier: &str) -> Result<LinearIssueResult> {
        if let Some((key, number)) = parse_identifier(id_or_identifier) {
            let query = format!(
                "query IssueByKey($key: String!, $number: Float!) {{ \
                    issues(first: 1, filter: {{ team: {{ key: {{ eq: $key }} }}, number: {{ eq: $number }} }}) \
                    {{ nodes {{ {ISSUE_FIELDS} }} }} \
                }}"
            );
            let variables = json!({ "key": key, "number": number });
            let data = self.client.graphql(&query, variables).await?;
            let node = data
                .pointer("/issues/nodes/0")
                .ok_or_else(|| Error::NotFound(format!("issue {id_or_identifier} not found")))?;
            Ok(map_issue(node))
        } else {
            let query =
                format!("query Issue($id: String!) {{ issue(id: $id) {{ {ISSUE_FIELDS} }} }}");
            let variables = json!({ "id": id_or_identifier });
            let data = self.client.graphql(&query, variables).await?;
            let node = data
                .get("issue")
                .filter(|v| !v.is_null())
                .ok_or_else(|| Error::NotFound(format!("issue {id_or_identifier} not found")))?;
            Ok(map_issue(node))
        }
    }

    async fn viewer(&self) -> Result<LinearUser> {
        let query = "query { viewer { id name displayName email avatarUrl } }";
        let data = self.client.graphql(query, json!({})).await?;
        let node = data
            .get("viewer")
            .filter(|v| !v.is_null())
            .ok_or_else(|| Error::NotFound("viewer not found".to_string()))?;
        Ok(map_user(node))
    }

    async fn list_teams(&self, limit: Option<u32>) -> Result<Vec<LinearTeam>> {
        let query = "query Teams($first: Int) { \
            teams(first: $first) { nodes { id key name description } } \
        }";
        let variables = json!({ "first": clamp_limit(limit) });
        let data = self.client.graphql(query, variables).await?;
        Ok(map_nodes(data.pointer("/teams/nodes"), map_team))
    }

    async fn list_workflow_states(&self, limit: Option<u32>) -> Result<Vec<LinearWorkflowState>> {
        let query = "query WorkflowStates($first: Int) { \
            workflowStates(first: $first) { nodes { id name type description color } } \
        }";
        let variables = json!({ "first": clamp_limit(limit) });
        let data = self.client.graphql(query, variables).await?;
        Ok(map_nodes(
            data.pointer("/workflowStates/nodes"),
            map_workflow_state,
        ))
    }

    async fn list_projects(&self, limit: Option<u32>) -> Result<Vec<LinearProject>> {
        let query = "query Projects($first: Int) { \
            projects(first: $first) { nodes { id name description state url } } \
        }";
        let variables = json!({ "first": clamp_limit(limit) });
        let data = self.client.graphql(query, variables).await?;
        Ok(map_nodes(data.pointer("/projects/nodes"), map_project))
    }

    async fn list_labels(&self, limit: Option<u32>) -> Result<Vec<LinearLabel>> {
        let query = "query Labels($first: Int) { \
            issueLabels(first: $first) { nodes { id name description color } } \
        }";
        let variables = json!({ "first": clamp_limit(limit) });
        let data = self.client.graphql(query, variables).await?;
        Ok(map_nodes(data.pointer("/issueLabels/nodes"), map_label))
    }

    async fn create_issue(&self, req: CreateIssueRequest) -> Result<LinearIssueResult> {
        let mutation = format!(
            "mutation CreateIssue($input: IssueCreateInput!) {{ \
                issueCreate(input: $input) {{ success issue {{ {ISSUE_FIELDS} }} }} \
            }}"
        );
        let variables = json!({ "input": build_create_input(&req) });
        let data = self.client.graphql(&mutation, variables).await?;
        let node = data
            .pointer("/issueCreate/issue")
            .filter(|v| !v.is_null())
            .ok_or_else(|| Error::Api("issueCreate returned no issue".to_string()))?;
        Ok(map_issue(node))
    }

    async fn update_issue(&self, req: UpdateIssueRequest) -> Result<LinearIssueResult> {
        let mutation = format!(
            "mutation UpdateIssue($id: String!, $input: IssueUpdateInput!) {{ \
                issueUpdate(id: $id, input: $input) {{ success issue {{ {ISSUE_FIELDS} }} }} \
            }}"
        );
        let variables = json!({
            "id": req.issue_id,
            "input": build_update_input(&req),
        });
        let data = self.client.graphql(&mutation, variables).await?;
        let node = data
            .pointer("/issueUpdate/issue")
            .filter(|v| !v.is_null())
            .ok_or_else(|| Error::Api("issueUpdate returned no issue".to_string()))?;
        Ok(map_issue(node))
    }
}

/// Clamp an optional limit into `[1, MAX_LIMIT]`, defaulting when absent.
fn clamp_limit(limit: Option<u32>) -> u32 {
    limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

/// Variables for the `issues` list query: the clamped `first`, the typed
/// `filter`, and the `after` cursor (`null` on the first page).
pub(crate) fn list_issues_variables(
    filter: IssueFilter,
    limit: Option<u32>,
    after: Option<&str>,
) -> Value {
    json!({
        "first": clamp_limit(limit),
        "after": after,
        "filter": build_issue_filter(filter),
    })
}

/// Variables for the `searchIssues` query: `term`, the clamped `first`, and
/// the `after` cursor (`null` on the first page).
pub(crate) fn search_issues_variables(
    term: &str,
    limit: Option<u32>,
    after: Option<&str>,
) -> Value {
    json!({ "term": term, "first": clamp_limit(limit), "after": after })
}

/// Build the GraphQL `IssueCreateInput` from [`CreateIssueRequest`]. Optional
/// fields are only included when present so they don't overwrite defaults.
pub(crate) fn build_create_input(req: &CreateIssueRequest) -> Value {
    let mut input = serde_json::Map::new();
    input.insert("title".to_string(), Value::String(req.title.clone()));
    input.insert("teamId".to_string(), Value::String(req.team_id.clone()));
    if let Some(d) = &req.description {
        input.insert("description".to_string(), Value::String(d.clone()));
    }
    if let Some(id) = &req.assignee_id {
        input.insert("assigneeId".to_string(), Value::String(id.clone()));
    }
    if let Some(id) = &req.state_id {
        input.insert("stateId".to_string(), Value::String(id.clone()));
    }
    if let Some(p) = req.priority {
        input.insert("priority".to_string(), json!(p));
    }
    if let Some(ids) = &req.label_ids {
        input.insert("labelIds".to_string(), json!(ids));
    }
    Value::Object(input)
}

/// Build the GraphQL `IssueUpdateInput` from [`UpdateIssueRequest`]. Only the
/// fields present in `req` are included so an absent field is left untouched on
/// the server.
pub(crate) fn build_update_input(req: &UpdateIssueRequest) -> Value {
    let mut input = serde_json::Map::new();
    if let Some(t) = &req.title {
        input.insert("title".to_string(), Value::String(t.clone()));
    }
    if let Some(d) = &req.description {
        input.insert("description".to_string(), Value::String(d.clone()));
    }
    if let Some(id) = &req.assignee_id {
        input.insert("assigneeId".to_string(), Value::String(id.clone()));
    }
    if let Some(id) = &req.state_id {
        input.insert("stateId".to_string(), Value::String(id.clone()));
    }
    if let Some(p) = req.priority {
        input.insert("priority".to_string(), json!(p));
    }
    Value::Object(input)
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
    let authenticated = viewer.is_some_and(|v| !v.is_null());
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

/// Map an issue connection (`{ nodes, pageInfo }`) into a [`LinearIssuePage`]:
/// `next_token` carries `pageInfo.endCursor` only when `hasNextPage` is true.
pub(crate) fn map_issue_page(connection: Option<&Value>) -> LinearIssuePage {
    let issues = map_issue_nodes(connection.and_then(|c| c.get("nodes")));
    let next_token = connection
        .and_then(|c| c.get("pageInfo"))
        .filter(|p| p.get("hasNextPage").and_then(Value::as_bool) == Some(true))
        .and_then(|p| str_field(p, "endCursor"));
    LinearIssuePage { issues, next_token }
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

/// Map a `nodes` array (if present) using `f`, defaulting to empty.
fn map_nodes<T>(nodes: Option<&Value>, f: impl Fn(&Value) -> T) -> Vec<T> {
    nodes
        .and_then(Value::as_array)
        .map(|arr| arr.iter().map(f).collect())
        .unwrap_or_default()
}

/// Map a `viewer`/user node into a [`LinearUser`].
pub(crate) fn map_user(node: &Value) -> LinearUser {
    LinearUser {
        id: str_field(node, "id").unwrap_or_default(),
        name: str_field(node, "name").unwrap_or_default(),
        display_name: str_field(node, "displayName"),
        email: str_field(node, "email"),
        avatar_url: str_field(node, "avatarUrl"),
    }
}

/// Map a team node into a [`LinearTeam`].
pub(crate) fn map_team(node: &Value) -> LinearTeam {
    LinearTeam {
        id: str_field(node, "id").unwrap_or_default(),
        key: str_field(node, "key").unwrap_or_default(),
        name: str_field(node, "name").unwrap_or_default(),
        description: str_field(node, "description"),
    }
}

/// Map a workflow-state node into a [`LinearWorkflowState`].
pub(crate) fn map_workflow_state(node: &Value) -> LinearWorkflowState {
    LinearWorkflowState {
        id: str_field(node, "id").unwrap_or_default(),
        name: str_field(node, "name").unwrap_or_default(),
        r#type: str_field(node, "type").unwrap_or_default(),
        description: str_field(node, "description"),
        color: str_field(node, "color"),
    }
}

/// Map a project node into a [`LinearProject`].
pub(crate) fn map_project(node: &Value) -> LinearProject {
    LinearProject {
        id: str_field(node, "id").unwrap_or_default(),
        name: str_field(node, "name").unwrap_or_default(),
        description: str_field(node, "description"),
        state: str_field(node, "state").unwrap_or_default(),
        url: str_field(node, "url"),
    }
}

/// Map a label node into a [`LinearLabel`].
pub(crate) fn map_label(node: &Value) -> LinearLabel {
    LinearLabel {
        id: str_field(node, "id").unwrap_or_default(),
        name: str_field(node, "name").unwrap_or_default(),
        description: str_field(node, "description"),
        color: str_field(node, "color"),
    }
}

/// Detect an `ENG-123`-shaped identifier and split it into `(team_key, number)`.
/// Matches `^[A-Z0-9]+-[0-9]+$`; anything else (e.g. a UUID) returns `None` and
/// routes through the `issue(id:)` path instead.
pub(crate) fn parse_identifier(s: &str) -> Option<(String, u64)> {
    let (key, num) = s.rsplit_once('-')?;
    if key.is_empty()
        || !key
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
    {
        return None;
    }
    if num.is_empty() || !num.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    num.parse().ok().map(|n| (key.to_string(), n))
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
    fn list_variables_thread_after_cursor() {
        let v = list_issues_variables(IssueFilter::Assigned, Some(10), Some("cursor-1"));
        assert_eq!(v["first"], json!(10));
        assert_eq!(v["after"], json!("cursor-1"));
        assert_eq!(
            v["filter"],
            json!({ "assignee": { "isMe": { "eq": true } } })
        );

        // First page: no cursor → `after` is null (limit still clamps/defaults).
        let v = list_issues_variables(IssueFilter::All, None, None);
        assert_eq!(v["first"], json!(DEFAULT_LIMIT));
        assert_eq!(v["after"], json!(null));
    }

    #[test]
    fn search_variables_thread_after_cursor() {
        let v = search_issues_variables("widget", Some(20), Some("cursor-2"));
        assert_eq!(v["term"], json!("widget"));
        assert_eq!(v["first"], json!(20));
        assert_eq!(v["after"], json!("cursor-2"));

        let v = search_issues_variables("widget", None, None);
        assert_eq!(v["first"], json!(DEFAULT_LIMIT));
        assert_eq!(v["after"], json!(null));
    }

    #[test]
    fn maps_issue_page_with_next_token_only_when_has_next_page() {
        let page = map_issue_page(Some(&json!({
            "nodes": [{ "id": "x", "identifier": "A-1", "title": "t" }],
            "pageInfo": { "endCursor": "cursor-9", "hasNextPage": true }
        })));
        assert_eq!(page.issues.len(), 1);
        assert_eq!(page.next_token.as_deref(), Some("cursor-9"));

        // Last page: hasNextPage false → no token even with an endCursor.
        let page = map_issue_page(Some(&json!({
            "nodes": [],
            "pageInfo": { "endCursor": "cursor-9", "hasNextPage": false }
        })));
        assert!(page.next_token.is_none());

        // Defensive: missing pageInfo / null endCursor / absent connection.
        let page = map_issue_page(Some(&json!({ "nodes": [] })));
        assert!(page.next_token.is_none());
        let page = map_issue_page(Some(&json!({
            "nodes": [],
            "pageInfo": { "endCursor": null, "hasNextPage": true }
        })));
        assert!(page.next_token.is_none());
        let page = map_issue_page(None);
        assert!(page.issues.is_empty());
        assert!(page.next_token.is_none());
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

    #[test]
    fn maps_user_node() {
        let u = map_user(&json!({
            "id": "u1", "name": "Ada", "displayName": "ada",
            "email": "a@x.io", "avatarUrl": "https://x/a.png"
        }));
        assert_eq!(u.id, "u1");
        assert_eq!(u.name, "Ada");
        assert_eq!(u.display_name.as_deref(), Some("ada"));
        assert_eq!(u.email.as_deref(), Some("a@x.io"));
        assert_eq!(u.avatar_url.as_deref(), Some("https://x/a.png"));

        let bare = map_user(&json!({ "id": "u2", "name": "Bob" }));
        assert!(bare.display_name.is_none());
        assert!(bare.email.is_none());
        assert!(bare.avatar_url.is_none());
    }

    #[test]
    fn maps_team_node() {
        let t = map_team(&json!({
            "id": "t1", "key": "ENG", "name": "Engineering", "description": "core"
        }));
        assert_eq!(t.key, "ENG");
        assert_eq!(t.name, "Engineering");
        assert_eq!(t.description.as_deref(), Some("core"));
        assert!(
            map_team(&json!({ "id": "t2", "key": "OPS", "name": "Ops" }))
                .description
                .is_none()
        );
    }

    #[test]
    fn maps_workflow_state_node() {
        let s = map_workflow_state(&json!({
            "id": "s1", "name": "In Progress", "type": "started",
            "description": "d", "color": "#abc"
        }));
        assert_eq!(s.name, "In Progress");
        assert_eq!(s.r#type, "started");
        assert_eq!(s.color.as_deref(), Some("#abc"));
        let bare = map_workflow_state(&json!({ "id": "s2", "name": "Todo", "type": "unstarted" }));
        assert!(bare.description.is_none());
        assert!(bare.color.is_none());
    }

    #[test]
    fn maps_project_node() {
        let p = map_project(&json!({
            "id": "p1", "name": "Apollo", "state": "started",
            "url": "https://linear.app/x/project/apollo"
        }));
        assert_eq!(p.name, "Apollo");
        assert_eq!(p.state, "started");
        assert_eq!(
            p.url.as_deref(),
            Some("https://linear.app/x/project/apollo")
        );
        assert!(p.description.is_none());
    }

    #[test]
    fn maps_label_node() {
        let l = map_label(&json!({ "id": "l1", "name": "bug", "color": "#f00" }));
        assert_eq!(l.name, "bug");
        assert_eq!(l.color.as_deref(), Some("#f00"));
        assert!(l.description.is_none());
    }

    #[test]
    fn maps_nodes_empty_and_missing() {
        assert!(map_nodes(None, map_team).is_empty());
        assert!(map_nodes(Some(&json!(null)), map_team).is_empty());
        let teams = map_nodes(
            Some(&json!([{ "id": "t", "key": "K", "name": "N" }])),
            map_team,
        );
        assert_eq!(teams.len(), 1);
    }

    #[test]
    fn parses_identifier_shape() {
        assert_eq!(parse_identifier("ENG-123"), Some(("ENG".to_string(), 123)));
        assert_eq!(parse_identifier("X1-7"), Some(("X1".to_string(), 7)));
        // UUIDs and other shapes are not identifiers.
        assert_eq!(
            parse_identifier("3d1b0f7a-8f3a-4b2a-9c1a-2b6a0c4b9a11"),
            None
        );
        assert_eq!(parse_identifier("eng-123"), None);
        assert_eq!(parse_identifier("ENG-"), None);
        assert_eq!(parse_identifier("-123"), None);
        assert_eq!(parse_identifier("ENG123"), None);
        assert_eq!(parse_identifier("ENG-12a"), None);
    }

    #[test]
    fn build_create_input_includes_required_and_omits_absent() {
        let req = CreateIssueRequest {
            title: "Do it".into(),
            team_id: "team-uuid".into(),
            ..Default::default()
        };
        let v = build_create_input(&req);
        assert_eq!(v["title"], "Do it");
        assert_eq!(v["teamId"], "team-uuid");
        assert!(v.get("description").is_none());
        assert!(v.get("assigneeId").is_none());
        assert!(v.get("stateId").is_none());
        assert!(v.get("priority").is_none());
        assert!(v.get("labelIds").is_none());
    }

    #[test]
    fn build_create_input_includes_all_optionals_when_set() {
        let req = CreateIssueRequest {
            title: "Do it".into(),
            description: Some("desc".into()),
            team_id: "team-uuid".into(),
            assignee_id: Some("u1".into()),
            state_id: Some("s1".into()),
            priority: Some(2.0),
            label_ids: Some(vec!["l1".into(), "l2".into()]),
        };
        let v = build_create_input(&req);
        assert_eq!(v["title"], "Do it");
        assert_eq!(v["teamId"], "team-uuid");
        assert_eq!(v["description"], "desc");
        assert_eq!(v["assigneeId"], "u1");
        assert_eq!(v["stateId"], "s1");
        assert_eq!(v["priority"], json!(2.0));
        assert_eq!(v["labelIds"], json!(["l1", "l2"]));
    }

    #[test]
    fn build_update_input_only_includes_present_fields() {
        let req = UpdateIssueRequest {
            issue_id: "uuid-1".into(),
            ..Default::default()
        };
        let v = build_update_input(&req);
        assert!(v.as_object().unwrap().is_empty());

        let req = UpdateIssueRequest {
            issue_id: "uuid-1".into(),
            title: Some("New".into()),
            priority: Some(0.0),
            ..Default::default()
        };
        let v = build_update_input(&req);
        assert_eq!(v["title"], "New");
        assert_eq!(v["priority"], json!(0.0));
        assert!(v.get("description").is_none());
        assert!(v.get("assigneeId").is_none());
        assert!(v.get("stateId").is_none());
    }
}

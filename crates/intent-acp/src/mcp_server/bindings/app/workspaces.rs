//! `ws.app.workspaces.*` bindings (chief-gated).
//!
//! Exposes workspace management methods (`list`, `get`) exclusively to
//! Chief-of-Staff workspace agents. Non-chief agents receive a clear gating
//! error. Shape parity with the TS reference
//! `packages/cloudlands-fe/src/features/mcp/main/mcp/ws-app-workspaces-api.ts`.

use std::sync::Arc;

use intent_core::{PublishEvent, WorkspaceApi, WorkspaceId, WorkspaceStatus};
use serde_json::{json, Value};

use crate::mcp_server::bindings::{map_err, opt_bool, opt_str, opt_vec_str};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.app = ws.app || {};
    ws.app.workspaces = {
        list: (options) => host({ method: 'app.workspaces.list', args: options || {} }),
        get: (id) => host({ method: 'app.workspaces.get', args: { id } }),
        create: (params) => host({ method: 'app.workspaces.create', args: params || {} }),
        archive: (id) => host({ method: 'app.workspaces.archive', args: { id } }),
        delete: (id) => host({ method: 'app.workspaces.delete', args: { id } }),
        open: (id, options) => host({ method: 'app.workspaces.open', args: { id, ...(options || {}) } }),
        bulkArchive: (ids) => host({ method: 'app.workspaces.bulkArchive', args: { ids } }),
        bulkDelete: (ids) => host({ method: 'app.workspaces.bulkDelete', args: { ids } }),
    };
";

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    // Chief-workspace gating: all ws.app.* methods require the caller to be
    // in the Chief workspace.
    if !workspace_id.is_chief() {
        return Err("ws.app.* is only available in the Chief of Staff workspace".to_string());
    }

    match method {
        "list" => list(api, args).await,
        "get" => get(api, args).await,
        "create" => create(api, args).await,
        "archive" => archive(api, args).await,
        "delete" => delete(api, args).await,
        "open" => open(api, workspace_id, args).await,
        "bulkArchive" => bulk_archive(api, args).await,
        "bulkDelete" => bulk_delete(api, args).await,
        other => Err(format!("host: unknown method `app.workspaces.{other}`")),
    }
}

async fn list(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    // Extract filter options
    let filter_obj = args.get("filter");
    let query = filter_obj.and_then(|f| opt_str(f, "query").or_else(|| opt_str(f, "search")));
    let status_filter = filter_obj.and_then(|f| {
        f.get("status").and_then(|s| {
            if let Some(arr) = s.as_array() {
                Some(
                    arr.iter()
                        .filter_map(|v| v.as_str().map(str::to_lowercase))
                        .collect::<Vec<_>>(),
                )
            } else {
                s.as_str().map(|s| vec![s.to_lowercase()])
            }
        })
    });
    let repository_path = filter_obj.and_then(|f| opt_str(f, "repositoryPath"));
    let repository_owner = filter_obj.and_then(|f| opt_str(f, "repositoryOwner"));
    let repository_name = filter_obj.and_then(|f| opt_str(f, "repositoryName"));
    let tag = filter_obj.and_then(|f| opt_str(f, "tag"));
    let tags = filter_obj.and_then(|f| opt_vec_str(f, "tags"));
    let include_deleted = filter_obj
        .and_then(|f| opt_bool(f, "includeDeleted"))
        .unwrap_or(false);

    // Fetch all workspaces (include archived so we can apply status filter)
    let workspaces = api.list_workspaces(true).await.map_err(map_err)?;

    // Filter and summarize
    let mut results = Vec::new();
    for ws in workspaces {
        // Never surface __chief__ itself
        if ws.id.is_chief() {
            continue;
        }

        // Status filtering
        let status_str = format!("{:?}", ws.status).to_lowercase();
        if !include_deleted && status_str == "deleted" && status_filter.is_none() {
            continue;
        }
        if let Some(ref statuses) = status_filter {
            if !statuses.contains(&status_str) {
                continue;
            }
        }

        // Repository filters
        if let Some(ref path) = repository_path {
            if ws.repository_path.as_deref() != Some(path.as_str()) {
                continue;
            }
        }
        if let Some(ref owner) = repository_owner {
            if ws.repository_owner.as_deref() != Some(owner.as_str()) {
                continue;
            }
        }
        if let Some(ref name) = repository_name {
            if ws.repository_name.as_deref() != Some(name.as_str()) {
                continue;
            }
        }

        // Tag filters
        if let Some(ref single_tag) = tag {
            if !ws.tags.contains(single_tag) {
                continue;
            }
        }
        if let Some(ref tag_list) = tags {
            if !tag_list.iter().all(|t| ws.tags.contains(t)) {
                continue;
            }
        }

        // Query filter (searches across multiple fields)
        if let Some(ref q) = query {
            let q_lower = q.to_lowercase();
            let matches = [
                ws.id.as_str(),
                &ws.title,
                ws.status_message.as_deref().unwrap_or(""),
                &ws.branch,
                ws.repository_path.as_deref().unwrap_or(""),
                ws.repository_owner.as_deref().unwrap_or(""),
                ws.repository_name.as_deref().unwrap_or(""),
            ]
            .iter()
            .any(|field| field.to_lowercase().contains(&q_lower));
            if !matches {
                continue;
            }
        }

        results.push(summarize_workspace(&ws));
    }

    // Apply sort
    if let Some(sort_obj) = args.get("sort") {
        let (sort_by, sort_order) = if let Some(s) = sort_obj.as_str() {
            let order = if s.starts_with('-') { "desc" } else { "asc" };
            let by = s.trim_start_matches('-').to_string();
            (by, order.to_string())
        } else {
            let by = opt_str(sort_obj, "by").unwrap_or_else(|| "updatedAt".to_string());
            let order = opt_str(sort_obj, "order").unwrap_or_else(|| "desc".to_string());
            (by, order)
        };

        results.sort_by(|a, b| {
            let left = a.get(&sort_by).and_then(|v| v.as_str()).unwrap_or("");
            let right = b.get(&sort_by).and_then(|v| v.as_str()).unwrap_or("");
            let cmp = left.to_lowercase().cmp(&right.to_lowercase());
            if sort_order == "asc" {
                cmp
            } else {
                cmp.reverse()
            }
        });
    }

    Ok(Value::Array(results))
}

async fn get(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    let id = args
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| "id is required".to_string())?;

    let workspace = api
        .get_workspace(WorkspaceId::from_string(id.to_string()))
        .await
        .map_err(map_err)?;

    // Never surface __chief__ via ws.app.workspaces.get
    if workspace.id.is_chief() {
        return Err(format!("Workspace not found: {id}"));
    }

    Ok(summarize_workspace(&workspace))
}

fn summarize_workspace(ws: &intent_core::Workspace) -> Value {
    json!({
        "id": ws.id.as_str(),
        "title": if ws.title.is_empty() { "Untitled" } else { &ws.title },
        "status": format!("{:?}", ws.status),
        "statusMessage": ws.status_message.as_deref(),
        "branch": &ws.branch,
        "baseRef": ws.base_ref.as_deref(),
        "repositoryPath": ws.repository_path.as_deref(),
        "repositoryOwner": ws.repository_owner.as_deref(),
        "repositoryName": ws.repository_name.as_deref(),
        "worktreePath": ws.worktree_path.as_deref(),
        "tags": &ws.tags,
        "createdAt": ws.created_at.as_str(),
        "updatedAt": ws.updated_at.as_str(),
        "lastActivity": ws.last_activity.as_deref(),
    })
}

async fn open(
    api: &Arc<dyn WorkspaceApi>,
    caller_workspace_id: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let id = args
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| "id is required".to_string())?;

    assert_mutable_workspace_id(id)?;

    let open_in_new_window = opt_bool(args, "openInNewWindow").unwrap_or(false);

    // Validate workspace exists
    let workspace_id = WorkspaceId::from_string(id.to_string());
    let _workspace = api
        .get_workspace(workspace_id.clone())
        .await
        .map_err(map_err)?;

    // Emit app:workspace-open event scoped to the caller (chief) workspace,
    // not the target workspace. App-level UI events are subscribed to via
    // the chief workspace context so subscribers can observe all workspace
    // navigation.
    let event_data = json!({
        "workspaceId": id,
        "openInNewWindow": open_in_new_window,
    });
    let event = PublishEvent {
        workspace_id: caller_workspace_id.clone(),
        event_type: intent_core::events::APP_WORKSPACE_OPEN.to_string(),
        data: event_data,
    };
    api.publish_event(event).await.map_err(map_err)?;

    Ok(json!({
        "ok": true,
        "queued": true,
    }))
}

/// MCP resource MIME type for proposals (parity with FE `proposal-resource.ts`).
const PROPOSAL_RESOURCE_MIME_TYPE: &str = "application/vnd.intent.proposal+json";

/// Build proposal resource URI (parity with TS `proposalResourceId` + `createProposalResource`).
fn proposal_resource_uri(proposal: &Value) -> String {
    let kind = proposal
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("unknown");

    // Use applyToolCallId if present, otherwise use preview.title
    let id = proposal
        .get("applyToolCallId")
        .and_then(Value::as_str)
        .or_else(|| {
            proposal
                .get("preview")
                .and_then(|p| p.get("title"))
                .and_then(Value::as_str)
        })
        .unwrap_or("untitled");

    // RFC3986 percent-encode the id portion for URI path segment use
    let encoded_id = super::proposal::percent_encode_path_segment(id);
    format!("intent-proposal://{kind}/{encoded_id}")
}

/// Return a proposal with dual text+resource content items.
#[allow(clippy::unnecessary_wraps)] // dispatch arm helper; keeps the uniform Result shape
pub(crate) fn proposal_result(proposal: &Value) -> Result<Value, String> {
    // Build resource name from preview.title
    let name = proposal
        .get("preview")
        .and_then(|p| p.get("title"))
        .and_then(Value::as_str)
        .unwrap_or("Proposal");

    // Build MCP content items: text item with {ok, proposal} + resource item
    let text_item = json!({
        "type": "text",
        "text": serde_json::to_string_pretty(&json!({
            "ok": true,
            "proposal": proposal
        })).unwrap_or_else(|_| "{}".to_string())
    });

    let resource_item = json!({
        "type": "resource",
        "resource": {
            "uri": proposal_resource_uri(proposal),
            "name": name,
            "mimeType": PROPOSAL_RESOURCE_MIME_TYPE,
            "text": serde_json::to_string(&proposal).unwrap_or_else(|_| "{}".to_string())
        }
    });

    // Return with __mcpContentItems marker (dispatch.rs will extract this)
    Ok(json!({
        "ok": true,
        "proposal": proposal,
        "__mcpContentItems": [text_item, resource_item]
    }))
}

/// Validate that a workspace ID is mutable (not __chief__).
fn assert_mutable_workspace_id(id: &str) -> Result<(), String> {
    if id.is_empty() {
        return Err("workspace id is required".to_string());
    }
    if id == "__chief__" {
        return Err("The Chief virtual workspace cannot be modified".to_string());
    }
    Ok(())
}

/// Normalized workspace-create fields (parity with the TS
/// `WorkspaceCreateProposalFields` shape in `shared/types/proposal.ts`).
#[derive(Debug, Default)]
struct WorkspaceCreateFields {
    initial_prompt: Option<String>,
    repo_path: Option<String>,
    repo_type: &'static str,
    github_url: Option<String>,
    pr_number: Option<u64>,
    clone_path: Option<String>,
    /// `None` when the caller supplied no branch/baseRef; `create()` fills in
    /// the repo's actual default branch (or `main`) before building the preview.
    branch: Option<String>,
    is_new_repo: bool,
    scope: Option<String>,
    specialist: Option<String>,
}

/// Trimmed non-empty string field (TS `stringValue`).
fn string_value(params: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
    params
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Strip a trailing `.git` suffix (case-insensitive). Compares bytes so
/// non-ASCII input can't hit a char-boundary panic; an ASCII byte match
/// guarantees the re-slice boundary is valid.
fn strip_git_suffix(s: &str) -> &str {
    if s.len() >= 4 && s.as_bytes()[s.len() - 4..].eq_ignore_ascii_case(b".git") {
        &s[..s.len() - 4]
    } else {
        s
    }
}

/// Case-insensitive prefix strip. Compares bytes so non-ASCII input can't
/// hit a char-boundary panic (prefixes are pure ASCII).
fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.len() >= prefix.len()
        && s.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
    {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}

/// `owner/repo` shorthand check (TS `GITHUB_OWNER_REPO_PATTERN`).
fn is_owner_repo_shorthand(s: &str) -> bool {
    let mut parts = s.split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(owner), Some(repo), None) => {
            let valid = |part: &str| {
                part.chars()
                    .all(|c| c.is_ascii_alphanumeric() || "_.-".contains(c))
            };
            !owner.is_empty() && !repo.is_empty() && valid(owner) && valid(repo)
        }
        _ => false,
    }
}

/// Convert a `repository` param (owner/repo shorthand, https URL, or git@
/// SSH form) to a canonical GitHub https URL (TS `repositoryToGithubUrl`).
fn repository_to_github_url(repository: &str) -> Option<String> {
    let trimmed = strip_git_suffix(repository.trim());
    if is_owner_repo_shorthand(trimmed) {
        return Some(format!("https://github.com/{trimmed}"));
    }

    if let Some(rest) = strip_prefix_ci(trimmed, "https://github.com/")
        .or_else(|| strip_prefix_ci(trimmed, "http://github.com/"))
    {
        let path = rest.split(['?', '#']).next().unwrap_or("");
        let mut segs = path.split('/').filter(|s| !s.is_empty());
        if let (Some(owner), Some(repo)) = (segs.next(), segs.next()) {
            return Some(format!(
                "https://github.com/{owner}/{}",
                strip_git_suffix(repo)
            ));
        }
        return None;
    }

    if let Some(rest) = strip_prefix_ci(trimmed, "git@github.com:") {
        let mut segs = rest.split('/').filter(|s| !s.is_empty());
        if let (Some(owner), Some(repo)) = (segs.next(), segs.next()) {
            return Some(format!(
                "https://github.com/{owner}/{}",
                strip_git_suffix(repo)
            ));
        }
    }

    None
}

/// Treat a `repository` param as a local path when it looks path-like
/// (TS `repositoryToLocalPath`).
fn repository_to_local_path(repository: &str) -> Option<String> {
    let t = repository.trim();
    if t.is_empty() {
        return None;
    }
    let bytes = t.as_bytes();
    let windows_drive = bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && (bytes[2] == b'/' || bytes[2] == b'\\');
    if t.starts_with('~') || t.starts_with("./") || t.starts_with('/') || windows_drive {
        return Some(t.to_string());
    }
    if !t.contains('/') {
        return Some(t.to_string());
    }
    None
}

/// Parse `https://github.com/<owner>/<repo>/pull/<n>` or `/issues/<n>` URLs
/// down to the plain repo URL. Returns the repo URL plus the PR number when
/// the URL was a pull-request link. The TS reference (`parseGithubPrUrl`)
/// only handles `/pull/`; issues URLs are normalized here too because Chief
/// agents pass them as `githubUrl` when proposing issue-fix workspaces.
fn parse_github_pr_or_issue_url(url: &str) -> Option<(String, Option<u64>)> {
    let rest = strip_prefix_ci(url.trim(), "https://")?;
    let (host, path) = rest.split_once('/')?;
    if !host.eq_ignore_ascii_case("github.com") && !host.eq_ignore_ascii_case("www.github.com") {
        return None;
    }
    let path = path.split(['?', '#']).next().unwrap_or("");
    let mut segs = path.split('/').filter(|s| !s.is_empty());
    let owner = segs.next()?;
    let repo = segs.next()?;
    let segment = segs.next()?;
    let number = segs.next()?;
    if segment != "pull" && segment != "issues" {
        return None;
    }
    let n: u64 = number.parse().ok()?;
    if n == 0 {
        return None;
    }
    let repo_url = format!("https://github.com/{owner}/{}", strip_git_suffix(repo));
    Some((repo_url, (segment == "pull").then_some(n)))
}

/// Parse a GitHub URL (https, www, or git@ form) into lowercase
/// `(owner, repo)` (TS `parseGithubOwnerRepo`).
fn parse_github_owner_repo(github_url: &str) -> Option<(String, String)> {
    let t = github_url.trim();
    let stripped = strip_prefix_ci(t, "https://www.github.com/")
        .or_else(|| strip_prefix_ci(t, "http://www.github.com/"))
        .or_else(|| strip_prefix_ci(t, "https://github.com/"))
        .or_else(|| strip_prefix_ci(t, "http://github.com/"))
        .or_else(|| strip_prefix_ci(t, "git@github.com:"))
        .unwrap_or(t);
    let stripped = strip_git_suffix(stripped);
    let mut segs = stripped.split('/').filter(|s| !s.is_empty());
    let owner = segs.next()?;
    let repo = segs.next()?;
    Some((owner.to_lowercase(), repo.to_lowercase()))
}

/// Port of the TS `normalizeWorkspaceCreateFields`: derive the editable
/// preview fields for a workspace-create proposal from raw caller params.
fn normalize_workspace_create_fields(
    params: &serde_json::Map<String, Value>,
) -> WorkspaceCreateFields {
    let initial_agent = params.get("initialAgent").and_then(Value::as_object);
    let repository = string_value(params, "repository");
    let owner_name_github_url = match (
        string_value(params, "repositoryOwner"),
        string_value(params, "repositoryName"),
    ) {
        (Some(owner), Some(name)) => Some(format!(
            "https://github.com/{owner}/{}",
            strip_git_suffix(&name)
        )),
        _ => None,
    };

    // Callers commonly pass a PR or issue URL directly as `githubUrl` (e.g.
    // when proposing a PR-review or issue-fix workspace). Strip the
    // `/pull/<n>` / `/issues/<n>` suffix down to the repo URL so the cloner
    // doesn't try to fetch a non-existent ref.
    let github_url_parsed =
        string_value(params, "githubUrl").and_then(|s| parse_github_pr_or_issue_url(&s));
    let pr_url_parsed =
        string_value(params, "prUrl").and_then(|s| parse_github_pr_or_issue_url(&s));
    let github_url = github_url_parsed
        .as_ref()
        .map(|(u, _)| u.clone())
        .or_else(|| string_value(params, "githubUrl"))
        .or_else(|| repository.as_deref().and_then(repository_to_github_url))
        .or(owner_name_github_url)
        .or_else(|| pr_url_parsed.as_ref().map(|(u, _)| u.clone()));
    // Only take `prUrl`'s number when its repo matches the resolved
    // `githubUrl`, so an issues URL of repo A plus a pull URL of repo B
    // can't yield a mismatched `(githubUrl, prNumber)` pair.
    let pr_number = github_url_parsed
        .as_ref()
        .and_then(|(_, n)| *n)
        .or_else(|| {
            pr_url_parsed.as_ref().and_then(|(u, n)| {
                if github_url.as_deref() == Some(u.as_str()) {
                    *n
                } else {
                    None
                }
            })
        });

    let repo_path = string_value(params, "repositoryPath")
        .or_else(|| string_value(params, "repoPath"))
        .or_else(|| {
            if github_url.is_none() {
                repository.as_deref().and_then(repository_to_local_path)
            } else {
                None
            }
        });

    // Intentional divergence from TS truthiness (`params.environmentConfig ?
    // 'remote' : 'local'`): only object values count as an environment
    // config, so `false` / `0` / `""` stay "local" here too.
    let repo_type = if github_url.is_some() {
        "github"
    } else if params
        .get("environmentConfig")
        .and_then(Value::as_object)
        .is_some()
    {
        "remote"
    } else {
        "local"
    };

    WorkspaceCreateFields {
        initial_prompt: initial_agent
            .and_then(|a| string_value(a, "prompt"))
            .or_else(|| string_value(params, "initialMessage"))
            .or_else(|| string_value(params, "initialPrompt"))
            .or_else(|| string_value(params, "prompt")),
        // workspace.service requires `clonePath` whenever `githubUrl` is set;
        // the cloner reuses an existing checkout when the directory already
        // points at the same remote, so falling back to `repoPath` lets MCP
        // callers omit `clonePath` when they only know the local repo path.
        clone_path: string_value(params, "clonePath").or_else(|| {
            if github_url.is_some() {
                repo_path.clone()
            } else {
                None
            }
        }),
        branch: string_value(params, "branch").or_else(|| string_value(params, "baseRef")),
        is_new_repo: params
            .get("isNewRepo")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        scope: string_value(params, "scope"),
        specialist: initial_agent
            .and_then(|a| string_value(a, "specialist"))
            .or_else(|| string_value(params, "specialist")),
        repo_path,
        repo_type,
        github_url,
        pr_number,
    }
}

/// Serialize normalized fields as the `preview.workspaceCreate` object,
/// omitting unset optionals (parity with TS JSON serialization dropping
/// `undefined` values).
fn workspace_create_preview(fields: &WorkspaceCreateFields) -> Value {
    let mut m = serde_json::Map::new();
    if let Some(v) = &fields.initial_prompt {
        m.insert("initialPrompt".to_string(), json!(v));
    }
    if let Some(v) = &fields.repo_path {
        m.insert("repoPath".to_string(), json!(v));
    }
    m.insert("repoType".to_string(), json!(fields.repo_type));
    if let Some(v) = &fields.github_url {
        m.insert("githubUrl".to_string(), json!(v));
    }
    if let Some(n) = fields.pr_number {
        m.insert("prNumber".to_string(), json!(n));
    }
    if let Some(v) = &fields.clone_path {
        m.insert("clonePath".to_string(), json!(v));
    }
    m.insert(
        "branch".to_string(),
        json!(fields.branch.as_deref().unwrap_or("main")),
    );
    m.insert("isNewRepo".to_string(), json!(fields.is_new_repo));
    if let Some(v) = &fields.scope {
        m.insert("scope".to_string(), json!(v));
    }
    if let Some(v) = &fields.specialist {
        m.insert("specialist".to_string(), json!(v));
    }
    Value::Object(m)
}

/// Resolve a GitHub URL to a locally-known repository path using workspace
/// rows the daemon already stores (mirrors the intent of the FE known-repos
/// lookup `lookupKnownRepoLocalPath`). Matching tiers:
/// 1. Strict: `repositoryOwner` + `repositoryName` both match.
/// 2. Name-only: ownerless rows whose `repositoryName` matches.
/// 3. Path-basename: ownerless rows whose folder name matches.
///
/// Returns `None` unless the winning tier yields a single distinct path.
async fn lookup_known_repo_local_path(
    api: &Arc<dyn WorkspaceApi>,
    github_url: &str,
) -> Option<String> {
    let (owner, repo) = parse_github_owner_repo(github_url)?;
    let workspaces = api.list_workspaces(true).await.ok()?;

    let mut strict = Vec::new();
    let mut name_only = Vec::new();
    let mut basename_only = Vec::new();
    for ws in &workspaces {
        // Skip deleted workspaces: their repositoryPath may no longer exist
        // on disk (the FE lookup consults user-visible checkouts only).
        if ws.status == WorkspaceStatus::Deleted {
            continue;
        }
        let Some(path) = ws.repository_path.as_deref().filter(|p| !p.is_empty()) else {
            continue;
        };
        if path.contains("/.clones/") || path.contains("\\.clones\\") {
            continue;
        }
        let entry_owner = ws
            .repository_owner
            .as_deref()
            .filter(|o| !o.is_empty())
            .map(str::to_lowercase);
        let entry_name = ws
            .repository_name
            .as_deref()
            .filter(|n| !n.is_empty())
            .map(|n| strip_git_suffix(&n.to_lowercase()).to_string());
        let entry_basename = path
            .rsplit(['/', '\\'])
            .next()
            .map(|b| strip_git_suffix(&b.to_lowercase()).to_string());

        if entry_name.as_deref() == Some(repo.as_str()) {
            if entry_owner.as_deref() == Some(owner.as_str()) {
                strict.push(path.to_string());
            } else if entry_owner.is_none() {
                name_only.push(path.to_string());
            }
        } else if entry_basename.as_deref() == Some(repo.as_str()) && entry_owner.is_none() {
            basename_only.push(path.to_string());
        }
    }

    strict.sort();
    strict.dedup();
    if strict.len() == 1 {
        return Some(strict.remove(0));
    }
    if !strict.is_empty() {
        return None;
    }
    name_only.sort();
    name_only.dedup();
    if name_only.len() == 1 {
        return Some(name_only.remove(0));
    }
    basename_only.sort();
    basename_only.dedup();
    if basename_only.len() == 1 {
        return Some(basename_only.remove(0));
    }
    None
}

async fn create(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    // Build workspace-create proposal from params
    let params = args.as_object().cloned().unwrap_or_default();

    // Extract title for preview
    let title = params
        .get("title")
        .and_then(Value::as_str)
        .map(|s| format!(": {s}"))
        .unwrap_or_default();

    let mut fields = normalize_workspace_create_fields(&params);

    // Hydrate repoPath/clonePath from repositories the daemon already knows
    // (parity with FE `hydrateWorkspaceCreateProposal`): when the caller only
    // sends a GitHub URL for a repo that already has local workspaces, fill
    // in the local checkout so the user can apply without picking a folder.
    if fields.repo_path.is_none() {
        if let Some(url) = fields.github_url.clone() {
            if let Some(path) = lookup_known_repo_local_path(api, &url).await {
                fields.repo_path = Some(path.clone());
                if fields.clone_path.is_none() {
                    fields.clone_path = Some(path);
                }
            }
        }
    }

    // Best-effort branch defaulting + validation against the local checkout
    // (monorepo#761). Only runs when the proposal references a local repo
    // path that exists on disk; any repo-open/IO failure degrades silently
    // (empty branch falls back to `main`, no warning) — propose never fails
    // because of validation. Apply-time worktree provisioning stays the
    // enforcement point.
    let mut warnings: Vec<String> = Vec::new();
    // The default branch actually resolved from the local repo (not the
    // static `main` fallback); written back into the payload below so
    // applying the proposal provisions from the same branch the preview
    // shows instead of whatever HEAD happens to be.
    let mut resolved_default_branch: Option<String> = None;
    let local_repo = fields
        .repo_path
        .clone()
        .filter(|p| std::path::Path::new(p).exists());
    match (fields.branch.clone(), local_repo) {
        // Explicit branch + local repo: apply-time canonicalises the wire
        // `baseRef` (allowlist remote-prefix strip) before `provision_worktree`
        // resolves it with the 3-spec order against `input.remote` (default
        // `origin`) — mirror both steps here so propose-time and apply-time
        // agree on what "resolvable" means.
        (Some(branch), Some(repo_path)) => {
            let canonical = intent_git::refs::canonicalise_base_ref(&branch);
            let remote = string_value(&params, "remote").unwrap_or_else(|| "origin".to_string());
            let p = repo_path.clone();
            let resolved = tokio::task::spawn_blocking(move || {
                intent_git::worktree::base_ref_resolves(
                    std::path::Path::new(&p),
                    &canonical,
                    &remote,
                )
            })
            .await;
            if matches!(resolved, Ok(Ok(false))) {
                warnings.push(format!(
                    "Base branch '{branch}' does not exist in {repo_path}; \
                     the default branch will be preselected in the dialog"
                ));
            }
        }
        // Empty branch + local repo: default to the repo's actual default
        // branch (origin/HEAD, else HEAD's branch), `main` as last resort.
        (None, Some(repo_path)) => {
            let default = tokio::task::spawn_blocking(move || {
                intent_git::branches::repo_default_branch(std::path::Path::new(&repo_path))
            })
            .await;
            fields.branch = Some(match default {
                Ok(Ok(name)) => {
                    resolved_default_branch = Some(name.clone());
                    name
                }
                _ => "main".to_string(),
            });
        }
        // No local repo to ask (GitHub-URL-only, missing path): static
        // default; the FE branch listing handles those flows.
        (None, None) => fields.branch = Some("main".to_string()),
        (Some(_), None) => {}
    }

    // Payload params: strip preview-only fields (TS
    // `workspaceCreatePayloadParams`) and write back the normalized repo
    // fields so applying the proposal uses the same sane values the preview
    // shows (e.g. an issues URL stripped down to the repo URL).
    let mut payload_params = params;
    payload_params.remove("title");
    payload_params.remove("statusMessage");
    if let Some(url) = &fields.github_url {
        payload_params.insert("githubUrl".to_string(), json!(url));
    }
    if let Some(n) = fields.pr_number {
        payload_params.insert("prNumber".to_string(), json!(n));
    }
    if let Some(path) = &fields.repo_path {
        payload_params.insert("repositoryPath".to_string(), json!(path));
    }
    if let Some(path) = &fields.clone_path {
        payload_params.insert("clonePath".to_string(), json!(path));
    }
    // Write the repo-resolved default branch back as `baseRef` (the wire key
    // the FE apply path also maps the dialog's branch into) so a direct
    // payload apply provisions from the branch the preview shows, not from
    // whatever HEAD the checkout happens to be on. The static `main` fallback
    // is intentionally NOT written back: it is a display guess, and forcing it
    // into the payload could turn a working HEAD-based apply into a failure
    // when no `main` exists.
    if let Some(branch) = &resolved_default_branch {
        payload_params.insert("baseRef".to_string(), json!(branch));
    }

    // `warnings` is the optional `preview.warnings?: string[]` of the TS
    // reference (`shared/types/proposal.ts`); omitted when empty, matching
    // JSON serialization dropping `undefined`.
    let mut preview = serde_json::Map::new();
    preview.insert(
        "title".to_string(),
        json!(format!("Create workspace{}", title)),
    );
    preview.insert(
        "summary".to_string(),
        json!("Review and adjust workspace creation details before creating a new space."),
    );
    preview.insert(
        "workspaceCreate".to_string(),
        workspace_create_preview(&fields),
    );
    if !warnings.is_empty() {
        preview.insert("warnings".to_string(), json!(warnings));
    }

    let proposal = json!({
        "kind": "workspace-create",
        "payload": {
            "operation": "workspace.create",
            "params": payload_params
        },
        "preview": preview
    });

    proposal_result(&proposal)
}

async fn archive(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    let id = args
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| "id is required".to_string())?;

    assert_mutable_workspace_id(id)?;

    // Validate workspace exists
    let workspaces = api.list_workspaces(true).await.map_err(map_err)?;
    let workspace = workspaces
        .iter()
        .find(|w| w.id.as_str() == id)
        .ok_or_else(|| format!("Workspace not found: {id}"))?;

    // Build bulk-op proposal with single item
    let summary = workspace
        .repository_name
        .as_deref()
        .or(workspace.repository_path.as_deref())
        .map_or_else(|| format!("{:?}", workspace.status), String::from);

    let proposal = json!({
        "kind": "bulk-op",
        "payload": {
            "operation": "workspace.bulkArchive",
            "ids": [id]
        },
        "preview": {
            "title": "Archive 1 workspace",
            "summary": "Review the selected workspaces before archiving them.",
            "applyLabel": "Archive",
            "bulkItems": [{
                "id": id,
                "title": if workspace.title.is_empty() { id } else { &workspace.title },
                "summary": summary,
                "selected": true,
                "metadata": summarize_workspace(workspace)
            }]
        }
    });

    proposal_result(&proposal)
}

async fn delete(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    let id = args
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| "id is required".to_string())?;

    assert_mutable_workspace_id(id)?;

    // Validate workspace exists
    let workspaces = api.list_workspaces(true).await.map_err(map_err)?;
    let workspace = workspaces
        .iter()
        .find(|w| w.id.as_str() == id)
        .ok_or_else(|| format!("Workspace not found: {id}"))?;

    // Build bulk-op proposal with single item
    let summary = workspace
        .repository_name
        .as_deref()
        .or(workspace.repository_path.as_deref())
        .map_or_else(|| format!("{:?}", workspace.status), String::from);

    let proposal = json!({
        "kind": "bulk-op",
        "payload": {
            "operation": "workspace.bulkDelete",
            "ids": [id]
        },
        "preview": {
            "title": "Delete 1 workspace",
            "summary": "Review the selected workspaces before deleting them.",
            "applyLabel": "Delete",
            "bulkItems": [{
                "id": id,
                "title": if workspace.title.is_empty() { id } else { &workspace.title },
                "summary": summary,
                "selected": true,
                "metadata": summarize_workspace(workspace)
            }],
            "warnings": ["Deleting workspaces is destructive. Confirm the selected workspaces before applying."]
        }
    });

    proposal_result(&proposal)
}

async fn bulk_archive(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    let ids = args
        .get("ids")
        .and_then(Value::as_array)
        .ok_or_else(|| "ids must be a non-empty array".to_string())?;

    if ids.is_empty() {
        return Err("ids must be a non-empty array".to_string());
    }

    // Validate all IDs are mutable
    for id_val in ids {
        if let Some(id) = id_val.as_str() {
            assert_mutable_workspace_id(id)?;
        } else {
            return Err("all ids must be strings".to_string());
        }
    }

    // Validate all workspaces exist
    let workspaces = api.list_workspaces(true).await.map_err(map_err)?;
    let mut bulk_items = Vec::new();

    for id_val in ids {
        let id = id_val.as_str().unwrap(); // Already validated above
        let workspace = workspaces
            .iter()
            .find(|w| w.id.as_str() == id)
            .ok_or_else(|| format!("Workspace not found: {id}"))?;

        let summary = workspace
            .repository_name
            .as_deref()
            .or(workspace.repository_path.as_deref())
            .map_or_else(|| format!("{:?}", workspace.status), String::from);

        bulk_items.push(json!({
            "id": id,
            "title": if workspace.title.is_empty() { id } else { &workspace.title },
            "summary": summary,
            "selected": true,
            "metadata": summarize_workspace(workspace)
        }));
    }

    let count = ids.len();
    let proposal = json!({
        "kind": "bulk-op",
        "payload": {
            "operation": "workspace.bulkArchive",
            "ids": ids
        },
        "preview": {
            "title": format!("Archive {} workspace{}", count, if count == 1 { "" } else { "s" }),
            "summary": "Review the selected workspaces before archiving them.",
            "applyLabel": "Archive",
            "bulkItems": bulk_items
        }
    });

    proposal_result(&proposal)
}

async fn bulk_delete(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    let ids = args
        .get("ids")
        .and_then(Value::as_array)
        .ok_or_else(|| "ids must be a non-empty array".to_string())?;

    if ids.is_empty() {
        return Err("ids must be a non-empty array".to_string());
    }

    // Validate all IDs are mutable
    for id_val in ids {
        if let Some(id) = id_val.as_str() {
            assert_mutable_workspace_id(id)?;
        } else {
            return Err("all ids must be strings".to_string());
        }
    }

    // Validate all workspaces exist
    let workspaces = api.list_workspaces(true).await.map_err(map_err)?;
    let mut bulk_items = Vec::new();

    for id_val in ids {
        let id = id_val.as_str().unwrap(); // Already validated above
        let workspace = workspaces
            .iter()
            .find(|w| w.id.as_str() == id)
            .ok_or_else(|| format!("Workspace not found: {id}"))?;

        let summary = workspace
            .repository_name
            .as_deref()
            .or(workspace.repository_path.as_deref())
            .map_or_else(|| format!("{:?}", workspace.status), String::from);

        bulk_items.push(json!({
            "id": id,
            "title": if workspace.title.is_empty() { id } else { &workspace.title },
            "summary": summary,
            "selected": true,
            "metadata": summarize_workspace(workspace)
        }));
    }

    let count = ids.len();
    let proposal = json!({
        "kind": "bulk-op",
        "payload": {
            "operation": "workspace.bulkDelete",
            "ids": ids
        },
        "preview": {
            "title": format!("Delete {} workspace{}", count, if count == 1 { "" } else { "s" }),
            "summary": "Review the selected workspaces before deleting them.",
            "applyLabel": "Delete",
            "bulkItems": bulk_items,
            "warnings": ["Deleting workspaces is destructive. Confirm the selected workspaces before applying."]
        }
    });

    proposal_result(&proposal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{
        BoxFuture, Error, Result, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus,
    };
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct FakeApi {
        workspaces: Arc<Mutex<Vec<Workspace>>>,
        events: Arc<Mutex<Vec<PublishEvent>>>,
    }

    impl FakeApi {
        fn published_events(&self) -> Vec<PublishEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    impl WorkspaceApi for FakeApi {
        fn list_workspaces(
            &self,
            _include_archived: bool,
        ) -> BoxFuture<'_, Result<Vec<Workspace>>> {
            let workspaces = self.workspaces.lock().unwrap().clone();
            Box::pin(async move { Ok(workspaces) })
        }

        fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
            let workspaces = self.workspaces.lock().unwrap().clone();
            Box::pin(async move {
                workspaces
                    .into_iter()
                    .find(|w| w.id == id)
                    .ok_or_else(|| Error::NotFound(format!("Workspace not found: {}", id.as_str())))
            })
        }

        fn publish_event(&self, event: PublishEvent) -> BoxFuture<'_, Result<()>> {
            let events = self.events.clone();
            Box::pin(async move {
                events.lock().unwrap().push(event);
                Ok(())
            })
        }
    }

    fn make_workspace(id: &str, title: &str) -> Workspace {
        Workspace {
            id: WorkspaceId::from_string(id),
            title: title.to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: Some("/repo".to_string()),
            repository_owner: Some("owner".to_string()),
            repository_name: Some("repo".to_string()),
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            context_links: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
            display_status: None,
            waiting: false,
            checkout_mode: None,
            execution_environment: None,
            disk_usage: None,
            pending_delete_at: None,
        }
    }

    #[tokio::test]
    async fn test_dispatch_rejects_non_chief_workspace() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let non_chief_id = WorkspaceId::from_string("amber-forest");
        let result = dispatch(&api, &non_chief_id, "list", &json!({})).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "ws.app.* is only available in the Chief of Staff workspace"
        );
    }

    #[tokio::test]
    async fn test_list_excludes_chief_workspace() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("__chief__", "Chief of Staff"));
            workspaces.push(make_workspace("ws-1", "Workspace 1"));
            workspaces.push(make_workspace("ws-2", "Workspace 2"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "list", &json!({})).await.unwrap();
        let workspaces = result.as_array().unwrap();

        // __chief__ should not appear in results
        assert_eq!(workspaces.len(), 2);
        assert!(workspaces
            .iter()
            .all(|w| w.get("id").unwrap().as_str().unwrap() != "__chief__"));
    }

    #[tokio::test]
    async fn test_get_missing_workspace_returns_error() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "get", &json!({ "id": "missing-ws" })).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Workspace not found: missing-ws"));
    }

    #[tokio::test]
    async fn test_get_chief_workspace_returns_error() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("__chief__", "Chief of Staff"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "get", &json!({ "id": "__chief__" })).await;
        // Even if chief exists in the list, get should reject it
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Workspace not found: __chief__");
    }

    #[tokio::test]
    async fn test_list_returns_expected_shape() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Test Workspace"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "list", &json!({})).await.unwrap();
        let workspaces = result.as_array().unwrap();
        assert_eq!(workspaces.len(), 1);

        let ws = &workspaces[0];
        // Check expected fields are present
        assert!(ws.get("id").is_some());
        assert!(ws.get("title").is_some());
        assert!(ws.get("status").is_some());
        assert!(ws.get("branch").is_some());
        assert!(ws.get("tags").is_some());
        assert!(ws.get("createdAt").is_some());
        assert!(ws.get("updatedAt").is_some());
    }

    #[tokio::test]
    async fn test_list_filter_by_status() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            let mut ws_active = make_workspace("ws-1", "Active");
            ws_active.status = WorkspaceStatus::Active;
            workspaces.push(ws_active);

            let mut ws_archived = make_workspace("ws-2", "Archived");
            ws_archived.status = WorkspaceStatus::Archived;
            workspaces.push(ws_archived);
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "list",
            &json!({ "filter": { "status": ["active"] } }),
        )
        .await
        .unwrap();
        let workspaces = result.as_array().unwrap();
        assert_eq!(workspaces.len(), 1);
        assert_eq!(workspaces[0].get("id").unwrap().as_str().unwrap(), "ws-1");
    }

    #[tokio::test]
    async fn test_list_sort_by_title() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Zebra"));
            workspaces.push(make_workspace("ws-2", "Apple"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "list",
            &json!({ "sort": { "by": "title", "order": "asc" } }),
        )
        .await
        .unwrap();
        let workspaces = result.as_array().unwrap();
        assert_eq!(workspaces.len(), 2);
        assert_eq!(
            workspaces[0].get("title").unwrap().as_str().unwrap(),
            "Apple"
        );
        assert_eq!(
            workspaces[1].get("title").unwrap().as_str().unwrap(),
            "Zebra"
        );
    }

    #[tokio::test]
    async fn test_get_returns_expected_shape() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Test Workspace"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "get", &json!({ "id": "ws-1" }))
            .await
            .unwrap();

        // Check expected fields are present
        assert_eq!(result.get("id").unwrap().as_str().unwrap(), "ws-1");
        assert_eq!(
            result.get("title").unwrap().as_str().unwrap(),
            "Test Workspace"
        );
        assert!(result.get("status").is_some());
        assert!(result.get("branch").is_some());
        assert!(result.get("tags").is_some());
        assert!(result.get("createdAt").is_some());
        assert!(result.get("updatedAt").is_some());
    }

    // Proposal methods tests

    #[tokio::test]
    async fn test_create_returns_proposal() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "create", &json!({ "title": "New WS" }))
            .await
            .unwrap();

        // Should have proposal and content items
        assert!(result.get("ok").unwrap().as_bool().unwrap());
        let proposal = result.get("proposal").unwrap();
        assert_eq!(
            proposal.get("kind").unwrap().as_str().unwrap(),
            "workspace-create"
        );
        assert!(proposal.get("preview").is_some());
        assert!(proposal.get("payload").is_some());

        let items = result.get("__mcpContentItems").unwrap().as_array().unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].get("type").unwrap().as_str().unwrap(), "text");
        assert_eq!(items[1].get("type").unwrap().as_str().unwrap(), "resource");
    }

    /// Helper: run `create` against a fresh `FakeApi` and return the proposal.
    async fn create_proposal(args: serde_json::Value) -> Value {
        create_proposal_with(Arc::new(FakeApi::default()), args).await
    }

    async fn create_proposal_with(fake: Arc<FakeApi>, args: serde_json::Value) -> Value {
        let api: Arc<dyn WorkspaceApi> = fake;
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "create", &args).await.unwrap();
        result.get("proposal").unwrap().clone()
    }

    fn preview_fields(proposal: &Value) -> &Value {
        proposal
            .get("preview")
            .unwrap()
            .get("workspaceCreate")
            .unwrap()
    }

    fn payload_params(proposal: &Value) -> &Value {
        proposal.get("payload").unwrap().get("params").unwrap()
    }

    fn preview_warnings(proposal: &Value) -> Option<&Value> {
        proposal.get("preview").unwrap().get("warnings")
    }

    /// Temp git repo fixture for the propose-time base-ref validation tests;
    /// removed on drop.
    struct TempRepo {
        path: std::path::PathBuf,
    }

    impl TempRepo {
        fn unique_dir(tag: &str) -> std::path::PathBuf {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!("intent-acp-ws-{tag}-{nanos}"));
            std::fs::create_dir_all(&path).unwrap();
            path
        }

        /// Init a repo whose initial (HEAD) branch is `head_branch`, with one
        /// commit so refs resolve.
        fn init(tag: &str, head_branch: &str) -> Self {
            let path = Self::unique_dir(tag);
            let mut opts = git2::RepositoryInitOptions::new();
            opts.initial_head(head_branch);
            let repo = git2::Repository::init_opts(&path, &opts).unwrap();
            let mut cfg = repo.config().unwrap();
            cfg.set_str("user.name", "Test").unwrap();
            cfg.set_str("user.email", "test@example.com").unwrap();
            std::fs::write(path.join("a.txt"), "x\n").unwrap();
            let mut index = repo.index().unwrap();
            index.add_path(std::path::Path::new("a.txt")).unwrap();
            index.write().unwrap();
            let tree_oid = index.write_tree().unwrap();
            let tree = repo.find_tree(tree_oid).unwrap();
            let sig = git2::Signature::now("Test", "test@example.com").unwrap();
            repo.commit(Some("HEAD"), &sig, &sig, "init", &tree, &[])
                .unwrap();
            Self { path }
        }

        /// A plain directory that exists on disk but is not a git repository
        /// (repo-open fails → validation must degrade silently).
        fn plain_dir(tag: &str) -> Self {
            Self {
                path: Self::unique_dir(tag),
            }
        }

        fn path_str(&self) -> String {
            self.path.to_string_lossy().to_string()
        }
    }

    impl Drop for TempRepo {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[tokio::test]
    async fn test_create_normalizes_issues_url_to_repo_url() {
        let proposal = create_proposal(json!({
            "title": "Fix bug",
            "githubUrl": "https://github.com/o/r/issues/465"
        }))
        .await;

        let fields = preview_fields(&proposal);
        assert_eq!(
            fields.get("githubUrl").unwrap().as_str().unwrap(),
            "https://github.com/o/r"
        );
        assert!(fields.get("prNumber").is_none());
        assert_eq!(fields.get("repoType").unwrap().as_str().unwrap(), "github");

        // Payload params are normalized the same way; title is stripped.
        let params = payload_params(&proposal);
        assert_eq!(
            params.get("githubUrl").unwrap().as_str().unwrap(),
            "https://github.com/o/r"
        );
        assert!(params.get("title").is_none());
    }

    #[tokio::test]
    async fn test_create_normalizes_pull_url_and_sets_pr_number() {
        let proposal = create_proposal(json!({
            "githubUrl": "https://github.com/o/r/pull/123"
        }))
        .await;

        let fields = preview_fields(&proposal);
        assert_eq!(
            fields.get("githubUrl").unwrap().as_str().unwrap(),
            "https://github.com/o/r"
        );
        assert_eq!(fields.get("prNumber").unwrap().as_u64().unwrap(), 123);

        let params = payload_params(&proposal);
        assert_eq!(
            params.get("githubUrl").unwrap().as_str().unwrap(),
            "https://github.com/o/r"
        );
        assert_eq!(params.get("prNumber").unwrap().as_u64().unwrap(), 123);
    }

    #[tokio::test]
    async fn test_create_parses_pr_url_param() {
        let proposal = create_proposal(json!({
            "prUrl": "https://github.com/o/r/pull/9"
        }))
        .await;

        let fields = preview_fields(&proposal);
        assert_eq!(
            fields.get("githubUrl").unwrap().as_str().unwrap(),
            "https://github.com/o/r"
        );
        assert_eq!(fields.get("prNumber").unwrap().as_u64().unwrap(), 9);
    }

    #[tokio::test]
    async fn test_create_owner_repo_shorthand_becomes_https_url() {
        let proposal = create_proposal(json!({ "repository": "octo/repo" })).await;

        let fields = preview_fields(&proposal);
        assert_eq!(
            fields.get("githubUrl").unwrap().as_str().unwrap(),
            "https://github.com/octo/repo"
        );
        assert_eq!(fields.get("repoType").unwrap().as_str().unwrap(), "github");
        assert_eq!(
            payload_params(&proposal)
                .get("githubUrl")
                .unwrap()
                .as_str()
                .unwrap(),
            "https://github.com/octo/repo"
        );
    }

    #[tokio::test]
    async fn test_create_git_ssh_repository_becomes_https_url() {
        let proposal =
            create_proposal(json!({ "repository": "git@github.com:octo/repo.git" })).await;

        let fields = preview_fields(&proposal);
        assert_eq!(
            fields.get("githubUrl").unwrap().as_str().unwrap(),
            "https://github.com/octo/repo"
        );
    }

    #[tokio::test]
    async fn test_create_owner_name_params_derive_github_url() {
        let proposal = create_proposal(json!({
            "repositoryOwner": "octo",
            "repositoryName": "repo.git"
        }))
        .await;

        let fields = preview_fields(&proposal);
        assert_eq!(
            fields.get("githubUrl").unwrap().as_str().unwrap(),
            "https://github.com/octo/repo"
        );
    }

    #[tokio::test]
    async fn test_create_local_repository_path() {
        let proposal = create_proposal(json!({ "repository": "/Users/me/code/thing" })).await;

        let fields = preview_fields(&proposal);
        assert_eq!(
            fields.get("repoPath").unwrap().as_str().unwrap(),
            "/Users/me/code/thing"
        );
        assert_eq!(fields.get("repoType").unwrap().as_str().unwrap(), "local");
        assert!(fields.get("githubUrl").is_none());
        assert!(fields.get("clonePath").is_none());
    }

    #[tokio::test]
    async fn test_create_remote_repo_type_from_environment_config() {
        let proposal = create_proposal(json!({ "environmentConfig": { "image": "ubuntu" } })).await;
        let fields = preview_fields(&proposal);
        assert_eq!(fields.get("repoType").unwrap().as_str().unwrap(), "remote");
    }

    #[tokio::test]
    async fn test_create_initial_prompt_and_specialist_from_initial_agent() {
        let proposal = create_proposal(json!({
            "initialMessage": "fallback message",
            "initialAgent": { "prompt": "agent prompt", "specialist": "implementor" }
        }))
        .await;

        let fields = preview_fields(&proposal);
        assert_eq!(
            fields.get("initialPrompt").unwrap().as_str().unwrap(),
            "agent prompt"
        );
        assert_eq!(
            fields.get("specialist").unwrap().as_str().unwrap(),
            "implementor"
        );
    }

    #[tokio::test]
    async fn test_create_initial_prompt_falls_back_to_initial_message() {
        let proposal = create_proposal(json!({ "initialMessage": "do the thing" })).await;
        let fields = preview_fields(&proposal);
        assert_eq!(
            fields.get("initialPrompt").unwrap().as_str().unwrap(),
            "do the thing"
        );
    }

    #[tokio::test]
    async fn test_create_branch_defaults_and_base_ref_fallback() {
        // No repo path at all → nothing to ask, static `main` fallback.
        let proposal = create_proposal(json!({})).await;
        let fields = preview_fields(&proposal);
        assert_eq!(fields.get("branch").unwrap().as_str().unwrap(), "main");
        assert!(!fields.get("isNewRepo").unwrap().as_bool().unwrap());

        let proposal = create_proposal(json!({ "baseRef": "develop" })).await;
        let fields = preview_fields(&proposal);
        assert_eq!(fields.get("branch").unwrap().as_str().unwrap(), "develop");
    }

    #[tokio::test]
    async fn test_create_unresolvable_branch_warns_but_returns_proposal() {
        let repo = TempRepo::init("badref", "trunk");
        let proposal = create_proposal(json!({
            "repositoryPath": repo.path_str(),
            "branch": "no-such-branch"
        }))
        .await;

        // Warn-don't-reject: the proposal is returned with the proposed
        // value intact in preview and payload.
        let fields = preview_fields(&proposal);
        assert_eq!(
            fields.get("branch").unwrap().as_str().unwrap(),
            "no-such-branch"
        );
        assert_eq!(
            payload_params(&proposal)
                .get("branch")
                .unwrap()
                .as_str()
                .unwrap(),
            "no-such-branch"
        );

        let warnings = preview_warnings(&proposal).unwrap().as_array().unwrap();
        assert_eq!(warnings.len(), 1);
        let warning = warnings[0].as_str().unwrap();
        assert!(warning.contains("Base branch 'no-such-branch' does not exist"));
        assert!(warning.contains(&repo.path_str()));
        assert!(warning.contains("default branch will be preselected"));
    }

    #[tokio::test]
    async fn test_create_resolvable_branch_no_warning() {
        let repo = TempRepo::init("goodref", "trunk");
        let proposal = create_proposal(json!({
            "repositoryPath": repo.path_str(),
            "branch": "trunk"
        }))
        .await;

        let fields = preview_fields(&proposal);
        assert_eq!(fields.get("branch").unwrap().as_str().unwrap(), "trunk");
        assert!(preview_warnings(&proposal).is_none());
    }

    #[tokio::test]
    async fn test_create_empty_branch_defaults_to_repo_default_branch() {
        let repo = TempRepo::init("repodefault", "trunk");
        let proposal = create_proposal(json!({ "repositoryPath": repo.path_str() })).await;

        // Empty branch → the repo's actual default branch, not hardcoded
        // `main`; defaulting is not a warning. The resolved default is also
        // written back into the payload as `baseRef` so a direct payload
        // apply provisions from the branch the preview shows, not from
        // whatever HEAD happens to be.
        let fields = preview_fields(&proposal);
        assert_eq!(fields.get("branch").unwrap().as_str().unwrap(), "trunk");
        assert!(preview_warnings(&proposal).is_none());
        assert_eq!(
            payload_params(&proposal)
                .get("baseRef")
                .unwrap()
                .as_str()
                .unwrap(),
            "trunk"
        );
    }

    #[tokio::test]
    async fn test_create_branch_validation_canonicalises_like_apply_time() {
        // Apply-time strips allowlisted remote prefixes (`origin/` etc.) via
        // canonicalise_base_ref before resolving; propose-time must probe the
        // same canonical value so `origin/trunk` never warns when `trunk`
        // resolves.
        let repo = TempRepo::init("canon", "trunk");
        let proposal = create_proposal(json!({
            "repositoryPath": repo.path_str(),
            "branch": "origin/trunk"
        }))
        .await;
        assert!(preview_warnings(&proposal).is_none());

        // Non-allowlisted first segments are not stripped: still a warning.
        let proposal = create_proposal(json!({
            "repositoryPath": repo.path_str(),
            "branch": "feature/trunk"
        }))
        .await;
        assert!(preview_warnings(&proposal).is_some());
    }

    #[tokio::test]
    async fn test_create_validation_degrades_silently_on_io_failure() {
        // Path missing on disk: no validation runs — no warning, no error.
        let proposal = create_proposal(json!({
            "repositoryPath": "/no/such/checkout",
            "branch": "anything-goes"
        }))
        .await;
        let fields = preview_fields(&proposal);
        assert_eq!(
            fields.get("branch").unwrap().as_str().unwrap(),
            "anything-goes"
        );
        assert!(preview_warnings(&proposal).is_none());

        // Existing dir that is not a git repo: repo open fails → degrade.
        let plain = TempRepo::plain_dir("plain");
        let proposal = create_proposal(json!({
            "repositoryPath": plain.path_str(),
            "branch": "anything-goes"
        }))
        .await;
        assert!(preview_warnings(&proposal).is_none());

        // Same dir with an empty branch: default-branch lookup fails →
        // static `main` fallback, which stays a display guess: it is NOT
        // written back into the payload as `baseRef`.
        let proposal = create_proposal(json!({ "repositoryPath": plain.path_str() })).await;
        let fields = preview_fields(&proposal);
        assert_eq!(fields.get("branch").unwrap().as_str().unwrap(), "main");
        assert!(preview_warnings(&proposal).is_none());
        assert!(payload_params(&proposal).get("baseRef").is_none());
    }

    #[tokio::test]
    async fn test_create_clone_path_falls_back_to_repo_path() {
        let proposal = create_proposal(json!({
            "githubUrl": "https://github.com/o/r",
            "repositoryPath": "/Users/me/code/r"
        }))
        .await;

        let fields = preview_fields(&proposal);
        assert_eq!(
            fields.get("clonePath").unwrap().as_str().unwrap(),
            "/Users/me/code/r"
        );
        assert_eq!(
            payload_params(&proposal)
                .get("clonePath")
                .unwrap()
                .as_str()
                .unwrap(),
            "/Users/me/code/r"
        );
    }

    #[tokio::test]
    async fn test_create_hydrates_repo_path_from_known_workspace() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            // make_workspace defaults: owner "owner", name "repo", path "/repo"
            workspaces.push(make_workspace("ws-1", "Existing"));
        }

        let proposal = create_proposal_with(
            fake,
            json!({ "githubUrl": "https://github.com/Owner/Repo" }),
        )
        .await;

        let fields = preview_fields(&proposal);
        assert_eq!(fields.get("repoPath").unwrap().as_str().unwrap(), "/repo");
        assert_eq!(fields.get("clonePath").unwrap().as_str().unwrap(), "/repo");

        let params = payload_params(&proposal);
        assert_eq!(
            params.get("repositoryPath").unwrap().as_str().unwrap(),
            "/repo"
        );
        assert_eq!(params.get("clonePath").unwrap().as_str().unwrap(), "/repo");
    }

    #[tokio::test]
    async fn test_create_hydration_ambiguous_match_leaves_paths_unset() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            let ws1 = make_workspace("ws-1", "One");
            let mut ws2 = make_workspace("ws-2", "Two");
            ws2.repository_path = Some("/other/repo".to_string());
            workspaces.push(ws1);
            workspaces.push(ws2);
        }

        let proposal = create_proposal_with(
            fake,
            json!({ "githubUrl": "https://github.com/owner/repo" }),
        )
        .await;

        let fields = preview_fields(&proposal);
        assert!(fields.get("repoPath").is_none());
        assert!(fields.get("clonePath").is_none());
    }

    #[tokio::test]
    async fn test_create_hydration_no_match_leaves_paths_unset() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Existing"));
        }

        let proposal = create_proposal_with(
            fake,
            json!({ "githubUrl": "https://github.com/someone/else" }),
        )
        .await;

        let fields = preview_fields(&proposal);
        assert!(fields.get("repoPath").is_none());
        assert!(fields.get("clonePath").is_none());
    }

    #[test]
    fn test_parse_github_pr_or_issue_url_shapes() {
        assert_eq!(
            parse_github_pr_or_issue_url("https://github.com/o/r/pull/12"),
            Some(("https://github.com/o/r".to_string(), Some(12)))
        );
        assert_eq!(
            parse_github_pr_or_issue_url("https://github.com/o/r/issues/465"),
            Some(("https://github.com/o/r".to_string(), None))
        );
        // Repo URL without a pull/issues segment is not parsed
        assert_eq!(parse_github_pr_or_issue_url("https://github.com/o/r"), None);
        // Non-https and non-github hosts are rejected
        assert_eq!(
            parse_github_pr_or_issue_url("http://github.com/o/r/pull/12"),
            None
        );
        assert_eq!(
            parse_github_pr_or_issue_url("https://gitlab.com/o/r/pull/12"),
            None
        );
        // Non-numeric or zero numbers are rejected
        assert_eq!(
            parse_github_pr_or_issue_url("https://github.com/o/r/pull/abc"),
            None
        );
        assert_eq!(
            parse_github_pr_or_issue_url("https://github.com/o/r/issues/0"),
            None
        );
    }

    #[test]
    fn test_repository_to_github_url_shapes() {
        assert_eq!(
            repository_to_github_url("octo/repo"),
            Some("https://github.com/octo/repo".to_string())
        );
        assert_eq!(
            repository_to_github_url("https://github.com/octo/repo.git"),
            Some("https://github.com/octo/repo".to_string())
        );
        assert_eq!(
            repository_to_github_url("git@github.com:octo/repo.git"),
            Some("https://github.com/octo/repo".to_string())
        );
        assert_eq!(repository_to_github_url("/local/path"), None);
        assert_eq!(repository_to_github_url("just-a-name"), None);
    }

    #[tokio::test]
    async fn test_create_non_ascii_github_url_does_not_panic() {
        // Regression: strip_prefix_ci used to slice at a byte offset that
        // could split a multi-byte char ("aaaaaaa€rest"[..8] panics).
        let proposal = create_proposal(json!({ "githubUrl": "aaaaaaa\u{20AC}rest" })).await;
        let fields = preview_fields(&proposal);
        // Falls through as an opaque githubUrl string, no panic.
        assert_eq!(
            fields.get("githubUrl").unwrap().as_str().unwrap(),
            "aaaaaaa\u{20AC}rest"
        );
    }

    #[tokio::test]
    async fn test_create_non_ascii_repository_does_not_panic() {
        // Regression: strip_git_suffix used to slice s[s.len()-4..] which
        // panics on "€€" (4 bytes, boundary at 2).
        let proposal = create_proposal(json!({ "repository": "\u{20AC}\u{20AC}" })).await;
        let fields = preview_fields(&proposal);
        assert!(fields.get("githubUrl").is_none());
    }

    #[tokio::test]
    async fn test_create_www_github_pull_url_normalized() {
        let proposal = create_proposal(json!({
            "githubUrl": "https://www.github.com/o/r/pull/7"
        }))
        .await;
        let fields = preview_fields(&proposal);
        assert_eq!(
            fields.get("githubUrl").unwrap().as_str().unwrap(),
            "https://github.com/o/r"
        );
        assert_eq!(fields.get("prNumber").unwrap().as_u64().unwrap(), 7);
    }

    #[tokio::test]
    async fn test_create_caller_pr_number_overridden_by_derived() {
        // Payload must agree with the preview: a caller-supplied prNumber is
        // replaced by the URL-derived one.
        let proposal = create_proposal(json!({
            "githubUrl": "https://github.com/o/r/pull/123",
            "prNumber": 999
        }))
        .await;
        assert_eq!(
            preview_fields(&proposal)
                .get("prNumber")
                .unwrap()
                .as_u64()
                .unwrap(),
            123
        );
        assert_eq!(
            payload_params(&proposal)
                .get("prNumber")
                .unwrap()
                .as_u64()
                .unwrap(),
            123
        );
    }

    #[tokio::test]
    async fn test_create_foreign_pr_url_number_not_mixed_with_other_repo() {
        // githubUrl resolves to repo A; prUrl points at repo B. The PR
        // number must not be attached to repo A.
        let proposal = create_proposal(json!({
            "githubUrl": "https://github.com/a/a/issues/1",
            "prUrl": "https://github.com/b/b/pull/2"
        }))
        .await;
        let fields = preview_fields(&proposal);
        assert_eq!(
            fields.get("githubUrl").unwrap().as_str().unwrap(),
            "https://github.com/a/a"
        );
        assert!(fields.get("prNumber").is_none());
    }

    #[tokio::test]
    async fn test_create_environment_config_non_object_stays_local() {
        let proposal = create_proposal(json!({ "environmentConfig": false })).await;
        let fields = preview_fields(&proposal);
        assert_eq!(fields.get("repoType").unwrap().as_str().unwrap(), "local");
    }

    #[tokio::test]
    async fn test_create_shorthand_repository_coexists_with_derived_github_url() {
        // Payload keeps the original `repository` param alongside the
        // derived githubUrl.
        let proposal = create_proposal(json!({ "repository": "octo/repo" })).await;
        let params = payload_params(&proposal);
        assert_eq!(
            params.get("repository").unwrap().as_str().unwrap(),
            "octo/repo"
        );
        assert_eq!(
            params.get("githubUrl").unwrap().as_str().unwrap(),
            "https://github.com/octo/repo"
        );
    }

    #[tokio::test]
    async fn test_create_hydration_skips_deleted_workspaces() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            let mut ws = make_workspace("ws-1", "Deleted");
            ws.status = WorkspaceStatus::Deleted;
            workspaces.push(ws);
        }

        let proposal = create_proposal_with(
            fake,
            json!({ "githubUrl": "https://github.com/owner/repo" }),
        )
        .await;

        let fields = preview_fields(&proposal);
        assert!(fields.get("repoPath").is_none());
        assert!(fields.get("clonePath").is_none());
    }

    #[tokio::test]
    async fn test_archive_rejects_chief_workspace() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "archive", &json!({ "id": "__chief__" })).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "The Chief virtual workspace cannot be modified"
        );
    }

    #[tokio::test]
    async fn test_archive_validates_workspace_exists() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "archive", &json!({ "id": "missing-ws" })).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Workspace not found: missing-ws"));
    }

    #[tokio::test]
    async fn test_archive_returns_proposal() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Test Workspace"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "archive", &json!({ "id": "ws-1" }))
            .await
            .unwrap();

        // Should have proposal and content items
        assert!(result.get("ok").unwrap().as_bool().unwrap());
        let proposal = result.get("proposal").unwrap();
        assert_eq!(proposal.get("kind").unwrap().as_str().unwrap(), "bulk-op");

        let payload = proposal.get("payload").unwrap();
        assert_eq!(
            payload.get("operation").unwrap().as_str().unwrap(),
            "workspace.bulkArchive"
        );
        assert_eq!(payload.get("ids").unwrap().as_array().unwrap().len(), 1);

        let preview = proposal.get("preview").unwrap();
        assert_eq!(
            preview.get("title").unwrap().as_str().unwrap(),
            "Archive 1 workspace"
        );
    }

    #[tokio::test]
    async fn test_delete_rejects_chief_workspace() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "delete", &json!({ "id": "__chief__" })).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "The Chief virtual workspace cannot be modified"
        );
    }

    #[tokio::test]
    async fn test_delete_validates_workspace_exists() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "delete", &json!({ "id": "missing-ws" })).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Workspace not found: missing-ws"));
    }

    #[tokio::test]
    async fn test_delete_returns_proposal_with_warnings() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Test Workspace"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "delete", &json!({ "id": "ws-1" }))
            .await
            .unwrap();

        let proposal = result.get("proposal").unwrap();
        assert_eq!(proposal.get("kind").unwrap().as_str().unwrap(), "bulk-op");

        let payload = proposal.get("payload").unwrap();
        assert_eq!(
            payload.get("operation").unwrap().as_str().unwrap(),
            "workspace.bulkDelete"
        );

        let preview = proposal.get("preview").unwrap();
        assert_eq!(
            preview.get("title").unwrap().as_str().unwrap(),
            "Delete 1 workspace"
        );
        assert!(preview.get("warnings").is_some());
    }

    #[tokio::test]
    async fn test_bulk_archive_rejects_empty_array() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "bulkArchive", &json!({ "ids": [] })).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "ids must be a non-empty array");
    }

    #[tokio::test]
    async fn test_bulk_archive_rejects_chief_in_list() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "bulkArchive",
            &json!({ "ids": ["ws-1", "__chief__"] }),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "The Chief virtual workspace cannot be modified"
        );
    }

    #[tokio::test]
    async fn test_bulk_archive_validates_all_exist() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "WS 1"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "bulkArchive",
            &json!({ "ids": ["ws-1", "ws-2"] }),
        )
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Workspace not found: ws-2"));
    }

    #[tokio::test]
    async fn test_bulk_archive_returns_proposal() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "WS 1"));
            workspaces.push(make_workspace("ws-2", "WS 2"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "bulkArchive",
            &json!({ "ids": ["ws-1", "ws-2"] }),
        )
        .await
        .unwrap();

        let proposal = result.get("proposal").unwrap();
        assert_eq!(proposal.get("kind").unwrap().as_str().unwrap(), "bulk-op");

        let payload = proposal.get("payload").unwrap();
        assert_eq!(
            payload.get("operation").unwrap().as_str().unwrap(),
            "workspace.bulkArchive"
        );
        assert_eq!(payload.get("ids").unwrap().as_array().unwrap().len(), 2);

        let preview = proposal.get("preview").unwrap();
        assert_eq!(
            preview.get("title").unwrap().as_str().unwrap(),
            "Archive 2 workspaces"
        );

        let bulk_items = preview.get("bulkItems").unwrap().as_array().unwrap();
        assert_eq!(bulk_items.len(), 2);
    }

    #[tokio::test]
    async fn test_bulk_delete_rejects_empty_array() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "bulkDelete", &json!({ "ids": [] })).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "ids must be a non-empty array");
    }

    #[tokio::test]
    async fn test_bulk_delete_returns_proposal_with_warnings() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "WS 1"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "bulkDelete", &json!({ "ids": ["ws-1"] }))
            .await
            .unwrap();

        let proposal = result.get("proposal").unwrap();
        assert_eq!(proposal.get("kind").unwrap().as_str().unwrap(), "bulk-op");

        let payload = proposal.get("payload").unwrap();
        assert_eq!(
            payload.get("operation").unwrap().as_str().unwrap(),
            "workspace.bulkDelete"
        );

        let preview = proposal.get("preview").unwrap();
        assert!(preview.get("warnings").is_some());
    }

    #[tokio::test]
    async fn test_proposal_has_mcp_content_items() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Test"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "archive", &json!({ "id": "ws-1" }))
            .await
            .unwrap();

        let items = result.get("__mcpContentItems").unwrap().as_array().unwrap();
        assert_eq!(items.len(), 2);

        // Text item
        assert_eq!(items[0].get("type").unwrap().as_str().unwrap(), "text");
        let text = items[0].get("text").unwrap().as_str().unwrap();
        assert!(text.contains("\"ok\": true"));

        // Resource item
        assert_eq!(items[1].get("type").unwrap().as_str().unwrap(), "resource");
        let resource = items[1].get("resource").unwrap();
        assert_eq!(
            resource.get("mimeType").unwrap().as_str().unwrap(),
            "application/vnd.intent.proposal+json"
        );
        assert!(resource
            .get("uri")
            .unwrap()
            .as_str()
            .unwrap()
            .starts_with("intent-proposal://"));
    }

    #[tokio::test]
    async fn test_open_rejects_chief_workspace() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "open", &json!({ "id": "__chief__" })).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "The Chief virtual workspace cannot be modified"
        );
    }

    #[tokio::test]
    async fn test_open_validates_workspace_exists() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "open", &json!({ "id": "missing-ws" })).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Workspace not found"));
    }

    #[tokio::test]
    async fn test_open_returns_expected_shape() {
        let fake = FakeApi::default();
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Test Workspace"));
        }
        let api: Arc<dyn WorkspaceApi> = Arc::new(fake.clone());

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "open", &json!({ "id": "ws-1" }))
            .await
            .unwrap();

        assert!(result.get("ok").unwrap().as_bool().unwrap());
        assert!(result.get("queued").unwrap().as_bool().unwrap());

        // Assert event was published
        let events = fake.published_events();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].event_type,
            intent_core::events::APP_WORKSPACE_OPEN
        );
        assert_eq!(
            events[0].data.get("workspaceId").unwrap().as_str().unwrap(),
            "ws-1"
        );
        assert!(!events[0]
            .data
            .get("openInNewWindow")
            .unwrap()
            .as_bool()
            .unwrap());
    }

    #[tokio::test]
    async fn test_open_accepts_open_in_new_window_option() {
        let fake = FakeApi::default();
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Test"));
        }
        let api: Arc<dyn WorkspaceApi> = Arc::new(fake.clone());

        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "open",
            &json!({ "id": "ws-1", "openInNewWindow": true }),
        )
        .await
        .unwrap();

        assert!(result.get("ok").unwrap().as_bool().unwrap());
        assert!(result.get("queued").unwrap().as_bool().unwrap());

        // Assert event includes openInNewWindow
        let events = fake.published_events();
        assert_eq!(events.len(), 1);
        assert!(events[0]
            .data
            .get("openInNewWindow")
            .unwrap()
            .as_bool()
            .unwrap());
    }
}

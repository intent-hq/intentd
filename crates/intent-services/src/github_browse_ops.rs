//! Pure DTO mappers for the `github.*` browse / auth / identity wire surface
//! (PROTOCOL §5.27, GH-WIRE-A). These project the host-agnostic engine models
//! (`intent_sourcecontrol`) onto the GitHub-shaped camelCase wire contract the
//! frontend consumes. Kept free of any I/O so they are unit-testable in
//! isolation; the live engine calls live in the `WorkspaceApi` handlers.
//!
//! 🔒 The PAT is never read or echoed here — only derived, non-sensitive
//! identity / connection fields cross the wire.

use intent_sourcecontrol::{RemoteBranches, Repo, UserIdentity};
use serde_json::{json, Map, Value};

/// Guidance returned by the no-op `github.connect` / `github.revoke` methods in
/// the PAT-from-env model (no OAuth / device flow; nothing to connect/revoke).
pub(crate) const CONNECT_GUIDANCE: &str =
    "GitHub uses a Personal Access Token from the environment. Set GITHUB_TOKEN (or GH_TOKEN) and restart.";
pub(crate) const REVOKE_GUIDANCE: &str =
    "GitHub credentials come from the environment (GITHUB_TOKEN / GH_TOKEN); there is nothing to revoke. Unset the variable and restart to disconnect.";

/// Project an engine [`Repo`] to the wire `GithubRepo` (§5.27): the engine
/// `url` carries GitHub's `html_url`, surfaced as `htmlUrl`; the remaining
/// fields already match the camelCase contract. Absent optionals are omitted.
pub(crate) fn repo_to_wire(repo: &Repo) -> Value {
    let mut obj = Map::new();
    obj.insert("owner".into(), json!(repo.owner));
    obj.insert("name".into(), json!(repo.name));
    if let Some(url) = &repo.url {
        obj.insert("htmlUrl".into(), json!(url));
    }
    if let Some(b) = &repo.default_branch {
        obj.insert("defaultBranch".into(), json!(b));
    }
    if let Some(c) = &repo.created_at {
        obj.insert("createdAt".into(), json!(c));
    }
    if let Some(u) = &repo.updated_at {
        obj.insert("updatedAt".into(), json!(u));
    }
    Value::Object(obj)
}

/// Project a list of engine repos to a `GithubRepo[]` wire array.
pub(crate) fn repos_to_wire(repos: &[Repo]) -> Value {
    Value::Array(repos.iter().map(repo_to_wire).collect())
}

/// Project engine [`RemoteBranches`] to the wire branch-name list (§5.27):
/// `branches: string[]`. `nextToken` is always `null` — the engine fetches a
/// single bounded page and exposes no continuation cursor, so no honorable
/// opaque token (§5.5) can be issued (see the module note / task learnings).
pub(crate) fn branch_names(page: &RemoteBranches) -> Vec<String> {
    page.branches.iter().map(|b| b.name.clone()).collect()
}

/// Project an engine [`UserIdentity`] to the wire `GithubUser` (§5.27): only
/// the non-sensitive identity fields (`login` / `avatarUrl` / `htmlUrl`) — the
/// engine `id` / `name` are dropped and the credential is never included.
pub(crate) fn user_to_wire(user: &UserIdentity) -> Value {
    json!({
        "login": user.login,
        "avatarUrl": user.avatar_url.clone().unwrap_or_default(),
        "htmlUrl": user.html_url.clone().unwrap_or_default(),
    })
}

/// Build the `github.authStatus` result (§5.27). `is_configured` reflects an
/// env PAT that resolves and whose `GET /user` succeeds; the remaining fields
/// are kept for FE shape parity in the PAT-from-env model. Never carries a
/// token.
pub(crate) fn auth_status_to_wire(is_configured: bool) -> Value {
    json!({
        "isConfigured": is_configured,
        "oauthUrl": "",
        "configuredButNeedsUpdate": false,
        "updatedScopes": "",
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_sourcecontrol::Branch;

    fn sample_repo() -> Repo {
        Repo {
            owner: "octocat".into(),
            name: "hello".into(),
            url: Some("https://github.com/octocat/hello".into()),
            default_branch: Some("main".into()),
            created_at: None,
            updated_at: Some("2026-01-02T03:04:05Z".into()),
        }
    }

    #[test]
    fn repo_renames_url_to_html_url_and_omits_absent() {
        let v = repo_to_wire(&sample_repo());
        assert_eq!(v["htmlUrl"], "https://github.com/octocat/hello");
        assert_eq!(v["defaultBranch"], "main");
        assert_eq!(v["updatedAt"], "2026-01-02T03:04:05Z");
        // `url` is projected, never echoed verbatim; `createdAt` is omitted.
        assert!(v.get("url").is_none());
        assert!(v.get("createdAt").is_none());
    }

    #[test]
    fn branch_names_extracts_names_only() {
        let page = RemoteBranches {
            branches: vec![
                Branch {
                    name: "main".into(),
                    commit_sha: Some("abc".into()),
                    protected: true,
                },
                Branch {
                    name: "dev".into(),
                    commit_sha: None,
                    protected: false,
                },
            ],
            has_next_page: true,
        };
        assert_eq!(branch_names(&page), vec!["main", "dev"]);
    }

    #[test]
    fn user_drops_id_name_and_defaults_optionals() {
        let user = UserIdentity {
            login: "octocat".into(),
            id: Some(583231),
            name: Some("The Octocat".into()),
            avatar_url: Some("https://avatars/u/1".into()),
            html_url: None,
        };
        let v = user_to_wire(&user);
        assert_eq!(v["login"], "octocat");
        assert_eq!(v["avatarUrl"], "https://avatars/u/1");
        assert_eq!(v["htmlUrl"], "");
        assert!(v.get("id").is_none());
        assert!(v.get("name").is_none());
    }

    #[test]
    fn auth_status_shape() {
        let v = auth_status_to_wire(true);
        assert_eq!(v["isConfigured"], true);
        assert_eq!(v["oauthUrl"], "");
        assert_eq!(v["configuredButNeedsUpdate"], false);
        assert_eq!(v["updatedScopes"], "");
    }
}

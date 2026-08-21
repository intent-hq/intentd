//! Pure DTO mappers for the `github.*` browse / auth / identity wire surface
//! (PROTOCOL §5.27, GH-WIRE-A). These project the host-agnostic engine models
//! (`intent_sourcecontrol`) onto the GitHub-shaped camelCase wire contract the
//! frontend consumes. Kept free of any I/O so they are unit-testable in
//! isolation; the live engine calls live in the `WorkspaceApi` handlers.
//!
//! 🔒 The PAT is never read or echoed here — only derived, non-sensitive
//! identity / connection fields cross the wire.

use intent_sourcecontrol::{Branch, Repo, UserIdentity};
use serde_json::{json, Map, Value};

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

/// Project a page of engine [`Branch`]es to the wire branch-name list (§5.27):
/// `branches: string[]`. The §5.5 `nextToken` is derived separately from the
/// engine page's continuation cursor by the handler.
pub(crate) fn branch_names(branches: &[Branch]) -> Vec<String> {
    branches.iter().map(|b| b.name.clone()).collect()
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let branches = vec![
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
        ];
        assert_eq!(branch_names(&branches), vec!["main", "dev"]);
    }

    #[test]
    fn user_drops_id_name_and_defaults_optionals() {
        let user = UserIdentity {
            login: "octocat".into(),
            id: Some(583_231),
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
}

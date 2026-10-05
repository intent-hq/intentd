//! Pure DTO mappers for the `github.*` browse / auth / identity wire surface
//! (PROTOCOL §5.27, GH-WIRE-A). These project the host-agnostic engine models
//! (`intent_sourcecontrol`) onto the GitHub-shaped camelCase wire contract the
//! frontend consumes. Kept free of any I/O so they are unit-testable in
//! isolation; the live engine calls live in the `WorkspaceApi` handlers.
//!
//! 🔒 The PAT is never read or echoed here — only derived, non-sensitive
//! identity / connection fields cross the wire.

use intent_core::GitRemoteUrl;
use intent_sourcecontrol::{Branch, Repo, RepoRef, UserIdentity};
use serde_json::{json, Map, Value};

/// Upper bound on the related repos `github.relatedRepos.list` answers
/// (§5.27): the FE widens a context search by at most this many siblings.
pub(crate) const RELATED_REPOS_CAP: usize = 5;

/// One `.gitmodules` entry that names a GitHub repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelatedRepo {
    pub repo: RepoRef,
    /// The submodule `path` exactly as written in `.gitmodules`.
    pub path: String,
}

/// The GitHub repository a `.gitmodules` `url` names, if any. Accepts every
/// form [`GitRemoteUrl`] understands (`https://github.com/o/r(.git)`,
/// `git@github.com:o/r(.git)`, `ssh://git@github.com/o/r(.git)`, …) plus the
/// bare scheme-less `github.com/o/r`. Relative URLs (`./`, `../` — resolved
/// against the superproject's own remote by git) and non-GitHub hosts yield
/// `None`.
pub(crate) fn github_repo_from_gitmodules_url(url: &str) -> Option<RepoRef> {
    let url = url.trim();
    if url.starts_with("./") || url.starts_with("../") {
        return None;
    }
    let parsed = GitRemoteUrl::parse(url).or_else(|| {
        if url.contains("://") {
            None
        } else {
            GitRemoteUrl::parse(&format!("https://{url}"))
        }
    })?;
    parsed.github_repo()
}

/// Project a `.gitmodules` file onto the GitHub repos it references
/// (§5.27 `github.relatedRepos.list`): one entry per `[submodule]` section
/// carrying both a `path` and a GitHub `url`, in file order, deduplicated by
/// repo identity (first occurrence wins), excluding `parent` itself
/// (case-insensitive, via [`RepoRef`] equality), capped at
/// [`RELATED_REPOS_CAP`]. Unparsable content simply yields no entries.
pub(crate) fn related_repos_from_gitmodules(content: &str, parent: &RepoRef) -> Vec<RelatedRepo> {
    let mut out: Vec<RelatedRepo> = Vec::new();
    let mut section: Option<(Option<String>, Option<String>)> = None;

    let flush = |section: &mut Option<(Option<String>, Option<String>)>,
                 out: &mut Vec<RelatedRepo>| {
        let Some((path, url)) = section.take() else {
            return;
        };
        let (Some(path), Some(url)) = (path, url) else {
            return;
        };
        let Some(repo) = github_repo_from_gitmodules_url(&url) else {
            return;
        };
        if repo == *parent || out.iter().any(|r| r.repo == repo) {
            return;
        }
        out.push(RelatedRepo { repo, path });
    };

    for line in content.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if line.starts_with('[') {
            flush(&mut section, &mut out);
            section = is_submodule_section_header(line).then_some((None, None));
            continue;
        }
        let Some((path, url)) = section.as_mut() else {
            continue;
        };
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let Some(value) = git_config_value(value) else {
            continue;
        };
        let key = key.trim();
        if key.eq_ignore_ascii_case("path") {
            *path = Some(value);
        } else if key.eq_ignore_ascii_case("url") {
            *url = Some(value);
        }
    }
    flush(&mut section, &mut out);
    out.truncate(RELATED_REPOS_CAP);
    out
}

/// `[submodule "name"]` (git-config section names are case-insensitive), with
/// the section keyword delimited by whitespace or `]` so `[submodulex]` does
/// not match.
fn is_submodule_section_header(line: &str) -> bool {
    let Some(rest) = line.strip_prefix('[') else {
        return false;
    };
    let end = rest
        .find(|c: char| c.is_whitespace() || c == ']')
        .unwrap_or(rest.len());
    rest[..end].eq_ignore_ascii_case("submodule")
}

/// Decode the right-hand side of a git-config `key = value` line: surrounding
/// double quotes are removed (with `\"` / `\\` escapes and the `\n` / `\t` /
/// `\b` letters git accepts), quoted and unquoted segments concatenate as git
/// does, and an unquoted `#` / `;` starts a trailing comment. `None` for an
/// empty result or an unterminated quote (git itself rejects the file).
fn git_config_value(raw: &str) -> Option<String> {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.trim().chars();
    let mut in_quotes = false;
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('b') => out.push('\u{8}'),
                Some(esc) => out.push(esc),
                None => return None,
            },
            '"' => in_quotes = !in_quotes,
            '#' | ';' if !in_quotes => break,
            _ => out.push(c),
        }
    }
    if in_quotes {
        return None;
    }
    let value = out.trim();
    (!value.is_empty()).then(|| value.to_string())
}

/// Project [`RelatedRepo`] entries to the wire `{ owner, repo, path }` list.
pub(crate) fn related_repos_to_wire(repos: &[RelatedRepo]) -> Value {
    Value::Array(
        repos
            .iter()
            .map(|r| json!({ "owner": r.repo.owner, "repo": r.repo.name, "path": r.path }))
            .collect(),
    )
}

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

/// `github.users.search` page size: default and inclusive cap.
pub(crate) const DEFAULT_USER_SEARCH_LIMIT: i64 = 8;
pub(crate) const MAX_USER_SEARCH_LIMIT: i64 = 10;

/// Clamp an optional `github.users.search` `limit` into `[1, 10]` (default 8)
/// and cast to the engine's `u8` width.
pub(crate) fn clamp_user_search_limit(limit: Option<i64>) -> u8 {
    u8::try_from(
        limit
            .unwrap_or(DEFAULT_USER_SEARCH_LIMIT)
            .clamp(1, MAX_USER_SEARCH_LIMIT),
    )
    .unwrap_or(1)
}

/// Project `github.users.search` hits to the wire user list: `{ id, login,
/// avatarUrl, htmlUrl }` per hit. `id` is required on the wire (the FE keys
/// the picker on it), so a hit the forge answered without one is dropped.
pub(crate) fn user_hits_to_wire(users: &[UserIdentity]) -> Value {
    Value::Array(
        users
            .iter()
            .filter_map(|user| {
                let id = user.id?;
                Some(json!({
                    "id": id,
                    "login": user.login,
                    "avatarUrl": user.avatar_url.clone().unwrap_or_default(),
                    "htmlUrl": user.html_url.clone().unwrap_or_default(),
                }))
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

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

    #[test]
    fn user_search_limit_defaults_and_clamps() {
        assert_eq!(clamp_user_search_limit(None), 8);
        assert_eq!(clamp_user_search_limit(Some(0)), 1);
        assert_eq!(clamp_user_search_limit(Some(-4)), 1);
        assert_eq!(clamp_user_search_limit(Some(3)), 3);
        assert_eq!(clamp_user_search_limit(Some(10)), 10);
        assert_eq!(clamp_user_search_limit(Some(500)), 10);
    }

    #[test]
    fn user_hits_carry_id_and_drop_hits_without_one() {
        let users = vec![
            UserIdentity {
                login: "octocat".into(),
                id: Some(583_231),
                name: Some("The Octocat".into()),
                avatar_url: Some("https://avatars/u/1".into()),
                html_url: Some("https://github.com/octocat".into()),
            },
            UserIdentity {
                login: "ghost".into(),
                id: None,
                name: None,
                avatar_url: None,
                html_url: None,
            },
        ];
        let v = user_hits_to_wire(&users);
        let hits = v.as_array().expect("array");
        assert_eq!(hits.len(), 1);
        assert_eq!(
            hits[0],
            json!({
                "id": 583_231,
                "login": "octocat",
                "avatarUrl": "https://avatars/u/1",
                "htmlUrl": "https://github.com/octocat",
            })
        );
        assert!(hits[0].get("name").is_none());
        assert_eq!(user_hits_to_wire(&[]), json!([]));
    }

    #[test]
    fn gitmodules_url_accepts_every_github_form() {
        let widget = RepoRef::new("acme", "widget");
        for url in [
            "https://github.com/acme/widget",
            "https://github.com/acme/widget.git",
            "git@github.com:acme/widget",
            "git@github.com:acme/widget.git",
            "ssh://git@github.com/acme/widget",
            "ssh://git@github.com/acme/widget.git",
            "github.com/acme/widget",
            "github.com/acme/widget.git",
            "  https://github.com/acme/widget.git  ",
        ] {
            assert_eq!(
                github_repo_from_gitmodules_url(url),
                Some(widget.clone()),
                "{url}"
            );
        }
        // Casing is preserved for display.
        assert_eq!(
            github_repo_from_gitmodules_url("https://github.com/Acme/Widget.git"),
            Some(RepoRef::new("Acme", "Widget"))
        );
    }

    #[test]
    fn gitmodules_url_rejects_relative_and_non_github() {
        for url in [
            "./sibling",
            "../sibling.git",
            "../../other/repo.git",
            "https://gitlab.com/acme/widget.git",
            "git@bitbucket.org:acme/widget.git",
            "https://github.com/acme",
            "https://github.com/acme/widget/extra",
            "file:///srv/git/widget.git",
            "/srv/git/widget.git",
            "",
        ] {
            assert_eq!(github_repo_from_gitmodules_url(url), None, "{url}");
        }
    }

    #[test]
    fn related_repos_preserve_order_dedupe_and_exclude_parent() {
        let parent = RepoRef::new("intent-hq", "intent");
        let content = "[submodule \"packages/intentd\"]\n\
             \tpath = packages/intentd\n\
             \turl = https://github.com/intent-hq/intentd.git\n\
             [submodule \"packages/fe\"]\n\
             \tpath = packages/fe\n\
             \turl = git@github.com:intent-hq/cloudlands-fe.git\n\
             \tupdate = none\n\
             [submodule \"self\"]\n\
             \tpath = vendor/self\n\
             \turl = https://github.com/Intent-HQ/Intent.git\n\
             [submodule \"dup\"]\n\
             \tpath = vendor/intentd-again\n\
             \turl = ssh://git@github.com/Intent-HQ/IntentD\n\
             [submodule \"foreign\"]\n\
             \tpath = vendor/foreign\n\
             \turl = https://gitlab.com/acme/widget.git\n\
             [submodule \"relative\"]\n\
             \tpath = vendor/relative\n\
             \turl = ../sibling.git\n\
             [submodule \"no-url\"]\n\
             \tpath = vendor/no-url\n\
             [submodule \"no-path\"]\n\
             \turl = https://github.com/acme/no-path\n\
             [core]\n\
             \tpath = not-a-submodule\n\
             \turl = https://github.com/acme/core\n";
        let repos = related_repos_from_gitmodules(content, &parent);
        assert_eq!(
            repos,
            vec![
                RelatedRepo {
                    repo: RepoRef::new("intent-hq", "intentd"),
                    path: "packages/intentd".into(),
                },
                RelatedRepo {
                    repo: RepoRef::new("intent-hq", "cloudlands-fe"),
                    path: "packages/fe".into(),
                },
            ]
        );
        assert_eq!(
            related_repos_to_wire(&repos),
            json!([
                { "owner": "intent-hq", "repo": "intentd", "path": "packages/intentd" },
                { "owner": "intent-hq", "repo": "cloudlands-fe", "path": "packages/fe" },
            ])
        );
    }

    #[test]
    fn related_repos_cap_at_five_after_dedupe() {
        let parent = RepoRef::new("acme", "mono");
        let mut content = String::new();
        // A duplicate and the parent come first so the cap applies to the
        // filtered list, not the raw section count.
        content.push_str("[submodule \"p\"]\n\tpath = p\n\turl = https://github.com/acme/mono\n");
        for i in 0..8 {
            let _ = writeln!(
                content,
                "[submodule \"s{i}\"]\n\tpath = libs/s{i}\n\turl = https://github.com/acme/s{i}.git"
            );
            if i == 0 {
                content.push_str(
                    "[submodule \"s0-dup\"]\n\tpath = libs/s0-dup\n\turl = git@github.com:ACME/S0.git\n",
                );
            }
        }
        let repos = related_repos_from_gitmodules(&content, &parent);
        assert_eq!(repos.len(), RELATED_REPOS_CAP);
        let paths: Vec<&str> = repos.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(
            paths,
            vec!["libs/s0", "libs/s1", "libs/s2", "libs/s3", "libs/s4"]
        );
    }

    #[test]
    fn git_config_value_unquotes_and_strips_comments() {
        for (raw, expected) in [
            (
                "https://github.com/acme/widget.git",
                Some("https://github.com/acme/widget.git"),
            ),
            (
                "\"https://github.com/acme/widget.git\"",
                Some("https://github.com/acme/widget.git"),
            ),
            ("  \"libs/widget\"  ", Some("libs/widget")),
            ("libs/widget # the widget", Some("libs/widget")),
            ("libs/widget ; the widget", Some("libs/widget")),
            ("\"libs/widget\" # the widget", Some("libs/widget")),
            ("\"libs/widget\"; the widget", Some("libs/widget")),
            // `#` / `;` inside quotes are literal; `\"` and `\\` unescape.
            ("\"libs/#1;a\"", Some("libs/#1;a")),
            ("\"say \\\"hi\\\"\"", Some("say \"hi\"")),
            ("\"a\\\\b\"", Some("a\\b")),
            // quoted and unquoted segments concatenate like git.
            ("\"libs/\"widget", Some("libs/widget")),
            ("", None),
            ("   ", None),
            ("# only a comment", None),
            ("\"\"", None),
            ("\"unterminated", None),
        ] {
            assert_eq!(git_config_value(raw).as_deref(), expected, "{raw:?}");
        }
    }

    #[test]
    fn related_repos_accept_quoted_and_commented_values() {
        let parent = RepoRef::new("intent-hq", "intent");
        let content = "# superproject submodules\n\
             [submodule \"quoted\"]\n\
             \tpath = \"packages/quoted\"\n\
             \turl = \"https://github.com/acme/quoted.git\"\n\
             [submodule \"commented\"]\n\
             \tpath = packages/commented # pinned\n\
             \turl = git@github.com:acme/commented.git ; ssh form\n\
             [submodule \"both\"]\n\
             \t; a full-line comment inside the section\n\
             \tpath = \"packages/both\" # quoted then commented\n\
             \turl = \"ssh://git@github.com/acme/both\"; quoted then commented\n\
             [submodule \"hash-in-quotes\"]\n\
             \tpath = \"packages/#hash\"\n\
             \turl = \"https://github.com/acme/hash\"\n\
             [Submodule \"mixed-case\"]\n\
             \tPath = packages/mixed\n\
             \tURL = https://github.com/acme/mixed.git\n\
             [submodulex \"not-a-submodule\"]\n\
             \tpath = packages/nope\n\
             \turl = https://github.com/acme/nope.git\n\
             [submodule \"broken\"]\n\
             \tpath = \"packages/broken\n\
             \turl = \"https://github.com/acme/broken\n";
        let repos = related_repos_from_gitmodules(content, &parent);
        assert_eq!(
            repos,
            vec![
                RelatedRepo {
                    repo: RepoRef::new("acme", "quoted"),
                    path: "packages/quoted".into(),
                },
                RelatedRepo {
                    repo: RepoRef::new("acme", "commented"),
                    path: "packages/commented".into(),
                },
                RelatedRepo {
                    repo: RepoRef::new("acme", "both"),
                    path: "packages/both".into(),
                },
                RelatedRepo {
                    repo: RepoRef::new("acme", "hash"),
                    path: "packages/#hash".into(),
                },
                RelatedRepo {
                    repo: RepoRef::new("acme", "mixed"),
                    path: "packages/mixed".into(),
                },
            ]
        );
    }

    #[test]
    fn related_repos_unparsable_content_is_empty() {
        let parent = RepoRef::new("acme", "mono");
        assert!(related_repos_from_gitmodules("", &parent).is_empty());
        assert!(related_repos_from_gitmodules("garbage\n= = =\n[", &parent).is_empty());
        assert!(
            related_repos_from_gitmodules("path = x\nurl = https://github.com/a/b", &parent)
                .is_empty()
        );
    }
}

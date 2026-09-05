//! Root-repository remote state for workspace transfer
//! (intent-hq/intent#4438): which remotes the source checkout had, where its
//! remote-tracking refs pointed, and which upstream the workspace branch
//! tracked. Without it the import lands as a remote-less repository and
//! every commit on the branch reads as unpublished.
//!
//! What travels is a narrow, validated allowlist — remote name, fetch URL,
//! optional push URL, the direct `refs/remotes/<name>/*` tips (whose objects
//! ride the bundle), and the workspace branch's `branch.<b>.remote/merge` —
//! never arbitrary git config, hooks, credential helpers or credentials. The
//! same validation runs on export and on import: the manifest is untrusted
//! input, so an entry a compliant daemon would never have written fails the
//! import instead of being configured.
//!
//! Known limitations, by design: a remote is skipped at export (logged by
//! name, never by URL) when its URL is a machine-local path, a `file://`
//! URL, or a remote-helper address (`<helper>::…`); embedded credentials are
//! stripped — the whole `http(s)` userinfo (a token may masquerade as a
//! username) and any `ssh`/`git` password — so the remote survives but the
//! target authenticates on its own; fetch refspecs are not copied (the
//! restored remote gets git's default); symbolic refs such as
//! `refs/remotes/origin/HEAD` are not restored. The restored tracking refs
//! are the source's snapshot at export time, not a fresh fetch.

use std::path::Path;

use intent_core::{Error, Result};

use crate::transfer_git::{
    run_git, BranchUpstream, RemoteBundleRef, TrackingRef, TransferRefsManifest,
};

/// Why a remote (or push URL) was left out of the transfer. Deliberately
/// value-free so it can be logged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteSkip {
    /// Not a form this module allowlists: local path, `file://`, unknown
    /// scheme, remote helper, malformed, or unsafe characters.
    Unportable,
    /// The remote name is not a plain, safe name.
    Name,
}

impl std::fmt::Display for RemoteSkip {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Unportable => "URL is not a portable remote form",
            Self::Name => "remote name is not a plain safe name",
        })
    }
}

/// Validate a remote name: the git `remote.<name>` key restricted to a plain
/// allowlist (`[A-Za-z0-9._-]`, no leading `-`/`.`, no `..`, no `.lock`
/// suffix), which is also what makes `refs/remotes/<name>/` a safe prefix.
pub(crate) fn validate_remote_name(name: &str) -> std::result::Result<(), RemoteSkip> {
    let ok = !name.is_empty()
        && !name.starts_with(['-', '.'])
        && !name.contains("..")
        && !has_lock_suffix(name)
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if ok {
        Ok(())
    } else {
        Err(RemoteSkip::Name)
    }
}

/// Reduce a remote URL to a portable, credential-free form or explain why it
/// cannot travel. Accepted: `https://`, `http://`, `ssh://`, `git://` URLs
/// and scp-like `[user@]host:path`. `http(s)` userinfo is dropped entirely;
/// `ssh` / `git` keep the username and drop any password.
pub(crate) fn sanitize_remote_url(url: &str) -> std::result::Result<String, RemoteSkip> {
    if url.is_empty()
        || url.starts_with('-')
        || url
            .chars()
            .any(|c| c.is_control() || c.is_whitespace() || c == '\\')
    {
        return Err(RemoteSkip::Unportable);
    }
    // git's remote-helper detection: a leading run of URL-scheme characters
    // followed by `::` selects `git-remote-<helper>` (ext, fd, …).
    let scheme_end = url
        .find(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '+' | '.' | '-')))
        .unwrap_or(url.len());
    if url[scheme_end..].starts_with("::") {
        return Err(RemoteSkip::Unportable);
    }
    if let Some(rest) = url[scheme_end..].strip_prefix("://") {
        let scheme = url[..scheme_end].to_ascii_lowercase();
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, ""),
        };
        let (userinfo, host) = match authority.rsplit_once('@') {
            Some((u, h)) => (Some(u), h),
            None => (None, authority),
        };
        if host.is_empty() || host.starts_with('-') {
            return Err(RemoteSkip::Unportable);
        }
        return match scheme.as_str() {
            "https" | "http" => Ok(format!("{scheme}://{host}{path}")),
            "ssh" | "git" => {
                match userinfo.map(|u| u.split_once(':').map_or(u, |(user, _)| user)) {
                    Some("") => Err(RemoteSkip::Unportable),
                    Some(user) => Ok(format!("{scheme}://{user}@{host}{path}")),
                    None => Ok(format!("{scheme}://{host}{path}")),
                }
            }
            _ => Err(RemoteSkip::Unportable),
        };
    }
    // scp-like: `[user@]host:path`, where no `/` precedes the first `:`
    // (otherwise git reads it as a local path). A single-letter prefix is a
    // Windows drive, i.e. local.
    let Some((prefix, path)) = url.split_once(':') else {
        return Err(RemoteSkip::Unportable);
    };
    let host = prefix.rsplit_once('@').map_or(prefix, |(_, h)| h);
    if prefix.contains('/')
        || prefix.len() < 2
        || host.is_empty()
        || host.starts_with('-')
        || prefix.starts_with('@')
        || path.is_empty()
    {
        return Err(RemoteSkip::Unportable);
    }
    Ok(url.to_string())
}

/// Validate a full ref name under a fixed `prefix` (`refs/remotes/<name>/`
/// or `refs/heads/`) against git's ref-format rules, so a manifest can never
/// steer a fetch or upstream at an unexpected destination.
pub(crate) fn validate_ref_under(name: &str, prefix: &str) -> std::result::Result<(), String> {
    let Some(rest) = name.strip_prefix(prefix) else {
        return Err(format!("ref {name:?} is not under {prefix}"));
    };
    let bad = |c: char| {
        c.is_control() || c.is_whitespace() || matches!(c, '~' | '^' | ':' | '?' | '*' | '[' | '\\')
    };
    if rest.is_empty()
        || rest.contains("..")
        || rest.contains("@{")
        || rest.ends_with('.')
        || rest.chars().any(bad)
    {
        return Err(format!("ref {name:?} is not a valid ref name"));
    }
    for component in rest.split('/') {
        if component.is_empty()
            || component.starts_with('.')
            || has_lock_suffix(component)
            || component == "@"
        {
            return Err(format!("ref {name:?} is not a valid ref name"));
        }
    }
    Ok(())
}

fn has_lock_suffix(s: &str) -> bool {
    s.get(s.len().saturating_sub(5)..)
        .is_some_and(|tail| tail.eq_ignore_ascii_case(".lock"))
}

fn is_full_sha(s: &str) -> bool {
    (s.len() == 40 || s.len() == 64) && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Export side: the remotes of `repo` that pass the allowlist, each with its
/// direct remote-tracking tips, plus the workspace branch's upstream when it
/// names one of those remotes. Skips are logged by remote name only.
pub(crate) fn capture_remotes(
    repo: &git2::Repository,
    workspace_branch: &str,
) -> Result<(Vec<RemoteBundleRef>, Option<BranchUpstream>)> {
    let names = repo
        .remotes()
        .map_err(|e| Error::Internal(format!("list remotes failed: {e}")))?;
    let mut remotes = Vec::new();
    for name in names.iter().flatten().flatten() {
        let remote = repo
            .find_remote(name)
            .map_err(|e| Error::Internal(format!("read remote {name} failed: {e}")))?;
        let sanitized = validate_remote_name(name)
            .and_then(|()| remote.url().map_err(|_| RemoteSkip::Unportable))
            .and_then(sanitize_remote_url);
        let url = match sanitized {
            Ok(url) => url,
            Err(reason) => {
                tracing::warn!(remote = %name, %reason, "transfer bundle: remote not transferred");
                continue;
            }
        };
        let push_url = match remote.pushurl().ok().flatten().map(sanitize_remote_url) {
            None => None,
            Some(Ok(u)) => Some(u),
            Some(Err(reason)) => {
                tracing::warn!(remote = %name, %reason, "transfer bundle: push URL not transferred");
                None
            }
        };
        let prefix = format!("refs/remotes/{name}/");
        let mut tracking_refs = Vec::new();
        let glob = repo
            .references_glob(&format!("{prefix}*"))
            .map_err(|e| Error::Internal(format!("list tracking refs of {name} failed: {e}")))?;
        for reference in glob.flatten() {
            let (Ok(ref_name), Some(oid)) = (reference.name(), reference.target()) else {
                continue;
            };
            if validate_ref_under(ref_name, &prefix).is_err() || repo.find_commit(oid).is_err() {
                continue;
            }
            tracking_refs.push(TrackingRef {
                ref_name: ref_name.to_string(),
                sha: oid.to_string(),
            });
        }
        remotes.push(RemoteBundleRef {
            name: name.to_string(),
            url,
            push_url,
            tracking_refs,
        });
    }

    let config = repo
        .config()
        .map_err(|e| Error::Internal(format!("read repo config failed: {e}")))?;
    let upstream = match (
        config
            .get_string(&format!("branch.{workspace_branch}.remote"))
            .ok(),
        config
            .get_string(&format!("branch.{workspace_branch}.merge"))
            .ok(),
    ) {
        (Some(remote), Some(merge_ref))
            if remotes.iter().any(|r| r.name == remote)
                && validate_ref_under(&merge_ref, "refs/heads/").is_ok() =>
        {
            Some(BranchUpstream { remote, merge_ref })
        }
        _ => None,
    };
    Ok((remotes, upstream))
}

/// Import side: recreate the manifest's remotes on the materialized checkout
/// and fetch their tracking refs from the bundle (no network), then restore
/// the workspace branch's upstream. Every field is re-validated; anything a
/// compliant export would not have written is an error, and a tracking ref
/// pointing at a WIP snapshot commit is rejected so a local-only sentinel
/// can never read as published.
pub(crate) fn restore_remotes(
    checkout_dir: &Path,
    bundle: &str,
    refs: &TransferRefsManifest,
) -> Result<()> {
    let wip_shas: Vec<&str> = refs
        .workspace_wip_commit_sha
        .iter()
        .chain(
            refs.sandboxes
                .iter()
                .filter_map(|s| s.wip_commit_sha.as_ref()),
        )
        .map(String::as_str)
        .collect();
    let mut seen = std::collections::HashSet::new();
    for remote in &refs.remotes {
        let name = remote.name.as_str();
        let fail = |what: String| Error::Internal(format!("restore remote {name}: {what}"));
        validate_remote_name(name).map_err(|e| fail(e.to_string()))?;
        if !seen.insert(name) {
            return Err(fail("listed twice".to_string()));
        }
        let url = sanitize_remote_url(&remote.url).map_err(|e| fail(e.to_string()))?;
        let push_url = remote
            .push_url
            .as_deref()
            .map(sanitize_remote_url)
            .transpose()
            .map_err(|e| fail(format!("push URL: {e}")))?;
        let prefix = format!("refs/remotes/{name}/");
        for t in &remote.tracking_refs {
            validate_ref_under(&t.ref_name, &prefix).map_err(fail)?;
            if !is_full_sha(&t.sha) {
                return Err(fail(format!(
                    "tracking ref {} sha is not a full sha",
                    t.ref_name
                )));
            }
            if wip_shas.contains(&t.sha.as_str()) {
                return Err(fail(format!(
                    "tracking ref {} points at a WIP snapshot commit",
                    t.ref_name
                )));
            }
        }

        run_git(checkout_dir, |cmd| {
            cmd.args(["remote", "add", name, &url]);
        })
        .map_err(|e| fail(format!("remote add failed: {e}")))?;
        if let Some(push_url) = &push_url {
            run_git(checkout_dir, |cmd| {
                cmd.args(["config", &format!("remote.{name}.pushurl"), push_url]);
            })
            .map_err(|e| fail(format!("set push URL failed: {e}")))?;
        }
        if remote.tracking_refs.is_empty() {
            continue;
        }
        run_git(checkout_dir, |cmd| {
            cmd.args(["fetch", "--no-tags", "--quiet", bundle]);
            cmd.args(
                remote
                    .tracking_refs
                    .iter()
                    .map(|t| format!("+{0}:{0}", t.ref_name)),
            );
        })
        .map_err(|e| fail(format!("fetch tracking refs from bundle failed: {e}")))?;
        let repo = git2::Repository::open(checkout_dir)
            .map_err(|e| fail(format!("open checkout failed: {e}")))?;
        for t in &remote.tracking_refs {
            let actual = repo
                .find_reference(&t.ref_name)
                .ok()
                .and_then(|r| r.target())
                .map(|o| o.to_string());
            if actual.as_deref() != Some(t.sha.as_str()) {
                return Err(fail(format!(
                    "tracking ref {} restored at {actual:?}, manifest recorded {}",
                    t.ref_name, t.sha
                )));
            }
        }
    }

    if let Some(up) = &refs.workspace_upstream {
        let fail = |what: String| Error::Internal(format!("restore upstream: {what}"));
        if !seen.contains(up.remote.as_str()) {
            return Err(fail(format!(
                "remote {:?} is not among the restored remotes",
                up.remote
            )));
        }
        validate_ref_under(&up.merge_ref, "refs/heads/").map_err(fail)?;
        let branch = &refs.workspace_branch;
        run_git(checkout_dir, |cmd| {
            cmd.args(["config", &format!("branch.{branch}.remote"), &up.remote]);
        })
        .map_err(|e| fail(format!("set branch remote failed: {e}")))?;
        run_git(checkout_dir, |cmd| {
            cmd.args(["config", &format!("branch.{branch}.merge"), &up.merge_ref]);
        })
        .map_err(|e| fail(format!("set branch merge failed: {e}")))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn portable_urls_pass_through() {
        for url in [
            "https://github.com/org/repo.git",
            "http://git.example.com/repo",
            "ssh://git@github.com/org/repo.git",
            "ssh://git@github.com:2222/org/repo.git",
            "git://github.com/org/repo.git",
            "git@github.com:org/repo.git",
            "github.com:org/repo.git",
            "ssh://[::1]/repo.git",
        ] {
            assert_eq!(sanitize_remote_url(url).as_deref(), Ok(url), "{url}");
        }
    }

    #[test]
    fn credentials_are_stripped() {
        assert_eq!(
            sanitize_remote_url("https://alice@github.com/org/repo.git").as_deref(),
            Ok("https://github.com/org/repo.git")
        );
        assert_eq!(
            sanitize_remote_url("https://alice:tok3n@github.com/org/repo.git").as_deref(),
            Ok("https://github.com/org/repo.git")
        );
        assert_eq!(
            sanitize_remote_url("HTTPS://ghp_tok3n@github.com/org/repo.git").as_deref(),
            Ok("https://github.com/org/repo.git")
        );
        assert_eq!(
            sanitize_remote_url("ssh://git:pw@host/repo.git").as_deref(),
            Ok("ssh://git@host/repo.git")
        );
        assert_eq!(
            sanitize_remote_url("git://u:pw@host:9418/repo.git").as_deref(),
            Ok("git://u@host:9418/repo.git")
        );
    }

    #[test]
    fn local_and_unsafe_urls_are_rejected() {
        for url in [
            "",
            "/srv/git/repo.git",
            "./repo",
            "../repo",
            "~/repo",
            "file:///srv/git/repo.git",
            "ftp://host/repo",
            "ext::sh -c id",
            "fd::17",
            "-oProxyCommand=id",
            "--upload-pack=id",
            "ssh://@host/repo",
            "https:///repo",
            "https://-host/repo",
            "host:",
            "C:\\repo",
            "c:repo",
            "dir/with:colon",
            "@host:repo",
            "git@host:repo with space",
            "git@host:repo\nother",
            "https://host/repo\u{7f}",
        ] {
            assert_eq!(
                sanitize_remote_url(url),
                Err(RemoteSkip::Unportable),
                "{url:?}"
            );
        }
    }

    #[test]
    fn remote_names_are_allowlisted() {
        for name in ["origin", "upstream", "my-fork", "fork_2", "r.1"] {
            assert_eq!(validate_remote_name(name), Ok(()), "{name}");
        }
        for name in [
            "", "-x", ".hidden", "a..b", "x.lock", "a/b", "a b", "a\nb", "ä",
        ] {
            assert_eq!(
                validate_remote_name(name),
                Err(RemoteSkip::Name),
                "{name:?}"
            );
        }
    }

    #[test]
    fn ref_names_stay_under_their_prefix() {
        let p = "refs/remotes/origin/";
        assert!(validate_ref_under("refs/remotes/origin/main", p).is_ok());
        assert!(validate_ref_under("refs/remotes/origin/feat/x-1.2", p).is_ok());
        assert!(validate_ref_under("refs/heads/feature", "refs/heads/").is_ok());
        for name in [
            "refs/remotes/origin/",
            "refs/remotes/origin",
            "refs/remotes/other/main",
            "refs/heads/main",
            "refs/remotes/origin/../../heads/main",
            "refs/remotes/origin/a//b",
            "refs/remotes/origin/.hidden",
            "refs/remotes/origin/x.lock",
            "refs/remotes/origin/end.",
            "refs/remotes/origin/a@{1}",
            "refs/remotes/origin/@",
            "refs/remotes/origin/a b",
            "refs/remotes/origin/a:b",
            "refs/remotes/origin/a?b",
            "refs/remotes/origin/a*",
            "refs/remotes/origin/a[b",
            "refs/remotes/origin/a\\b",
            "refs/remotes/origin/a~1",
            "refs/remotes/origin/a^",
            "refs/remotes/origin/a\nb",
        ] {
            assert!(validate_ref_under(name, p).is_err(), "{name:?}");
        }
    }
}

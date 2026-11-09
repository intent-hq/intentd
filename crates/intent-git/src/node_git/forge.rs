//! Head side of a sub-agent's forge remote (nodeProtocol 16, monorepo
//! `docs/protocol/node-link.md` § Sub-agent fetch through the backend).
//!
//! Head fetches the workspace's GitHub repository into the private git
//! namespace [`NAMESPACE`] of the workspace repository
//! (`refs/namespaces/intent-forge/refs/{heads,tags}/*` and its `HEAD`), never
//! touching the user's own refs. `git upload-pack` run in that namespace
//! then advertises exactly those refs as `refs/heads/*`, `refs/tags/*` and
//! `HEAD`, so a sub-agent's `origin/<branch>` is the branch on GitHub.

use std::fmt::Write as _;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use intent_core::{Error, Result};
use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;

use super::{run, sanitized_git};

/// The git namespace holding head's copy of the GitHub refs.
pub const NAMESPACE: &str = "intent-forge";

const PREFIX: &str = "refs/namespaces/intent-forge/";

/// How long one refresh from GitHub may run.
pub const REFRESH_TIMEOUT: Duration = Duration::from_secs(30);

fn git_in(repository: &Path, token: Option<&str>) -> Command {
    let mut command = sanitized_git();
    command.arg("-C").arg(repository);
    if let Some(token) = crate::auth::usable_token(token) {
        command
            .args(["-c", "credential.https://github.com.helper="])
            .arg("-c")
            .arg(crate::auth::token_helper_config())
            .env(crate::auth::TOKEN_ENV, token);
    }
    command
}

/// Fetch `url` into the namespace, pruning what GitHub no longer has, and
/// record GitHub's default branch as the namespace's `HEAD`. Bounded by
/// [`REFRESH_TIMEOUT`]. The token reaches git only through the child's
/// environment.
///
/// # Errors
///
/// `node-git-failed` when the fetch fails or times out; the namespace then
/// keeps the refs of the last successful refresh.
pub async fn refresh(repository: &Path, url: &str, token: Option<&str>) -> Result<()> {
    let deadline = tokio::time::Instant::now() + REFRESH_TIMEOUT;
    let mut fetch = git_in(repository, token);
    fetch
        .args([
            "fetch",
            "--quiet",
            "--prune",
            "--no-tags",
            "--no-write-fetch-head",
            "--no-recurse-submodules",
            "--no-auto-gc",
        ])
        .arg(url)
        .arg(format!("+refs/heads/*:{PREFIX}refs/heads/*"))
        .arg(format!("+refs/tags/*:{PREFIX}refs/tags/*"));
    tokio::time::timeout_at(deadline, run(fetch))
        .await
        .map_err(|_| Error::Internal("node-git-failed: the GitHub refresh timed out".into()))??;
    let mut symref = git_in(repository, token);
    symref.args(["ls-remote", "--symref", url, "HEAD"]);
    let branch = match tokio::time::timeout_at(deadline, run(symref)).await {
        Ok(Ok(out)) => default_branch(&out).map(str::to_owned),
        _ => None,
    };
    let mut head = sanitized_git();
    head.arg("-C").arg(repository);
    match branch {
        Some(branch) => head.args([
            "symbolic-ref".to_owned(),
            format!("{PREFIX}HEAD"),
            format!("{PREFIX}{branch}"),
        ]),
        None => head.args([
            "update-ref".to_owned(),
            "--no-deref".to_owned(),
            "-d".to_owned(),
            format!("{PREFIX}HEAD"),
        ]),
    };
    let _ = run(head).await;
    Ok(())
}

/// The `refs/heads/<branch>` that `git ls-remote --symref <url> HEAD`
/// names.
fn default_branch(ls_remote: &str) -> Option<&str> {
    ls_remote.lines().find_map(|line| {
        let (target, name) = line.strip_prefix("ref: ")?.split_once('\t')?;
        (name == "HEAD" && target.starts_with("refs/heads/")).then_some(target)
    })
}

/// Whether the namespace holds any ref.
pub async fn has_refs(repository: &Path) -> bool {
    let mut command = sanitized_git();
    command.arg("-C").arg(repository).args([
        "for-each-ref",
        "--count=1",
        "--format=%(refname)",
        PREFIX,
    ]);
    run(command).await.is_ok_and(|out| !out.is_empty())
}

/// Fill an empty namespace from the repository's `origin` refs and tags,
/// for a first refresh that failed. `false` when there was nothing to copy.
///
/// # Errors
///
/// When git cannot list or write the refs.
pub async fn seed_from_origin(repository: &Path) -> Result<bool> {
    let mut list = sanitized_git();
    list.arg("-C").arg(repository).args([
        "for-each-ref",
        "--format=%(objectname) %(refname) %(symref)",
        "refs/remotes/origin/",
        "refs/tags/",
    ]);
    let listed = run(list).await?;
    let mut updates = String::new();
    let mut head = None;
    for line in listed.lines() {
        let mut parts = line.splitn(3, ' ');
        let (Some(oid), Some(name)) = (parts.next(), parts.next()) else {
            continue;
        };
        let symref = parts.next().unwrap_or_default();
        if let Some(branch) = name.strip_prefix("refs/remotes/origin/") {
            if branch == "HEAD" {
                head = symref
                    .strip_prefix("refs/remotes/origin/")
                    .map(|target| format!("refs/heads/{target}"));
                continue;
            }
            let _ = writeln!(updates, "update {PREFIX}refs/heads/{branch} {oid}");
        } else if name.starts_with("refs/tags/") {
            let _ = writeln!(updates, "update {PREFIX}{name} {oid}");
        }
    }
    if updates.is_empty() {
        return Ok(false);
    }
    let mut update = sanitized_git();
    update
        .arg("-C")
        .arg(repository)
        .args(["update-ref", "--stdin"])
        .stdin(Stdio::piped());
    let mut child = update.spawn().map_err(super::io)?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(updates.as_bytes())
            .await
            .map_err(super::io)?;
    }
    let output = child.wait_with_output().await.map_err(super::io)?;
    if !output.status.success() {
        return Err(Error::Internal(
            "node-git-failed: could not copy the origin refs".into(),
        ));
    }
    if let Some(head) = head {
        let mut symref = sanitized_git();
        symref.arg("-C").arg(repository).args([
            "symbolic-ref".to_owned(),
            format!("{PREFIX}HEAD"),
            format!("{PREFIX}{head}"),
        ]);
        let _ = run(symref).await;
    }
    Ok(true)
}

/// `git upload-pack` serving the namespace: only its refs are advertised,
/// and only advertised tips may be wanted.
#[must_use]
pub fn upload_pack_command(repository: &Path) -> Command {
    let mut command = sanitized_git();
    command
        .env("GIT_NAMESPACE", NAMESPACE)
        .args([
            "-c",
            "uploadpack.allowTipSHA1InWant=false",
            "-c",
            "uploadpack.allowReachableSHA1InWant=false",
            "-c",
            "uploadpack.allowAnySHA1InWant=false",
            "upload-pack",
        ])
        .arg(repository)
        .stdin(Stdio::piped())
        .stderr(Stdio::null());
    command
}

#[cfg(test)]
mod tests {
    use super::default_branch;

    #[test]
    fn the_default_branch_comes_from_the_head_symref_line() {
        assert_eq!(
            default_branch("ref: refs/heads/trunk\tHEAD\nabc\tHEAD\n"),
            Some("refs/heads/trunk")
        );
        assert_eq!(default_branch("abc\tHEAD\n"), None);
    }
}

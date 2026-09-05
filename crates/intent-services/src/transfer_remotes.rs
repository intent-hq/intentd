//! Root-repository remote state for workspace transfer
//! (intent-hq/intent#4438): which remotes the source checkout had, where its
//! remote-tracking refs pointed, and which upstream the workspace branch
//! tracked. Without it the import lands as a remote-less repository and
//! every commit on the branch reads as unpublished.
//!
//! What travels is a narrow, validated allowlist — remote names, ordered
//! fetch/push URLs and refspecs, direct `refs/remotes/<name>/*` tips (whose
//! objects ride the bundle), and workspace upstream/push selection —
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
//! target authenticates on its own. Query/fragment URLs, unsupported explicit
//! push destinations, refspecs and behavior-changing configuration fail with
//! value-free errors rather than silently changing addressing or enabling a
//! push fallback. Symbolic refs such as `refs/remotes/origin/HEAD` are not restored.
//! The restored tracking refs
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
    /// Ambiguous addressing may embed a credential; never strip or echo it.
    Ambiguous,
    /// The remote name is not a plain, safe name.
    Name,
}

impl std::fmt::Display for RemoteSkip {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Unportable => "URL is not a portable remote form",
            Self::Ambiguous => "URL query or fragment cannot be transferred safely",
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
    if url.contains(['?', '#']) {
        return Err(RemoteSkip::Ambiguous);
    }
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
        if !matches!(scheme.as_str(), "https" | "http" | "ssh" | "git") {
            return Err(RemoteSkip::Unportable);
        }
        let parsed = reqwest::Url::parse(url).map_err(|_| RemoteSkip::Unportable)?;
        if parsed.host_str().is_none() || parsed.cannot_be_a_base() {
            return Err(RemoteSkip::Unportable);
        }
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, ""),
        };
        let (userinfo, host) = match authority.rsplit_once('@') {
            Some((u, h)) => (Some(u), h),
            None => (None, authority),
        };
        // Reject encoded authority outright: URL parsers and Git/SSH differ
        // in when they decode it. In particular, encoded controls, separators
        // and leading options must not acquire meaning on the target.
        if authority.contains('%')
            || authority.matches('@').count() > 1
            || !safe_host_port(host)
            || userinfo.is_some_and(|u| !safe_user(u.split(':').next().unwrap_or("")))
        {
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
    let (user, host) = prefix
        .rsplit_once('@')
        .map_or((None, prefix), |(u, h)| (Some(u), h));
    if prefix.contains('/')
        || prefix.len() < 2
        || !safe_host_port(host)
        || prefix.contains('%')
        || user.is_some_and(|u| !safe_user(u))
        || prefix.starts_with('@')
        || path.is_empty()
    {
        return Err(RemoteSkip::Unportable);
    }
    Ok(url.to_string())
}

fn safe_user(user: &str) -> bool {
    !user.is_empty()
        && !user.starts_with('-')
        && user
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b'+' | b'~'))
}

fn safe_host_port(host: &str) -> bool {
    let (host, port) = if let Some(ipv6) = host.strip_prefix('[') {
        let Some((address, tail)) = ipv6.split_once(']') else {
            return false;
        };
        if address.parse::<std::net::Ipv6Addr>().is_err() {
            return false;
        }
        (
            address,
            if tail.is_empty() {
                None
            } else {
                let Some(port) = tail.strip_prefix(':') else {
                    return false;
                };
                Some(port)
            },
        )
    } else {
        let (host, port) = host
            .split_once(':')
            .map_or((host, None), |(h, p)| (h, Some(p)));
        if host.is_empty()
            || host.starts_with('-')
            || !host
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
        {
            return false;
        }
        (host, port)
    };
    !host.is_empty() && port.is_none_or(|p| !p.is_empty() && p.parse::<u16>().is_ok())
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

fn unsupported_config() -> Error {
    Error::Internal("remote transfer cannot safely preserve configured Git behavior; review remote URLs, refspecs and push selection on the source".into())
}

/// Read only an explicitly selected key, preserving order and empty values.
/// Never propagate libgit2 diagnostics, which may contain configuration data.
fn config_values(config: &git2::Config, key: &str) -> Result<Vec<String>> {
    let mut entries = match config.multivar(key, None) {
        Ok(entries) => entries,
        Err(e) if e.code() == git2::ErrorCode::NotFound => return Ok(Vec::new()),
        Err(_) => return Err(unsupported_config()),
    };
    let mut values = Vec::new();
    while let Some(entry) = entries.next() {
        let entry = entry.map_err(|_| unsupported_config())?;
        if !entry.has_value() {
            return Err(unsupported_config());
        }
        values.push(entry.value().map_err(|_| unsupported_config())?.to_string());
    }
    Ok(values)
}

fn config_last(config: &git2::Config, key: &str) -> Result<Option<String>> {
    Ok(config_values(config, key)?.pop())
}

/// Rewriting rules can change a portable literal's effective fetch/push
/// destination. They are not part of the allowlist, so reject applicable
/// rules without serializing their keys (which themselves may contain secrets).
fn reject_url_rewrites(config: &git2::Config, urls: &[String]) -> Result<()> {
    let mut entries = config
        .entries(Some(r"^url\..*\.(insteadof|pushinsteadof)$"))
        .map_err(|_| unsupported_config())?;
    while let Some(entry) = entries.next() {
        let entry = entry.map_err(|_| unsupported_config())?;
        if !entry.has_value() {
            return Err(unsupported_config());
        }
        let prefix = entry.value().map_err(|_| unsupported_config())?;
        if urls.iter().any(|url| url.starts_with(prefix)) {
            return Err(unsupported_config());
        }
    }
    Ok(())
}

/// Refspecs never select executable config or arbitrary local namespaces.
/// Unsupported forms are errors, not replacements with Git's defaults.
fn validate_refspec(spec: &str, remote: &str, push: bool) -> Result<()> {
    let valid_pattern = |name: &str, prefix: &str| {
        name.matches('*').count() <= 1
            && validate_ref_under(&name.replace('*', "wildcard"), prefix).is_ok()
    };
    let remote_source =
        |name: &str| valid_pattern(name, "refs/heads/") || valid_pattern(name, "refs/tags/");
    if !push {
        if let Some(negative) = spec.strip_prefix('^') {
            return if remote_source(negative) {
                Ok(())
            } else {
                Err(unsupported_config())
            };
        }
    }
    let spec = spec.strip_prefix('+').unwrap_or(spec);
    if push && spec == ":" {
        return Ok(());
    }
    let Some((source, destination)) = spec.split_once(':') else {
        return Err(unsupported_config());
    };
    let valid_source = remote_source(source) || (push && (source == "HEAD" || source.is_empty()));
    let valid_destination = if push {
        remote_source(destination)
    } else {
        valid_pattern(destination, &format!("refs/remotes/{remote}/"))
    };
    if valid_source
        && valid_destination
        && source.matches('*').count() == destination.matches('*').count()
    {
        Ok(())
    } else {
        Err(unsupported_config())
    }
}

fn validate_push_default(value: &str) -> Result<()> {
    if matches!(
        value,
        "nothing" | "current" | "upstream" | "tracking" | "simple" | "matching"
    ) {
        Ok(())
    } else {
        Err(unsupported_config())
    }
}

type PushSelection = (Option<String>, Option<String>, Option<String>);

pub(crate) fn capture_push_selection(
    repo: &git2::Repository,
    branch: &str,
    remotes: &[RemoteBundleRef],
) -> Result<PushSelection> {
    let config = repo.config().map_err(|_| unsupported_config())?;
    let branch_remote = config_last(&config, &format!("branch.{branch}.pushRemote"))?;
    let default_remote = config_last(&config, "remote.pushDefault")?;
    for name in branch_remote.iter().chain(default_remote.iter()) {
        if !remotes.iter().any(|r| &r.name == name) {
            return Err(unsupported_config());
        }
    }
    let push_default = config_last(&config, "push.default")?;
    if let Some(value) = &push_default {
        validate_push_default(value)?;
    }
    Ok((branch_remote, default_remote, push_default))
}

/// Export side: the remotes of `repo` that pass the allowlist, each with its
/// direct remote-tracking tips, plus the workspace branch's upstream when it
/// names one of those remotes. Skips are logged by remote name only.
pub(crate) fn capture_remotes(
    repo: &git2::Repository,
    workspace_branch: &str,
) -> Result<(Vec<RemoteBundleRef>, Option<BranchUpstream>)> {
    let names = repo.remotes().map_err(|_| unsupported_config())?;
    let config = repo.config().map_err(|_| unsupported_config())?;
    let mut remotes = Vec::new();
    for name in names.iter().flatten().flatten() {
        if validate_remote_name(name).is_err() {
            return Err(unsupported_config());
        }
        let key = |suffix: &str| format!("remote.{name}.{suffix}");
        let urls = config_values(&config, &key("url"))?;
        let push_urls = config_values(&config, &key("pushurl"))?;
        reject_url_rewrites(&config, &urls)?;
        reject_url_rewrites(&config, &push_urls)?;
        let mut sanitized = Vec::new();
        for url in &urls {
            match sanitize_remote_url(url) {
                Ok(url) => sanitized.push(url),
                Err(RemoteSkip::Ambiguous) => return Err(unsupported_config()),
                Err(_) => {}
            }
        }
        // A wholly unportable remote can be omitted conservatively. Never
        // drop just one URL from a portable multi-destination configuration.
        if sanitized.is_empty() {
            // Dropping a configured push destination can change selection of
            // the default push remote, even if this fetch URL is unportable.
            if !push_urls.is_empty() {
                return Err(unsupported_config());
            }
            tracing::warn!(remote = %name, "transfer bundle: unportable remote not transferred");
            continue;
        }
        if sanitized.len() != urls.len() {
            return Err(unsupported_config());
        }
        let mut sanitized = sanitized.into_iter();
        let url = sanitized.next().ok_or_else(unsupported_config)?;
        let mut push_urls = push_urls
            .iter()
            .map(|u| sanitize_remote_url(u).map_err(|_| unsupported_config()))
            .collect::<Result<Vec<_>>>()?
            .into_iter();
        let push_url = push_urls.next();
        let fetch_refspecs = config_values(&config, &key("fetch"))?;
        let push_refspecs = config_values(&config, &key("push"))?;
        for spec in &fetch_refspecs {
            validate_refspec(spec, name, false)?;
        }
        for spec in &push_refspecs {
            validate_refspec(spec, name, true)?;
        }
        // These settings change fetch/push behavior or invoke external code.
        // Do not copy them and do not silently restore different semantics.
        for suffix in [
            "mirror",
            "receivepack",
            "uploadpack",
            "vcs",
            "proxy",
            "tagopt",
            "prune",
            "prunetags",
            "promisor",
            "partialclonefilter",
            "skipdefaultupdate",
            "skipfetchall",
        ] {
            if !config_values(&config, &key(suffix))?.is_empty() {
                return Err(unsupported_config());
            }
        }
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
                bundle_ref: None,
            });
        }
        remotes.push(RemoteBundleRef {
            name: name.to_string(),
            url,
            push_url,
            additional_urls: sanitized.collect(),
            additional_push_urls: push_urls.collect(),
            fetch_refspecs: Some(fetch_refspecs),
            push_refspecs,
            tracking_refs,
        });
    }

    let branch_remote = config_last(&config, &format!("branch.{workspace_branch}.remote"))?;
    let merge_refs = config_values(&config, &format!("branch.{workspace_branch}.merge"))?;
    let upstream = if let Some(remote) = branch_remote {
        if remote == "." {
            return Err(unsupported_config());
        }
        if remotes.iter().any(|r| r.name == remote) {
            for merge_ref in &merge_refs {
                validate_ref_under(merge_ref, "refs/heads/").map_err(|_| unsupported_config())?;
            }
            let mut merge_refs = merge_refs.into_iter();
            Some(BranchUpstream {
                remote,
                merge_ref: merge_refs.next().ok_or_else(unsupported_config)?,
                additional_merge_refs: merge_refs.collect(),
            })
        } else if names.iter().flatten().flatten().any(|name| name == remote) {
            None
        } else {
            return Err(unsupported_config());
        }
    } else if merge_refs.is_empty() {
        None
    } else {
        return Err(unsupported_config());
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
    validate_ref_under(
        &format!("refs/heads/{}", refs.workspace_branch),
        "refs/heads/",
    )
    .map_err(|_| unsupported_config())?;
    for remote in &refs.remotes {
        let name = remote.name.as_str();
        validate_remote_name(name).map_err(|_| unsupported_config())?;
        if !seen.insert(name) {
            return Err(unsupported_config());
        }
        let url = sanitize_remote_url(&remote.url).map_err(|_| unsupported_config())?;
        let push_url = remote
            .push_url
            .as_deref()
            .map(sanitize_remote_url)
            .transpose()
            .map_err(|_| unsupported_config())?;
        let additional_urls = remote
            .additional_urls
            .iter()
            .map(|u| sanitize_remote_url(u).map_err(|_| unsupported_config()))
            .collect::<Result<Vec<_>>>()?;
        let additional_push_urls = remote
            .additional_push_urls
            .iter()
            .map(|u| sanitize_remote_url(u).map_err(|_| unsupported_config()))
            .collect::<Result<Vec<_>>>()?;
        if push_url.is_none() && !additional_push_urls.is_empty() {
            return Err(unsupported_config());
        }
        if let Some(specs) = &remote.fetch_refspecs {
            for spec in specs {
                validate_refspec(spec, name, false)?;
            }
        }
        for spec in &remote.push_refspecs {
            validate_refspec(spec, name, true)?;
        }
        let prefix = format!("refs/remotes/{name}/");
        let mut tracking_names = std::collections::HashSet::new();
        for t in &remote.tracking_refs {
            validate_ref_under(&t.ref_name, &prefix).map_err(|_| unsupported_config())?;
            if !tracking_names.insert(&t.ref_name) {
                return Err(unsupported_config());
            }
            if let Some(anchor) = &t.bundle_ref {
                validate_ref_under(anchor, "refs/intent/transfer/")
                    .map_err(|_| unsupported_config())?;
            }
            if !is_full_sha(&t.sha) {
                return Err(unsupported_config());
            }
            if wip_shas.contains(&t.sha.as_str()) {
                return Err(unsupported_config());
            }
        }

        run_git(checkout_dir, |cmd| {
            cmd.args(["remote", "add", name, &url]);
        })
        .map_err(|_| unsupported_config())?;
        if let Some(specs) = &remote.fetch_refspecs {
            // `remote add` installed an unrestricted default. Remove it even
            // when the source intentionally had zero fetch mappings.
            run_git(checkout_dir, |cmd| {
                cmd.args([
                    "config",
                    "--local",
                    "--unset-all",
                    &format!("remote.{name}.fetch"),
                ]);
            })
            .map_err(|_| unsupported_config())?;
            for spec in specs {
                add_config_value(checkout_dir, &format!("remote.{name}.fetch"), spec)?;
            }
        }
        for url in &additional_urls {
            add_config_value(checkout_dir, &format!("remote.{name}.url"), url)?;
        }
        for url in push_url.iter().chain(additional_push_urls.iter()) {
            add_config_value(checkout_dir, &format!("remote.{name}.pushurl"), url)?;
        }
        for spec in &remote.push_refspecs {
            add_config_value(checkout_dir, &format!("remote.{name}.push"), spec)?;
        }
        if remote.tracking_refs.is_empty() {
            continue;
        }
        run_git(checkout_dir, |cmd| {
            cmd.args([
                "fetch",
                "--no-tags",
                "--no-write-fetch-head",
                "--quiet",
                bundle,
            ]);
            cmd.args(remote.tracking_refs.iter().map(|t| {
                format!(
                    "+{}:{}",
                    t.bundle_ref.as_deref().unwrap_or(&t.ref_name),
                    t.ref_name
                )
            }));
        })
        .map_err(|_| unsupported_config())?;
        let repo = git2::Repository::open(checkout_dir).map_err(|_| unsupported_config())?;
        for t in &remote.tracking_refs {
            let actual = repo
                .find_reference(&t.ref_name)
                .ok()
                .and_then(|r| r.target())
                .map(|o| o.to_string());
            if actual.as_deref() != Some(t.sha.as_str()) {
                return Err(unsupported_config());
            }
        }
    }

    if let Some(up) = &refs.workspace_upstream {
        if !seen.contains(up.remote.as_str()) {
            return Err(unsupported_config());
        }
        validate_ref_under(&up.merge_ref, "refs/heads/").map_err(|_| unsupported_config())?;
        for merge_ref in &up.additional_merge_refs {
            validate_ref_under(merge_ref, "refs/heads/").map_err(|_| unsupported_config())?;
        }
        let branch = &refs.workspace_branch;
        run_git(checkout_dir, |cmd| {
            cmd.args(["config", &format!("branch.{branch}.remote"), &up.remote]);
        })
        .map_err(|_| unsupported_config())?;
        run_git(checkout_dir, |cmd| {
            cmd.args(["config", &format!("branch.{branch}.merge"), &up.merge_ref]);
        })
        .map_err(|_| unsupported_config())?;
        for merge_ref in &up.additional_merge_refs {
            add_config_value(checkout_dir, &format!("branch.{branch}.merge"), merge_ref)?;
        }
    }
    for (key, remote) in [
        (
            format!("branch.{}.pushRemote", refs.workspace_branch),
            &refs.workspace_push_remote,
        ),
        ("remote.pushDefault".into(), &refs.remote_push_default),
    ] {
        if let Some(remote) = remote {
            if !seen.contains(remote.as_str()) {
                return Err(unsupported_config());
            }
            add_config_value(checkout_dir, &key, remote)?;
        }
    }
    if let Some(value) = &refs.push_default {
        validate_push_default(value)?;
        add_config_value(checkout_dir, "push.default", value)?;
    }
    Ok(())
}

fn add_config_value(checkout: &Path, key: &str, value: &str) -> Result<()> {
    run_git(checkout, |cmd| {
        cmd.args(["config", "--local", "--add", key, value]);
    })
    .map_err(|_| unsupported_config())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn review_r1_rejects_ambiguous_urls_and_decoded_authority_controls() {
        for url in [
            "https://example.invalid/repo?access_token=PUBLIC_TEST_MARKER",
            "https://example.invalid/repo#PUBLIC_TEST_MARKER",
            "ssh://git@example.invalid/repo?PUBLIC_TEST_MARKER",
            "https://example.invalid?PUBLIC_TEST_MARKER",
            "ssh://%2doption@example.invalid/repo",
            "ssh://git@%2dhost/repo",
            "ssh://git%0auser@example.invalid/repo",
            "https://user%00name@example.invalid/repo",
            "ssh://git@host%09name/repo",
            "ssh://git@host:invalid/repo",
            "git@host:repo#PUBLIC_TEST_MARKER",
            "-option@host:repo",
        ] {
            assert!(sanitize_remote_url(url).is_err(), "unsafe URL accepted");
        }
    }

    #[test]
    fn review_r2_explicit_unsupported_push_destination_fails_export() {
        for push in [
            "DISABLED",
            "/local/repo.git",
            "ext::false",
            "file:///local/repo",
            "",
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let repo = git2::Repository::init(tmp.path()).unwrap();
            repo.remote("origin", "https://example.invalid/repo")
                .unwrap();
            repo.config()
                .unwrap()
                .set_str("remote.origin.pushurl", push)
                .unwrap();
            assert!(
                capture_remotes(&repo, "main").is_err(),
                "must not enable push fallback"
            );
        }
    }

    #[test]
    fn review_r3_unsupported_config_is_not_silently_replaced() {
        for (key, value) in [
            ("remote.origin.fetch", "+refs/heads/*:refs/heads/*"),
            (
                "remote.origin.fetch",
                "^refs/heads/main:refs/remotes/origin/main",
            ),
            ("remote.origin.fetch", "+refs/heads/*:refs/remotes/other/*"),
            ("remote.origin.push", "HEAD~1:refs/heads/main"),
            ("remote.origin.push", "refs/heads/main:refs/intent/unsafe"),
            ("remote.origin.mirror", "true"),
            ("remote.origin.uploadpack", "PUBLIC_TEST_MARKER"),
            ("remote.origin.receivepack", "PUBLIC_TEST_MARKER"),
            ("remote.origin.tagopt", "--no-tags"),
            ("branch.main.remote", "."),
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let repo = git2::Repository::init(tmp.path()).unwrap();
            repo.remote("origin", "https://example.invalid/repo")
                .unwrap();
            repo.config().unwrap().set_str(key, value).unwrap();
            let error = capture_remotes(&repo, "main").unwrap_err();
            assert!(!error.to_string().contains("PUBLIC_TEST_MARKER"));
        }
    }

    #[test]
    fn review_r2_r3_mixed_url_lists_fail_without_dropping_destinations() {
        for suffix in ["url", "pushurl"] {
            let tmp = tempfile::tempdir().unwrap();
            let repo = git2::Repository::init(tmp.path()).unwrap();
            repo.remote("origin", "https://example.invalid/repo")
                .unwrap();
            let key = format!("remote.origin.{suffix}");
            add_config_value(tmp.path(), &key, "https://example.invalid/safe").unwrap();
            add_config_value(tmp.path(), &key, "DISABLED").unwrap();
            assert!(capture_remotes(&repo, "main").is_err());
        }
    }

    #[test]
    fn review_r3_refspec_allowlist_preserves_safe_forms_only() {
        for spec in [
            "+refs/heads/*:refs/remotes/origin/*",
            "refs/heads/main:refs/remotes/origin/published",
            "^refs/heads/private/*",
            "^refs/heads/excluded",
            "refs/tags/v*:refs/remotes/origin/tags/v*",
        ] {
            assert!(validate_refspec(spec, "origin", false).is_ok());
        }
        for spec in [
            "refs/heads/main:refs/heads/main",
            "+refs/heads/*:refs/heads/*",
            "HEAD:refs/heads/review",
            "refs/tags/v1:refs/tags/v1",
            ":refs/heads/old",
            ":",
            "+:",
        ] {
            assert!(validate_refspec(spec, "origin", true).is_ok());
        }
        for spec in [
            "",
            "--upload-pack=PUBLIC_TEST_MARKER",
            "+^refs/heads/x",
            "refs/heads/*:refs/remotes/origin/plain",
            "refs/heads/*/*:refs/remotes/origin/*/*",
            "refs/heads/main:refs/remotes/origin/../main",
        ] {
            assert!(validate_refspec(spec, "origin", false).is_err());
        }
    }

    #[test]
    fn review_r3_unrestorable_push_selection_fails_export() {
        for key in [
            "branch.main.pushRemote",
            "remote.pushDefault",
            "push.default",
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let repo = git2::Repository::init(tmp.path()).unwrap();
            repo.remote("origin", "https://example.invalid/repo")
                .unwrap();
            let (remotes, _) = capture_remotes(&repo, "main").unwrap();
            repo.config()
                .unwrap()
                .set_str(key, "PUBLIC_TEST_MARKER")
                .unwrap();
            let error = capture_push_selection(&repo, "main", &remotes).unwrap_err();
            assert!(!error.to_string().contains("PUBLIC_TEST_MARKER"));
        }
    }

    #[test]
    fn review_r3_matching_url_rewrites_fail_without_copying_config() {
        for suffix in ["insteadOf", "pushInsteadOf"] {
            let tmp = tempfile::tempdir().unwrap();
            let repo = git2::Repository::init(tmp.path()).unwrap();
            repo.remote("origin", "https://example.invalid/repo")
                .unwrap();
            repo.config()
                .unwrap()
                .set_str(
                    &format!("url.ssh://git@example.invalid/PUBLIC_TEST_MARKER.{suffix}"),
                    "https://example.invalid/",
                )
                .unwrap();
            assert!(
                capture_remotes(&repo, "main").is_err(),
                "cannot drop an effective URL rewrite"
            );
        }
    }

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

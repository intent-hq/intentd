//! Read-only repository context production from effective local Git state.
//!
//! The caller supplies already-admitted roots, authority, connection facts and
//! stored-choice provenance. Neither a path nor a DTO establishes permission.
//! Private root observations precede target enrichment and carry no admission
//! or revision. Run this blocking reader off the async request path. It registers
//! no RPC, watcher or cache and performs no credential lookup, Git write or remote call.
//!
//! Qualified project resolution remains provider-owned. The callback receives
//! the original effective URL, including port/prefix/escapes; the legacy Git URL
//! parser is only a syntax filter, never a qualified identity source.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

use base64::Engine as _;
use intent_core::{
    resolve_review_selection, Error, ExecutionScope, GitRemoteUrl, RepositoryContext,
    RepositoryContextRevision, RepositoryEndpointResolution, RepositoryProvider, RepositoryRemote,
    RepositoryRemoteEndpoint, RepositoryRootContext, RepositoryRootId, RepositoryTarget,
    RepositoryTargetContext, RepositoryUnresolvedReason, Result, SavedReviewSelection,
};
use intent_sourcecontrol::remote_project::{
    CanonicalRemoteResolver, RemoteProvider, UnresolvedRemote,
};
use sha2::{Digest, Sha256};

const CONFIG_KEYS: &str = r"^(remote\..*\.(url|pushurl|partialclonefilter)|url\..*\.(insteadof|pushinsteadof)|include\.path|includeif\..*\.path|extensions\.worktreeconfig|core\.(bare|worktree))$";

/// Inputs supplied by the owner of current admission and revision sequencing.
///
/// The owner must advance revision when context/change inputs differ and must
/// revalidate authority before delivery. This reader does not assign revisions.
pub struct RepositoryContextInput {
    pub scope: ExecutionScope,
    pub revision: RepositoryContextRevision,
    pub roots: Vec<AdmittedRepositoryRoot>,
}

/// Explicit root, saved selection and admitted target facts; not an auth token.
pub struct AdmittedRepositoryRoot {
    pub root: RepositoryRootId,
    pub path: PathBuf,
    pub saved_selection: SavedReviewSelection,
    pub explicit_target: Option<RepositoryTarget>,
    pub targets: Vec<RepositoryTargetContext>,
}

/// Optional explicit Git config environment, also useful for isolated fixtures.
///
/// None inherits Git's effective environment. Extra paths let the owner include
/// absent system/global configuration candidates in future change observation.
/// Environment/authority changes require an owner-driven refresh as well.
#[derive(Default)]
pub struct GitConfigEnvironment {
    pub global_config: Option<PathBuf>,
    pub system_config: Option<PathBuf>,
    pub extra_config_paths: Vec<PathBuf>,
}

/// A fresh context and inputs for later watcher/revision integration.
pub struct RepositoryContextRead {
    pub context: RepositoryContext,
    pub change_inputs: Vec<RepositoryChangeInputs>,
    /// Internal original transport facts. Never serialize or log this member.
    pub(crate) private_roots: Vec<RepositoryPrivateRoot>,
}

/// Local facts before target enrichment. Resolved projects remain observations,
/// not selected targets or supplied permission, account or connection facts.
/// Deliberately neither Debug nor Serde because it retains private transports.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct RepositoryObservedRoot {
    pub root: RepositoryRootId,
    pub branch: Option<String>,
    pub head_sha: Option<String>,
    pub remotes: Vec<RepositoryRemote>,
    pub change_inputs: RepositoryChangeInputs,
    pub private_root: RepositoryPrivateRoot,
    /// Config prefix of the existing mixed fingerprint; private continuity only.
    pub config_fingerprint: String,
}

/// Original effective values from the same consistency-checked Git read.
/// Deliberately neither Debug nor Serde. These observations grant no authority.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct RepositoryPrivateRoot {
    pub root: RepositoryRootId,
    /// Exact symbolic ref; the public shortened branch name is display only.
    pub source_ref: Option<String>,
    pub remotes: Vec<RepositoryPrivateRemote>,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct RepositoryPrivateRemote {
    pub name: String,
    pub fetch: Vec<String>,
    pub push: Vec<String>,
}

/// Local change sources, not a claim that watchers are installed or exhaustive
/// across process-environment changes. Paths may be absent (watch their parent).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryChangeInputs {
    pub root: RepositoryRootId,
    pub git_entry: PathBuf,
    pub git_dir: PathBuf,
    pub common_dir: PathBuf,
    pub config_files: Vec<PathBuf>,
    pub head_paths: Vec<PathBuf>,
    /// Fingerprint of effective relevant config and actual HEAD observations.
    /// Contains no raw config values; it is not an authority/revision token.
    pub fingerprint: String,
}

/// Read each exact admitted worktree without discovering a replacement root.
///
/// # Errors
/// Fails closed on invalid roots/config, changed reads, duplicate roots/target
/// facts, or a resolved project for which no explicit admitted facts were supplied.
/// Never substitutes a previous/default context after a read failure.
pub fn read_repository_context(
    input: &RepositoryContextInput,
    resolve: &impl Fn(&str) -> RepositoryEndpointResolution,
    environment: &GitConfigEnvironment,
) -> Result<RepositoryContextRead> {
    let mut seen = Vec::new();
    let mut roots = Vec::new();
    let mut change_inputs = Vec::new();
    let mut private_roots = Vec::new();
    for root in &input.roots {
        if seen.contains(&root.root) {
            return Err(invalid("duplicate admitted root"));
        }
        seen.push(root.root.clone());
        let (context, changes, private) = read_root(root, resolve, environment)?;
        roots.push(context);
        change_inputs.push(changes);
        private_roots.push(private);
    }
    Ok(RepositoryContextRead {
        context: RepositoryContext {
            revision: input.revision.clone(),
            scope: input.scope.clone(),
            roots,
        },
        change_inputs,
        private_roots,
    })
}

/// Read using the provider-owned canonical resolver without duplicating its
/// transport mappings or treating the resulting identity as authorization.
///
/// # Errors
/// Has the same local read/admitted-fact requirements as [`read_repository_context`].
pub fn read_repository_context_with_resolver(
    input: &RepositoryContextInput,
    resolver: &CanonicalRemoteResolver,
    environment: &GitConfigEnvironment,
) -> Result<RepositoryContextRead> {
    read_repository_context(input, &|url| resolve_endpoint(resolver, url), environment)
}

/// Observe one exact local root without inventing admission, selection or a
/// context revision. Uses the same Git snapshot and consistency checks as the
/// public admitted-context reader; private transport values stay in-process.
#[expect(
    clippy::allow_attributes,
    reason = "This private API is used by the external fixture but not the library targets"
)]
#[allow(
    dead_code,
    reason = "Private read observation consumers are not integrated yet"
)]
pub(crate) fn observe_repository_root_with_resolver(
    root: &RepositoryRootId,
    path: &Path,
    resolver: &CanonicalRemoteResolver,
    environment: &GitConfigEnvironment,
) -> Result<RepositoryObservedRoot> {
    read_root_with(
        root,
        path,
        &|url| resolve_endpoint(resolver, url),
        environment,
        |_| Ok(()),
    )
    .map(|(observed, ())| observed)
}

/// Deterministic local Git mutation after observation, before the SAME final
/// consistency check. This schedules a test writer; it supplies no source facts.
#[cfg(test)]
#[expect(
    clippy::allow_attributes,
    reason = "This hook is used by the external fixture but not the library test target"
)]
#[allow(dead_code, reason = "Used by the external reader fixture target")]
pub(crate) fn observe_repository_root_before_check(
    root: &RepositoryRootId,
    path: &Path,
    resolver: &CanonicalRemoteResolver,
    environment: &GitConfigEnvironment,
    before_check: impl FnOnce(),
) -> Result<RepositoryObservedRoot> {
    read_root_with(
        root,
        path,
        &|url| resolve_endpoint(resolver, url),
        environment,
        |_| {
            before_check();
            Ok(())
        },
    )
    .map(|(observed, ())| observed)
}

/// Enrich a fresh unstamped observation before the SAME final Git/config
/// consistency check. Target facts come only from the supplied actual owner.
pub(crate) fn read_context_root_with_resolver(
    root: &RepositoryRootId,
    path: &Path,
    saved: &SavedReviewSelection,
    explicit: Option<&RepositoryTarget>,
    resolver: &CanonicalRemoteResolver,
    environment: &GitConfigEnvironment,
    enrich: impl Fn(&RepositoryTarget) -> RepositoryTargetContext,
) -> Result<(RepositoryRootContext, RepositoryObservedRoot)> {
    let (observed, (selection, targets)) = read_root_with(
        root,
        path,
        &|url| resolve_endpoint(resolver, url),
        environment,
        |remotes| {
            let selection = resolve_review_selection(saved, remotes, explicit);
            let mut targets = BTreeSet::new();
            for endpoint in remotes.iter().flat_map(|r| r.fetch.iter().chain(&r.push)) {
                if let RepositoryEndpointResolution::Resolved { target } = &endpoint.resolution {
                    targets.insert(target.clone());
                }
            }
            if let intent_core::ReviewSelectionOutcome::Resolved { target, .. } = &selection.outcome
            {
                targets.insert(target.clone());
            }
            Ok((selection, targets.iter().map(enrich).collect()))
        },
    )?;
    let context = RepositoryRootContext {
        root: observed.root.clone(),
        branch: observed.branch.clone(),
        head_sha: observed.head_sha.clone(),
        remotes: observed.remotes.clone(),
        targets,
        review_selection: selection,
    };
    Ok((context, observed))
}

fn resolve_endpoint(resolver: &CanonicalRemoteResolver, url: &str) -> RepositoryEndpointResolution {
    match resolver.resolve(url) {
        Ok(project) => RepositoryEndpointResolution::Resolved {
            target: RepositoryTarget {
                provider: match project.provider {
                    RemoteProvider::Github => RepositoryProvider::Github,
                    RemoteProvider::Gitlab => RepositoryProvider::Gitlab,
                },
                instance_base_url: project.instance_base_url,
                project_path: project.project_path,
            },
        },
        Err(reason) => RepositoryEndpointResolution::Unresolved {
            reason: match reason {
                UnresolvedRemote::UnknownInstance => RepositoryUnresolvedReason::UnknownInstance,
                UnresolvedRemote::UnsupportedTransport => {
                    RepositoryUnresolvedReason::UnsupportedTransport
                }
                UnresolvedRemote::AmbiguousMapping => RepositoryUnresolvedReason::AmbiguousMapping,
                UnresolvedRemote::InvalidRemote => RepositoryUnresolvedReason::InvalidRemote,
            },
        },
    }
}

fn invalid(message: &str) -> Error {
    Error::Internal(format!("repository context: {message}"))
}

struct GitRead<'a> {
    path: &'a Path,
    environment: &'a GitConfigEnvironment,
}

impl GitRead<'_> {
    fn run(&self, args: &[&str], allow_absent: bool) -> Result<String> {
        let mut command = Command::new("git");
        command.arg("-C").arg(self.path).args(args);
        // A caller's cwd/GIT_DIR must never redirect an admitted root read.
        for key in [
            "GIT_DIR",
            "GIT_WORK_TREE",
            "GIT_COMMON_DIR",
            "GIT_INDEX_FILE",
            "GIT_CONFIG",
        ] {
            command.env_remove(key);
        }
        command.env("GIT_OPTIONAL_LOCKS", "0");
        command.env("LC_ALL", "C");
        if let Some(path) = &self.environment.global_config {
            command.env("GIT_CONFIG_GLOBAL", path);
        }
        if let Some(path) = &self.environment.system_config {
            command.env("GIT_CONFIG_SYSTEM", path);
        }
        let output = command
            .output()
            .map_err(|_| invalid("Git read could not start"))?;
        if !(output.status.success() || allow_absent && output.status.code() == Some(1)) {
            // Git stderr can contain private URLs/config. Do not reflect it.
            return Err(invalid("Git read failed"));
        }
        String::from_utf8(output.stdout).map_err(|_| invalid("non-UTF8 Git read"))
    }

    fn line(&self, args: &[&str], allow_absent: bool) -> Result<Option<String>> {
        let value = self.run(args, allow_absent)?;
        let value = value.trim_end_matches('\n');
        if value.contains(['\n', '\r', '\0']) {
            return Err(invalid("ambiguous Git line"));
        }
        Ok((!value.is_empty()).then(|| value.to_owned()))
    }

    fn required(&self, args: &[&str]) -> Result<String> {
        self.line(args, false)?
            .ok_or_else(|| invalid("missing Git path"))
    }

    fn config(&self) -> Result<ConfigRead> {
        let origins = self.run(
            &[
                "config",
                "--null",
                "--show-origin",
                "--name-only",
                "--includes",
                "--list",
            ],
            false,
        )?;
        let relevant = self.run(
            &[
                "config",
                "--null",
                "--show-origin",
                "--includes",
                "--get-regexp",
                CONFIG_KEYS,
            ],
            true,
        )?;
        Ok(ConfigRead { origins, relevant })
    }

    fn head(&self) -> Result<(Option<String>, Option<String>)> {
        Ok((
            self.line(&["symbolic-ref", "--quiet", "--short", "HEAD"], true)?,
            self.line(&["rev-parse", "--verify", "--quiet", "HEAD"], true)?,
        ))
    }

    fn fetch_url(&self, url: &str, remote_names: &BTreeSet<String>) -> Result<String> {
        // --get-url exits before transport setup. Unlike remote get-url, it
        // also supports a URL supplied only through global/system includes.
        // Reject the unusual remote-name collision rather than resolving it
        // as another named remote instead of the configured URL itself.
        if remote_names.contains(url) {
            return Err(invalid("configured URL collides with a remote name"));
        }
        self.required(&["ls-remote", "--get-url", "--", url])
    }

    fn push_urls(&self) -> Result<BTreeMap<String, Vec<String>>> {
        // Git emits every effective push URL, including pushInsteadOf and
        // fetch fallback, for all config scopes. Relevant values (including
        // the optional fetch filter suffix) are control-checked beforehand.
        let mut remotes: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for line in self.run(&["remote", "--verbose"], false)?.lines() {
            let (name, value) = line
                .split_once('\t')
                .ok_or_else(|| invalid("invalid remote inventory"))?;
            if let Some(url) = value.strip_suffix(" (push)") {
                remotes.entry(name.into()).or_default().push(url.into());
            }
        }
        Ok(remotes)
    }
}

#[derive(PartialEq, Eq)]
struct ConfigRead {
    origins: String,
    relevant: String,
}

fn nul_pairs(value: &str) -> Result<Vec<(&str, &str)>> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    if !value.ends_with('\0') {
        return Err(invalid("incomplete config records"));
    }
    let fields: Vec<_> = value[..value.len() - 1].split('\0').collect();
    if !fields.len().is_multiple_of(2) {
        return Err(invalid("invalid config records"));
    }
    Ok(fields.chunks_exact(2).map(|p| (p[0], p[1])).collect())
}

fn file_origin(origin: &str, worktree: &Path) -> Option<PathBuf> {
    origin.strip_prefix("file:").map(|file| {
        let path = PathBuf::from(file);
        if path.is_absolute() {
            path
        } else {
            worktree.join(path)
        }
    })
}

fn read_root(
    input: &AdmittedRepositoryRoot,
    resolve: &impl Fn(&str) -> RepositoryEndpointResolution,
    environment: &GitConfigEnvironment,
) -> Result<(
    RepositoryRootContext,
    RepositoryChangeInputs,
    RepositoryPrivateRoot,
)> {
    let (observed, (selection, targets)) =
        read_root_with(&input.root, &input.path, resolve, environment, |remotes| {
            let selection = resolve_review_selection(
                &input.saved_selection,
                remotes,
                input.explicit_target.as_ref(),
            );
            let mut required_targets = BTreeSet::new();
            for endpoint in remotes.iter().flat_map(|r| r.fetch.iter().chain(&r.push)) {
                if let RepositoryEndpointResolution::Resolved { target } = &endpoint.resolution {
                    required_targets.insert(target.clone());
                }
            }
            if let intent_core::ReviewSelectionOutcome::Resolved { target, .. } = &selection.outcome
            {
                required_targets.insert(target.clone());
            }
            let mut supplied = BTreeMap::new();
            for target in &input.targets {
                if supplied.insert(target.target.clone(), target).is_some() {
                    return Err(invalid("duplicate target facts"));
                }
            }
            let targets = required_targets
                .into_iter()
                .map(|target| {
                    supplied
                        .get(&target)
                        .map(|v| (*v).clone())
                        .ok_or_else(|| invalid("resolved project lacks admitted target facts"))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((selection, targets))
        })?;
    Ok((
        RepositoryRootContext {
            root: observed.root,
            branch: observed.branch,
            head_sha: observed.head_sha,
            remotes: observed.remotes,
            targets,
            review_selection: selection,
        },
        observed.change_inputs,
        observed.private_root,
    ))
}

/// The continuation enriches the already-read remotes before the original
/// final consistency check. Its failure keeps the public reader's existing
/// error precedence; successful enrichment never starts a second Git read.
fn read_root_with<T>(
    root: &RepositoryRootId,
    path: &Path,
    resolve: &impl Fn(&str) -> RepositoryEndpointResolution,
    environment: &GitConfigEnvironment,
    enrich: impl FnOnce(&[RepositoryRemote]) -> Result<T>,
) -> Result<(RepositoryObservedRoot, T)> {
    let path = path
        .canonicalize()
        .map_err(|_| invalid("root is unavailable"))?;
    let git = GitRead {
        path: &path,
        environment,
    };
    let top = PathBuf::from(git.required(&["rev-parse", "--show-toplevel"])?);
    if top
        .canonicalize()
        .map_err(|_| invalid("root is unavailable"))?
        != path
    {
        return Err(invalid("admitted path is no longer the repository root"));
    }
    let git_dir_args = ["rev-parse", "--path-format=absolute", "--git-dir"];
    let common_dir_args = ["rev-parse", "--path-format=absolute", "--git-common-dir"];
    let git_dir = PathBuf::from(git.required(&git_dir_args)?);
    let common_dir = PathBuf::from(git.required(&common_dir_args)?);
    let before = git.config()?;
    let head = git.head()?;
    let source_ref = git.line(&["symbolic-ref", "--quiet", "HEAD"], true)?;
    let mut config_files =
        BTreeSet::from([common_dir.join("config"), git_dir.join("config.worktree")]);
    config_files.extend(environment.extra_config_paths.iter().cloned());
    config_files.extend(environment.global_config.iter().cloned());
    config_files.extend(environment.system_config.iter().cloned());
    let mut remote_names = BTreeSet::new();
    for (origin, key) in nul_pairs(&before.origins)? {
        if key.chars().any(char::is_control) {
            return Err(invalid("control character in config key"));
        }
        if let Some((name, _)) = key.strip_prefix("remote.").and_then(|v| v.rsplit_once('.')) {
            remote_names.insert(name.to_owned());
        }
        if let Some(path) = file_origin(origin, &path) {
            config_files.insert(path);
        }
    }
    let mut fetch_configured: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (origin, entry) in nul_pairs(&before.relevant)? {
        let (key, value) = entry
            .split_once('\n')
            .ok_or_else(|| invalid("invalid config entry"))?;
        // get-url emits line records; control characters could masquerade as
        // extra URLs. Reject before consuming rewrites or projecting endpoints.
        if value.chars().any(char::is_control) || key.chars().any(char::is_control) {
            return Err(invalid("control character in relevant Git config"));
        }
        if let Some(key) = key.strip_prefix("remote.") {
            if let Some(name) = key.strip_suffix(".url") {
                fetch_configured
                    .entry(name.to_owned())
                    .or_default()
                    .push(value.to_owned());
            }
        }
        if (key == "include.path"
            || key.starts_with("includeif.")
                && key.rsplit_once('.').is_some_and(|(_, name)| name == "path"))
            && !value.is_empty()
        {
            let include = intent_core::expand_tilde(value);
            let resolved = if include.is_absolute() {
                include
            } else {
                file_origin(origin, &path)
                    .and_then(|p| p.parent().map(|p| p.join(&include)))
                    .unwrap_or_else(|| path.join(include))
            };
            config_files.insert(resolved);
        }
    }
    let mut remotes = Vec::new();
    let mut private_remotes = Vec::new();
    let mut push_urls = git.push_urls()?;
    for name in &remote_names {
        let fetch = fetch_configured
            .get(name)
            .into_iter()
            .flatten()
            .map(|url| git.fetch_url(url, &remote_names))
            .collect::<Result<Vec<_>>>()?;
        let push = push_urls.remove(name).unwrap_or_default();
        remotes.push(RepositoryRemote {
            name: name.clone(),
            fetch: fetch.iter().map(|url| endpoint(url, resolve)).collect(),
            push: push.iter().map(|url| endpoint(url, resolve)).collect(),
        });
        private_remotes.push(RepositoryPrivateRemote {
            name: name.clone(),
            fetch,
            push,
        });
    }
    let enriched = enrich(&remotes)?;

    // Do not return a torn read or silently advance the caller's revision.
    if before != git.config()?
        || head != git.head()?
        || source_ref != git.line(&["symbolic-ref", "--quiet", "HEAD"], true)?
        || Path::new(&git.required(&git_dir_args)?) != git_dir
        || Path::new(&git.required(&common_dir_args)?) != common_dir
    {
        return Err(invalid("Git/config changed during read"));
    }
    let mut digest = Sha256::new();
    for value in [&before.origins, &before.relevant] {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    let config_fingerprint =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest.clone().finalize());
    digest.update(serde_json::to_vec(&head).map_err(|_| invalid("HEAD encoding failed"))?);
    let fingerprint = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest.finalize());
    let changes = RepositoryChangeInputs {
        root: root.clone(),
        git_entry: path.join(".git"),
        config_files: config_files.into_iter().collect(),
        head_paths: vec![
            git_dir.join("HEAD"),
            common_dir.join("refs"),
            common_dir.join("packed-refs"),
        ],
        git_dir,
        common_dir,
        fingerprint,
    };
    Ok((
        RepositoryObservedRoot {
            config_fingerprint,
            root: root.clone(),
            branch: head.0,
            head_sha: head.1,
            remotes,
            change_inputs: changes,
            private_root: RepositoryPrivateRoot {
                root: root.clone(),
                source_ref,
                remotes: private_remotes,
            },
        },
        enriched,
    ))
}

fn endpoint(
    original: &str,
    resolve: &impl Fn(&str) -> RepositoryEndpointResolution,
) -> RepositoryRemoteEndpoint {
    let is_file = original
        .split_once("://")
        .is_some_and(|(s, _)| s.eq_ignore_ascii_case("file"));
    let resolution = if is_file || GitRemoteUrl::parse(original).is_none() {
        RepositoryEndpointResolution::Unresolved {
            reason: RepositoryUnresolvedReason::UnsupportedTransport,
        }
    } else {
        resolve(original)
    };
    // Display sanitization is deliberately after resolution. Never admit the
    // rewritten display string as though it were the original transport URL.
    let display = original.split(['?', '#']).next().unwrap_or(original);
    RepositoryRemoteEndpoint {
        url: intent_git::redact::redact_credentials(display),
        resolution,
    }
}

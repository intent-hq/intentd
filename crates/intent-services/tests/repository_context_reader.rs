//! Real local Git fixtures for the inactive repository-context producer.
//! Provider canonicalization is an explicit fixture lookup, not a new parser.
#[path = "../src/repository_context_reader.rs"]
mod repository_context_reader;
#[path = "../src/test_support.rs"]
mod test_support;

use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::process::Command;

use intent_core::{
    ExecutionScope, HistoricalTargetProvenance, HistoricalTargetSource, RepositoryAvailability,
    RepositoryCapability, RepositoryCapabilityState, RepositoryConnectionScope,
    RepositoryContextRevision, RepositoryEndpointResolution, RepositoryOperation,
    RepositoryProvider, RepositoryRootContext, RepositoryRootId, RepositoryRootKind,
    RepositoryTarget, RepositoryTargetContext, RepositoryUnresolvedReason, ReviewSelectionOutcome,
    ReviewSelectionRequiredReason, SavedReviewSelection, WorkspaceGitRootId, WorkspaceId,
};
use intent_sourcecontrol::{
    remote_project::{CanonicalRemoteResolver, RemoteInstance, RemoteTransportMapping},
    GitlabInstance,
};
use repository_context_reader::{
    read_repository_context, read_repository_context_with_resolver, AdmittedRepositoryRoot,
    GitConfigEnvironment, RepositoryContextInput, RepositoryContextRead,
};

const A: &str = "https://github.com/team/a.git";
const B: &str = "https://github.com/team/b.git";
const GL: &str = "https://git.example:8443/gitlab/team/sub/app.git";

struct Fixture {
    guard: tempfile::TempDir,
    path: PathBuf,
    env: GitConfigEnvironment,
}

impl Fixture {
    fn new() -> Self {
        let guard = test_support::test_tempdir("repository-context-reader-");
        let path = guard.path().join("repo");
        init(&path, true);
        let global = guard.path().join("global-config");
        let system = guard.path().join("system-config");
        std::fs::write(&global, "").unwrap();
        std::fs::write(&system, "").unwrap();
        Self {
            path,
            guard,
            env: GitConfigEnvironment {
                global_config: Some(global),
                system_config: Some(system),
                extra_config_paths: Vec::new(),
            },
        }
    }

    fn git(&self, path: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .arg("-C")
            .arg(path)
            .args(args)
            .env(
                "GIT_CONFIG_GLOBAL",
                self.env.global_config.as_ref().unwrap(),
            )
            .env(
                "GIT_CONFIG_SYSTEM",
                self.env.system_config.as_ref().unwrap(),
            )
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_COMMON_DIR")
            .env_remove("GIT_CONFIG")
            .output()
            .unwrap();
        assert!(out.status.success(), "fixture Git failed: {args:?}");
        String::from_utf8(out.stdout).unwrap()
    }

    fn config(&self, key: &str, value: &str) {
        self.git(&self.path, &["config", key, value]);
    }

    fn input(&self) -> RepositoryContextInput {
        RepositoryContextInput {
            scope: ExecutionScope {
                daemon_id: "daemon-A".into(),
                authority_scope_id: "admitted-caller".into(),
                authority_generation: 4,
            },
            revision: RepositoryContextRevision::new("boot", 1),
            roots: vec![root(&self.path, RepositoryRootKind::Primary)],
        }
    }

    fn read(&self, input: &RepositoryContextInput) -> RepositoryContextRead {
        read_repository_context(input, &resolve, &self.env).unwrap()
    }
}

fn init(path: &Path, commit: bool) {
    let repo = git2::Repository::init_opts(
        path,
        git2::RepositoryInitOptions::new().initial_head("main"),
    )
    .unwrap();
    if commit {
        let tree_id = repo.index().unwrap().write_tree().unwrap();
        let tree = repo.find_tree(tree_id).unwrap();
        let sig = git2::Signature::now("Fixture", "fixture@example.invalid").unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "fixture", &tree, &[])
            .unwrap();
    }
}

fn target(name: &str) -> RepositoryTarget {
    if name == "gl" {
        RepositoryTarget {
            provider: RepositoryProvider::Gitlab,
            instance_base_url: "https://git.example:8443/gitlab".into(),
            project_path: "team/sub/app".into(),
        }
    } else {
        RepositoryTarget {
            provider: RepositoryProvider::Github,
            instance_base_url: "https://github.com".into(),
            project_path: format!("team/{name}"),
        }
    }
}

fn facts(name: &str) -> RepositoryTargetContext {
    RepositoryTargetContext {
        target: target(name),
        provider_project_id: None,
        connection: Some(RepositoryConnectionScope {
            connection_id: format!("connection-{name}"),
            account_id: "account-A".into(),
            connection_generation: 7,
        }),
        availability: RepositoryAvailability::Connected,
        capabilities: vec![RepositoryCapability {
            operation: RepositoryOperation::ReadReview,
            state: RepositoryCapabilityState::Available,
        }],
    }
}

fn root(path: &Path, kind: RepositoryRootKind) -> AdmittedRepositoryRoot {
    AdmittedRepositoryRoot {
        root: RepositoryRootId {
            workspace_id: WorkspaceId::from("workspace-A"),
            kind,
        },
        path: path.to_owned(),
        saved_selection: SavedReviewSelection::Automatic,
        explicit_target: None,
        targets: vec![facts("a"), facts("b"), facts("gl")],
    }
}

fn resolve(url: &str) -> RepositoryEndpointResolution {
    let name = match url {
        A | "git@github.com:team/a.git" | "https://www.github.com/team/a.git" => "a",
        B => "b",
        GL => "gl",
        _ => {
            return RepositoryEndpointResolution::Unresolved {
                reason: RepositoryUnresolvedReason::UnknownInstance,
            }
        }
    };
    RepositoryEndpointResolution::Resolved {
        target: target(name),
    }
}

fn selected(context: &RepositoryRootContext) -> Option<&RepositoryTarget> {
    match &context.review_selection.outcome {
        ReviewSelectionOutcome::Resolved { target, .. } => Some(target),
        _ => None,
    }
}

#[test]
fn regular_root_reads_actual_head_aliases_and_explicit_connection_facts() {
    let f = Fixture::new();
    f.config("remote.origin.url", A);
    f.config("remote.alias.url", "git@github.com:team/a.git");
    let before = std::fs::read(f.path.join(".git/config")).unwrap();
    let input = f.input();
    let read = f.read(&input);
    let r = &read.context.roots[0];
    assert_eq!(r.branch.as_deref(), Some("main"));
    assert_eq!(
        r.head_sha.as_deref(),
        Some(f.git(&f.path, &["rev-parse", "HEAD"]).trim())
    );
    assert_eq!(selected(r), Some(&target("a")));
    assert_eq!(r.remotes.len(), 2);
    assert_eq!(r.targets, vec![facts("a")]);
    assert_eq!(read.context.scope, input.scope);
    assert_eq!(read.context.revision, input.revision);
    assert_eq!(std::fs::read(f.path.join(".git/config")).unwrap(), before);
    assert!(read.change_inputs[0]
        .config_files
        .contains(&f.path.join(".git/config")));
}

#[test]
fn distinct_and_unknown_targets_never_prefer_origin() {
    let f = Fixture::new();
    f.config("remote.origin.url", A);
    f.config("remote.other.url", B);
    let mut input = f.input();
    let read = f.read(&input);
    assert!(matches!(
        read.context.roots[0].review_selection.outcome,
        ReviewSelectionOutcome::SelectionRequired {
            reason: ReviewSelectionRequiredReason::AmbiguousTargets
        }
    ));
    f.config("remote.other.url", "git@unbound-alias:team/a.git");
    let read = f.read(&input);
    assert!(matches!(
        read.context.roots[0].review_selection.outcome,
        ReviewSelectionOutcome::SelectionRequired {
            reason: ReviewSelectionRequiredReason::UnresolvedCandidates
        }
    ));
    assert_eq!(read.context.roots[0].remotes.len(), 2);
    input.roots[0].saved_selection = SavedReviewSelection::ExplicitRemote {
        remote_name: "origin".into(),
    };
    assert_eq!(
        selected(&f.read(&input).context.roots[0]),
        Some(&target("a"))
    );
}

#[test]
fn zero_unsupported_and_url_less_remotes_remain_distinct() {
    let f = Fixture::new();
    let input = f.input();
    let read = f.read(&input);
    assert!(read.context.roots[0].review_selection.no_remotes);
    f.config("remote.local.url", "/some/local/repository");
    let read = f.read(&input);
    assert!(!read.context.roots[0].review_selection.no_remotes);
    assert!(selected(&read.context.roots[0]).is_none());
    assert!(matches!(
        read.context.roots[0].remotes[0].fetch[0].resolution,
        RepositoryEndpointResolution::Unresolved {
            reason: RepositoryUnresolvedReason::UnsupportedTransport
        }
    ));
    f.git(&f.path, &["config", "--remove-section", "remote.local"]);
    f.config("remote.empty.fetch", "+refs/heads/*:refs/remotes/empty/*");
    let read = f.read(&input);
    assert_eq!(read.context.roots[0].remotes[0].name, "empty");
    assert!(read.context.roots[0].remotes[0].fetch.is_empty());
    assert!(selected(&read.context.roots[0]).is_none());
}

#[test]
fn fetch_and_push_disagreement_in_both_directions_and_multiple_urls() {
    let f = Fixture::new();
    for (fetch, push, expected) in [(A, B, "a"), (B, A, "b")] {
        f.config("remote.origin.url", fetch);
        f.config("remote.origin.pushurl", push);
        let read = f.read(&f.input());
        let r = &read.context.roots[0];
        assert_eq!(selected(r), Some(&target(expected)));
        assert_eq!(r.remotes[0].fetch[0].url, fetch);
        assert_eq!(r.remotes[0].push[0].url, push);
        assert_eq!(r.targets.len(), 2);
    }
    f.config("remote.origin.url", A);
    f.git(
        &f.path,
        &[
            "config",
            "--add",
            "remote.origin.url",
            "git@github.com:team/a.git",
        ],
    );
    f.git(&f.path, &["config", "--add", "remote.origin.pushurl", B]);
    let read = f.read(&f.input());
    assert_eq!(selected(&read.context.roots[0]), Some(&target("a")));
    assert_eq!(read.context.roots[0].remotes[0].fetch.len(), 2);
    assert_eq!(read.context.roots[0].remotes[0].push.len(), 2);
    f.git(&f.path, &["config", "--add", "remote.origin.url", B]);
    assert!(selected(&f.read(&f.input()).context.roots[0]).is_none());
}

#[test]
fn common_worktree_and_included_config_are_effective_in_linked_roots() {
    let f = Fixture::new();
    let included = f.path.join(".git/remote-include");
    std::fs::write(&included, format!("[remote \"included\"]\nurl = {A}\n")).unwrap();
    f.config("include.path", "remote-include");
    f.config("extensions.worktreeConfig", "true");
    let linked = f.guard.path().join("linked");
    f.git(
        &f.path,
        &["worktree", "add", "-b", "feature", linked.to_str().unwrap()],
    );
    f.git(&linked, &["config", "--worktree", "remote.local.url", B]);

    let primary = f.read(&f.input());
    assert_eq!(selected(&primary.context.roots[0]), Some(&target("a")));
    let mut input = f.input();
    input.roots[0].path = linked.clone();
    input.roots[0].saved_selection = SavedReviewSelection::ExplicitRemote {
        remote_name: "local".into(),
    };
    let read = f.read(&input);
    assert_eq!(read.context.roots[0].branch.as_deref(), Some("feature"));
    assert_eq!(selected(&read.context.roots[0]), Some(&target("b")));
    assert_eq!(read.context.roots[0].remotes.len(), 2);
    let changes = &read.change_inputs[0];
    assert_ne!(changes.git_dir, changes.common_dir);
    assert_eq!(changes.git_entry, linked.join(".git"));
    assert!(changes.config_files.contains(&included));
    assert!(changes.head_paths.contains(&changes.git_dir.join("HEAD")));
    assert!(changes
        .config_files
        .contains(&changes.git_dir.join("config.worktree")));
    assert!(changes
        .config_files
        .contains(&changes.common_dir.join("config")));
}

#[test]
fn registered_root_keeps_its_own_identity_configuration_and_selection() {
    let f = Fixture::new();
    f.config("remote.origin.url", A);
    let registered = f.guard.path().join("registered");
    init(&registered, true);
    f.git(&registered, &["config", "remote.origin.url", GL]);
    let mut input = f.input();
    input.roots.push(root(
        &registered,
        RepositoryRootKind::Registered {
            git_root_id: WorkspaceGitRootId::from("root-B"),
        },
    ));
    let read = f.read(&input);
    assert_eq!(selected(&read.context.roots[0]), Some(&target("a")));
    assert_eq!(selected(&read.context.roots[1]), Some(&target("gl")));
    assert_eq!(read.context.roots[1].root, input.roots[1].root);
    assert_ne!(read.change_inputs[0].git_dir, read.change_inputs[1].git_dir);
}

#[test]
fn renamed_and_removed_explicit_selection_never_recovers_via_origin() {
    let f = Fixture::new();
    f.config("remote.origin.url", A);
    let mut input = f.input();
    input.roots[0].saved_selection = SavedReviewSelection::ExplicitRemote {
        remote_name: "origin".into(),
    };
    f.git(&f.path, &["remote", "rename", "origin", "renamed"]);
    let read = f.read(&input);
    assert!(matches!(
        read.context.roots[0].review_selection.outcome,
        ReviewSelectionOutcome::SelectionRequired {
            reason: ReviewSelectionRequiredReason::MissingSelectedRemote
        }
    ));
    assert_eq!(
        read.context.roots[0].review_selection.saved,
        input.roots[0].saved_selection
    );
    f.git(&f.path, &["remote", "remove", "renamed"]);
    let read = f.read(&input);
    assert!(read.context.roots[0].review_selection.no_remotes);
    assert_eq!(
        read.context.roots[0].review_selection.saved,
        input.roots[0].saved_selection
    );
}

#[test]
fn changed_origin_preserves_only_the_explicitly_supplied_historical_choice() {
    let f = Fixture::new();
    f.config("remote.origin.url", A);
    let mut input = f.input();
    assert_eq!(
        selected(&f.read(&input).context.roots[0]),
        Some(&target("a"))
    );
    f.config("remote.origin.url", B);
    assert_eq!(
        selected(&f.read(&input).context.roots[0]),
        Some(&target("b"))
    );
    let saved = SavedReviewSelection::MigratedCanonical {
        target: target("a"),
        provenance: HistoricalTargetProvenance {
            source: HistoricalTargetSource::WorkspaceMetadata,
            record_id: "old-row".into(),
            resolver_version: "historical".into(),
            evidence_id: "verified-evidence".into(),
        },
    };
    input.roots[0].saved_selection = saved.clone();
    let read = f.read(&input);
    assert_eq!(selected(&read.context.roots[0]), Some(&target("a")));
    assert_eq!(read.context.roots[0].review_selection.saved, saved);
    input.roots[0].explicit_target = Some(target("gl"));
    assert_eq!(
        selected(&f.read(&input).context.roots[0]),
        Some(&target("gl"))
    );
    assert_eq!(input.roots[0].saved_selection, saved);
    input.roots[0].explicit_target = None;
    f.git(&f.path, &["remote", "remove", "origin"]);
    let read = f.read(&input);
    assert!(selected(&read.context.roots[0]).is_none());
    assert_eq!(read.context.roots[0].review_selection.saved, saved);
}

#[test]
fn fresh_read_observes_include_edits_and_reports_changed_inputs() {
    let f = Fixture::new();
    let include = f.guard.path().join("global-include");
    let global = f.env.global_config.as_ref().unwrap();
    std::fs::write(global, "[include]\npath = global-include\n").unwrap();
    std::fs::write(&include, format!("[remote \"origin\"]\nurl = {A}\n")).unwrap();
    let mut input = f.input();
    let first = f.read(&input);
    assert!(first.change_inputs[0].config_files.contains(&include));
    std::fs::write(&include, format!("[remote \"origin\"]\nurl = {B}\n")).unwrap();
    input.revision = RepositoryContextRevision::new("boot", 2);
    let second = f.read(&input);
    assert_eq!(selected(&first.context.roots[0]), Some(&target("a")));
    assert_eq!(selected(&second.context.roots[0]), Some(&target("b")));
    assert_ne!(
        first.change_inputs[0].fingerprint,
        second.change_inputs[0].fingerprint
    );
    assert_eq!(
        first.context.compare_revision(&second.context),
        Some(std::cmp::Ordering::Less)
    );
    std::fs::write(
        &include,
        format!("[remote \"origin\"]\nurl = {A}\nurl = {B}\npushurl = {GL}\n"),
    )
    .unwrap();
    let multiple = f.read(&input);
    let remote = &multiple.context.roots[0].remotes[0];
    assert_eq!(remote.fetch.len(), 2);
    assert_eq!(remote.push.len(), 1);
    assert_eq!(remote.push[0].url, GL);
    assert!(selected(&multiple.context.roots[0]).is_none());
}

#[test]
fn git_itself_applies_fetch_and_push_url_rewrites() {
    let f = Fixture::new();
    f.config("url.https://github.com/team/.insteadOf", "fixture:");
    f.config("remote.origin.url", "fixture:a.git");
    f.config(
        "url.https://github.com/team/b.git.pushInsteadOf",
        "fixture:a.git",
    );
    let read = f.read(&f.input());
    assert_eq!(read.context.roots[0].remotes[0].fetch[0].url, A);
    assert_eq!(read.context.roots[0].remotes[0].push[0].url, B);
    assert_eq!(selected(&read.context.roots[0]), Some(&target("a")));
}

#[test]
fn resolver_sees_original_full_identity_before_display_redaction() {
    let f = Fixture::new();
    let original = "https://user:fixture-secret@git.example:8443/gitlab/team/sub/app.git?access_token=fixture-query#fixture-fragment";
    f.config("remote.origin.url", original);
    let calls = Cell::new(0);
    let resolver = |url: &str| {
        calls.set(calls.get() + 1);
        assert_eq!(url, original);
        RepositoryEndpointResolution::Unresolved {
            reason: RepositoryUnresolvedReason::InvalidRemote,
        }
    };
    let read = read_repository_context(&f.input(), &resolver, &f.env).unwrap();
    assert_eq!(calls.get(), 2);
    assert!(selected(&read.context.roots[0]).is_none());
    let wire = serde_json::to_string(&read.context).unwrap();
    for secret in [
        "fixture-secret",
        "fixture-query",
        "fixture-fragment",
        "user:",
    ] {
        assert!(!wire.contains(secret));
    }
    assert!(wire.contains("git.example:8443/gitlab/team/sub/app.git"));
}

#[test]
fn unavailable_roots_missing_facts_and_torn_config_fail_without_fallback() {
    let f = Fixture::new();
    f.config("remote.origin.url", A);
    let child = f.path.join("not-a-repository");
    std::fs::create_dir(&child).unwrap();
    let mut input = f.input();
    input.roots[0].path = child;
    assert!(read_repository_context(&input, &resolve, &f.env).is_err());
    input.roots[0].path = f.path.clone();
    input.roots[0].targets.clear();
    assert!(read_repository_context(&input, &resolve, &f.env).is_err());
    input.roots[0].targets = vec![facts("a"), facts("b")];
    let changed = Cell::new(false);
    let resolver = |url: &str| {
        if !changed.replace(true) {
            f.config("remote.origin.url", B);
        }
        resolve(url)
    };
    let error = read_repository_context(&input, &resolver, &f.env)
        .err()
        .unwrap();
    assert!(error.to_string().contains("changed during read"));
    assert_eq!(
        selected(&f.read(&input).context.roots[0]),
        Some(&target("b"))
    );
}

#[test]
fn conditional_includes_and_detached_or_unborn_head_are_observed_without_guessing() {
    let f = Fixture::new();
    let include = f.path.join(".git/conditional");
    std::fs::write(&include, format!("[remote \"conditional\"]\nurl = {A}\n")).unwrap();
    f.config("includeIf.onbranch:main.path", "conditional");
    let read = f.read(&f.input());
    assert_eq!(selected(&read.context.roots[0]), Some(&target("a")));
    f.git(&f.path, &["checkout", "--detach"]);
    let read = f.read(&f.input());
    assert!(read.context.roots[0].branch.is_none());
    assert!(read.context.roots[0].head_sha.is_some());
    assert!(read.context.roots[0].review_selection.no_remotes);
    assert!(read.change_inputs[0].config_files.contains(&include));
    let unborn = f.guard.path().join("unborn");
    init(&unborn, false);
    let mut input = f.input();
    input.roots[0].path = unborn;
    let read = f.read(&input);
    assert_eq!(read.context.roots[0].branch.as_deref(), Some("main"));
    assert!(read.context.roots[0].head_sha.is_none());
}

#[test]
fn disconnected_facts_and_push_only_remotes_do_not_create_a_hosted_default() {
    let f = Fixture::new();
    f.config("remote.origin.pushurl", A);
    let mut input = f.input();
    input.roots[0].targets[0].availability = RepositoryAvailability::Disconnected;
    input.roots[0].targets[0].connection = None;
    input.roots[0].targets[0].capabilities.clear();
    let read = f.read(&input);
    let r = &read.context.roots[0];
    assert!(r.remotes[0].fetch.is_empty());
    assert_eq!(r.remotes[0].push[0].url, A);
    assert!(selected(r).is_none());
    assert_eq!(
        r.targets[0].availability,
        RepositoryAvailability::Disconnected
    );
    assert!(r.targets[0].connection.is_none());
    f.config("remote.origin.url", A);
    let read = f.read(&input);
    assert_eq!(selected(&read.context.roots[0]), Some(&target("a")));
    assert!(read.context.roots[0].targets[0].connection.is_none());
}

#[test]
fn malformed_config_and_control_characters_return_sanitized_failure() {
    let f = Fixture::new();
    f.config(
        "remote.origin.url",
        "https://fixture-secret@github.com/team/a.git\nhttps://github.com/team/b.git",
    );
    let called = Cell::new(false);
    let resolver = |url: &str| {
        called.set(true);
        resolve(url)
    };
    let error = read_repository_context(&f.input(), &resolver, &f.env)
        .err()
        .unwrap();
    assert!(!called.get());
    assert!(!error.to_string().contains("fixture-secret"));
    std::fs::write(f.path.join(".git/config"), "[invalid fixture-secret").unwrap();
    let error = read_repository_context(&f.input(), &resolve, &f.env)
        .err()
        .unwrap();
    assert!(!error.to_string().contains("fixture-secret"));
}

fn canonical_resolver() -> CanonicalRemoteResolver {
    let gl =
        RemoteInstance::gitlab(GitlabInstance::parse("https://git.example:8443/gitlab").unwrap());
    CanonicalRemoteResolver::new(
        vec![RemoteInstance::github_com(), gl.clone()],
        vec![
            RemoteTransportMapping::new(gl.clone(), "git@configured-alias:").unwrap(),
            RemoteTransportMapping::new(gl, "ssh://git@configured-alias:2222/repos/").unwrap(),
        ],
    )
    .unwrap()
}

#[test]
fn actual_resolver_preserves_full_instance_and_fetch_push_projects_both_directions() {
    let f = Fixture::new();
    let resolver = canonical_resolver();
    for (fetch, push, expected, other) in [(GL, A, "gl", "a"), (A, GL, "a", "gl")] {
        f.config("remote.origin.url", fetch);
        f.config("remote.origin.pushurl", push);
        let read = read_repository_context_with_resolver(&f.input(), &resolver, &f.env).unwrap();
        let root = &read.context.roots[0];
        assert_eq!(selected(root), Some(&target(expected)));
        assert_eq!(
            root.remotes[0].fetch[0].resolution,
            RepositoryEndpointResolution::Resolved {
                target: target(expected)
            }
        );
        assert_eq!(
            root.remotes[0].push[0].resolution,
            RepositoryEndpointResolution::Resolved {
                target: target(other)
            }
        );
        assert_eq!(root.targets.len(), 2);
    }
}

#[test]
fn actual_resolver_composes_git_rewrites_with_explicit_ssh_and_scp_aliases() {
    let f = Fixture::new();
    let resolver = canonical_resolver();
    f.config("url.https://git.example:8443/gitlab/.insteadOf", "fixture:");
    f.config("remote.origin.url", "fixture:team/sub/app.git");
    f.config(
        "remote.origin.pushurl",
        "ssh://git@configured-alias:2222/repos/team/sub/app.git",
    );
    f.config("remote.alias.url", "git@configured-alias:team/sub/app.git");
    let read = read_repository_context_with_resolver(&f.input(), &resolver, &f.env).unwrap();
    let root = &read.context.roots[0];
    assert_eq!(selected(root), Some(&target("gl")));
    assert_eq!(root.targets, vec![facts("gl")]);
    assert_eq!(root.remotes.len(), 2);
    for endpoint in root
        .remotes
        .iter()
        .flat_map(|r| r.fetch.iter().chain(&r.push))
    {
        assert_eq!(
            endpoint.resolution,
            RepositoryEndpointResolution::Resolved {
                target: target("gl")
            }
        );
    }
    assert!(root
        .remotes
        .iter()
        .any(|r| r.fetch.iter().any(|e| e.url == GL)));
}

#[test]
fn actual_resolver_unknown_ambiguous_and_invalid_candidates_never_choose_origin() {
    let f = Fixture::new();
    let gl =
        RemoteInstance::gitlab(GitlabInstance::parse("https://git.example:8443/gitlab").unwrap());
    let other =
        RemoteInstance::gitlab(GitlabInstance::parse("https://other.example/forge").unwrap());
    let resolver = CanonicalRemoteResolver::new(
        vec![gl.clone(), other.clone()],
        vec![
            RemoteTransportMapping::new(gl, "git@ambiguous:").unwrap(),
            RemoteTransportMapping::new(other, "git@ambiguous:").unwrap(),
        ],
    )
    .unwrap();
    f.config("remote.origin.url", GL);
    for (url, reason) in [
        (
            "git@unknown:team/sub/app.git",
            RepositoryUnresolvedReason::UnknownInstance,
        ),
        (
            "git@ambiguous:team/sub/app.git",
            RepositoryUnresolvedReason::AmbiguousMapping,
        ),
        (
            "https://git.example:8443/gitlab/team/sub/a%70p.git",
            RepositoryUnresolvedReason::InvalidRemote,
        ),
        (
            "git://github.com/team/a.git",
            RepositoryUnresolvedReason::UnsupportedTransport,
        ),
    ] {
        f.config("remote.other.url", url);
        let read = read_repository_context_with_resolver(&f.input(), &resolver, &f.env).unwrap();
        let root = &read.context.roots[0];
        assert!(matches!(
            root.review_selection.outcome,
            ReviewSelectionOutcome::SelectionRequired {
                reason: ReviewSelectionRequiredReason::UnresolvedCandidates
            }
        ));
        let other = root.remotes.iter().find(|r| r.name == "other").unwrap();
        assert_eq!(
            other.fetch[0].resolution,
            RepositoryEndpointResolution::Unresolved { reason }
        );
    }
}

#[test]
fn actual_resolver_checks_original_before_any_display_sanitization() {
    let f = Fixture::new();
    let resolver = canonical_resolver();
    f.config(
        "remote.origin.url",
        "https://user:fixture-secret@git.example:8443/gitlab/team/sub/app.git?token=fixture-query",
    );
    let rejected = read_repository_context_with_resolver(&f.input(), &resolver, &f.env).unwrap();
    assert_eq!(
        rejected.context.roots[0].remotes[0].fetch[0].resolution,
        RepositoryEndpointResolution::Unresolved {
            reason: RepositoryUnresolvedReason::InvalidRemote
        }
    );
    f.config(
        "remote.origin.url",
        "https://user:fixture-secret@git.example:8443/gitlab/team/sub/app.git",
    );
    let identified = read_repository_context_with_resolver(&f.input(), &resolver, &f.env).unwrap();
    assert_eq!(selected(&identified.context.roots[0]), Some(&target("gl")));
    for read in [&rejected, &identified] {
        let json = serde_json::to_string(&read.context).unwrap();
        assert!(!json.contains("fixture-secret"));
        assert!(!json.contains("fixture-query"));
    }
}

#[test]
fn configured_url_that_collides_with_a_remote_name_is_not_reinterpreted() {
    let f = Fixture::new();
    f.config("remote.origin.url", A);
    f.config(&format!("remote.{A}.url"), B);
    let error = read_repository_context(&f.input(), &resolve, &f.env)
        .err()
        .unwrap();
    assert!(error.to_string().contains("collides with a remote name"));
}

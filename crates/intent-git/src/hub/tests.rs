use super::*;
use crate::testutil::{commit_file, init_repo, TempDir};
use std::process::Command;

struct Fixture {
    _root: tempfile::TempDir,
    forge: TempDir,
    cache_root: PathBuf,
    cache: PathBuf,
    hubs: PathBuf,
    identity: HubIdentity,
    hub: Hub,
    tip: Oid,
}

impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let forge = init_repo("hub-forge");
        commit_file(forge.path(), "tracked", "base\n");
        let source = Repository::open(forge.path()).unwrap();
        let tip = source.head().unwrap().target().unwrap();
        source
            .reference("refs/heads/main", tip, true, "fixture")
            .unwrap();
        source.set_head("refs/heads/main").unwrap();
        let cache_root = repo_cache::cache_root_for(root.path());
        let cache = repo_cache::cache_path_for(&cache_root, "acme", "widget");
        let cache_repo = Repository::clone(forge.path().to_str().unwrap(), &cache).unwrap();
        // Real local forge objects with canonical production identity metadata;
        // hub operations never contact this network origin.
        cache_repo
            .remote_set_url("origin", "https://github.com/acme/widget.git")
            .unwrap();
        let identity = HubIdentity::new("github", "github.com", "acme", "widget").unwrap();
        let hubs = root.path().join("hubs");
        let hub = Hub::ensure(&cache_root, &hubs, identity.clone())
            .await
            .unwrap();
        Self {
            _root: root,
            forge,
            cache_root,
            cache,
            hubs,
            identity,
            hub,
            tip,
        }
    }

    fn scope(&self) -> HubAssignment {
        HubAssignment {
            workspace_id: "workspace-a".into(),
            agent_id: "agent-a".into(),
            repo_key: self.identity.key(),
            lease_id: "lease-a".into(),
            incarnation_id: "incarnation-a".into(),
            connection_generation: 2,
            assignment_epoch: 3,
        }
    }

    fn update(&self, kind: AgentRef) -> RefUpdate {
        RefUpdate {
            name: agent_ref("workspace-a", "agent-a", kind).unwrap(),
            expected: None,
            new: Some(self.tip),
        }
    }
}

fn git(path: &Path, args: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(path)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn canonical_key_separates_provider_host_and_slug() {
    let first = HubIdentity::new("GitHub", "GitHub.COM", "Acme", "Widget").unwrap();
    assert_eq!(
        first,
        HubIdentity::new("github", "github.com", "acme", "widget").unwrap()
    );
    assert_eq!(first.key().len(), 64);
    let keys: BTreeSet<_> = [
        first.key(),
        HubIdentity::new("gitlab", "github.com", "acme", "widget")
            .unwrap()
            .key(),
        HubIdentity::new("github", "enterprise.example", "acme", "widget")
            .unwrap()
            .key(),
        HubIdentity::new("github", "github.com", "other", "widget")
            .unwrap()
            .key(),
        HubIdentity::new("github", "github.com", "acme", "different")
            .unwrap()
            .key(),
    ]
    .into_iter()
    .collect();
    assert_eq!(keys.len(), 5);
    assert!(!first.matches_origin("https://other.example/acme/widget.git"));
    assert!(!first.matches_origin("https://github.com/foreign/acme/widget.git"));
    assert!(!first.matches_origin("https://evil.example/a@github.com/acme/widget.git"));
    assert!(first.matches_origin("git@github.com:Acme/Widget.git"));
}

#[test]
fn deterministic_refs_and_invalid_names() {
    assert_eq!(
        agent_ref("ws", "agent", AgentRef::Head).unwrap(),
        "refs/intent/ws/ws/agents/agent/head"
    );
    assert_eq!(
        agent_ref("ws", "agent", AgentRef::Wip).unwrap(),
        "refs/intent/ws/ws/agents/agent/wip"
    );
    for (kind, suffix) in [
        (CheckpointRef::Head, "head"),
        (CheckpointRef::Wip, "wip"),
        (CheckpointRef::Index, "index"),
    ] {
        assert_eq!(
            checkpoint_ref("capture", "repo", kind).unwrap(),
            format!("refs/intent/checkpoints/capture/repo/{suffix}")
        );
    }
    assert_eq!(
        publish_ref("feature/one").unwrap(),
        "refs/intent/publish/feature/one"
    );
    for invalid in [
        "", ".", "..", "a/agent", "a\\b", "a\0b", "a\nb", "a b", "x.lock", "a..b", "a@{b",
        "-option", ".hidden", "a:", "a*", "a.",
    ] {
        assert!(
            agent_ref(invalid, "agent", AgentRef::Head).is_err(),
            "{invalid:?}"
        );
        assert!(
            agent_ref("ws", invalid, AgentRef::Wip).is_err(),
            "{invalid:?}"
        );
        assert!(checkpoint_ref(invalid, "repo", CheckpointRef::Head).is_err());
        assert!(HubIdentity::new("github", "github.com", invalid, "repo").is_err());
    }
    for invalid in [
        "",
        "a..b",
        "a:refs/heads/main",
        "a\0b",
        "a.lock",
        "-option",
        "/main",
        "main/",
    ] {
        assert!(publish_ref(invalid).is_err(), "{invalid:?}");
    }
}

#[tokio::test]
async fn creation_is_bare_idempotent_and_detached_from_cache() {
    let fixture = Fixture::new().await;
    let again = Hub::ensure(&fixture.cache_root, &fixture.hubs, fixture.identity.clone())
        .await
        .unwrap();
    assert_eq!(again.path(), fixture.hub.path());
    let repo = Repository::open_bare(again.path()).unwrap();
    assert!(repo.is_bare());
    assert_eq!(repo.refname_to_id("refs/heads/main").unwrap(), fixture.tip);
    assert!(repo.remotes().unwrap().is_empty());
    assert!(!repo.path().join("objects/info/alternates").exists());
    assert!(repo.find_reference("refs/heads/HEAD").is_err());
    let scope = fixture.scope();
    fixture
        .hub
        .finalize_agent_refs(&scope, &scope, &[fixture.update(AgentRef::Head)])
        .unwrap();
    fixture
        .hub
        .anchor("cp", CheckpointRef::Head, fixture.tip)
        .unwrap();
    // Model the strongest cache cleanup, not just GC policy settings.
    std::fs::remove_dir_all(&fixture.cache).unwrap();
    git(fixture.hub.path(), &["gc", "--prune=now"]);
    git(fixture.hub.path(), &["fsck", "--full", "--no-dangling"]);
    let repo = Repository::open_bare(fixture.hub.path()).unwrap();
    let commit = repo.find_commit(fixture.tip).unwrap();
    let tree = commit.tree().unwrap();
    let blob = repo
        .find_blob(tree.get_name("tracked").unwrap().id())
        .unwrap();
    assert_eq!(blob.content(), b"base\n");
    // Reopening a durable hub does not require a surviving cache.
    Hub::ensure(&fixture.cache_root, &fixture.hubs, fixture.identity)
        .await
        .unwrap();
}

#[tokio::test]
async fn receives_are_scoped_staged_and_compare_and_swap() {
    let fixture = Fixture::new().await;
    let scope = fixture.scope();
    let updates = [
        fixture.update(AgentRef::Head),
        fixture.update(AgentRef::Wip),
    ];
    fixture
        .hub
        .validate_receive(&scope, &scope, &updates)
        .unwrap();
    let repo = fixture.hub.open().unwrap();
    assert!(
        repo.find_reference(&updates[0].name).is_err(),
        "admission must not repair aliases"
    );
    fixture
        .hub
        .finalize_agent_refs(&scope, &scope, &updates)
        .unwrap();
    assert_eq!(repo.refname_to_id(&updates[0].name).unwrap(), fixture.tip);
    assert_eq!(repo.refname_to_id(&updates[1].name).unwrap(), fixture.tip);
    assert!(
        fixture
            .hub
            .finalize_agent_refs(&scope, &scope, &updates)
            .is_err(),
        "stale expected OIDs"
    );
    let deletes: Vec<_> = updates
        .into_iter()
        .map(|u| RefUpdate {
            name: u.name,
            expected: Some(fixture.tip),
            new: None,
        })
        .collect();
    fixture
        .hub
        .finalize_agent_refs(&scope, &scope, &deletes)
        .unwrap();
    assert!(repo.find_reference(&deletes[0].name).is_err());
    assert!(
        Repository::open(fixture.forge.path())
            .unwrap()
            .references_glob("refs/intent/*")
            .unwrap()
            .next()
            .is_none(),
        "checkpoint never pushes to forge"
    );
}

#[tokio::test]
async fn forged_identity_and_expired_fencing_are_rejected() {
    let fixture = Fixture::new().await;
    let current = fixture.scope();
    for field in 0..7 {
        let mut forged = current.clone();
        match field {
            0 => forged.workspace_id = "workspace-b".into(),
            1 => forged.agent_id = "agent-b".into(),
            2 => forged.repo_key = "other-repo".into(),
            3 => forged.lease_id = "lease-b".into(),
            4 => forged.incarnation_id = "incarnation-b".into(),
            5 => forged.connection_generation += 1,
            _ => forged.assignment_epoch += 1,
        }
        assert!(
            fixture
                .hub
                .validate_receive(&forged, &current, &[fixture.update(AgentRef::Head)])
                .is_err(),
            "field {field}"
        );
        assert!(fixture
            .hub
            .finalize_agent_refs(&forged, &current, &[fixture.update(AgentRef::Head)])
            .is_err());
    }
    let mut other_repo = current;
    other_repo.repo_key = HubIdentity::new("github", "other.example", "acme", "widget")
        .unwrap()
        .key();
    assert!(fixture
        .hub
        .validate_receive(&other_repo, &other_repo, &[fixture.update(AgentRef::Head)])
        .is_err());
}

#[tokio::test]
async fn unauthorized_refs_and_symbolic_escape_never_change_any_ref() {
    let fixture = Fixture::new().await;
    let scope = fixture.scope();
    let allowed = fixture.update(AgentRef::Head);
    for name in [
        "refs/heads/main",
        "refs/intent/publish/main",
        "refs/intent/checkpoints/cp/repo/head",
        "refs/intent/ws/workspace-b/agents/agent-a/head",
        "refs/intent/ws/workspace-a/agents/agent-b/wip",
        "refs/intent/ws/workspace-a/agents/agent-a/head/extra",
        "refs/intent/ws/workspace-a/agents/agent-a/index",
        "HEAD",
        "refs/intent/ws/workspace-a/agents/agent-a/../head",
        "refs/x\0",
    ] {
        let updates = [
            allowed.clone(),
            RefUpdate {
                name: name.into(),
                expected: None,
                new: Some(fixture.tip),
            },
        ];
        assert!(
            fixture
                .hub
                .finalize_agent_refs(&scope, &scope, &updates)
                .is_err(),
            "{name:?}"
        );
        assert!(fixture
            .hub
            .open()
            .unwrap()
            .find_reference(&allowed.name)
            .is_err());
    }
    assert!(fixture
        .hub
        .finalize_agent_refs(&scope, &scope, &[allowed.clone(), allowed.clone()])
        .is_err());
    let repo = fixture.hub.open().unwrap();
    repo.reference_symbolic(&allowed.name, "refs/heads/main", true, "attack")
        .unwrap();
    let mut escape = allowed;
    escape.expected = Some(fixture.tip);
    escape.new = None;
    assert!(fixture
        .hub
        .finalize_agent_refs(&scope, &scope, &[escape])
        .is_err());
    assert_eq!(repo.refname_to_id("refs/heads/main").unwrap(), fixture.tip);
}

#[tokio::test]
async fn targets_and_all_expected_oids_are_checked_before_writes() {
    let fixture = Fixture::new().await;
    let scope = fixture.scope();
    let repo = fixture.hub.open().unwrap();
    let blob = repo.blob(b"not a commit").unwrap();
    for oid in [blob, Oid::ZERO_SHA1] {
        let mut update = fixture.update(AgentRef::Wip);
        update.new = Some(oid);
        assert!(fixture
            .hub
            .finalize_agent_refs(&scope, &scope, &[fixture.update(AgentRef::Head), update])
            .is_err());
        assert!(repo
            .find_reference(&fixture.update(AgentRef::Head).name)
            .is_err());
    }
    let mut stale = fixture.update(AgentRef::Wip);
    stale.expected = Some(fixture.tip);
    assert!(fixture
        .hub
        .finalize_agent_refs(&scope, &scope, &[fixture.update(AgentRef::Head), stale])
        .is_err());
    assert!(repo
        .find_reference(&fixture.update(AgentRef::Head).name)
        .is_err());
}

#[tokio::test]
async fn checkpoint_anchors_are_immutable_commits_including_index() {
    let fixture = Fixture::new().await;
    let repo = fixture.hub.open().unwrap();
    let tree = repo.find_commit(fixture.tip).unwrap().tree_id();
    for kind in [
        CheckpointRef::Head,
        CheckpointRef::Wip,
        CheckpointRef::Index,
    ] {
        fixture.hub.anchor("capture", kind, fixture.tip).unwrap();
        fixture.hub.anchor("capture", kind, fixture.tip).unwrap();
        assert!(fixture.hub.anchor("capture", kind, tree).is_err());
        assert!(fixture.hub.anchor("wrong-type", kind, tree).is_err());
    }
}

#[tokio::test]
async fn base_sync_imports_objects_prunes_only_base_and_preserves_old_checkpoints() {
    let fixture = Fixture::new().await;
    let scope = fixture.scope();
    fixture
        .hub
        .finalize_agent_refs(&scope, &scope, &[fixture.update(AgentRef::Head)])
        .unwrap();
    fixture
        .hub
        .anchor("old", CheckpointRef::Head, fixture.tip)
        .unwrap();
    let hub_repo = fixture.hub.open().unwrap();
    hub_repo
        .reference("refs/heads/stale", fixture.tip, false, "fixture")
        .unwrap();
    hub_repo
        .reference(
            &publish_ref("topic").unwrap(),
            fixture.tip,
            false,
            "fixture",
        )
        .unwrap();
    commit_file(fixture.forge.path(), "new", "next\n");
    // Simulate an upstream history rewrite: the old checkpoint is no longer
    // reachable from the new forge base.
    let forge = Repository::open(fixture.forge.path()).unwrap();
    let tree = forge
        .head()
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .tree()
        .unwrap();
    let sig = git2::Signature::now("Test", "test@example.com").unwrap();
    let rewritten = forge
        .commit(None, &sig, &sig, "rewritten history", &tree, &[])
        .unwrap();
    forge
        .reference("refs/heads/main", rewritten, true, "rewrite")
        .unwrap();
    // Refresh cache from the real local forge without needing network.
    let cache = Repository::open(&fixture.cache).unwrap();
    cache
        .remote_anonymous(fixture.forge.path().to_str().unwrap())
        .unwrap()
        .fetch(&["+refs/heads/*:refs/remotes/origin/*"], None, None)
        .unwrap();
    let next = cache.refname_to_id("refs/remotes/origin/main").unwrap();
    assert_ne!(next, fixture.tip);
    fixture.hub.sync_base(&fixture.cache_root).await.unwrap();
    assert_eq!(hub_repo.refname_to_id("refs/heads/main").unwrap(), next);
    assert!(hub_repo.find_reference("refs/heads/stale").is_err());
    assert_eq!(
        hub_repo
            .refname_to_id(&fixture.update(AgentRef::Head).name)
            .unwrap(),
        fixture.tip
    );
    assert_eq!(
        hub_repo
            .refname_to_id(&publish_ref("topic").unwrap())
            .unwrap(),
        fixture.tip
    );
    std::fs::remove_dir_all(&fixture.cache).unwrap();
    git(fixture.hub.path(), &["gc", "--prune=now"]);
    assert!(hub_repo.find_commit(next).is_ok());
    assert!(hub_repo.find_commit(fixture.tip).is_ok());
}

#[tokio::test]
async fn wrong_origin_and_existing_identity_mismatch_fail_closed() {
    let fixture = Fixture::new().await;
    let cache = Repository::open(&fixture.cache).unwrap();
    cache
        .remote_set_url("origin", "https://other.example/acme/widget.git")
        .unwrap();
    assert!(fixture.hub.sync_base(&fixture.cache_root).await.is_err());
    let other_identity = HubIdentity::new("github", "github.com", "acme", "widget").unwrap();
    assert!(Hub::ensure(
        &fixture.cache_root,
        &fixture.hubs.join("new-root"),
        other_identity
    )
    .await
    .is_err());
    fixture
        .hub
        .open()
        .unwrap()
        .config()
        .unwrap()
        .set_str("intent.hubIdentity", "wrong")
        .unwrap();
    assert!(
        Hub::ensure(&fixture.cache_root, &fixture.hubs, fixture.identity)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn concurrent_creation_recovers_unpublished_initialization() {
    let fixture = Fixture::new().await;
    let other_root = fixture.hubs.join("fresh");
    let staging = other_root.join(format!("{}.initializing", fixture.identity.key()));
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::write(staging.join("interrupted"), "partial state").unwrap();
    let (first, second) = tokio::join!(
        Hub::ensure(&fixture.cache_root, &other_root, fixture.identity.clone()),
        Hub::ensure(&fixture.cache_root, &other_root, fixture.identity.clone())
    );
    let first = first.unwrap();
    assert_eq!(first.path(), second.unwrap().path());
    assert!(!staging.exists());
    assert!(!first.path().join("interrupted").exists());
    assert_eq!(
        first
            .open()
            .unwrap()
            .refname_to_id("refs/heads/main")
            .unwrap(),
        fixture.tip
    );
}

#[tokio::test]
async fn different_forges_share_no_hub_even_when_cache_slot_is_reused() {
    let fixture = Fixture::new().await;
    Repository::open(&fixture.cache)
        .unwrap()
        .remote_set_url("origin", "https://enterprise.example/acme/widget.git")
        .unwrap();
    let identity = HubIdentity::new("github", "enterprise.example", "acme", "widget").unwrap();
    let other = Hub::ensure(&fixture.cache_root, &fixture.hubs, identity)
        .await
        .unwrap();
    assert_ne!(other.path(), fixture.hub.path());
    let scope = fixture.scope();
    assert!(other
        .validate_receive(&scope, &scope, &[fixture.update(AgentRef::Head)])
        .is_err());
    fixture
        .hub
        .validate_receive(&scope, &scope, &[fixture.update(AgentRef::Head)])
        .unwrap();
}

#[tokio::test]
async fn failed_git_initialization_is_not_published_and_retry_rebuilds() {
    let fixture = Fixture::new().await;
    let root = fixture.hubs.join("failed-initialization");
    let final_path = root.join(format!("{}.git", fixture.identity.key()));
    let staging = final_path.with_extension("initializing");
    let cache = Repository::open(&fixture.cache).unwrap();
    let broken = cache.path().join("refs/remotes/origin/broken");
    // Valid ref syntax, but a nonexistent object: real upload-pack/fetch fails
    // after the staging repo and alternate were created.
    std::fs::write(&broken, "1111111111111111111111111111111111111111\n").unwrap();
    assert!(
        Hub::ensure(&fixture.cache_root, &root, fixture.identity.clone())
            .await
            .is_err()
    );
    assert!(
        !final_path.exists(),
        "failed Git seed must never publish a hub"
    );
    assert!(staging.join("objects/info/alternates").exists());
    std::fs::remove_file(broken).unwrap();
    let hub = Hub::ensure(&fixture.cache_root, &root, fixture.identity)
        .await
        .unwrap();
    assert!(!staging.exists());
    assert!(!hub.path().join("objects/info/alternates").exists());
    assert_eq!(
        hub.open()
            .unwrap()
            .refname_to_id("refs/heads/main")
            .unwrap(),
        fixture.tip
    );
    std::fs::remove_dir_all(&fixture.cache).unwrap();
    git(hub.path(), &["gc", "--prune=now"]);
    git(hub.path(), &["fsck", "--full", "--no-dangling"]);
}

async fn assert_base_prefix_transition(old: &str, new: &str, packed: bool) {
    let fixture = Fixture::new().await;
    let forge = Repository::open(fixture.forge.path()).unwrap();
    forge
        .reference(&format!("refs/heads/{old}"), fixture.tip, false, "fixture")
        .unwrap();
    let fetch_args = [
        "fetch",
        "--prune",
        "--no-tags",
        fixture.forge.path().to_str().unwrap(),
        "+refs/heads/*:refs/remotes/origin/*",
    ];
    git(&fixture.cache, &fetch_args);
    fixture.hub.sync_base(&fixture.cache_root).await.unwrap();

    // This object is reachable exclusively through hidden refs, never base
    // branches or the cache. Pruning/fetching must preserve its full closure.
    let repo = fixture.hub.open().unwrap();
    let blob = repo.blob(b"private checkpoint data\n").unwrap();
    let mut builder = repo.treebuilder(None).unwrap();
    builder.insert("private", blob, 0o100_644).unwrap();
    let tree = repo.find_tree(builder.write().unwrap()).unwrap();
    let signature = git2::Signature::now("Test", "test@example.com").unwrap();
    let hidden = repo
        .commit(
            None,
            &signature,
            &signature,
            "hidden checkpoint",
            &tree,
            &[],
        )
        .unwrap();
    let scope = fixture.scope();
    let updates: Vec<_> = [AgentRef::Head, AgentRef::Wip]
        .into_iter()
        .map(|kind| RefUpdate {
            new: Some(hidden),
            ..fixture.update(kind)
        })
        .collect();
    fixture
        .hub
        .finalize_agent_refs(&scope, &scope, &updates)
        .unwrap();
    let mut retained: Vec<_> = updates.iter().map(|update| update.name.clone()).collect();
    for kind in [
        CheckpointRef::Head,
        CheckpointRef::Wip,
        CheckpointRef::Index,
    ] {
        fixture.hub.anchor("prefix-change", kind, hidden).unwrap();
        retained.push(checkpoint_ref("prefix-change", &scope.repo_key, kind).unwrap());
    }
    let publication = publish_ref("topic").unwrap();
    repo.reference(&publication, hidden, false, "fixture")
        .unwrap();
    retained.push(publication);
    if packed {
        git(fixture.hub.path(), &["pack-refs", "--all", "--prune"]);
    }

    forge
        .find_reference(&format!("refs/heads/{old}"))
        .unwrap()
        .delete()
        .unwrap();
    forge
        .reference(&format!("refs/heads/{new}"), fixture.tip, false, "rename")
        .unwrap();
    git(&fixture.cache, &fetch_args);
    // Also prove a retry settles to the same result rather than getting stuck
    // forever behind the obsolete prefix. Collect both errors before asserting.
    let first = fixture.hub.sync_base(&fixture.cache_root).await;
    let retry = fixture.hub.sync_base(&fixture.cache_root).await;
    assert!(
        first.is_ok() && retry.is_ok(),
        "{old} -> {new}: first={first:?}, retry={retry:?}"
    );
    assert!(repo.find_reference(&format!("refs/heads/{old}")).is_err());
    assert_eq!(
        repo.refname_to_id(&format!("refs/heads/{new}")).unwrap(),
        fixture.tip
    );
    assert_eq!(repo.refname_to_id("refs/heads/main").unwrap(), fixture.tip);
    for name in &retained {
        assert_eq!(
            repo.refname_to_id(name).unwrap(),
            hidden,
            "hidden ref {name} moved"
        );
    }
    std::fs::remove_dir_all(&fixture.cache).unwrap();
    git(fixture.hub.path(), &["gc", "--prune=now"]);
    git(fixture.hub.path(), &["fsck", "--full", "--no-dangling"]);
    assert_eq!(
        repo.find_blob(blob).unwrap().content(),
        b"private checkpoint data\n"
    );
    assert!(repo.find_commit(hidden).is_ok());
}

#[tokio::test]
async fn base_prefix_transition_topic_to_subtopic_preserves_hidden_refs() {
    assert_base_prefix_transition("topic", "topic/subtopic", false).await;
}

#[tokio::test]
async fn base_prefix_transition_subtopic_to_topic_preserves_packed_hidden_refs() {
    assert_base_prefix_transition("topic/subtopic", "topic", true).await;
}

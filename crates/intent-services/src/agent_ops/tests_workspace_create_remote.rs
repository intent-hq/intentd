//! `workspace.create` with `initialAgent.placement`: the placement, remote
//! compatibility and machine choice are checked before any workspace
//! effect; after the workspace commits the initial agent starts on the
//! chosen machine, and a start failure keeps both and reports it.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use intent_core::placement::{AgentPlacement, PlatformAvailability};
use intent_core::remote_launch::{
    EffectiveIsolation, RemoteLaunchNeeds, RemoteLaunchRequest, RemotePlacementView,
    REMOTE_PLACEMENT_METADATA_KEY,
};
use intent_core::{
    AgentId, BoxFuture, Error, WorkspaceApi, WorkspaceCreate, WorkspaceCreateInitialAgent,
    WorkspaceId, WorkspaceListFilter,
};
use intent_store::Store;
use serde_json::{json, Value};

use super::tests::{test_registry_with_default_provider, TempDb};
use crate::feature_ports::{RemoteLaunchPort, RemoteRuntime};
use crate::Services;

/// A remote-launch port that places every agent on one machine and records
/// what it was asked.
#[derive(Default)]
struct FakeRemote {
    needs: Mutex<Vec<RemoteLaunchNeeds>>,
    base_commits: Mutex<Vec<WorkspaceId>>,
    launched: Mutex<Vec<RemoteLaunchRequest>>,
    fail_launch: bool,
}

const BASE: &str = "0123456789abcdef0123456789abcdef01234567";

impl RemoteLaunchPort for FakeRemote {
    fn placements<'a>(
        &'a self,
        _svc: &'a Services,
    ) -> BoxFuture<'a, intent_core::Result<PlatformAvailability>> {
        Box::pin(async { Ok(PlatformAvailability::none(intent_core::now_iso())) })
    }

    fn placement_help<'a>(&'a self, _svc: &'a Services) -> BoxFuture<'a, Option<String>> {
        Box::pin(async { None })
    }

    fn admit<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a WorkspaceId,
        _placement: &'a AgentPlacement,
        _needs: &'a RemoteLaunchNeeds,
    ) -> BoxFuture<'a, intent_core::Result<RemotePlacementView>> {
        Box::pin(async { panic!("workspace.create admits before its workspace exists") })
    }

    fn admit_new_workspace<'a>(
        &'a self,
        _svc: &'a Services,
        placement: &'a AgentPlacement,
        needs: &'a RemoteLaunchNeeds,
    ) -> BoxFuture<'a, intent_core::Result<RemotePlacementView>> {
        self.needs.lock().unwrap().push(needs.clone());
        let mut resolved = placement.clone();
        resolved.node_id = Some("node-1".into());
        Box::pin(async move {
            Ok(RemotePlacementView {
                node_id: "node-1".into(),
                lease_id: "lease-1".into(),
                node_name: "Mac Studio".into(),
                placement: resolved,
                effective_isolation: EffectiveIsolation::Pending,
                placement_error: None,
                base_commit: String::new(),
            })
        })
    }

    fn base_commit<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, intent_core::Result<String>> {
        self.base_commits.lock().unwrap().push(workspace_id.clone());
        Box::pin(async { Ok(BASE.to_owned()) })
    }

    fn launch<'a>(
        &'a self,
        _svc: &'a Services,
        request: RemoteLaunchRequest,
    ) -> BoxFuture<'a, intent_core::Result<()>> {
        Box::pin(async move {
            self.launched.lock().unwrap().push(request);
            if self.fail_launch {
                return Err(Error::Internal("base ref write failed".into()));
            }
            Ok(())
        })
    }

    fn runtime<'a>(
        &'a self,
        _svc: &'a Services,
        _agent_id: &'a AgentId,
    ) -> BoxFuture<'a, intent_core::Result<Arc<RemoteRuntime>>> {
        Box::pin(async { Err(Error::InvalidParams("node-unavailable: test".into())) })
    }
}

struct Fixture {
    _db: TempDb,
    _repo: tempfile::TempDir,
    root: tempfile::TempDir,
    repo_path: PathBuf,
    branch: String,
    store: Store,
    svc: Services,
}

fn seed_repo(dir: &Path) -> String {
    let repo = git2::Repository::init(dir).unwrap();
    std::fs::write(dir.join("README.md"), "init\n").unwrap();
    let mut index = repo.index().unwrap();
    index
        .add_all(["*"].iter(), git2::IndexAddOption::DEFAULT, None)
        .unwrap();
    index.write().unwrap();
    let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
    let sig = git2::Signature::now("Tester", "t@e.dev").unwrap();
    repo.commit(Some("HEAD"), &sig, &sig, "chore: init", &tree, &[])
        .unwrap();
    let branch = repo.head().unwrap().shorthand().unwrap().to_owned();
    branch
}

async fn fixture(remote: Option<Arc<FakeRemote>>) -> Fixture {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");
    let repo = crate::test_support::test_tempdir("intentd-wscreate-remote-repo-");
    let branch = seed_repo(repo.path());
    let root = crate::test_support::test_tempdir("intentd-wscreate-remote-root-");
    let registry = test_registry_with_default_provider(&db);
    let mut svc = Services::new_isolated(store.clone())
        .with_workspaces_root(root.path().to_path_buf())
        .with_settings_registry(registry);
    if let Some(remote) = remote {
        svc = svc.with_remote_launch_port(remote);
    }
    Fixture {
        repo_path: repo.path().to_path_buf(),
        _db: db,
        _repo: repo,
        root,
        branch,
        store,
        svc,
    }
}

fn request(fx: &Fixture, initial_agent: Value) -> WorkspaceCreate {
    WorkspaceCreate {
        repository_path: Some(fx.repo_path.to_string_lossy().into_owned()),
        base_ref: Some(fx.branch.clone()),
        initial_agent: Some(serde_json::from_value(initial_agent).unwrap()),
        ..Default::default()
    }
}

/// Nothing was created: no workspace row and no directory under the root.
async fn assert_nothing_created(fx: &Fixture) {
    let workspaces = fx
        .store
        .list_workspaces(WorkspaceListFilter {
            include_archived: true,
            include_retired: true,
        })
        .await
        .unwrap();
    assert!(workspaces.is_empty(), "{workspaces:?}");
    let entries: Vec<_> = std::fs::read_dir(fx.root.path()).unwrap().collect();
    assert!(entries.is_empty(), "{entries:?}");
}

#[intent_test_macros::daemon_test]
async fn a_placed_initial_agent_starts_on_the_chosen_machine() {
    let remote = Arc::new(FakeRemote::default());
    let fx = fixture(Some(remote.clone())).await;
    let result = fx
        .svc
        .create_workspace(
            request(
                &fx,
                json!({
                    "model": "sonnet4.5",
                    "placement": {"target": "remote", "os": "macos"},
                }),
            ),
            None,
        )
        .await
        .expect("create");
    let agent = result.initial_agent.expect("initial agent");
    assert_eq!(agent["nodeId"], "node-1");
    assert_eq!(agent["nodeName"], "Mac Studio");
    assert_eq!(agent["effectiveIsolation"], "pending");
    assert_eq!(
        agent["placement"],
        json!({"target": "remote", "os": "macos", "nodeId": "node-1"})
    );
    assert!(agent.get("placementError").is_none(), "{agent}");
    let notice = result.notice.expect("launch notice");
    assert!(notice.contains("\"Mac Studio\""), "{notice}");
    assert!(notice.contains(&BASE[..12]), "{notice}");

    assert_eq!(
        remote.needs.lock().unwrap().clone(),
        [RemoteLaunchNeeds {
            provider: "auggie".into(),
            model: Some("sonnet4.5".into()),
            with_profile: false,
        }]
    );
    let ws = result.workspace.id;
    assert_eq!(
        remote.base_commits.lock().unwrap().as_slice(),
        std::slice::from_ref(&ws)
    );
    let launched = remote.launched.lock().unwrap().clone();
    assert_eq!(launched.len(), 1);
    assert_eq!(launched[0].workspace_id, ws);
    assert_eq!(launched[0].agent_id.as_str(), agent["id"].as_str().unwrap());
    assert_eq!(launched[0].view.base_commit, BASE);

    // The placement is daemon-owned metadata on the initial agent.
    let session = fx
        .store
        .get_agent_session_with_transcript(&launched[0].agent_id)
        .await
        .unwrap();
    assert!(session.is_initial_agent());
    let view = RemotePlacementView::from_metadata(session.metadata.as_ref()).expect("placed");
    assert_eq!(view.base_commit, BASE);
    assert_eq!(view.node_id, "node-1");
}

#[intent_test_macros::daemon_test]
async fn an_invalid_placement_creates_nothing() {
    let remote = Arc::new(FakeRemote::default());
    let fx = fixture(Some(remote.clone())).await;
    for (placement, expected) in [
        (
            json!({"target": "local", "os": "linux"}),
            "omit os, arch, nodeId, checkout and exclusive",
        ),
        (
            json!({"target": "remote", "exclusive": true}),
            "exclusive is not supported",
        ),
    ] {
        let result = fx
            .svc
            .create_workspace(request(&fx, json!({ "placement": placement })), None)
            .await;
        assert!(
            matches!(&result, Err(Error::InvalidParams(m)) if m.contains(expected)),
            "{result:?}"
        );
    }
    assert!(remote.needs.lock().unwrap().is_empty());
    assert!(remote.launched.lock().unwrap().is_empty());
    assert_nothing_created(&fx).await;
}

#[intent_test_macros::daemon_test]
async fn a_local_placement_creates_the_workspace_on_the_backend() {
    let remote = Arc::new(FakeRemote::default());
    let fx = fixture(Some(remote.clone())).await;
    let result = fx
        .svc
        .create_workspace(
            request(&fx, json!({ "placement": { "target": "local" } })),
            None,
        )
        .await;
    assert!(result.is_ok(), "{result:?}");
    assert!(remote.needs.lock().unwrap().is_empty());
    assert!(remote.launched.lock().unwrap().is_empty());
}

#[intent_test_macros::daemon_test]
async fn a_placement_no_machine_can_take_creates_nothing() {
    // The default port: this backend has no machines.
    let fx = fixture(None).await;
    let result = fx
        .svc
        .create_workspace(
            request(&fx, json!({ "placement": {"target": "remote"} })),
            None,
        )
        .await;
    let Err(Error::PlacementUnavailable(failure)) = result else {
        panic!("expected placement-unavailable, got {result:?}");
    };
    assert_eq!(failure.data()["code"], "placement-unavailable");
    assert_nothing_created(&fx).await;
}

#[intent_test_macros::daemon_test]
async fn a_profile_tied_to_this_backend_creates_nothing() {
    let remote = Arc::new(FakeRemote::default());
    let fx = fixture(Some(remote.clone())).await;
    let created = intent_core::caller::with_caller(
        intent_core::caller::Caller::Daemon,
        fx.svc.provider_profiles_create_impl(json!({
            "baseProviderId": "claude-code",
            "name": "Work",
            "env": [{ "key": "CLAUDE_CONFIG_DIR", "kind": "set", "value": "~/.claude-work" }],
        })),
    )
    .await
    .expect("create profile");
    let reference = created["profile"]["providerRef"].as_str().unwrap();
    let result = fx
        .svc
        .create_workspace(
            request(
                &fx,
                json!({ "provider": reference, "placement": {"target": "remote"} }),
            ),
            None,
        )
        .await;
    let Err(Error::ProviderProfile(failure)) = &result else {
        panic!("expected profile-not-remote-compatible, got {result:?}");
    };
    assert_eq!(failure.data["code"], "profile-not-remote-compatible");
    assert!(remote.needs.lock().unwrap().is_empty());
    assert_nothing_created(&fx).await;
}

#[intent_test_macros::daemon_test]
async fn a_placed_plain_folder_creates_nothing() {
    let remote = Arc::new(FakeRemote::default());
    let fx = fixture(Some(remote.clone())).await;
    let folder = crate::test_support::test_tempdir("intentd-wscreate-remote-folder-");
    let result = fx
        .svc
        .create_workspace(
            WorkspaceCreate {
                repository_path: Some(folder.path().to_string_lossy().into_owned()),
                plain_folder: Some(true),
                initial_agent: Some(WorkspaceCreateInitialAgent {
                    placement: Some(serde_json::from_value(json!({"target": "remote"})).unwrap()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            None,
        )
        .await;
    assert!(
        matches!(&result, Err(Error::InvalidParams(m)) if m.contains("git repository")),
        "{result:?}"
    );
    assert!(remote.needs.lock().unwrap().is_empty());
    assert_nothing_created(&fx).await;
}

/// A start failure after the workspace commits keeps the workspace and the
/// placed initial agent (never local) and reports the failure; a keyed
/// retry replays the same failure instead of creating again.
#[intent_test_macros::daemon_test]
async fn a_failed_start_keeps_the_workspace_and_reports_the_initial_agent() {
    let remote = Arc::new(FakeRemote {
        fail_launch: true,
        ..FakeRemote::default()
    });
    let fx = fixture(Some(remote.clone())).await;
    let create = || {
        fx.svc.create_workspace(
            request(&fx, json!({ "placement": {"target": "remote"} })),
            Some("key-1".into()),
        )
    };
    let result = create().await;
    let Err(Error::RemoteMachine(failure)) = &result else {
        panic!("expected launch-failed, got {result:?}");
    };
    assert!(failure.internal, "launch-failed is -32603");
    let launched = remote.launched.lock().unwrap().clone();
    assert_eq!(launched.len(), 1);
    let ws = launched[0].workspace_id.clone();
    let agent_id = launched[0].agent_id.clone();
    assert_eq!(
        failure.data,
        json!({
            "code": "launch-failed",
            "retryable": false,
            "workspaceId": ws.as_str(),
            "launch": { "agentId": agent_id.as_str(), "state": "failed" },
            "cause": {
                "code": "remote-launch-failed",
                "message": "internal error: base ref write failed",
            },
        }),
    );
    assert!(
        failure.message.contains("Mac Studio"),
        "{}",
        failure.message
    );

    // The workspace and its placed initial agent are kept.
    fx.store.get_workspace(&ws).await.expect("workspace kept");
    let session = fx
        .store
        .get_agent_session_with_transcript(&agent_id)
        .await
        .unwrap();
    assert!(session.is_initial_agent());
    let view = RemotePlacementView::from_metadata(session.metadata.as_ref()).expect("placed");
    let error = view.placement_error.expect("placement error recorded");
    assert_eq!(error.code, "remote-launch-failed");
    let metadata = session.metadata.as_ref().unwrap();
    assert!(metadata.get(REMOTE_PLACEMENT_METADATA_KEY).is_some());

    // A keyed retry replays the failure without creating or launching again.
    let replay = create().await;
    let Err(Error::RemoteMachine(again)) = &replay else {
        panic!("expected the replayed launch-failed, got {replay:?}");
    };
    assert_eq!(again.data, failure.data);
    assert_eq!(remote.launched.lock().unwrap().len(), 1);
    let workspaces = fx
        .store
        .list_workspaces(WorkspaceListFilter::default())
        .await
        .unwrap();
    assert_eq!(workspaces.len(), 1);
}

#[intent_test_macros::daemon_test]
async fn an_omitted_placement_keeps_the_initial_agent_local() {
    let remote = Arc::new(FakeRemote::default());
    let fx = fixture(Some(remote.clone())).await;
    let result = fx
        .svc
        .create_workspace(request(&fx, json!({ "model": "sonnet4.5" })), None)
        .await
        .expect("create");
    let agent = result.initial_agent.expect("initial agent");
    assert!(agent.get("nodeId").is_none(), "{agent}");
    assert!(agent.get("placement").is_none(), "{agent}");
    assert!(result.notice.is_none());
    assert!(remote.needs.lock().unwrap().is_empty());
    assert!(remote.base_commits.lock().unwrap().is_empty());
    assert!(remote.launched.lock().unwrap().is_empty());
}

/// The wire shape: `initialAgent.placement` is the closed placement object;
/// `null` is refused as for `agent.create`.
#[test]
fn the_initial_agent_placement_wire_shape_is_closed() {
    let parse = |agent: Value| {
        serde_json::from_value::<WorkspaceCreate>(json!({ "initialAgent": agent }))
            .map(|input| input.initial_agent.unwrap().placement)
    };
    assert!(parse(json!({})).unwrap().is_none());
    let placement = parse(json!({ "placement": {"target": "remote", "os": "macos"} }))
        .unwrap()
        .expect("placement");
    assert_eq!(
        serde_json::to_value(&placement).unwrap(),
        json!({"target": "remote", "os": "macos"})
    );
    assert!(parse(json!({ "placement": null })).is_err());
    assert!(parse(json!({ "placement": {"target": "remote", "bogus": 1} })).is_err());
}

/// The first message of a remote agent cannot carry attachments, so a
/// placed initial agent with images or files (top-level or under
/// `metadata`) is refused before anything is created; empty arrays are
/// no attachments.
#[intent_test_macros::daemon_test]
async fn a_placed_initial_agent_with_attachments_creates_nothing() {
    let remote = Arc::new(FakeRemote::default());
    let fx = fixture(Some(remote.clone())).await;
    let image = json!([{ "type": "image", "mimeType": "image/png", "data": "iVBORw0KGgo=" }]);
    for agent in [
        json!({ "prompt": "look", "imageBlocks": image, "placement": {"target": "remote"} }),
        json!({
            "prompt": "look",
            "metadata": { "imageBlocks": image },
            "placement": {"target": "remote"},
        }),
    ] {
        let result = fx.svc.create_workspace(request(&fx, agent), None).await;
        assert!(
            matches!(&result, Err(Error::InvalidParams(m)) if m.contains("attachments are not supported on remote agents")),
            "{result:?}"
        );
    }
    assert!(remote.needs.lock().unwrap().is_empty());
    assert_nothing_created(&fx).await;

    let result = fx
        .svc
        .create_workspace(
            request(
                &fx,
                json!({
                    "prompt": "look",
                    "imageBlocks": [],
                    "fileBlocks": [],
                    "placement": {"target": "remote"},
                }),
            ),
            None,
        )
        .await
        .expect("empty attachment arrays are no attachments");
    assert_eq!(result.initial_agent.unwrap()["nodeId"], "node-1");
}

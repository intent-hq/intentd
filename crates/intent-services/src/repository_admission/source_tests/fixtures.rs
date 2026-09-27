use std::path::{Path, PathBuf};
use std::process::Command;

use intent_core::{
    chief_workspace, ExecutionScope, RepositoryAvailability, RepositoryContextRevision,
    RepositoryProvider, RepositoryRootId, RepositoryRootKind, RepositoryTarget,
    RepositoryTargetContext, SavedReviewSelection, Workspace, WorkspaceId,
};
use intent_sourcecontrol::remote_project::{CanonicalRemoteResolver, RemoteInstance};
use intent_store::Store;

use crate::repository_context_reader::{
    AdmittedRepositoryRoot, GitConfigEnvironment, RepositoryContextInput,
};

pub(crate) struct Fixture {
    pub dir: tempfile::TempDir,
    pub store: Store,
    pub path: PathBuf,
    pub workspace: Workspace,
}

impl Fixture {
    pub async fn new() -> Self {
        let dir = crate::test_support::test_tempdir("repository-admission-sources-");
        let path = dir.path().join("repo");
        init(&path);
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let mut workspace = chief_workspace();
        workspace.id = WorkspaceId::new();
        workspace.branch = "main".into();
        workspace.repository_path = Some(path.to_str().unwrap().into());
        workspace.path = Some(dir.path().join("metadata").to_str().unwrap().into());
        store.insert_workspace(&workspace).await.unwrap();
        std::fs::write(dir.path().join("global"), "").unwrap();
        std::fs::write(dir.path().join("system"), "").unwrap();
        Self {
            dir,
            store,
            path,
            workspace,
        }
    }

    pub fn root(&self) -> RepositoryRootId {
        RepositoryRootId {
            workspace_id: self.workspace.id.clone(),
            kind: RepositoryRootKind::Primary,
        }
    }

    pub fn environment(&self) -> GitConfigEnvironment {
        GitConfigEnvironment {
            global_config: Some(self.dir.path().join("global")),
            system_config: Some(self.dir.path().join("system")),
            extra_config_paths: Vec::new(),
        }
    }

    pub fn input(root: RepositoryRootId, path: &Path) -> RepositoryContextInput {
        RepositoryContextInput {
            scope: ExecutionScope {
                daemon_id: "host-A".into(),
                authority_scope_id: "explicit-fixture".into(),
                authority_generation: 8,
            },
            revision: RepositoryContextRevision::new("boot", 3),
            roots: vec![AdmittedRepositoryRoot {
                root,
                path: path.to_path_buf(),
                saved_selection: SavedReviewSelection::Automatic,
                explicit_target: None,
                targets: vec![target("a"), target("b")],
            }],
        }
    }

    pub fn git(&self, path: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(path)
            .args(args)
            .env("GIT_CONFIG_GLOBAL", self.dir.path().join("global"))
            .env("GIT_CONFIG_SYSTEM", self.dir.path().join("system"))
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_COMMON_DIR")
            .output()
            .unwrap();
        assert!(output.status.success(), "fixture Git failed");
        String::from_utf8(output.stdout).unwrap()
    }
}

pub(super) fn resolver() -> CanonicalRemoteResolver {
    CanonicalRemoteResolver::new(vec![RemoteInstance::github_com()], Vec::new()).unwrap()
}

fn target(name: &str) -> RepositoryTargetContext {
    RepositoryTargetContext {
        target: RepositoryTarget {
            provider: RepositoryProvider::Github,
            instance_base_url: "https://github.com".into(),
            project_path: format!("team/{name}"),
        },
        provider_project_id: None,
        connection: None,
        availability: RepositoryAvailability::Disconnected,
        capabilities: Vec::new(),
    }
}

pub(super) fn init(path: &Path) {
    let repo = git2::Repository::init_opts(
        path,
        git2::RepositoryInitOptions::new().initial_head("main"),
    )
    .unwrap();
    let tree_id = repo.index().unwrap().write_tree().unwrap();
    let tree = repo.find_tree(tree_id).unwrap();
    let signature = git2::Signature::now("Fixture", "fixture@example.invalid").unwrap();
    repo.commit(Some("HEAD"), &signature, &signature, "fixture", &tree, &[])
        .unwrap();
}

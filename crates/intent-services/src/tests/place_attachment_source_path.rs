//! `file.placeAttachment` `sourcePath` arm: the host-local copy fast path must
//! never be reachable by a non-administrator workspace guest. A guest that
//! could name any absolute host path would copy the host owner's files
//! (`~/.ssh`, credentials, the daemon store) into the workspace and read them
//! back with `file.read`, escaping workspace confinement.

use intent_core::{
    now_iso, with_caller, Caller, Error, HostRole, Principal, PrincipalId, WorkspaceApi,
    WorkspaceId, WorkspaceRole,
};
use intent_store::Store;

use crate::test_support::capability_fixtures::host_role;
use crate::test_support::fixtures::{workspace, TempDb};
use crate::test_support::test_tempdir;
use crate::Services;

struct Fixture {
    svc: Services,
    ws: WorkspaceId,
    guest: PrincipalId,
    member: PrincipalId,
    secret: std::path::PathBuf,
    root: std::path::PathBuf,
    _dirs: (tempfile::TempDir, tempfile::TempDir),
}

async fn fixture(tmp: &TempDb) -> Fixture {
    let (svc, _owner, member) = host_role::fixture(tmp).await;
    let store: &Store = &svc.store;

    let root_guard = test_tempdir("intentd-placeatt-guest-ws-");
    let root = std::fs::canonicalize(root_guard.path()).unwrap();
    git2::Repository::init(&root).unwrap();
    let ws = WorkspaceId::new();
    let mut w = workspace(&ws);
    w.worktree_path = Some(root.to_string_lossy().into_owned());
    store.insert_workspace(&w).await.unwrap();

    let outside_guard = test_tempdir("intentd-placeatt-guest-host-");
    let secret = std::fs::canonicalize(outside_guard.path())
        .unwrap()
        .join("id_ed25519");
    std::fs::write(&secret, b"host-owner-private-key").unwrap();

    let guest = Principal {
        id: PrincipalId::new(),
        identity: None,
        github_user_id: None,
        login: None,
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: now_iso(),
        updated_at: now_iso(),
    };
    store.upsert_principal(&guest).await.unwrap();
    store
        .add_workspace_member(&ws, &guest.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();

    Fixture {
        svc,
        ws,
        guest: guest.id,
        member,
        secret,
        root,
        _dirs: (root_guard, outside_guard),
    }
}

fn guest_caller(id: &PrincipalId) -> Caller {
    Caller::Wire {
        principal_id: id.clone(),
        host_role: HostRole::Guest,
    }
}

async fn place_source(f: &Fixture, caller: Caller) -> intent_core::Result<serde_json::Value> {
    let svc = &f.svc;
    let ws = f.ws.clone();
    let src = f.secret.to_string_lossy().into_owned();
    with_caller(caller, async move {
        svc.file_place_attachment(ws, "loot.txt".to_string(), None, Some(src), None, None)
            .await
    })
    .await
}

#[tokio::test]
async fn guest_cannot_copy_host_files_via_source_path() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;

    let result = place_source(&f, guest_caller(&f.guest)).await;
    assert!(
        matches!(result, Err(Error::Forbidden(_))),
        "a workspace guest must not reach the host-local sourcePath arm: {result:?}"
    );
    let attachments = f.root.join(".intent/attachments");
    let leaked = std::fs::read_dir(&attachments).is_ok_and(|entries| {
        entries
            .filter_map(Result::ok)
            .any(|e| e.file_name() != ".gitignore")
    });
    assert!(!leaked, "no file may be placed for a refused guest call");
}

#[tokio::test]
async fn guest_keeps_the_inline_data_arm() {
    use base64::Engine as _;
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let b64 = base64::engine::general_purpose::STANDARD.encode(b"guest upload");
    let svc = &f.svc;
    let ws = f.ws.clone();
    let placed = with_caller(guest_caller(&f.guest), async move {
        svc.file_place_attachment(ws, "note.txt".to_string(), Some(b64), None, None, None)
            .await
    })
    .await
    .expect("a guest may still attach inline bytes");
    assert_eq!(placed["size"], 12);
}

#[tokio::test]
async fn host_member_and_owner_keep_the_source_path_arm() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;

    let placed = place_source(&f, host_role::caller(&f.member))
        .await
        .expect("a host member keeps the same-host fast path");
    assert_eq!(placed["size"], 22);

    let placed = place_source(&f, Caller::Daemon)
        .await
        .expect("the owner's local client keeps the same-host fast path");
    assert_eq!(placed["size"], 22);
}

//! Workspace import × runtime, on the composed services, through public
//! requests only: an import commit rehydrates the imported hooks and refreshes
//! the runtime's script cache, a failed commit leaves that cache alone, and
//! another workspace's scripts are untouched either way.
//!
//! The runtime halves of `intent-svc-transfer`'s
//! `import_lifecycle_begin_chunk_commit`,
//! `import_reclaims_orphan_metadata_atomically_with_attachment_files` and
//! `transfer_round_trip_between_two_stacks`; those keep the transfer-side
//! assertions (rows, files, fences) and build no runtime.

use std::fmt::Write as _;
use std::future::Future;
use std::io::Write as _;

use base64::Engine as _;
use intent_core::transfer::{
    TransferGitSummary, TransferManifest, CREATING_INTENTD_VERSION, TRANSFER_FORMAT_VERSION,
};
use intent_core::{with_caller, Caller, PrincipalId, WorkspaceApi, WorkspaceId};
use intent_services::Services;
use intent_store::Store;
use serde_json::{json, Value};
use sha2::Digest as _;

const T: &str = "2026-08-11T00:00:00Z";

struct Fixture {
    #[cfg_attr(
        not(unix),
        expect(dead_code, reason = "only the unix script run reads the checkout")
    )]
    dir: tempfile::TempDir,
    services: Services,
    owner: PrincipalId,
}

impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(&dir.path().join("e2e.db"))
            .await
            .expect("open store");
        let owner = store
            .get_primary_principal()
            .await
            .expect("primary principal")
            .id;
        // Credentials stay in this TempDir, not the developer's own secrets file.
        let secrets = intent_core::FileSecretStore::with_path(dir.path().join("secrets.json"));
        let services = intent_app::compose(
            Services::new_repository_fixture(store, secrets, None)
                .with_workspaces_root(dir.path().join("workspaces"))
                .with_assets_root(dir.path().join("assets")),
        );
        Self {
            dir,
            services,
            owner,
        }
    }

    /// Run `call` as the app: the host owner's wire identity.
    async fn as_app<T>(&self, call: impl Future<Output = T>) -> T {
        with_caller(
            Caller::Wire {
                principal_id: self.owner.clone(),
                host_role: intent_core::HostRole::Owner,
            },
            call,
        )
        .await
    }

    async fn scripts(&self, ws: &str) -> Value {
        self.as_app(self.services.script_list(WorkspaceId::from(ws)))
            .await
            .expect("script.list")
    }

    /// `workspace.import.begin` → one chunk → `workspace.import.commit`.
    async fn import(&self, archive: &[u8], ws: &str) -> (String, intent_core::Result<Value>) {
        let sha = sha2::Sha256::digest(archive)
            .iter()
            .fold(String::new(), |mut hex, b| {
                let _ = write!(hex, "{b:02x}");
                hex
            });
        let begun = self
            .as_app(self.services.workspace_import_begin(
                serde_json::to_value(manifest(ws)).expect("manifest json"),
                archive.len() as u64,
                sha,
            ))
            .await
            .expect("workspace.import.begin");
        let id = begun["importId"].as_str().expect("importId").to_string();
        self.as_app(self.services.workspace_import_chunk(
            id.clone(),
            0,
            base64::engine::general_purpose::STANDARD.encode(archive),
        ))
        .await
        .expect("workspace.import.chunk");
        let committed = self
            .as_app(self.services.workspace_import_commit(id.clone()))
            .await;
        (id, committed)
    }
}

fn manifest(ws: &str) -> TransferManifest {
    TransferManifest {
        format_version: TRANSFER_FORMAT_VERSION,
        creating_intentd_version: CREATING_INTENTD_VERSION.to_string(),
        workspace_id: WorkspaceId::from(ws),
        created_at: T.to_string(),
        tables: vec![],
        assets: vec![],
        attachments: vec![],
        git: TransferGitSummary {
            has_repository: false,
            branch: None,
            dirty_files: vec![],
            sandbox_branches: vec![],
            submodules: vec![],
        },
    }
}

/// manifest.json + rows/*.jsonl + one attachment file, as the exporter lays
/// out an archive.
fn archive(ws: &str, rows: &[(&str, Vec<Value>)]) -> Vec<u8> {
    let mut buf = std::io::Cursor::new(Vec::new());
    let mut zip = zip::ZipWriter::new(&mut buf);
    let options: zip::write::FileOptions<'_, ()> = zip::write::FileOptions::default();
    zip.start_file("manifest.json", options).expect("manifest");
    zip.write_all(
        serde_json::to_string(&manifest(ws))
            .expect("manifest json")
            .as_bytes(),
    )
    .expect("manifest bytes");
    for (table, objects) in rows {
        zip.start_file(format!("rows/{table}.jsonl"), options)
            .expect("row file");
        for o in objects {
            zip.write_all(format!("{o}\n").as_bytes())
                .expect("row bytes");
        }
    }
    zip.start_file("attachments/att-live", options)
        .expect("attachment");
    zip.write_all(b"attachment-bytes")
        .expect("attachment bytes");
    zip.finish().expect("finish zip");
    buf.into_inner()
}

fn rows(ws: &str) -> Vec<(&'static str, Vec<Value>)> {
    vec![
        (
            "workspace",
            vec![json!({
                "id": ws, "title": "Imported", "branch": "main", "status": "Active",
                "worktree_path": "/src/ws/repo", "created_at": T, "updated_at": T
            })],
        ),
        (
            "agent_session",
            vec![json!({
                "id": "agent-live", "workspace_id": ws, "name": "A", "status": "active",
                "is_active": 1, "created_at": T, "updated_at": T
            })],
        ),
        (
            "hook",
            vec![json!({
                "hook_id": "h-1", "workspace_id": ws, "agent_id": "agent-live",
                "name": "H", "code": "return { dispatch: false }", "delay_ms": 10000,
                "state": "scheduled", "created_at": T,
                "next_run_at": "2027-01-01T00:00:00Z", "expires_at": "2027-01-01T00:30:00Z"
            })],
        ),
        (
            "attachments",
            vec![json!({
                "id": "att-live", "workspace_id": ws, "file_name": "doc.pdf",
                "mime_type": "application/pdf", "size": 16, "uploaded_at": T,
                "stored_path": ".intent/attachments/doc.pdf"
            })],
        ),
        (
            "script",
            vec![json!({
                "id": "script-reimport", "workspace_id": ws, "name": "Script",
                "command": "echo imported > imported-script.txt",
                "cwd": ".intent/attachments", "mode": "command", "source": "user",
                "created_at": "t0"
            })],
        ),
    ]
}

#[track_caller]
fn script<'a>(list: &'a Value, id: &str) -> &'a Value {
    list["scripts"]
        .as_array()
        .expect("scripts")
        .iter()
        .find(|script| script["id"] == id)
        .unwrap_or_else(|| panic!("script {id} listed: {list}"))
}

/// A re-import over a workspace whose scripts the runtime already cached:
/// a committed import rehydrates the imported hook and refreshes the cached
/// definitions; a commit that fails late leaves the cache as it was.
#[tokio::test]
async fn import_commit_rehydrates_hooks_and_refreshes_the_script_cache() {
    for fail_late in [false, true] {
        let f = Fixture::new().await;
        let ws = "ws-reimport";
        // The workspace's stale script row (no workspace row: the import
        // creates it), plus a script the archive does not list and another
        // workspace's script.
        let mut stale = rows(ws)
            .into_iter()
            .filter(|(table, _)| *table == "script")
            .map(|(table, rows)| (table.to_string(), rows))
            .collect::<Vec<_>>();
        let scripts = &mut stale[0].1;
        scripts[0]["command"] = json!("echo stale");
        scripts[0]["cwd"] = json!("old-cwd");
        scripts.push(json!({
            "id": "script-unlisted", "workspace_id": ws, "name": "Unlisted",
            "command": "echo unlisted", "mode": "command", "source": "user",
            "created_at": "t0"
        }));
        scripts.push(json!({
            "id": "script-keeper", "workspace_id": "keeper", "name": "Keep",
            "command": "echo keep", "mode": "command", "source": "user",
            "created_at": "t0"
        }));
        f.services
            .store()
            .transfer_import_rows(&stale)
            .await
            .expect("seed stale rows");
        assert_eq!(f.services.hydrate_scripts().await.expect("hydrate"), 3);
        let cached_before = f.scripts(ws).await;
        assert_eq!(
            script(&cached_before, "script-reimport")["command"],
            "echo stale"
        );
        let keeper_before = f.scripts("keeper").await;

        let mut archived = rows(ws);
        if fail_late {
            archived
                .iter_mut()
                .find(|(table, _)| *table == "attachments")
                .expect("attachments")
                .1
                .push(json!({"id": "bad", "workspace_id": ws}));
        }
        let (id, committed) = f.import(&archive(ws, &archived), ws).await;
        if fail_late {
            let err = committed.expect_err("a late row failure fails the commit");
            assert!(err.to_string().contains("attachments.file_name"), "{err}");
            assert_eq!(f.scripts(ws).await, cached_before);
            f.as_app(f.services.workspace_import_abort(id))
                .await
                .expect("workspace.import.abort");
        } else {
            let committed = committed.expect("workspace.import.commit");
            assert_eq!(committed["rehydrated"]["hooks"], 1, "{committed}");
            let scripts = f.scripts(ws).await;
            let imported = script(&scripts, "script-reimport");
            assert_eq!(imported["command"], "echo imported > imported-script.txt");
            assert_eq!(imported["cwd"], ".intent/attachments");
            assert_eq!(
                script(&scripts, "script-unlisted")["command"],
                "echo unlisted"
            );
            #[cfg(unix)]
            {
                f.as_app(f.services.script_run(
                    WorkspaceId::from(ws),
                    "script-reimport".into(),
                    None,
                    Some(5),
                ))
                .await
                .expect("script.run");
                let written = f
                    .dir
                    .path()
                    .join("workspaces")
                    .join(ws)
                    .join("repo/.intent/attachments/imported-script.txt");
                assert_eq!(
                    std::fs::read_to_string(written)
                        .expect("script output")
                        .trim(),
                    "imported"
                );
            }
        }
        assert_eq!(f.scripts("keeper").await, keeper_before);
    }
}

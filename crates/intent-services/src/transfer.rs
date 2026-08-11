//! `workspace.transfer.plan` (PROTOCOL §5.1): read-only preview of a
//! workspace transfer — the versioned manifest plus the size estimate the FE
//! wizard shows before starting a transfer. No side effects: nothing is
//! written, snapshotted, or bundled here (the export archive itself is built
//! by the transfer orchestrator, a separate surface).

use std::path::{Path, PathBuf};

use intent_core::transfer::{
    TransferAsset, TransferGitSummary, TransferManifest, TransferPlan, TransferTableStat,
    TransferWarning, TRANSFER_FORMAT_VERSION,
};
use intent_core::{clock::now_iso, AgentStatus, Error, Result, WorkspaceId};
use intent_store::SandboxStatus;

use crate::{git_ops, Services};

impl Services {
    /// Build the transfer plan for `id`: per-table row stats (spec §1
    /// inventory, `event` excluded), asset list with sizes, git state summary
    /// (branch, dirty files, sandbox branches), an estimated git bundle size,
    /// and non-blocking pre-flight warnings. `NotFound` for unknown ids;
    /// the chief workspace is not transferable (`InvalidParams`).
    pub(crate) async fn workspace_transfer_plan_op(&self, id: WorkspaceId) -> Result<TransferPlan> {
        if id.is_chief() {
            return Err(Error::InvalidParams(
                "The chief workspace cannot be transferred".to_string(),
            ));
        }
        let ws = self.store.get_workspace(&id).await?;

        let tables = self.store.transfer_table_stats(&id).await?;
        let assets = self.transfer_assets(&id).await;

        // Sandbox branches ride in the bundle only while they can still hold
        // unmerged work; merged/discarded sandboxes may have deleted branches.
        let sandboxes = self.store.list_sandboxes(&id).await?;
        let live_sandboxes: Vec<_> = sandboxes
            .iter()
            .filter(|s| {
                matches!(
                    s.status,
                    SandboxStatus::Created
                        | SandboxStatus::Merging
                        | SandboxStatus::MergePending
                        | SandboxStatus::ConflictBounced
                )
            })
            .collect();
        let sandbox_branches: Vec<String> =
            live_sandboxes.iter().map(|s| s.branch.clone()).collect();

        let worktree = git_ops::worktree_path(&ws);
        let (git, estimated_git_bundle_bytes) = match worktree {
            Some(root) if intent_git::is_repository(&root) => {
                let branches = sandbox_branches.clone();
                tokio::task::spawn_blocking(move || {
                    let status = intent_git::status::status(&root);
                    let (branch, dirty_files) = match status {
                        Ok(s) => {
                            // status() emits one entry per index/worktree change, so a
                            // path that is both staged and unstaged appears twice —
                            // dedupe to one dirty file per path.
                            let mut paths: Vec<String> =
                                s.files.into_iter().map(|f| f.path).collect();
                            paths.sort();
                            paths.dedup();
                            (Some(s.branch), paths)
                        }
                        Err(_) => (intent_git::status::current_branch_at(&root), Vec::new()),
                    };
                    let bundle = estimate_bundle_bytes(&root, &branches);
                    (
                        TransferGitSummary {
                            has_repository: true,
                            branch,
                            dirty_files,
                            sandbox_branches: branches,
                        },
                        bundle,
                    )
                })
                .await
                .map_err(|e| Error::Internal(format!("transfer plan git scan failed: {e}")))?
            }
            _ => (
                TransferGitSummary {
                    has_repository: false,
                    branch: None,
                    dirty_files: Vec::new(),
                    sandbox_branches,
                },
                0,
            ),
        };

        let mut warnings = Vec::new();
        let sessions = self.store.list_agent_session_summaries(&id).await?;
        let running = sessions
            .iter()
            .filter(|s| {
                s.is_active
                    || matches!(
                        s.status,
                        AgentStatus::Active | AgentStatus::Pending | AgentStatus::Processing
                    )
            })
            .count();
        if running > 0 {
            warnings.push(TransferWarning {
                code: "agents-running".to_string(),
                message: format!(
                    "{running} agent(s) are running or starting; they will be stopped \
                     before export and marked interrupted on the target"
                ),
            });
        }
        if !git.dirty_files.is_empty() {
            warnings.push(TransferWarning {
                code: "uncommitted-changes".to_string(),
                message: format!(
                    "{} uncommitted file(s) will be snapshotted as a WIP commit in the bundle",
                    git.dirty_files.len()
                ),
            });
        }
        if !live_sandboxes.is_empty() {
            warnings.push(TransferWarning {
                code: "unmerged-sandboxes".to_string(),
                message: format!(
                    "{} unmerged sandbox(es); their branches ride in the bundle and are \
                     re-provisioned on the target",
                    live_sandboxes.len()
                ),
            });
        }

        let db_row_bytes: u64 = tables
            .iter()
            .map(|t: &TransferTableStat| t.approx_bytes.max(0) as u64)
            .sum();
        let asset_bytes: u64 = assets.iter().map(|a| a.size_bytes).sum();

        let manifest = TransferManifest {
            format_version: TRANSFER_FORMAT_VERSION,
            creating_intentd_version: env!("CARGO_PKG_VERSION").to_string(),
            workspace_id: id,
            created_at: now_iso(),
            tables,
            assets,
            git,
        };

        Ok(TransferPlan {
            total_size_bytes: db_row_bytes + asset_bytes + estimated_git_bundle_bytes,
            db_row_bytes,
            asset_bytes,
            estimated_git_bundle_bytes,
            manifest,
            warnings,
        })
    }

    /// List `<assets_root>/<workspaceId>/` as manifest assets (id = file
    /// name), sorted by id. Missing root/dir or read errors degrade to an
    /// empty list — a plan must not fail because a workspace has no assets.
    async fn transfer_assets(&self, id: &WorkspaceId) -> Vec<TransferAsset> {
        let Some(root) = self.assets_root.clone() else {
            return Vec::new();
        };
        let dir: PathBuf = root.join(&id.0);
        let mut assets = Vec::new();
        let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
            return assets;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let Ok(meta) = entry.metadata().await else {
                continue;
            };
            if !meta.is_file() {
                continue;
            }
            assets.push(TransferAsset {
                id: entry.file_name().to_string_lossy().to_string(),
                size_bytes: meta.len(),
            });
        }
        assets.sort_by(|a, b| a.id.cmp(&b.id));
        assets
    }
}

/// Estimate the size of the git bundle a transfer would carry: the on-disk
/// bytes of all objects reachable from HEAD plus the (existing) sandbox
/// branches, via `git rev-list --disk-usage --objects`. An estimate only —
/// the real bundle also snapshots dirty state as WIP commits and recompresses
/// on pack — and degrades to 0 when git or the refs are unavailable.
fn estimate_bundle_bytes(root: &Path, sandbox_branches: &[String]) -> u64 {
    let mut refs: Vec<String> = vec!["HEAD".to_string()];
    for branch in sandbox_branches {
        let exists = std::process::Command::new("git")
            .arg("-C")
            .arg(root)
            .args(["rev-parse", "--verify", "--quiet"])
            .arg(format!("refs/heads/{branch}"))
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if exists {
            refs.push(branch.clone());
        }
    }
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["rev-list", "--disk-usage", "--objects"])
        .args(&refs)
        .output();
    match out {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout)
            .trim()
            .parse::<u64>()
            .unwrap_or(0),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use intent_core::{AgentId, AgentSession, AgentStatus, NoteId, WorkspaceId};
    use intent_store::{Sandbox, SandboxStatus, Store};

    use crate::tests::{workspace, TempDb};
    use crate::Services;

    fn run_git(dir: &std::path::Path, args: &[&str]) {
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t")
            .output()
            .expect("run git");
        assert!(
            out.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn new(prefix: &str) -> Self {
            let p = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&p).expect("mkdir");
            Self(p)
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn session(agent_id: &AgentId, ws: &WorkspaceId, status: AgentStatus) -> AgentSession {
        AgentSession {
            id: agent_id.clone(),
            workspace_id: ws.clone(),
            backend_session_id: None,
            acp_session_id: None,
            name: "a".to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            status,
            is_active: matches!(status, AgentStatus::Active),
            system_prompt: None,
            messages: vec![],
            created_at: intent_core::clock::now_iso(),
            updated_at: intent_core::clock::now_iso(),
            parent_agent_id: None,
            specialist: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            is_background: false,
            metadata: None,
            stats: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
        }
    }

    fn plain_note(ws: &WorkspaceId, id: &str, content: &str) -> intent_core::Note {
        use intent_core::{ContentType, Note, NoteMetadata, NoteVisibility};
        let ts = intent_core::clock::now_iso();
        Note {
            id: NoteId::from(id),
            workspace_id: ws.clone(),
            title: "Title".to_string(),
            content: content.to_string(),
            content_type: ContentType::Markdown,
            tags: vec![],
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: None,
            visibility: NoteVisibility::Workspace,
            metadata: NoteMetadata::default(),
            created_at: ts.clone(),
            rev: 0,
            updated_at: ts,
        }
    }

    /// Full plan over a seeded workspace: versioned manifest, `event`
    /// excluded, assets listed with sizes, git summary (branch, dirty files,
    /// sandbox branches), bundle estimate > 0, size breakdown sums to total,
    /// and all three pre-flight warnings raised.
    #[tokio::test]
    async fn transfer_plan_manifest_and_sizes() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();

        let repo = TempDir::new("intentd-transfer-repo");
        run_git(&repo.0, &["init", "-b", "main"]);
        std::fs::write(repo.0.join("a.txt"), "committed content").expect("write");
        run_git(&repo.0, &["add", "."]);
        run_git(&repo.0, &["commit", "-m", "init"]);
        run_git(&repo.0, &["branch", "sandbox/agent-1"]);
        // Stage dirty.txt, then modify it again so the path has both staged and
        // unstaged changes — it must still appear once in dirtyFiles.
        std::fs::write(repo.0.join("dirty.txt"), "uncommitted").expect("write");
        run_git(&repo.0, &["add", "dirty.txt"]);
        std::fs::write(repo.0.join("dirty.txt"), "uncommitted again").expect("write");

        let mut w = workspace(&ws);
        w.worktree_path = Some(repo.0.to_string_lossy().to_string());
        store.insert_workspace(&w).await.expect("ws");

        store
            .insert_note(&plain_note(&ws, "n1", "hello world"))
            .await
            .expect("note");
        let agent = AgentId::new();
        store
            .insert_agent_session(&session(&agent, &ws, AgentStatus::Active))
            .await
            .expect("session");
        store
            .append_agent_message(
                &agent,
                "user",
                &serde_json::json!([{ "type": "text", "text": "hi" }]),
                &intent_core::clock::now_iso(),
            )
            .await
            .expect("message");
        store
            .insert_sandbox(&Sandbox {
                id: "sb-1".to_string(),
                workspace_id: ws.clone(),
                agent_id: agent.clone(),
                path: "/tmp/sb".to_string(),
                branch: "sandbox/agent-1".to_string(),
                base_commit_sha: "abc".to_string(),
                snapshot_commit_sha: None,
                status: SandboxStatus::Created,
                retry_count: 0,
                created_at: intent_core::clock::now_iso(),
                updated_at: intent_core::clock::now_iso(),
            })
            .await
            .expect("sandbox");

        let assets_root = TempDir::new("intentd-transfer-assets");
        std::fs::create_dir_all(assets_root.0.join(&ws.0)).expect("mkdir assets");
        std::fs::write(assets_root.0.join(&ws.0).join("img.png"), b"12345").expect("asset");

        let svc = Services::new(store).with_assets_root(assets_root.0.clone());
        let plan = svc
            .workspace_transfer_plan_op(ws.clone())
            .await
            .expect("plan");

        let m = &plan.manifest;
        assert_eq!(
            m.format_version,
            intent_core::transfer::TRANSFER_FORMAT_VERSION
        );
        assert_eq!(m.creating_intentd_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(m.workspace_id, ws);
        assert!(m.tables.iter().all(|t| t.name != "event"));
        let table = |n: &str| m.tables.iter().find(|t| t.name == n).expect("table");
        assert_eq!(table("workspace").row_count, 1);
        assert_eq!(table("note").row_count, 1);
        assert_eq!(table("agent_session").row_count, 1);
        assert_eq!(table("agent_message").row_count, 1);
        assert_eq!(table("sandbox").row_count, 1);
        assert!(table("note").approx_bytes > 0);

        assert_eq!(m.assets.len(), 1);
        assert_eq!(m.assets[0].id, "img.png");
        assert_eq!(m.assets[0].size_bytes, 5);

        assert!(m.git.has_repository);
        assert_eq!(m.git.branch.as_deref(), Some("main"));
        assert_eq!(m.git.dirty_files, vec!["dirty.txt".to_string()]);
        assert_eq!(m.git.sandbox_branches, vec!["sandbox/agent-1".to_string()]);

        assert!(plan.db_row_bytes > 0);
        assert_eq!(plan.asset_bytes, 5);
        assert!(plan.estimated_git_bundle_bytes > 0);
        assert_eq!(
            plan.total_size_bytes,
            plan.db_row_bytes + plan.asset_bytes + plan.estimated_git_bundle_bytes
        );

        let codes: Vec<&str> = plan.warnings.iter().map(|w| w.code.as_str()).collect();
        assert!(codes.contains(&"agents-running"));
        assert!(codes.contains(&"uncommitted-changes"));
        assert!(codes.contains(&"unmerged-sandboxes"));
    }

    /// A workspace with no git repository and no assets still plans cleanly:
    /// `hasRepository: false`, zero bundle/asset bytes, no warnings for an
    /// idle agent set.
    #[tokio::test]
    async fn transfer_plan_without_repo_or_assets() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        let agent = AgentId::new();
        store
            .insert_agent_session(&session(&agent, &ws, AgentStatus::Idle))
            .await
            .expect("session");

        let svc = Services::new(store);
        let plan = svc
            .workspace_transfer_plan_op(ws.clone())
            .await
            .expect("plan");

        assert!(!plan.manifest.git.has_repository);
        assert_eq!(plan.estimated_git_bundle_bytes, 0);
        assert_eq!(plan.asset_bytes, 0);
        assert!(plan.manifest.assets.is_empty());
        assert!(plan.warnings.is_empty());
        assert_eq!(plan.total_size_bytes, plan.db_row_bytes);
    }

    /// Unknown workspaces are `NotFound`; the chief workspace is rejected.
    #[tokio::test]
    async fn transfer_plan_rejects_missing_and_chief() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let svc = Services::new(store);

        let missing = svc.workspace_transfer_plan_op(WorkspaceId::new()).await;
        assert!(matches!(missing, Err(intent_core::Error::NotFound(_))));

        let chief = svc.workspace_transfer_plan_op(WorkspaceId::chief()).await;
        assert!(matches!(chief, Err(intent_core::Error::InvalidParams(_))));
    }

    /// The plan is read-only: two consecutive calls see identical row counts
    /// and byte totals (nothing was written by the first).
    #[tokio::test]
    async fn transfer_plan_is_read_only() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        store
            .insert_note(&plain_note(&ws, "n1", "content"))
            .await
            .expect("note");

        let svc = Services::new(store);
        let first = svc.workspace_transfer_plan_op(ws.clone()).await.expect("1");
        let second = svc.workspace_transfer_plan_op(ws).await.expect("2");
        assert_eq!(first.manifest.tables, second.manifest.tables);
        assert_eq!(first.total_size_bytes, second.total_size_bytes);
    }

    /// The manifest serializes with the camelCase wire field names the FE
    /// wizard consumes.
    #[test]
    fn transfer_plan_wire_shape_is_camel_case() {
        let plan = intent_core::transfer::TransferPlan {
            manifest: intent_core::transfer::TransferManifest {
                format_version: 1,
                creating_intentd_version: "0.0.0".to_string(),
                workspace_id: WorkspaceId::from("ws-1"),
                created_at: "2026-01-01T00:00:00Z".to_string(),
                tables: vec![intent_core::transfer::TransferTableStat {
                    name: "note".to_string(),
                    row_count: 1,
                    approx_bytes: 2,
                }],
                assets: vec![intent_core::transfer::TransferAsset {
                    id: "a".to_string(),
                    size_bytes: 3,
                }],
                git: intent_core::transfer::TransferGitSummary {
                    has_repository: true,
                    branch: Some("main".to_string()),
                    dirty_files: vec![],
                    sandbox_branches: vec![],
                },
            },
            total_size_bytes: 5,
            db_row_bytes: 2,
            asset_bytes: 3,
            estimated_git_bundle_bytes: 0,
            warnings: vec![],
        };
        let v = serde_json::to_value(&plan).expect("serialize");
        assert_eq!(v["manifest"]["formatVersion"], 1);
        assert_eq!(v["manifest"]["creatingIntentdVersion"], "0.0.0");
        assert_eq!(v["manifest"]["tables"][0]["rowCount"], 1);
        assert_eq!(v["manifest"]["tables"][0]["approxBytes"], 2);
        assert_eq!(v["manifest"]["assets"][0]["sizeBytes"], 3);
        assert_eq!(v["manifest"]["git"]["hasRepository"], true);
        assert_eq!(v["totalSizeBytes"], 5);
        assert_eq!(v["dbRowBytes"], 2);
        assert_eq!(v["assetBytes"], 3);
        assert_eq!(v["estimatedGitBundleBytes"], 0);
    }
}

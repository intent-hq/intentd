//! `workspace.transfer.plan` (PROTOCOL §5.1): read-only preview of a
//! workspace transfer — the versioned manifest plus the size estimate the FE
//! wizard shows before starting a transfer. No side effects: nothing is
//! written, snapshotted, or bundled here (the export archive itself is built
//! by the transfer orchestrator, a separate surface).

use std::path::{Path, PathBuf};

use intent_core::transfer::{
    TransferAsset, TransferAttachment, TransferGitSummary, TransferManifest, TransferPlan,
    TransferSubmoduleSummary, TransferTableStat, TransferWarning, TRANSFER_FORMAT_VERSION,
};
use intent_core::{clock::now_iso, AgentStatus, Error, Result, Workspace, WorkspaceId};
use intent_store::SandboxStatus;

use crate::transfer_submodules::{
    estimate_submodule_bundle_bytes, find_unpublished_submodules, UnpublishedSubmodule,
};
use crate::{file_ops, git_ops, Services};

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
        let attachments = self.transfer_attachments(&id, &ws).await?;

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
        let sandbox_paths: Vec<PathBuf> = live_sandboxes
            .iter()
            .map(|s| PathBuf::from(&s.path))
            .collect();

        let worktree = git_ops::worktree_path(&ws);
        let (git, estimated_git_bundle_bytes, nested_repo_dirs, submodule_bundle_bytes) =
            match worktree {
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
                        let nested = crate::nested_repos::nested_repo_dirs(&root);
                        let (submodules, submodule_bytes) =
                            scan_unpublished_submodules(&root, &sandbox_paths);
                        (
                            TransferGitSummary {
                                has_repository: true,
                                branch,
                                dirty_files,
                                sandbox_branches: branches,
                                submodules,
                            },
                            bundle + submodule_bytes,
                            nested,
                            submodule_bytes,
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
                        submodules: Vec::new(),
                    },
                    0,
                    Vec::new(),
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
        if !nested_repo_dirs.is_empty() {
            warnings.push(TransferWarning {
                code: "nested-repos-skipped".to_string(),
                message: format!(
                    "{} nested git repo(s)/worktree(s) will not travel with the export: {}",
                    nested_repo_dirs.len(),
                    nested_repo_dirs.join(", ")
                ),
            });
        }
        if let Some(message) = submodule_warning(&git.submodules, submodule_bundle_bytes) {
            warnings.push(TransferWarning {
                code: "submodule-unpublished-commits".to_string(),
                message,
            });
        }

        let db_row_bytes: u64 = tables
            .iter()
            .map(|t: &TransferTableStat| t.approx_bytes.max(0).cast_unsigned())
            .sum();
        let asset_bytes: u64 = assets.iter().map(|a| a.size_bytes).sum();
        let attachment_bytes: u64 = attachments.iter().map(|a| a.size_bytes).sum();

        let manifest = TransferManifest {
            format_version: TRANSFER_FORMAT_VERSION,
            creating_intentd_version: env!("CARGO_PKG_VERSION").to_string(),
            workspace_id: id,
            created_at: now_iso(),
            tables,
            assets,
            attachments,
            git,
        };

        Ok(TransferPlan {
            total_size_bytes: db_row_bytes
                + asset_bytes
                + attachment_bytes
                + estimated_git_bundle_bytes,
            db_row_bytes,
            asset_bytes,
            attachment_bytes,
            estimated_git_bundle_bytes,
            manifest,
            warnings,
        })
    }

    /// List the workspace's attachment-registry rows as manifest entries,
    /// probing each stored file in the canonical workspace store. A row whose
    /// file is gone (deleted-is-deleted) is listed with `exists: false` and 0
    /// bytes — the row still transfers, the archive just carries no file
    /// entry for it. A registry row whose `stored_path` escapes the workspace
    /// root is treated the same way (never read outside the store).
    pub(crate) async fn transfer_attachments(
        &self,
        id: &WorkspaceId,
        ws: &Workspace,
    ) -> Result<Vec<TransferAttachment>> {
        let records = self.store.list_attachments(id).await?;
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let root = file_ops::workspace_root(ws);
        Ok(records
            .into_iter()
            .map(|r| {
                let size = if root.is_empty() {
                    None
                } else {
                    file_ops::resolve_attachment_source(&root, &r.stored_path)
                        .ok()
                        .and_then(|p| std::fs::metadata(p).ok())
                        .filter(std::fs::Metadata::is_file)
                        .map(|m| m.len())
                };
                TransferAttachment {
                    id: r.id,
                    file_name: r.file_name,
                    size_bytes: size.unwrap_or(0),
                    exists: size.is_some(),
                }
            })
            .collect())
    }

    /// List `<assets_root>/<workspaceId>/` as manifest assets (id = file
    /// name), sorted by id. Missing root/dir or read errors degrade to an
    /// empty list — a plan must not fail because a workspace has no assets.
    /// `<assetId>.meta.json` sidecars are intentionally listed as first-class
    /// entries: the archive carries them, so the manifest (and its byte
    /// total) reflects exactly what a transfer would copy.
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
///
/// Sandbox branches are resolved in the worktree repo, while the bundler
/// (`transfer_git`) fetches each branch from its sandbox's own repo — so for
/// `CoW` sandboxes whose branch exists only in the sandbox repo, the estimate
/// may undercount those refs.
fn estimate_bundle_bytes(root: &Path, sandbox_branches: &[String]) -> u64 {
    let mut refs: Vec<String> = vec!["HEAD".to_string()];
    for branch in sandbox_branches {
        let exists = std::process::Command::new("git")
            .arg("-C")
            .arg(root)
            .args(["rev-parse", "--verify", "--quiet"])
            .arg(format!("refs/heads/{branch}"))
            .output()
            .is_ok_and(|o| o.status.success());
        if exists {
            // Push the unambiguous form: a worktree path or `-`-prefixed name
            // matching the short branch name would otherwise break rev-list.
            refs.push(format!("refs/heads/{branch}"));
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

/// Detect submodules pointing at unpublished commits in the worktree
/// (`carried: true`, bundled — together with the published ancestors a
/// nested finding needs, `published: true`) and in each live sandbox
/// checkout (`carried: false`, reported only; published ancestors are
/// skipped there since nothing is bundled), deduped by `(path, commit_sha)`
/// with worktree findings taking precedence. Returns the summaries plus the
/// estimated bundle bytes of the carried ones. Detection failures degrade to
/// an empty result — the plan must never fail on a submodule scan.
fn scan_unpublished_submodules(
    root: &Path,
    sandbox_paths: &[PathBuf],
) -> (Vec<TransferSubmoduleSummary>, u64) {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    let mut bytes = 0u64;
    let summarize = |s: &UnpublishedSubmodule, carried: bool| TransferSubmoduleSummary {
        name: s.name.clone(),
        path: s.path.clone(),
        commit_sha: s.commit_sha.clone(),
        branch: s.branch.clone(),
        carried,
        published: s.published,
    };
    for sub in find_unpublished_submodules(root).unwrap_or_default() {
        if seen.insert((sub.path.clone(), sub.commit_sha.clone())) {
            bytes += estimate_submodule_bundle_bytes(&sub);
            out.push(summarize(&sub, true));
        }
    }
    for sandbox in sandbox_paths {
        if !intent_git::is_repository(sandbox) {
            continue;
        }
        for sub in find_unpublished_submodules(sandbox).unwrap_or_default() {
            if !sub.published && seen.insert((sub.path.clone(), sub.commit_sha.clone())) {
                out.push(summarize(&sub, false));
            }
        }
    }
    (out, bytes)
}

/// The single `submodule-unpublished-commits` warning message, or `None`
/// when no submodule points at an unpublished commit. Published ancestors
/// bundled for a nested finding are named in a trailing clause, not counted
/// as unpublished.
fn submodule_warning(
    submodules: &[TransferSubmoduleSummary],
    carried_bytes: u64,
) -> Option<String> {
    use std::fmt::Write as _;
    if submodules.is_empty() {
        return None;
    }
    let describe = |s: &TransferSubmoduleSummary| {
        let short: String = s.commit_sha.chars().take(7).collect();
        match &s.branch {
            Some(b) => format!("{} @ {short} ({b})", s.path),
            None => format!("{} @ {short}", s.path),
        }
    };
    let carried: Vec<String> = submodules
        .iter()
        .filter(|s| s.carried && !s.published)
        .map(describe)
        .collect();
    let carried_parents: Vec<String> = submodules
        .iter()
        .filter(|s| s.carried && s.published)
        .map(describe)
        .collect();
    let sandbox_only: Vec<String> = submodules
        .iter()
        .filter(|s| !s.carried)
        .map(describe)
        .collect();
    let mut message = if carried.is_empty() {
        format!(
            "{} submodule(s) point at commits not on any remote.",
            sandbox_only.len()
        )
    } else {
        format!(
            "{} submodule(s) point at commits not on any remote and will ride in the archive (~{}): {}. \
             Transfer will not push them; publish the branches yourself when ready.",
            carried.len(),
            human_bytes(carried_bytes),
            carried.join(", ")
        )
    };
    if !carried_parents.is_empty() {
        let _ = write!(
            message,
            " Also bundled so the nested submodule(s) can be checked out: {}.",
            carried_parents.join(", ")
        );
    }
    if !sandbox_only.is_empty() {
        let _ = write!(
            message,
            " Not carried (sandbox only): {}.",
            sandbox_only.join(", ")
        );
    }
    Some(message)
}

/// Approximate human-readable size (`512 B`, `1.5 KB`, `41 MB`) for warning
/// text: one decimal below 10 units, none above.
fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut tenths = bytes.saturating_mul(10);
    let mut unit = 0;
    while tenths >= 10 * 1024 && unit < UNITS.len() - 1 {
        tenths /= 1024;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else if tenths >= 100 {
        format!("{} {}", tenths / 10, UNITS[unit])
    } else {
        format!("{}.{} {}", tenths / 10, tenths % 10, UNITS[unit])
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
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
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
            file_blocks: None,
            is_background: false,
            metadata: None,
            stats: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
            retired_at: None,
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

        // Two registry rows: one with its stored file present, one whose
        // file was deleted out-of-band (deleted-is-deleted). The ignore-all
        // marker mirrors place_attachment so the store stays out of git
        // status (and out of dirtyFiles).
        let att_dir = repo.0.join(".intent/attachments");
        std::fs::create_dir_all(&att_dir).expect("attachments dir");
        std::fs::write(att_dir.join(".gitignore"), "*\n").expect("marker");
        std::fs::write(att_dir.join("doc.pdf"), b"attachment-bytes").expect("attachment");
        for (id, name) in [("att-1", "doc.pdf"), ("att-2", "gone.txt")] {
            store
                .insert_attachment(&intent_store::AttachmentRecord {
                    id: id.to_string(),
                    workspace_id: ws.clone(),
                    file_name: name.to_string(),
                    mime_type: None,
                    size: 16,
                    uploaded_at: intent_core::clock::now_iso(),
                    stored_path: format!(".intent/attachments/{name}"),
                })
                .await
                .expect("attachment row");
        }

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

        assert_eq!(m.attachments.len(), 2);
        let att = |id: &str| m.attachments.iter().find(|a| a.id == id).expect("att");
        assert!(att("att-1").exists);
        assert_eq!(att("att-1").size_bytes, 16);
        assert_eq!(att("att-1").file_name, "doc.pdf");
        assert!(!att("att-2").exists, "deleted file → exists: false");
        assert_eq!(att("att-2").size_bytes, 0);

        assert!(plan.db_row_bytes > 0);
        assert_eq!(plan.asset_bytes, 5);
        assert_eq!(plan.attachment_bytes, 16);
        assert!(plan.estimated_git_bundle_bytes > 0);
        assert_eq!(
            plan.total_size_bytes,
            plan.db_row_bytes
                + plan.asset_bytes
                + plan.attachment_bytes
                + plan.estimated_git_bundle_bytes
        );

        let codes: Vec<&str> = plan.warnings.iter().map(|w| w.code.as_str()).collect();
        assert!(codes.contains(&"agents-running"));
        assert!(codes.contains(&"uncommitted-changes"));
        assert!(codes.contains(&"unmerged-sandboxes"));
    }

    /// A repo whose only anomaly is an untracked nested git repo plans
    /// cleanly: the nested dir never reaches `dirtyFiles` (so no
    /// `uncommitted-changes` warning) but the `nested-repos-skipped` warning
    /// names it so the user knows it will not travel.
    #[tokio::test]
    async fn transfer_plan_warns_on_nested_repos() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();

        let repo = TempDir::new("intentd-transfer-nested");
        run_git(&repo.0, &["init", "-b", "main"]);
        std::fs::write(repo.0.join("a.txt"), "committed content").expect("write");
        run_git(&repo.0, &["add", "."]);
        run_git(&repo.0, &["commit", "-m", "init"]);
        let nested = repo.0.join(".import-wt");
        std::fs::create_dir_all(&nested).expect("mkdir nested");
        run_git(&nested, &["init", "-b", "main"]);
        std::fs::write(nested.join("inner.txt"), "inner").expect("write");

        let mut w = workspace(&ws);
        w.worktree_path = Some(repo.0.to_string_lossy().to_string());
        store.insert_workspace(&w).await.expect("ws");

        let svc = Services::new(store);
        let plan = svc
            .workspace_transfer_plan_op(ws.clone())
            .await
            .expect("plan");

        assert!(plan.manifest.git.dirty_files.is_empty());
        assert!(
            !plan
                .warnings
                .iter()
                .any(|w| w.code == "uncommitted-changes"),
            "nested repo alone is not an uncommitted change"
        );
        let warn = plan
            .warnings
            .iter()
            .find(|w| w.code == "nested-repos-skipped")
            .expect("nested-repos-skipped warning");
        assert!(warn.message.contains(".import-wt"), "{}", warn.message);
    }

    /// A submodule whose checkout HEAD was never pushed yields exactly one
    /// `submodule-unpublished-commits` warning naming path, short sha and
    /// branch, a `carried: true` manifest entry, and a bundle estimate that
    /// grows by the submodule's objects. Pushing the commit to the bare origin
    /// clears all three; the plan itself writes nothing to either repo.
    #[tokio::test]
    async fn transfer_plan_warns_on_unpublished_submodule_until_pushed() {
        use crate::transfer_submodules::test_fixture::{
            git, local_commit, superproject_with_submodule,
        };

        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();

        let root = TempDir::new("intentd-transfer-submodule");
        let (sup, _origin) = superproject_with_submodule(&root.0);
        let sub = sup.join("sub");
        git(&sub, &["checkout", "-q", "main"]);

        let mut w = workspace(&ws);
        w.worktree_path = Some(sup.to_string_lossy().to_string());
        store.insert_workspace(&w).await.expect("ws");
        let svc = Services::new(store);

        let clean = svc
            .workspace_transfer_plan_op(ws.clone())
            .await
            .expect("plan");
        assert!(clean.manifest.git.submodules.is_empty());
        assert!(
            !clean
                .warnings
                .iter()
                .any(|w| w.code == "submodule-unpublished-commits"),
            "{:?}",
            clean.warnings
        );

        let sha = local_commit(&sub, "wip.txt");
        let superproject_status = git(&sup, &["status", "--porcelain"]);
        let submodule_status = git(&sub, &["status", "--porcelain"]);
        let plan = svc
            .workspace_transfer_plan_op(ws.clone())
            .await
            .expect("plan");

        let warns: Vec<_> = plan
            .warnings
            .iter()
            .filter(|w| w.code == "submodule-unpublished-commits")
            .collect();
        assert_eq!(warns.len(), 1, "{:?}", plan.warnings);
        let msg = &warns[0].message;
        assert!(msg.contains("1 submodule(s)"), "{msg}");
        assert!(
            msg.contains(&format!("sub @ {} (main)", &sha[..7])),
            "{msg}"
        );
        assert!(msg.contains("will ride in the archive"), "{msg}");
        assert!(!msg.contains("Not carried"), "{msg}");

        assert_eq!(plan.manifest.git.submodules.len(), 1);
        let s = &plan.manifest.git.submodules[0];
        assert_eq!(s.name, "sub");
        assert_eq!(s.path, "sub");
        assert_eq!(s.commit_sha, sha);
        assert_eq!(s.branch.as_deref(), Some("main"));
        assert!(s.carried);
        assert!(
            plan.estimated_git_bundle_bytes > clean.estimated_git_bundle_bytes,
            "{} > {}",
            plan.estimated_git_bundle_bytes,
            clean.estimated_git_bundle_bytes
        );

        assert_eq!(git(&sup, &["status", "--porcelain"]), superproject_status);
        assert_eq!(git(&sub, &["status", "--porcelain"]), submodule_status);

        git(&sub, &["push", "-q", "origin", "main"]);
        let pushed = svc.workspace_transfer_plan_op(ws).await.expect("plan");
        assert!(pushed.manifest.git.submodules.is_empty());
        assert!(
            !pushed
                .warnings
                .iter()
                .any(|w| w.code == "submodule-unpublished-commits"),
            "{:?}",
            pushed.warnings
        );
    }

    /// A nested unpublished submodule under a PUBLISHED parent: exactly one
    /// warning counts only the nested finding as unpublished and names the
    /// parent in the "Also bundled" clause; the manifest lists the parent
    /// first (`carried: true, published: true`) then the child
    /// (`published: false`), and the estimate includes both bundles.
    #[tokio::test]
    async fn transfer_plan_carries_published_parent_of_nested_submodule() {
        use crate::transfer_submodules::test_fixture::{
            nested_unpublished_under_published_parent, NestedFixture,
        };

        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();

        let root = TempDir::new("intentd-transfer-submodule-nested");
        let NestedFixture {
            sup,
            sub_sha,
            inner_sha,
            ..
        } = nested_unpublished_under_published_parent(&root.0);

        let mut w = workspace(&ws);
        w.worktree_path = Some(sup.to_string_lossy().to_string());
        store.insert_workspace(&w).await.expect("ws");
        let svc = Services::new(store);
        let plan = svc.workspace_transfer_plan_op(ws).await.expect("plan");

        let warns: Vec<_> = plan
            .warnings
            .iter()
            .filter(|w| w.code == "submodule-unpublished-commits")
            .collect();
        assert_eq!(warns.len(), 1, "{:?}", plan.warnings);
        let msg = &warns[0].message;
        assert!(msg.contains("1 submodule(s) point at commits"), "{msg}");
        assert!(msg.contains("will ride in the archive (~"), "{msg}");
        assert!(
            msg.contains(&format!("): sub/inner @ {} (feat/x).", &inner_sha[..7])),
            "{msg}"
        );
        assert!(
            msg.contains(&format!(
                "Also bundled so the nested submodule(s) can be checked out: sub @ {} (main).",
                &sub_sha[..7]
            )),
            "{msg}"
        );
        assert!(!msg.contains("Not carried"), "{msg}");

        let subs = &plan.manifest.git.submodules;
        assert_eq!(subs.len(), 2, "{subs:?}");
        assert_eq!(subs[0].path, "sub");
        assert_eq!(subs[0].commit_sha, sub_sha);
        assert!(subs[0].carried && subs[0].published, "{subs:?}");
        assert_eq!(subs[1].path, "sub/inner");
        assert_eq!(subs[1].commit_sha, inner_sha);
        assert!(subs[1].carried && !subs[1].published, "{subs:?}");
        assert!(plan.estimated_git_bundle_bytes > 0);
    }

    /// An unpublished submodule commit found only in a live sandbox checkout
    /// is reported `carried: false` under "Not carried (sandbox only)" and
    /// adds nothing to the bundle estimate; a sandbox finding that matches
    /// the worktree's `(path, sha)` is not repeated.
    #[tokio::test]
    async fn transfer_plan_reports_sandbox_only_submodule_as_not_carried() {
        use crate::transfer_submodules::test_fixture::{
            git, local_commit, superproject_with_submodule,
        };

        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();

        let root = TempDir::new("intentd-transfer-submodule-sb");
        let (sup, _origin) = superproject_with_submodule(&root.0);
        git(
            &root.0,
            &[
                "clone",
                "-q",
                "--recurse-submodules",
                sup.to_str().unwrap(),
                "sandbox",
            ],
        );
        let sandbox = root.0.join("sandbox");
        let sb_sub = sandbox.join("sub");
        git(&sb_sub, &["checkout", "-q", "-b", "feat/sb"]);
        let sb_sha = local_commit(&sb_sub, "sandbox.txt");

        let mut w = workspace(&ws);
        w.worktree_path = Some(sup.to_string_lossy().to_string());
        store.insert_workspace(&w).await.expect("ws");
        let agent = AgentId::new();
        store
            .insert_agent_session(&session(&agent, &ws, AgentStatus::Idle))
            .await
            .expect("session");
        store
            .insert_sandbox(&Sandbox {
                id: "sb-1".to_string(),
                workspace_id: ws.clone(),
                agent_id: agent,
                path: sandbox.to_string_lossy().to_string(),
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
        let svc = Services::new(store);

        let plan = svc
            .workspace_transfer_plan_op(ws.clone())
            .await
            .expect("plan");
        let warns: Vec<_> = plan
            .warnings
            .iter()
            .filter(|w| w.code == "submodule-unpublished-commits")
            .collect();
        assert_eq!(warns.len(), 1, "{:?}", plan.warnings);
        let msg = &warns[0].message;
        assert!(!msg.contains("will ride in the archive"), "{msg}");
        assert!(
            msg.contains(&format!(
                "Not carried (sandbox only): sub @ {} (feat/sb)",
                &sb_sha[..7]
            )),
            "{msg}"
        );
        assert_eq!(plan.manifest.git.submodules.len(), 1);
        assert_eq!(plan.manifest.git.submodules[0].commit_sha, sb_sha);
        assert!(!plan.manifest.git.submodules[0].carried);

        // Same unpublished commit in the worktree and the sandbox: one
        // carried entry, no duplicate.
        let sub = sup.join("sub");
        git(&sub, &["fetch", "-q", sb_sub.to_str().unwrap(), "feat/sb"]);
        git(&sub, &["checkout", "-q", "--detach", &sb_sha]);
        let plan = svc.workspace_transfer_plan_op(ws).await.expect("plan");
        assert_eq!(
            plan.manifest.git.submodules.len(),
            1,
            "{:?}",
            plan.manifest.git.submodules
        );
        assert_eq!(plan.manifest.git.submodules[0].commit_sha, sb_sha);
        assert!(plan.manifest.git.submodules[0].carried);
        let msg = &plan
            .warnings
            .iter()
            .find(|w| w.code == "submodule-unpublished-commits")
            .expect("warning")
            .message;
        assert!(msg.contains("will ride in the archive"), "{msg}");
        assert!(!msg.contains("Not carried"), "{msg}");
    }

    /// Message formatting: carried, carried-published-parent and sandbox-only
    /// findings, short sha, optional branch, approximate size.
    #[test]
    fn submodule_warning_message_shape() {
        use intent_core::transfer::TransferSubmoduleSummary;
        assert_eq!(super::submodule_warning(&[], 0), None);
        let mut subs = vec![
            TransferSubmoduleSummary {
                name: "intentd".to_string(),
                path: "packages/intentd".to_string(),
                commit_sha: "6b079e7aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
                branch: Some("feat/x".to_string()),
                carried: true,
                published: false,
            },
            TransferSubmoduleSummary {
                name: "fe".to_string(),
                path: "packages/cloudlands-fe".to_string(),
                commit_sha: "a1b2c3dbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
                branch: None,
                carried: false,
                published: false,
            },
        ];
        let msg = super::submodule_warning(&subs, 41 * 1024 * 1024).expect("message");
        assert_eq!(
            msg,
            "1 submodule(s) point at commits not on any remote and will ride in the archive \
             (~41 MB): packages/intentd @ 6b079e7 (feat/x). Transfer will not push them; \
             publish the branches yourself when ready. \
             Not carried (sandbox only): packages/cloudlands-fe @ a1b2c3d."
        );
        subs.insert(
            0,
            TransferSubmoduleSummary {
                name: "packages".to_string(),
                path: "packages".to_string(),
                commit_sha: "0f0f0f0ccccccccccccccccccccccccccccccccc".to_string(),
                branch: Some("main".to_string()),
                carried: true,
                published: true,
            },
        );
        let msg = super::submodule_warning(&subs, 41 * 1024 * 1024).expect("message");
        assert_eq!(
            msg,
            "1 submodule(s) point at commits not on any remote and will ride in the archive \
             (~41 MB): packages/intentd @ 6b079e7 (feat/x). Transfer will not push them; \
             publish the branches yourself when ready. \
             Also bundled so the nested submodule(s) can be checked out: packages @ 0f0f0f0 (main). \
             Not carried (sandbox only): packages/cloudlands-fe @ a1b2c3d."
        );
        assert_eq!(super::human_bytes(0), "0 B");
        assert_eq!(super::human_bytes(1536), "1.5 KB");
        assert_eq!(super::human_bytes(3 * 1024 * 1024 * 1024), "3.0 GB");
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
                attachments: vec![intent_core::transfer::TransferAttachment {
                    id: "att-1".to_string(),
                    file_name: "spec.pdf".to_string(),
                    size_bytes: 4,
                    exists: true,
                }],
                git: intent_core::transfer::TransferGitSummary {
                    has_repository: true,
                    branch: Some("main".to_string()),
                    dirty_files: vec![],
                    sandbox_branches: vec![],
                    submodules: vec![intent_core::transfer::TransferSubmoduleSummary {
                        name: "sub".to_string(),
                        path: "packages/sub".to_string(),
                        commit_sha: "0123456789abcdef0123456789abcdef01234567".to_string(),
                        branch: None,
                        carried: true,
                        published: false,
                    }],
                },
            },
            total_size_bytes: 9,
            db_row_bytes: 2,
            asset_bytes: 3,
            attachment_bytes: 4,
            estimated_git_bundle_bytes: 0,
            warnings: vec![],
        };
        let v = serde_json::to_value(&plan).expect("serialize");
        assert_eq!(v["manifest"]["formatVersion"], 1);
        assert_eq!(v["manifest"]["creatingIntentdVersion"], "0.0.0");
        assert_eq!(v["manifest"]["tables"][0]["rowCount"], 1);
        assert_eq!(v["manifest"]["tables"][0]["approxBytes"], 2);
        assert_eq!(v["manifest"]["assets"][0]["sizeBytes"], 3);
        assert_eq!(v["manifest"]["attachments"][0]["fileName"], "spec.pdf");
        assert_eq!(v["manifest"]["attachments"][0]["sizeBytes"], 4);
        assert_eq!(v["manifest"]["attachments"][0]["exists"], true);
        assert_eq!(v["manifest"]["git"]["hasRepository"], true);
        assert_eq!(
            v["manifest"]["git"]["submodules"][0]["path"],
            "packages/sub"
        );
        assert_eq!(
            v["manifest"]["git"]["submodules"][0]["commitSha"],
            "0123456789abcdef0123456789abcdef01234567"
        );
        assert_eq!(v["manifest"]["git"]["submodules"][0]["carried"], true);
        assert!(
            v["manifest"]["git"]["submodules"][0]
                .get("branch")
                .is_none(),
            "absent branch is omitted on the wire"
        );
        let back: intent_core::transfer::TransferPlan = serde_json::from_value(serde_json::json!({
            "manifest": {
                "formatVersion": 1,
                "creatingIntentdVersion": "0.0.0",
                "workspaceId": "ws-1",
                "createdAt": "2026-01-01T00:00:00Z",
                "tables": [],
                "assets": [],
                "git": { "hasRepository": false, "dirtyFiles": [], "sandboxBranches": [] }
            },
            "totalSizeBytes": 0,
            "dbRowBytes": 0,
            "assetBytes": 0,
            "attachmentBytes": 0,
            "estimatedGitBundleBytes": 0,
            "warnings": []
        }))
        .expect("submodules is additive");
        assert!(back.manifest.git.submodules.is_empty());
        assert_eq!(v["totalSizeBytes"], 9);
        assert_eq!(v["dbRowBytes"], 2);
        assert_eq!(v["assetBytes"], 3);
        assert_eq!(v["attachmentBytes"], 4);
        assert_eq!(v["estimatedGitBundleBytes"], 0);
    }
}

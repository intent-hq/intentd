//! Workspace transfer vocabulary (Transfer/Download feature): the versioned
//! export manifest and the `workspace.transfer.plan` result. Defined in the
//! leaf crate so the store (row stats), services (plan/export) and the staged
//! import surface all share one wire shape.

use serde::{Deserialize, Serialize};

use crate::ids::WorkspaceId;

/// Version of the transfer archive format. Bump on any breaking change to the
/// manifest shape or archive layout; `workspace.import.begin` refuses archives
/// whose format version it does not understand.
pub const TRANSFER_FORMAT_VERSION: u32 = 1;

/// Per-table row statistics for one workspace-scoped table included in a
/// transfer (`event` is deliberately absent: event history stays on the
/// source). `approx_bytes` is the summed byte length of every column value
/// (cast to BLOB) across the workspace's rows — an estimate of the serialized
/// payload, not on-disk size.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferTableStat {
    pub name: String,
    pub row_count: i64,
    pub approx_bytes: i64,
}

/// One asset file under `<assets_root>/<workspaceId>/` (id = file name).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferAsset {
    pub id: String,
    pub size_bytes: u64,
}

/// One attachment-registry entry (the `attachments` table rides in the row
/// payload; this mirrors it into the manifest for size estimates and the
/// archive's `attachments/<attachmentId>` file entries). `exists: false`
/// means the stored file was already deleted from the canonical
/// `.intent/attachments/` store at plan time — the row still transfers
/// (deleted-is-deleted is a first-class state) but the archive carries no
/// file entry and `size_bytes` is 0.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferAttachment {
    pub id: String,
    pub file_name: String,
    pub size_bytes: u64,
    pub exists: bool,
}

/// One tracked submodule whose checked-out commit is not reachable from any
/// remote-tracking ref of that submodule repo (monorepo#4219). `path` is
/// superproject-relative with forward slashes (nested submodules compose,
/// e.g. `sub/inner`); `branch` is the submodule's attached HEAD branch when
/// there is one. `carried: true` means the objects ride in the archive as a
/// submodule bundle (a workspace-worktree finding); `carried: false` marks a
/// sandbox-only finding that is reported but not bundled. `published: true`
/// marks a commit that IS on a remote but is listed (and bundled) anyway
/// because a nested submodule below it is unpublished and cannot be checked
/// out without its containing repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferSubmoduleSummary {
    pub name: String,
    pub path: String,
    pub commit_sha: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    pub carried: bool,
    #[serde(default)]
    pub published: bool,
}

/// Git state summary for the manifest: the checked-out branch, dirty paths
/// (snapshotted as WIP commits at export time), the sandbox branches that
/// ride in the bundle, and the submodules whose commits exist only locally.
/// `has_repository: false` means the workspace has no local git repository
/// and the archive will carry no bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferGitSummary {
    pub has_repository: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    pub dirty_files: Vec<String>,
    pub sandbox_branches: Vec<String>,
    /// Submodules pointing at unpublished commits (additive to format v1;
    /// exact intentd version gating makes `default` tolerance sufficient).
    #[serde(default)]
    pub submodules: Vec<TransferSubmoduleSummary>,
}

/// The versioned transfer manifest embedded in every export archive.
/// `creating_intentd_version` is the exact daemon version that produced it
/// (`CARGO_PKG_VERSION`); the import side rejects on mismatch (exact-match
/// gating, spec "Resolved Design Decisions" #5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferManifest {
    pub format_version: u32,
    pub creating_intentd_version: String,
    pub workspace_id: WorkspaceId,
    pub created_at: String,
    pub tables: Vec<TransferTableStat>,
    pub assets: Vec<TransferAsset>,
    /// Attachment-registry entries (additive to format v1: exact intentd
    /// version gating means no pre-attachments archive can be imported by a
    /// daemon that expects this field, so `default` tolerance is enough — no
    /// format-version bump).
    #[serde(default)]
    pub attachments: Vec<TransferAttachment>,
    pub git: TransferGitSummary,
}

/// A non-blocking pre-flight notice surfaced by `workspace.transfer.plan`
/// (e.g. running agents, uncommitted changes, unmerged sandboxes). `code` is
/// machine-readable and stable; `message` is human-readable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferWarning {
    pub code: String,
    pub message: String,
}

/// `workspace.transfer.plan` result: the manifest preview plus the size
/// estimate shown by the FE wizard before starting a transfer.
/// `total_size_bytes = db_row_bytes + asset_bytes + attachment_bytes +
/// estimated_git_bundle_bytes`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferPlan {
    pub manifest: TransferManifest,
    pub total_size_bytes: u64,
    pub db_row_bytes: u64,
    pub asset_bytes: u64,
    /// Summed size of the attachment FILES the archive will carry (rows with
    /// a deleted stored file contribute 0).
    pub attachment_bytes: u64,
    pub estimated_git_bundle_bytes: u64,
    pub warnings: Vec<TransferWarning>,
}

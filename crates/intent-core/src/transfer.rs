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

/// Released versions (including numeric releases distributed on alpha) may
/// transfer across patches within one major/minor pair. Prereleases require
/// an exact string match, including build metadata. Malformed versions fail
/// closed even when the strings are identical. Archive/schema validation is
/// still required: a compatible version is not proof of compatible data.
#[must_use]
pub fn transfer_versions_compatible(source: &str, target: &str) -> bool {
    let (Ok(source_version), Ok(target_version)) = (
        semver::Version::parse(source),
        semver::Version::parse(target),
    ) else {
        return false;
    };
    if !source_version.pre.is_empty() || !target_version.pre.is_empty() {
        return source == target;
    }
    source_version.major == target_version.major && source_version.minor == target_version.minor
}

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

/// Git state summary for the manifest: the checked-out branch, dirty paths
/// (snapshotted as WIP commits at export time), and the sandbox branches that
/// ride in the bundle. `has_repository: false` means the workspace has no
/// local git repository and the archive will carry no bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferGitSummary {
    pub has_repository: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    pub dirty_files: Vec<String>,
    pub sandbox_branches: Vec<String>,
}

/// The versioned transfer manifest embedded in every export archive.
/// `creating_intentd_version` is the exact daemon version that produced it
/// (`CARGO_PKG_VERSION`); import applies [`transfer_versions_compatible`]
/// in addition to archive-format and row-schema validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferManifest {
    pub format_version: u32,
    pub creating_intentd_version: String,
    pub workspace_id: WorkspaceId,
    pub created_at: String,
    pub tables: Vec<TransferTableStat>,
    pub assets: Vec<TransferAsset>,
    /// Attachment-registry entries, additive to format v1. Older archives
    /// without attachment support omit this list. Registry rows and archive
    /// files are validated independently; unknown payloads must not be dropped.
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

#[cfg(test)]
mod tests {
    use super::transfer_versions_compatible;

    #[test]
    fn transfer_version_policy() {
        for (source, target) in [
            ("0.9.1", "0.9.6"),
            ("0.9.6", "0.9.1"),
            ("1.2.0", "1.2.999"),
            ("1.2.3+alpha-build", "1.2.4+other"),
            ("1.2.3-rc.1+build", "1.2.3-rc.1+build"),
        ] {
            assert!(
                transfer_versions_compatible(source, target),
                "{source} -> {target}"
            );
        }
        for (source, target) in [
            ("0.9.6", "0.10.0"),
            ("1.2.3", "2.2.3"),
            ("1.2.3-rc.1", "1.2.3"),
            ("1.2.3", "1.2.3-rc.1"),
            ("1.2.3-rc.1", "1.2.4-rc.1"),
            ("1.2.3-rc.1", "1.2.3-rc.2"),
            ("1.2.3-rc.1+a", "1.2.3-rc.1+b"),
        ] {
            assert!(
                !transfer_versions_compatible(source, target),
                "{source} -> {target}"
            );
        }
        for invalid in [
            "", "v1.2.3", "1.2", "01.2.3", "1.2.3-01", "1.2.3 ", "1.2.3+",
        ] {
            assert!(!transfer_versions_compatible(invalid, invalid));
            assert!(!transfer_versions_compatible(invalid, "1.2.3"));
            assert!(!transfer_versions_compatible("1.2.3", invalid));
        }
    }
}

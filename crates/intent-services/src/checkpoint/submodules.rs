//! Strict submodule inventory for checkpoint capture.

use std::path::Path;

use git2::Repository;
use intent_core::{Error, Result};

/// A strict inventory for checkpoint capture, unlike transfer's deliberately
/// narrower published/unpublished scan. Includes every initialized tracked
/// gitlink from HEAD or index, even when HEAD differs from the recorded pin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckpointSubmodule {
    pub path: String,
    pub parent: String,
    pub relative_path: String,
}

pub(crate) fn checkpoint_submodules(root: &Path) -> Result<Vec<CheckpointSubmodule>> {
    fn visit(
        root: &Path,
        prefix: &str,
        depth: u32,
        out: &mut Vec<CheckpointSubmodule>,
    ) -> Result<()> {
        let directory = root.join(prefix);
        let repo = Repository::open(&directory)
            .map_err(|e| Error::Internal(format!("checkpoint submodule scan: {e}")))?;
        let mut tracked = std::collections::BTreeSet::new();
        let index = repo.index().map_err(|e| Error::Internal(e.to_string()))?;
        let mut head_index = git2::Index::new().map_err(|e| Error::Internal(e.to_string()))?;
        let head = repo
            .head()
            .and_then(|h| h.peel_to_tree())
            .map_err(|e| Error::Internal(format!("checkpoint requires born HEAD: {e}")))?;
        head_index
            .read_tree(&head)
            .map_err(|e| Error::Internal(e.to_string()))?;
        for entry in index
            .iter()
            .chain(head_index.iter())
            .filter(|e| e.mode == 0o16_0000)
        {
            let path = String::from_utf8(entry.path)
                .map_err(|_| Error::Internal("checkpoint submodule path is not UTF-8".into()))?;
            if !intent_git::checkpoint::safe_relative_path(&path) {
                return Err(Error::Internal("checkpoint unsafe submodule path".into()));
            }
            tracked.insert(path);
        }
        for relative_path in tracked {
            let mut checkout = directory.clone();
            for part in Path::new(&relative_path).components() {
                checkout.push(part);
                if std::fs::symlink_metadata(&checkout).is_ok_and(|m| m.file_type().is_symlink()) {
                    return Err(Error::Internal(
                        "checkpoint submodule symlink escape".into(),
                    ));
                }
            }
            if !checkout
                .join(".git")
                .try_exists()
                .map_err(|e| Error::Internal(e.to_string()))?
            {
                continue;
            }
            if depth >= intent_git::submodule::MAX_SUBMODULE_NESTING {
                return Err(Error::Internal(
                    "checkpoint submodule nesting limit exceeded".into(),
                ));
            }
            let path = if prefix == "." {
                relative_path.clone()
            } else {
                format!("{prefix}/{relative_path}")
            };
            // Opening or reading an initialized checkout must fail the capture,
            // not quietly discard dirty or unpublished child content.
            Repository::open(&checkout)
                .map_err(|e| Error::Internal(format!("checkpoint open {path}: {e}")))?;
            out.push(CheckpointSubmodule {
                path: path.clone(),
                parent: prefix.to_owned(),
                relative_path,
            });
            visit(root, &path, depth + 1, out)?;
        }
        Ok(())
    }
    let mut out = Vec::new();
    visit(root, ".", 0, &mut out)?;
    Ok(out)
}

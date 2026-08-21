//! Client-served filesystem capability: a minimal sandboxed text-file service
//! backing `fs/read_text_file` and `fs/write_text_file` (§6.7).
//!
//! Every path is resolved against the session's worktree (`root`, optionally
//! narrowed by `scope`) and rejected when it escapes that base — mirroring the
//! TS `FileSystemHandler.resolvePath` sandbox. The service performs only the IO;
//! the request handler ([`crate::handler`]) fires the `file:changed` event off a
//! returned [`FileChange`], keeping this layer free of the event bus so the
//! future `file.*` RPC can reuse it unchanged.

use std::path::{Component, Path, PathBuf};

use crate::error::{AcpError, AcpResult};

/// The `data.action` of the emitted `file:changed` event — a write either
/// creates a new file or modifies an existing one (parity with the M2 watcher's
/// lowercase action vocabulary).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileAction {
    /// The target path did not exist before the write.
    Create,
    /// The target path already existed and was overwritten.
    Modify,
}

impl FileAction {
    /// Lowercase wire value carried on `file:changed.data.action`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            FileAction::Create => "create",
            FileAction::Modify => "modify",
        }
    }
}

/// The side effect of a successful [`FileService::write`]: the workspace-relative
/// (forward-slash) path that changed and whether it was created or modified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileChange {
    /// Workspace-relative, forward-slash path (matches the watcher payload).
    pub relative_path: String,
    /// Whether the write created or modified the file.
    pub action: FileAction,
}

/// Minimal sandboxed text-file service scoped to one session's worktree.
#[derive(Debug, Clone)]
pub struct FileService {
    root: PathBuf,
    scope: Option<PathBuf>,
}

impl FileService {
    /// A service rooted at the session `cwd` (the full worktree).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            scope: None,
        }
    }

    /// The effective sandbox base (`root`, optionally narrowed by `scope`).
    fn base(&self) -> PathBuf {
        match &self.scope {
            Some(scope) => self.root.join(scope),
            None => self.root.clone(),
        }
    }

    /// Resolve `requested` against the sandbox base and reject any path that
    /// escapes it. Absolute paths are honoured but must still fall inside the
    /// base; relative paths join onto it. Resolution is lexical (no reliance on
    /// the file existing), so traversal via `..` is caught even for writes.
    ///
    /// # Errors
    ///
    /// Returns [`AcpError::Fs`] if the resolved path escapes the sandbox base.
    pub fn resolve(&self, requested: &Path) -> AcpResult<PathBuf> {
        let base = normalize_lexical(&self.base());
        let joined = if requested.is_absolute() {
            requested.to_path_buf()
        } else {
            base.join(requested)
        };
        let resolved = normalize_lexical(&joined);
        if !resolved.starts_with(&base) {
            return Err(AcpError::Fs(format!(
                "path is outside workspace scope: {}",
                requested.display()
            )));
        }
        Ok(resolved)
    }

    /// Read a UTF-8 text file inside the sandbox (`fs/read_text_file`).
    ///
    /// # Errors
    ///
    /// Returns [`AcpError::Fs`] if the path escapes the sandbox or the file cannot be read as UTF-8.
    pub async fn read(&self, requested: &Path) -> AcpResult<String> {
        let path = self.resolve(requested)?;
        tokio::fs::read_to_string(&path)
            .await
            .map_err(|e| AcpError::Fs(format!("read {}: {e}", path.display())))
    }

    /// Write a UTF-8 text file inside the sandbox, creating parent directories
    /// (`fs/write_text_file`). Returns the [`FileChange`] the handler publishes.
    ///
    /// # Errors
    ///
    /// Returns [`AcpError::Fs`] if the path escapes the sandbox or creating parent directories / writing the file fails.
    pub async fn write(&self, requested: &Path, content: &str) -> AcpResult<FileChange> {
        let path = self.resolve(requested)?;
        let existed = tokio::fs::metadata(&path).await.is_ok();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| AcpError::Fs(format!("create_dir_all {}: {e}", parent.display())))?;
        }
        tokio::fs::write(&path, content)
            .await
            .map_err(|e| AcpError::Fs(format!("write {}: {e}", path.display())))?;
        // Best-effort durability fsync (parity: TS `fsyncFile`); a sync failure
        // does not fail the write the agent already observed.
        if let Ok(file) = tokio::fs::File::open(&path).await {
            let _ = file.sync_all().await;
        }
        Ok(FileChange {
            relative_path: self.relative_to_root(&path),
            action: if existed {
                FileAction::Modify
            } else {
                FileAction::Create
            },
        })
    }

    /// Workspace-relative, forward-slash path for the `file:changed` payload.
    fn relative_to_root(&self, abs: &Path) -> String {
        let root = normalize_lexical(&self.root);
        let rel = abs.strip_prefix(&root).unwrap_or(abs);
        rel.components()
            .filter_map(|c| match c {
                Component::Normal(s) => s.to_str(),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("/")
    }
}

/// Lexically normalize a path: fold `.`/`..` without touching the filesystem so
/// the sandbox check works for not-yet-existing write targets. Leading `..`
/// components (above the path's anchor) are preserved so they can never silently
/// resolve inside the base.
fn normalize_lexical(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    out.push("..");
                }
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

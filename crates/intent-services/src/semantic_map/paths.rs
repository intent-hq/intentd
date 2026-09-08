use std::collections::HashMap;
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use intent_core::events::{GIT_ROOT_REGISTERED, GIT_ROOT_UNREGISTERED, GIT_ROOT_UPDATED};
use intent_core::{Event, WorkspaceGitRoot, WorkspaceId};
use intent_store::Store;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspacePathError(String);

impl fmt::Display for WorkspacePathError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for WorkspacePathError {}

/// Lexically normalizes a workspace-relative path without consulting the filesystem.
///
/// Both POSIX and Windows separators are accepted. Absolute paths, Windows drive/prefix
/// paths, parent traversal, and empty paths are rejected.
///
/// # Errors
///
/// Returns an error when `path` is not a non-empty workspace-relative path.
pub fn normalize_workspace_relative_path(path: &str) -> Result<String, WorkspacePathError> {
    let normalized = path.replace('\\', "/");
    if normalized.starts_with('/')
        || normalized
            .as_bytes()
            .get(1)
            .is_some_and(|separator| *separator == b':')
    {
        return Err(WorkspacePathError("must be workspace-relative".to_string()));
    }
    let mut components = Vec::new();
    for component in normalized.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                return Err(WorkspacePathError(
                    "must not contain parent traversal".to_string(),
                ));
            }
            component => components.push(component),
        }
    }
    if components.is_empty() {
        return Err(WorkspacePathError("must not be empty".to_string()));
    }
    Ok(components.join("/"))
}

pub(crate) fn normalize_workspace_relative_pattern(
    pattern: &str,
) -> Result<String, WorkspacePathError> {
    let (negated, pattern) = pattern
        .strip_prefix('!')
        .map_or((false, pattern), |pattern| (true, pattern));
    let normalized = normalize_workspace_relative_path(pattern)?;
    Ok(if negated {
        format!("!{normalized}")
    } else {
        normalized
    })
}

#[derive(Clone, Debug, Default)]
pub struct WorkspacePaths {
    prefixes: HashMap<String, String>,
}

impl WorkspacePaths {
    fn new(workspace_root: &Path, roots: &[WorkspaceGitRoot]) -> Self {
        let prefixes = roots
            .iter()
            .map(|root| {
                let prefix = relative_path(workspace_root, Path::new(&root.path));
                (
                    root.id.as_str().to_string(),
                    crate::file_tracking::normalize_path(&prefix.to_string_lossy()),
                )
            })
            .collect();
        Self { prefixes }
    }

    /// Normalizes a path and prefixes it with its registered git-root location.
    ///
    /// # Errors
    ///
    /// Returns an error when the caller-supplied `path` is unsafe.
    pub fn normalize(
        &self,
        path: &str,
        git_root_id: Option<&str>,
    ) -> Result<String, WorkspacePathError> {
        let path = normalize_workspace_relative_path(path)?;
        let Some(prefix) = git_root_id.and_then(|id| self.prefixes.get(id)) else {
            return Ok(path);
        };
        if prefix.is_empty() {
            return Ok(path);
        }
        Ok(format!("{prefix}/{path}"))
    }

    #[cfg(test)]
    pub(super) fn with_prefix(id: &str, prefix: &str) -> Self {
        Self {
            prefixes: HashMap::from([(id.to_string(), prefix.to_string())]),
        }
    }
}

fn relative_path(base: &Path, target: &Path) -> PathBuf {
    if let Ok(relative) = target.strip_prefix(base) {
        return relative.to_path_buf();
    }

    let base = base.components().collect::<Vec<_>>();
    let target = target.components().collect::<Vec<_>>();
    let shared = base
        .iter()
        .zip(&target)
        .take_while(|(left, right)| left == right)
        .count();
    let mut relative = PathBuf::new();
    for component in &base[shared..] {
        if matches!(component, Component::Normal(_)) {
            relative.push("..");
        }
    }
    for component in &target[shared..] {
        relative.push(component.as_os_str());
    }
    relative
}

fn cache() -> &'static Mutex<HashMap<WorkspaceId, WorkspacePaths>> {
    static CACHE: OnceLock<Mutex<HashMap<WorkspaceId, WorkspacePaths>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Loads the registered git-root prefixes used to normalize workspace paths.
///
/// # Errors
///
/// Returns an error when the workspace or its registered git roots cannot be loaded.
pub async fn workspace_paths(
    store: &Store,
    workspace_id: &WorkspaceId,
) -> Result<WorkspacePaths, intent_core::Error> {
    if let Some(paths) = cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(workspace_id)
        .cloned()
    {
        return Ok(paths);
    }

    let workspace = store.get_workspace(workspace_id).await?;
    let roots = store.list_workspace_git_roots(workspace_id).await?;
    let paths = crate::git_ops::worktree_path(&workspace)
        .map_or_else(WorkspacePaths::default, |root| {
            WorkspacePaths::new(&root, &roots)
        });
    cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(workspace_id.clone(), paths.clone());
    Ok(paths)
}

pub fn invalidate_workspace_paths(workspace_id: &WorkspaceId) {
    cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(workspace_id);
}

pub fn invalidate_workspace_paths_on_event(event: &Event) {
    if matches!(
        event.event_type.as_str(),
        GIT_ROOT_REGISTERED | GIT_ROOT_UPDATED | GIT_ROOT_UNREGISTERED
    ) {
        invalidate_workspace_paths(&event.workspace_id);
    }
}

#[cfg(test)]
mod tests {
    use intent_core::{ActorType, EventActor};
    use serde_json::Value;

    use super::*;

    #[test]
    fn validates_cross_platform_workspace_relative_paths() {
        assert_eq!(
            normalize_workspace_relative_path(r".\src\lib.rs").unwrap(),
            "src/lib.rs"
        );
        for invalid in [
            "/src/lib.rs",
            r"\server\share\file.rs",
            "C:/src/lib.rs",
            "../src/lib.rs",
            "src/../lib.rs",
            "",
        ] {
            assert!(
                normalize_workspace_relative_path(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
    }

    #[test]
    fn normalizes_negated_manifest_patterns() {
        assert_eq!(
            normalize_workspace_relative_pattern(r"!src\generated\**").unwrap(),
            "!src/generated/**"
        );
        assert!(normalize_workspace_relative_pattern("!/absolute/**").is_err());
    }

    #[test]
    fn unregistered_root_invalidates_the_workspace_cache() {
        let workspace_id = WorkspaceId::from("root-cache-test");
        cache()
            .lock()
            .unwrap()
            .insert(workspace_id.clone(), WorkspacePaths::default());
        invalidate_workspace_paths_on_event(&Event {
            id: "event-1".into(),
            workspace_id: workspace_id.clone(),
            timestamp: "2026-09-06T00:00:00Z".into(),
            event_type: GIT_ROOT_UNREGISTERED.into(),
            actor: EventActor {
                actor_type: ActorType::System,
                ..Default::default()
            },
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: Value::Null,
        });
        assert!(!cache().lock().unwrap().contains_key(&workspace_id));
    }

    #[test]
    fn computes_prefixes_for_nested_and_sibling_roots() {
        assert_eq!(
            relative_path(
                Path::new("/work/repo"),
                Path::new("/work/repo/packages/intentd")
            ),
            Path::new("packages/intentd")
        );
        assert_eq!(
            relative_path(Path::new("/work/repo"), Path::new("/work/intentd")),
            Path::new("../intentd")
        );
    }

    #[test]
    fn normalizes_paths_under_trusted_sibling_root_prefixes() {
        let paths = WorkspacePaths::with_prefix("intentd-root", "../intentd");

        assert_eq!(
            paths
                .normalize("./crates/intent-core/src/lib.rs", Some("intentd-root"))
                .unwrap(),
            "../intentd/crates/intent-core/src/lib.rs"
        );
        assert!(paths
            .normalize("../outside.rs", Some("intentd-root"))
            .is_err());
    }
}

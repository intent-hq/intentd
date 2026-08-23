//! Repository config service — read/write `.intent/config.json` in a repo root.
//!
//! FE-parity with `cloudlands-fe/src/features/workspace/main/repo-config.service.ts`.
//! Provides tolerant reads (missing/invalid → empty config, never errors), ensures
//! `.intent/.gitignore` is present with the exact FE content, and preserves unknown
//! JSON keys on round-trip.

use std::path::{Path, PathBuf};

use intent_core::{RepoConfig, Result};
use serde_json::Value;

/// `.intent` directory name in repo roots (FE `REPO_INTENT_DIR`).
const REPO_INTENT_DIR: &str = ".intent";

/// Config file name within `.intent/` (FE `REPO_CONFIG_FILENAME`).
const REPO_CONFIG_FILENAME: &str = "config.json";

/// Repo-relative path of the config file (`.intent/config.json`), as sent to
/// a forge contents API by `github.repoConfig.get`.
pub(crate) const REPO_CONFIG_REL_PATH: &str = ".intent/config.json";

/// Default `.gitignore` content for `.intent/` (FE `REPO_INTENT_GITIGNORE`).
/// Excludes everything except `config.json` and the `.gitignore` itself.
const REPO_INTENT_GITIGNORE: &str = "# Intent workspace config directory
# Only config.json is tracked in git — everything else is local
*
!.gitignore
!config.json
";

/// Get the path to the `.intent` directory for a repository.
pub(crate) fn get_intent_dir_path(repo_path: &Path) -> PathBuf {
    repo_path.join(REPO_INTENT_DIR)
}

/// Get the path to the `config.json` file for a repository.
pub(crate) fn get_config_file_path(repo_path: &Path) -> PathBuf {
    repo_path.join(REPO_INTENT_DIR).join(REPO_CONFIG_FILENAME)
}

/// Read the repo config from `.intent/config.json`.
/// Returns an empty config if the file doesn't exist or is invalid (tolerant, never errors).
pub async fn read_repo_config(repo_path: &Path) -> RepoConfig {
    let config_path = get_config_file_path(repo_path);

    match tokio::fs::read_to_string(&config_path).await {
        Ok(content) => parse_repo_config_tolerant(&content, &repo_path.display().to_string()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // File doesn't exist — that's fine, return empty config
            RepoConfig::default()
        }
        Err(e) => {
            tracing::warn!("Failed to read repo config at {:?}: {}", repo_path, e);
            RepoConfig::default()
        }
    }
}

/// Parse `config.json` text into a [`RepoConfig`] tolerantly: invalid JSON,
/// non-object JSON, and schema mismatches all yield the empty default (never
/// an error). `source` labels warn logs (a repo path or `owner/repo@ref`).
/// Shared by the local read above and the remote `github.repoConfig.get`.
pub(crate) fn parse_repo_config_tolerant(content: &str, source: &str) -> RepoConfig {
    match serde_json::from_str::<Value>(content) {
        Ok(value) => {
            // Validate it's an object (not an array or primitive)
            if !value.is_object() {
                tracing::warn!("Invalid repo config format (not an object): {}", source);
                return RepoConfig::default();
            }
            // Deserialize into RepoConfig
            match serde_json::from_value::<RepoConfig>(value) {
                Ok(config) => config,
                Err(e) => {
                    tracing::warn!("Failed to deserialize repo config at {}: {}", source, e);
                    RepoConfig::default()
                }
            }
        }
        Err(e) => {
            tracing::warn!("Failed to parse repo config JSON at {}: {}", source, e);
            RepoConfig::default()
        }
    }
}

/// Write the repo config to `.intent/config.json`.
/// Creates the `.intent/` directory and `.gitignore` if they don't exist.
/// Preserves unknown keys by merging with the existing file.
pub(crate) async fn write_repo_config(repo_path: &Path, config: RepoConfig) -> Result<()> {
    let intent_dir = get_intent_dir_path(repo_path);
    let config_path = get_config_file_path(repo_path);
    let gitignore_path = intent_dir.join(".gitignore");

    // Ensure .intent directory exists
    tokio::fs::create_dir_all(&intent_dir).await.map_err(|e| {
        intent_core::Error::Internal(format!("Failed to create .intent directory: {e}"))
    })?;

    // Ensure .gitignore exists (never overwrite)
    if !gitignore_path.exists() {
        tokio::fs::write(&gitignore_path, REPO_INTENT_GITIGNORE)
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("Failed to write .intent/.gitignore: {e}"))
            })?;
        tracing::info!("Created .intent/.gitignore at {:?}", gitignore_path);
    }

    // Read existing config to preserve unknown keys
    let existing = read_repo_config(repo_path).await;

    // Merge: new config's known fields + existing config's extra fields (new config wins for overlaps)
    let mut merged = config;
    for (key, value) in existing.extra {
        merged.extra.entry(key).or_insert(value);
    }

    write_config_file(&config_path, &merged).await
}

/// Merge a partial config patch into `.intent/config.json` (JSON-level field merge):
/// keys **present** in `patch` overwrite the on-disk values, keys **absent** from
/// `patch` are preserved, and an explicit `null` clears a field. Unknown keys
/// round-trip both ways (preserved when absent, settable/clearable when present).
/// Creates the `.intent/` directory and `.gitignore` if they don't exist.
/// Returns the merged config as written.
pub(crate) async fn merge_repo_config(
    repo_path: &Path,
    patch: serde_json::Map<String, Value>,
) -> Result<RepoConfig> {
    ensure_intent_dir(repo_path).await?;

    let mut merged = read_raw_config_object(repo_path).await;
    for (key, value) in patch {
        if value.is_null() {
            merged.remove(&key);
        } else {
            merged.insert(key, value);
        }
    }

    let config: RepoConfig = serde_json::from_value(Value::Object(merged))
        .map_err(|e| intent_core::Error::InvalidParams(format!("invalid config: {e}")))?;

    let config_path = get_config_file_path(repo_path);
    write_config_file(&config_path, &config).await?;
    Ok(config)
}

/// Read the raw JSON object from `.intent/config.json`, tolerant like
/// [`read_repo_config`]: returns an empty object when the file is missing,
/// unreadable, invalid JSON, not an object, or not a valid `RepoConfig`.
async fn read_raw_config_object(repo_path: &Path) -> serde_json::Map<String, Value> {
    let config_path = get_config_file_path(repo_path);
    let Ok(content) = tokio::fs::read_to_string(&config_path).await else {
        return serde_json::Map::new();
    };
    match serde_json::from_str::<Value>(&content) {
        Ok(Value::Object(map))
            if serde_json::from_value::<RepoConfig>(Value::Object(map.clone())).is_ok() =>
        {
            map
        }
        _ => serde_json::Map::new(),
    }
}

/// Serialize a config with pretty formatting + trailing newline (FE parity)
/// and write it to `config_path`.
async fn write_config_file(config_path: &Path, config: &RepoConfig) -> Result<()> {
    let content = serde_json::to_string_pretty(config).map_err(|e| {
        intent_core::Error::Internal(format!("Failed to serialize repo config: {e}"))
    })?;
    let content_with_newline = format!("{content}\n");

    tokio::fs::write(config_path, content_with_newline)
        .await
        .map_err(|e| intent_core::Error::Internal(format!("Failed to write repo config: {e}")))?;

    tracing::info!("Wrote repo config at {:?}", config_path);
    Ok(())
}

/// Ensure the `.intent/` directory exists with a proper `.gitignore`.
/// Call this when initializing a workspace from a repo that doesn't have one yet.
pub(crate) async fn ensure_intent_dir(repo_path: &Path) -> Result<()> {
    let intent_dir = get_intent_dir_path(repo_path);
    let gitignore_path = intent_dir.join(".gitignore");

    tokio::fs::create_dir_all(&intent_dir).await.map_err(|e| {
        intent_core::Error::Internal(format!("Failed to create .intent directory: {e}"))
    })?;

    if !gitignore_path.exists() {
        tokio::fs::write(&gitignore_path, REPO_INTENT_GITIGNORE)
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("Failed to write .intent/.gitignore: {e}"))
            })?;
        tracing::info!("Initialized .intent directory at {:?}", gitignore_path);
    }

    Ok(())
}

/// Check if a repo has an `.intent/config.json` file.
#[must_use]
pub fn has_repo_config(repo_path: &Path) -> bool {
    get_config_file_path(repo_path).exists()
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{RepoScript, RepoScriptCategory, RepoScriptMode};
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    /// Helper to create a temp repo directory for testing.
    fn temp_repo() -> TempDir {
        TempDir::new().unwrap()
    }

    #[tokio::test]
    async fn read_missing_file_returns_empty_config() {
        let repo = temp_repo();
        let config = read_repo_config(repo.path()).await;
        assert_eq!(config, RepoConfig::default());
        assert!(config.branch_prefix.is_none());
        assert!(config.scripts.is_none());
    }

    #[tokio::test]
    async fn read_corrupt_json_returns_empty_config() {
        let repo = temp_repo();
        let intent_dir = get_intent_dir_path(repo.path());
        tokio::fs::create_dir_all(&intent_dir).await.unwrap();
        let config_path = get_config_file_path(repo.path());
        tokio::fs::write(&config_path, "{ invalid json")
            .await
            .unwrap();

        let config = read_repo_config(repo.path()).await;
        assert_eq!(config, RepoConfig::default());
    }

    #[tokio::test]
    async fn read_non_object_json_returns_empty_config() {
        let repo = temp_repo();
        let intent_dir = get_intent_dir_path(repo.path());
        tokio::fs::create_dir_all(&intent_dir).await.unwrap();
        let config_path = get_config_file_path(repo.path());

        // Test with array
        tokio::fs::write(&config_path, "[]").await.unwrap();
        let config = read_repo_config(repo.path()).await;
        assert_eq!(config, RepoConfig::default());

        // Test with primitive
        tokio::fs::write(&config_path, "\"string\"").await.unwrap();
        let config = read_repo_config(repo.path()).await;
        assert_eq!(config, RepoConfig::default());
    }

    #[tokio::test]
    async fn write_creates_intent_dir_and_gitignore() {
        let repo = temp_repo();
        let config = RepoConfig {
            branch_prefix: Some("feature/".to_string()),
            ..Default::default()
        };

        write_repo_config(repo.path(), config.clone())
            .await
            .unwrap();

        // Check .intent dir exists
        let intent_dir = get_intent_dir_path(repo.path());
        assert!(intent_dir.exists());

        // Check .gitignore exists and has correct content
        let gitignore_path = intent_dir.join(".gitignore");
        assert!(gitignore_path.exists());
        let gitignore_content = tokio::fs::read_to_string(&gitignore_path).await.unwrap();
        assert_eq!(gitignore_content, REPO_INTENT_GITIGNORE);

        // Check config was written
        let config_path = get_config_file_path(repo.path());
        assert!(config_path.exists());
        let read_config = read_repo_config(repo.path()).await;
        assert_eq!(read_config.branch_prefix.as_deref(), Some("feature/"));
    }

    #[tokio::test]
    async fn write_does_not_overwrite_existing_gitignore() {
        let repo = temp_repo();
        let intent_dir = get_intent_dir_path(repo.path());
        tokio::fs::create_dir_all(&intent_dir).await.unwrap();
        let gitignore_path = intent_dir.join(".gitignore");

        // Write a custom .gitignore
        let custom_content = "# Custom content\n*.log\n";
        tokio::fs::write(&gitignore_path, custom_content)
            .await
            .unwrap();

        // Write config
        let config = RepoConfig {
            branch_prefix: Some("bugfix/".to_string()),
            ..Default::default()
        };
        write_repo_config(repo.path(), config).await.unwrap();

        // Check .gitignore was NOT overwritten
        let gitignore_content = tokio::fs::read_to_string(&gitignore_path).await.unwrap();
        assert_eq!(gitignore_content, custom_content);
    }

    #[tokio::test]
    async fn write_preserves_unknown_keys() {
        let repo = temp_repo();

        // Write initial config with unknown key
        let intent_dir = get_intent_dir_path(repo.path());
        tokio::fs::create_dir_all(&intent_dir).await.unwrap();
        let config_path = get_config_file_path(repo.path());
        tokio::fs::write(
            &config_path,
            r#"{
  "branchPrefix": "feature/",
  "customKey": "customValue",
  "anotherKey": 42
}"#,
        )
        .await
        .unwrap();

        // Read it back
        let mut config = read_repo_config(repo.path()).await;
        assert_eq!(config.branch_prefix.as_deref(), Some("feature/"));
        assert_eq!(config.extra.get("customKey").unwrap(), "customValue");
        assert_eq!(config.extra.get("anotherKey").unwrap(), 42);

        // Update a known field
        config.setup_script = Some("npm install".to_string());

        // Write it back
        write_repo_config(repo.path(), config).await.unwrap();

        // Read again and verify unknown keys are still there
        let final_config = read_repo_config(repo.path()).await;
        assert_eq!(final_config.branch_prefix.as_deref(), Some("feature/"));
        assert_eq!(final_config.setup_script.as_deref(), Some("npm install"));
        assert_eq!(final_config.extra.get("customKey").unwrap(), "customValue");
        assert_eq!(final_config.extra.get("anotherKey").unwrap(), 42);
    }

    /// Helper to build a merge patch from a JSON literal.
    fn patch(v: &Value) -> serde_json::Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    /// Regression (PR #270-style clobber): merging a patch that only contains
    /// `branchPrefix` must not erase an existing `setupScript`.
    #[tokio::test]
    async fn merge_preserves_fields_absent_from_patch() {
        let repo = temp_repo();
        let config = RepoConfig {
            branch_prefix: Some("feature/".to_string()),
            setup_script: Some("npm install".to_string()),
            instructions: Some("Use TypeScript".to_string()),
            ..Default::default()
        };
        write_repo_config(repo.path(), config).await.unwrap();

        let merged = merge_repo_config(
            repo.path(),
            patch(&serde_json::json!({"branchPrefix": "x/"})),
        )
        .await
        .unwrap();
        assert_eq!(merged.branch_prefix.as_deref(), Some("x/"));
        assert_eq!(merged.setup_script.as_deref(), Some("npm install"));
        assert_eq!(merged.instructions.as_deref(), Some("Use TypeScript"));

        let on_disk = read_repo_config(repo.path()).await;
        assert_eq!(on_disk.branch_prefix.as_deref(), Some("x/"));
        assert_eq!(on_disk.setup_script.as_deref(), Some("npm install"));
        assert_eq!(on_disk.instructions.as_deref(), Some("Use TypeScript"));
    }

    #[tokio::test]
    async fn merge_null_clears_field() {
        let repo = temp_repo();
        let config = RepoConfig {
            branch_prefix: Some("feature/".to_string()),
            setup_script: Some("npm install".to_string()),
            ..Default::default()
        };
        write_repo_config(repo.path(), config).await.unwrap();

        let merged = merge_repo_config(
            repo.path(),
            patch(&serde_json::json!({"setupScript": null})),
        )
        .await
        .unwrap();
        assert!(merged.setup_script.is_none());
        assert_eq!(merged.branch_prefix.as_deref(), Some("feature/"));

        let on_disk = read_repo_config(repo.path()).await;
        assert!(on_disk.setup_script.is_none());
        assert_eq!(on_disk.branch_prefix.as_deref(), Some("feature/"));
        // The cleared key must be gone from the file, not written as `null`.
        let raw = tokio::fs::read_to_string(get_config_file_path(repo.path()))
            .await
            .unwrap();
        assert!(!raw.contains("setupScript"));
    }

    #[tokio::test]
    async fn merge_round_trips_and_clears_unknown_keys() {
        let repo = temp_repo();
        let intent_dir = get_intent_dir_path(repo.path());
        tokio::fs::create_dir_all(&intent_dir).await.unwrap();
        tokio::fs::write(
            get_config_file_path(repo.path()),
            r#"{
  "branchPrefix": "feature/",
  "customKey": "customValue",
  "anotherKey": 42
}"#,
        )
        .await
        .unwrap();

        // Unknown keys absent from the patch survive a merge.
        let merged = merge_repo_config(
            repo.path(),
            patch(&serde_json::json!({"branchPrefix": "x/"})),
        )
        .await
        .unwrap();
        assert_eq!(merged.extra.get("customKey").unwrap(), "customValue");
        assert_eq!(merged.extra.get("anotherKey").unwrap(), 42);

        // Unknown keys present in the patch overwrite; explicit null clears them.
        let merged = merge_repo_config(
            repo.path(),
            patch(&serde_json::json!({"customKey": "updated", "anotherKey": null})),
        )
        .await
        .unwrap();
        assert_eq!(merged.extra.get("customKey").unwrap(), "updated");
        assert!(!merged.extra.contains_key("anotherKey"));

        let on_disk = read_repo_config(repo.path()).await;
        assert_eq!(on_disk.branch_prefix.as_deref(), Some("x/"));
        assert_eq!(on_disk.extra.get("customKey").unwrap(), "updated");
        assert!(!on_disk.extra.contains_key("anotherKey"));
    }

    #[tokio::test]
    async fn merge_creates_intent_dir_and_gitignore() {
        let repo = temp_repo();
        let merged = merge_repo_config(
            repo.path(),
            patch(&serde_json::json!({"branchPrefix": "x/"})),
        )
        .await
        .unwrap();
        assert_eq!(merged.branch_prefix.as_deref(), Some("x/"));

        let gitignore_path = get_intent_dir_path(repo.path()).join(".gitignore");
        assert!(gitignore_path.exists());
        let gitignore_content = tokio::fs::read_to_string(&gitignore_path).await.unwrap();
        assert_eq!(gitignore_content, REPO_INTENT_GITIGNORE);
        assert!(get_config_file_path(repo.path()).exists());
    }

    #[tokio::test]
    async fn merge_over_corrupt_file_treats_existing_as_empty() {
        let repo = temp_repo();
        let intent_dir = get_intent_dir_path(repo.path());
        tokio::fs::create_dir_all(&intent_dir).await.unwrap();
        tokio::fs::write(get_config_file_path(repo.path()), "{ invalid json")
            .await
            .unwrap();

        let merged = merge_repo_config(
            repo.path(),
            patch(&serde_json::json!({"branchPrefix": "x/"})),
        )
        .await
        .unwrap();
        assert_eq!(merged.branch_prefix.as_deref(), Some("x/"));
        assert!(merged.setup_script.is_none());
    }

    #[tokio::test]
    async fn merge_invalid_field_type_errors() {
        let repo = temp_repo();
        let err = merge_repo_config(repo.path(), patch(&serde_json::json!({"branchPrefix": 42})))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("invalid config"));
    }

    #[tokio::test]
    async fn round_trip_full_config() {
        let repo = temp_repo();

        let mut env = BTreeMap::new();
        env.insert("PORT".to_string(), "3000".to_string());

        let config = RepoConfig {
            branch_prefix: Some("feat/".to_string()),
            setup_script: Some("pnpm install".to_string()),
            instructions: Some("Use TypeScript strict mode".to_string()),
            run_script: Some("pnpm dev".to_string()),
            archive_script: Some("docker compose down".to_string()),
            scripts: Some(vec![
                RepoScript {
                    name: "dev".to_string(),
                    command: "pnpm dev".to_string(),
                    mode: RepoScriptMode::Service,
                    category: Some(RepoScriptCategory::Dev),
                    cwd: Some("frontend".to_string()),
                    env: Some(env),
                    auto_start: Some(true),
                },
                RepoScript {
                    name: "test".to_string(),
                    command: "cargo test".to_string(),
                    mode: RepoScriptMode::Command,
                    category: Some(RepoScriptCategory::Test),
                    cwd: None,
                    env: None,
                    auto_start: None,
                },
            ]),
            cow_clone_exclude: Some(vec!["node_modules".to_string()]),
            execution_environment: None,
            extra: BTreeMap::new(),
        };

        write_repo_config(repo.path(), config.clone())
            .await
            .unwrap();
        let read_config = read_repo_config(repo.path()).await;

        assert_eq!(read_config.branch_prefix, config.branch_prefix);
        assert_eq!(read_config.setup_script, config.setup_script);
        assert_eq!(read_config.instructions, config.instructions);
        assert_eq!(read_config.run_script, config.run_script);
        assert_eq!(read_config.archive_script, config.archive_script);
        assert_eq!(read_config.scripts, config.scripts);
        assert_eq!(read_config.cow_clone_exclude, config.cow_clone_exclude);
    }

    #[tokio::test]
    async fn has_repo_config_returns_true_when_exists() {
        let repo = temp_repo();
        assert!(!has_repo_config(repo.path()));

        let config = RepoConfig {
            branch_prefix: Some("main/".to_string()),
            ..Default::default()
        };
        write_repo_config(repo.path(), config).await.unwrap();

        assert!(has_repo_config(repo.path()));
    }

    #[tokio::test]
    async fn ensure_intent_dir_creates_directory_and_gitignore() {
        let repo = temp_repo();

        ensure_intent_dir(repo.path()).await.unwrap();

        let intent_dir = get_intent_dir_path(repo.path());
        assert!(intent_dir.exists());

        let gitignore_path = intent_dir.join(".gitignore");
        assert!(gitignore_path.exists());
        let gitignore_content = tokio::fs::read_to_string(&gitignore_path).await.unwrap();
        assert_eq!(gitignore_content, REPO_INTENT_GITIGNORE);
    }

    #[tokio::test]
    async fn ensure_intent_dir_does_not_overwrite_gitignore() {
        let repo = temp_repo();
        let intent_dir = get_intent_dir_path(repo.path());
        tokio::fs::create_dir_all(&intent_dir).await.unwrap();
        let gitignore_path = intent_dir.join(".gitignore");

        let custom_content = "# My custom .gitignore\n";
        tokio::fs::write(&gitignore_path, custom_content)
            .await
            .unwrap();

        ensure_intent_dir(repo.path()).await.unwrap();

        let gitignore_content = tokio::fs::read_to_string(&gitignore_path).await.unwrap();
        assert_eq!(gitignore_content, custom_content);
    }
}

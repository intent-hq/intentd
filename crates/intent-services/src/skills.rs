//! Skills discovery module - ports skills-loader.ts faithfully.
//!
//! Scans user and project directories for SKILL.md files following the
//! precedence order (higher p wins name collisions):
//! 1. `~/.agents/skills` (p1)
//! 2. `~/.claude/skills` (p2)
//! 3. `~/.augment/skills` (p3, auggie convention for back-compat)
//! 4. `~/.intent/skills` (p4, app-owned)
//! 5. `<workspace>/.agents/skills` (p5)
//! 6. `<workspace>/.augment/skills` (p6, auggie convention for back-compat)
//! 7. `<workspace>/.intent/skills` (p7, app-owned)

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

const SKILL_FILENAME: &str = "SKILL.md";
const MAX_SCAN_DEPTH: usize = 4;
const MAX_SCANNED_DIRECTORIES: usize = 2000;
const NO_WORKSPACE_CACHE_KEY: &str = "__no_workspace__";

static NOISE_DIRECTORIES: &[&str] = &[
    ".git",
    "node_modules",
    ".hg",
    ".svn",
    ".next",
    "dist",
    "build",
    "coverage",
    ".turbo",
];

/// Resolve the user's home directory from the environment (cross-platform).
fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillMetadata {
    pub name: String,
    pub description: String,
    pub location: String,
    pub scope: String, // "project" or "user"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_tools: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compatibility: Option<String>,
}

#[derive(Debug, Clone)]
struct ParsedSkillFile {
    metadata: SkillMetadata,
}

#[derive(Debug, Clone)]
struct DiscoveredSkill {
    metadata: SkillMetadata,
    precedence: u8,
}

#[derive(Debug, Clone)]
struct ScanTarget {
    root: PathBuf,
    precedence: u8,
    scope: String, // "project" or "user"
}

#[derive(Debug, Clone)]
struct PathFingerprint {
    path: PathBuf,
    exists: bool,
    mtime_ms: u128,
}

#[derive(Debug, Clone)]
struct CachePayload {
    skills: Vec<SkillMetadata>,
    catalog: String,
    fingerprints: Vec<PathFingerprint>,
}

struct CacheEntry {
    payload: CachePayload,
    load_promise: Option<Arc<tokio::sync::Mutex<()>>>,
}

/// Global discovery cache keyed by normalized workspace path
static DISCOVERY_CACHE: once_cell::sync::Lazy<Mutex<HashMap<String, CacheEntry>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(HashMap::new()));

/// Public API: discover skills for a workspace
pub async fn discover_skills(workspace_path: &str) -> Vec<SkillMetadata> {
    let payload = load_skills_payload(workspace_path).await;
    payload.skills.clone()
}

/// Public API: format skills catalog for prompt injection
pub async fn format_skills_catalog_for_prompt(workspace_path: &str) -> String {
    let payload = load_skills_payload(workspace_path).await;
    payload.catalog.clone()
}

/// Public API: check if skills have changed and return new list if they have.
/// Returns (skills, changed) where changed=true if the skill set differs from cache.
pub async fn check_skills_changed(workspace_path: &str) -> (Vec<SkillMetadata>, bool) {
    let normalized = normalize_workspace_path(workspace_path);
    let cache_key = normalized
        .as_deref()
        .unwrap_or(NO_WORKSPACE_CACHE_KEY)
        .to_string();

    // Get old skills from cache if present
    let old_skills = {
        let cache = DISCOVERY_CACHE.lock().unwrap();
        cache.get(&cache_key).map(|e| e.payload.skills.clone())
    };

    // Get current skills (will refresh cache if needed)
    let payload = load_skills_payload(workspace_path).await;
    let new_skills = payload.skills.clone();

    // Compare: changed if cache was empty or skill names/count differ
    let changed = match old_skills {
        None => !new_skills.is_empty(), // Changed if we now have skills
        Some(old) => {
            if old.len() != new_skills.len() {
                true
            } else {
                // Compare sorted skill names
                let old_names: Vec<_> = old.iter().map(|s| &s.name).collect();
                let new_names: Vec<_> = new_skills.iter().map(|s| &s.name).collect();
                old_names != new_names
            }
        }
    };

    (new_skills, changed)
}

/// Normalize workspace path (trim and resolve to absolute)
fn normalize_workspace_path(workspace_path: &str) -> Option<String> {
    let trimmed = workspace_path.trim();
    if trimmed.is_empty() {
        return None;
    }
    std::fs::canonicalize(trimmed)
        .ok()
        .map(|p| p.to_string_lossy().into_owned())
}

/// Get scan targets in precedence order
/// If `home_override` is provided, use it instead of reading from env (for tests)
fn get_scan_targets(
    workspace_path: Option<&str>,
    home_override: Option<PathBuf>,
) -> Vec<ScanTarget> {
    let mut targets = Vec::new();

    let home = home_override.or_else(home_dir);
    if let Some(home) = home {
        targets.push(ScanTarget {
            root: home.join(".agents").join("skills"),
            precedence: 1,
            scope: "user".to_string(),
        });
        targets.push(ScanTarget {
            root: home.join(".claude").join("skills"),
            precedence: 2,
            scope: "user".to_string(),
        });
        targets.push(ScanTarget {
            root: home.join(".augment").join("skills"),
            precedence: 3,
            scope: "user".to_string(),
        });
        targets.push(ScanTarget {
            root: home.join(".intent").join("skills"),
            precedence: 4,
            scope: "user".to_string(),
        });
    }

    if let Some(ws) = workspace_path {
        let ws_path = PathBuf::from(ws);
        targets.push(ScanTarget {
            root: ws_path.join(".agents").join("skills"),
            precedence: 5,
            scope: "project".to_string(),
        });
        targets.push(ScanTarget {
            root: ws_path.join(".augment").join("skills"),
            precedence: 6,
            scope: "project".to_string(),
        });
        targets.push(ScanTarget {
            root: ws_path.join(".intent").join("skills"),
            precedence: 7,
            scope: "project".to_string(),
        });
    }

    targets
}

/// Load skills payload with caching and concurrent-load coalescing
async fn load_skills_payload(workspace_path: &str) -> CachePayload {
    load_skills_payload_with_home(workspace_path, None).await
}

/// Internal: load skills payload with optional home directory override (for tests)
async fn load_skills_payload_with_home(
    workspace_path: &str,
    home_override: Option<PathBuf>,
) -> CachePayload {
    let normalized = normalize_workspace_path(workspace_path);
    let cache_key = normalized
        .as_deref()
        .unwrap_or(NO_WORKSPACE_CACHE_KEY)
        .to_string();

    loop {
        let (should_wait, should_check_fingerprints, fingerprints) = {
            let cache = DISCOVERY_CACHE.lock().unwrap();

            if let Some(entry) = cache.get(&cache_key) {
                if let Some(lock) = &entry.load_promise {
                    (Some(lock.clone()), false, Vec::new())
                } else {
                    (None, true, entry.payload.fingerprints.clone())
                }
            } else {
                (None, false, Vec::new())
            }
        };

        if let Some(lock) = should_wait {
            let _guard = lock.lock().await;
            continue;
        }

        if should_check_fingerprints {
            let fingerprints_current = are_fingerprints_current(&fingerprints).await;
            if fingerprints_current {
                let cache = DISCOVERY_CACHE.lock().unwrap();
                if let Some(entry) = cache.get(&cache_key) {
                    return entry.payload.clone();
                }
            }
        }

        // Start new scan
        let load_lock = Arc::new(tokio::sync::Mutex::new(()));
        let guard = load_lock.clone().try_lock_owned().unwrap();

        {
            let mut cache = DISCOVERY_CACHE.lock().unwrap();
            let old_payload = cache.get(&cache_key).map(|e| e.payload.clone());
            cache.insert(
                cache_key.clone(),
                CacheEntry {
                    payload: old_payload.clone().unwrap_or_else(|| CachePayload {
                        skills: Vec::new(),
                        catalog: String::new(),
                        fingerprints: Vec::new(),
                    }),
                    load_promise: Some(load_lock.clone()),
                },
            );
        }

        let result = scan_skills_with_home(normalized.as_deref(), home_override.clone()).await;

        {
            let mut cache = DISCOVERY_CACHE.lock().unwrap();
            cache.insert(
                cache_key.clone(),
                CacheEntry {
                    payload: result.clone(),
                    load_promise: None,
                },
            );
        }
        drop(guard);

        return result;
    }
}

/// Internal: scan skills with optional home directory override (for tests)
async fn scan_skills_with_home(
    workspace_path: Option<&str>,
    home_override: Option<PathBuf>,
) -> CachePayload {
    let mut observed_paths = std::collections::HashSet::new();
    let mut discovered_by_name = BTreeMap::new();
    let mut scan_state = ScanState {
        scanned_directories: 0,
    };

    for target in get_scan_targets(workspace_path, home_override) {
        observed_paths.insert(target.root.clone());
        let skill_files =
            find_skill_files(&target.root, &mut observed_paths, &mut scan_state).await;

        for skill_file in skill_files {
            if let Some(parsed) = parse_skill_file(&skill_file).await {
                let name = parsed.metadata.name.clone();

                if let Some(existing) = discovered_by_name.get(&name) {
                    let existing: &DiscoveredSkill = existing;
                    eprintln!(
                        "WARN: Skill name collision detected, keeping higher-precedence skill: name={}, kept={}, shadowed={}",
                        name,
                        if target.precedence >= existing.precedence { &parsed.metadata.location } else { &existing.metadata.location },
                        if target.precedence >= existing.precedence { &existing.metadata.location } else { &parsed.metadata.location }
                    );
                }

                if !discovered_by_name.contains_key(&name)
                    || target.precedence >= discovered_by_name[&name].precedence
                {
                    discovered_by_name.insert(
                        name.clone(),
                        DiscoveredSkill {
                            metadata: SkillMetadata {
                                scope: target.scope.clone(),
                                ..parsed.metadata
                            },
                            precedence: target.precedence,
                        },
                    );
                }
            }
        }
    }

    let mut skills: Vec<SkillMetadata> = discovered_by_name
        .into_values()
        .map(|s| s.metadata)
        .collect();
    skills.sort_by(|a, b| a.name.cmp(&b.name));

    let mut fingerprint_paths: Vec<_> = observed_paths.into_iter().collect();
    fingerprint_paths.sort();
    let mut fingerprints = Vec::new();
    for path in fingerprint_paths {
        fingerprints.push(get_path_fingerprint(&path).await);
    }

    let catalog = build_skills_catalog(&skills);

    CachePayload {
        skills,
        catalog,
        fingerprints,
    }
}

struct ScanState {
    scanned_directories: usize,
}

/// Find all SKILL.md files under root_path
async fn find_skill_files(
    root_path: &Path,
    observed_paths: &mut std::collections::HashSet<PathBuf>,
    scan_state: &mut ScanState,
) -> Vec<PathBuf> {
    if !tokio::fs::try_exists(root_path).await.unwrap_or(false) {
        return Vec::new();
    }

    let mut skill_files = Vec::new();
    walk_directory(root_path, 0, observed_paths, scan_state, &mut skill_files).await;
    skill_files
}

/// Recursive directory walker
#[async_recursion::async_recursion]
async fn walk_directory(
    current_path: &Path,
    depth: usize,
    observed_paths: &mut std::collections::HashSet<PathBuf>,
    scan_state: &mut ScanState,
    skill_files: &mut Vec<PathBuf>,
) {
    if scan_state.scanned_directories >= MAX_SCANNED_DIRECTORIES {
        return;
    }

    scan_state.scanned_directories += 1;
    observed_paths.insert(current_path.to_path_buf());

    let mut entries = match tokio::fs::read_dir(current_path).await {
        Ok(entries) => entries,
        Err(e) => {
            eprintln!(
                "WARN: Failed to read potential skills directory: path={}, error={}",
                current_path.display(),
                e
            );
            return;
        }
    };

    let mut dir_entries = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        dir_entries.push(entry);
    }
    dir_entries.sort_by_key(|e| e.file_name());

    // Check for SKILL.md in this directory
    let has_skill_file = dir_entries
        .iter()
        .any(|entry| entry.file_name() == SKILL_FILENAME);

    if has_skill_file {
        let skill_path = current_path.join(SKILL_FILENAME);
        observed_paths.insert(skill_path.clone());
        skill_files.push(skill_path);
        return; // Stop descending once SKILL.md is found
    }

    if depth >= MAX_SCAN_DEPTH {
        return;
    }

    // Recurse into subdirectories
    for entry in dir_entries {
        let file_name = entry.file_name();
        let file_name_str = file_name.to_string_lossy();

        if let Ok(metadata) = entry.metadata().await {
            if metadata.is_dir()
                && !metadata.is_symlink()
                && !NOISE_DIRECTORIES.contains(&file_name_str.as_ref())
            {
                walk_directory(
                    &entry.path(),
                    depth + 1,
                    observed_paths,
                    scan_state,
                    skill_files,
                )
                .await;
                if scan_state.scanned_directories >= MAX_SCANNED_DIRECTORIES {
                    eprintln!(
                        "WARN: Skills discovery stopped after reaching directory scan limit: rootPath={}, limit={}",
                        current_path.display(),
                        MAX_SCANNED_DIRECTORIES
                    );
                    return;
                }
            }
        }
    }
}

/// Parse a SKILL.md file
async fn parse_skill_file(skill_path: &Path) -> Option<ParsedSkillFile> {
    let content = match tokio::fs::read_to_string(skill_path).await {
        Ok(content) => content,
        Err(e) => {
            eprintln!(
                "WARN: Failed to read skill file: skillPath={}, error={}",
                skill_path.display(),
                e
            );
            return None;
        }
    };

    let parsed = parse_skill_content(&content)?;

    if parsed.name.is_none() {
        eprintln!(
            "WARN: Skipping skill missing required name field: skillPath={}",
            skill_path.display()
        );
        return None;
    }

    if parsed.description.is_none() {
        eprintln!(
            "WARN: Skipping skill missing required description field: skillPath={}",
            skill_path.display()
        );
        return None;
    }

    let name = parsed.name.unwrap();
    let description = parsed.description.unwrap();

    // Validate skill name (warn but don't reject)
    if let Some(parent_dir_name) = skill_path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
    {
        if name != parent_dir_name {
            eprintln!(
                "WARN: Skill name does not match parent directory name: skillPath={}, name={}, parentDirectoryName={}",
                skill_path.display(),
                name,
                parent_dir_name
            );
        }
    }

    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        eprintln!(
            "WARN: Skill name does not match the Agent Skills naming convention: skillPath={}, name={}",
            skill_path.display(),
            name
        );
    }

    Some(ParsedSkillFile {
        metadata: SkillMetadata {
            name,
            description,
            location: skill_path.to_string_lossy().into_owned(),
            scope: "user".to_string(), // Will be overridden by scan_skills
            allowed_tools: parsed.allowed_tools,
            compatibility: parsed.compatibility,
        },
    })
}

#[derive(Debug)]
struct ParsedSkillFrontmatter {
    name: Option<String>,
    description: Option<String>,
    allowed_tools: Option<String>,
    compatibility: Option<String>,
}

/// Parse skill content (frontmatter + body)
fn parse_skill_content(content: &str) -> Option<ParsedSkillFrontmatter> {
    let extracted = extract_frontmatter(content)?;

    let parsed_yaml = parse_frontmatter_yaml(&extracted.frontmatter_text).or_else(|| {
        parse_frontmatter_yaml(&quote_malformed_scalar_values(&extracted.frontmatter_text))
    })?;

    Some(ParsedSkillFrontmatter {
        name: normalize_scalar(parsed_yaml.get("name")),
        description: normalize_scalar(parsed_yaml.get("description")),
        allowed_tools: normalize_scalar(parsed_yaml.get("allowed-tools")),
        compatibility: normalize_scalar(parsed_yaml.get("compatibility")),
    })
}

struct FrontmatterExtraction {
    frontmatter_text: String,
}

/// Extract YAML frontmatter from markdown content
fn extract_frontmatter(content: &str) -> Option<FrontmatterExtraction> {
    let normalized = content.replace("\r\n", "\n");

    if !normalized.starts_with("---\n") && !normalized.starts_with("---\r\n") {
        return None;
    }

    let after_start = &normalized[4..]; // Skip "---\n"
    let end_pos = after_start.find("\n---")?;

    let frontmatter_text = after_start[..end_pos].to_string();

    Some(FrontmatterExtraction { frontmatter_text })
}

/// Parse YAML frontmatter text
fn parse_frontmatter_yaml(frontmatter_text: &str) -> Option<serde_yaml::Value> {
    serde_yaml::from_str::<serde_yaml::Value>(frontmatter_text)
        .ok()
        .filter(|v| v.is_mapping())
}

/// Quote malformed scalar values in YAML (unquoted colons)
fn quote_malformed_scalar_values(frontmatter_text: &str) -> String {
    frontmatter_text
        .lines()
        .map(|line| {
            // Match: <key>: <value>
            if let Some(colon_pos) = line.find(':') {
                let prefix = &line[..=colon_pos];
                let value_part = line[colon_pos + 1..].trim();

                // Skip if empty or already quoted or special YAML syntax
                if value_part.is_empty()
                    || value_part.starts_with('"')
                    || value_part.starts_with('\'')
                    || value_part.starts_with('[')
                    || value_part.starts_with('{')
                    || value_part == "|"
                    || value_part == ">"
                    || value_part == "|-"
                    || value_part == ">-"
                    || !value_part.contains(':')
                {
                    return line.to_string();
                }

                // Quote the value
                format!("{} \"{}\"", prefix, value_part.replace('"', "\\\""))
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Normalize a YAML scalar value to Option<String>
fn normalize_scalar(value: Option<&serde_yaml::Value>) -> Option<String> {
    match value? {
        serde_yaml::Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        serde_yaml::Value::Number(n) => Some(n.to_string()),
        serde_yaml::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Build the skills catalog XML for prompt injection
pub(crate) fn build_skills_catalog(skills: &[SkillMetadata]) -> String {
    if skills.is_empty() {
        return String::new();
    }

    let skill_xml = skills
        .iter()
        .map(|skill| {
            format!(
                "  <skill>\n    <name>{}</name>\n    <description>{}</description>\n    <location>{}</location>\n  </skill>",
                escape_xml(&skill.name),
                escape_xml(&skill.description),
                escape_xml(&skill.location)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    [
        "The following skills provide specialized instructions for specific tasks.",
        "When a task matches a skill's description, use your file-read tool to load",
        "the SKILL.md at the listed location before proceeding.",
        "When a skill references relative paths, resolve them against the skill's",
        "directory (the parent of SKILL.md) and use absolute paths in tool calls.",
        "",
        "<available_skills>",
        &skill_xml,
        "</available_skills>",
    ]
    .join("\n")
}

/// Escape XML special characters
fn escape_xml(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Get path fingerprint (existence + mtime)
async fn get_path_fingerprint(path: &Path) -> PathFingerprint {
    match tokio::fs::metadata(path).await {
        Ok(metadata) => {
            let mtime_ms = metadata
                .modified()
                .ok()
                .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
                .map(|d| d.as_millis())
                .unwrap_or(0);
            PathFingerprint {
                path: path.to_path_buf(),
                exists: true,
                mtime_ms,
            }
        }
        Err(_) => PathFingerprint {
            path: path.to_path_buf(),
            exists: false,
            mtime_ms: 0,
        },
    }
}

/// Check if all fingerprints are still current
async fn are_fingerprints_current(fingerprints: &[PathFingerprint]) -> bool {
    for fingerprint in fingerprints {
        let current = get_path_fingerprint(&fingerprint.path).await;
        if current.exists != fingerprint.exists || current.mtime_ms != fingerprint.mtime_ms {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_skill_content(frontmatter: &str, body: &str) -> String {
        format!("---\n{}\n---\n\n{}\n", frontmatter, body)
    }

    async fn write_skill(skill_root: &Path, skill_name: &str, content: &str) -> PathBuf {
        let skill_dir = skill_root.join(skill_name);
        tokio::fs::create_dir_all(&skill_dir).await.unwrap();
        let skill_path = skill_dir.join("SKILL.md");
        tokio::fs::write(&skill_path, content).await.unwrap();
        skill_path
    }

    // Test helper: discover skills with explicit home dir (no env mutation)
    async fn discover_skills_test(workspace_path: &str, home_dir: PathBuf) -> Vec<SkillMetadata> {
        let payload = load_skills_payload_with_home(workspace_path, Some(home_dir)).await;
        payload.skills
    }

    // Test helper: format catalog with explicit home dir (no env mutation)
    async fn format_catalog_test(workspace_path: &str, home_dir: PathBuf) -> String {
        let payload = load_skills_payload_with_home(workspace_path, Some(home_dir)).await;
        payload.catalog
    }

    // Clear the global cache before each test
    fn clear_cache() {
        let mut cache = DISCOVERY_CACHE.lock().unwrap();
        cache.clear();
    }

    #[tokio::test]
    async fn test_discover_and_parse_valid_skill() {
        clear_cache();
        let temp_dir = tempfile::tempdir().unwrap();
        let home_dir = temp_dir.path().join("home");
        let workspace_path = temp_dir.path().join("workspace");
        tokio::fs::create_dir_all(&workspace_path).await.unwrap();

        let skills_root = workspace_path.join(".agents").join("skills");
        let skill_path = write_skill(
            &skills_root,
            "valid-skill",
            &build_skill_content(
                "name: valid-skill\ndescription: Valid skill description",
                "Use this skill when needed.",
            ),
        )
        .await;

        let skills = discover_skills_test(&workspace_path.to_string_lossy(), home_dir).await;
        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].name, "valid-skill");
        assert_eq!(skills[0].description, "Valid skill description");
        assert_eq!(
            PathBuf::from(&skills[0].location),
            std::fs::canonicalize(&skill_path).unwrap()
        );
    }

    #[tokio::test]
    async fn test_skip_skills_missing_description() {
        clear_cache();
        let temp_dir = tempfile::tempdir().unwrap();
        let home_dir = temp_dir.path().join("home");
        let workspace_path = temp_dir.path().join("workspace");
        tokio::fs::create_dir_all(&workspace_path).await.unwrap();

        let skills_root = workspace_path.join(".agents").join("skills");
        write_skill(
            &skills_root,
            "missing-description",
            &build_skill_content("name: missing-description", "Body"),
        )
        .await;

        let skills = discover_skills_test(&workspace_path.to_string_lossy(), home_dir).await;
        assert_eq!(skills.len(), 0);
    }

    #[tokio::test]
    async fn test_parse_malformed_yaml_with_fallback() {
        clear_cache();
        let temp_dir = tempfile::tempdir().unwrap();
        let home_dir = temp_dir.path().join("home");
        let workspace_path = temp_dir.path().join("workspace");
        tokio::fs::create_dir_all(&workspace_path).await.unwrap();

        let skills_root = workspace_path.join(".agents").join("skills");
        write_skill(
            &skills_root,
            "fallback-skill",
            &build_skill_content(
                "name: fallback-skill\ndescription: Use when: the user asks",
                "Use this skill when needed.",
            ),
        )
        .await;

        let skills = discover_skills_test(&workspace_path.to_string_lossy(), home_dir).await;
        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].name, "fallback-skill");
        assert_eq!(skills[0].description, "Use when: the user asks");
    }

    #[tokio::test]
    async fn test_format_catalog_empty() {
        clear_cache();
        let temp_dir = tempfile::tempdir().unwrap();
        let home_dir = temp_dir.path().join("home");
        let workspace_path = temp_dir.path().join("workspace");
        tokio::fs::create_dir_all(&workspace_path).await.unwrap();

        let catalog = format_catalog_test(&workspace_path.to_string_lossy(), home_dir).await;
        assert_eq!(catalog, "");
    }

    #[tokio::test]
    async fn test_format_catalog_xml() {
        clear_cache();
        let temp_dir = tempfile::tempdir().unwrap();
        let home_dir = temp_dir.path().join("home");
        let workspace_path = temp_dir.path().join("workspace");
        tokio::fs::create_dir_all(&workspace_path).await.unwrap();

        let skills_root = workspace_path.join(".intent").join("skills");
        let _skill_path = write_skill(
            &skills_root,
            "catalog-skill",
            &build_skill_content(
                "name: catalog-skill\ndescription: Formats catalog output",
                "Use this skill when needed.",
            ),
        )
        .await;

        let catalog = format_catalog_test(&workspace_path.to_string_lossy(), home_dir).await;

        assert!(catalog.contains("<available_skills>"));
        assert!(catalog.contains("<skill>"));
        assert!(catalog.contains("<name>catalog-skill</name>"));
        assert!(catalog.contains("<description>Formats catalog output</description>"));
        // Check that location contains the skill path (canonicalized may have /private prefix on macOS)
        assert!(catalog.contains("<location>"));
        assert!(catalog.contains("catalog-skill/SKILL.md</location>"));
        assert!(catalog.contains("</available_skills>"));
    }

    #[tokio::test]
    async fn test_precedence_project_over_user() {
        clear_cache();
        let temp_dir = tempfile::tempdir().unwrap();
        let home_dir = temp_dir.path().join("home");
        let workspace_path = temp_dir.path().join("workspace");
        tokio::fs::create_dir_all(&workspace_path).await.unwrap();

        // User-level skill
        let user_skills_root = home_dir.join(".intent").join("skills");
        write_skill(
            &user_skills_root,
            "shared-skill",
            &build_skill_content(
                "name: shared-skill\ndescription: User-level description",
                "Use this skill when needed.",
            ),
        )
        .await;

        // Project-level skill (higher precedence)
        let project_skills_root = workspace_path.join(".agents").join("skills");
        let project_skill_path = write_skill(
            &project_skills_root,
            "shared-skill",
            &build_skill_content(
                "name: shared-skill\ndescription: Project-level description",
                "Use this skill when needed.",
            ),
        )
        .await;

        let skills = discover_skills_test(&workspace_path.to_string_lossy(), home_dir).await;
        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].name, "shared-skill");
        assert_eq!(skills[0].description, "Project-level description");
        assert_eq!(
            PathBuf::from(&skills[0].location),
            std::fs::canonicalize(&project_skill_path).unwrap()
        );
    }

    #[tokio::test]
    async fn test_precedence_intent_over_augment_on_collision() {
        clear_cache();
        let temp_dir = tempfile::tempdir().unwrap();
        let home_dir = temp_dir.path().join("home");
        let workspace_path = temp_dir.path().join("workspace");
        tokio::fs::create_dir_all(&workspace_path).await.unwrap();

        // Back-compat auggie-convention skill (lower precedence)
        write_skill(
            &workspace_path.join(".augment").join("skills"),
            "shared-skill",
            &build_skill_content(
                "name: shared-skill\ndescription: Augment back-compat description",
                "Use this skill when needed.",
            ),
        )
        .await;

        // App-owned skill (higher precedence) wins the name collision
        let intent_skill_path = write_skill(
            &workspace_path.join(".intent").join("skills"),
            "shared-skill",
            &build_skill_content(
                "name: shared-skill\ndescription: Intent app-owned description",
                "Use this skill when needed.",
            ),
        )
        .await;

        let skills = discover_skills_test(&workspace_path.to_string_lossy(), home_dir).await;
        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].name, "shared-skill");
        assert_eq!(skills[0].description, "Intent app-owned description");
        assert_eq!(
            PathBuf::from(&skills[0].location),
            std::fs::canonicalize(&intent_skill_path).unwrap()
        );
    }

    #[tokio::test]
    async fn test_xml_escaping() {
        assert_eq!(escape_xml("&<>\""), "&amp;&lt;&gt;&quot;");
        assert_eq!(escape_xml("normal text"), "normal text");
    }

    #[tokio::test]
    async fn test_depth_limit() {
        clear_cache();
        let temp_dir = tempfile::tempdir().unwrap();
        let home_dir = temp_dir.path().join("home");
        let workspace_path = temp_dir.path().join("workspace");
        tokio::fs::create_dir_all(&workspace_path).await.unwrap();

        let skills_root = workspace_path.join(".agents").join("skills");

        // Create a deep directory structure beyond MAX_SCAN_DEPTH
        let mut deep_path = skills_root.clone();
        for i in 0..=MAX_SCAN_DEPTH + 1 {
            deep_path = deep_path.join(format!("level{}", i));
        }
        tokio::fs::create_dir_all(&deep_path).await.unwrap();

        // Put a skill at the deepest level (should not be found)
        write_skill(
            &deep_path,
            "too-deep",
            &build_skill_content("name: too-deep\ndescription: Too deep to find", "Body"),
        )
        .await;

        let skills = discover_skills_test(&workspace_path.to_string_lossy(), home_dir).await;
        assert_eq!(
            skills.len(),
            0,
            "Skills beyond MAX_SCAN_DEPTH should not be found"
        );
    }

    #[tokio::test]
    async fn test_stop_descending_after_skill_found() {
        clear_cache();
        let temp_dir = tempfile::tempdir().unwrap();
        let home_dir = temp_dir.path().join("home");
        let workspace_path = temp_dir.path().join("workspace");
        tokio::fs::create_dir_all(&workspace_path).await.unwrap();

        let skills_root = workspace_path.join(".agents").join("skills");

        // Create parent directory with SKILL.md
        let _parent_skill_path = write_skill(
            &skills_root,
            "parent-skill",
            &build_skill_content("name: parent-skill\ndescription: Parent skill", "Body"),
        )
        .await;

        // Create child directory with another SKILL.md (should not be found)
        let child_dir = skills_root.join("parent-skill").join("child");
        tokio::fs::create_dir_all(&child_dir).await.unwrap();
        tokio::fs::write(
            child_dir.join("SKILL.md"),
            build_skill_content("name: child-skill\ndescription: Child skill", "Body"),
        )
        .await
        .unwrap();

        let skills = discover_skills_test(&workspace_path.to_string_lossy(), home_dir).await;
        assert_eq!(skills.len(), 1, "Should only find parent skill, not child");
        assert_eq!(skills[0].name, "parent-skill");
    }

    #[tokio::test]
    async fn test_cache_invalidation_on_mtime_change() {
        clear_cache();
        let temp_dir = tempfile::tempdir().unwrap();
        let home_dir = temp_dir.path().join("home");
        let workspace_path = temp_dir.path().join("workspace");
        tokio::fs::create_dir_all(&workspace_path).await.unwrap();

        let skills_root = workspace_path.join(".agents").join("skills");

        let _skill_path = write_skill(
            &skills_root,
            "cached-skill",
            &build_skill_content(
                "name: cached-skill\ndescription: Original description",
                "Body",
            ),
        )
        .await;

        // First load
        let skills1 =
            discover_skills_test(&workspace_path.to_string_lossy(), home_dir.clone()).await;
        assert_eq!(skills1.len(), 1);
        assert_eq!(skills1[0].description, "Original description");

        // Wait a bit to ensure different mtime
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Modify the file
        let skill_path_full = skills_root.join("cached-skill").join("SKILL.md");
        tokio::fs::write(
            &skill_path_full,
            build_skill_content(
                "name: cached-skill\ndescription: Updated description",
                "Body",
            ),
        )
        .await
        .unwrap();

        // Second load should pick up the change
        let skills2 = discover_skills_test(&workspace_path.to_string_lossy(), home_dir).await;
        assert_eq!(skills2.len(), 1);
        assert_eq!(skills2[0].description, "Updated description");
    }
}

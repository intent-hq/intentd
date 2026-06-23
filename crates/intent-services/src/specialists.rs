//! File-backed specialist definitions (§18.2, PROTOCOL §5.11): markdown-with-
//! frontmatter files resolved in **3 tiers**, highest priority first — project
//! (`<workspacePath>/.augment/specialists/`), then user
//! (`~/.augment/specialists/`), then bundled (`resources/specialists/`,
//! read-only). Ports `specialist-file-loader.ts` + `specialists.ipc.ts`'s
//! combined load. Nothing is persisted in SQLite; `create`/`edit`/`delete`
//! write user/project files only and `bundled` definitions are read-only.

use std::path::{Path, PathBuf};

use intent_core::{Error, Result};
use serde_json::{json, Map, Value};

/// Folder name under `.augment/` (and the bundled `resources/`) holding files.
const SPECIALISTS_FOLDER: &str = "specialists";
/// Env override for the bundled (read-only) specialists directory; lets the
/// daemon and tests point at app-shipped resources hermetically.
const BUNDLED_DIR_ENV: &str = "INTENTD_BUNDLED_SPECIALISTS_DIR";

/// Resolve the user's home directory from the environment (cross-platform).
fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
}

/// Default user specialists directory (`~/.augment/specialists/`).
fn default_user_dir() -> Option<PathBuf> {
    home_dir().map(|h| h.join(".augment").join(SPECIALISTS_FOLDER))
}

/// Default bundled specialists directory: the [`BUNDLED_DIR_ENV`] override if
/// set, else `resources/specialists/` next to the running executable.
fn default_bundled_dir() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os(BUNDLED_DIR_ENV) {
        let p = PathBuf::from(p);
        if !p.as_os_str().is_empty() {
            return Some(p);
        }
    }
    std::env::current_exe().ok().and_then(|e| {
        e.parent()
            .map(|d| d.join("resources").join(SPECIALISTS_FOLDER))
    })
}

/// The project specialists directory for a worktree.
fn project_dir(workspace_path: &Path) -> PathBuf {
    workspace_path.join(".augment").join(SPECIALISTS_FOLDER)
}

/// Reject ids that are empty or would escape the specialists directory; ids map
/// 1:1 to `<id>.md` filenames so they must be a single safe path component.
fn validate_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id == "."
        || id == ".."
        || id.contains('/')
        || id.contains('\\')
        || id.contains(std::path::MAIN_SEPARATOR)
    {
        return Err(Error::InvalidParams(format!(
            "invalid specialist id: {id:?}"
        )));
    }
    Ok(())
}

/// Parse a `scope` string into a writable tier; `bundled`/unknown are rejected
/// (bundled is read-only, PROTOCOL §5.11).
fn parse_scope(scope: &str) -> Result<&'static str> {
    match scope {
        "project" => Ok("project"),
        "user" => Ok("user"),
        "bundled" => Err(Error::InvalidParams(
            "bundled specialists are read-only".to_string(),
        )),
        other => Err(Error::InvalidParams(format!("invalid scope: {other:?}"))),
    }
}

/// Escape a string for a double-quoted YAML frontmatter scalar (port of
/// `escapeYamlValue`).
fn escape_yaml(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\t', "\\t")
}

/// Reverse [`escape_yaml`] for a double-quoted scalar (port of
/// `unescapeYamlValue`).
fn unescape_yaml(value: &str) -> String {
    value
        .replace("\\\"", "\"")
        .replace("\\'", "'")
        .replace("\\n", "\n")
        .replace("\\t", "\t")
        .replace("\\\\", "\\")
}

/// Split optional leading `---`-delimited YAML frontmatter from the markdown
/// body (port of `parseFrontmatter`). Returns `(frontmatter, body)`; when there
/// is no valid frontmatter block the whole content is the body.
fn parse_frontmatter(content: &str) -> (Map<String, Value>, String) {
    let norm = content.replace("\r\n", "\n");
    let mut lines = norm.split('\n');
    if lines.next().map(str::trim) != Some("---") {
        return (Map::new(), norm.trim().to_string());
    }
    let mut fm_lines: Vec<&str> = Vec::new();
    let mut found_end = false;
    let mut body_lines: Vec<&str> = Vec::new();
    for line in lines {
        if !found_end {
            if line.trim() == "---" {
                found_end = true;
            } else {
                fm_lines.push(line);
            }
        } else {
            body_lines.push(line);
        }
    }
    if !found_end {
        return (Map::new(), norm.trim().to_string());
    }
    let mut fm = Map::new();
    for line in fm_lines {
        let Some(colon) = line.find(':') else {
            continue;
        };
        let key = line[..colon].trim();
        if key.is_empty() {
            continue;
        }
        let mut value = line[colon + 1..].trim().to_string();
        if value.len() >= 2
            && ((value.starts_with('"') && value.ends_with('"'))
                || (value.starts_with('\'') && value.ends_with('\'')))
        {
            let quote = value.as_bytes()[0];
            value = value[1..value.len() - 1].to_string();
            if quote == b'"' {
                value = unescape_yaml(&value);
            }
        }
        fm.insert(key.to_string(), Value::String(value));
    }
    (fm, body_lines.join("\n").trim().to_string())
}

/// Build a wire `SpecialistDef` from one file's `content`. `source` is the
/// winning tier; `path` is the resolved file (omitted for `bundled`,
/// PROTOCOL §5.11). `prompt` is the markdown body; `modelTier` is carried
/// through from frontmatter when present so it round-trips losslessly.
fn build_def(id: &str, content: &str, source: &str, path: &Path) -> Value {
    let (fm, body) = parse_frontmatter(content);
    let name = fm
        .get("name")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or(id)
        .to_string();
    let description = fm
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let mut def = Map::new();
    def.insert("id".into(), json!(id));
    def.insert("name".into(), json!(name));
    def.insert("description".into(), json!(description));
    if let Some(tier) = fm.get("modelTier").and_then(Value::as_str) {
        if !tier.is_empty() {
            def.insert("modelTier".into(), json!(tier));
        }
    }
    def.insert("prompt".into(), json!(body));
    def.insert("source".into(), json!(source));
    // `bundled` is read-only and exposes no editable path (PROTOCOL §5.11).
    if source != "bundled" {
        def.insert("path".into(), json!(path.to_string_lossy()));
    }
    Value::Object(def)
}

/// Serialize a wire `spec` into markdown-with-frontmatter (port of
/// `writeSpecialistFile`): quoted `name`/`description`/`modelTier` scalars then
/// the `prompt` body. Only documented fields are written so parse→write→parse
/// round-trips losslessly.
fn render_file(id: &str, spec: &Value) -> String {
    let name = spec.get("name").and_then(Value::as_str).unwrap_or(id);
    let description = spec
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or("");
    let mut fm = vec![
        format!("name: \"{}\"", escape_yaml(name)),
        format!("description: \"{}\"", escape_yaml(description)),
    ];
    if let Some(tier) = spec.get("modelTier").and_then(Value::as_str) {
        if !tier.is_empty() {
            fm.push(format!("modelTier: \"{}\"", escape_yaml(tier)));
        }
    }
    let prompt = spec.get("prompt").and_then(Value::as_str).unwrap_or("");
    format!("---\n{}\n---\n\n{}", fm.join("\n"), prompt)
}

/// Stateless executor for the file-backed `specialist.*` namespace. Construct
/// one per call from the long-lived `Services`; it carries the resolved user and
/// bundled directory roots (project comes from each call's `workspacePath`).
pub(crate) struct SpecialistsService {
    user_dir: Option<PathBuf>,
    bundled_dir: Option<PathBuf>,
}

impl SpecialistsService {
    /// Build the service, resolving any unset directory from the environment
    /// (`~/.augment/specialists/` for user, [`BUNDLED_DIR_ENV`]/exe-relative for
    /// bundled). Tests inject explicit roots for hermetic 3-tier coverage.
    pub(crate) fn new(user_dir: Option<PathBuf>, bundled_dir: Option<PathBuf>) -> Self {
        Self {
            user_dir: user_dir.or_else(default_user_dir),
            bundled_dir: bundled_dir.or_else(default_bundled_dir),
        }
    }

    /// Load one specialist file from `dir` as `source`, if it exists and reads.
    fn load_from_dir(dir: &Path, id: &str, source: &str) -> Option<Value> {
        let path = dir.join(format!("{id}.md"));
        let content = std::fs::read_to_string(&path).ok()?;
        Some(build_def(id, &content, source, &path))
    }

    /// Resolve a single id through the 3-tier order project > user > bundled.
    fn resolve(&self, id: &str, workspace_path: Option<&Path>) -> Option<Value> {
        if let Some(wp) = workspace_path {
            if let Some(def) = Self::load_from_dir(&project_dir(wp), id, "project") {
                return Some(def);
            }
        }
        if let Some(dir) = &self.user_dir {
            if let Some(def) = Self::load_from_dir(dir, id, "user") {
                return Some(def);
            }
        }
        if let Some(dir) = &self.bundled_dir {
            if let Some(def) = Self::load_from_dir(dir, id, "bundled") {
                return Some(def);
            }
        }
        None
    }

    /// Enumerate every `<id>.md` in `dir`, inserting resolved defs into `acc`
    /// keyed by id (later tiers overwrite earlier — the precedence merge).
    fn collect_dir(dir: &Path, source: &str, acc: &mut std::collections::BTreeMap<String, Value>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("md") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            if let Ok(content) = std::fs::read_to_string(&path) {
                acc.insert(stem.to_string(), build_def(stem, &content, source, &path));
            }
        }
    }

    /// `specialist.list` → `{ specialists: SpecialistDef[] }` resolved in tier
    /// order (bundled < user < project), higher tiers overriding lower ones for
    /// the same id (PROTOCOL §5.11). `workspace_path` adds the project tier.
    pub(crate) fn list(&self, workspace_path: Option<&Path>) -> Result<Value> {
        let mut acc = std::collections::BTreeMap::new();
        if let Some(dir) = &self.bundled_dir {
            Self::collect_dir(dir, "bundled", &mut acc);
        }
        if let Some(dir) = &self.user_dir {
            Self::collect_dir(dir, "user", &mut acc);
        }
        if let Some(wp) = workspace_path {
            Self::collect_dir(&project_dir(wp), "project", &mut acc);
        }
        let specialists: Vec<Value> = acc.into_values().collect();
        Ok(json!({ "specialists": specialists }))
    }

    /// `specialist.get` → `{ specialist: SpecialistDef }`, the resolved view;
    /// unknown id → `-32602` (PROTOCOL §5.11).
    pub(crate) fn get(&self, id: &str, workspace_path: Option<&Path>) -> Result<Value> {
        validate_id(id)?;
        match self.resolve(id, workspace_path) {
            Some(def) => Ok(json!({ "specialist": def })),
            None => Err(Error::NotFound(format!("specialist not found: {id}"))),
        }
    }

    /// Resolve the writable directory for `scope`, creating it; `project`
    /// requires a `workspace_path`.
    fn writable_dir(&self, scope: &str, workspace_path: Option<&Path>) -> Result<PathBuf> {
        let dir = match scope {
            "project" => {
                let wp = workspace_path.ok_or_else(|| {
                    Error::InvalidParams("workspacePath is required for project scope".to_string())
                })?;
                project_dir(wp)
            }
            _ => self
                .user_dir
                .clone()
                .ok_or_else(|| Error::Internal("user specialists directory unavailable".into()))?,
        };
        std::fs::create_dir_all(&dir)
            .map_err(|e| Error::Internal(format!("create specialists dir failed: {e}")))?;
        Ok(dir)
    }

    /// `specialist.create` → write a new user/project file (default scope
    /// `user`); an existing id in that scope → `-32602` (PROTOCOL §5.11).
    pub(crate) fn create(
        &self,
        id: &str,
        spec: &Value,
        scope: Option<&str>,
        workspace_path: Option<&Path>,
    ) -> Result<Value> {
        validate_id(id)?;
        let scope = parse_scope(scope.unwrap_or("user"))?;
        if !spec.is_object() {
            return Err(Error::InvalidParams("spec must be an object".to_string()));
        }
        let dir = self.writable_dir(scope, workspace_path)?;
        let path = dir.join(format!("{id}.md"));
        if path.exists() {
            return Err(Error::InvalidParams(format!(
                "specialist already exists in {scope} scope: {id}"
            )));
        }
        std::fs::write(&path, render_file(id, spec))
            .map_err(|e| Error::Internal(format!("write specialist file failed: {e}")))?;
        Ok(json!({ "specialist": build_def(id, &render_file(id, spec), scope, &path) }))
    }

    /// `specialist.edit` → overwrite an existing user/project file; a missing
    /// file (e.g. a `bundled`-only id) → `-32602` (PROTOCOL §5.11).
    pub(crate) fn edit(
        &self,
        id: &str,
        spec: &Value,
        scope: &str,
        workspace_path: Option<&Path>,
    ) -> Result<Value> {
        validate_id(id)?;
        let scope = parse_scope(scope)?;
        if !spec.is_object() {
            return Err(Error::InvalidParams("spec must be an object".to_string()));
        }
        let dir = self.writable_dir(scope, workspace_path)?;
        let path = dir.join(format!("{id}.md"));
        if !path.exists() {
            return Err(Error::NotFound(format!(
                "specialist not found in {scope} scope: {id}"
            )));
        }
        std::fs::write(&path, render_file(id, spec))
            .map_err(|e| Error::Internal(format!("write specialist file failed: {e}")))?;
        Ok(json!({ "specialist": build_def(id, &render_file(id, spec), scope, &path) }))
    }

    /// `specialist.delete` → remove a user/project file; a missing file
    /// (including any `bundled`-only id) → `-32602` (PROTOCOL §5.11).
    pub(crate) fn delete(
        &self,
        id: &str,
        scope: &str,
        workspace_path: Option<&Path>,
    ) -> Result<Value> {
        validate_id(id)?;
        let scope = parse_scope(scope)?;
        let dir = match scope {
            "project" => {
                let wp = workspace_path.ok_or_else(|| {
                    Error::InvalidParams("workspacePath is required for project scope".to_string())
                })?;
                project_dir(wp)
            }
            _ => self
                .user_dir
                .clone()
                .ok_or_else(|| Error::Internal("user specialists directory unavailable".into()))?,
        };
        let path = dir.join(format!("{id}.md"));
        if !path.exists() {
            return Err(Error::NotFound(format!(
                "specialist not found in {scope} scope: {id}"
            )));
        }
        std::fs::remove_file(&path)
            .map_err(|e| Error::Internal(format!("delete specialist file failed: {e}")))?;
        Ok(json!({ "success": true }))
    }
}

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

/// The reference specialist definitions embedded at compile time (PP-2,
/// byte-identical to the reference `resources/specialists/` bundle). They form
/// the floor of the bundled tier so daemon-side resolution works with zero
/// local files; an on-disk bundled file (env override / packaged resources)
/// with the same id still wins over the embedded copy.
const EMBEDDED_BUNDLED: &[(&str, &str)] = &[
    (
        "chief-of-staff",
        include_str!("../resources/specialists/chief-of-staff.md"),
    ),
    (
        "developer",
        include_str!("../resources/specialists/developer.md"),
    ),
    (
        "implementor",
        include_str!("../resources/specialists/implementor.md"),
    ),
    (
        "pr-reviewer",
        include_str!("../resources/specialists/pr-reviewer.md"),
    ),
    (
        "pr-shepherd",
        include_str!("../resources/specialists/pr-shepherd.md"),
    ),
    ("ralph", include_str!("../resources/specialists/ralph.md")),
    (
        "spec-writer",
        include_str!("../resources/specialists/spec-writer.md"),
    ),
    (
        "ui-designer",
        include_str!("../resources/specialists/ui-designer.md"),
    ),
    (
        "verifier",
        include_str!("../resources/specialists/verifier.md"),
    ),
];

/// Resolve an embedded bundled specialist by id (the lowest tier).
fn load_embedded(id: &str) -> Option<Value> {
    EMBEDDED_BUNDLED
        .iter()
        .find(|(k, _)| *k == id)
        .map(|(k, content)| build_def(k, content, "bundled", Path::new("")))
}

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

/// Optional frontmatter scalar keys carried through `build_def`/`render_file`
/// verbatim so parse→write→parse round-trips losslessly (port of
/// `SpecialistFileFrontmatter`'s optional fields: `codingAgent`, `model`,
/// `modelTier`, `roleReminder`, `agentType`).
const OPTIONAL_FRONTMATTER_KEYS: &[&str] = &[
    "codingAgent",
    "model",
    "modelTier",
    "roleReminder",
    "agentType",
];

/// Build a wire `SpecialistDef` from one file's `content`. `source` is the
/// winning tier; `path` is the resolved file (omitted for `bundled`,
/// PROTOCOL §5.11). `prompt` is the markdown body; the optional frontmatter
/// scalars ([`OPTIONAL_FRONTMATTER_KEYS`]) are carried through when present so
/// they round-trip losslessly. `behaviorPrompt` mirrors `prompt` and
/// `isCustomized` is `true` for any non-`bundled` source (port of
/// `serializeSpecialist`).
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
    for &key in OPTIONAL_FRONTMATTER_KEYS {
        if let Some(v) = fm.get(key).and_then(Value::as_str) {
            if !v.is_empty() {
                def.insert(key.into(), json!(v));
            }
        }
    }
    def.insert("prompt".into(), json!(body));
    // `behaviorPrompt` is the wire alias for the body (port of
    // `serializeSpecialist`: both `prompt` and `behaviorPrompt` carry it).
    def.insert("behaviorPrompt".into(), json!(body));
    def.insert("source".into(), json!(source));
    // Any non-bundled (user/project) definition is a customization.
    def.insert("isCustomized".into(), json!(source != "bundled"));
    // `bundled` is read-only and exposes no editable path (PROTOCOL §5.11).
    if source != "bundled" {
        def.insert("path".into(), json!(path.to_string_lossy()));
    }
    Value::Object(def)
}

/// Serialize a wire `spec` into markdown-with-frontmatter (port of
/// `writeSpecialistFile`): quoted `name`/`description` scalars, then any
/// supplied optional scalars ([`OPTIONAL_FRONTMATTER_KEYS`]) in declaration
/// order, then the prompt body. The body is taken from `prompt`, falling back
/// to the `behaviorPrompt` alias (mirroring `SpecialistProposalPayload`). Only
/// documented fields are written so parse→write→parse round-trips losslessly.
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
    for &key in OPTIONAL_FRONTMATTER_KEYS {
        if let Some(v) = spec.get(key).and_then(Value::as_str) {
            if !v.is_empty() {
                fm.push(format!("{key}: \"{}\"", escape_yaml(v)));
            }
        }
    }
    let prompt = spec
        .get("prompt")
        .and_then(Value::as_str)
        .or_else(|| spec.get("behaviorPrompt").and_then(Value::as_str))
        .unwrap_or("");
    format!("---\n{}\n---\n\n{}", fm.join("\n"), prompt)
}

/// Extract the first numbered bold rule (`N. **text**`) under a `## Hard Rules`
/// heading, if any (port of the `getRoleReminder` Hard-Rules regex). Scans from
/// the heading until the next `##` heading and returns the first non-empty inner
/// `**…**` (rejecting any inner `*`, mirroring the TS `[^*]+`).
fn first_hard_rule(behavior_prompt: &str) -> Option<String> {
    let mut in_section = false;
    for line in behavior_prompt.split('\n') {
        let trimmed = line.trim();
        if !in_section {
            if let Some(rest) = trimmed.strip_prefix("##") {
                if rest.trim().to_lowercase().starts_with("hard rules") {
                    in_section = true;
                }
            }
            continue;
        }
        if trimmed.starts_with("##") {
            break;
        }
        // Match a leading `N.` then a `**…**` span.
        let digits = trimmed.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 || trimmed.as_bytes().get(digits) != Some(&b'.') {
            continue;
        }
        let after_num = trimmed[digits + 1..].trim_start();
        let Some(after_open) = after_num.strip_prefix("**") else {
            continue;
        };
        let Some(end) = after_open.find("**") else {
            continue;
        };
        let inner = &after_open[..end];
        if inner.is_empty() || inner.contains('*') {
            continue;
        }
        return Some(inner.trim().to_string());
    }
    None
}

/// Auto-generate a role reminder from a behavior prompt (port of
/// `autoGenerateRoleReminder`): the first meaningful (non-header, non-bold-only)
/// line, suffixed with the first numbered rule under a `## Hard Rules` section
/// when present.
fn auto_generate_role_reminder(behavior_prompt: &str) -> String {
    if behavior_prompt.is_empty() {
        return String::new();
    }
    let mut first_meaningful = String::new();
    for line in behavior_prompt.split('\n') {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if trimmed.starts_with("**") && trimmed.ends_with("**") {
            continue;
        }
        // Strip a leading and/or trailing `**` marker (port of the
        // `replace(/^\*\*|\*\*$/g, '')` core-role normalization).
        let stripped = trimmed.strip_prefix("**").unwrap_or(trimmed);
        let stripped = stripped.strip_suffix("**").unwrap_or(stripped);
        first_meaningful = stripped.trim().to_string();
        break;
    }
    if let Some(rule) = first_hard_rule(behavior_prompt) {
        if !first_meaningful.is_empty() {
            return format!("{first_meaningful} {rule}.");
        }
        return rule;
    }
    first_meaningful
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
    /// Within the bundled tier an on-disk file wins over the embedded copy;
    /// the compile-time [`EMBEDDED_BUNDLED`] set is the always-available floor.
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
        load_embedded(id)
    }

    /// Resolve a specialist's `agentType` frontmatter scalar through the 3-tier
    /// order (project > user > bundled), used at spawn time to derive a created
    /// agent's `agent_type` (SP-B / §18.2 → §18.4 denylist). Returns `None` when
    /// the specialist is unknown or declares no `agentType`, leaving the caller's
    /// default agent type intact.
    pub(crate) fn resolve_agent_type(
        &self,
        id: &str,
        workspace_path: Option<&Path>,
    ) -> Option<String> {
        self.resolve(id, workspace_path).and_then(|def| {
            def.get("agentType")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
    }

    /// Resolve a specialist's `model` frontmatter scalar through the 3-tier
    /// order (project > user > bundled), used at spawn time when no explicit
    /// model parameter is supplied. Returns `None` when the specialist is
    /// unknown or declares no `model`, allowing the caller to fall through to
    /// the settings chain.
    pub(crate) fn resolve_model(&self, id: &str, workspace_path: Option<&Path>) -> Option<String> {
        // Validate id before passing to resolve() to prevent path traversal
        validate_id(id).ok()?;
        self.resolve(id, workspace_path).and_then(|def| {
            def.get("model")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
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
    /// order (embedded < bundled dir < user < project), higher tiers overriding
    /// lower ones for the same id (PROTOCOL §5.11). `workspace_path` adds the
    /// project tier.
    pub(crate) fn list(&self, workspace_path: Option<&Path>) -> Result<Value> {
        let mut acc = std::collections::BTreeMap::new();
        for (id, content) in EMBEDDED_BUNDLED {
            acc.insert(
                id.to_string(),
                build_def(id, content, "bundled", Path::new("")),
            );
        }
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

    /// Resolve `(name, roleReminder)` for an agent's specialist id, or `None`
    /// when the specialist is unknown or yields no usable reminder (port of
    /// `resolveSpecialistForAgent` + `getRoleReminder`). The reminder is the
    /// explicit `roleReminder` frontmatter scalar when present, else
    /// auto-generated from the behavior prompt.
    pub(crate) fn resolve_role_reminder(
        &self,
        id: &str,
        workspace_path: Option<&Path>,
    ) -> Option<(String, String)> {
        let def = self.resolve(id, workspace_path)?;
        let name = def
            .get("name")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or(id)
            .to_string();
        let reminder = def
            .get("roleReminder")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| {
                let body = def
                    .get("behaviorPrompt")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                auto_generate_role_reminder(body)
            });
        if reminder.is_empty() {
            return None;
        }
        Some((name, reminder))
    }

    /// Resolve the spawn-prompt injection fields for a specialist id (PP-1):
    /// `(behaviorPrompt body, display name, roleReminder)`. The reminder falls
    /// back to the auto-generated one (same policy as
    /// [`Self::resolve_role_reminder`]); empty values resolve to `None`.
    /// Returns `None` when the specialist is unknown.
    #[allow(clippy::type_complexity)]
    pub(crate) fn resolve_prompt_injection(
        &self,
        id: &str,
        workspace_path: Option<&Path>,
    ) -> Option<(Option<String>, String, Option<String>)> {
        let def = self.resolve(id, workspace_path)?;
        let behavior_prompt = def
            .get("behaviorPrompt")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let name = def
            .get("name")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or(id)
            .to_string();
        let reminder = def
            .get("roleReminder")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| {
                let auto = auto_generate_role_reminder(behavior_prompt.as_deref().unwrap_or(""));
                (!auto.is_empty()).then_some(auto)
            });
        Some((behavior_prompt, name, reminder))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_frontmatter_captures_optional_scalars() {
        let content = "---\nname: \"Ralph\"\ndescription: \"Loops\"\ncodingAgent: \"claude\"\nmodel: \"opus4.5\"\nmodelTier: \"smart\"\nroleReminder: \"Never stop early\"\nagentType: \"ralph-loop\"\n---\n\nYou loop.";
        let (fm, body) = parse_frontmatter(content);
        assert_eq!(fm.get("codingAgent").unwrap(), "claude");
        assert_eq!(fm.get("model").unwrap(), "opus4.5");
        assert_eq!(fm.get("modelTier").unwrap(), "smart");
        assert_eq!(fm.get("roleReminder").unwrap(), "Never stop early");
        assert_eq!(fm.get("agentType").unwrap(), "ralph-loop");
        assert_eq!(body, "You loop.");
    }

    #[test]
    fn build_def_emits_wire_fields() {
        let content = "---\nname: \"Ralph\"\ndescription: \"Loops\"\ncodingAgent: \"claude\"\nmodel: \"opus4.5\"\nmodelTier: \"smart\"\nroleReminder: \"Never stop early\"\nagentType: \"ralph-loop\"\n---\n\nYou loop.";
        let def = build_def("ralph", content, "user", Path::new("/tmp/ralph.md"));
        assert_eq!(def["id"], "ralph");
        assert_eq!(def["name"], "Ralph");
        assert_eq!(def["description"], "Loops");
        assert_eq!(def["codingAgent"], "claude");
        assert_eq!(def["model"], "opus4.5");
        assert_eq!(def["modelTier"], "smart");
        assert_eq!(def["roleReminder"], "Never stop early");
        assert_eq!(def["agentType"], "ralph-loop");
        assert_eq!(def["prompt"], "You loop.");
        assert_eq!(def["behaviorPrompt"], "You loop.");
        assert_eq!(def["source"], "user");
        assert_eq!(def["isCustomized"], true);
        assert_eq!(def["path"], "/tmp/ralph.md");
    }

    #[test]
    fn build_def_bundled_is_not_customized_and_omits_path() {
        let content = "---\nname: \"Impl\"\ndescription: \"d\"\n---\n\nbody";
        let def = build_def("impl", content, "bundled", Path::new("/tmp/impl.md"));
        assert_eq!(def["isCustomized"], false);
        assert!(def.get("path").is_none());
        // Absent optional scalars are not emitted.
        assert!(def.get("codingAgent").is_none());
        assert!(def.get("agentType").is_none());
    }

    #[test]
    fn render_file_round_trips_losslessly() {
        let spec = json!({
            "id": "ralph",
            "name": "Ralph",
            "description": "Loops",
            "codingAgent": "claude",
            "model": "opus4.5",
            "modelTier": "smart",
            "roleReminder": "Never stop early",
            "agentType": "ralph-loop",
            "prompt": "You loop.\nForever."
        });
        let rendered = render_file("ralph", &spec);
        let def = build_def("ralph", &rendered, "user", Path::new("/tmp/ralph.md"));
        assert_eq!(def["codingAgent"], "claude");
        assert_eq!(def["model"], "opus4.5");
        assert_eq!(def["modelTier"], "smart");
        assert_eq!(def["roleReminder"], "Never stop early");
        assert_eq!(def["agentType"], "ralph-loop");
        assert_eq!(def["prompt"], "You loop.\nForever.");
        assert_eq!(def["behaviorPrompt"], "You loop.\nForever.");
    }

    #[test]
    fn render_file_accepts_behavior_prompt_alias() {
        let spec = json!({
            "name": "Ralph",
            "description": "Loops",
            "behaviorPrompt": "body via alias"
        });
        let rendered = render_file("ralph", &spec);
        let def = build_def("ralph", &rendered, "user", Path::new("/tmp/ralph.md"));
        assert_eq!(def["prompt"], "body via alias");
    }

    #[test]
    fn auto_generate_role_reminder_combines_first_line_and_hard_rule() {
        let body = "# Implementor\n\nImplement your assigned task — nothing more.\n\n## Hard Rules\n1. **No scope creep** — only what the task note asks\n2. **No refactors** — ask first\n";
        assert_eq!(
            auto_generate_role_reminder(body),
            "Implement your assigned task — nothing more. No scope creep."
        );
    }

    #[test]
    fn auto_generate_role_reminder_first_line_only_without_hard_rules() {
        let body = "# Verifier\n\nVerify the work thoroughly.\n";
        assert_eq!(
            auto_generate_role_reminder(body),
            "Verify the work thoroughly."
        );
    }

    #[test]
    fn auto_generate_role_reminder_empty_is_empty() {
        assert_eq!(auto_generate_role_reminder(""), "");
        assert_eq!(auto_generate_role_reminder("# Only A Header\n"), "");
    }

    /// A temp dir holding seeded `<id>.md` specialist files for hermetic
    /// resolver tests; removed on drop.
    struct TempSpecialistsDir {
        path: PathBuf,
    }

    impl TempSpecialistsDir {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("intentd-specialists-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn write(&self, id: &str, content: &str) {
            std::fs::write(self.path.join(format!("{id}.md")), content).unwrap();
        }
    }

    impl Drop for TempSpecialistsDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn service_over(dir: &TempSpecialistsDir) -> SpecialistsService {
        SpecialistsService::new(Some(dir.path.clone()), Some(dir.path.clone()))
    }

    #[test]
    fn resolve_role_reminder_uses_explicit_reminder() {
        let dir = TempSpecialistsDir::new();
        dir.write(
            "implementor",
            "---\nname: \"Implementor\"\ndescription: \"d\"\nroleReminder: \"Stay in scope. No refactors.\"\n---\n\nbody",
        );
        let svc = service_over(&dir);
        let (name, reminder) = svc.resolve_role_reminder("implementor", None).unwrap();
        assert_eq!(name, "Implementor");
        assert_eq!(reminder, "Stay in scope. No refactors.");
    }

    #[test]
    fn resolve_role_reminder_auto_generates_when_absent() {
        let dir = TempSpecialistsDir::new();
        dir.write(
            "implementor",
            "---\nname: \"Implementor\"\ndescription: \"d\"\n---\n\nImplement your assigned task.\n\n## Hard Rules\n1. **No scope creep** — only the task\n",
        );
        let svc = service_over(&dir);
        let (name, reminder) = svc.resolve_role_reminder("implementor", None).unwrap();
        assert_eq!(name, "Implementor");
        assert_eq!(reminder, "Implement your assigned task. No scope creep.");
    }

    #[test]
    fn resolve_role_reminder_none_for_unknown_or_empty() {
        let dir = TempSpecialistsDir::new();
        // No file → unknown specialist.
        let svc = service_over(&dir);
        assert!(svc.resolve_role_reminder("missing", None).is_none());
        // Present but no reminder and an unparseable body → None.
        dir.write(
            "blank",
            "---\nname: \"Blank\"\ndescription: \"d\"\n---\n\n# Only Headers\n",
        );
        assert!(svc.resolve_role_reminder("blank", None).is_none());
    }

    /// The nine reference specialist ids embedded via `include_str!` (PP-2).
    const EMBEDDED_IDS: [&str; 9] = [
        "spec-writer",
        "implementor",
        "verifier",
        "developer",
        "chief-of-staff",
        "ralph",
        "ui-designer",
        "pr-reviewer",
        "pr-shepherd",
    ];

    #[test]
    fn embedded_bundled_resolves_all_nine_with_zero_local_files() {
        // Empty user + bundled dirs: every embedded id still resolves through
        // get()/list()/resolve_agent_type()/resolve_role_reminder().
        let dir = TempSpecialistsDir::new();
        let svc = service_over(&dir);
        for id in EMBEDDED_IDS {
            let got = svc.get(id, None).expect("embedded specialist resolves");
            let def = &got["specialist"];
            assert_eq!(def["source"], "bundled", "{id}");
            assert_eq!(def["isCustomized"], false, "{id}");
            assert!(def.get("path").is_none(), "{id}: bundled exposes no path");
            assert!(
                !def["behaviorPrompt"].as_str().unwrap().trim().is_empty(),
                "{id}: non-empty body"
            );
        }
        let list = svc.list(None).unwrap();
        let specs = list["specialists"].as_array().unwrap();
        for id in EMBEDDED_IDS {
            assert!(specs.iter().any(|s| s["id"] == id), "{id} listed");
        }
        // Frontmatter-driven resolution works too: ralph declares an agentType,
        // implementor an explicit roleReminder.
        assert_eq!(
            svc.resolve_agent_type("ralph", None).as_deref(),
            Some("ralph-loop")
        );
        let (name, reminder) = svc.resolve_role_reminder("implementor", None).unwrap();
        assert_eq!(name, "Implementor");
        assert!(reminder.starts_with("Stay within task scope."));
    }

    #[test]
    fn user_file_overrides_embedded_bundled() {
        let dir = TempSpecialistsDir::new();
        dir.write(
            "implementor",
            "---\nname: \"Custom Implementor\"\ndescription: \"d\"\n---\n\nCustom body",
        );
        let svc = service_over(&dir);
        let got = svc.get("implementor", None).unwrap();
        assert_eq!(got["specialist"]["source"], "user");
        assert_eq!(got["specialist"]["name"], "Custom Implementor");
        let list = svc.list(None).unwrap();
        let specs = list["specialists"].as_array().unwrap();
        let imp = specs.iter().find(|s| s["id"] == "implementor").unwrap();
        assert_eq!(imp["source"], "user", "user tier wins in list too");
    }

    /// Rot-check regression test: the bundled `verifier.md` prompt must instruct
    /// the verifier to mark verified tasks complete with `update_note_task_status`
    /// and explain the completion policy (only APPROVED tasks, DEVIATION/MISSING
    /// stay in review_required). This prevents future prompt rewrites from
    /// silently dropping the workflow instruction and leaving tasks stuck in
    /// `review_required`.
    #[test]
    fn verifier_prompt_mentions_update_note_task_status_and_marking_complete() {
        let dir = TempSpecialistsDir::new();
        let svc = service_over(&dir);
        let got = svc
            .get("verifier", None)
            .expect("embedded verifier resolves");
        let body = got["specialist"]["behaviorPrompt"]
            .as_str()
            .expect("prompt body");

        // Assert the prompt contains the `update_note_task_status` tool.
        assert!(
            body.contains("update_note_task_status"),
            "verifier.md must mention update_note_task_status tool"
        );

        // Assert the prompt uses object-style call syntax with placeholder noteId value.
        assert!(
            body.contains(
                r#"update_note_task_status({ noteId: "<task-note-id>", status: "complete" })"#
            ),
            "verifier.md must show complete object-style call with placeholder noteId"
        );

        // Assert the prompt instructs marking verified tasks complete with the exact phrase.
        assert!(
            body.contains("mark each verified task note `complete`"),
            "verifier.md must instruct marking verified tasks complete"
        );

        // Assert the prompt specifies the APPROVED → complete policy and that
        // tasks with DEVIATION/MISSING stay in review_required.
        assert!(
            (body.contains("APPROVED") || body.contains("✅ APPROVED"))
                && (body.contains("DEVIATION") || body.contains("⚠️ DEVIATION"))
                && (body.contains("MISSING") || body.contains("❌ MISSING")),
            "verifier.md must specify APPROVED/DEVIATION/MISSING completion policy"
        );
    }

    #[test]
    fn bundled_dir_file_overrides_embedded_copy() {
        // An on-disk bundled file (env override / packaged resources) with the
        // same id wins over the embedded copy within the bundled tier.
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        bundled.write(
            "verifier",
            "---\nname: \"Patched Verifier\"\ndescription: \"d\"\n---\n\nPatched body",
        );
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let got = svc.get("verifier", None).unwrap();
        assert_eq!(got["specialist"]["source"], "bundled");
        assert_eq!(got["specialist"]["name"], "Patched Verifier");
        assert_eq!(got["specialist"]["prompt"], "Patched body");
    }
}

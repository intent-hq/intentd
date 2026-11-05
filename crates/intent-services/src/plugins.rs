//! Agent Plugins v1 (<https://agent-plugins.org/specification>): app-wide plugins
//! installed under `~/.intent/plugins/<name>/`. Intent is the only loader, so
//! every provider receives the same skills and MCP servers.
//!
//! Failures stay at the narrowest boundary (spec §6): a bad `plugin.json`
//! rejects the plugin, a bad `skills/` or `mcp.json` drops that component, a bad
//! skill or server entry drops only itself. Each drop is a diagnostic.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub(crate) const PLUGIN_SCHEMA: &str = "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json";
pub(crate) const MCP_SCHEMA: &str = "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json";
pub(crate) const MANIFEST_FILE: &str = "plugin.json";
const MCP_FILE: &str = "mcp.json";
const SKILLS_DIR: &str = "skills";
const SKILL_FILE: &str = "SKILL.md";
const ROOT_VAR: &str = "PLUGIN_ROOT";
const DATA_VAR: &str = "PLUGIN_DATA";
const REGISTRY_FILE: &str = "plugins.json";
const MAX_MANIFEST_BYTES: u64 = 256 * 1024;
const MAX_PLUGINS: usize = 256;

const MANIFEST_FIELDS: &[&str] = &[
    "$schema",
    "name",
    "version",
    "description",
    "author",
    "homepage",
    "repository",
    "license",
    "keywords",
    "extensions",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PluginAuthor {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PluginManifest {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<PluginAuthor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub homepage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub keywords: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PluginMcpServer {
    Stdio {
        /// Bare executable name, or an absolute path inside the plugin root.
        command: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,
        cwd: PathBuf,
    },
    Http {
        url: String,
        headers: BTreeMap<String, String>,
    },
    Sse {
        url: String,
        headers: BTreeMap<String, String>,
    },
}

impl PluginMcpServer {
    pub(crate) fn transport(&self) -> &'static str {
        match self {
            Self::Stdio { .. } => "stdio",
            Self::Http { .. } => "streamable-http",
            Self::Sse { .. } => "sse",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginSkill {
    pub directory: String,
    pub file: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoadedPlugin {
    pub root: PathBuf,
    pub data_dir: PathBuf,
    pub manifest: PluginManifest,
    pub skills: Vec<PluginSkill>,
    pub mcp_servers: BTreeMap<String, PluginMcpServer>,
    pub diagnostics: Vec<String>,
}

/// A rejected plugin: `plugin.json` is missing or violates the manifest rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginRejection {
    pub root: PathBuf,
    pub reason: String,
}

/// Validate and load the plugin at `root`. `data_dir` is the plugin's
/// persistent `PLUGIN_DATA` directory; it is not created here.
pub(crate) fn load_plugin(root: &Path, data_dir: &Path) -> Result<LoadedPlugin, PluginRejection> {
    let reject = |reason: String| PluginRejection {
        root: root.to_path_buf(),
        reason,
    };
    let canonical_root = root
        .canonicalize()
        .map_err(|e| reject(format!("plugin directory is not readable: {e}")))?;
    if !canonical_root.is_dir() {
        return Err(reject("plugin path is not a directory".into()));
    }
    let mut diagnostics = Vec::new();
    let manifest_path = contained_file(&canonical_root, MANIFEST_FILE)
        .map_err(|e| reject(format!("{MANIFEST_FILE}: {e}")))?
        .ok_or_else(|| reject(format!("{MANIFEST_FILE} is missing")))?;
    let manifest_value =
        read_json(&manifest_path).map_err(|e| reject(format!("{MANIFEST_FILE}: {e}")))?;
    let manifest = parse_manifest(&manifest_value, &mut diagnostics)
        .map_err(|e| reject(format!("{MANIFEST_FILE}: {e}")))?;

    let skills = match discover_skills(&canonical_root) {
        Ok((skills, mut skipped)) => {
            diagnostics.append(&mut skipped);
            skills
        }
        Err(e) => {
            diagnostics.push(format!("{SKILLS_DIR}/: {e}; skills disabled"));
            Vec::new()
        }
    };
    let mcp_servers = match load_mcp(&canonical_root, data_dir) {
        Ok((servers, mut skipped)) => {
            diagnostics.append(&mut skipped);
            servers
        }
        Err(e) => {
            diagnostics.push(format!("{MCP_FILE}: {e}; MCP servers disabled"));
            BTreeMap::new()
        }
    };
    Ok(LoadedPlugin {
        root: canonical_root,
        data_dir: data_dir.to_path_buf(),
        manifest,
        skills,
        mcp_servers,
        diagnostics,
    })
}

/// Spec §5.3 name rule: 1–64 of `[a-z0-9.-]`, alphanumeric ends, no `--`/`..`.
pub(crate) fn is_valid_plugin_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    (1..=64).contains(&bytes.len())
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-' || *b == b'.')
        && bytes[0].is_ascii_alphanumeric()
        && bytes[bytes.len() - 1].is_ascii_alphanumeric()
        && !name.contains("--")
        && !name.contains("..")
}

fn parse_manifest(value: &Value, diagnostics: &mut Vec<String>) -> Result<PluginManifest, String> {
    let obj = value.as_object().ok_or("must be a JSON object")?;
    for key in obj
        .keys()
        .filter(|k| !MANIFEST_FIELDS.contains(&k.as_str()))
    {
        diagnostics.push(format!("{MANIFEST_FILE}: unknown field `{key}` ignored"));
    }
    match obj.get("$schema") {
        Some(Value::String(s)) if s == PLUGIN_SCHEMA => {}
        Some(Value::String(s)) => return Err(format!("unsupported `$schema` `{s}`")),
        Some(_) => return Err("`$schema` must be a string".into()),
        None => return Err("`$schema` is required".into()),
    }
    let name = match obj.get("name") {
        Some(Value::String(s)) if is_valid_plugin_name(s) => s.clone(),
        Some(Value::String(s)) => return Err(format!("invalid `name` `{s}`")),
        Some(_) => return Err("`name` must be a string".into()),
        None => return Err("`name` is required".into()),
    };
    let author = match obj.get("author") {
        None => None,
        Some(Value::Object(a)) => {
            if let Some(key) = a
                .keys()
                .find(|k| !matches!(k.as_str(), "name" | "email" | "url"))
            {
                return Err(format!("`author` has unknown field `{key}`"));
            }
            Some(PluginAuthor {
                name: opt_string(a, "name", "author.name")?,
                email: opt_string(a, "email", "author.email")?,
                url: opt_string(a, "url", "author.url")?,
            })
        }
        Some(_) => return Err("`author` must be an object".into()),
    };
    let keywords = match obj.get("keywords") {
        None => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .ok_or("`keywords` must contain only strings")
            })
            .collect::<Result<_, _>>()?,
        Some(_) => return Err("`keywords` must be an array".into()),
    };
    if obj.get("extensions").is_some_and(|v| !v.is_object()) {
        diagnostics.push(format!(
            "{MANIFEST_FILE}: `extensions` is not an object; ignored"
        ));
    }
    Ok(PluginManifest {
        name,
        version: opt_string(obj, "version", "version")?,
        description: opt_string(obj, "description", "description")?,
        author,
        homepage: opt_string(obj, "homepage", "homepage")?,
        repository: opt_string(obj, "repository", "repository")?,
        license: opt_string(obj, "license", "license")?,
        keywords,
    })
}

fn opt_string(obj: &Map<String, Value>, key: &str, label: &str) -> Result<Option<String>, String> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(format!("`{label}` must be a string")),
    }
}

/// Resolve a fixed location under the plugin root. `Ok(None)` when absent;
/// `Err` when it exists but escapes the root or is not a regular file.
fn contained_file(root: &Path, relative: &str) -> Result<Option<PathBuf>, String> {
    let path = root.join(relative);
    if std::fs::symlink_metadata(&path).is_err() {
        return Ok(None);
    }
    let resolved = contained(root, &path)?;
    if !resolved.is_file() {
        return Err("is not a regular file".into());
    }
    Ok(Some(resolved))
}

/// Canonicalize `path` and require it to stay inside the (canonical) `root`.
fn contained(root: &Path, path: &Path) -> Result<PathBuf, String> {
    let resolved = path
        .canonicalize()
        .map_err(|e| format!("cannot resolve `{}`: {e}", path.display()))?;
    if resolved.starts_with(root) {
        Ok(resolved)
    } else {
        Err(format!(
            "`{}` resolves outside the plugin root",
            path.display()
        ))
    }
}

fn read_json(path: &Path) -> Result<Value, String> {
    let meta = std::fs::metadata(path).map_err(|e| e.to_string())?;
    if meta.len() > MAX_MANIFEST_BYTES {
        return Err(format!("larger than {MAX_MANIFEST_BYTES} bytes"));
    }
    let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    serde_json::from_str(&text).map_err(|e| format!("invalid JSON: {e}"))
}

fn discover_skills(root: &Path) -> Result<(Vec<PluginSkill>, Vec<String>), String> {
    let dir = root.join(SKILLS_DIR);
    if std::fs::symlink_metadata(&dir).is_err() {
        return Ok((Vec::new(), Vec::new()));
    }
    let dir = contained(root, &dir)?;
    if !dir.is_dir() {
        return Err("is not a directory".into());
    }
    let mut entries: Vec<_> = std::fs::read_dir(&dir)
        .map_err(|e| e.to_string())?
        .filter_map(Result::ok)
        .collect();
    entries.sort_by_key(std::fs::DirEntry::file_name);
    let mut skills = Vec::new();
    let mut skipped = Vec::new();
    for entry in entries {
        let Some(directory) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let file = entry.path().join(SKILL_FILE);
        if std::fs::symlink_metadata(&file).is_err() {
            continue;
        }
        match contained(root, &file) {
            Ok(resolved) if resolved.is_file() => skills.push(PluginSkill {
                directory,
                file: resolved,
            }),
            Ok(_) => skipped.push(format!(
                "{SKILLS_DIR}/{directory}: {SKILL_FILE} is not a file; skipped"
            )),
            Err(e) => skipped.push(format!("{SKILLS_DIR}/{directory}: {e}; skipped")),
        }
    }
    Ok((skills, skipped))
}

type McpLoad = (BTreeMap<String, PluginMcpServer>, Vec<String>);

fn load_mcp(root: &Path, data_dir: &Path) -> Result<McpLoad, String> {
    let Some(path) = contained_file(root, MCP_FILE)? else {
        return Ok((BTreeMap::new(), Vec::new()));
    };
    let value = read_json(&path)?;
    let obj = value.as_object().ok_or("must be a JSON object")?;
    if let Some(key) = obj
        .keys()
        .find(|k| !matches!(k.as_str(), "$schema" | "mcpServers"))
    {
        return Err(format!("unknown top-level field `{key}`"));
    }
    match obj.get("$schema") {
        Some(Value::String(s)) if s == MCP_SCHEMA => {}
        Some(_) => return Err(format!("`$schema` must be `{MCP_SCHEMA}`")),
        None => return Err("`$schema` is required".into()),
    }
    let servers = obj
        .get("mcpServers")
        .ok_or("`mcpServers` is required")?
        .as_object()
        .ok_or("`mcpServers` must be an object")?;
    let mut out = BTreeMap::new();
    let mut skipped = Vec::new();
    for (name, raw) in servers {
        match parse_server(raw, root, data_dir) {
            Ok(server) => {
                out.insert(name.clone(), server);
            }
            Err(e) => skipped.push(format!("{MCP_FILE}: server `{name}`: {e}; skipped")),
        }
    }
    Ok((out, skipped))
}

fn parse_server(raw: &Value, root: &Path, data_dir: &Path) -> Result<PluginMcpServer, String> {
    let obj = raw.as_object().ok_or("must be an object")?;
    let kind = obj
        .get("type")
        .and_then(Value::as_str)
        .ok_or("`type` is required")?;
    match kind {
        "stdio" => parse_stdio(obj, root, data_dir),
        "streamable-http" | "sse" => {
            let url = obj
                .get("url")
                .and_then(Value::as_str)
                .ok_or("`url` is required")?;
            validate_remote_url(url)?;
            let headers = string_map(obj.get("headers"), "headers")?;
            let mut seen = BTreeSet::new();
            if let Some(dup) = headers
                .keys()
                .find(|k| !seen.insert(k.to_ascii_lowercase()))
            {
                return Err(format!("duplicate header `{dup}`"));
            }
            let url = url.to_string();
            Ok(if kind == "sse" {
                PluginMcpServer::Sse { url, headers }
            } else {
                PluginMcpServer::Http { url, headers }
            })
        }
        other => Err(format!("unsupported `type` `{other}`")),
    }
}

fn parse_stdio(
    obj: &Map<String, Value>,
    root: &Path,
    data_dir: &Path,
) -> Result<PluginMcpServer, String> {
    let command = obj
        .get("command")
        .and_then(Value::as_str)
        .ok_or("`command` is required")?;
    let command = resolve_command(command, root)?;
    let args = match obj.get("args") {
        None => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|v| {
                v.as_str()
                    .map(|s| expand(s, root, data_dir))
                    .ok_or("`args` must contain only strings")
            })
            .collect::<Result<_, _>>()?,
        Some(_) => return Err("`args` must be an array".into()),
    };
    let env = string_map(obj.get("env"), "env")?;
    if let Some(key) = env
        .keys()
        .find(|k| k.as_str() == ROOT_VAR || k.as_str() == DATA_VAR)
    {
        return Err(format!("`env` must not set `{key}`"));
    }
    let env = env
        .into_iter()
        .map(|(k, v)| (k, expand(&v, root, data_dir)))
        .collect();
    let cwd = match obj.get("cwd") {
        None => root.to_path_buf(),
        Some(Value::String(s)) => resolve_cwd(s, root, data_dir)?,
        Some(_) => return Err("`cwd` must be a string".into()),
    };
    Ok(PluginMcpServer::Stdio {
        command,
        args,
        env,
        cwd,
    })
}

/// `command` is one token: a bare name (resolved on `PATH` at launch) or a
/// `./` path that must stay inside the plugin root. Never expanded.
fn resolve_command(command: &str, root: &Path) -> Result<String, String> {
    if let Some(rest) = command.strip_prefix("./") {
        if rest.is_empty() {
            return Err("`command` must name a file".into());
        }
        let path = contained(root, &root.join(rest))?;
        if !path.is_file() {
            return Err("`command` is not a file".into());
        }
        return Ok(path.to_string_lossy().into_owned());
    }
    let bare = !command.is_empty()
        && !command.contains(['/', '\\'])
        && !command.chars().any(char::is_whitespace)
        && command != "."
        && command != "..";
    if bare {
        Ok(command.to_string())
    } else {
        Err("`command` must be a bare executable name or a `./` path".into())
    }
}

fn resolve_cwd(cwd: &str, root: &Path, data_dir: &Path) -> Result<PathBuf, String> {
    if let Some(rest) = cwd.strip_prefix("./") {
        return contained_dir(root, &root.join(rest));
    }
    for (var, base) in [(ROOT_VAR, root), (DATA_VAR, data_dir)] {
        let token = format!("${{{var}}}");
        if let Some(rest) = cwd.strip_prefix(&token) {
            if rest.is_empty() {
                return Ok(base.to_path_buf());
            }
            let Some(rest) = rest.strip_prefix('/') else {
                continue;
            };
            if var == DATA_VAR {
                return lexically_contained(base, rest)
                    .ok_or_else(|| format!("`cwd` `{cwd}` escapes {DATA_VAR}"));
            }
            return contained_dir(root, &root.join(rest));
        }
    }
    Err("`cwd` must be a `./` path or start with `${PLUGIN_ROOT}` or `${PLUGIN_DATA}`".into())
}

fn contained_dir(root: &Path, path: &Path) -> Result<PathBuf, String> {
    let resolved = contained(root, path)?;
    if resolved.is_dir() {
        Ok(resolved)
    } else {
        Err(format!("`{}` is not a directory", path.display()))
    }
}

/// `PLUGIN_DATA` may not exist yet, so its subpaths are checked lexically.
fn lexically_contained(base: &Path, rest: &str) -> Option<PathBuf> {
    let mut out = base.to_path_buf();
    for part in rest.split('/').filter(|p| !p.is_empty() && *p != ".") {
        if part == ".." {
            return None;
        }
        out.push(part);
    }
    Some(out)
}

/// One non-recursive pass replacing `${PLUGIN_ROOT}` and `${PLUGIN_DATA}`.
fn expand(value: &str, root: &Path, data_dir: &Path) -> String {
    let root_token = format!("${{{ROOT_VAR}}}");
    let data_token = format!("${{{DATA_VAR}}}");
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let tail = &rest[start..];
        if tail.starts_with(&root_token) {
            out.push_str(&root.to_string_lossy());
            rest = &tail[root_token.len()..];
        } else if tail.starts_with(&data_token) {
            out.push_str(&data_dir.to_string_lossy());
            rest = &tail[data_token.len()..];
        } else {
            out.push_str("${");
            rest = &tail[2..];
        }
    }
    out.push_str(rest);
    out
}

fn string_map(value: Option<&Value>, label: &str) -> Result<BTreeMap<String, String>, String> {
    match value {
        None => Ok(BTreeMap::new()),
        Some(Value::Object(map)) => map
            .iter()
            .map(|(k, v)| {
                v.as_str()
                    .map(|s| (k.clone(), s.to_string()))
                    .ok_or_else(|| format!("`{label}` values must be strings"))
            })
            .collect(),
        Some(_) => Err(format!("`{label}` must be an object")),
    }
}

fn validate_remote_url(raw: &str) -> Result<(), String> {
    let url = reqwest::Url::parse(raw).map_err(|e| format!("invalid `url`: {e}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("`url` must use http or https".into());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("`url` must not contain user info".into());
    }
    if url.fragment().is_some() {
        return Err("`url` must not contain a fragment".into());
    }
    let host = url.host_str().ok_or("`url` must have a host")?;
    let loopback = host.eq_ignore_ascii_case("localhost")
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    if url.scheme() == "http" && !loopback {
        return Err("`url` must use https for non-loopback hosts".into());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Installed plugins (user scope)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum PluginSourceKind {
    Path,
    Git,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RegistryEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<PluginSourceKind>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub installed_at: Option<u64>,
}

fn default_true() -> bool {
    true
}

impl Default for RegistryEntry {
    fn default() -> Self {
        Self {
            source: None,
            kind: None,
            enabled: true,
            installed_at: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Registry {
    #[serde(default)]
    pub plugins: BTreeMap<String, RegistryEntry>,
}

/// Filesystem layout for user-scope plugins: `<base>/plugins/<name>/`,
/// `<base>/plugin-data/<name>/` and the `<base>/plugins.json` registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginPaths {
    base: PathBuf,
}

impl PluginPaths {
    pub(crate) fn new(base: impl Into<PathBuf>) -> Self {
        Self { base: base.into() }
    }

    /// `~/.intent`. Unit tests get no user tier unless they pass one.
    pub(crate) fn user() -> Option<Self> {
        if cfg!(test) {
            return None;
        }
        std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .filter(|h| !h.is_empty())
            .map(|h| Self::new(PathBuf::from(h).join(".intent")))
    }

    pub(crate) fn plugins_dir(&self) -> PathBuf {
        self.base.join("plugins")
    }

    pub(crate) fn plugin_dir(&self, name: &str) -> PathBuf {
        self.plugins_dir().join(name)
    }

    pub(crate) fn data_dir(&self, name: &str) -> PathBuf {
        self.base.join("plugin-data").join(name)
    }

    pub(crate) fn registry_file(&self) -> PathBuf {
        self.base.join(REGISTRY_FILE)
    }

    pub(crate) fn read_registry(&self) -> Registry {
        match std::fs::read_to_string(self.registry_file()) {
            Ok(text) => serde_json::from_str(&text).unwrap_or_else(|e| {
                tracing::warn!(error = %e, "plugins.json is invalid; treating every plugin as enabled");
                Registry::default()
            }),
            Err(_) => Registry::default(),
        }
    }

    pub(crate) fn write_registry(&self, registry: &Registry) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.base)?;
        let tmp = self
            .base
            .join(format!(".{REGISTRY_FILE}.{}", uuid::Uuid::new_v4()));
        let body = serde_json::to_vec_pretty(registry).map_err(std::io::Error::other)?;
        std::fs::write(&tmp, body)?;
        std::fs::rename(&tmp, self.registry_file())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InstalledPlugin {
    Loaded {
        plugin: LoadedPlugin,
        entry: RegistryEntry,
    },
    Rejected {
        directory: String,
        rejection: PluginRejection,
        entry: RegistryEntry,
    },
}

impl InstalledPlugin {
    pub(crate) fn enabled_plugin(&self) -> Option<&LoadedPlugin> {
        match self {
            Self::Loaded { plugin, entry } if entry.enabled => Some(plugin),
            _ => None,
        }
    }
}

/// Every directory under `plugins/`, sorted by name. A directory whose name
/// differs from its manifest `name` is rejected so names stay unique.
pub(crate) fn list_installed(paths: &PluginPaths) -> Vec<InstalledPlugin> {
    let registry = paths.read_registry();
    let Ok(entries) = std::fs::read_dir(paths.plugins_dir()) else {
        return Vec::new();
    };
    let mut dirs: Vec<(String, PathBuf)> = entries
        .filter_map(Result::ok)
        .filter(|e| e.path().is_dir())
        .filter_map(|e| Some((e.file_name().to_str()?.to_owned(), e.path())))
        .filter(|(name, _)| !name.starts_with('.'))
        .collect();
    dirs.sort();
    dirs.truncate(MAX_PLUGINS);
    dirs.into_iter()
        .map(|(directory, path)| {
            let entry = registry
                .plugins
                .get(&directory)
                .cloned()
                .unwrap_or_default();
            match load_plugin(&path, &paths.data_dir(&directory)) {
                Ok(plugin) if plugin.manifest.name == directory => {
                    InstalledPlugin::Loaded { plugin, entry }
                }
                Ok(plugin) => InstalledPlugin::Rejected {
                    rejection: PluginRejection {
                        root: path,
                        reason: format!(
                            "directory name `{directory}` does not match plugin name `{}`",
                            plugin.manifest.name
                        ),
                    },
                    directory,
                    entry,
                },
                Err(rejection) => InstalledPlugin::Rejected {
                    directory,
                    rejection,
                    entry,
                },
            }
        })
        .collect()
}

/// Enabled, valid user plugins.
pub(crate) fn enabled_user_plugins() -> Vec<LoadedPlugin> {
    let Some(paths) = PluginPaths::user() else {
        return Vec::new();
    };
    list_installed(&paths)
        .into_iter()
        .filter_map(|p| p.enabled_plugin().cloned())
        .collect()
}

/// Session MCP name for a plugin server: `<plugin>-<server>`, limited to
/// `[A-Za-z0-9_-]` because several providers reject other characters.
pub(crate) fn mcp_server_name(plugin: &str, server: &str) -> String {
    format!("{plugin}-{server}")
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Session MCP entries for `plugins`. Neither the ACP stdio shape nor the
/// provider config files carry a working directory, so stdio servers start
/// through `<launcher> plugin-exec --cwd <dir> -- <command> <args…>`.
/// Existing names in `out` win; a clash is logged and the plugin entry dropped.
pub(crate) fn merge_session_servers(
    plugins: &[LoadedPlugin],
    launcher: &str,
    launcher_env: &BTreeMap<String, String>,
    out: &mut intent_acp::NormalizedMcpServers,
) {
    for plugin in plugins {
        for (server, config) in &plugin.mcp_servers {
            let name = mcp_server_name(&plugin.manifest.name, server);
            if out.contains_key(&name) {
                tracing::warn!(plugin = %plugin.manifest.name, server = %server, name = %name,
                    "plugin MCP server name is taken; skipping");
                continue;
            }
            let normalized = match config {
                PluginMcpServer::Stdio {
                    command,
                    args,
                    env,
                    cwd,
                } => {
                    let mut full_env = launcher_env.clone();
                    full_env.extend(env.clone());
                    full_env.insert(ROOT_VAR.into(), plugin.root.to_string_lossy().into_owned());
                    full_env.insert(
                        DATA_VAR.into(),
                        plugin.data_dir.to_string_lossy().into_owned(),
                    );
                    let mut launch = vec![
                        "plugin-exec".to_string(),
                        "--cwd".to_string(),
                        cwd.to_string_lossy().into_owned(),
                        "--".to_string(),
                        command.clone(),
                    ];
                    launch.extend(args.iter().cloned());
                    intent_acp::NormalizedMcpServer::Stdio {
                        command: launcher.to_string(),
                        args: launch,
                        env: full_env,
                    }
                }
                PluginMcpServer::Http { url, headers } => intent_acp::NormalizedMcpServer::Http {
                    url: url.clone(),
                    headers: (!headers.is_empty()).then(|| headers.clone()),
                },
                PluginMcpServer::Sse { url, headers } => intent_acp::NormalizedMcpServer::Sse {
                    url: url.clone(),
                    headers: (!headers.is_empty()).then(|| headers.clone()),
                },
            };
            out.insert(name, normalized);
        }
    }
}

/// Create each plugin's `PLUGIN_DATA` directory before its servers start.
pub(crate) fn ensure_data_dirs(plugins: &[LoadedPlugin]) {
    for plugin in plugins.iter().filter(|p| !p.mcp_servers.is_empty()) {
        if let Err(e) = std::fs::create_dir_all(&plugin.data_dir) {
            tracing::warn!(plugin = %plugin.manifest.name, error = %e, "cannot create PLUGIN_DATA");
        }
    }
}

/// Skill catalog name for a plugin skill.
pub(crate) fn skill_name(plugin: &str, skill: &str) -> String {
    format!("{plugin}:{skill}")
}

#[cfg(test)]
mod tests;

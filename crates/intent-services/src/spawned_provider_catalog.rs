//! Read-only workspace catalog. This is deliberately separate from the public
//! provider registry and from host policy/profile construction. Call in a
//! blocking pool; never on a hot RPC path. A snapshot is NOT invocation authority.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::Read;
use std::path::{Path, PathBuf};

use intent_acp::mcp_config::{NormalizedMcpServer, NormalizedMcpServers};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

use crate::skills::spawned::{discover_project_skills, ProjectSkillSnapshot};

mod parser;
#[cfg(test)]
mod tests;

const MAX_SOURCE_BYTES: usize = 1024 * 1024;
const MAX_TOTAL_BYTES: usize = 8 * 1024 * 1024;
const MAX_SERVERS: usize = 1024;
const MAX_PROJECT_DEPTH: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CatalogPurpose {
    Interactive,
    Ephemeral,
}

/// Restrictions must travel with the normalized transport; ACP alone cannot
/// represent them. Consumers must enforce them or reject unsupported delivery.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ExecutionOptions {
    pub cwd: Option<PathBuf>,
    pub startup_timeout_ms: Option<u64>,
    pub tool_timeout_ms: Option<u64>,
    pub required: bool,
    /// None means unrestricted; Some([]) means no tools.
    pub enabled_tools: Option<Vec<String>>,
    pub disabled_tools: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) enum ServerIdentity {
    Intent {
        id: String,
    },
    Project {
        workspace_id: String,
        source_relative_path: String,
        server_key: String,
    },
}

#[derive(Clone)]
pub(crate) struct ExplicitServer {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    /// A disabled tombstone need not contain an executable definition.
    pub server: Option<NormalizedMcpServer>,
    pub execution: ExecutionOptions,
}

pub(crate) struct CatalogInputs<'a> {
    pub workspace_id: &'a str,
    pub root: Option<&'a Path>,
    pub cwd: &'a Path,
    pub purpose: CatalogPurpose,
    pub enable_user_servers: bool,
    pub explicit: &'a [ExplicitServer],
    pub global_disabled_ids: &'a [String],
    pub workspace_disabled_ids: &'a [String],
    /// Supplied by the caller; discovery never reads the process environment.
    pub environment: BTreeMap<String, String>,
}

#[derive(Clone)]
pub(crate) struct CatalogEntry {
    pub identity: ServerIdentity,
    pub source: String,
    pub enabled: bool,
    pub server: Option<NormalizedMcpServer>,
    pub execution: ExecutionOptions,
}

pub(crate) struct CatalogSnapshot {
    /// Eligible transports only. Apply selected-provider/host policy next, and
    /// recheck live service restrictions when forwarding a call by identity.
    pub servers: NormalizedMcpServers,
    /// Includes tombstones; never resurrect an overwritten project definition.
    pub entries: BTreeMap<String, CatalogEntry>,
    pub skills: ProjectSkillSnapshot,
    pub fingerprint: String,
}

/// No parser error text or field VALUES: parsers often echo credentials.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CatalogError {
    pub source: String,
    pub field: String,
    pub reason: &'static str,
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}: {}", self.source, self.field, self.reason)
    }
}
impl std::error::Error for CatalogError {}

pub(super) fn error(source: &str, field: &str, reason: &'static str) -> CatalogError {
    CatalogError {
        source: source.into(),
        field: field.into(),
        reason,
    }
}

/// Canonical root-to-cwd sequence; no ancestor outside the repository is read.
pub(crate) fn project_directories(root: &Path, cwd: &Path) -> Result<Vec<PathBuf>, CatalogError> {
    let root = root
        .canonicalize()
        .map_err(|_| error("workspace", "root", "unreadable directory"))?;
    let cwd = cwd
        .canonicalize()
        .map_err(|_| error("workspace", "cwd", "unreadable directory"))?;
    if !root.is_dir() || !cwd.is_dir() || !cwd.starts_with(&root) {
        return Err(error("workspace", "cwd", "outside workspace directory"));
    }
    let relative = cwd.strip_prefix(&root).expect("checked boundary");
    if relative.components().count() >= MAX_PROJECT_DEPTH {
        return Err(error("workspace", "cwd", "project depth limit exceeded"));
    }
    let mut directories = vec![root.clone()];
    let mut current = root;
    for component in relative.components() {
        current.push(component);
        directories.push(current.clone());
    }
    Ok(directories)
}

pub(crate) fn resolve_project_catalog(
    inputs: &CatalogInputs<'_>,
) -> Result<CatalogSnapshot, CatalogError> {
    // No discovery, bridge, or bundled inventory for ephemeral requests.
    if inputs.purpose == CatalogPurpose::Ephemeral {
        return Ok(snapshot(
            BTreeMap::new(),
            ProjectSkillSnapshot::empty(),
            b"ephemeral",
        ));
    }
    let directories = inputs
        .root
        .map(|root| project_directories(root, inputs.cwd))
        .transpose()?
        .unwrap_or_default();
    let skills = discover_project_skills(&directories);
    if !inputs.enable_user_servers {
        return Ok(snapshot(BTreeMap::new(), skills, b"external-mcp-disabled"));
    }
    if inputs
        .environment
        .iter()
        .try_fold(0usize, |total, (key, value)| {
            total.checked_add(key.len())?.checked_add(value.len())
        })
        .is_none_or(|bytes| bytes > MAX_TOTAL_BYTES)
    {
        return Err(error(
            "Intent",
            "environment",
            "environment snapshot size limit exceeded",
        ));
    }
    let mut entries = BTreeMap::new();
    let mut project_definitions = BTreeMap::new();
    let mut source_digest = Sha256::new();
    let mut bytes_read = 0;
    let mut definitions_read = 0;
    let expanded_bytes = std::cell::Cell::new(0);
    if let Some(root) = directories.first() {
        for directory in &directories {
            for &(relative, format) in parser::SOURCES {
                let path = directory.join(relative);
                let source = path
                    .strip_prefix(root)
                    .expect("project source")
                    .to_string_lossy()
                    .replace('\\', "/");
                let Some(bytes) = read_source(root, &path, &source)? else {
                    continue;
                };
                bytes_read += bytes.len();
                if bytes_read > MAX_TOTAL_BYTES {
                    return Err(error(&source, "file", "total source size limit exceeded"));
                }
                source_digest.update(source.as_bytes());
                source_digest.update(&bytes);
                let parsed = parser::parse(&bytes, format, &source, directory)?;
                definitions_read += parsed.len();
                if definitions_read > MAX_SERVERS {
                    return Err(error(&source, "servers", "server limit exceeded"));
                }
                for (name, project_definition) in parsed {
                    let definition = &project_definition.definition;
                    validate_name(&name, &source)?;
                    entries.insert(
                        name.clone(),
                        CatalogEntry {
                            identity: ServerIdentity::Project {
                                workspace_id: inputs.workspace_id.into(),
                                source_relative_path: source.clone(),
                                server_key: name.clone(),
                            },
                            source: source.clone(),
                            enabled: definition.enabled,
                            server: definition.server.clone(),
                            execution: definition.execution.clone(),
                        },
                    );
                    project_definitions.insert(name, project_definition);
                }
            }
        }
    }
    if inputs.explicit.len() > MAX_SERVERS {
        return Err(error("Intent", "servers", "server limit exceeded"));
    }
    let mut explicit_names = BTreeSet::new();
    let mut explicit_ids = BTreeSet::new();
    for definition in inputs.explicit {
        let source = "Intent mcp.servers";
        validate_name(&definition.name, source)?;
        if definition.id.is_empty() || !explicit_ids.insert(&definition.id) {
            return Err(error(source, "id", "empty or duplicate explicit ID"));
        }
        if !explicit_names.insert(&definition.name) {
            return Err(error(source, "name", "duplicate explicit name"));
        }
        let enabled = definition.enabled
            && !inputs.global_disabled_ids.contains(&definition.id)
            && !inputs.workspace_disabled_ids.contains(&definition.id);
        if enabled && definition.server.is_none() {
            return Err(error(source, "server", "enabled server lacks a definition"));
        }
        project_definitions.remove(&definition.name);
        entries.insert(
            definition.name.clone(),
            CatalogEntry {
                identity: ServerIdentity::Intent {
                    id: definition.id.clone(),
                },
                source: source.into(),
                enabled,
                server: definition.server.clone(),
                execution: definition.execution.clone(),
            },
        );
    }
    let mut codex_names = BTreeSet::new();
    let mut pi_names = BTreeSet::new();
    for (name, entry) in &entries {
        if !codex_names.insert(codex_name(name)) || !pi_names.insert(pi_name(name)) {
            return Err(error(
                &entry.source,
                "name",
                "names collide after provider normalization",
            ));
        }
    }
    // Expand credentials only after atomic precedence and explicit tombstones.
    for (name, raw) in project_definitions {
        if let Some(entry) = entries.get_mut(&name).filter(|entry| entry.enabled) {
            let definition =
                raw.resolve(&inputs.environment, &expanded_bytes)
                    .map_err(|mut error| {
                        error.field = format!("{name}.{}", error.field);
                        error
                    })?;
            entry.server = definition.server;
            entry.execution = definition.execution;
        }
    }
    Ok(snapshot(entries, skills, &source_digest.finalize()))
}

fn codex_name(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_whitespace() { '_' } else { c })
        .collect()
}

fn pi_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn validate_name(name: &str, source: &str) -> Result<(), CatalogError> {
    if name.trim().is_empty() || name.len() > 256 || name.chars().any(char::is_control) {
        return Err(error(source, "name", "invalid server name"));
    }
    if pi_name(name) == pi_name("workspace-mcp") || codex_name(name) == "workspace-mcp" {
        return Err(error(source, "name", "reserved workspace bridge name"));
    }
    Ok(())
}

fn read_source(root: &Path, path: &Path, source: &str) -> Result<Option<Vec<u8>>, CatalogError> {
    // Check symlink metadata first: a broken declared link is an error, not absence.
    match std::fs::symlink_metadata(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Missing leaf below a dangling symlink must not silently become absent.
            let mut ancestor = path.parent();
            while let Some(parent) = ancestor.filter(|p| p.starts_with(root)) {
                if std::fs::symlink_metadata(parent).is_ok_and(|m| m.is_symlink())
                    && parent.canonicalize().is_err()
                {
                    return Err(error(source, "file", "broken source symlink"));
                }
                ancestor = parent.parent();
            }
            return Ok(None);
        }
        Err(_) => return Err(error(source, "file", "unreadable source")),
        Ok(_) => {}
    }
    let canonical = path
        .canonicalize()
        .map_err(|_| error(source, "file", "unresolvable source"))?;
    if !canonical.starts_with(root) {
        return Err(error(
            source,
            "file",
            "MCP source symlink escapes workspace",
        ));
    }
    let metadata =
        std::fs::metadata(&canonical).map_err(|_| error(source, "file", "unreadable source"))?;
    if !metadata.is_file() || metadata.len() > MAX_SOURCE_BYTES as u64 {
        return Err(error(
            source,
            "file",
            "source is not a bounded regular file",
        ));
    }
    let mut bytes = Vec::new();
    std::fs::File::open(&canonical)
        .and_then(|file| {
            file.take(MAX_SOURCE_BYTES as u64 + 1)
                .read_to_end(&mut bytes)
        })
        .map_err(|_| error(source, "file", "unreadable source"))?;
    if bytes.len() > MAX_SOURCE_BYTES {
        return Err(error(source, "file", "source size limit exceeded"));
    }
    Ok(Some(bytes))
}

fn snapshot(
    entries: BTreeMap<String, CatalogEntry>,
    skills: ProjectSkillSnapshot,
    sources: &[u8],
) -> CatalogSnapshot {
    let mut digest = Sha256::new();
    digest.update(sources);
    digest.update(skills.fingerprint.as_bytes());
    let mut servers = NormalizedMcpServers::new();
    for (name, entry) in &entries {
        // JSON framing avoids concatenation ambiguity; values only enter a hash.
        let transport = entry.server.as_ref().map(parser::transport_value);
        digest.update(
            serde_json::to_vec(&(
                name,
                &entry.identity,
                entry.enabled,
                transport,
                &entry.execution,
            ))
            .expect("catalog is serializable"),
        );
        if entry.enabled {
            if let Some(server) = &entry.server {
                servers.insert(name.clone(), server.clone());
            }
        }
    }
    CatalogSnapshot {
        servers,
        entries,
        skills,
        fingerprint: digest
            .finalize()
            .iter()
            .fold(String::with_capacity(64), |mut text, byte| {
                write!(text, "{byte:02x}").expect("writing to a String is infallible");
                text
            }),
    }
}

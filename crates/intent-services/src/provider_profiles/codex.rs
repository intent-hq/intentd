//! Codex ACP 2.1.0 / native 0.160.0 snapshot controls. This deliberately does
//! not claim to suppress ACP command discovery, hot reload or future layers.
use super::{storage, ProviderLaunchProfile, ProviderProfileError, ProviderProfileRequest};
use intent_acp::mcp_config::NormalizedMcpServer;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use storage::{io_error, read_optional};

#[derive(Clone, PartialEq, Eq)]
pub enum NativeTransport {
    Stdio(String),
    Http(String),
}

/// Union inventory for suppression only; never imported into Intent's catalog.
#[derive(Clone)]
pub struct NativeMcpEntry {
    pub name: String,
    pub transport: NativeTransport,
    pub sources: Vec<PathBuf>,
}

fn config_error() -> ProviderProfileError {
    ProviderProfileError::new("native-config-invalid", "Native Codex configuration could not be inventoried; correct the configuration before launching.")
}

fn document(path: &Path) -> Result<Option<toml_edit::DocumentMut>, ProviderProfileError> {
    let Some(bytes) = read_optional(path)? else {
        return Ok(None);
    };
    Ok(Some(
        std::str::from_utf8(&bytes)
            .map_err(|_| config_error())?
            .parse()
            .map_err(|_| config_error())?,
    ))
}

pub(super) fn prepare(
    request: &ProviderProfileRequest<'_>,
    profile: &mut ProviderLaunchProfile,
) -> Result<(), ProviderProfileError> {
    let source_home = request
        .provider_home
        .map_or_else(|| request.home.join(".codex"), Path::to_path_buf);
    let source_config = source_home.join("config.toml");
    let source_doc = document(&source_config)?;
    profile
        .storage
        .seed_once(&source_home.join("auth.json"), "auth.json")?;
    // Reuse the established routing allowlist; never copy MCP, skills, hooks,
    // plugins, trust or permission settings from a writable user config.
    let seed =
        crate::provider_models::minimal_codex_config_seed(&source_config).unwrap_or_default();
    let mut seed: toml_edit::DocumentMut = seed.parse().map_err(|_| config_error())?;
    if let Some(doc) = source_doc {
        for key in [
            "cli_auth_credentials_store",
            "forced_login_method",
            "forced_chatgpt_workspace_id",
        ] {
            if let Some(value) = doc.get(key).and_then(toml_edit::Item::as_str) {
                seed[key] = toml_edit::value(value);
            }
        }
    }
    profile
        .storage
        .write("config.toml", seed.to_string().as_bytes())?;
    profile
        .env
        .insert("CODEX_HOME".into(), profile.path().to_string_lossy().into());
    // Overrides supplied by a launcher are not native configuration inventory.
    // CODEX_CONFIG is replaced as one complete owned map below.
    let mut files = BTreeSet::from([source_config]);
    files.extend(request.native_config_files.iter().cloned());
    // Native autoload can inspect ancestors beyond Intent's import boundary.
    // Reading names for suppression does not grant project capabilities/trust.
    for ancestor in request.launch_cwd.ancestors().take(128) {
        files.insert(ancestor.join(".codex/config.toml"));
    }
    let mut inventory = BTreeMap::<String, NativeMcpEntry>::new();
    for file in files {
        let Some(doc) = document(&file)? else {
            continue;
        };
        let Some(servers) = doc.get("mcp_servers") else {
            continue;
        };
        for (name, entry) in servers.as_table_like().ok_or_else(config_error)?.iter() {
            if entry.get("type").and_then(toml_edit::Item::as_str) == Some("sse") {
                return Err(ProviderProfileError::new("mcp-transport-unsupported", "Native Codex SSE entries are not an audited transport; configure streamable HTTP before launching."));
            }
            let command = entry.get("command").and_then(toml_edit::Item::as_str);
            let url = entry.get("url").and_then(toml_edit::Item::as_str);
            let transport = match (command, url) {
                (Some(command), None) if !command.is_empty() => {
                    NativeTransport::Stdio(command.into())
                }
                (None, Some(url)) if !url.is_empty() => NativeTransport::Http(url.into()),
                _ => return Err(config_error()),
            };
            if let Some(existing) = inventory.get_mut(name) {
                if std::mem::discriminant(&existing.transport) != std::mem::discriminant(&transport)
                {
                    return Err(ProviderProfileError::new("native-transport-conflict", "Native Codex layers disagree on an MCP transport; resolve that name collision before launching."));
                }
                existing.sources.push(file.clone());
            } else {
                inventory.insert(
                    name.into(),
                    NativeMcpEntry {
                        name: name.into(),
                        transport,
                        sources: vec![file.clone()],
                    },
                );
            }
        }
    }
    let mut config = json!({"agents":{"enabled":false},"features":{"multi_agent_v2":false},"mcp_servers":{},"skills":{"config":[]}});
    for entry in inventory.values() {
        config["mcp_servers"][&entry.name] = match &entry.transport {
            NativeTransport::Stdio(command) => json!({"command":command,"enabled":false}),
            NativeTransport::Http(url) => json!({"url":url,"enabled":false}),
        };
    }
    if !profile.policy.permits_internal_aliases() && !profile.approved_mcp.is_empty() {
        return Err(ProviderProfileError::new("managed-policy-conflict", "Codex managed MCP names cannot be safely remapped for isolated injection; ask your administrator to resolve the collision policy."));
    }
    for (name, server) in &profile.approved_mcp {
        let hash = storage::digest_hex(name.as_bytes());
        let mut internal = format!("intent_{hash}");
        while inventory.contains_key(&internal) || profile.approved_mcp.contains_key(&internal) {
            internal.push('_');
        }
        config["mcp_servers"][&internal] = match server {
            NormalizedMcpServer::Stdio{command,args,env} => json!({"command":command,"args":args,"env":env,"enabled":true}),
            NormalizedMcpServer::Http{url,headers} => {
                let mut value = json!({"url":url,"enabled":true});
                if let Some(headers) = headers {value["http_headers"] = json!(headers);}
                value
            }
            NormalizedMcpServer::Sse{..} => return Err(ProviderProfileError::new("mcp-transport-unsupported", "Codex's audited native transport does not support explicit SSE; configure a streamable HTTP endpoint.")),
        };
        profile.mcp_name_mapping.insert(internal, name.clone());
    }
    let mut roots = vec![
        request.home.join(".agents/skills"),
        source_home.join("skills"),
        profile.path().join("skills"),
    ];
    roots.extend(request.native_skill_roots.iter().cloned());
    for ancestor in request.launch_cwd.ancestors().take(128) {
        roots.push(ancestor.join(".agents/skills"));
    }
    let (paths, bounded) = inventory_skills(&roots)?;
    config["skills"]["config"] = Value::Array(
        paths
            .iter()
            .map(|path| json!({"path":path,"enabled":false}))
            .collect(),
    );
    if bounded {
        profile.diagnostic("native-inventory-incomplete", "Skill inventory reached its traversal limit or a symbolic link; native skills may remain. Use a clean profile and restart after configuration changes.");
    }
    profile.native_mcp_inventory = inventory.into_values().collect();
    profile
        .env
        .insert("CODEX_CONFIG".into(), config.to_string());
    profile.session_mcp.clear();
    profile.diagnostic("native-command-catalog-residual", "Codex ACP 2.1.0 still publishes native skill commands independently of thread skill disables; prompt behavior is unverified.");
    profile.diagnostic("native-config-reload-residual", "Codex suppression inventories are rebuilt on launch/resume only. New names and inactive layer transitions can escape a running snapshot; restart after native configuration changes.");
    profile.diagnostic("auth-store-unverified", "File credentials and model routing are seeded narrowly; keychain identity, token refresh and authenticated persisted resume require verification.");
    Ok(())
}

/// Bounded local metadata inventory; no skill content is injected and links are
/// never followed. Native symlink/custom/plugin sources remain diagnosed.
fn inventory_skills(roots: &[PathBuf]) -> Result<(BTreeSet<PathBuf>, bool), ProviderProfileError> {
    let mut pending: Vec<_> = roots.iter().map(|root| (root.clone(), 0)).collect();
    let mut seen = BTreeSet::new();
    let mut paths = BTreeSet::new();
    let mut entries = 0;
    let mut bounded = false;
    while let Some((path, depth)) = pending.pop() {
        let meta = match fs::symlink_metadata(&path) {
            Ok(meta) => meta,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(io_error(e)),
        };
        if meta.file_type().is_symlink() {
            bounded = true;
            continue;
        }
        if !meta.is_dir() {
            continue;
        }
        let canonical = path.canonicalize().map_err(io_error)?;
        if !seen.insert(canonical.clone()) {
            continue;
        }
        if seen.len() > 2000 {
            bounded = true;
            break;
        }
        for entry in fs::read_dir(&canonical).map_err(io_error)? {
            entries += 1;
            if entries > 20_000 {
                return Ok((paths, true));
            }
            let entry = entry.map_err(io_error)?;
            let kind = entry.file_type().map_err(io_error)?;
            if kind.is_symlink() {
                bounded = true;
                continue;
            }
            if kind.is_file() && entry.file_name() == "SKILL.md" {
                paths.insert(entry.path());
            }
            if kind.is_dir() {
                if depth < 4 {
                    pending.push((entry.path(), depth + 1));
                } else {
                    bounded = true;
                }
            }
        }
    }
    Ok((paths, bounded))
}

//! `plugin.*` operations on user-scope plugins. Mutations are serialized,
//! stage into a hidden sibling directory, validate, then swap in by rename so a
//! failed install or update never leaves a half-written plugin behind.

use std::path::{Path, PathBuf};
use std::time::Duration;

use intent_core::{Error, Result};
use serde_json::{json, Value};

use super::{
    list_installed, load_plugin, mcp_server_name, InstalledPlugin, LoadedPlugin, PluginPaths,
    PluginSourceKind, RegistryEntry,
};

const CLONE_TIMEOUT: Duration = Duration::from_secs(180);
const MAX_COPY_ENTRIES: usize = 20_000;

static MUTATION_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn user_paths() -> Result<PluginPaths> {
    PluginPaths::user().ok_or_else(|| Error::Internal("home directory is not set".into()))
}

pub(crate) async fn list() -> Result<Value> {
    let paths = user_paths()?;
    blocking(move || Ok(list_value(&paths))).await
}

pub(crate) fn list_value(paths: &PluginPaths) -> Value {
    let plugins: Vec<Value> = list_installed(paths).iter().map(info).collect();
    json!({ "plugins": plugins })
}

pub(crate) async fn install(source: String) -> Result<Value> {
    let paths = user_paths()?;
    install_at(&paths, &source).await
}

pub(crate) async fn update(name: String) -> Result<Value> {
    let paths = user_paths()?;
    update_at(&paths, &name).await
}

pub(crate) async fn uninstall(name: String, keep_data: bool) -> Result<Value> {
    let paths = user_paths()?;
    uninstall_at(&paths, &name, keep_data).await
}

pub(crate) async fn toggle(name: String, enabled: bool) -> Result<Value> {
    let paths = user_paths()?;
    toggle_at(&paths, &name, enabled).await
}

pub(crate) async fn install_at(paths: &PluginPaths, source: &str) -> Result<Value> {
    let _guard = MUTATION_LOCK.lock().await;
    let (kind, source) = classify_source(source)?;
    let staged = stage(paths, kind, &source).await?;
    let name = staged.plugin.manifest.name.clone();
    let target = paths.plugin_dir(&name);
    if target.exists() {
        staged.discard();
        return Err(Error::InvalidParams(format!(
            "plugin `{name}` is already installed; update or uninstall it first"
        )));
    }
    let paths = paths.clone();
    blocking(move || {
        std::fs::rename(&staged.dir, &target).map_err(io_error("install plugin"))?;
        let mut registry = paths.read_registry();
        registry.plugins.insert(
            name.clone(),
            RegistryEntry {
                source: Some(source),
                kind: Some(kind),
                enabled: true,
                installed_at: Some(now_millis()),
            },
        );
        paths
            .write_registry(&registry)
            .map_err(io_error("write plugins.json"))?;
        plugin_value(&paths, &name)
    })
    .await
}

pub(crate) async fn update_at(paths: &PluginPaths, name: &str) -> Result<Value> {
    let _guard = MUTATION_LOCK.lock().await;
    let mut registry = paths.read_registry();
    let entry = registry.plugins.get(name).cloned().unwrap_or_default();
    let target = paths.plugin_dir(name);
    if !target.is_dir() {
        return Err(Error::NotFound(format!("plugin `{name}` is not installed")));
    }
    let (Some(source), Some(kind)) = (entry.source.clone(), entry.kind) else {
        return Err(Error::InvalidParams(format!(
            "plugin `{name}` was not installed from a source Intent can update"
        )));
    };
    let staged = stage(paths, kind, &source).await?;
    if staged.plugin.manifest.name != name {
        let found = staged.plugin.manifest.name.clone();
        staged.discard();
        return Err(Error::InvalidParams(format!(
            "source now provides plugin `{found}`, not `{name}`"
        )));
    }
    let paths = paths.clone();
    let name = name.to_string();
    blocking(move || {
        let retired = paths
            .plugins_dir()
            .join(format!(".retired-{}", uuid::Uuid::new_v4()));
        std::fs::rename(&target, &retired).map_err(io_error("replace plugin"))?;
        if let Err(e) = std::fs::rename(&staged.dir, &target) {
            let _ = std::fs::rename(&retired, &target);
            staged.discard();
            return Err(io_error("replace plugin")(e));
        }
        let _ = std::fs::remove_dir_all(&retired);
        registry.plugins.insert(
            name.clone(),
            RegistryEntry {
                installed_at: Some(now_millis()),
                ..entry
            },
        );
        paths
            .write_registry(&registry)
            .map_err(io_error("write plugins.json"))?;
        plugin_value(&paths, &name)
    })
    .await
}

pub(crate) async fn uninstall_at(
    paths: &PluginPaths,
    name: &str,
    keep_data: bool,
) -> Result<Value> {
    let _guard = MUTATION_LOCK.lock().await;
    let paths = paths.clone();
    let name = installed_name(&paths, name)?;
    blocking(move || {
        std::fs::remove_dir_all(paths.plugin_dir(&name)).map_err(io_error("remove plugin"))?;
        if !keep_data {
            let data = paths.data_dir(&name);
            if data.exists() {
                std::fs::remove_dir_all(data).map_err(io_error("remove plugin data"))?;
            }
        }
        let mut registry = paths.read_registry();
        if registry.plugins.remove(&name).is_some() {
            paths
                .write_registry(&registry)
                .map_err(io_error("write plugins.json"))?;
        }
        Ok(json!({ "name": name, "removed": true }))
    })
    .await
}

pub(crate) async fn toggle_at(paths: &PluginPaths, name: &str, enabled: bool) -> Result<Value> {
    let _guard = MUTATION_LOCK.lock().await;
    let paths = paths.clone();
    let name = installed_name(&paths, name)?;
    blocking(move || {
        let mut registry = paths.read_registry();
        registry.plugins.entry(name.clone()).or_default().enabled = enabled;
        paths
            .write_registry(&registry)
            .map_err(io_error("write plugins.json"))?;
        plugin_value(&paths, &name)
    })
    .await
}

fn installed_name(paths: &PluginPaths, name: &str) -> Result<String> {
    if !name.starts_with('.') && !name.contains(['/', '\\']) && paths.plugin_dir(name).is_dir() {
        Ok(name.to_string())
    } else {
        Err(Error::NotFound(format!("plugin `{name}` is not installed")))
    }
}

/// Git URLs clone; anything else must be an absolute local directory
/// (a leading `~/` is expanded).
fn classify_source(raw: &str) -> Result<(PluginSourceKind, String)> {
    let source = raw.trim();
    if source.is_empty() {
        return Err(Error::InvalidParams("source is required".into()));
    }
    let git = ["https://", "http://", "ssh://", "git://", "git@"]
        .iter()
        .any(|p| source.starts_with(p));
    if git {
        if source.starts_with('-') || source.chars().any(char::is_whitespace) {
            return Err(Error::InvalidParams("invalid git URL".into()));
        }
        return Ok((PluginSourceKind::Git, source.to_string()));
    }
    let path = match source.strip_prefix("~/") {
        Some(rest) => PluginPaths::home()
            .ok_or_else(|| Error::InvalidParams("home directory is not set".into()))?
            .join(rest),
        None => PathBuf::from(source),
    };
    if !path.is_absolute() {
        return Err(Error::InvalidParams(
            "source must be a git URL or an absolute folder path".into(),
        ));
    }
    if !path.is_dir() {
        return Err(Error::InvalidParams(format!(
            "folder `{}` does not exist",
            path.display()
        )));
    }
    Ok((PluginSourceKind::Path, path.to_string_lossy().into_owned()))
}

struct Staged {
    dir: PathBuf,
    plugin: LoadedPlugin,
}

impl Staged {
    fn discard(&self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn stage(paths: &PluginPaths, kind: PluginSourceKind, source: &str) -> Result<Staged> {
    let plugins_dir = paths.plugins_dir();
    std::fs::create_dir_all(&plugins_dir).map_err(io_error("create plugins folder"))?;
    let dir = plugins_dir.join(format!(".staging-{}", uuid::Uuid::new_v4()));
    let fetched = match kind {
        PluginSourceKind::Git => clone(source, &dir).await,
        PluginSourceKind::Path => {
            let (from, to) = (PathBuf::from(source), dir.clone());
            blocking(move || copy_tree(&from, &to)).await
        }
    };
    if let Err(e) = fetched {
        let _ = std::fs::remove_dir_all(&dir);
        return Err(e);
    }
    let paths = paths.clone();
    let check = dir.clone();
    let loaded = blocking(move || {
        load_plugin(&check, &paths.data_dir("staging"))
            .map_err(|r| Error::InvalidParams(format!("not a valid plugin: {}", r.reason)))
    })
    .await;
    match loaded {
        Ok(plugin) => Ok(Staged { dir, plugin }),
        Err(e) => {
            let _ = std::fs::remove_dir_all(&dir);
            Err(e)
        }
    }
}

async fn clone(url: &str, dir: &Path) -> Result<()> {
    let mut command = tokio::process::Command::new("git");
    command
        .args(["clone", "--depth", "1", "--quiet", "--", url])
        .arg(dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    let output = tokio::time::timeout(CLONE_TIMEOUT, command.output())
        .await
        .map_err(|_| Error::InvalidParams("git clone timed out".into()))?
        .map_err(|e| Error::Internal(format!("cannot run git: {e}")))?;
    if output.status.success() {
        let _ = std::fs::remove_dir_all(dir.join(".git"));
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(Error::InvalidParams(format!(
            "git clone failed: {}",
            stderr.trim()
        )))
    }
}

/// Copy a plugin folder, skipping `.git`. Links are copied as links so the
/// loader's containment checks still apply to them.
fn copy_tree(from: &Path, to: &Path) -> Result<()> {
    let mut remaining = MAX_COPY_ENTRIES;
    copy_dir(from, to, &mut remaining)
}

fn copy_dir(from: &Path, to: &Path, remaining: &mut usize) -> Result<()> {
    std::fs::create_dir_all(to).map_err(io_error("copy plugin"))?;
    for entry in std::fs::read_dir(from).map_err(io_error("read plugin folder"))? {
        let entry = entry.map_err(io_error("read plugin folder"))?;
        *remaining = remaining
            .checked_sub(1)
            .ok_or_else(|| Error::InvalidParams("plugin folder has too many files".into()))?;
        let name = entry.file_name();
        if name == ".git" {
            continue;
        }
        let (src, dst) = (entry.path(), to.join(&name));
        let kind = entry.file_type().map_err(io_error("read plugin folder"))?;
        if kind.is_symlink() {
            copy_link(&src, &dst)?;
        } else if kind.is_dir() {
            copy_dir(&src, &dst, remaining)?;
        } else if kind.is_file() {
            std::fs::copy(&src, &dst).map_err(io_error("copy plugin"))?;
        }
    }
    Ok(())
}

#[cfg(unix)]
fn copy_link(src: &Path, dst: &Path) -> Result<()> {
    let target = std::fs::read_link(src).map_err(io_error("copy plugin"))?;
    std::os::unix::fs::symlink(target, dst).map_err(io_error("copy plugin"))
}

#[cfg(not(unix))]
fn copy_link(_src: &Path, _dst: &Path) -> Result<()> {
    Ok(())
}

fn plugin_value(paths: &PluginPaths, name: &str) -> Result<Value> {
    list_installed(paths)
        .iter()
        .find(|p| installed_dir(p) == name)
        .map(|p| json!({ "plugin": info(p) }))
        .ok_or_else(|| Error::NotFound(format!("plugin `{name}` is not installed")))
}

fn installed_dir(p: &InstalledPlugin) -> &str {
    match p {
        InstalledPlugin::Loaded { plugin, .. } => &plugin.manifest.name,
        InstalledPlugin::Rejected { directory, .. } => directory,
    }
}

/// Wire shape of one installed plugin (`plugin.list` item).
fn info(p: &InstalledPlugin) -> Value {
    match p {
        InstalledPlugin::Loaded { plugin, entry } => {
            let mut value = serde_json::to_value(&plugin.manifest).unwrap_or_else(|_| json!({}));
            let obj = value
                .as_object_mut()
                .expect("manifest serializes to an object");
            obj.insert("enabled".into(), json!(entry.enabled));
            obj.insert("status".into(), json!("ok"));
            obj.insert("path".into(), json!(plugin.root.to_string_lossy()));
            insert_source(obj, entry);
            obj.insert(
                "skills".into(),
                json!(plugin
                    .skills
                    .iter()
                    .map(|s| &s.directory)
                    .collect::<Vec<_>>()),
            );
            obj.insert(
                "mcpServers".into(),
                plugin
                    .mcp_servers
                    .iter()
                    .map(|(server, config)| {
                        json!({
                            "id": format!("{}{}/{server}", super::MCP_ID_PREFIX, plugin.manifest.name),
                            "name": server,
                            "sessionName": mcp_server_name(&plugin.manifest.name, server),
                            "transport": config.transport(),
                        })
                    })
                    .collect(),
            );
            obj.insert("diagnostics".into(), json!(plugin.diagnostics));
            value
        }
        InstalledPlugin::Rejected {
            directory,
            rejection,
            entry,
        } => {
            let mut value = json!({
                "name": directory,
                "enabled": entry.enabled,
                "status": "invalid",
                "error": rejection.reason,
                "path": rejection.root.to_string_lossy(),
                "skills": [],
                "mcpServers": [],
                "diagnostics": [],
            });
            insert_source(value.as_object_mut().expect("object"), entry);
            value
        }
    }
}

fn insert_source(obj: &mut serde_json::Map<String, Value>, entry: &RegistryEntry) {
    if let Some(source) = &entry.source {
        obj.insert("source".into(), json!(source));
    }
    if let Some(kind) = entry.kind {
        obj.insert("sourceKind".into(), json!(kind));
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn io_error(action: &'static str) -> impl Fn(std::io::Error) -> Error {
    move |e| Error::Internal(format!("{action}: {e}"))
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Internal(format!("plugin task failed: {e}")))?
}

#[cfg(test)]
mod tests;

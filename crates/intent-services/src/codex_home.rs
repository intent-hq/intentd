//! Persistent Codex storage for Intent. Existing native sessions stay in their
//! original home until an explicit migration can preserve their resume IDs.

use std::io;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

fn key(value: &str) -> String {
    use std::fmt::Write;
    Sha256::digest(value.as_bytes())
        .iter()
        .fold(String::with_capacity(64), |mut text, byte| {
            let _ = write!(text, "{byte:02x}");
            text
        })
}

/// Match the usual login remedy to the home actually used by this agent.
/// Keychain entries are namespaced by Codex home, so a normal `codex login`
/// would otherwise authenticate the desktop again instead of this session.
pub(crate) fn login_command(root: &Path, agent: &str) -> Option<String> {
    let home = std::fs::read_to_string(root.join("agents").join(key(agent))).ok()?;
    Some(format!(
        "CODEX_HOME='{}' codex login",
        home.replace('\'', "'\\''")
    ))
}

pub(crate) fn user_home() -> io::Result<PathBuf> {
    if let Some(path) = std::env::var_os("CODEX_HOME").filter(|p| !p.is_empty()) {
        return std::path::absolute(PathBuf::from(path));
    }
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(|home| PathBuf::from(home).join(".codex"))
        .ok_or_else(|| io::Error::other("Cannot resolve the user's Codex home"))
}

/// Return the selected home. A durable per-agent marker distinguishes sessions
/// created here from pre-upgrade sessions, including across daemon restarts.
pub(crate) fn prepare(
    root: &Path,
    source: &Path,
    agent: &str,
    has_native_session: bool,
) -> io::Result<PathBuf> {
    private_dir(root)?;
    let root = root.canonicalize()?;
    let marker_dir = root.join("agents");
    private_dir(&marker_dir)?;
    let marker = marker_dir.join(key(agent));
    let selected = match std::fs::read_to_string(&marker) {
        Ok(path) => PathBuf::from(path),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            if has_native_session {
                // Do not silently lose native context by attempting to load an
                // old ID from an empty home. Legacy chats remain visible.
                return Ok(source.to_path_buf());
            }
            root.join("homes").join(key(&source.to_string_lossy()))
        }
        Err(e) => return Err(e),
    };
    if selected.parent() != Some(root.join("homes").as_path())
        || !selected
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
    {
        return Err(io::Error::other("Invalid Intent Codex home marker"));
    }
    private_dir(&root.join("homes"))?;
    private_dir(&selected)?;
    // Share configuration resources, never sessions, indexes or databases.
    // A symlink (not a snapshot) keeps file-auth refreshes and subsequent
    // login replacements visible to both applications. Codex writes auth.json
    // in place; Intent never reads or logs credential bytes.
    for name in [
        "skills",
        "plugins",
        "rules",
        "AGENTS.md",
        "AGENTS.override.md",
        "instructions.md",
        "hooks.json",
        ".credentials.json",
    ] {
        // An absent resource must not become a dangling directory symlink:
        // Codex may create its own system skills in an otherwise empty home.
        if source.join(name).exists() && !selected.join(name).exists() {
            link(&source.join(name), &selected.join(name))?;
        }
    }
    // A first login writes through the link and needs its target parent.
    // Preserve permissions on an existing user-managed home.
    if !source.exists() {
        private_dir(source)?;
    }
    link(&source.join("auth.json"), &selected.join("auth.json"))?;
    write_config(source, &selected)?;
    // Record routing before spawn: retries must select the same storage.
    atomic_write(&marker, selected.to_string_lossy().as_bytes())?;
    Ok(selected)
}

fn write_config(source: &Path, home: &Path) -> io::Result<()> {
    write_config_file(source, home, "config.toml")?;
    // Named profile files are loaded alongside config.toml. Copy them with
    // the same path and storage adjustments rather than losing that layer.
    let entries = match std::fs::read_dir(source) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        if let Some(name) = entry.file_name().to_str() {
            if name.ends_with(".config.toml") && entry.path().is_file() {
                write_config_file(source, home, name)?;
            }
        }
    }
    Ok(())
}

fn write_config_file(source: &Path, home: &Path, name: &str) -> io::Result<()> {
    let text = match std::fs::read_to_string(source.join(name)) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e),
    };
    let mut config = text
        .parse::<toml_edit::DocumentMut>()
        .map_err(|e| io::Error::other(format!("Invalid Codex config: {e}")))?;
    rebase_paths(config.as_table_mut(), source, false);
    // A user-level SQLite override must not reconnect the isolated sessions
    // to the desktop's index. Preserve every unrelated setting.
    config["sqlite_home"] = toml_edit::value(home.to_string_lossy().as_ref());
    if let Some(profiles) = config
        .get_mut("profiles")
        .and_then(|v| v.as_table_like_mut())
    {
        for (_, profile) in profiles.iter_mut() {
            if let Some(table) = profile.as_table_like_mut() {
                table.remove("sqlite_home");
            }
        }
    }
    atomic_write(&home.join(name), config.to_string().as_bytes())
}

/// These paths are relative to the declaring configuration, unlike MCP
/// command arguments. Provider token-helper directories also use this base.
/// Point role config
/// files at their originals so their own relative references stay anchored.
fn rebase_paths(table: &mut dyn toml_edit::TableLike, source: &Path, skills: bool) {
    for (name, item) in table.iter_mut() {
        let name = name.get();
        if name == "model_providers" {
            if let Some(providers) = item.as_table_like_mut() {
                for (_, provider) in providers.iter_mut() {
                    if let Some(auth) = provider
                        .as_table_like_mut()
                        .and_then(|provider| provider.get_mut("auth"))
                        .and_then(|auth| auth.as_table_like_mut())
                    {
                        let cwd = match auth.get("cwd") {
                            None => Some(source.to_path_buf()),
                            Some(value) => value.as_str().and_then(|path| {
                                (Path::new(path).is_relative() && !path.starts_with('~'))
                                    .then(|| source.join(path))
                            }),
                        };
                        if let Some(cwd) = cwd {
                            auth.insert("cwd", toml_edit::value(cwd.to_string_lossy().as_ref()));
                        }
                    }
                }
            }
        }
        if matches!(
            name,
            "config_file"
                | "model_instructions_file"
                | "experimental_compact_prompt_file"
                | "model_catalog_json"
        ) || (skills && name == "path")
        {
            if let Some(path) = item.as_str() {
                if Path::new(path).is_relative() && !path.starts_with('~') {
                    *item = toml_edit::value(source.join(path).to_string_lossy().as_ref());
                }
            }
        }
        let skills = skills || name == "skills";
        if let Some(child) = item.as_table_like_mut() {
            rebase_paths(child, source, skills);
        } else if let Some(array) = item.as_array_of_tables_mut() {
            for child in array.iter_mut() {
                rebase_paths(child, source, skills);
            }
        } else if let Some(array) = item.as_array_mut() {
            for value in array.iter_mut() {
                if let Some(child) = value.as_inline_table_mut() {
                    rebase_paths(child, source, skills);
                }
            }
        }
    }
}

fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write;
    let mut file = tempfile::NamedTempFile::new_in(path.parent().unwrap())?;
    file.write_all(bytes)?;
    file.persist(path).map_err(|e| e.error)?;
    Ok(())
}

fn private_dir(path: &Path) -> io::Result<()> {
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(io::Error::other("Codex storage must be a real directory"));
        }
    }
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn link(source: &Path, target: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(target) {
        Ok(_) if std::fs::read_link(target).ok().as_deref() == Some(source) => return Ok(()),
        Ok(_) => {
            return Err(io::Error::other(format!(
                "Refusing to replace {}",
                target.display()
            )))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    #[cfg(unix)]
    {
        match std::os::unix::fs::symlink(source, target) {
            Ok(()) => Ok(()),
            Err(e)
                if e.kind() == io::ErrorKind::AlreadyExists
                    && std::fs::read_link(target).ok().as_deref() == Some(source) =>
            {
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
    #[cfg(not(unix))]
    Err(io::Error::other(
        "Isolated Codex storage requires filesystem symlink support",
    ))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn first_login_can_write_through_shared_auth_link() {
        let tmp = crate::test_support::test_tempdir("codex-first-login-");
        let source = tmp.path().join("missing/user");
        let home = prepare(&tmp.path().join("intent"), &source, "new-agent", false).unwrap();
        std::fs::write(home.join("auth.json"), "test-credential").unwrap();
        assert_eq!(
            std::fs::read_to_string(source.join("auth.json")).unwrap(),
            "test-credential"
        );
    }

    #[test]
    fn separates_state_and_keeps_file_auth_live_across_restarts() {
        let tmp = crate::test_support::test_tempdir("codex-home-");
        let source = tmp.path().join("user");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(source.join("auth.json"), "initial-test-credential").unwrap();
        std::fs::write(source.join("config.toml"), "model = 'test-model'\nsqlite_home = '/shared'\n[profiles.work]\nsqlite_home = '/also-shared'\n").unwrap();
        let root = tmp.path().join("intent");
        let home = prepare(&root, &source, "agent-1", false).unwrap();
        assert_ne!(home, source);
        assert_eq!(
            login_command(&root, "agent-1"),
            Some(format!("CODEX_HOME='{}' codex login", home.display()))
        );
        std::fs::create_dir(home.join("skills")).unwrap();
        std::fs::create_dir(home.join("sessions")).unwrap();
        std::fs::write(home.join("sessions/session"), "native-context").unwrap();
        assert!(!source.join("sessions").exists());
        std::fs::write(home.join("auth.json"), "refreshed-test-credential").unwrap();
        assert_eq!(
            std::fs::read_to_string(source.join("auth.json")).unwrap(),
            "refreshed-test-credential"
        );
        atomic_write(&source.join("auth.json"), b"replacement-login").unwrap();
        assert_eq!(
            std::fs::read_to_string(home.join("auth.json")).unwrap(),
            "replacement-login"
        );
        assert_eq!(prepare(&root, &source, "agent-1", true).unwrap(), home);
        assert_eq!(
            std::fs::read_to_string(home.join("sessions/session")).unwrap(),
            "native-context"
        );
        let config = std::fs::read_to_string(home.join("config.toml"))
            .unwrap()
            .parse::<toml_edit::DocumentMut>()
            .unwrap();
        assert_eq!(config["model"].as_str(), Some("test-model"));
        assert_eq!(config["sqlite_home"].as_str(), home.to_str());
        assert!(config["profiles"]["work"].get("sqlite_home").is_none());
    }

    #[test]
    fn legacy_sessions_keep_their_original_storage() {
        let tmp = crate::test_support::test_tempdir("codex-legacy-");
        let source = tmp.path().join("user");
        let root = tmp.path().join("intent");
        assert_eq!(prepare(&root, &source, "old-agent", true).unwrap(), source);
        assert!(login_command(&root, "old-agent").is_none());
        assert!(!source.exists());
        assert!(!root.join("agents").join(key("old-agent")).exists());
    }

    #[test]
    fn refuses_to_overwrite_existing_auth_material() {
        let tmp = crate::test_support::test_tempdir("codex-auth-");
        let target = tmp.path().join("auth.json");
        std::fs::write(&target, "keep-me").unwrap();
        assert!(link(&tmp.path().join("source"), &target).is_err());
        assert_eq!(std::fs::read_to_string(target).unwrap(), "keep-me");
    }

    #[test]
    fn preserves_token_helper_working_directories() {
        let tmp = crate::test_support::test_tempdir("codex-token-helper-");
        let source = tmp.path().join("user");
        std::fs::create_dir(&source).unwrap();
        let text = r#"
[model_providers.relative.auth]
command = "./token"
args = ["relative-argument"]
cwd = "auth-helper"
[model_providers.default.auth]
command = "./token"
[model_providers.absolute.auth]
command = "token"
cwd = "/opt/token-helper"
[model_providers.no_auth]
env_key = "TEST_TOKEN"
[profiles.work.model_providers.inline]
auth = {command = "token", cwd = "profile-helper"}
[mcp_servers.example]
cwd = "unchanged"
"#;
        for name in ["config.toml", "work.config.toml"] {
            std::fs::write(source.join(name), text).unwrap();
        }
        let home = prepare(&tmp.path().join("intent"), &source, "agent", false).unwrap();
        for name in ["config.toml", "work.config.toml"] {
            let config = std::fs::read_to_string(home.join(name))
                .unwrap()
                .parse::<toml_edit::DocumentMut>()
                .unwrap();
            let providers = &config["model_providers"];
            assert_eq!(
                providers["relative"]["auth"]["cwd"].as_str(),
                source.join("auth-helper").to_str()
            );
            assert_eq!(
                providers["default"]["auth"]["cwd"].as_str(),
                source.to_str()
            );
            assert_eq!(
                providers["absolute"]["auth"]["cwd"].as_str(),
                Some("/opt/token-helper")
            );
            assert_eq!(
                providers["relative"]["auth"]["command"].as_str(),
                Some("./token")
            );
            assert_eq!(
                providers["relative"]["auth"]["args"][0].as_str(),
                Some("relative-argument")
            );
            assert!(providers["no_auth"].get("auth").is_none());
            assert_eq!(
                config["profiles"]["work"]["model_providers"]["inline"]["auth"]["cwd"].as_str(),
                source.join("profile-helper").to_str()
            );
            assert_eq!(
                config["mcp_servers"]["example"]["cwd"].as_str(),
                Some("unchanged")
            );
            assert_eq!(std::fs::read_to_string(source.join(name)).unwrap(), text);
        }
    }

    #[test]
    fn preserves_relative_config_references_and_named_profiles() {
        let tmp = crate::test_support::test_tempdir("codex-config-");
        let source = tmp.path().join("user");
        std::fs::create_dir(&source).unwrap();
        let text = "model_instructions_file = 'instructions/custom.md'\n[agents.reviewer]\nconfig_file = 'roles/reviewer.toml'\n[skills]\nconfig = [{path = 'custom-skills/review', enabled = true}]\n[mcp_servers.test]\nargs = ['relative-argument']\n";
        std::fs::write(source.join("config.toml"), text).unwrap();
        std::fs::write(
            source.join("work.config.toml"),
            "model_catalog_json = 'catalog.json'\nsqlite_home = '/shared'\n",
        )
        .unwrap();
        let home = prepare(&tmp.path().join("intent"), &source, "agent", false).unwrap();
        let config = std::fs::read_to_string(home.join("config.toml"))
            .unwrap()
            .parse::<toml_edit::DocumentMut>()
            .unwrap();
        assert_eq!(
            config["model_instructions_file"].as_str(),
            source.join("instructions/custom.md").to_str()
        );
        assert_eq!(
            config["agents"]["reviewer"]["config_file"].as_str(),
            source.join("roles/reviewer.toml").to_str()
        );
        assert_eq!(
            config["skills"]["config"][0]["path"].as_str(),
            source.join("custom-skills/review").to_str()
        );
        assert_eq!(
            config["mcp_servers"]["test"]["args"][0].as_str(),
            Some("relative-argument")
        );
        let profile = std::fs::read_to_string(home.join("work.config.toml"))
            .unwrap()
            .parse::<toml_edit::DocumentMut>()
            .unwrap();
        assert_eq!(profile["sqlite_home"].as_str(), home.to_str());
        assert_eq!(
            profile["model_catalog_json"].as_str(),
            source.join("catalog.json").to_str()
        );
        assert_eq!(
            std::fs::read_to_string(source.join("config.toml")).unwrap(),
            text
        );
    }
}

//! Isolated configuration for the official Antigravity ACP server.
//!
//! `GEMINI_HOME` contains configuration and conversation state, not a copy of
//! the user's credentials. Personal OAuth remains in the official macOS
//! Keychain entry. Only an explicit login command may open a browser.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::json;

/// Exact stdout marker emitted by the internal browser guard. No OAuth URL
/// or credential is included in the signal.
pub const AUTH_REQUIRED_MARKER: &str = "INTENT_ACP_AUTH_REQUIRED";

/// Private child-process notification; never forwarded through daemon RPCs.
pub const LOGIN_URL_NOTIFICATION: &str = "_intent/antigravity_login_url";

/// Persistent per-agent state. Unlike generated rules/MCP files, this must
/// survive both child teardown and the daemon's startup configuration sweep.
#[derive(Clone)]
pub(crate) struct SessionProfile {
    home: PathBuf,
    helper: PathBuf,
    tools: Vec<String>,
    denied_tools: Vec<String>,
}

impl SessionProfile {
    pub(crate) fn new(
        root: &Path,
        identity: &str,
        helper: &Path,
        workspace_tools: impl IntoIterator<Item = String>,
        removed: &[&str],
    ) -> io::Result<Self> {
        use sha2::Digest;
        use std::fmt::Write;
        private_dir(root)?;
        let root = root.canonicalize()?;
        // Workspace/agent ids are wire inputs, not trusted path components.
        let digest = sha2::Sha256::digest(identity.as_bytes());
        let name = digest
            .iter()
            .fold(String::with_capacity(64), |mut text, byte| {
                let _ = write!(text, "{byte:02x}");
                text
            });
        let home = root.join(name);
        let mut tools: Vec<String> = workspace_tools.into_iter().collect();
        tools.extend(["view_file".into(), "client_view_file".into()]);
        if !intent_acp::FILE_WRITE_TOOLS
            .iter()
            .any(|tool| removed.contains(tool))
        {
            tools.extend(["client_create_file".into(), "client_edit_file".into()]);
        }
        if !intent_acp::EXECUTION_TOOLS
            .iter()
            .any(|tool| removed.contains(tool))
        {
            tools.push("run_command".into());
        }
        tools.retain(|tool| !removed.contains(&tool.as_str()));
        tools.sort();
        tools.dedup();
        let profile = Self {
            home,
            helper: helper.to_path_buf(),
            tools,
            denied_tools: removed.iter().map(|name| (*name).to_owned()).collect(),
        };
        profile.configure_servers(&[])?;
        Ok(profile)
    }

    pub(crate) fn configure_servers(&self, names: &[String]) -> io::Result<()> {
        // Workspace tools are already allowlisted from the caller-filtered
        // bridge registry. Stale schemas must not expand that role's surface.
        let user_servers: Vec<_> = names
            .iter()
            .filter(|name| name.as_str() != "workspace-mcp")
            .cloned()
            .collect();
        prepare_session_profile(
            &self.home,
            &self.helper,
            &self.tools,
            &self.denied_tools,
            &user_servers,
        )
    }

    pub(crate) fn env(&self) -> io::Result<BTreeMap<String, String>> {
        unattended_env(&self.home, &self.helper)
    }
}

/// Accept only the verified personal Google OAuth authorization endpoint.
/// Never include an invalid URL in an error or persistent diagnostic.
#[must_use]
pub fn valid_login_url(value: &str) -> bool {
    reqwest::Url::parse(value).is_ok_and(|url| {
        url.scheme() == "https"
            && url.host_str() == Some("accounts.google.com")
            && url.username().is_empty()
            && url.password().is_none()
            && url.port_or_known_default() == Some(443)
            && matches!(url.path(), "/o/oauth2/auth" | "/o/oauth2/v2/auth")
            && url.fragment().is_none()
    })
}

/// Run explicit personal OAuth, then verify the login in a fresh guarded
/// process. Only `on_url` receives the authorization URL; callers must write
/// it directly to the invoking terminal, never to tracing or telemetry.
///
/// # Errors
///
/// Returns a safe diagnostic if configuration, authentication, or fresh-session
/// verification fails, times out, or is cancelled.
pub async fn login<F, C>(binary: PathBuf, on_url: F, cancelled: C) -> Result<(), String>
where
    F: FnMut(&str),
    C: std::future::Future<Output = ()>,
{
    let helper = std::env::current_exe().map_err(|_| "Cannot locate the intentd login helper")?;
    tokio::pin!(cancelled);
    login_session(
        &binary,
        &helper,
        on_url,
        cancelled.as_mut(),
        Duration::from_secs(300),
        true,
    )
    .await?;
    login_session(
        &binary,
        &helper,
        |_| {},
        cancelled.as_mut(),
        Duration::from_secs(60),
        false,
    )
    .await
}

pub(crate) async fn login_session<F, C>(
    binary: &Path,
    helper: &Path,
    mut on_url: F,
    cancelled: C,
    budget: Duration,
    interactive: bool,
) -> Result<(), String>
where
    F: FnMut(&str),
    C: std::future::Future<Output = ()>,
{
    use crate::acp_adapter::{initialize_params, reap_child, spawn_adapter, AcpAdapterCommand};
    let profile = probe_profile(helper)
        .map_err(|_| "Cannot create private Antigravity login configuration")?;
    let mut env =
        unattended_env(profile.path(), helper).map_err(|_| "Invalid intentd helper path")?;
    if interactive {
        env.insert(
            "BROWSER".into(),
            format!(
                "{} provider antigravity-login-url %s",
                quote_path(helper).map_err(|_| "Invalid intentd helper path")?
            ),
        );
    }
    let mut cmd = AcpAdapterCommand::binary(binary.to_path_buf(), vec![])
        .cwd(profile.path().to_path_buf())
        .auth_required_marker(AUTH_REQUIRED_MARKER);
    for (key, value) in env {
        cmd = cmd.env(key, value);
    }
    tokio::pin!(cancelled);
    let mut adapter = tokio::select! {
        () = &mut cancelled => return Err("Antigravity login cancelled".into()),
        result = spawn_adapter(&cmd, cmd.setup_timeout()) => result.map_err(|_| "Could not start the official Antigravity ACP server")?,
    };
    let flow = async {
        let initialized = adapter
            .conn
            .request_timeout("initialize", initialize_params(), cmd.initialize_timeout())
            .await
            .map_err(|_| "Antigravity initialization failed")?;
        if !interactive {
            let session = adapter.conn.request_timeout("session/new", json!({
                "cwd": profile.path().to_string_lossy(), "mcpServers": []
            }), cmd.session_new_timeout()).await
                .map_err(|_| "Login completed, but fresh-session authentication failed. Retry on the daemon host with the official macOS Keychain backend.")?;
            return session
                .get("sessionId")
                .and_then(serde_json::Value::as_str)
                .filter(|id| !id.is_empty())
                .map(|_| ())
                .ok_or("Login completed, but the fresh ACP session response was invalid");
        }
        let personal = initialized
            .get("authMethods")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|methods| {
                methods.iter().any(|method| {
                    method.get("id").and_then(serde_json::Value::as_str) == Some("oauth-personal")
                })
            });
        if !personal {
            return Err("This ACP server does not advertise personal Google OAuth");
        }
        let authenticate = adapter.conn.request_timeout(
            "authenticate",
            json!({"methodId": "oauth-personal"}),
            budget,
        );
        tokio::pin!(authenticate);
        let mut notifications_open = true;
        loop {
            tokio::select! {
                result = &mut authenticate => return result.map(|_| ()).map_err(|error| authenticate_error(&error)),
                note = adapter.notifications.recv(), if notifications_open => {
                    match note {
                        Some(note) if note.method == LOGIN_URL_NOTIFICATION => {
                            let url = note.params.get("url").and_then(serde_json::Value::as_str)
                                .filter(|url| valid_login_url(url))
                                .ok_or("The server returned an unsupported sign-in URL")?;
                            on_url(url);
                        }
                        None => notifications_open = false,
                        _ => {}
                    }
                }
            }
        }
    };
    let result = tokio::select! {
        () = &mut cancelled => Err("Antigravity login cancelled"),
        result = tokio::time::timeout(budget, flow) => result.unwrap_or(Err(LOGIN_TIMED_OUT)),
    };
    reap_child(&mut adapter.child).await;
    result.map_err(str::to_string)
}

const LOGIN_TIMED_OUT: &str = "Antigravity login timed out; retry the login command";

/// The `authenticate` request shares the login budget with the outer deadline,
/// so its own timeout can fire first under load; both must read as a timeout.
pub(crate) fn authenticate_error(error: &intent_acp::AcpError) -> &'static str {
    match error {
        intent_acp::AcpError::Timeout(_) => LOGIN_TIMED_OUT,
        _ => "Antigravity authentication failed; retry the login command",
    }
}

/// Create a private probe profile. Its guard denies every tool: discovery
/// only needs initialize and session/new, never a prompt or MCP execution.
pub(crate) fn probe_profile(helper: &Path) -> io::Result<tempfile::TempDir> {
    let profile = tempfile::Builder::new()
        .prefix("intent-antigravity-probe-")
        .tempdir()?;
    prepare_profile(profile.path(), helper, &[])?;
    Ok(profile)
}

/// Initialize Intent-owned configuration. Never load the user's global MCP
/// servers, hooks, or settings. Persistent sessions reuse this directory.
pub(crate) fn prepare_profile(
    home: &Path,
    helper: &Path,
    allowed_tools: &[String],
) -> io::Result<()> {
    prepare_session_profile(home, helper, allowed_tools, &[], &[])
}

pub(crate) fn prepare_session_profile(
    home: &Path,
    helper: &Path,
    allowed_tools: &[String],
    denied_tools: &[String],
    mcp_servers: &[String],
) -> io::Result<()> {
    private_dir(home)?;
    let config = home.join("config");
    let acp = home.join("antigravity-acp");
    private_dir(&config)?;
    private_dir(&acp)?;
    let mut command = format!("{} provider antigravity-tool-guard", quote_path(helper)?);
    for tool in allowed_tools {
        command.push_str(" --allow-tool ");
        command.push_str(&quote_arg(tool));
    }
    for tool in denied_tools {
        command.push_str(" --deny-tool ");
        command.push_str(&quote_arg(tool));
    }
    if !mcp_servers.is_empty() {
        command.push_str(" --gemini-home ");
        command.push_str(&quote_path(home)?);
        for server in mcp_servers {
            command.push_str(" --mcp-server ");
            command.push_str(&quote_arg(server));
        }
    }
    private_json(&config.join("mcp_config.json"), &json!({"mcpServers": {}}))?;
    private_json(
        &config.join("hooks.json"),
        &json!({"intent-provider-policy": {
            "enabled": true,
            "PreToolUse": [{"matcher": "*", "hooks": [{
                "type": "command", "command": command, "timeout": 5
            }]}]
        }}),
    )?;
    private_json(
        &acp.join("settings.json"),
        &json!({"auth": {"type": "oauth-personal"}}),
    )
}

/// Environment for an unattended server. The browser helper deliberately
/// ignores its URL argument and reports authentication-required instead.
pub(crate) fn unattended_env(home: &Path, helper: &Path) -> io::Result<BTreeMap<String, String>> {
    Ok(BTreeMap::from([
        ("GEMINI_HOME".into(), utf8_path(home)?.into()),
        (
            "BROWSER".into(),
            format!(
                "{} provider antigravity-browser-guard %s",
                quote_path(helper)?
            ),
        ),
    ]))
}

/// Evaluate the supported external `PreToolUse` payload. Unknown tools and
/// both native subagent names are denied, even if mistakenly allowlisted.
#[must_use]
pub fn tool_guard(payload: &serde_json::Value, allowed_tools: &[String]) -> serde_json::Value {
    let name = payload
        .pointer("/toolCall/name")
        .and_then(serde_json::Value::as_str);
    let allowed = name.is_some_and(|name| {
        !matches!(name, "start_subagent" | "invoke_subagent")
            && allowed_tools.iter().any(|allowed| allowed == name)
    });
    json!({"allowTool": allowed, "denyReason": if allowed { "" } else {
        "Intent denied this tool. Native subagents and unrecognized tools are not supported."
    }})
}

/// The verified server materializes MCP schemas before its `PreToolUse` hook.
/// Require an exact schema in THIS conversation under an explicitly permitted
/// server. An upstream layout change denies the call instead of widening the
/// native-tool allowlist. These files are metadata, never credential files.
#[must_use]
pub fn mcp_tool_allowed(payload: &serde_json::Value, home: &Path, servers: &[String]) -> bool {
    let Ok(meta) = std::fs::symlink_metadata(home) else {
        return false;
    };
    if !home.is_absolute() || !meta.is_dir() || meta.file_type().is_symlink() {
        return false;
    }
    let Some(name) = payload
        .pointer("/toolCall/name")
        .and_then(serde_json::Value::as_str)
    else {
        return false;
    };
    let Some(conversation) = payload
        .get("conversationId")
        .and_then(serde_json::Value::as_str)
    else {
        return false;
    };
    if !normal_component(name)
        || uuid::Uuid::parse_str(conversation).is_err()
        || matches!(
            name,
            "start_subagent"
                | "invoke_subagent"
                | "view_file"
                | "client_view_file"
                | "client_create_file"
                | "client_edit_file"
                | "run_command"
        )
    {
        return false;
    }
    servers
        .iter()
        .filter(|server| normal_component(server))
        .any(|server| {
            let mut path = home.to_path_buf();
            for component in [
                "antigravity-acp",
                "brain",
                conversation,
                "mcp",
                server.as_str(),
            ] {
                path.push(component);
                let Ok(meta) = std::fs::symlink_metadata(&path) else {
                    return false;
                };
                if !meta.is_dir() || meta.file_type().is_symlink() {
                    return false;
                }
            }
            path.push(format!("{name}.json"));
            let Ok(meta) = std::fs::symlink_metadata(&path) else {
                return false;
            };
            if !meta.is_file() || meta.file_type().is_symlink() || meta.len() > 1024 * 1024 {
                return false;
            }
            let Ok(bytes) = std::fs::read(path) else {
                return false;
            };
            serde_json::from_slice::<serde_json::Value>(&bytes).is_ok_and(|schema| {
                schema.get("name").and_then(serde_json::Value::as_str) == Some(name)
            })
        })
}

fn normal_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && !value.contains(['\\', '\0', '\n', '\r'])
        && matches!(
            (
                Path::new(value).components().next(),
                Path::new(value).components().count()
            ),
            (Some(std::path::Component::Normal(_)), 1)
        )
}

fn utf8_path(path: &Path) -> io::Result<&str> {
    path.to_str()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Antigravity path is not UTF-8"))
}

fn quote_path(path: &Path) -> io::Result<String> {
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Antigravity helper path must be absolute",
        ));
    }
    Ok(quote_arg(utf8_path(path)?))
}

// Official hooks and Python webbrowser parse these arguments with shlex;
// no shell is invoked. Keep spaces, quotes, and percent signs literal.
fn quote_arg(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn private_dir(path: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if !meta.is_dir() || meta.file_type().is_symlink() => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Antigravity profile directory is not a real directory",
            ));
        }
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => std::fs::create_dir(path)?,
        Err(err) => return Err(err),
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn private_json(path: &Path, value: &serde_json::Value) -> io::Result<()> {
    use std::io::Write;
    let mut file = tempfile::NamedTempFile::new_in(path.parent().expect("profile file parent"))?;
    serde_json::to_writer(file.as_file_mut(), value)?;
    file.flush()?;
    file.persist(path).map_err(|err| err.error)?;
    Ok(())
}

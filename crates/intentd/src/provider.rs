//! Explicit provider login and fail-closed helpers for official ACP adapters.

use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub(crate) enum ProviderCommand {
    /// Sign in on the daemon host using personal Google OAuth. Open the
    /// printed HTTPS URL in a browser; select the account and consent there.
    Login {
        #[arg(value_parser = ["antigravity"])]
        provider: String,
        /// Override providers.paths.antigravity for this invocation only.
        #[arg(long)]
        path: Option<PathBuf>,
    },
    #[cfg(unix)]
    #[command(hide = true)]
    CodexAuthOwner {
        #[arg(long)]
        runtime: PathBuf,
        #[arg(long)]
        native_home: PathBuf,
        #[arg(long)]
        user_home: PathBuf,
        #[arg(long)]
        socket: PathBuf,
    },
    #[command(hide = true)]
    CodexAuthBridge {
        #[arg(long)]
        runtime: PathBuf,
        #[arg(long)]
        native_home: PathBuf,
        #[arg(long)]
        user_home: PathBuf,
        #[arg(long)]
        profile: PathBuf,
        #[arg(long)]
        authority_socket: PathBuf,
        #[arg(last = true)]
        args: Vec<String>,
    },
    #[command(hide = true)]
    AntigravityLoginUrl {
        /// Relayed to the explicit login command, never logged or persisted.
        url: String,
    },
    #[command(hide = true)]
    AntigravityBrowserGuard {
        /// Discarded. Never log, open, or persist this URL.
        url: String,
    },
    #[command(hide = true)]
    AntigravityToolGuard {
        #[arg(long)]
        allow_tool: Vec<String>,
        #[arg(long)]
        deny_tool: Vec<String>,
        #[arg(long)]
        gemini_home: Option<PathBuf>,
        #[arg(long)]
        mcp_server: Vec<String>,
    },
}

impl ProviderCommand {
    pub(crate) fn is_internal_helper(&self) -> bool {
        #[cfg(unix)]
        if matches!(self, Self::CodexAuthOwner { .. }) {
            return true;
        }
        matches!(
            self,
            Self::CodexAuthBridge { .. }
                | Self::AntigravityBrowserGuard { .. }
                | Self::AntigravityToolGuard { .. }
                | Self::AntigravityLoginUrl { .. }
        )
    }
}

pub(crate) async fn run(command: ProviderCommand) -> ExitCode {
    let result = match command {
        #[cfg(unix)]
        ProviderCommand::CodexAuthOwner {
            runtime,
            native_home,
            user_home,
            socket,
        } => {
            let result =
                intent_services::codex_auth::run_owner(runtime, native_home, user_home, socket)
                    .await;
            std::process::exit(i32::from(result.is_err()));
        }
        ProviderCommand::CodexAuthBridge {
            runtime,
            native_home,
            user_home,
            profile,
            authority_socket,
            args,
        } => {
            let result = intent_services::codex_auth::run(
                runtime,
                native_home,
                user_home,
                profile,
                authority_socket,
                args,
            )
            .await;
            if let Err(error) = &result {
                eprintln!("{error}");
            }
            // A pending Tokio stdin read must not keep this private helper alive.
            std::process::exit(i32::from(result.is_err()));
        }
        ProviderCommand::Login { provider: _, path } => {
            return match login(path).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("{error}");
                    ExitCode::FAILURE
                }
            };
        }
        ProviderCommand::AntigravityLoginUrl { url } => {
            // Return success even for an invalid URL so webbrowser cannot
            // fall back to opening it. The login client rejects empty params.
            let params = if intent_services::antigravity::valid_login_url(&url) {
                serde_json::json!({"url": url})
            } else {
                serde_json::json!({})
            };
            let notification = serde_json::json!({"jsonrpc":"2.0", "method": intent_services::antigravity::LOGIN_URL_NOTIFICATION, "params": params});
            serde_json::to_writer(std::io::stdout(), &notification)
                .map_err(std::io::Error::other)
                .and_then(|()| writeln!(std::io::stdout()))
                .and_then(|()| std::io::stdout().flush())
        }
        ProviderCommand::AntigravityBrowserGuard { url: _ } => {
            // webbrowser invokes this child before printing its (buffered)
            // login banner. Flush the safe marker immediately to ACP stdout.
            writeln!(
                std::io::stdout(),
                "{}",
                intent_services::antigravity::AUTH_REQUIRED_MARKER
            )
            .and_then(|()| std::io::stdout().flush())
        }
        ProviderCommand::AntigravityToolGuard {
            mut allow_tool,
            deny_tool,
            gemini_home,
            mcp_server,
        } => {
            let mut input = Vec::new();
            let payload = std::io::stdin()
                .take(1024 * 1024)
                .read_to_end(&mut input)
                .ok()
                .and_then(|_| serde_json::from_slice(&input).ok())
                .unwrap_or(serde_json::Value::Null);
            if gemini_home.as_deref().is_some_and(|home| {
                intent_services::antigravity::mcp_tool_allowed(&payload, home, &mcp_server)
            }) {
                if let Some(name) = payload
                    .pointer("/toolCall/name")
                    .and_then(serde_json::Value::as_str)
                {
                    allow_tool.push(name.to_owned());
                }
            }
            allow_tool.retain(|tool| !deny_tool.contains(tool));
            let decision = intent_services::antigravity::tool_guard(&payload, &allow_tool);
            serde_json::to_writer(std::io::stdout(), &decision)
                .map_err(std::io::Error::other)
                .and_then(|()| std::io::stdout().flush())
        }
    };
    if result.is_ok() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

async fn login(path: Option<PathBuf>) -> Result<(), String> {
    let explicit_path = if let Some(path) = path {
        Some(
            path.into_os_string()
                .into_string()
                .map_err(|_| "Provider path must be UTF-8")?,
        )
    } else {
        let config = intent_core::Config::resolve().map_err(|err| err.to_string())?;
        let settings = intent_core::settings_file::SettingsFile::load_or_init(&config.config_path)
            .map_err(|err| err.to_string())?;
        settings.providers.paths.get("antigravity").cloned()
    };
    let binary = intent_providers::find_provider_binary("antigravity", "antigravity-acp", explicit_path.as_deref())
        .ok_or("Official antigravity-acp server not found. Install the server and companion harness together, or configure providers.paths.antigravity.")?;
    eprintln!("Signing in to Antigravity on this daemon host. Open the URL below if requested. Press Ctrl+C to cancel.");
    intent_services::antigravity::login(
        binary,
        |url| {
            // Direct terminal output only. Never route this through tracing.
            eprintln!("Open this Google sign-in URL:\n{url}");
        },
        async {
            let _ = tokio::signal::ctrl_c().await;
        },
    )
    .await?;
    println!("Antigravity sign-in verified in a fresh session.");
    Ok(())
}

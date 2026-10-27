//! `intentd node`: the execution node's composition root. It listens for
//! head's authenticated link (pinned TLS, lease token, generation-fenced
//! binding), runs the agents head launches with its locally installed
//! provider CLIs, and journals their output for head.
//!
//! No head database, settings or service layer is opened here: everything
//! the node knows comes from its private configuration file, its private
//! storage root and the link.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::bail;

use intent_core::node_link::LinkWelcome;
use intent_core::nodes::{NodeArch, NodeOs};
use intent_core::placement::AgentPlatform;
use intent_nodes::journal::Journal;
use intent_transport::node_link::{NodeConnection, NodeListener, NodeListenerConfig, WelcomeFn};

mod config;
mod runner;
mod session;
mod storage;

use config::{Config, ProviderEntry};
use runner::{ProviderLaunch, Runner, RunnerConfig};
use storage::Storage;

#[derive(Debug, clap::Args)]
pub(super) struct Args {
    /// The node's private JSON configuration (mode 600): nodeId, listen
    /// address, lease token, maxAgents, permission and providers.
    #[arg(long)]
    config: PathBuf,
    /// The node's private storage root (created mode 700): link binding, TLS
    /// identity, journal and agent working directories. Never the head's
    /// data directory.
    #[arg(long)]
    root: PathBuf,
}

/// Run the node until SIGINT or SIGTERM.
pub(super) async fn run(args: Args) -> anyhow::Result<()> {
    init_tracing();
    let config = Config::read(&args.config)?;
    let node = Node::start(&config, &args.root).await?;
    println!(
        "{}",
        serde_json::json!({
            "role": "node",
            "nodeId": config.node_id,
            "address": node.listener.local_addr().to_string(),
            "certificateSha256": node.certificate_sha256,
            "maxAgents": config.max_agents,
            "platform": node.platform,
        })
    );
    node.serve(shutdown_signal()).await
}

/// A started node: storage locked, listener bound, runner ready.
struct Node {
    listener: NodeListener,
    runner: Runner,
    certificate_sha256: String,
    platform: AgentPlatform,
    _storage: Arc<Storage>,
}

impl Node {
    async fn start(config: &Config, root: &std::path::Path) -> anyhow::Result<Self> {
        let platform = host_platform()?;
        let storage = Arc::new(Storage::open(root)?);
        let certificate = storage.tls_certificate()?;
        let journal_dir = storage.journal_dir();
        let journal = tokio::task::spawn_blocking(move || Journal::open(&journal_dir)).await??;
        let runner = Runner::new(RunnerConfig {
            max_agents: config.max_agents,
            policy: config.permission.policy(),
            providers: provider_launches(&config.providers),
            storage: storage.clone(),
            journal,
        });
        let welcome: WelcomeFn = {
            let runner = runner.clone();
            Arc::new(move |_hello| LinkWelcome {
                protocol: 0,
                intentd_version: env!("CARGO_PKG_VERSION").to_owned(),
                node_id: String::new(),
                platform,
                max_agents: runner.max_agents(),
                running_agents: runner.running(),
            })
        };
        let listener = NodeListener::bind(
            config.listen,
            NodeListenerConfig {
                node_id: config.node_id.clone(),
                token: config.token.clone(),
                binding_file: storage.binding_file(),
                certificate: certificate.clone(),
            },
            welcome,
        )
        .await
        .map_err(|e| anyhow::anyhow!("start node listener on {}: {e}", config.listen))?;
        Ok(Self {
            listener,
            runner,
            certificate_sha256: certificate.fingerprint256,
            platform,
            _storage: storage,
        })
    }

    /// Accept head's connections until `shutdown` resolves, then stop every
    /// agent.
    async fn serve(
        mut self,
        shutdown: impl std::future::Future<Output = anyhow::Result<()>>,
    ) -> anyhow::Result<()> {
        tokio::pin!(shutdown);
        let mut links = Vec::new();
        let mut sessions = tokio::task::JoinSet::new();
        let result = loop {
            tokio::select! {
                result = &mut shutdown => break result,
                connection = self.listener.accept() => match connection {
                    Some(NodeConnection::Link { link, events, hello }) => {
                        tracing::info!(generation = hello.binding.generation.0, "node: head bound");
                        links.retain(|old: &intent_transport::node_link::NodeLink| !old.is_closed());
                        links.push(link.clone());
                        sessions.spawn(session::serve_link(self.runner.clone(), link, events, hello));
                    }
                    Some(NodeConnection::Git(git)) => {
                        tracing::info!("node: git connections are not served yet; closing");
                        drop(git);
                    }
                    None => break Ok(()),
                },
            }
        };
        drop(self.listener);
        // Stop taking commands first: once its link is closed a session
        // applies none of its queued frames, and joining the sessions
        // settles any frame being applied. Only then stop the agents, so
        // none can be launched behind the shutdown.
        for link in &links {
            link.close();
        }
        while sessions.join_next().await.is_some() {}
        self.runner.shutdown().await;
        result
    }
}

/// The launchable providers: each configured registry entry, with its
/// launch arguments replaced when the operator configured `args`.
fn provider_launches(
    providers: &BTreeMap<String, ProviderEntry>,
) -> BTreeMap<String, ProviderLaunch> {
    providers
        .iter()
        .filter_map(|(id, entry)| {
            let mut config = *intent_providers::find_provider(id)?;
            if let Some(args) = &entry.args {
                // Bounded by the configuration file and done once at start.
                let args: Vec<&'static str> = args
                    .iter()
                    .map(|arg| &*Box::leak(arg.clone().into_boxed_str()))
                    .collect();
                config.base_args = Box::leak(args.into_boxed_slice());
            }
            Some((
                id.clone(),
                ProviderLaunch {
                    config,
                    binary: entry.binary.clone(),
                },
            ))
        })
        .collect()
}

/// The platform this node runs on, as head's placement sees it.
fn host_platform() -> anyhow::Result<AgentPlatform> {
    platform_of(std::env::consts::OS, std::env::consts::ARCH)
}

fn platform_of(os: &str, arch: &str) -> anyhow::Result<AgentPlatform> {
    let os = match os {
        "linux" => NodeOs::Linux,
        "macos" => NodeOs::Macos,
        other => bail!("intentd node runs on Linux and macOS, not {other}"),
    };
    let arch = match arch {
        "x86_64" => NodeArch::X86_64,
        "aarch64" => NodeArch::Aarch64,
        other => bail!("intentd node runs on x86_64 and aarch64, not {other}"),
    };
    Ok(AgentPlatform { os, arch })
}

/// Node logs go to stderr only (the node has no data directory), with the
/// WebSocket library's wire logging removed: it prints the lease token.
fn init_tracing() {
    use tracing_subscriber::{
        fmt, layer::SubscriberExt as _, util::SubscriberInitExt as _, Layer as _,
    };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_writer(std::io::stderr)
                .with_filter(filter)
                .with_filter(tracing_subscriber::filter::filter_fn(|meta| {
                    !intent_transport::node_link::is_wire_trace(meta.target(), meta.level())
                })),
        )
        .try_init();
}

async fn shutdown_signal() -> anyhow::Result<()> {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        _ = term.recv() => {},
    }
    Ok(())
}

#[cfg(test)]
mod tests;

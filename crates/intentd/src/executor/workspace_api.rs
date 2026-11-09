//! A remote agent's `workspace_api` on the node.
//!
//! Each agent gets its own workspace MCP server, reached by its provider
//! through `intentd mcp-bridge` like a local agent's. The agent's code is
//! evaluated here, and each `ws.*` frame is routed by the shared table in
//! `intent_core::remote_launch::workspace_api`: file, `git.commit` and
//! `host.exec` calls run against the agent's own worktree on this node
//! (`file.getAttachment` with the bytes from head), the
//! control-plane and backend-data calls are forwarded to head over the
//! current link (head checks the agent is on the link's lease), and every
//! other namespace fails with an explicit `remote-agent-unsupported:` error.
//! Head also supplies the machine-placement text of the agent's
//! `workspace_api` description and `ws.help("agent")`.
//! Tool outputs too large for the reply are written next to the worktree,
//! on this node.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

use intent_acp::session::McpServer;
use intent_acp::{HostFrameRouter, McpBridge, NormalizedMcpServer, WorkspaceMcpServer};
use intent_core::node_link::{LinkOutcome, McpProxy, PREPARED_CHECKOUT_PROTOCOL, SECRETS_PROTOCOL};
use intent_core::remote_launch::workspace_api::{
    self, Route, ATTACHMENTS_METHOD, ATTACHMENT_DATA_METHOD, CALL_METHOD, COMMIT_POLICY_METHOD,
    GIT_ROOTS_METHOD, MAX_LINK_ATTACHMENT_CHUNK_BYTES, MAX_PULL_BYTES, MCP_PROXY_LINK_TIMEOUT,
    PLACEMENT_HELP_METHOD, PULL_DATA_METHOD,
};
use intent_core::settings_file::AgentFeaturesSettings;
use intent_core::{
    AgentId, BoxFuture, Error, GitAgentCommitResult, NoteId, Result, TurnAttachment, Workspace,
    WorkspaceApi, WorkspaceGitRootId, WorkspaceId,
};
use intent_services::feature_api::file_ops;
use intent_services::secret_requests::{exec_with_secrets, remote_unavailable, HeldSecrets};
use intent_transport::node_link::NodeLink;

/// The link to head the agents' forwarded calls use: the newest bound one,
/// the secrets this backend's agents hold on the machine (nodeProtocol
/// 14), and the backend's GitHub token (nodeProtocol 15).
#[derive(Clone, Default)]
pub(super) struct HeadLink {
    current: Arc<Mutex<Option<NodeLink>>>,
    secrets: Arc<HeldSecrets>,
    forge: Arc<super::forge::ForgeToken>,
}

impl HeadLink {
    /// The backend's GitHub token, memory only (node-link.md § Forge token
    /// on the machine).
    pub(super) fn forge(&self) -> &Arc<super::forge::ForgeToken> {
        &self.forge
    }

    /// The agents' secrets, memory only (node-link.md § Secrets on the
    /// machine).
    pub(super) fn secrets(&self) -> &Arc<HeldSecrets> {
        &self.secrets
    }

    pub(super) fn set(&self, link: NodeLink) {
        *lock(&self.current) = Some(link);
    }

    pub(super) fn get(&self) -> Option<NodeLink> {
        lock(&self.current).clone().filter(|link| !link.is_closed())
    }

    /// The protocol the last link to head negotiated, closed or not; 0
    /// before any link. Records gated on it are written even while the
    /// link is briefly down, and replay to the same backend.
    pub(super) fn last_protocol(&self) -> u32 {
        lock(&self.current).as_ref().map_or(0, NodeLink::protocol)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// What gates an agent's workspace API and which user MCP servers its
/// provider sees, as head sent them in the launch.
pub(super) struct Gates {
    /// The session's `agentFeatures` snapshot (`AgentFeaturesSettings`).
    pub agent_features: Value,
    pub is_sub_agent: bool,
    /// Serve the compact `workspace_api` description, as a local bridge
    /// does for a provider that truncates long tool descriptions (this
    /// machine's provider entry decides). Head then carries the condensed
    /// reference in the agent's instructions.
    pub compact_tool_descriptions: bool,
    /// The user MCP servers head proxies to its hub (nodeProtocol 11).
    pub mcp_proxies: Vec<McpProxy>,
}

/// Push the agent's checkout to head now, one push of it at a time;
/// `true` once head has it. The lock wait and push share one budget that
/// fits inside the calling script's.
pub(super) type PushNow = Arc<dyn Fn() -> BoxFuture<'static, bool> + Send + Sync>;

/// The agent's MCP listeners: its workspace server's and one per proxied
/// user server. They stop when this is dropped.

pub(super) struct Bridges(#[expect(dead_code, reason = "held for its drop")] Vec<McpBridge>);

/// Start `agent_id`'s workspace MCP server and one listener per proxied
/// user MCP server, and return them with the `session/new` server entries
/// that point the provider at them, `workspace-mcp` first. The servers stop
/// when the returned bridges are dropped.
pub(super) async fn serve(
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    worktree: &Path,
    (gates, setup_status): (&Gates, Option<Value>),
    (head, push): (HeadLink, PushNow),
    bridge_exe: &Path,
) -> std::io::Result<(Bridges, Vec<McpServer>)> {
    let server = Arc::new(
        described_server(
            agent_id,
            workspace_id,
            worktree,
            (gates, setup_status),
            (head.clone(), Some(push)),
        )
        .await?,
    );
    let workspace = intent_acp::serve_workspace_mcp_tcp(server).await?;
    let mut entries = vec![bridge_entry(WORKSPACE_SERVER, &workspace, bridge_exe)?];
    let mut bridges = vec![workspace];
    let mut names = vec![WORKSPACE_SERVER];
    let proxy_router = Router {
        agent_id: agent_id.clone(),
        head,
        timeout: MCP_PROXY_LINK_TIMEOUT,
        setup_status: None,
        checkout: None,
        push: None,
    };
    for proxy in &gates.mcp_proxies {
        // Head never names the reserved server; a proxy must not shadow it.
        if names.contains(&proxy.name.as_str()) {
            continue;
        }
        names.push(&proxy.name);
        let router = proxy_router.clone();
        let forward: crate::executor::mcp_proxy::Forward =
            Arc::new(move |method, args| Box::pin(router.forward(method, &args)));
        let bridge = intent_acp::serve_mcp_handler_tcp(
            crate::executor::mcp_proxy::handler(proxy.clone(), forward),
            MCP_PROXY_LINK_TIMEOUT + PROXY_WATCHDOG_MARGIN,
        )
        .await?;
        entries.push(bridge_entry(&proxy.name, &bridge, bridge_exe)?);
        bridges.push(bridge);
    }
    Ok((Bridges(bridges), entries))
}

/// The reserved name of the agent's own workspace server.
const WORKSPACE_SERVER: &str = "workspace-mcp";

/// How much longer than a proxied frame's link budget its listener lets a
/// request run before answering with the watchdog error.
const PROXY_WATCHDOG_MARGIN: Duration = Duration::from_secs(30);

/// The `session/new` entry named `name` that runs `intentd mcp-bridge`
/// against `bridge`, with the listener's secret in its environment and
/// `GH_TOKEN` blanked.
fn bridge_entry(name: &str, bridge: &McpBridge, bridge_exe: &Path) -> std::io::Result<McpServer> {
    let (command, path) = intent_acp::normalize_spaced_bridge_command(
        bridge_exe,
        std::env::var_os("PATH").as_deref(),
    );
    let env = path
        .map(|path| ("PATH".to_owned(), path))
        .into_iter()
        .chain(std::iter::once((
            intent_acp::BRIDGE_TOKEN_ENV.to_owned(),
            bridge.auth_token().to_owned(),
        )))
        // The provider has the backend's GitHub token (nodeProtocol 15); a
        // provider that merges its own environment into an MCP child's must
        // not hand it on.
        .chain(std::iter::once((
            super::runner::GH_TOKEN_ENV.to_owned(),
            String::new(),
        )))
        .collect();
    let servers = std::collections::BTreeMap::from([(
        name.to_owned(),
        NormalizedMcpServer::Stdio {
            command,
            args: vec![
                "mcp-bridge".to_owned(),
                "--connect".to_owned(),
                bridge.connect_addr(),
            ],
            env,
        },
    )]);
    intent_acp::to_acp_session_mcp_servers(&servers)
        .pop()
        .ok_or_else(|| std::io::Error::other(format!("no MCP server entry for {name}")))
}

/// `agent_id`'s workspace MCP server over its worktree and the head link,
/// gated like a local agent's bridge. A feature snapshot the node cannot
/// read starts nothing.
#[cfg(test)]
fn server(
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    worktree: &Path,
    gates: &Gates,
    head: HeadLink,
) -> std::io::Result<WorkspaceMcpServer> {
    server_with_setup(
        agent_id,
        workspace_id,
        worktree,
        (gates, None),
        (head, None),
    )
}

/// [`server`] whose `workspace.details` shows `setup_status`.
fn server_with_setup(
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    worktree: &Path,
    (gates, setup_status): (&Gates, Option<Value>),
    (head, push): (HeadLink, Option<PushNow>),
) -> std::io::Result<WorkspaceMcpServer> {
    let mut features: AgentFeaturesSettings = serde_json::from_value(gates.agent_features.clone())
        .map_err(|e| {
            std::io::Error::other(format!("the agent's feature settings are invalid: {e}"))
        })?;
    // Below nodeProtocol 14 neither side can hold a remote agent's secrets:
    // the agent gets no ws.secret and no secretEnv.
    if head.last_protocol() < SECRETS_PROTOCOL {
        features.secret_requests = false;
    }
    let secrets = head.secrets().clone();
    let router = Router {
        agent_id: agent_id.clone(),
        head,
        timeout: DEFAULT_TIMEOUT,
        setup_status: setup_status.map(Arc::new),
        checkout: Some(Arc::new(worktree.to_path_buf())),
        push: push.clone(),
    };
    let api = Arc::new(NodeApi {
        workspace_id: workspace_id.clone(),
        worktree: worktree.to_path_buf(),
        head: router.clone(),
        agent_id: agent_id.clone(),
        secrets,
        push,
    });
    let server = WorkspaceMcpServer::new(api, workspace_id.clone())
        .with_caller_agent_id(Some(agent_id.clone()))
        .with_agent_features(features)
        .with_sub_agent(gates.is_sub_agent)
        .with_compact_tool_descriptions(gates.compact_tool_descriptions);
    let router = Router {
        timeout: server.workspace_api_timeout(),
        ..router
    };
    Ok(server.with_frame_router(Arc::new(router)))
}

/// [`server`] with head's machine-placement text in its `workspace_api`
/// description, as a local agent's has, so the agent can place sub-agents
/// on other machines. Without an answer (no link, an older backend) the
/// text is left out.
async fn described_server(
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    worktree: &Path,
    gates: (&Gates, Option<Value>),
    (head, push): (HeadLink, Option<PushNow>),
) -> std::io::Result<WorkspaceMcpServer> {
    let help = Router {
        agent_id: agent_id.clone(),
        head: head.clone(),
        timeout: DEFAULT_TIMEOUT,
        setup_status: None,
        checkout: None,
        push: None,
    }
    .placement_help()
    .await;
    Ok(
        server_with_setup(agent_id, workspace_id, worktree, gates, (head, push))?
            .with_remote_placement_help(help),
    )
}

/// The forwarded call whose `setupStatus` the machine replaces.
const DETAILS_METHOD: &str = "workspace.details";

/// Head's `git.listRoots` rows with each relative `path` made absolute in
/// `checkout`, with that repository's branch read there. A relative row
/// whose repository is not checked out on this machine is left out.
/// Absolute paths (an older backend's) and a non-list answer are left as
/// they are.
fn resolve_roots(checkout: &Path, roots: Value) -> Value {
    let Value::Array(rows) = roots else {
        return roots;
    };
    let rows = rows
        .into_iter()
        .filter_map(|mut row| {
            let Some(rel) = row
                .get("path")
                .and_then(Value::as_str)
                .filter(|path| Path::new(path).is_relative())
            else {
                return Some(row);
            };
            let path = checkout.join(rel);
            std::fs::symlink_metadata(path.join(".git")).ok()?;
            let fields = row.as_object_mut()?;
            match intent_git::status::current_branch_at(&path) {
                Some(branch) => fields.insert("branch".to_owned(), Value::String(branch)),
                None => fields.remove("branch"),
            };
            fields.insert(
                "path".to_owned(),
                Value::String(path.to_string_lossy().into_owned()),
            );
            Some(row)
        })
        .collect();
    Value::Array(rows)
}

/// How long the node waits for head to answer its own policy read.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the agent's start and `ws.help("agent")` wait for head's
/// placement text.
const PLACEMENT_HELP_TIMEOUT: Duration = Duration::from_secs(5);

/// Routes one agent's frames: local ones to [`NodeApi`], forwarded ones to
/// head, the rest refused.
#[derive(Clone)]
struct Router {
    agent_id: AgentId,
    head: HeadLink,
    timeout: Duration,
    /// The agent's checkout's setup status, shown as `setupStatus` in
    /// `workspace.details` (nodeProtocol 10) instead of head's worktree's.
    setup_status: Option<Arc<Value>>,
    /// The agent's checkout, where the git roots head lists relative to it
    /// are resolved.
    checkout: Option<Arc<PathBuf>>,
    /// Pushes the agent's checkout before a report reaches its parent.
    push: Option<PushNow>,
}

/// The forwarded call that pushes the agent's checkout first, so a commit
/// it reports is on head when the parent reads the report.
const REPORT_METHOD: &str = "agent.reportToParent";

impl HostFrameRouter for Router {
    fn route(
        &self,
        method: &str,
        args: &Value,
    ) -> Option<BoxFuture<'static, std::result::Result<Value, String>>> {
        match workspace_api::route(method) {
            Route::Local => None,
            Route::Unsupported => {
                let message = workspace_api::unsupported_message(method);
                Some(Box::pin(async move { Err(message) }))
            }
            Route::Forward if method == REPORT_METHOD => {
                let push = self.push.clone();
                let forward = self.forward(method, args);
                Some(Box::pin(async move {
                    if let Some(push) = push {
                        push().await;
                    }
                    forward.await
                }))
            }
            Route::Forward => Some(Box::pin(self.forward(method, args))),
        }
    }

    /// The node has no turn-attachment registry (the agent's transcript is
    /// on head), so a call's cards go to head as one batch.
    fn publishes_attachments(&self) -> bool {
        true
    }

    fn publish_attachments(&self, batch: Vec<TurnAttachment>) -> BoxFuture<'static, ()> {
        let attachments: Vec<Value> = batch
            .iter()
            .map(|card| {
                json!({
                    "id": card.id,
                    "mimeType": card.mime_type,
                    "uri": card.uri,
                    "name": card.name,
                    "text": card.text,
                })
            })
            .collect();
        let sent = self.forward(ATTACHMENTS_METHOD, &json!({ "attachments": attachments }));
        Box::pin(async move {
            // An older backend refuses the frame; the card still reaches the
            // transcript in the tool output.
            if let Err(error) = sent.await {
                tracing::debug!(%error, "head did not take the call's attachments");
            }
        })
    }
}

impl Router {
    /// Forward the frame `method(args)` to head for the agent.
    fn forward(
        &self,
        method: &str,
        args: &Value,
    ) -> impl std::future::Future<Output = std::result::Result<Value, String>> + Send + 'static
    {
        let link = self.head.get();
        let agent_id = self.agent_id.clone();
        let min_protocol = workspace_api::forward_min_protocol(method);
        let refused = workspace_api::forward_refusal(method);
        let params = json!({ "method": method, "args": args });
        let timeout = self.timeout;
        let overlay = self
            .setup_status
            .clone()
            .filter(|_| method == DETAILS_METHOD);
        let roots_in = self.checkout.clone().filter(|_| method == GIT_ROOTS_METHOD);
        async move {
            let link = link.ok_or_else(|| {
                "node-link-unavailable: head is not connected to this node; try again shortly"
                    .to_owned()
            })?;
            if link.protocol() < min_protocol {
                return Err(refused);
            }
            match link.call(agent_id, CALL_METHOD, params, timeout).await {
                Ok(LinkOutcome::Ok(mut value)) => {
                    if let (Some(status), Some(details)) = (
                        overlay.filter(|_| link.protocol() >= PREPARED_CHECKOUT_PROTOCOL),
                        value.as_object_mut(),
                    ) {
                        details.insert("setupStatus".to_owned(), (*status).clone());
                    }
                    if let Some(checkout) = roots_in {
                        value =
                            tokio::task::spawn_blocking(move || resolve_roots(&checkout, value))
                                .await
                                .map_err(|error| format!("git.listRoots: {error}"))?;
                    }
                    Ok(value)
                }
                Ok(LinkOutcome::Err(error)) => Err(error.message),
                Err(error) => Err(format!("the call did not reach head: {error}")),
            }
        }
    }

    /// Head's machine-placement text for the agent, or `None` when head
    /// has none or does not answer within [`PLACEMENT_HELP_TIMEOUT`] (an
    /// older backend refuses the frame).
    async fn placement_help(&self) -> Option<String> {
        let quick = Self {
            timeout: self.timeout.min(PLACEMENT_HELP_TIMEOUT),
            ..self.clone()
        };
        match quick.forward(PLACEMENT_HELP_METHOD, &Value::Null).await {
            Ok(answer) => answer
                .get("help")
                .and_then(Value::as_str)
                .map(str::to_owned),
            Err(error) => {
                tracing::debug!(%error, "head did not answer the placement help");
                None
            }
        }
    }

    /// One answer from head for attachment `attachment_id`, for
    /// `ws.file.getAttachment` on this node: the piece from `offset`, or, from
    /// an older backend that ignores the range, the whole file. An older
    /// backend that refuses the frame tells the agent what to update.
    async fn attachment_piece(&self, attachment_id: &str, offset: u64) -> Result<Value> {
        self.forward(
            ATTACHMENT_DATA_METHOD,
            &json!({
                "attachmentId": attachment_id,
                "offset": offset,
                "length": MAX_LINK_ATTACHMENT_CHUNK_BYTES,
            }),
        )
        .await
        .map_err(|error| {
            if error.starts_with(workspace_api::UNSUPPORTED_CODE) {
                Error::InvalidParams(format!(
                    "{}: ws.file.getAttachment on a remote agent needs a newer Intent on the backend; update it, or continue without this file",
                    workspace_api::UNSUPPORTED_CODE
                ))
            } else {
                Error::InvalidParams(error)
            }
        })
    }

    /// One piece of `path` in `from`'s checkout from head, for
    /// `ws.file.pull` on this node. An older backend that refuses the frame
    /// tells the agent what to update.
    async fn pull_piece(
        &self,
        from: &AgentId,
        path: &str,
        offset: u64,
        transfer_id: Option<String>,
    ) -> Result<intent_core::node_link::CheckoutPiece> {
        let mut args = json!({
            "fromAgentId": from,
            "path": path,
            "offset": offset,
            "length": MAX_LINK_ATTACHMENT_CHUNK_BYTES,
        });
        if let Some(id) = transfer_id {
            args["transferId"] = json!(id);
        }
        let answer = self
            .forward(PULL_DATA_METHOD, &args)
            .await
            .map_err(|error| {
                if error.starts_with(workspace_api::UNSUPPORTED_CODE) {
                    Error::InvalidParams(format!(
                        "{}: ws.file.pull on a remote agent needs a newer Intent on the backend; update it",
                        workspace_api::UNSUPPORTED_CODE
                    ))
                } else {
                    Error::InvalidParams(error)
                }
            })?;
        serde_json::from_value(answer).map_err(|error| {
            Error::InvalidParams(format!(
                "file-pull-mismatch: head sent a malformed piece ({error}); try again"
            ))
        })
    }

    /// Whether the workspace lets agents commit on their own, as head
    /// resolves it. Fails closed: no answer is no.
    async fn auto_commit(&self) -> Result<bool> {
        let answer = self
            .forward(COMMIT_POLICY_METHOD, &Value::Null)
            .await
            .map_err(|error| {
                Error::Internal(format!(
                    "could not check the workspace's auto-commit setting with head ({error}); retry, or pass userRequested: true if the user asked for this commit"
                ))
            })?;
        Ok(answer.get("autoCommit").and_then(Value::as_bool) == Some(true))
    }
}

/// One answer head sent for `ws.file.getAttachment`: the file's metadata
/// and the bytes of this piece (or of the whole file, from an older
/// backend).
struct AttachmentData {
    file_name: String,
    mime_type: Option<String>,
    uploaded_at: String,
    bytes: Vec<u8>,
    /// The whole file's size.
    size: u64,
    /// Where the piece starts; `None` for a whole file.
    offset: Option<u64>,
    sha256: Option<String>,
}

impl AttachmentData {
    fn parse(answer: &Value) -> Result<Self> {
        use base64::Engine as _;
        let text = |key: &str| answer.get(key).and_then(Value::as_str).map(str::to_owned);
        let malformed = || Error::Internal("head sent a malformed attachment".to_owned());
        let bytes = text("data")
            .and_then(|data| base64::engine::general_purpose::STANDARD.decode(data).ok())
            .ok_or_else(malformed)?;
        Ok(Self {
            file_name: text("fileName").ok_or_else(malformed)?,
            mime_type: text("mimeType"),
            uploaded_at: text("uploadedAt").unwrap_or_default(),
            size: answer
                .get("size")
                .and_then(Value::as_u64)
                .unwrap_or(bytes.len() as u64),
            offset: match answer.get("offset") {
                None => None,
                Some(offset) => Some(offset.as_u64().ok_or_else(malformed)?),
            },
            sha256: text("sha256"),
            bytes,
        })
    }
}

/// The node-local half of the API: the agent's worktree is its workspace
/// root. Everything not implemented here keeps the trait's default
/// (unavailable), and the router never sends it here.
struct NodeApi {
    workspace_id: WorkspaceId,
    worktree: PathBuf,
    /// Asks head for the workspace's auto-commit policy.
    head: Router,
    /// The agent this server serves: the only one whose secrets it uses.
    agent_id: AgentId,
    secrets: Arc<HeldSecrets>,
    /// Pushes the checkout after a commit.
    push: Option<PushNow>,
}

impl NodeApi {
    fn root(&self) -> String {
        self.worktree.to_string_lossy().into_owned()
    }

    fn check(&self, workspace_id: &WorkspaceId) -> Result<()> {
        if *workspace_id == self.workspace_id {
            Ok(())
        } else {
            Err(Error::Forbidden(
                "a remote agent can only use its own workspace on the node".into(),
            ))
        }
    }

    /// Run `f` on the worktree on a blocking thread.
    fn blocking<'a>(
        &'a self,
        workspace_id: &WorkspaceId,
        f: impl FnOnce(&str) -> Result<Value> + Send + 'static,
    ) -> BoxFuture<'a, Result<Value>> {
        let checked = self.check(workspace_id);
        let root = self.root();
        Box::pin(async move {
            checked?;
            tokio::task::spawn_blocking(move || f(&root))
                .await
                .map_err(|e| Error::Internal(format!("node file task failed: {e}")))?
        })
    }

    /// Copy a file head sends in pieces, from its `first` piece: ask for
    /// each next piece where the copy ends, then keep the copy only when its
    /// size and sha256 match head's.
    async fn copy_attachment_pieces(
        &self,
        attachment_id: &str,
        dest_dir: Option<String>,
        first: AttachmentData,
    ) -> Result<Value> {
        let malformed = || Error::Internal("head sent a malformed attachment piece".to_owned());
        let AttachmentData {
            file_name,
            mime_type,
            uploaded_at,
            size,
            sha256,
            ..
        } = &first;
        let sha256 = sha256.clone().ok_or_else(malformed)?;
        let (file_name, mime_type, uploaded_at, size) = (
            file_name.clone(),
            mime_type.clone(),
            uploaded_at.clone(),
            *size,
        );
        let root = self.root();
        let name = file_name.clone();
        let mut copy = on_blocking(move || {
            file_ops::AttachmentPieces::begin(&root, dest_dir.as_deref(), &name)
        })
        .await?;
        let mut piece = first;
        loop {
            if piece.offset != Some(copy.written()) || piece.size != size {
                return Err(malformed());
            }
            let done = piece.bytes.is_empty();
            copy = on_blocking(move || copy.append(&piece.bytes).map(|()| copy)).await?;
            if done || copy.written() >= size {
                break;
            }
            piece = AttachmentData::parse(
                &self
                    .head
                    .attachment_piece(attachment_id, copy.written())
                    .await?,
            )?;
        }
        on_blocking(move || {
            copy.finish(
                size,
                &sha256,
                &file_name,
                mime_type.as_deref(),
                &uploaded_at,
            )
        })
        .await
    }
}

/// Run `f` on a blocking thread.
async fn on_blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Internal(format!("node file task failed: {e}")))?
}

impl WorkspaceApi for NodeApi {
    fn agent_placement_help(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Option<String>> {
        Box::pin(async move {
            self.check(&workspace_id).ok()?;
            self.head.placement_help().await
        })
    }

    fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        Box::pin(async move {
            self.check(&id)?;
            let now = intent_core::now_iso();
            Ok(Workspace {
                id,
                title: "Remote agent checkout".into(),
                worktree_path: Some(self.root()),
                skip_worktree: true,
                created_at: now.clone(),
                updated_at: now,
                ..Default::default()
            })
        })
    }

    fn file_read(
        &self,
        workspace_id: WorkspaceId,
        path: String,
        _caller_agent_id: Option<AgentId>,
        git_root_id: Option<WorkspaceGitRootId>,
    ) -> BoxFuture<'_, Result<Value>> {
        if git_root_id.is_some() {
            return Box::pin(async { Err(no_git_roots()) });
        }
        self.blocking(&workspace_id, move |root| file_ops::read(root, &path))
    }

    fn file_write(
        &self,
        workspace_id: WorkspaceId,
        path: String,
        content: String,
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Value>> {
        self.blocking(&workspace_id, move |root| {
            file_ops::write(root, &path, &content)
        })
    }

    fn file_list(
        &self,
        workspace_id: WorkspaceId,
        path: String,
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Value>> {
        self.blocking(&workspace_id, move |root| file_ops::list(root, &path))
    }

    fn file_delete(
        &self,
        workspace_id: WorkspaceId,
        path: String,
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Value>> {
        self.blocking(&workspace_id, move |root| file_ops::delete(root, &path))
    }

    fn file_mkdir(
        &self,
        workspace_id: WorkspaceId,
        path: String,
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Value>> {
        self.blocking(&workspace_id, move |root| file_ops::mkdir(root, &path))
    }

    fn file_rename(
        &self,
        workspace_id: WorkspaceId,
        old_path: String,
        new_path: String,
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Value>> {
        self.blocking(&workspace_id, move |root| {
            file_ops::rename(root, &old_path, &new_path)
        })
    }

    /// The bytes come from head; the copy lands in the agent's checkout,
    /// as a local agent's lands in its worktree.
    fn file_get_attachment(
        &self,
        workspace_id: WorkspaceId,
        attachment_id: String,
        _caller_agent_id: Option<AgentId>,
        dest_dir: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            self.check(&workspace_id)?;
            file_ops::check_attachment_dest(&self.root(), dest_dir.as_deref())?;
            let first =
                AttachmentData::parse(&self.head.attachment_piece(&attachment_id, 0).await?)?;
            file_ops::check_attachment_size(first.size)?;
            if first.offset.is_none() {
                return self
                    .blocking(&workspace_id, move |root| {
                        file_ops::write_attachment_copy(
                            root,
                            dest_dir.as_deref(),
                            &first.file_name,
                            first.mime_type.as_deref(),
                            &first.uploaded_at,
                            &first.bytes,
                        )
                    })
                    .await;
            }
            self.copy_attachment_pieces(&attachment_id, dest_dir, first)
                .await
        })
    }

    /// The pieces come from head (read on the source agent's machine, or in
    /// head's worktree); the copy lands in this agent's checkout.
    fn file_pull(
        &self,
        workspace_id: WorkspaceId,
        _caller_agent_id: Option<AgentId>,
        from_agent_id: AgentId,
        path: String,
        to: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            self.check(&workspace_id)?;
            let to = to.unwrap_or_else(|| path.clone());
            let head = &self.head;
            let (from, path) = (&from_agent_id, path.as_str());
            // The agent's own checkout: a path onto itself is refused.
            let same = (from_agent_id == self.agent_id).then_some(path);
            intent_services::checkout_transfer::pull_into(
                &self.worktree,
                &to,
                same,
                MAX_PULL_BYTES,
                |offset, transfer| head.pull_piece(from, path, offset, transfer),
            )
            .await
        })
    }

    fn host_exec(&self, workspace_id: WorkspaceId, params: Value) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            self.check(&workspace_id)?;
            intent_services::host_exec::run_for_workspace(self, workspace_id, params).await
        })
    }

    /// `ws.host.exec` with `secretEnv` (nodeProtocol 14): the values come
    /// only from this agent's secrets held on the machine.
    fn agent_host_exec(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        params: Value,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            self.check(&workspace_id)?;
            let own = agent_id == self.agent_id;
            let secrets = &self.secrets;
            exec_with_secrets(
                self,
                workspace_id,
                params,
                |names| {
                    if own {
                        secrets.resolve(&agent_id, names)
                    } else {
                        Err(names
                            .first()
                            .map_or_else(String::new, |name| (*name).to_owned()))
                    }
                },
                remote_unavailable,
            )
            .await
        })
    }

    fn git_agent_commit(
        &self,
        workspace_id: WorkspaceId,
        message: String,
        _agent_id: Option<AgentId>,
        _linked_note_id: Option<NoteId>,
        files: Option<Vec<String>>,
        user_requested: bool,
        git_root_id: Option<WorkspaceGitRootId>,
    ) -> BoxFuture<'_, Result<GitAgentCommitResult>> {
        let checked = self.check(&workspace_id);
        let worktree = self.worktree.clone();
        Box::pin(async move {
            checked?;
            if git_root_id.is_some() {
                return Err(no_git_roots());
            }
            // The same gate a local agent's commit passes: the workspace's
            // auto-commit setting (resolved on head), unless the user asked.
            let enabled = user_requested || self.head.auto_commit().await?;
            intent_services::feature_api::git_ops::assert_agent_commit_allowed(
                enabled,
                user_requested,
            )?;
            let mut result =
                tokio::task::spawn_blocking(move || commit(&worktree, &message, files.as_deref()))
                    .await
                    .map_err(|e| Error::Internal(format!("node commit task failed: {e}")))??;
            if let Some(push) = &self.push {
                result.pushed = Some(push().await);
            }
            Ok(result)
        })
    }
}

fn no_git_roots() -> Error {
    Error::InvalidParams(workspace_api::GIT_ROOTS_MESSAGE.to_owned())
}

/// Commit in the agent's worktree: the named `files`, or every change. The
/// caller pushes it to head right after. The caller checked the
/// auto-commit gate. Uses
/// the node's git identity, or a fixed agent
/// identity when the node has none.
fn commit(
    worktree: &Path,
    message: &str,
    files: Option<&[String]>,
) -> Result<GitAgentCommitResult> {
    let git = |args: &[&str]| -> Result<std::process::Output> {
        std::process::Command::new("git")
            .arg("-C")
            .arg(worktree)
            .args(args)
            .output()
            .map_err(|e| Error::Internal(format!("git could not run: {e}")))
    };
    let checked = |args: &[&str]| -> Result<String> {
        let output = git(args)?;
        if !output.status.success() {
            return Err(Error::InvalidParams(format!(
                "git {} failed: {}",
                args.first().copied().unwrap_or_default(),
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    };
    match files {
        Some(files) if !files.is_empty() => {
            let mut args = vec!["add", "--"];
            args.extend(files.iter().map(String::as_str));
            checked(&args)?;
        }
        _ => {
            checked(&["add", "-A"])?;
        }
    }
    if git(&["diff", "--cached", "--quiet"])?.status.success() {
        return Err(Error::InvalidParams("nothing to commit".into()));
    }
    let has_identity = git(&["config", "user.email"])?.status.success();
    let mut args = Vec::new();
    if !has_identity {
        args.extend([
            "-c",
            "user.name=Intent remote agent",
            "-c",
            "user.email=remote-agent@intent.invalid",
        ]);
    }
    args.extend(["commit", "--quiet", "-m", message]);
    checked(&args)?;
    let hash = checked(&["rev-parse", "HEAD"])?;
    let changed = checked(&[
        "diff-tree",
        "--root",
        "--no-commit-id",
        "--name-only",
        "-r",
        "HEAD",
    ])?;
    let files: Vec<String> = changed.lines().map(str::to_owned).collect();
    Ok(GitAgentCommitResult {
        hash,
        file_count: i64::try_from(files.len()).unwrap_or(i64::MAX),
        files,
        pushed: None,
    })
}

#[cfg(test)]
mod tests;

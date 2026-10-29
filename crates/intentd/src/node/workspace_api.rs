//! A remote agent's `workspace_api` on the node.
//!
//! Each agent gets its own workspace MCP server, reached by its provider
//! through `intentd mcp-bridge` like a local agent's. The agent's code is
//! evaluated here, and each `ws.*` frame is routed by the shared table in
//! `intent_core::remote_launch::workspace_api`: file, `git.commit` and
//! `host.exec` calls run against the agent's own worktree on this node, the
//! control-plane namespaces are forwarded to head over the current link
//! (head checks the agent is on the link's lease), and every other
//! namespace fails with an explicit "not supported on remote agents yet".
//! Tool outputs too large for the reply are written next to the worktree,
//! on this node.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

use intent_acp::session::McpServer;
use intent_acp::{HostFrameRouter, McpBridge, NormalizedMcpServer, WorkspaceMcpServer};
use intent_core::node_link::LinkOutcome;
use intent_core::remote_launch::workspace_api::{self, Route, CALL_METHOD};
use intent_core::{
    AgentId, BoxFuture, Error, GitAgentCommitResult, NoteId, Result, Workspace, WorkspaceApi,
    WorkspaceGitRootId, WorkspaceId,
};
use intent_services::feature_api::file_ops;
use intent_transport::node_link::NodeLink;

/// The link to head the agents' forwarded calls use: the newest bound one.
#[derive(Clone, Default)]
pub(super) struct HeadLink {
    current: Arc<Mutex<Option<NodeLink>>>,
}

impl HeadLink {
    pub(super) fn set(&self, link: NodeLink) {
        *lock(&self.current) = Some(link);
    }

    fn get(&self) -> Option<NodeLink> {
        lock(&self.current).clone().filter(|link| !link.is_closed())
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Start `agent_id`'s workspace MCP server and return it with the
/// `session/new` server entry that points the provider at it. The server
/// stops when the returned bridge is dropped.
pub(super) async fn serve(
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    worktree: &Path,
    head: HeadLink,
    bridge_exe: &Path,
) -> std::io::Result<(McpBridge, McpServer)> {
    let server = Arc::new(server(agent_id, workspace_id, worktree, head));
    let bridge = intent_acp::serve_workspace_mcp_tcp(server).await?;
    let (command, path) = intent_acp::normalize_spaced_bridge_command(
        bridge_exe,
        std::env::var_os("PATH").as_deref(),
    );
    let env = path
        .map(|path| ("PATH".to_owned(), path))
        .into_iter()
        .collect();
    let servers = std::collections::BTreeMap::from([(
        "workspace-mcp".to_owned(),
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
    let entry = intent_acp::to_acp_session_mcp_servers(&servers)
        .pop()
        .ok_or_else(|| std::io::Error::other("no workspace MCP server entry"))?;
    Ok((bridge, entry))
}

/// `agent_id`'s workspace MCP server over its worktree and the head link.
fn server(
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    worktree: &Path,
    head: HeadLink,
) -> WorkspaceMcpServer {
    let api = Arc::new(NodeApi {
        workspace_id: workspace_id.clone(),
        worktree: worktree.to_path_buf(),
    });
    let server = WorkspaceMcpServer::new(api, workspace_id.clone())
        .with_caller_agent_id(Some(agent_id.clone()));
    let router = Arc::new(Router {
        agent_id: agent_id.clone(),
        head,
        timeout: server.workspace_api_timeout(),
    });
    server.with_frame_router(router)
}

/// Routes one agent's frames: local ones to [`NodeApi`], forwarded ones to
/// head, the rest refused.
struct Router {
    agent_id: AgentId,
    head: HeadLink,
    timeout: Duration,
}

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
            Route::Forward => {
                let link = self.head.get();
                let agent_id = self.agent_id.clone();
                let params = json!({ "method": method, "args": args });
                let timeout = self.timeout;
                Some(Box::pin(async move {
                    let link = link.ok_or_else(|| {
                        "node-link-unavailable: head is not connected to this node; try again shortly"
                            .to_owned()
                    })?;
                    match link.call(agent_id, CALL_METHOD, params, timeout).await {
                        Ok(LinkOutcome::Ok(value)) => Ok(value),
                        Ok(LinkOutcome::Err(error)) => Err(error.message),
                        Err(error) => Err(format!("the call did not reach head: {error}")),
                    }
                }))
            }
        }
    }
}

/// The node-local half of the API: the agent's worktree is its workspace
/// root. Everything not implemented here keeps the trait's default
/// (unavailable), and the router never sends it here.
struct NodeApi {
    workspace_id: WorkspaceId,
    worktree: PathBuf,
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
}

impl WorkspaceApi for NodeApi {
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

    fn host_exec(&self, workspace_id: WorkspaceId, params: Value) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            self.check(&workspace_id)?;
            intent_services::host_exec::run_for_workspace(self, workspace_id, params).await
        })
    }

    fn git_agent_commit(
        &self,
        workspace_id: WorkspaceId,
        message: String,
        _agent_id: Option<AgentId>,
        _linked_note_id: Option<NoteId>,
        files: Option<Vec<String>>,
        _user_requested: bool,
        git_root_id: Option<WorkspaceGitRootId>,
    ) -> BoxFuture<'_, Result<GitAgentCommitResult>> {
        let checked = self.check(&workspace_id);
        let worktree = self.worktree.clone();
        Box::pin(async move {
            checked?;
            if git_root_id.is_some() {
                return Err(no_git_roots());
            }
            tokio::task::spawn_blocking(move || commit(&worktree, &message, files.as_deref()))
                .await
                .map_err(|e| Error::Internal(format!("node commit task failed: {e}")))?
        })
    }
}

fn no_git_roots() -> Error {
    Error::InvalidParams(format!(
        "{}: secondary git roots are not supported on remote agents yet",
        workspace_api::UNSUPPORTED_CODE
    ))
}

/// Commit in the agent's worktree: the named `files`, or every change. Its
/// committed `HEAD` reaches head as the agent's `head` ref on the next WIP
/// push (after the turn). Uses the node's git identity, or a fixed agent
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
    })
}

#[cfg(test)]
mod tests;

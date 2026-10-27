//! Agent refs exchanged with an execution node over the node link's git
//! connection.
//!
//! Head keeps a remote agent's refs in the workspace's own repository under
//! `refs/intent/ws/<wsId>/agents/<agentId>/{base,head,wip}`. The node names
//! them with the `intent://ws/<wsId>/agents/<agentId>` remote, which git
//! hands to the `git-remote-intent` helper. Head answers each session with
//! `git upload-pack` / `git receive-pack` run with every other ref hidden, so
//! the node can neither read nor update anything outside that agent's refs:
//! git does not advertise hidden refs, refuses wants that were not
//! advertised, and rejects an update of a hidden ref.
//!
//! On the node, a per-repository bare cache receives the base ref and hosts
//! the agent's worktree; [`NodeGit::push_wip`] sends the agent's committed
//! `head` and a `wip` commit of its dirty tree back.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use intent_core::{Error, Result};
use tokio::process::Command;

use crate::hub::{self, AgentRef};

/// The executable name git looks up for `intent://` remotes.
pub const HELPER_NAME: &str = "git-remote-intent";

/// Environment variable naming the node's local git session socket, set on
/// every git command the node runs so the helper can reach it.
pub const SOCKET_ENV: &str = "INTENT_NODE_GIT_SOCKET";

/// Largest pack a node may push in one session.
pub const MAX_RECEIVE_BYTES: u64 = 1 << 30;

const URL_PREFIX: &str = "intent://ws/";

const WIP_NAME: &str = "Intent";
const WIP_EMAIL: &str = "noreply@intent.invalid";

/// One agent's refs as a git remote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentRemote {
    workspace_id: String,
    agent_id: String,
}

fn invalid_remote() -> Error {
    Error::InvalidParams("node-git-invalid-remote".into())
}

impl AgentRemote {
    /// # Errors
    ///
    /// `node-git-invalid-remote` when either id is not a safe ref component.
    pub fn new(workspace_id: &str, agent_id: &str) -> Result<Self> {
        hub::component(workspace_id)
            .and_then(|()| hub::component(agent_id))
            .map_err(|_| invalid_remote())?;
        Ok(Self {
            workspace_id: workspace_id.into(),
            agent_id: agent_id.into(),
        })
    }

    /// Parse the canonical `intent://ws/<wsId>/agents/<agentId>` URL.
    ///
    /// # Errors
    ///
    /// `node-git-invalid-remote` for any other spelling.
    pub fn parse(url: &str) -> Result<Self> {
        let rest = url.strip_prefix(URL_PREFIX).ok_or_else(invalid_remote)?;
        let (workspace, agent) = rest.split_once("/agents/").ok_or_else(invalid_remote)?;
        let remote = Self::new(workspace, agent)?;
        if remote.url() != url {
            return Err(invalid_remote());
        }
        Ok(remote)
    }

    #[must_use]
    pub fn url(&self) -> String {
        format!("{URL_PREFIX}{}/agents/{}", self.workspace_id, self.agent_id)
    }

    #[must_use]
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    #[must_use]
    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    /// `refs/intent/ws/<wsId>/agents/<agentId>/`: the only refs a session
    /// for this remote may read or update.
    #[must_use]
    pub fn ref_prefix(&self) -> String {
        format!(
            "refs/intent/ws/{}/agents/{}/",
            self.workspace_id, self.agent_id
        )
    }

    #[must_use]
    pub fn ref_name(&self, kind: AgentRef) -> String {
        let suffix = match kind {
            AgentRef::Base => "base",
            AgentRef::Head => "head",
            AgentRef::Wip => "wip",
        };
        format!("{}{suffix}", self.ref_prefix())
    }
}

/// The git service a session runs on head.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GitService {
    UploadPack,
    ReceivePack,
}

impl GitService {
    /// The name git's `connect` helper command uses.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UploadPack => "git-upload-pack",
            Self::ReceivePack => "git-receive-pack",
        }
    }

    /// # Errors
    ///
    /// `node-git-invalid-service` for anything but the two pack services.
    pub fn parse(name: &str) -> Result<Self> {
        match name {
            "git-upload-pack" => Ok(Self::UploadPack),
            "git-receive-pack" => Ok(Self::ReceivePack),
            _ => Err(Error::InvalidParams("node-git-invalid-service".into())),
        }
    }

    fn subcommand(self) -> &'static str {
        match self {
            Self::UploadPack => "upload-pack",
            Self::ReceivePack => "receive-pack",
        }
    }
}

/// A git command with no ambient `GIT_*` environment, system or global
/// configuration, hooks or prompts. Removing `GIT_PROTOCOL` keeps head's
/// services on protocol v0, where upload-pack serves only advertised tips.
fn sanitized_git() -> Command {
    let mut command = Command::new("git");
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("GIT_") {
            command.env_remove(key);
        }
    }
    command
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .args(["-c", "core.hooksPath=/dev/null"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
}

/// The `-c` arguments that confine a head service to `remote`'s refs.
/// Command-line configuration is read last, so these override any
/// repository setting.
#[must_use]
pub fn scope_args(remote: &AgentRemote) -> Vec<String> {
    let settings = [
        "transfer.hideRefs=refs/".to_owned(),
        "transfer.hideRefs=HEAD".to_owned(),
        format!("transfer.hideRefs=!{}", remote.ref_prefix()),
        "uploadpack.allowTipSHA1InWant=false".to_owned(),
        "uploadpack.allowReachableSHA1InWant=false".to_owned(),
        "uploadpack.allowAnySHA1InWant=false".to_owned(),
        "receive.fsckObjects=true".to_owned(),
        format!("receive.maxInputSize={MAX_RECEIVE_BYTES}"),
        "receive.autogc=false".to_owned(),
        "receive.advertisePushOptions=false".to_owned(),
        "receive.denyNonFastForwards=false".to_owned(),
        "receive.denyDeletes=false".to_owned(),
    ];
    settings
        .into_iter()
        .flat_map(|setting| ["-c".to_owned(), setting])
        .collect()
}

/// Head side: the `git upload-pack` / `git receive-pack` process answering
/// one node session for `remote` against `repository` (the workspace
/// checkout or its git directory). Stdin and stdout carry the session;
/// stderr is discarded because it is not part of the protocol.
#[must_use]
pub fn service_command(service: GitService, repository: &Path, remote: &AgentRemote) -> Command {
    let mut command = sanitized_git();
    command
        .args(scope_args(remote))
        .arg(service.subcommand())
        .arg(repository)
        .stdin(Stdio::piped())
        .stderr(Stdio::null());
    command
}

/// Point head's `base` ref for `remote` at `commit` in `repository`.
///
/// # Errors
///
/// When the repository can't be opened or the commit doesn't exist.
pub fn write_base_ref(repository: &Path, remote: &AgentRemote, commit: git2::Oid) -> Result<()> {
    let repo = git2::Repository::open(repository).map_err(crate::map_git_err)?;
    repo.find_commit(commit).map_err(crate::map_git_err)?;
    repo.reference(
        &remote.ref_name(AgentRef::Base),
        commit,
        true,
        "intent: remote agent base",
    )
    .map_err(crate::map_git_err)?;
    Ok(())
}

/// Node side: how the node's git commands reach head.
#[derive(Clone, Debug)]
pub struct NodeGit {
    helper_dir: PathBuf,
    socket: PathBuf,
    #[cfg(test)]
    test_head: Option<PathBuf>,
}

/// The refs a WIP push sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WipRefs {
    /// The agent's committed work.
    pub head: String,
    /// `head` plus the dirty tree; equal to `head` when the tree is clean.
    pub wip: String,
}

impl NodeGit {
    /// `helper_dir` holds the `git-remote-intent` executable (see
    /// [`NodeGit::install_helper`]); `socket` is the node's local git
    /// session socket.
    #[must_use]
    pub fn new(helper_dir: PathBuf, socket: PathBuf) -> Self {
        Self {
            helper_dir,
            socket,
            #[cfg(test)]
            test_head: None,
        }
    }

    /// Link `helper_dir/git-remote-intent` to `executable` (intentd, which
    /// runs as the helper when invoked under that name).
    ///
    /// # Errors
    ///
    /// When the directory or link can't be created.
    #[cfg(unix)]
    pub fn install_helper(helper_dir: &Path, executable: &Path) -> Result<()> {
        std::fs::create_dir_all(helper_dir).map_err(io)?;
        let link = helper_dir.join(HELPER_NAME);
        match std::fs::remove_file(&link) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io(error)),
        }
        std::os::unix::fs::symlink(executable, &link).map_err(io)
    }

    fn command(&self, dir: &Path) -> Command {
        let mut command = sanitized_git();
        let existing = std::env::var_os("PATH").unwrap_or_default();
        let path = std::env::join_paths(
            std::iter::once(self.helper_dir.clone()).chain(std::env::split_paths(&existing)),
        )
        .unwrap_or_else(|_| OsString::from(self.helper_dir.as_os_str()));
        command
            .env("PATH", path)
            .env(SOCKET_ENV, &self.socket)
            .args(["-c", "protocol.intent.allow=always"])
            .arg("-C")
            .arg(dir);
        #[cfg(test)]
        if self.test_head.is_some() {
            command.args(["-c", "protocol.ext.allow=always"]);
        }
        command
    }

    #[cfg_attr(
        not(test),
        expect(clippy::unused_self, reason = "tests route the remote through ext::")
    )]
    fn remote_url(&self, remote: &AgentRemote) -> String {
        #[cfg(test)]
        if let Some(head) = &self.test_head {
            return format!(
                "ext::git {} %s {}",
                scope_args(remote).join(" "),
                head.display()
            );
        }
        remote.url()
    }

    /// Commit the worktree's dirty tree on top of its `HEAD` without moving
    /// `HEAD` or touching its index, then push `HEAD` to the agent's `head`
    /// ref and that commit to its `wip` ref, atomically.
    ///
    /// # Errors
    ///
    /// When a git step fails or head rejects the push.
    pub async fn push_wip(&self, worktree: &Path, remote: &AgentRemote) -> Result<WipRefs> {
        let head = self
            .git(worktree, &["rev-parse", "--verify", "HEAD^{commit}"])
            .await?;
        let head_tree = self
            .git(worktree, &["rev-parse", "--verify", "HEAD^{tree}"])
            .await?;
        let scratch = tempfile::tempdir().map_err(io)?;
        let index = scratch.path().join("index");
        let with_index = |args: &[&str]| {
            let mut command = self.command(worktree);
            command.env("GIT_INDEX_FILE", &index).args(args);
            command
        };
        run(with_index(&["read-tree", "HEAD"])).await?;
        run(with_index(&["add", "--all"])).await?;
        let tree = run(with_index(&["write-tree"])).await?;
        let wip = if tree == head_tree {
            head.clone()
        } else {
            let mut commit = self.command(worktree);
            commit
                .env("GIT_AUTHOR_NAME", WIP_NAME)
                .env("GIT_AUTHOR_EMAIL", WIP_EMAIL)
                .env("GIT_COMMITTER_NAME", WIP_NAME)
                .env("GIT_COMMITTER_EMAIL", WIP_EMAIL)
                .args(["commit-tree", &tree, "-p", &head, "-m"])
                .arg(format!("WIP {}", remote.agent_id()));
            run(commit).await?
        };
        let url = self.remote_url(remote);
        let head_spec = format!("+{head}:{}", remote.ref_name(AgentRef::Head));
        let wip_spec = format!("+{wip}:{}", remote.ref_name(AgentRef::Wip));
        self.git(
            worktree,
            &["push", "--atomic", "--quiet", &url, &head_spec, &wip_spec],
        )
        .await?;
        Ok(WipRefs { head, wip })
    }

    async fn git(&self, dir: &Path, args: &[&str]) -> Result<String> {
        let mut command = self.command(dir);
        command.args(args);
        run(command).await
    }
}

/// The node's per-repository bare caches, holding every agent worktree.
#[derive(Debug)]
pub struct CheckoutCache {
    root: PathBuf,
    git: NodeGit,
    lock: tokio::sync::Mutex<()>,
}

impl CheckoutCache {
    #[must_use]
    pub fn new(root: PathBuf, git: NodeGit) -> Self {
        Self {
            root,
            git,
            lock: tokio::sync::Mutex::new(()),
        }
    }

    #[must_use]
    pub fn git(&self) -> &NodeGit {
        &self.git
    }

    fn cache(&self, repo_key: &str) -> Result<PathBuf> {
        hub::component(repo_key)?;
        Ok(self.root.join(format!("{repo_key}.git")))
    }

    /// Fetch `remote`'s base ref from head into the `repo_key` cache
    /// (creating it on first use) and add a detached worktree for it at
    /// `worktree`. Returns the base commit.
    ///
    /// # Errors
    ///
    /// When `repo_key` is not a safe name, head refuses the fetch, or a git
    /// step fails.
    pub async fn checkout(
        &self,
        repo_key: &str,
        remote: &AgentRemote,
        worktree: &Path,
    ) -> Result<String> {
        let cache = self.cache(repo_key)?;
        let _serial = self.lock.lock().await;
        if !cache.join("HEAD").is_file() {
            std::fs::create_dir_all(&self.root).map_err(io)?;
            let mut init = self.git.command(&self.root);
            init.args(["init", "--quiet", "--bare"]).arg(&cache);
            run(init).await?;
        }
        let base = remote.ref_name(AgentRef::Base);
        let url = self.git.remote_url(remote);
        let refspec = format!("+{base}:{base}");
        self.git
            .git(
                &cache,
                &[
                    "fetch",
                    "--quiet",
                    "--no-tags",
                    "--no-write-fetch-head",
                    &url,
                    &refspec,
                ],
            )
            .await?;
        let commit = self
            .git
            .git(
                &cache,
                &["rev-parse", "--verify", &format!("{base}^{{commit}}")],
            )
            .await?;
        let mut add = self.git.command(&cache);
        add.args(["worktree", "add", "--quiet", "--detach"])
            .arg(worktree)
            .arg(&commit);
        run(add).await?;
        Ok(commit)
    }

    /// Remove an agent's worktree from the `repo_key` cache.
    ///
    /// # Errors
    ///
    /// When `repo_key` is not a safe name or git can't remove the worktree.
    pub async fn remove(&self, repo_key: &str, worktree: &Path) -> Result<()> {
        let cache = self.cache(repo_key)?;
        let _serial = self.lock.lock().await;
        let mut remove = self.git.command(&cache);
        remove.args(["worktree", "remove", "--force"]).arg(worktree);
        run(remove).await?;
        Ok(())
    }
}

// By value for direct use with `map_err`.
#[expect(clippy::needless_pass_by_value)]
fn io(error: std::io::Error) -> Error {
    Error::Internal(format!("node git I/O: {error}"))
}

/// Run a git command to completion, returning its trimmed stdout.
async fn run(mut command: Command) -> Result<String> {
    let output = command.output().await.map_err(io)?;
    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned());
    }
    // The end of git's stderr names the failure; keep a bounded tail.
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = stderr.trim();
    let skip = stderr.chars().count().saturating_sub(600);
    let detail: String = stderr.chars().skip(skip).collect();
    Err(Error::Internal(format!("node-git-failed: {detail}")))
}

#[cfg(test)]
mod tests;

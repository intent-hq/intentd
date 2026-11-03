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

const UNAPPLIED: &str = "unapplied/";

/// Head's `update` hook for node pushes. Inside the agent's prefix (the
/// only refs a session can reach) it allows exactly `head`, `wip` and
/// `unapplied/<full lowercase hex sha>`. An `unapplied/` ref is create-only
/// and named by the commit it holds; pinning the same commit again is a
/// no-op success, so a pin whose answer was lost can be retried.
const UPDATE_HOOK: &str = r#"#!/bin/sh
ref="$1"
old="$2"
new="$3"
refuse() {
  echo "intent: $ref: $1" >&2
  exit 1
}
case "$ref" in
refs/intent/ws/*/agents/*/*) ;;
*) refuse "not an agent ref" ;;
esac
name="${ref#refs/intent/ws/*/agents/*/}"
case "$name" in
head|wip) exit 0 ;;
unapplied/*)
  sha="${name#unapplied/}"
  case "$sha" in
  ''|*[!0-9a-f]*) refuse "not a full lowercase commit id" ;;
  esac
  if [ "${#sha}" != 40 ] && [ "${#sha}" != 64 ]; then
    refuse "not a full lowercase commit id"
  fi
  [ "$sha" = "$new" ] || refuse "must be named by its commit"
  if [ "$old" != "$new" ]; then
    case "$old" in
    *[!0]*) refuse "is create-only" ;;
    esac
  fi
  exit 0
  ;;
*) refuse "not a ref a node may push" ;;
esac
"#;

/// Where head keeps its receive hooks (see [`configure_receive_hooks`]).
static HOOKS_DIR: std::sync::RwLock<Option<PathBuf>> = std::sync::RwLock::new(None);

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

    /// `<prefix>unapplied/<commit>`: where a node pins a saved `wip` it
    /// could not restore. Head lets a session create one only under the
    /// name of the commit it holds, and never update or delete it; pinning
    /// the commit it already holds succeeds.
    #[must_use]
    pub fn unapplied_ref(&self, commit: &str) -> String {
        format!("{}{UNAPPLIED}{commit}", self.ref_prefix())
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
///
/// # Errors
///
/// When head's receive hooks cannot be installed: a push is then refused
/// rather than run without them.
pub fn service_command(
    service: GitService,
    repository: &Path,
    remote: &AgentRemote,
) -> Result<Command> {
    let mut command = sanitized_git();
    command.args(scope_args(remote));
    if service == GitService::ReceivePack {
        command.args(receive_hook_args()?);
    }
    command
        .arg(service.subcommand())
        .arg(repository)
        .stdin(Stdio::piped())
        .stderr(Stdio::null());
    Ok(command)
}

/// Head side, at daemon start: keep the receive hooks in
/// `<data_dir>/node-git-hooks`, removing whatever an earlier run left there
/// and installing them fresh. Until this runs, every node push is refused.
///
/// # Errors
///
/// When the directory can't be cleared or the hook can't be written; node
/// pushes are then refused.
pub fn configure_receive_hooks(data_dir: &Path) -> Result<()> {
    let dir = data_dir.join("node-git-hooks");
    match std::fs::remove_dir_all(&dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(io(e)),
    }
    *HOOKS_DIR
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(dir.clone());
    ensure_hooks(&dir)
}

/// The `-c` arguments that run head's [`UPDATE_HOOK`] (and no repository
/// hook) on a node push, after checking the installed hook is intact and
/// reinstalling it if not. Fails closed: no verified hook, no push.
fn receive_hook_args() -> Result<Vec<String>> {
    let dir = HOOKS_DIR
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .ok_or_else(|| {
            Error::Internal("node git: head's receive hooks are not configured".into())
        })?;
    ensure_hooks(&dir)?;
    Ok(vec![
        "-c".to_owned(),
        format!("core.hooksPath={}", dir.display()),
    ])
}

/// Whether `dir/update` is a regular file with exactly [`UPDATE_HOOK`] and
/// mode 0700, in a real (not symlinked) directory.
fn hooks_intact(dir: &Path) -> bool {
    let Ok(dir_meta) = std::fs::symlink_metadata(dir) else {
        return false;
    };
    if !dir_meta.is_dir() {
        return false;
    }
    let hook = dir.join("update");
    let Ok(meta) = std::fs::symlink_metadata(&hook) else {
        return false;
    };
    if !meta.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if meta.permissions().mode() & 0o7777 != 0o700 {
            return false;
        }
    }
    std::fs::read(&hook).is_ok_and(|content| content == UPDATE_HOOK.as_bytes())
}

/// Verify the hook, reinstalling it (written aside, then renamed into
/// place) when it is missing or altered.
fn ensure_hooks(dir: &Path) -> Result<()> {
    if hooks_intact(dir) {
        return Ok(());
    }
    let unavailable =
        |e: std::io::Error| Error::Internal(format!("node git: head's receive hooks: {e}"));
    if std::fs::symlink_metadata(dir).is_ok_and(|meta| !meta.is_dir()) {
        std::fs::remove_file(dir).map_err(unavailable)?;
    }
    std::fs::create_dir_all(dir).map_err(unavailable)?;
    let staged = tempfile::NamedTempFile::new_in(dir).map_err(unavailable)?;
    std::fs::write(staged.path(), UPDATE_HOOK).map_err(unavailable)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(staged.path(), std::fs::Permissions::from_mode(0o700))
            .map_err(unavailable)?;
    }
    let hook = dir.join("update");
    if std::fs::symlink_metadata(&hook).is_ok_and(|meta| meta.is_dir()) {
        std::fs::remove_dir_all(&hook).map_err(unavailable)?;
    }
    staged.persist(&hook).map_err(|e| unavailable(e.error))?;
    if hooks_intact(dir) {
        Ok(())
    } else {
        Err(Error::Internal(
            "node git: head's receive hook could not be verified".into(),
        ))
    }
}

/// The commit `repository`'s `HEAD` points at: where a remote agent of the
/// workspace starts.
///
/// # Errors
///
/// When the repository can't be opened or has no commit yet.
pub fn head_commit(repository: &Path) -> Result<String> {
    let repo = git2::Repository::open(repository).map_err(crate::map_git_err)?;
    let commit = repo
        .head()
        .and_then(|head| head.peel_to_commit())
        .map_err(|_| Error::InvalidParams("the workspace repository has no commit yet".into()))?;
    Ok(commit.id().to_string())
}

/// The node's checkout-cache name for `repository`: stable for every
/// worktree of one repository (it hashes the common git directory), and
/// revealing nothing about head's paths.
///
/// # Errors
///
/// When the repository can't be opened.
pub fn repo_key(repository: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;
    let repo = git2::Repository::open(repository).map_err(crate::map_git_err)?;
    let common = repo
        .commondir()
        .canonicalize()
        .map_err(|e| Error::Internal(format!("node git: resolve the repository: {e}")))?;
    let digest = Sha256::digest(common.as_os_str().as_encoded_bytes());
    let mut key = String::from("repo-");
    for byte in &digest[..16] {
        write!(key, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(key)
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
                "ext::git {} {} %s {}",
                scope_args(remote).join(" "),
                receive_hook_args().unwrap_or_default().join(" "),
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

/// A worktree [`CheckoutCache::checkout`] added.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkout {
    /// The agent's `base` ref: the commit head launched it at.
    pub base: String,
    /// The commit the worktree is at: the agent's saved `head`, else `base`.
    pub at: String,
    /// The agent's `wip` commit when it was ahead of `at` but could not be
    /// restored on top. It is pinned on head under
    /// [`AgentRemote::unapplied_ref`] and readable from the worktree.
    pub unrestored_wip: Option<String>,
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

    /// Fetch `remote`'s refs from head into the `repo_key` cache (creating
    /// it on first use) and add a detached worktree at `worktree`.
    ///
    /// The worktree is at the agent's saved `head` ref when head has one (a
    /// relaunched agent whose checkout was removed), else at its `base`. A
    /// `wip` ref ahead of that commit is restored on top as uncommitted
    /// changes; when it does not apply cleanly the worktree stays at the
    /// saved head, the wip is pinned on head under
    /// [`AgentRemote::unapplied_ref`] (create-only) before this returns, and
    /// [`Checkout::unrestored_wip`] names it. Head's `base`, `head` and `wip`
    /// refs are only read.
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
    ) -> Result<Checkout> {
        let cache = self.cache(repo_key)?;
        let _serial = self.lock.lock().await;
        if !cache.join("HEAD").is_file() {
            std::fs::create_dir_all(&self.root).map_err(io)?;
            let mut init = self.git.command(&self.root);
            init.args(["init", "--quiet", "--bare"]).arg(&cache);
            run(init).await?;
        }
        // Head advertises only this agent's refs; the glob takes whichever
        // of base, head and wip exist, and prunes local copies it dropped.
        let prefix = remote.ref_prefix();
        let url = self.git.remote_url(remote);
        let refspec = format!("+{prefix}*:{prefix}*");
        self.git
            .git(
                &cache,
                &[
                    "fetch",
                    "--quiet",
                    "--no-tags",
                    "--prune",
                    "--no-write-fetch-head",
                    &url,
                    &refspec,
                ],
            )
            .await?;
        let resolve = |kind: AgentRef| {
            let name = format!("{}^{{commit}}", remote.ref_name(kind));
            let mut command = self.git.command(&cache);
            command.args(["rev-parse", "--verify", "--quiet", &name]);
            async move { run(command).await.ok() }
        };
        let base = resolve(AgentRef::Base).await.ok_or_else(|| {
            Error::Internal("node-git-failed: head has no base ref for the agent".into())
        })?;
        let saved_head = resolve(AgentRef::Head).await;
        let wip = resolve(AgentRef::Wip).await;
        let at = saved_head.unwrap_or_else(|| base.clone());
        let mut add = self.git.command(&cache);
        add.args(["worktree", "add", "--quiet", "--detach"])
            .arg(worktree)
            .arg(&at);
        run(add).await?;
        let unrestored_wip = match wip {
            Some(wip) if self.wip_is_ahead(&cache, &at, &wip).await => {
                match self.restore_wip(worktree, &wip).await {
                    Ok(()) => None,
                    Err(error) => {
                        tracing::warn!(error = %error, "node git: the wip ref did not apply");
                        // Pinned before the agent runs, so its next push to
                        // wip cannot lose this work. A pin that already
                        // landed is up to date, which git reports as success.
                        let pin = format!("{wip}:{}", remote.unapplied_ref(&wip));
                        if let Err(push_error) =
                            self.git.git(&cache, &["push", "--quiet", &url, &pin]).await
                        {
                            // No checkout is left behind: the next launch
                            // restores (and pins) again instead of reusing it.
                            let mut cleanup = self.git.command(&cache);
                            cleanup
                                .args(["worktree", "remove", "--force"])
                                .arg(worktree);
                            if let Err(error) = run(cleanup).await {
                                tracing::warn!(error = %error, "node git: could not remove the unpinned checkout");
                            }
                            return Err(Error::Internal(format!(
                                "pin the unapplied wip {wip} on head: {push_error}"
                            )));
                        }
                        // The same ref locally, so the checkout can name it.
                        self.git
                            .git(&cache, &["update-ref", &remote.unapplied_ref(&wip), &wip])
                            .await?;
                        Some(wip)
                    }
                }
            }
            _ => None,
        };
        Ok(Checkout {
            base,
            at,
            unrestored_wip,
        })
    }

    /// Whether `wip` holds work `at` lacks: it differs and is not already
    /// part of `at`'s history.
    async fn wip_is_ahead(&self, cache: &Path, at: &str, wip: &str) -> bool {
        if wip == at {
            return false;
        }
        let mut ancestor = self.git.command(cache);
        ancestor.args(["merge-base", "--is-ancestor", wip, at]);
        run(ancestor).await.is_err()
    }

    /// Apply `wip`'s own changes (against its parent) to the clean worktree
    /// as uncommitted changes. `git apply` without `--index` touches only
    /// the working tree and applies all or nothing.
    async fn restore_wip(&self, worktree: &Path, wip: &str) -> Result<()> {
        let scratch = tempfile::tempdir().map_err(io)?;
        let patch = scratch.path().join("wip.patch");
        let mut diff = self.git.command(worktree);
        diff.args(["diff", "--binary", "--full-index", "--no-color"])
            .arg(format!("--output={}", patch.display()))
            .arg(format!("{wip}^"))
            .arg(wip);
        run(diff).await?;
        if std::fs::metadata(&patch).map_err(io)?.len() == 0 {
            return Ok(());
        }
        let mut apply = self.git.command(worktree);
        apply.args(["apply", "--whitespace=nowarn"]).arg(&patch);
        run(apply).await?;
        Ok(())
    }

    /// Forget the worktrees whose directories are gone, in every cache.
    ///
    /// # Errors
    ///
    /// When the caches cannot be listed or git fails.
    pub async fn prune(&self) -> Result<()> {
        let _serial = self.lock.lock().await;
        let entries = match std::fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(io(e)),
        };
        for entry in entries {
            let cache = entry.map_err(io)?.path();
            if cache.extension().is_some_and(|ext| ext == "git") && cache.join("HEAD").is_file() {
                let mut prune = self.git.command(&cache);
                prune.args(["worktree", "prune"]);
                run(prune).await?;
            }
        }
        Ok(())
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

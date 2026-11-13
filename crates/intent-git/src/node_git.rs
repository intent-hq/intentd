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

use intent_core::node_link::SubmodulePin;
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

/// Suffix of the directory beside a repository's cache that holds its
/// submodules' caches: `<repoKey>.modules/<moduleKey>.git`.
const MODULE_CACHES: &str = ".modules";

/// Head's `update` hook for node pushes. Inside the session's own prefix
/// ([`PREFIX_ENV`], the agent's or the home's refs; the only refs a session
/// can reach) it allows exactly `head`, `wip` and
/// `unapplied/<full lowercase hex sha>`; a home's `rewritten/*` and
/// `orphaned/*` are never a node push's. An `unapplied/` ref is create-only
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
prefix="$INTENT_NODE_GIT_PREFIX"
case "$prefix" in
refs/intent/ws/*/) ;;
*) refuse "no session prefix" ;;
esac
case "$ref" in
"$prefix"*) name="${ref#"$prefix"}" ;;
*) refuse "not this session's ref" ;;
esac
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

/// The environment variable naming a receive session's ref prefix
/// ([`AgentRemote::ref_prefix`]) to [`UPDATE_HOOK`].
pub const PREFIX_ENV: &str = "INTENT_NODE_GIT_PREFIX";

/// Where head keeps its receive hooks (see [`configure_receive_hooks`]).
static HOOKS_DIR: std::sync::RwLock<Option<PathBuf>> = std::sync::RwLock::new(None);

const WIP_NAME: &str = "Intent";
const WIP_EMAIL: &str = "noreply@intent.invalid";

/// One agent's refs as a git remote: in the workspace's repository, or
/// with a module key in one of its submodules' repositories.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentRemote {
    workspace_id: String,
    agent_id: String,
    module_key: Option<String>,
    forge: bool,
    /// A workspace home's checkout (nodeProtocol 19): `agent_id` holds the
    /// `homeId`, and the refs are the workspace's `home/*`.
    home: bool,
}

const MODULES: &str = "/modules/";

/// The path segment of a home's remote: `intent://ws/<wsId>/homes/<homeId>`.
const HOMES: &str = "/homes/";

/// Suffix of a sub-agent's forge remote (nodeProtocol 17).
const FORGE: &str = "/forge";

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
            module_key: None,
            forge: false,
            home: false,
        })
    }

    /// The home checkout `home_id` of the workspace (nodeProtocol 19). Its
    /// refs are `refs/intent/ws/<wsId>/home/{head,wip}`; it has no `base`
    /// of its own, so [`AgentRef::Base`] names `home/head` too.
    ///
    /// # Errors
    ///
    /// `node-git-invalid-remote` when either id is not a safe ref component.
    pub fn home(workspace_id: &str, home_id: &str) -> Result<Self> {
        Ok(Self {
            home: true,
            ..Self::new(workspace_id, home_id)?
        })
    }

    /// Whether this remote names a home checkout.
    #[must_use]
    pub fn is_home(&self) -> bool {
        self.home
    }

    /// The same agent's forge remote (nodeProtocol 17): the workspace's
    /// GitHub repository, fetched through head.
    #[must_use]
    pub fn forge(&self) -> Self {
        Self {
            module_key: None,
            forge: true,
            home: false,
            ..self.clone()
        }
    }

    /// Whether this is a forge remote.
    #[must_use]
    pub fn is_forge(&self) -> bool {
        self.forge
    }

    /// The same agent's remote in the submodule repository `module_key`
    /// names.
    ///
    /// # Errors
    ///
    /// `node-git-invalid-remote` when the key is not a safe ref component.
    pub fn with_module(&self, module_key: &str) -> Result<Self> {
        hub::component(module_key).map_err(|_| invalid_remote())?;
        Ok(Self {
            module_key: Some(module_key.into()),
            forge: false,
            ..self.clone()
        })
    }

    /// Parse the canonical `intent://ws/<wsId>/agents/<agentId>` URL,
    /// `…/modules/<moduleKey>` for a submodule, or `…/forge` for the forge
    /// remote.
    ///
    /// # Errors
    ///
    /// `node-git-invalid-remote` for any other spelling.
    pub fn parse(url: &str) -> Result<Self> {
        let rest = url.strip_prefix(URL_PREFIX).ok_or_else(invalid_remote)?;
        if let Some((workspace, home)) = rest.split_once(HOMES) {
            let remote = match home.split_once(MODULES) {
                Some((home, module_key)) => Self::home(workspace, home)?.with_module(module_key)?,
                None => Self::home(workspace, home)?,
            };
            if remote.url() != url {
                return Err(invalid_remote());
            }
            return Ok(remote);
        }
        let (workspace, agent) = rest.split_once("/agents/").ok_or_else(invalid_remote)?;
        let remote = match agent.split_once(MODULES) {
            Some((agent, module_key)) => Self::new(workspace, agent)?.with_module(module_key)?,
            None => match agent.strip_suffix(FORGE) {
                Some(agent) => Self::new(workspace, agent)?.forge(),
                None => Self::new(workspace, agent)?,
            },
        };
        if remote.url() != url {
            return Err(invalid_remote());
        }
        Ok(remote)
    }

    #[must_use]
    pub fn url(&self) -> String {
        let owner = if self.home { HOMES } else { "/agents/" };
        let url = format!("{URL_PREFIX}{}{owner}{}", self.workspace_id, self.agent_id);
        match &self.module_key {
            Some(key) => format!("{url}{MODULES}{key}"),
            None if self.forge => format!("{url}{FORGE}"),
            None => url,
        }
    }

    /// The submodule repository this remote names; `None` for the
    /// workspace's own.
    #[must_use]
    pub fn module_key(&self) -> Option<&str> {
        self.module_key.as_deref()
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
        if self.home {
            return format!("refs/intent/ws/{}/home/", self.workspace_id);
        }
        format!(
            "refs/intent/ws/{}/agents/{}/",
            self.workspace_id, self.agent_id
        )
    }

    #[must_use]
    pub fn ref_name(&self, kind: AgentRef) -> String {
        let suffix = match kind {
            AgentRef::Base if self.home => "head",
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
    let mut settings = vec![
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
    // A home reads every agent's work of its workspace (so the user can
    // merge it there) and writes none of it.
    if remote.is_home() && remote.module_key().is_none() {
        settings.push(format!(
            "uploadpack.hideRefs=!refs/intent/ws/{}/agents/",
            remote.workspace_id()
        ));
    }
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
        command.env(PREFIX_ENV, remote.ref_prefix());
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
    let repo = git2::Repository::open(repository).map_err(crate::map_git_err)?;
    key_of(&repo)
}

fn key_of(repo: &git2::Repository) -> Result<String> {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;
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
    write_agent_ref(repository, remote, AgentRef::Base, commit)
}

/// Point head's `kind` ref for `remote` at `commit` in `repository`: a
/// sub-agent that starts from its parent's work gets the parent's `head`
/// as its `base` and the parent's `wip` as its own (nodeProtocol 13).
///
/// # Errors
///
/// When the repository can't be opened or the commit doesn't exist.
pub fn write_agent_ref(
    repository: &Path,
    remote: &AgentRemote,
    kind: AgentRef,
    commit: git2::Oid,
) -> Result<()> {
    let repo = git2::Repository::open(repository).map_err(crate::map_git_err)?;
    repo.find_commit(commit).map_err(crate::map_git_err)?;
    repo.reference(
        &remote.ref_name(kind),
        commit,
        true,
        "intent: remote agent ref",
    )
    .map_err(crate::map_git_err)?;
    Ok(())
}

/// The commit head's `kind` ref for `remote` names in `repository`, if it
/// has one.
///
/// # Errors
///
/// When the repository can't be opened or the ref does not name a commit.
pub fn read_agent_ref(
    repository: &Path,
    remote: &AgentRemote,
    kind: AgentRef,
) -> Result<Option<git2::Oid>> {
    let repo = git2::Repository::open(repository).map_err(crate::map_git_err)?;
    let reference = match repo.find_reference(&remote.ref_name(kind)) {
        Ok(reference) => reference,
        Err(error) if error.code() == git2::ErrorCode::NotFound => return Ok(None),
        Err(error) => return Err(crate::map_git_err(error)),
    };
    let commit = reference.peel_to_commit().map_err(crate::map_git_err)?;
    Ok(Some(commit.id()))
}

const GITLINK_MODE: u32 = 0o160_000;

/// The `/`-separated components of a submodule path relative to its
/// superproject, or `None` when it is empty, absolute, or has `.`, `..` or
/// empty components.
fn module_components(path: &str) -> Option<Vec<&str>> {
    let parts: Vec<&str> = path.split('/').collect();
    let valid = parts
        .iter()
        .all(|part| !part.is_empty() && *part != "." && *part != ".." && !part.contains('\\'));
    valid.then_some(parts)
}

/// Whether `rel` is a gitlink in `repo`'s index or `HEAD` tree.
fn is_gitlink(repo: &git2::Repository, rel: &str) -> bool {
    let path = Path::new(rel);
    let in_index = repo
        .index()
        .ok()
        .and_then(|index| index.get_path(path, 0))
        .is_some_and(|entry| entry.mode == GITLINK_MODE);
    in_index
        || repo
            .head()
            .and_then(|head| head.peel_to_tree())
            .and_then(|tree| tree.get_path(path))
            .is_ok_and(|entry| u32::try_from(entry.filemode()).ok() == Some(GITLINK_MODE))
}

/// The populated submodule repository checked out at `dir`: one with its
/// own `.git` there, whose work tree is `dir` itself.
fn open_populated(dir: &Path) -> Option<git2::Repository> {
    std::fs::symlink_metadata(dir.join(".git")).ok()?;
    let repo = git2::Repository::open(dir).ok()?;
    let workdir = repo.workdir()?.canonicalize().ok()?;
    (workdir == dir.canonicalize().ok()?).then_some(repo)
}

/// Head side, when an agent is placed: every submodule populated in
/// `checkout` (it has its own `.git` and a commit checked out), recursively,
/// parents before children, at the commit its `HEAD` has checked out.
/// Unpopulated submodules are skipped, whatever `.gitmodules` says.
///
/// # Errors
///
/// When the checkout can't be opened.
pub fn submodule_pins(checkout: &Path) -> Result<Vec<SubmodulePin>> {
    let repo = git2::Repository::open(checkout).map_err(crate::map_git_err)?;
    let mut pins = Vec::new();
    collect_pins(&repo, "", &mut pins)?;
    let mut keys = std::collections::HashSet::new();
    pins.retain(|pin| keys.insert(pin.module_key.clone()));
    Ok(pins)
}

fn collect_pins(repo: &git2::Repository, prefix: &str, pins: &mut Vec<SubmodulePin>) -> Result<()> {
    let Some(workdir) = repo.workdir() else {
        return Ok(());
    };
    // An unreadable `.gitmodules` carries nothing rather than blocking
    // every remote agent of the workspace.
    let Ok(mut submodules) = repo.submodules() else {
        return Ok(());
    };
    submodules.sort_by(|a, b| a.path().cmp(b.path()));
    for submodule in submodules {
        let Some(rel) = submodule.path().to_str() else {
            continue;
        };
        let path = format!("{prefix}{rel}");
        if module_components(&path).is_none() || !is_gitlink(repo, rel) {
            continue;
        }
        let dir = workdir.join(rel);
        if std::fs::symlink_metadata(&dir).is_ok_and(|meta| meta.file_type().is_symlink()) {
            continue;
        }
        let Some(module) = open_populated(&dir) else {
            continue;
        };
        let Ok(commit) = module.head().and_then(|head| head.peel_to_commit()) else {
            continue;
        };
        pins.push(SubmodulePin {
            path: path.clone(),
            name: submodule.name().unwrap_or(rel).to_owned(),
            commit: commit.id().to_string(),
            module_key: key_of(&module)?,
        });
        collect_pins(&module, &format!("{path}/"), pins)?;
    }
    Ok(())
}

/// Node side: the submodules populated in `checkout` that are not among
/// `pins` (a setup script or the agent initialized them on the machine),
/// top-most only, parents before children. Their work is not carried back
/// to head. Pinned submodules are searched for unpinned children.
///
/// # Errors
///
/// When the checkout can't be opened.
pub fn unpinned_submodules(checkout: &Path, pins: &[SubmodulePin]) -> Result<Vec<String>> {
    let repo = git2::Repository::open(checkout).map_err(crate::map_git_err)?;
    let mut unpinned = Vec::new();
    collect_unpinned(&repo, "", pins, &mut unpinned);
    Ok(unpinned)
}

fn collect_unpinned(
    repo: &git2::Repository,
    prefix: &str,
    pins: &[SubmodulePin],
    unpinned: &mut Vec<String>,
) {
    let (Some(workdir), Ok(mut submodules)) = (repo.workdir(), repo.submodules()) else {
        return;
    };
    submodules.sort_by(|a, b| a.path().cmp(b.path()));
    for submodule in submodules {
        let Some(rel) = submodule.path().to_str() else {
            continue;
        };
        let path = format!("{prefix}{rel}");
        if module_components(&path).is_none() || !is_gitlink(repo, rel) {
            continue;
        }
        let dir = workdir.join(rel);
        if std::fs::symlink_metadata(&dir).is_ok_and(|meta| meta.file_type().is_symlink()) {
            continue;
        }
        let Some(module) = open_populated(&dir) else {
            continue;
        };
        if pins.iter().any(|pin| pin.path == path) {
            collect_unpinned(&module, &format!("{path}/"), pins, unpinned);
        } else {
            unpinned.push(path);
        }
    }
}

/// Why head will not serve a submodule repository for a module session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModuleRefusal {
    /// The path is not a submodule of the checkout (no gitlink, a symlink,
    /// outside it), or its repository is not the pinned one.
    Denied,
    /// The submodule is not populated in the checkout any more.
    Unavailable(String),
}

/// A submodule repository head serves a module session from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleRepository {
    /// The submodule's work tree in the checkout.
    pub worktree: PathBuf,
    /// Its repository's common git directory, which holds its refs.
    pub git_dir: PathBuf,
}

/// Head side: the populated submodule at `path` of `checkout`, only when
/// every step of `path` is a gitlink of its parent repository, no step is a
/// symlink, and its repository's key is `module_key`.
///
/// # Errors
///
/// [`ModuleRefusal::Denied`] or [`ModuleRefusal::Unavailable`].
pub fn module_repository(
    checkout: &Path,
    path: &str,
    module_key: &str,
) -> std::result::Result<ModuleRepository, ModuleRefusal> {
    let parts = module_components(path).ok_or(ModuleRefusal::Denied)?;
    let unavailable = |what: &str| ModuleRefusal::Unavailable(format!("{path}: {what}"));
    let root = checkout
        .canonicalize()
        .map_err(|_| unavailable("the checkout is gone"))?;
    let mut repo = git2::Repository::open(&root).map_err(|_| unavailable("no repository"))?;
    let mut dir = root.clone();
    let mut start = 0;
    while start < parts.len() {
        let end = (start + 1..=parts.len())
            .find(|&end| is_gitlink(&repo, &parts[start..end].join("/")))
            .ok_or(ModuleRefusal::Denied)?;
        for part in &parts[start..end] {
            dir.push(part);
            match std::fs::symlink_metadata(&dir) {
                Ok(meta) if meta.file_type().is_symlink() => return Err(ModuleRefusal::Denied),
                Ok(meta) if meta.is_dir() => {}
                _ => return Err(unavailable("not populated")),
            }
        }
        repo = open_populated(&dir).ok_or_else(|| unavailable("not populated"))?;
        start = end;
    }
    if key_of(&repo).ok().as_deref() != Some(module_key) {
        return Err(ModuleRefusal::Denied);
    }
    let git_dir = repo
        .commondir()
        .canonicalize()
        .map_err(|_| unavailable("its repository is gone"))?;
    Ok(ModuleRepository {
        worktree: dir,
        git_dir,
    })
}

fn invalid_pin(path: &str, why: &str) -> Error {
    Error::InvalidParams(format!("submodule {path:?}: {why}"))
}

/// Node side: check that `pins` is a launch's valid submodule set: safe,
/// unique relative paths, each listed after the pin it is nested in, with
/// a safe module key, a name and a full commit id.
///
/// # Errors
///
/// `InvalidParams` naming the first offending pin.
pub fn validate_pins(pins: &[SubmodulePin]) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for (index, pin) in pins.iter().enumerate() {
        let path = pin.path.as_str();
        if module_components(path).is_none() {
            return Err(invalid_pin(path, "not a safe relative path"));
        }
        if !seen.insert(path) {
            return Err(invalid_pin(path, "repeats another pin's path"));
        }
        if hub::component(&pin.module_key).is_err() {
            return Err(invalid_pin(path, "not a safe module key"));
        }
        if pin.name.is_empty() || pin.name.contains(['\n', '\0']) {
            return Err(invalid_pin(path, "not a submodule name"));
        }
        let hex = pin
            .commit
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
        if !hex || !matches!(pin.commit.len(), 40 | 64) {
            return Err(invalid_pin(path, "not a full commit id"));
        }
        if pins[index + 1..]
            .iter()
            .any(|later| is_nested_in(path, &later.path))
        {
            return Err(invalid_pin(path, "listed before its parent"));
        }
    }
    Ok(())
}

fn is_nested_in(path: &str, parent: &str) -> bool {
    path.strip_prefix(parent)
        .is_some_and(|rest| rest.starts_with('/'))
}

/// The pin `pins[index]` is nested in, if any: the deepest one before it
/// whose path contains it.
fn parent_of(pins: &[SubmodulePin], index: usize) -> Option<usize> {
    (0..index)
        .rev()
        .find(|&parent| is_nested_in(&pins[index].path, &pins[parent].path))
}

/// The `(path relative to the parent, wip)` gitlinks of the pushed pins
/// directly inside `parent` (`None`: the superproject).
fn child_links(
    pins: &[SubmodulePin],
    wips: &[Option<String>],
    parent: Option<usize>,
) -> Vec<(String, String)> {
    pins.iter()
        .enumerate()
        .filter(|&(index, _)| parent_of(pins, index) == parent)
        .filter_map(|(index, pin)| {
            let wip = wips[index].clone()?;
            let rel = match parent {
                Some(parent) => pin.path[pins[parent].path.len() + 1..].to_owned(),
                None => pin.path.clone(),
            };
            Some((rel, wip))
        })
        .collect()
}

/// Whether a submodule is checked out at `dir`: it has its own `.git`
/// and `dir` is not a symlink.
fn populated_here(dir: &Path) -> bool {
    std::fs::symlink_metadata(dir).is_ok_and(|meta| meta.is_dir())
        && std::fs::symlink_metadata(dir.join(".git")).is_ok()
}

/// Refuse a submodule path inside `worktree` with a symlinked step, so a
/// checkout never writes outside it.
fn ensure_no_symlink(worktree: &Path, path: &str) -> Result<()> {
    let mut dir = worktree.to_path_buf();
    for part in path.split('/') {
        dir.push(part);
        match std::fs::symlink_metadata(&dir) {
            Ok(meta) if meta.file_type().is_symlink() => {
                return Err(Error::Internal(format!(
                    "submodule {path}: {} is a symlink",
                    dir.display()
                )));
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
    Ok(())
}

/// A pinned submodule [`CheckoutCache::checkout_modules`] added.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleCheckout {
    pub path: String,
    /// The module's `base` ref: the pin.
    pub base: String,
    /// The commit it is at: the module's saved `head`, else `base`.
    pub at: String,
    /// Its `wip` commit when it did not apply on top of `at`; pinned on
    /// head under [`AgentRemote::unapplied_ref`] in the module's repository.
    pub unrestored_wip: Option<String>,
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
                "ext::env {PREFIX_ENV}={} git {} {} %s {}",
                remote.ref_prefix(),
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
        self.push_repository(worktree, remote, &[], None).await
    }

    /// [`Self::push_wip`] for a checkout with pinned submodules
    /// (nodeProtocol 10): each populated pin first, deepest first, to its
    /// module remote, then the superproject. Each repository's `wip` tree
    /// names its pinned children's `wip` commits. A failed module push ends
    /// the run before its parents; the next push sends it again.
    ///
    /// # Errors
    ///
    /// When the pins are not a valid set, a git step fails, or head rejects
    /// a push.
    pub async fn push_work(
        &self,
        worktree: &Path,
        remote: &AgentRemote,
        pins: &[SubmodulePin],
    ) -> Result<WipRefs> {
        self.push_work_excluding(worktree, remote, pins, None).await
    }

    /// [`Self::push_work`], leaving out of each repository's `wip` the
    /// untracked paths its [`exclude_file`] in `excludes` lists (a remote
    /// agent's setup-created files). The file replaces git's global
    /// excludes file for the push, so it carries that file's patterns too.
    ///
    /// # Errors
    ///
    /// As [`Self::push_work`].
    pub async fn push_work_excluding(
        &self,
        worktree: &Path,
        remote: &AgentRemote,
        pins: &[SubmodulePin],
        excludes: Option<&Path>,
    ) -> Result<WipRefs> {
        validate_pins(pins)?;
        let excludes_of = |pin: Option<usize>| {
            excludes
                .map(|dir| exclude_file(dir, pin))
                .filter(|file| file.is_file())
        };
        let mut wips: Vec<Option<String>> = vec![None; pins.len()];
        for (index, pin) in pins.iter().enumerate().rev() {
            let dir = worktree.join(&pin.path);
            if !populated_here(&dir) {
                continue;
            }
            let children = child_links(pins, &wips, Some(index));
            let module = remote.with_module(&pin.module_key)?;
            let pushed = self
                .push_repository(
                    &dir,
                    &module,
                    &children,
                    excludes_of(Some(index)).as_deref(),
                )
                .await
                .map_err(|error| Error::Internal(format!("submodule {}: {error}", pin.path)))?;
            wips[index] = Some(pushed.wip);
        }
        let children = child_links(pins, &wips, None);
        self.push_repository(worktree, remote, &children, excludes_of(None).as_deref())
            .await
    }

    /// Push one repository's `head` and `wip`; `links` are the
    /// `(path, commit)` gitlinks its `wip` tree records instead of what
    /// its children have checked out.
    async fn push_repository(
        &self,
        worktree: &Path,
        remote: &AgentRemote,
        links: &[(String, String)],
        excludes: Option<&Path>,
    ) -> Result<WipRefs> {
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
        let excludes_file = excludes.map(|file| format!("core.excludesFile={}", file.display()));
        match &excludes_file {
            Some(config) => run(with_index(&["-c", config, "add", "--all"])).await?,
            None => run(with_index(&["add", "--all"])).await?,
        };
        for (path, commit) in links {
            let entry = format!("160000,{commit},{path}");
            run(with_index(&[
                "update-index",
                "--add",
                "--cacheinfo",
                &entry,
            ]))
            .await?;
        }
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

/// The excludes file of one repository of a checkout in `dir`: the
/// superproject's for `None`, else the one of the pin at that index (pins
/// never change after an agent's creation).
#[must_use]
pub fn exclude_file(dir: &Path, pin: Option<usize>) -> PathBuf {
    match pin {
        None => dir.join("root"),
        Some(index) => dir.join(format!("module-{index}")),
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
        self.checkout_repository(&cache, remote, worktree, None)
            .await
    }

    /// Node side, after [`Self::checkout`] (nodeProtocol 10): check out each
    /// of `pins` not yet populated in `worktree`, parents first, from its
    /// module cache (created on first use) at the module's saved `head`,
    /// else its `base`, which must be the pinned commit. A module `wip` is
    /// restored on top, or pinned under `unapplied/` in the module's
    /// repository on head, as [`Self::checkout`] does. Each module is
    /// activated in its parent's own worktree config with fetch recursion off
    /// (also when already populated); no URL is set. A pin
    /// that is no longer a gitlink of its parent's index (the agent removed
    /// it in a commit) is skipped. Returns the modules it added.
    ///
    /// # Errors
    ///
    /// When the pins are not a valid set, a path has a symlinked step, head
    /// refuses a fetch, a base is not its pin, or a git step fails; the
    /// error names the submodule.
    pub async fn checkout_modules(
        &self,
        repo_key: &str,
        remote: &AgentRemote,
        worktree: &Path,
        pins: &[SubmodulePin],
    ) -> Result<Vec<ModuleCheckout>> {
        self.checkout_modules_reporting(repo_key, remote, worktree, pins, &|_| {})
            .await
    }

    /// [`Self::checkout_modules`], calling `fetching` with each pin's path
    /// before its module is fetched.
    ///
    /// # Errors
    ///
    /// As [`Self::checkout_modules`].
    pub async fn checkout_modules_reporting(
        &self,
        repo_key: &str,
        remote: &AgentRemote,
        worktree: &Path,
        pins: &[SubmodulePin],
        fetching: &(dyn Fn(&str) + Send + Sync),
    ) -> Result<Vec<ModuleCheckout>> {
        validate_pins(pins)?;
        hub::component(repo_key)?;
        let _serial = self.lock.lock().await;
        let mut added = Vec::new();
        for (index, pin) in pins.iter().enumerate() {
            let context =
                |error: Error| Error::Internal(format!("submodule {}: {error}", pin.path));
            ensure_no_symlink(worktree, &pin.path)?;
            let dir = worktree.join(&pin.path);
            let (parent, rel) = match parent_of(pins, index) {
                Some(parent) => (
                    worktree.join(&pins[parent].path),
                    &pin.path[pins[parent].path.len() + 1..],
                ),
                None => (worktree.to_path_buf(), pin.path.as_str()),
            };
            if populated_here(&dir) {
                // An earlier launch's checkout gets this launch's config.
                if let Err(error) = self.no_fetch_recursion(&parent, &pin.name).await {
                    tracing::warn!(%error, path = %pin.path, "node git: could not turn off fetch recursion");
                }
                continue;
            }
            // A submodule the agent removed in a commit stays removed.
            let pathspec = format!(":(literal){rel}");
            let staged = self
                .git
                .git(&parent, &["ls-files", "--stage", "--", &pathspec])
                .await
                .map_err(context)?;
            if !staged.starts_with("160000 ") {
                continue;
            }
            fetching(&pin.path);
            let cache = self.module_cache(repo_key, &pin.module_key)?;
            // A worktree the agent deleted by hand is still registered.
            self.prune_cache(&cache).await.map_err(context)?;
            let module = remote.with_module(&pin.module_key)?;
            let checkout = self
                .checkout_repository(&cache, &module, &dir, Some(&pin.commit))
                .await
                .map_err(context)?;
            self.activate(&parent, &pin.name).await.map_err(context)?;
            added.push(ModuleCheckout {
                path: pin.path.clone(),
                base: checkout.base,
                at: checkout.at,
                unrestored_wip: checkout.unrestored_wip,
            });
        }
        Ok(added)
    }

    fn module_cache(&self, repo_key: &str, module_key: &str) -> Result<PathBuf> {
        hub::component(repo_key)?;
        hub::component(module_key)?;
        Ok(self
            .root
            .join(format!("{repo_key}{MODULE_CACHES}"))
            .join(format!("{module_key}.git")))
    }

    /// Set `submodule.<name>.active` in `parent`'s own worktree config, and
    /// turn off its fetch recursion (see [`Self::no_fetch_recursion`]). The
    /// first time, turn on per-worktree config in `parent`'s shared cache,
    /// moving its `core.bare` to the cache's own worktree config first, so
    /// no other worktree of it ever reads itself as bare.
    async fn activate(&self, parent: &Path, name: &str) -> Result<()> {
        self.enable_worktree_config(parent).await?;
        let key = format!("submodule.{name}.active");
        self.git
            .git(parent, &["config", "--worktree", &key, "true"])
            .await?;
        self.no_fetch_recursion(parent, name).await
    }

    /// Set `submodule.<name>.fetchRecurseSubmodules` and
    /// `fetch.recurseSubmodules` to `false` in `parent`'s own worktree
    /// config: a machine's submodule has no remote, so git's on-demand
    /// recursion would fail every fetch in `parent` whose commits move its
    /// gitlink. The per-module key wins over `.gitmodules`; the worktree's
    /// `fetch.recurseSubmodules` wins over a global `submodule.recurse` or
    /// `fetch.recurseSubmodules`, which outrank the per-module key.
    async fn no_fetch_recursion(&self, parent: &Path, name: &str) -> Result<()> {
        let module = format!("submodule.{name}.fetchRecurseSubmodules");
        for key in [module.as_str(), "fetch.recurseSubmodules"] {
            self.git
                .git(parent, &["config", "--worktree", key, "false"])
                .await?;
        }
        Ok(())
    }

    /// Turn on per-worktree config in `worktree`'s shared cache (see
    /// [`Self::activate`]).
    async fn enable_worktree_config(&self, worktree: &Path) -> Result<()> {
        let parent = worktree;
        let common = PathBuf::from(
            self.git
                .git(
                    parent,
                    &["rev-parse", "--path-format=absolute", "--git-common-dir"],
                )
                .await?,
        );
        let config = common.join("config");
        let config_arg = config.to_string_lossy().into_owned();
        let get = |key: &'static str| {
            let mut command = self.git.command(parent);
            command.args(["config", "--file", &config_arg, "--get", key]);
            async move { run(command).await.ok() }
        };
        if get("extensions.worktreeConfig").await.as_deref() != Some("true") {
            if get("core.bare").await.as_deref() == Some("true") {
                let own = common.join("config.worktree");
                let own = own.to_string_lossy().into_owned();
                self.git
                    .git(parent, &["config", "--file", &own, "core.bare", "true"])
                    .await?;
                self.git
                    .git(
                        parent,
                        &["config", "--file", &config_arg, "--unset", "core.bare"],
                    )
                    .await?;
            }
            self.git
                .git(
                    parent,
                    &[
                        "config",
                        "--file",
                        &config_arg,
                        "extensions.worktreeConfig",
                        "true",
                    ],
                )
                .await?;
        }
        Ok(())
    }

    /// Node side (nodeProtocol 15): put `worktree` on the agent-local
    /// branch `local` at its current commit (created, or reset on a
    /// relaunch) and make `branch` on `remote_url` its upstream, with
    /// worktree-scoped config so no other checkout of the shared cache sees
    /// it: `origin`, its fetch refspec, the branch's remote and merge, and
    /// `push.default = upstream`. No credential is configured.
    ///
    /// # Errors
    ///
    /// When a git step fails, for example because another checkout uses
    /// `local`.
    pub async fn set_forge(
        &self,
        worktree: &Path,
        local: &str,
        remote_url: &str,
        branch: &str,
    ) -> Result<()> {
        let _serial = self.lock.lock().await;
        self.enable_worktree_config(worktree).await?;
        // A relaunch keeps a checkout already on `local`, and leaves one
        // stopped mid-rebase or mid-merge as it is: git would refuse the
        // switch, and the agent finishes that operation itself.
        let on_branch = self
            .git
            .git(worktree, &["symbolic-ref", "--quiet", "HEAD"])
            .await
            .is_ok_and(|head| head == format!("refs/heads/{local}"));
        if !on_branch && !self.operation_in_progress(worktree).await {
            self.git
                .git(worktree, &["switch", "--quiet", "-C", local])
                .await?;
        }
        let merge = format!("refs/heads/{branch}");
        let branch_remote = format!("branch.{local}.remote");
        let branch_merge = format!("branch.{local}.merge");
        for (key, value) in [
            ("remote.origin.url", remote_url),
            ("remote.origin.fetch", "+refs/heads/*:refs/remotes/origin/*"),
            (branch_remote.as_str(), "origin"),
            (branch_merge.as_str(), merge.as_str()),
            ("push.default", "upstream"),
        ] {
            self.git
                .git(
                    worktree,
                    &["config", "--worktree", "--replace-all", key, value],
                )
                .await?;
        }
        Ok(())
    }

    /// Whether `worktree` is in the middle of a rebase, merge, cherry-pick
    /// or revert.
    async fn operation_in_progress(&self, worktree: &Path) -> bool {
        for state in [
            "rebase-merge",
            "rebase-apply",
            "MERGE_HEAD",
            "CHERRY_PICK_HEAD",
            "REVERT_HEAD",
        ] {
            if let Ok(path) = self
                .git
                .git(
                    worktree,
                    &["rev-parse", "--path-format=absolute", "--git-path", state],
                )
                .await
            {
                if Path::new(&path).exists() {
                    return true;
                }
            }
        }
        false
    }

    /// Node side (nodeProtocol 17): give `worktree` an `origin` at
    /// `remote_url` with the usual fetch refspec, worktree-scoped and with
    /// no upstream branch, unless it already has one.
    ///
    /// # Errors
    ///
    /// When a git step fails.
    pub async fn ensure_origin(&self, worktree: &Path, remote_url: &str) -> Result<()> {
        let _serial = self.lock.lock().await;
        let has = self
            .git
            .git(worktree, &["config", "--get", "remote.origin.url"])
            .await
            .is_ok_and(|url| !url.is_empty());
        if has {
            return Ok(());
        }
        self.enable_worktree_config(worktree).await?;
        for (key, value) in [
            ("remote.origin.url", remote_url),
            ("remote.origin.fetch", "+refs/heads/*:refs/remotes/origin/*"),
        ] {
            self.git
                .git(
                    worktree,
                    &["config", "--worktree", "--replace-all", key, value],
                )
                .await?;
        }
        Ok(())
    }

    /// Undo [`Self::set_forge`]'s config in `worktree` (a relaunch without
    /// `forge`): it then has no `origin`. Missing entries are fine.
    pub async fn clear_forge(&self, worktree: &Path, local: &str) {
        let _serial = self.lock.lock().await;
        let Ok(own) = self
            .git
            .git(
                worktree,
                &[
                    "rev-parse",
                    "--path-format=absolute",
                    "--git-path",
                    "config.worktree",
                ],
            )
            .await
        else {
            return;
        };
        if !Path::new(&own).is_file() {
            return;
        }
        let branch = format!("branch.{local}");
        for args in [
            ["config", "--worktree", "--remove-section", "remote.origin"],
            ["config", "--worktree", "--remove-section", branch.as_str()],
            ["config", "--worktree", "--unset-all", "push.default"],
        ] {
            let _ = self.git.git(worktree, &args).await;
        }
    }

    /// Delete the agent-local branch `local` from every repository cache,
    /// once its checkout is gone. Missing branches are fine.
    pub async fn delete_branch(&self, local: &str) {
        let _serial = self.lock.lock().await;
        let Ok(entries) = std::fs::read_dir(&self.root) else {
            return;
        };
        let name = format!("refs/heads/{local}");
        for entry in entries.flatten() {
            let cache = entry.path();
            if cache.extension().is_some_and(|ext| ext == "git") && cache.join("HEAD").is_file() {
                if let Err(error) = self.git.git(&cache, &["update-ref", "-d", &name]).await {
                    tracing::warn!(%error, "node git: could not delete an agent branch");
                }
            }
        }
    }

    /// Fetch `remote`'s refs into the bare `cache` (created on first use)
    /// and add a detached worktree at `worktree`; see [`Self::checkout`].
    /// With `expect_base`, a `base` ref at another commit fails before the
    /// worktree is added.
    async fn checkout_repository(
        &self,
        cache: &Path,
        remote: &AgentRemote,
        worktree: &Path,
        expect_base: Option<&str>,
    ) -> Result<Checkout> {
        let cache = cache.to_path_buf();
        if !cache.join("HEAD").is_file() {
            let parent = cache.parent().unwrap_or(&self.root).to_path_buf();
            std::fs::create_dir_all(&parent).map_err(io)?;
            let mut init = self.git.command(&parent);
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
        if let Some(expected) = expect_base {
            if base != expected {
                return Err(Error::Internal(format!(
                    "node-git-failed: head's base ref is at {base}, not the pinned commit {expected}"
                )));
            }
        }
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
        // Gitlinks are the submodules' business: each restores its own.
        diff.args([
            "diff",
            "--binary",
            "--full-index",
            "--no-color",
            "--ignore-submodules=all",
        ])
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
            let path = entry.map_err(io)?.path();
            if path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().ends_with(MODULE_CACHES))
            {
                self.prune_dir(&path).await?;
            } else {
                self.prune_cache(&path).await?;
            }
        }
        Ok(())
    }

    /// `git worktree prune` in every bare cache directly in `dir`.
    async fn prune_dir(&self, dir: &Path) -> Result<()> {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(io(e)),
        };
        for entry in entries {
            self.prune_cache(&entry.map_err(io)?.path()).await?;
        }
        Ok(())
    }

    async fn prune_cache(&self, cache: &Path) -> Result<()> {
        if cache.extension().is_some_and(|ext| ext == "git") && cache.join("HEAD").is_file() {
            let mut prune = self.git.command(cache);
            prune.args(["worktree", "prune"]);
            run(prune).await?;
        }
        Ok(())
    }

    /// Remove an agent's worktree, with its submodules' worktrees, from the
    /// `repo_key` cache and its module caches.
    ///
    /// # Errors
    ///
    /// When `repo_key` is not a safe name, the directory can't be removed or
    /// git can't prune the caches.
    pub async fn remove(&self, repo_key: &str, worktree: &Path) -> Result<()> {
        let cache = self.cache(repo_key)?;
        let _serial = self.lock.lock().await;
        match std::fs::remove_dir_all(worktree) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io(e)),
        }
        self.prune_cache(&cache).await?;
        self.prune_dir(&self.root.join(format!("{repo_key}{MODULE_CACHES}")))
            .await
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
    let skip = stderr.chars().count().saturating_sub(1200);
    let detail: String = stderr.chars().skip(skip).collect();
    Err(Error::Internal(format!("node-git-failed: {detail}")))
}

pub mod forge;

#[cfg(test)]
mod tests;

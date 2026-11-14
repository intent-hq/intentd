//! Files and git on the home, orchestrator side (monorepo
//! `docs/protocol/node-link.md`, "Files and git on the home", nodeProtocol
//! 19): a client's file, git, file-tracking and search call for a workspace
//! whose home is on a machine runs there as a `homeCall`, and its result
//! comes back unchanged. Hub operations (`git.push`, `git.clone`,
//! `git.getConfig`, `git.getRemoteUrl`, `git.removeLockFile`) stay here;
//! `git.push` first has the home push its commits to the hub. Attachments
//! are kept here while they upload and placed on the home in pieces of at
//! most [`HOME_PIECE_BYTES`]. `host.exec` runs on the home as a home call
//! too, and `host.execStream` as an exec-mode `ptyOpen` there.

use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use base64::Engine as _;
use intent_core::node_link::{HomeCallFailure, HOME_METHODS};
use intent_core::{Error, Result, WorkspaceHome, WorkspaceId};
use serde_json::{json, Map, Value};
use sha2::Digest as _;

use crate::Services;

/// The most attachment bytes one home call carries.
pub(crate) const HOME_PIECE_BYTES: usize = 4 * 1024 * 1024;

/// Params that name one checkout path.
const PATH_PARAMS: &[&str] = &["path", "oldPath", "newPath", "filePath", "cwd"];

/// Params that name a list of checkout paths: an array, or one
/// comma-separated string.
const PATH_LIST_PARAMS: &[&str] = &["paths", "files"];

/// The path-based methods: `repoPath` names the repository, and
/// `workspaceId` only routes them.
const REPO_PATH_METHODS: &[&str] = &["git.pull", "git.getBranches", "git.branchStatus"];

/// Home methods that do not go to the home from here: uploads are kept
/// here, and the placement then sends them on through
/// [`Services::place_on_home`].
const NOT_ROUTED: &[&str] = &[
    "file.attachmentUpload.abort",
    "file.attachmentUpload.begin",
    "file.attachmentUpload.chunk",
    "file.attachmentUpload.commit",
    "file.placeAttachment",
];

/// The streaming exec, which runs on the home as an exec-mode `ptyOpen`
/// rather than a home call.
const EXEC_STREAM: &str = "host.execStream";

/// The margin a routed `host.exec` leaves past its own `timeoutMs`, so the
/// home's timed-out result arrives before the call's deadline.
const EXEC_REPLY_MARGIN: Duration = Duration::from_secs(10);

/// The longest a routed `host.exec` without a `timeoutMs` may run: the
/// machine's bound on any home call.
const EXEC_UNBOUNDED: Duration = Duration::from_secs(60 * 60);

/// Routed methods that change the checkout or its history: like local
/// writes, they are refused while the workspace is being deleted.
const HOME_WRITES: &[&str] = &[
    "file-tracking.stage",
    "file-tracking.unstage",
    "file.delete",
    "file.mkdir",
    "file.rename",
    "file.write",
    "git.agentCommit",
    "git.checkoutBranch",
    "git.createBranch",
    "git.discard",
    "git.fetch",
    "git.pull",
    "git.push",
    "git.renameBranch",
    "git.stage",
    "git.stageHunk",
    "git.unstage",
    "git.unstageHunk",
];

tokio::task_local! {
    /// On a machine, the home call this task runs: a search it starts ends
    /// with it ([`Services::run_home_call`]).
    static HOME_CALL: intent_search::CancelToken;
}

/// The cancel of the home call this task runs, if any.
pub(crate) fn home_call_cancel() -> Option<intent_search::CancelToken> {
    HOME_CALL.try_with(Clone::clone).ok()
}

/// Routed searches: `search.cancel` ends them on the home too.
const HOME_SEARCHES: &[&str] = &["search.inFiles", "search.fileNames", "search.codebase"];

/// What a home search `search.cancel` ended before the home answered
/// returns: no results, as a local search cancelled before its first match.
fn cancelled_search(method: &str, request_id: &str) -> Value {
    match method {
        "search.fileNames" => json!({ "requestId": request_id, "files": [], "truncated": false }),
        "search.inFiles" => json!({ "requestId": request_id, "matches": [], "truncated": false }),
        _ => json!({ "requestId": request_id, "matches": [] }),
    }
}

/// How long a routed call may run on the home.
fn call_timeout(method: &str, params: &Map<String, Value>) -> Duration {
    match method {
        "host.exec" => {
            params
                .get("timeoutMs")
                .and_then(Value::as_u64)
                .map_or(EXEC_UNBOUNDED, |ms| {
                    Duration::from_millis(ms.min(crate::host_exec::MAX_TIMEOUT_MS))
                        + EXEC_REPLY_MARGIN
                })
        }
        // Below the app's own 150 s bound for a pull, so the home's answer
        // arrives first.
        "git.pull" => Duration::from_secs(130),
        "git.fetch" | "git.agentCommit" | "git.checkoutBranch" => Duration::from_secs(120),
        _ => Duration::from_secs(60),
    }
}

/// Whether `method` runs on the workspace's home when it has one.
pub(crate) fn routed(method: &str) -> bool {
    HOME_METHODS.contains(&method) && !NOT_ROUTED.contains(&method)
}

/// `path` as a path relative to the home checkout: a relative path as
/// given (the machine checks it), an absolute one under `worktree` (as
/// given or with links resolved) stripped of it. `None` for an absolute
/// path anywhere else.
pub(crate) fn checkout_relative(worktree: &Path, path: &str) -> Option<String> {
    let given = Path::new(path);
    if !given.is_absolute() {
        return Some(path.to_owned());
    }
    let mut roots = vec![worktree.to_path_buf()];
    if let Ok(canonical) = std::fs::canonicalize(worktree) {
        roots.push(canonical);
    }
    let mut candidates = vec![given.to_path_buf()];
    if let Some(resolved) = resolve_existing(given) {
        candidates.push(resolved);
    }
    for candidate in &candidates {
        for root in &roots {
            if let Ok(rest) = candidate.strip_prefix(root) {
                if rest
                    .components()
                    .all(|part| matches!(part, Component::Normal(_)))
                {
                    return Some(rest.to_string_lossy().into_owned());
                }
            }
        }
    }
    None
}

/// `path` with links resolved in its deepest existing part; the rest
/// (a file a write makes) follows it.
fn resolve_existing(path: &Path) -> Option<PathBuf> {
    let mut existing = path;
    let mut rest = Vec::new();
    loop {
        if let Ok(canonical) = std::fs::canonicalize(existing) {
            let mut resolved = canonical;
            for part in rest.iter().rev() {
                resolved.push(part);
            }
            return Some(resolved);
        }
        rest.push(existing.file_name()?.to_owned());
        existing = existing.parent()?;
    }
}

/// A `host.execStream` `cwd` relative to the home checkout (`None` for
/// its root), mapped as [`checkout_relative`] maps a path; a relative one
/// that leaves the checkout is refused here, as a local stream refuses it
/// before it starts.
fn exec_cwd(worktree: &Path, cwd: &str) -> Result<Option<String>> {
    let cwd = cwd.trim();
    let rel = checkout_relative(worktree, cwd).ok_or_else(|| outside("cwd", cwd))?;
    if !Path::new(&rel)
        .components()
        .all(|part| matches!(part, Component::Normal(_) | Component::CurDir))
    {
        return Err(outside("cwd", cwd));
    }
    Ok((!rel.is_empty() && rel != ".").then_some(rel))
}

fn outside(param: &str, path: &str) -> Error {
    Error::InvalidParams(format!(
        "{param} {path:?} is outside the workspace's checkout"
    ))
}

/// The call's params with every absolute path under the orchestrator's
/// `worktree` made relative to the home checkout.
///
/// # Errors
///
/// `InvalidParams` for an absolute path outside `worktree`.
pub(crate) fn home_params(
    worktree: &Path,
    mut params: Map<String, Value>,
) -> Result<Map<String, Value>> {
    for param in PATH_PARAMS.iter().chain(std::iter::once(&"repoPath")) {
        if let Some(Value::String(path)) = params.get(*param) {
            let rel =
                checkout_relative(worktree, path.trim()).ok_or_else(|| outside(param, path))?;
            params.insert((*param).to_owned(), Value::String(rel));
        }
    }
    for param in PATH_LIST_PARAMS {
        match params.get(*param) {
            Some(Value::Array(paths)) => {
                let mapped = paths
                    .iter()
                    .map(|path| match path {
                        Value::String(path) => checkout_relative(worktree, path.trim())
                            .map(Value::String)
                            .ok_or_else(|| outside(param, path)),
                        other => Ok(other.clone()),
                    })
                    .collect::<Result<Vec<_>>>()?;
                params.insert((*param).to_owned(), Value::Array(mapped));
            }
            Some(Value::String(paths)) => {
                let mapped = paths
                    .split(',')
                    .map(|path| {
                        let path = path.trim();
                        if path.is_empty() {
                            return Ok(String::new());
                        }
                        checkout_relative(worktree, path).ok_or_else(|| outside(param, path))
                    })
                    .collect::<Result<Vec<_>>>()?;
                params.insert((*param).to_owned(), Value::String(mapped.join(",")));
            }
            _ => {}
        }
    }
    Ok(params)
}

/// Whether a path-based call's `repoPath` names the workspace's checkout
/// (or a folder in it); a call for another repository runs here.
fn names_checkout(worktree: &Path, params: &Map<String, Value>) -> bool {
    match params.get("repoPath") {
        Some(Value::String(path)) => checkout_relative(worktree, path.trim())
            .is_some_and(|_| Path::new(path.trim()).is_absolute()),
        _ => true,
    }
}

impl Services {
    /// On a machine, run the home call `call` until it ends, or until
    /// `stop` turns `true` (the backend sent `homeCallCancel`): a search the
    /// call runs then stops at its next file and answers with what it found.
    pub async fn run_home_call<F: std::future::Future>(
        mut stop: tokio::sync::watch::Receiver<bool>,
        call: F,
    ) -> F::Output {
        let cancel = intent_search::CancelToken::new();
        if *stop.borrow() {
            cancel.cancel();
        }
        let running = HOME_CALL.scope(cancel.clone(), call);
        tokio::pin!(running);
        // `false` when the call's end can no longer be observed.
        let stopped = async move { stop.wait_for(|stop| *stop).await.is_ok() };
        tokio::select! {
            biased;
            output = &mut running => output,
            true = stopped => {
                cancel.cancel();
                running.await
            }
        }
    }

    /// The workspace's home when it is on a machine and homes are on.
    pub(crate) async fn machine_home(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<WorkspaceHome>> {
        if !self.workspace_homes() || workspace_id.is_home_record() {
            return Ok(None);
        }
        Ok(self
            .store
            .get_workspace_home(workspace_id)
            .await?
            .filter(|home| home.node_id.is_some()))
    }

    /// See [`intent_core::WorkspaceApi::home_route`].
    pub(crate) async fn home_route(
        &self,
        method: &str,
        params: &Map<String, Value>,
    ) -> Option<std::result::Result<Value, HomeCallFailure>> {
        if method != "git.push" && method != EXEC_STREAM && !routed(method) {
            return None;
        }
        let workspace_id = WorkspaceId::from(params.get("workspaceId")?.as_str()?);
        let home = match self.machine_home(&workspace_id).await {
            Ok(home) => home?,
            Err(error) => return Some(Err(error.into())),
        };
        match self
            .route_to_home(&workspace_id, &home, method, params)
            .await
        {
            Ok(Some(result)) => Some(result),
            Ok(None) => None,
            Err(error) => Some(Err(error.into())),
        }
    }

    async fn route_to_home(
        &self,
        workspace_id: &WorkspaceId,
        home: &WorkspaceHome,
        method: &str,
        params: &Map<String, Value>,
    ) -> Result<Option<std::result::Result<Value, HomeCallFailure>>> {
        self.require_member(workspace_id).await?;
        // Held for the whole call, as a local write holds it.
        let _mutation = if HOME_WRITES.contains(&method) {
            Some(self.workspace_mutations.enter(workspace_id)?)
        } else {
            None
        };
        let port = self.feature_remote_launch_port();
        if method == "git.push" {
            // The hub pushes what the home has committed: have the home
            // push first, then push from here.
            port.push_home(self, home, workspace_id).await?;
            return Ok(None);
        }
        let workspace = self.store.get_workspace(workspace_id).await?;
        let worktree = crate::git_ops::worktree_path(&workspace)
            .ok_or_else(|| Error::InvalidParams("the workspace has no checkout".to_owned()))?;
        if REPO_PATH_METHODS.contains(&method) && !names_checkout(&worktree, params) {
            return Ok(None);
        }
        if method == EXEC_STREAM {
            return self
                .home_exec_stream(workspace_id, &worktree, params)
                .await
                .map(|request_id| Some(Ok(json!({ "requestId": request_id }))));
        }
        if method == "host.exec" {
            // Refused here as a local exec refuses it (`secretEnv`, a
            // missing command), before the home is asked.
            crate::host_exec::parse_args(params).map_err(crate::host_exec::domain_err)?;
        }
        if params.get("gitRootId").is_some_and(|id| !id.is_null()) {
            // The home's record knows no registered git roots of this
            // workspace.
            return Err(Error::InvalidParams(
                "home-root-unsupported: a workspace whose home is on a machine has no registered git roots yet"
                    .to_owned(),
            ));
        }
        if method == "git.agentCommit" {
            self.check_home_commit(workspace_id, params).await?;
        }
        let timeout = call_timeout(method, params);
        let mut params = home_params(&worktree, params.clone())?;
        if method == "git.agentCommit" {
            // The home commits as a remote agent's launch would: with the
            // identity this backend's git config gives the workspace.
            let checkout = worktree.clone();
            let identity = tokio::task::spawn_blocking(move || {
                intent_git::identity::commit_identity(&checkout)
            })
            .await
            .ok()
            .flatten();
            if let Some((name, email)) = identity {
                params.insert(
                    "gitIdentity".into(),
                    json!({ "name": name, "email": email }),
                );
            }
        }
        // A search runs on the home under the id the client cancels it by
        // (one minted here when it gave none, as a local search mints one).
        let search_id = HOME_SEARCHES.contains(&method).then(|| {
            if let Some(Value::String(id)) = params.get("requestId") {
                return id.clone();
            }
            let id = intent_search::mint_request_id();
            params.insert("requestId".into(), Value::String(id.clone()));
            id
        });
        let call = port.home_call(self, home, workspace_id, method, params, timeout);
        let result = match search_id {
            Some(request_id) => {
                let token = self
                    .search_cancels
                    .register_as(&request_id, crate::capability::search_owner()?);
                // Cancelled: the call is dropped, which sends the machine
                // `homeCallCancel`, and its late answer is never shown.
                let answered = tokio::select! {
                    biased;
                    answered = call => Some(answered),
                    () = token.cancelled() => None,
                };
                self.search_cancels.unregister(&request_id);
                match answered {
                    Some(answered) => answered?,
                    None => return Ok(Some(Ok(cancelled_search(method, &request_id)))),
                }
            }
            None => call.await?,
        };
        let result = match result {
            Ok(found) if method == "search.codebase" => {
                Ok(self.restream_search(workspace_id, found)?)
            }
            other => other,
        };
        Ok(Some(result.map_err(HomeCallFailure::Home)))
    }

    /// `host.execStream` on the home: the stream's `requestId`.
    async fn home_exec_stream(
        &self,
        workspace_id: &WorkspaceId,
        worktree: &Path,
        params: &Map<String, Value>,
    ) -> Result<String> {
        let args =
            crate::host_exec_stream::parse_args(params).map_err(crate::host_exec::domain_err)?;
        let cwd = args
            .common
            .cwd
            .as_deref()
            .map(|cwd| exec_cwd(worktree, cwd))
            .transpose()?
            .flatten();
        let bus = self
            .event_bus
            .clone()
            .ok_or_else(|| Error::Internal("host.execStream requires an event bus".into()))?;
        crate::host_exec_stream::start_home_stream(self, bus, workspace_id, args, cwd).await
    }

    /// The home answers a code search inline; more matches than a client
    /// takes inline stream from here as `search:result` and `search:done`,
    /// as a local search's do.
    fn restream_search(&self, workspace_id: &WorkspaceId, mut found: Value) -> Result<Value> {
        let many = found
            .get("matches")
            .and_then(Value::as_array)
            .is_some_and(|matches| matches.len() > crate::search_ops::INLINE_THRESHOLD);
        let request_id = found
            .get("requestId")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let (true, Some(request_id)) = (many, request_id) else {
            return Ok(found);
        };
        let Some(Value::Array(matches)) = found.get_mut("matches").map(Value::take) else {
            return Ok(found);
        };
        let token = self
            .search_cancels
            .register_as(&request_id, crate::capability::search_owner()?);
        Ok(self.deliver_search(&request_id, Some(workspace_id), matches, token))
    }

    /// The workspace's commit policy, applied before a commit goes to the
    /// home (the machine commits as asked): an agent-initiated commit needs
    /// auto-commit on and, from a client, an explicit `files` list.
    async fn check_home_commit(
        &self,
        workspace_id: &WorkspaceId,
        params: &Map<String, Value>,
    ) -> Result<()> {
        let user_requested = match params.get("userRequested") {
            Some(Value::Bool(flag)) => *flag,
            Some(Value::String(flag)) => flag == "true",
            _ => false,
        };
        if user_requested {
            return Ok(());
        }
        let auto_commit = self.effective_auto_commit(workspace_id).await;
        crate::git_ops::assert_agent_commit_allowed(auto_commit, false)?;
        let files = params
            .get("files")
            .and_then(Value::as_array)
            .is_some_and(|files| !files.is_empty());
        if !files {
            return Err(Error::Internal(
                "Agent-initiated commit without an agentId cannot be attributed: \
                 pass an explicit `files` list"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    /// Place an attachment on the workspace's home instead of here, in
    /// pieces of at most [`HOME_PIECE_BYTES`]: one `file.placeAttachment`
    /// when it fits one piece, else the home's upload session
    /// (`file.attachmentUpload.begin`, `.chunk` per piece, `.commit`).
    /// `None` when the workspace has no home on a machine. The home's
    /// result is the answer, as its registry now holds the attachment.
    pub(crate) async fn place_on_home(
        &self,
        workspace_id: &WorkspaceId,
        file_name: &str,
        source: &crate::file_ops::AttachmentSource<'_>,
        mime_type: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Option<Result<Value>> {
        let home = match self.machine_home(workspace_id).await {
            Ok(home) => home?,
            Err(error) => return Some(Err(error)),
        };
        let placed = self
            .place_pieces(
                &home,
                workspace_id,
                file_name,
                source,
                mime_type,
                idempotency_key,
            )
            .await;
        if let Ok(placed) = &placed {
            self.keep_home_placement(workspace_id, placed, source, mime_type)
                .await;
        }
        Some(placed)
    }

    /// Keep a copy of an attachment the home placed, with a registry row of
    /// the same id: chat images and `ws.file.getAttachment` resolve
    /// attachments here. A failure only loses the local copy.
    async fn keep_home_placement(
        &self,
        workspace_id: &WorkspaceId,
        placed: &Value,
        source: &crate::file_ops::AttachmentSource<'_>,
        mime_type: Option<&str>,
    ) {
        if let Err(error) = self
            .keep_home_placement_inner(workspace_id, placed, source, mime_type)
            .await
        {
            tracing::warn!(workspace = %workspace_id, %error, "could not keep a copy of an attachment placed on the home");
        }
    }

    async fn keep_home_placement_inner(
        &self,
        workspace_id: &WorkspaceId,
        placed: &Value,
        source: &crate::file_ops::AttachmentSource<'_>,
        mime_type: Option<&str>,
    ) -> Result<()> {
        let text = |key: &str| placed.get(key).and_then(Value::as_str).map(str::to_owned);
        let (Some(id), Some(path)) = (text("attachmentId"), text("path")) else {
            return Ok(());
        };
        if self.store.get_attachment(&id).await.is_ok() {
            return Ok(());
        }
        let inside = Path::new(&path)
            .strip_prefix(crate::file_ops::ATTACHMENTS_DIR)
            .is_ok_and(|rest| {
                rest.components().count() == 1
                    && rest
                        .components()
                        .all(|part| matches!(part, Component::Normal(_)))
            });
        if !inside {
            return Err(Error::Internal(format!(
                "the home placed the attachment outside the attachments folder: {path}"
            )));
        }
        let root = crate::file_ops::resolve_root(
            &self.store,
            workspace_id,
            None,
            self.chief_cwd_root.as_deref(),
        )
        .await;
        if root.is_empty() {
            return Ok(());
        }
        crate::repo_config::ensure_intent_dir(Path::new(&root)).await?;
        let target = crate::file_ops::resolve_within(&root, &path)?;
        if let Some(dir) = target.parent() {
            tokio::fs::create_dir_all(dir)
                .await
                .map_err(|e| Error::Internal(format!("keep the attachment: {e}")))?;
            let marker = dir.join(".gitignore");
            if tokio::fs::metadata(&marker).await.is_err() {
                tokio::fs::write(&marker, "*\n")
                    .await
                    .map_err(|e| Error::Internal(format!("keep the attachment: {e}")))?;
            }
        }
        match source {
            crate::file_ops::AttachmentSource::Bytes(bytes) => tokio::fs::write(&target, bytes)
                .await
                .map_err(|e| Error::Internal(format!("keep the attachment: {e}")))?,
            crate::file_ops::AttachmentSource::CopyFrom(from) => {
                tokio::fs::copy(from, &target)
                    .await
                    .map_err(|e| Error::Internal(format!("keep the attachment: {e}")))?;
            }
        }
        self.store
            .insert_attachment(&intent_store::AttachmentRecord {
                id,
                workspace_id: workspace_id.clone(),
                file_name: text("fileName").unwrap_or_default(),
                mime_type: mime_type
                    .filter(|mime| !mime.trim().is_empty())
                    .map(str::to_owned),
                size: placed
                    .get("size")
                    .and_then(Value::as_i64)
                    .unwrap_or_default(),
                uploaded_at: text("uploadedAt").unwrap_or_else(intent_core::now_iso),
                stored_path: path,
            })
            .await
    }

    async fn place_pieces(
        &self,
        home: &WorkspaceHome,
        workspace_id: &WorkspaceId,
        file_name: &str,
        source: &crate::file_ops::AttachmentSource<'_>,
        mime_type: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<Value> {
        let payload = Payload::open(source).await?;
        let mut header = Map::new();
        header.insert("fileName".into(), json!(file_name));
        if let Some(mime) = mime_type {
            header.insert("mimeType".into(), json!(mime));
        }
        if let Some(key) = idempotency_key {
            header.insert("idempotencyKey".into(), json!(key));
        }
        if payload.size <= HOME_PIECE_BYTES as u64 {
            let bytes = payload.piece(0).await?;
            let mut params = header;
            params.insert(
                "data".into(),
                json!(base64::engine::general_purpose::STANDARD.encode(bytes)),
            );
            return self
                .attachment_call(home, workspace_id, "file.placeAttachment", params)
                .await;
        }
        let mut begin = header;
        begin.insert("sizeBytes".into(), json!(payload.size));
        begin.insert("sha256".into(), json!(payload.sha256().await?));
        let opened = match self
            .attachment_call(home, workspace_id, "file.attachmentUpload.begin", begin)
            .await
        {
            Err(Error::InvalidParams(message))
                if idempotency_key.is_some() && message.contains("already committed") =>
            {
                // A retry of a placement that landed: replay it, as a
                // keyed placement here does.
                return self
                    .replay_home_placement(home, workspace_id, idempotency_key.unwrap_or_default())
                    .await;
            }
            opened => opened?,
        };
        let upload_id = opened
            .get("uploadId")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Internal("the home opened no upload".to_owned()))?
            .to_owned();
        // Ends the home's session unless it is committed, also when this
        // call is dropped midway.
        let mut session = OpenUpload {
            services: self.clone(),
            home: home.clone(),
            workspace_id: workspace_id.clone(),
            upload_id: upload_id.clone(),
            settled: false,
        };
        self.send_pieces(home, workspace_id, &upload_id, &payload)
            .await?;
        let mut commit = Map::new();
        commit.insert("uploadId".into(), json!(upload_id));
        let committed = self
            .attachment_call(home, workspace_id, "file.attachmentUpload.commit", commit)
            .await;
        session.settled = committed.is_ok();
        committed
    }

    /// The original result of the home's placement bound to `key`, marked
    /// `replayed`.
    async fn replay_home_placement(
        &self,
        home: &WorkspaceHome,
        workspace_id: &WorkspaceId,
        key: &str,
    ) -> Result<Value> {
        let mut lookup = Map::new();
        lookup.insert("idempotencyKey".into(), json!(key));
        let info = self
            .attachment_call(home, workspace_id, "file.getAttachmentInfo", lookup)
            .await?;
        let mut result = json!({
            "ok": true,
            "path": info.get("path").cloned().unwrap_or(Value::Null),
            "fileName": info.get("fileName").cloned().unwrap_or(Value::Null),
            "size": info.get("size").cloned().unwrap_or(Value::Null),
            "attachmentId": info.get("attachmentId").cloned().unwrap_or(Value::Null),
            "uploadedAt": info.get("uploadedAt").cloned().unwrap_or(Value::Null),
            "replayed": true,
        });
        if let Some(mime) = info.get("mimeType").filter(|mime| !mime.is_null()) {
            result["mimeType"] = mime.clone();
        }
        Ok(result)
    }

    async fn send_pieces(
        &self,
        home: &WorkspaceHome,
        workspace_id: &WorkspaceId,
        upload_id: &str,
        payload: &Payload<'_>,
    ) -> Result<()> {
        let pieces = payload.size.div_ceil(HOME_PIECE_BYTES as u64);
        for seq in 0..pieces {
            let bytes = payload.piece(seq).await?;
            let mut chunk = Map::new();
            chunk.insert("uploadId".into(), json!(upload_id));
            chunk.insert("seq".into(), json!(seq));
            chunk.insert(
                "data".into(),
                json!(base64::engine::general_purpose::STANDARD.encode(bytes)),
            );
            self.attachment_call(home, workspace_id, "file.attachmentUpload.chunk", chunk)
                .await?;
        }
        Ok(())
    }

    /// One attachment call on the home; its error as this daemon's.
    async fn attachment_call(
        &self,
        home: &WorkspaceHome,
        workspace_id: &WorkspaceId,
        method: &str,
        mut params: Map<String, Value>,
    ) -> Result<Value> {
        params.insert("workspaceId".into(), json!(workspace_id.as_str()));
        self.feature_remote_launch_port()
            .home_call(
                self,
                home,
                workspace_id,
                method,
                params,
                Duration::from_secs(120),
            )
            .await?
            .map_err(|error| home_error(&error))
    }
}

/// The home's upload session of one placement: aborted when dropped
/// before its commit landed.
struct OpenUpload {
    services: Services,
    home: WorkspaceHome,
    workspace_id: WorkspaceId,
    upload_id: String,
    settled: bool,
}

impl Drop for OpenUpload {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let services = self.services.clone();
        let home = self.home.clone();
        let workspace_id = self.workspace_id.clone();
        let mut abort = Map::new();
        abort.insert("uploadId".into(), json!(self.upload_id));
        // caller-binding: allow — best-effort end of an abandoned upload on the home
        tokio::spawn(async move {
            let _ = services
                .attachment_call(&home, &workspace_id, "file.attachmentUpload.abort", abort)
                .await;
        });
    }
}

/// The home's error for an attachment call, as a domain error: its
/// message, `-32602` as invalid params, anything else internal.
pub(crate) fn home_error(error: &intent_core::node_link::HomeCallError) -> Error {
    if error.code == -32602 {
        Error::InvalidParams(error.message.clone())
    } else {
        Error::Internal(error.message.clone())
    }
}

/// An attachment's bytes, read a piece at a time.
struct Payload<'a> {
    source: &'a crate::file_ops::AttachmentSource<'a>,
    size: u64,
}

impl<'a> Payload<'a> {
    /// The payload of `source`, refusing a `sourcePath` that is not a
    /// readable regular file as a local placement does.
    async fn open(source: &'a crate::file_ops::AttachmentSource<'a>) -> Result<Self> {
        let size = match source {
            crate::file_ops::AttachmentSource::Bytes(bytes) => bytes.len() as u64,
            crate::file_ops::AttachmentSource::CopyFrom(path) => {
                crate::file_ops::check_attachment_source(path)?;
                tokio::fs::metadata(path)
                    .await
                    .map_err(|e| Error::Internal(format!("read the attachment: {e}")))?
                    .len()
            }
        };
        Ok(Self { source, size })
    }

    /// Piece `seq` (of [`HOME_PIECE_BYTES`] each).
    async fn piece(&self, seq: u64) -> Result<Vec<u8>> {
        let start = seq.saturating_mul(HOME_PIECE_BYTES as u64);
        match self.source {
            crate::file_ops::AttachmentSource::Bytes(bytes) => {
                let start = usize::try_from(start)
                    .unwrap_or(usize::MAX)
                    .min(bytes.len());
                let end = start.saturating_add(HOME_PIECE_BYTES).min(bytes.len());
                Ok(bytes[start..end].to_vec())
            }
            crate::file_ops::AttachmentSource::CopyFrom(path) => {
                use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};
                let mut file = tokio::fs::File::open(path)
                    .await
                    .map_err(|e| Error::Internal(format!("read the attachment: {e}")))?;
                file.seek(std::io::SeekFrom::Start(start))
                    .await
                    .map_err(|e| Error::Internal(format!("read the attachment: {e}")))?;
                let mut piece = Vec::with_capacity(HOME_PIECE_BYTES);
                file.take(HOME_PIECE_BYTES as u64)
                    .read_to_end(&mut piece)
                    .await
                    .map_err(|e| Error::Internal(format!("read the attachment: {e}")))?;
                Ok(piece)
            }
        }
    }

    /// The lowercase-hex SHA-256 of the whole payload.
    async fn sha256(&self) -> Result<String> {
        let mut hasher = sha2::Sha256::new();
        let pieces = self.size.div_ceil(HOME_PIECE_BYTES as u64);
        for seq in 0..pieces {
            hasher.update(self.piece(seq).await?);
        }
        Ok(hasher
            .finalize()
            .iter()
            .fold(String::with_capacity(64), |mut hex, byte| {
                use std::fmt::Write as _;
                let _ = write!(hex, "{byte:02x}");
                hex
            }))
    }
}

#[cfg(test)]
#[path = "home_routing/tests.rs"]
mod tests;

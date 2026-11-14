//! Files and git on the home (monorepo `docs/protocol/node-link.md`, "Files
//! and git on the home", nodeProtocol 19), machine side: a backend's
//! `homeCall` runs one wire method of [`HOME_METHODS`] against the home
//! checkout, through the machine's hidden workspace record of it
//! ([`super::HomeRecords`]), and answers with that method's own result.
//!
//! Every path a call names is a checkout path: relative, inside the
//! checkout once links are followed, and without a `.git` component. No
//! result or error carries the checkout's absolute path. A reply that does
//! not fit one frame is `-32603` [`HOME_REPLY_TOO_LARGE`]. Writing calls
//! push `home/wip` soon after; `git.agentCommit` pushes `home/head` before
//! it answers.

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};

use intent_core::node_link::{
    HomeCall, HomeCallError, HomeCallReply, HomeRef, LinkFrame, LinkId, HOME_METHODS,
    HOME_REPLY_TOO_LARGE, MAX_FRAME_BYTES, NODE_HOME_UNKNOWN, NODE_HOME_UNSUPPORTED,
};
use intent_core::nodes::NodeCounter;

use super::homes::Home;
use super::{lock, Runner};

/// JSON-RPC `Invalid params`.
const INVALID_PARAMS: i64 = -32602;
/// JSON-RPC `Method not found`.
const METHOD_NOT_FOUND: i64 = -32601;
/// JSON-RPC `Internal error`.
const INTERNAL_ERROR: i64 = -32603;

/// The longest `git.agentCommit` waits for its `home/head` push before it
/// answers with `backupPending`.
const COMMIT_PUSH_TIMEOUT: Duration = Duration::from_secs(60);

/// The least time a commit's push leaves before the call's deadline, so
/// its `backupPending` reply still reaches the backend in time.
const REPLY_MARGIN: Duration = Duration::from_secs(2);

/// The longest a home call may run (one hour); a larger `timeoutMs` is
/// clamped.
const MAX_TIMEOUT_MS: u64 = 60 * 60 * 1000;

/// Result members that carry file or command contents, never paths: they
/// are passed on unchanged.
const CONTENT_KEYS: &[&str] = &[
    "content",
    "data",
    "patch",
    "diff",
    "hunk",
    "hunkPatch",
    "line",
    "lineText",
    "text",
    "preview",
    "match",
    "stdout",
    "stderr",
    "output",
    "before",
    "after",
    "oldContent",
    "newContent",
    "body",
];

/// Params that name one checkout path, passed on as given.
const PATH_PARAMS: &[&str] = &["path", "oldPath", "newPath", "filePath", "cwd"];

/// Params that name a list of checkout paths.
const PATH_LIST_PARAMS: &[&str] = &["paths", "files"];

/// Params the wire method reads as an absolute path on its host: a checkout
/// path is resolved to the checkout's own path here.
const HOST_PATH_PARAMS: &[&str] = &["repoPath", "sourcePath"];

/// The methods whose repository defaults to the checkout when `repoPath`
/// is absent (they take no `workspaceId`).
const REPO_PATH_METHODS: &[&str] = &["git.pull", "git.getBranches", "git.branchStatus"];

/// The methods that change the checkout's files or index: a `home/wip`
/// push follows them.
const WRITING_METHODS: &[&str] = &[
    "file-tracking.stage",
    "file-tracking.unstage",
    "file.delete",
    "file.mkdir",
    "file.placeAttachment",
    "file.rename",
    "file.write",
    "git.checkoutBranch",
    "git.createBranch",
    "git.discard",
    "git.pull",
    "git.renameBranch",
    "git.stage",
    "git.stageHunk",
    "git.unstage",
    "git.unstageHunk",
    "host.exec",
];

impl Runner {
    /// Register a home call before its task runs, so a cancel or the end of
    /// its link always finds it; pass the receiver to [`Self::home_call`].
    pub(in crate::executor) fn begin_home_call(
        &self,
        id: LinkId,
        generation: NodeCounter,
    ) -> Option<tokio::sync::oneshot::Receiver<()>> {
        let (cancel, cancelled) = tokio::sync::oneshot::channel();
        match lock(&self.inner.home_calls).entry(id) {
            // A second call with a running call's id is refused; the
            // first keeps running and can still be cancelled.
            std::collections::hash_map::Entry::Occupied(_) => None,
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert((generation, cancel));
                Some(cancelled)
            }
        }
    }

    /// The reply to a call reusing a running call's `requestId`.
    pub(in crate::executor) fn duplicate_home_call(id: LinkId) -> HomeCallReply {
        HomeCallReply::err(
            id,
            INVALID_PARAMS,
            "Invalid params: a home call with this requestId is running".to_owned(),
            Some(json!({ "code": "invalid-params" })),
        )
    }

    /// How many home calls are running (test seam).
    #[cfg(test)]
    pub(in crate::executor) fn home_calls_for_test(&self) -> usize {
        lock(&self.inner.home_calls).len()
    }

    /// End the home call `id`; it is not answered.
    pub(in crate::executor) fn cancel_home_call(&self, id: LinkId) {
        if let Some((_, cancel)) = lock(&self.inner.home_calls).remove(&id) {
            let _ = cancel.send(());
        }
    }

    /// End every home call link generation `generation` asked for: that
    /// link is gone, and the backend fails them itself.
    pub(in crate::executor) fn cancel_binding_home_calls(&self, generation: NodeCounter) {
        let mut calls = lock(&self.inner.home_calls);
        let ended: Vec<_> = calls
            .iter()
            .filter(|(_, (owner, _))| *owner == generation)
            .map(|(id, _)| *id)
            .collect();
        for id in ended {
            if let Some((_, cancel)) = calls.remove(&id) {
                let _ = cancel.send(());
            }
        }
    }

    /// Run the home call `frame` and return its reply, or `None` when it was
    /// cancelled, its link ended or its `timeoutMs` passed (the backend has
    /// stopped waiting for it then).
    pub(in crate::executor) async fn home_call(
        &self,
        frame: HomeCall,
        cancelled: tokio::sync::oneshot::Receiver<()>,
    ) -> Option<HomeCallReply> {
        let id = frame.request_id;
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(frame.timeout_ms.min(MAX_TIMEOUT_MS));
        let shown_as = frame.workspace_id.clone();
        let answer = tokio::select! {
            answer = tokio::time::timeout_at(deadline, self.answer_home_call(frame, deadline)) => {
                answer.ok()
            }
            Ok(()) = cancelled => None,
        };
        lock(&self.inner.home_calls).remove(&id);
        let (result, shown) = answer?;
        Some(fit_reply(id, result, &shown.with_workspace(shown_as)))
    }

    /// The call's result and what its reply must not show.
    async fn answer_home_call(
        &self,
        frame: HomeCall,
        deadline: tokio::time::Instant,
    ) -> (Result<Value, HomeCallError>, Hidden) {
        if !HOME_METHODS.contains(&frame.method.as_str()) {
            return (
                Err(rpc_error(
                    METHOD_NOT_FOUND,
                    format!("Method not found: {} is not a home method", frame.method),
                    None,
                )),
                Hidden::default(),
            );
        }
        let Some(records) = self.inner.home_records.clone() else {
            return (
                Err(rpc_error(
                    INTERNAL_ERROR,
                    format!("{NODE_HOME_UNSUPPORTED}: this machine answers no home calls"),
                    Some(json!({ "code": NODE_HOME_UNSUPPORTED })),
                )),
                Hidden::default(),
            );
        };
        let home_ref = frame.home();
        let Some(home) = self.home_for_call(&home_ref).await else {
            return (
                Err(rpc_error(
                    INVALID_PARAMS,
                    format!(
                        "{NODE_HOME_UNKNOWN}: this machine holds no home {}",
                        frame.home_id
                    ),
                    Some(json!({ "code": NODE_HOME_UNKNOWN })),
                )),
                Hidden::default(),
            );
        };
        let mut hidden = Hidden {
            roots: shown_roots(&home.worktree),
            ..Hidden::default()
        };
        let record = match self.home_record(&records, &home).await {
            Ok(record) => record,
            Err(error) => {
                return (
                    Err(rpc_error(
                        INTERNAL_ERROR,
                        format!(
                            "{NODE_HOME_UNKNOWN}: the home's record could not be made: {error:#}"
                        ),
                        Some(json!({ "code": NODE_HOME_UNKNOWN })),
                    )),
                    hidden,
                );
            }
        };
        hidden.record = Some(record.as_str().to_owned());
        let method = frame.method;
        let frame_timeout_ms = frame.timeout_ms.min(MAX_TIMEOUT_MS);
        let params = match checkout_params(&method, frame.params, &home.worktree, &record) {
            Ok(params) => params,
            Err(error) => return (Err(error), hidden),
        };
        let result = records.dispatch(method.clone(), params).await;
        if WRITING_METHODS.contains(&method.as_str()) {
            *lock(&home.wip_due) = Some(Instant::now());
        }
        let result = match (method.as_str(), result) {
            ("git.agentCommit", Ok(mut committed)) => {
                // The commit landed: a failed push still answers success, so
                // a client never retries a commit that exists.
                // The push leaves the reply time to arrive before the
                // backend's own timeout.
                let margin = REPLY_MARGIN.max(Duration::from_millis(frame_timeout_ms / 10));
                let push_by =
                    (deadline - margin).min(tokio::time::Instant::now() + COMMIT_PUSH_TIMEOUT);
                let pushed = if push_by > tokio::time::Instant::now() {
                    self.push_home_now(&home, push_by).await
                } else {
                    Err(anyhow::anyhow!("no time was left for the push"))
                };
                if let Err(error) = pushed {
                    tracing::info!(home = %home.of.home_id, %error, "node: home head push after a commit failed");
                    if let Some(object) = committed.as_object_mut() {
                        object.insert("backupPending".into(), json!(true));
                    }
                }
                Ok(committed)
            }
            (_, result) => result,
        };
        (result, hidden)
    }

    /// The open home a call names, reopening a leased home after a restart.
    async fn home_for_call(&self, home: &HomeRef) -> Option<Arc<Home>> {
        if self.leased_unopened(home) {
            self.resume_leased_homes(std::slice::from_ref(home)).await;
        }
        self.leased_home(&home.workspace_id, &home.home_id)
    }

    /// The home's hidden record, made now when its open could not.
    async fn home_record(
        &self,
        records: &Arc<dyn super::HomeRecords>,
        home: &Home,
    ) -> anyhow::Result<intent_core::WorkspaceId> {
        let mut record = home.record.lock().await;
        if let Some(record) = record.as_ref() {
            return Ok(record.clone());
        }
        // A closed home makes no new record; its removal already ran.
        anyhow::ensure!(!*home.closed.borrow(), "the home is closed");
        let made = records.register(&home.of, &home.worktree).await?;
        *record = Some(made.clone());
        Ok(made)
    }

    /// Make the hidden record of a home just opened, on its own task.
    pub(super) fn make_home_record(&self, home: &Arc<Home>) {
        let Some(records) = self.inner.home_records.clone() else {
            return;
        };
        let home = home.clone();
        let runner = self.clone();
        // caller-binding: allow — node-local record of one home checkout
        tokio::spawn(async move {
            if let Err(error) = runner.home_record(&records, &home).await {
                tracing::warn!(home = %home.of.home_id, %error, "node: could not record a home checkout; the first call retries");
            }
        });
    }

    /// Remove the hidden record of a home being closed, on its own task.
    pub(super) fn drop_home_record(&self, home: &Arc<Home>) {
        let Some(records) = self.inner.home_records.clone() else {
            return;
        };
        let home = home.clone();
        // caller-binding: allow — node-local removal of one home's record
        tokio::spawn(async move {
            if let Some(record) = home.record.lock().await.take() {
                records.remove(record).await;
            }
        });
    }
}

fn rpc_error(code: i64, message: String, data: Option<Value>) -> HomeCallError {
    HomeCallError {
        code,
        message,
        data,
    }
}

/// A path the call may not name: `-32602`.
fn outside(param: &str, path: &str) -> HomeCallError {
    rpc_error(
        INVALID_PARAMS,
        format!("Invalid params: {param} {path:?} is not a path in the home checkout"),
        Some(json!({ "code": "invalid-params" })),
    )
}

/// Whether `part` is `.git` (ASCII case-insensitive).
fn is_git(part: &std::ffi::OsStr) -> bool {
    part.to_str()
        .is_some_and(|part| part.eq_ignore_ascii_case(".git"))
}

/// `rel` resolved in `checkout`: a relative path with no `..` and no
/// `.git` component, whose deepest existing part stays inside the checkout
/// (outside any `.git`) once links are followed. `""` and `.` are the
/// checkout itself.
pub(super) fn checkout_path(checkout: &Path, rel: &str) -> Option<PathBuf> {
    let mut joined = checkout.to_path_buf();
    for component in Path::new(rel).components() {
        match component {
            Component::Normal(part) if !is_git(part) => joined.push(part),
            Component::CurDir => {}
            _ => return None,
        }
    }
    let root = std::fs::canonicalize(checkout).ok()?;
    // The deepest part that exists decides where the path leads; what does
    // not exist yet (a file a write makes) follows it.
    let mut existing = joined.as_path();
    while std::fs::symlink_metadata(existing).is_err() {
        existing = existing.parent()?;
    }
    let canonical = std::fs::canonicalize(existing).ok()?;
    let inside = canonical
        .strip_prefix(&root)
        .is_ok_and(|rest| !rest.components().any(|part| is_git(part.as_os_str())));
    inside.then_some(joined)
}

/// The call's params as the wire method takes them on this machine: its
/// `workspaceId` names the hidden `record`, every path is checked as a
/// checkout path, and host paths are made absolute in the checkout.
fn checkout_params(
    method: &str,
    mut params: Map<String, Value>,
    checkout: &Path,
    record: &intent_core::WorkspaceId,
) -> Result<Map<String, Value>, HomeCallError> {
    let check = |param: &str, value: &Value| -> Result<PathBuf, HomeCallError> {
        let Some(path) = value.as_str() else {
            // Not a string: the wire method answers its own type error.
            return Ok(PathBuf::new());
        };
        checkout_path(checkout, path).ok_or_else(|| outside(param, path))
    };
    for param in PATH_PARAMS {
        if let Some(value) = params.get(*param) {
            check(param, &trimmed(value))?;
        }
    }
    for param in PATH_LIST_PARAMS {
        // As `file-tracking` reads them: an array of strings or one
        // comma-separated string, each entry trimmed.
        let entries: Vec<String> = match params.get(*param) {
            Some(Value::Array(paths)) => paths
                .iter()
                .map(|path| {
                    path.as_str()
                        .map(|path| path.trim().to_owned())
                        .ok_or_else(|| outside(param, &path.to_string()))
                })
                .collect::<Result<_, _>>()?,
            Some(Value::String(paths)) => paths
                .split(',')
                .map(|path| path.trim().to_owned())
                .collect(),
            _ => Vec::new(),
        };
        for path in entries.iter().filter(|path| !path.is_empty()) {
            checkout_path(checkout, path).ok_or_else(|| outside(param, path))?;
        }
    }
    for param in HOST_PATH_PARAMS {
        if let Some(value) = params.get(*param) {
            if value.is_string() {
                let resolved = check(param, value)?;
                params.insert(
                    (*param).to_owned(),
                    json!(resolved.to_string_lossy().into_owned()),
                );
            }
        }
    }
    if REPO_PATH_METHODS.contains(&method)
        && !params.contains_key("repoPath")
        && !params.contains_key("gitRootId")
    {
        params.insert(
            "repoPath".into(),
            json!(checkout.to_string_lossy().into_owned()),
        );
    }
    if method == "git.agentCommit" {
        // The backend applies the workspace's commit policy before it asks;
        // the machine's own `git.autoCommit` setting never decides.
        params.insert("userRequested".into(), json!(true));
    }
    params.insert("workspaceId".into(), json!(record.as_str()));
    Ok(params)
}

/// `value` with whitespace trimmed when it is a string, as handlers read
/// single paths.
fn trimmed(value: &Value) -> Value {
    match value {
        Value::String(text) => Value::String(text.trim().to_owned()),
        other => other.clone(),
    }
}

/// What a reply must not show: the checkout's absolute paths (made
/// relative) and the hidden record's id (shown as the backend's workspace).
#[derive(Default)]
struct Hidden {
    /// The checkout as given and canonical, longest first.
    roots: Vec<String>,
    record: Option<String>,
    workspace: Option<String>,
}

impl Hidden {
    fn with_workspace(mut self, workspace: intent_core::WorkspaceId) -> Self {
        self.workspace = Some(workspace.0);
        self
    }

    /// A whole value: the checkout itself is `""`, a path in it is made
    /// relative, the record is the backend's workspace; anything else
    /// (another path, a sibling such as `<root>2/x`) is unchanged.
    fn whole(&self, text: &str) -> Option<String> {
        if let (Some(record), Some(workspace)) = (&self.record, &self.workspace) {
            if text == record {
                return Some(workspace.clone());
            }
        }
        for root in &self.roots {
            if text == root {
                return Some(String::new());
            }
            if let Some(rest) = text
                .strip_prefix(root.as_str())
                .and_then(|rest| rest.strip_prefix('/'))
            {
                return Some(rest.to_owned());
            }
        }
        None
    }

    /// An error message: each checkout path in it made relative (the
    /// checkout itself `.`), only where the root ends a path component,
    /// and the record shown as the backend's workspace.
    fn message(&self, text: &str) -> String {
        let mut text = text.to_owned();
        if let (Some(record), Some(workspace)) = (&self.record, &self.workspace) {
            text = text.replace(record.as_str(), workspace);
        }
        for root in &self.roots {
            let mut out = String::with_capacity(text.len());
            let mut rest = text.as_str();
            while let Some(at) = rest.find(root.as_str()) {
                out.push_str(&rest[..at]);
                let after = &rest[at + root.len()..];
                match after.chars().next() {
                    Some('/') => rest = &after[1..],
                    Some(next) if next.is_alphanumeric() || "-_.".contains(next) => {
                        out.push_str(root);
                        rest = after;
                    }
                    _ => {
                        out.push('.');
                        rest = after;
                    }
                }
            }
            out.push_str(rest);
            text = out;
        }
        text
    }

    /// `value` with every path-shaped string and key made relative, never
    /// touching the members of [`CONTENT_KEYS`].
    fn value(&self, value: Value) -> Value {
        match value {
            Value::String(text) => Value::String(self.whole(&text).unwrap_or(text)),
            Value::Array(items) => {
                Value::Array(items.into_iter().map(|item| self.value(item)).collect())
            }
            Value::Object(object) => Value::Object(
                object
                    .into_iter()
                    .map(|(key, item)| {
                        let item = if CONTENT_KEYS.contains(&key.as_str()) {
                            item
                        } else {
                            self.value(item)
                        };
                        (self.whole(&key).unwrap_or(key), item)
                    })
                    .collect(),
            ),
            other => other,
        }
    }
}

/// The checkout's absolute path as given and canonical, longest first: no
/// reply shows either.
fn shown_roots(checkout: &Path) -> Vec<String> {
    let mut roots = vec![checkout.to_string_lossy().into_owned()];
    if let Ok(canonical) = std::fs::canonicalize(checkout) {
        roots.push(canonical.to_string_lossy().into_owned());
    }
    roots.retain(|root| !root.is_empty() && root != "/");
    roots.sort_by_key(|root| std::cmp::Reverse(root.len()));
    roots.dedup();
    roots
}

/// The reply for `result`, without the checkout's absolute paths, and
/// [`HOME_REPLY_TOO_LARGE`] when it would not fit one frame.
fn fit_reply(id: LinkId, result: Result<Value, HomeCallError>, hidden: &Hidden) -> HomeCallReply {
    let reply = match result {
        Ok(result) => HomeCallReply::ok(id, hidden.value(result)),
        Err(error) => {
            let oversized = error
                .data
                .as_ref()
                .and_then(|data| data.get("code"))
                .and_then(Value::as_str)
                == Some("oversized-response");
            if oversized {
                return too_large(id, None);
            }
            HomeCallReply::err(
                id,
                error.code,
                hidden.message(&error.message),
                error.data.map(|data| hidden.value(data)),
            )
        }
    };
    match LinkFrame::HomeCallReply(reply.clone()).encode() {
        Ok(_) => reply,
        Err(_) => too_large(id, serde_json::to_vec(&reply).ok().map(|bytes| bytes.len())),
    }
}

fn too_large(id: LinkId, bytes: Option<usize>) -> HomeCallReply {
    let size = bytes.map_or_else(String::new, |bytes| format!(" ({bytes} bytes)"));
    HomeCallReply::err(
        id,
        INTERNAL_ERROR,
        format!(
            "{HOME_REPLY_TOO_LARGE}: the result{size} does not fit one {MAX_FRAME_BYTES}-byte frame; page it"
        ),
        Some(json!({ "code": HOME_REPLY_TOO_LARGE })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkout_paths_stay_inside_and_outside_git() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("work");
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::create_dir_all(root.join(".git")).unwrap();
        std::fs::create_dir_all(dir.path().join("elsewhere")).unwrap();
        assert_eq!(checkout_path(&root, ""), Some(root.clone()));
        assert_eq!(checkout_path(&root, "."), Some(root.clone()));
        assert_eq!(
            checkout_path(&root, "src/new/file.rs"),
            Some(root.join("src/new/file.rs"))
        );
        for refused in [
            "../elsewhere",
            "src/../../elsewhere",
            "/etc/passwd",
            ".git/config",
            "src/.GIT/x",
        ] {
            assert_eq!(checkout_path(&root, refused), None, "{refused}");
        }
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(dir.path().join("elsewhere"), root.join("out")).unwrap();
            std::os::unix::fs::symlink(root.join(".git"), root.join("git-dir")).unwrap();
            assert_eq!(checkout_path(&root, "out/file"), None);
            assert_eq!(checkout_path(&root, "out"), None);
            assert_eq!(checkout_path(&root, "git-dir/HEAD"), None);
        }
    }

    #[test]
    fn params_name_the_record_and_absolute_host_paths() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let record = intent_core::WorkspaceId::new_home_record();
        let params = |value: Value| value.as_object().cloned().unwrap();
        let pulled = checkout_params(
            "git.pull",
            params(json!({ "branchName": "main", "workspaceId": "ws-1" })),
            &root,
            &record,
        )
        .unwrap();
        assert_eq!(pulled["workspaceId"], record.as_str());
        assert_eq!(pulled["repoPath"], root.to_string_lossy().as_ref());
        let sub = checkout_params(
            "git.getBranches",
            params(json!({ "repoPath": "sub" })),
            &root,
            &record,
        )
        .unwrap();
        assert_eq!(sub["repoPath"], root.join("sub").to_string_lossy().as_ref());
        let refused = checkout_params(
            "git.stage",
            params(json!({ "paths": ["a.txt", "../x"] })),
            &root,
            &record,
        )
        .unwrap_err();
        assert_eq!(refused.code, INVALID_PARAMS);
        for paths in [json!("a.txt, ../x"), json!([" ../x"]), json!([1])] {
            assert!(
                checkout_params(
                    "file-tracking.stage",
                    params(json!({ "paths": paths })),
                    &root,
                    &record,
                )
                .is_err(),
                "{paths}"
            );
        }
        let committed = checkout_params(
            "git.agentCommit",
            params(json!({ "message": "m", "userRequested": false })),
            &root,
            &record,
        )
        .unwrap();
        assert_eq!(committed["userRequested"], true);
        assert!(checkout_params(
            "git.getBranches",
            params(json!({ "repoPath": root.to_string_lossy() })),
            &root,
            &record,
        )
        .is_err());
    }

    #[test]
    fn replies_never_show_the_checkout_path_or_the_record() {
        let hidden = Hidden {
            roots: vec!["/srv/homes/h1/work".to_owned()],
            record: Some("__home__:r1".to_owned()),
            workspace: Some("ws-1".to_owned()),
        };
        let shown = hidden.value(json!({
            "repoPath": "/srv/homes/h1/work",
            "files": ["/srv/homes/h1/work/src/a.rs", "b.rs", "/srv/homes/h1/work2/x"],
            "/srv/homes/h1/work/c.rs": 1,
            "workspaceId": "__home__:r1",
            "content": "built in /srv/homes/h1/work/build/out",
            "hunks": [{ "patch": "+/srv/homes/h1/work/x", "path": "/srv/homes/h1/work/x" }],
        }));
        assert_eq!(
            shown,
            json!({
                "repoPath": "",
                "files": ["src/a.rs", "b.rs", "/srv/homes/h1/work2/x"],
                "c.rs": 1,
                "workspaceId": "ws-1",
                "content": "built in /srv/homes/h1/work/build/out",
                "hunks": [{ "patch": "+/srv/homes/h1/work/x", "path": "x" }],
            })
        );
        assert_eq!(
            hidden.message(
                "fatal: /srv/homes/h1/work/d is outside /srv/homes/h1/work; see /srv/homes/h1/work2 for __home__:r1"
            ),
            "fatal: d is outside .; see /srv/homes/h1/work2 for ws-1"
        );
    }

    #[test]
    fn an_oversized_reply_is_refused_as_too_large() {
        let id = LinkId::nil();
        let big = "x".repeat(MAX_FRAME_BYTES);
        let reply = fit_reply(id, Ok(json!({ "content": big })), &Hidden::default());
        let error = reply.error.unwrap();
        assert_eq!(error.code, INTERNAL_ERROR);
        assert!(error.message.starts_with(HOME_REPLY_TOO_LARGE));
        assert_eq!(error.data, Some(json!({ "code": HOME_REPLY_TOO_LARGE })));
        let capped = fit_reply(
            id,
            Err(rpc_error(
                INTERNAL_ERROR,
                "response too large".into(),
                Some(json!({ "code": "oversized-response" })),
            )),
            &Hidden::default(),
        );
        assert_eq!(
            capped.error.unwrap().data,
            Some(json!({ "code": HOME_REPLY_TOO_LARGE }))
        );
    }
}

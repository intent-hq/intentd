//! `ws.pr.*` bindings (WSAPI-6).
//!
//! Thin wrappers over the [`WorkspaceApi`] explicit-addressing PR surface:
//! every method requires `repo` (an `"owner/name"` slug) and `prNumber` —
//! there is no active-PR or workspace-repo fallback on the agent-facing MCP
//! surface, so there is never ambiguity about which PR is addressed. The
//! binding peels + validates arguments (stable error strings, before any
//! trait call) and forwards the trait's `serde_json::Value` result — which
//! echoes the resolved `repo`/`prNumber` — unchanged.

use std::sync::Arc;

use intent_core::{WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, opt_str, req_i64, req_str};

pub(crate) const PRELUDE: &str = r#"
    globalThis.ws = globalThis.ws || {};
    ws.pr = {
        status: (repo, prNumber) => host({ method: 'pr.status', args: { repo, prNumber } }),
        snapshot: (repo, prNumber) =>
            host({ method: 'pr.snapshot', args: { repo, prNumber } }),
        merge: (repo, prNumber, options) =>
            host({ method: 'pr.merge', args: { repo, prNumber, ...(options || {}) } }),
        updateBranch: (repo, prNumber) =>
            host({ method: 'pr.updateBranch', args: { repo, prNumber } }),
        listReviewComments: (repo, prNumber, options) =>
            host({ method: 'pr.listReviewComments', args: { repo, prNumber, ...(options || {}) } }),
        replyToReviewComment: (repo, prNumber, commentId, body) =>
            host({ method: 'pr.replyToReviewComment', args: { repo, prNumber, commentId, body } }),
        resolveThread: (repo, prNumber, threadId, action) =>
            host({ method: 'pr.resolveThread', args: { repo, prNumber, threadId, action } }),
        listComments: (repo, prNumber, options) =>
            host({ method: 'pr.listComments', args: { repo, prNumber, ...(options || {}) } }),
        postComment: (repo, prNumber, body) =>
            host({ method: 'pr.postComment', args: { repo, prNumber, body } }),
    };
"#;

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "status" => status(api, ws, args).await,
        "snapshot" => snapshot(api, ws, args).await,
        "merge" => merge(api, ws, args).await,
        "updateBranch" => update_branch(api, ws, args).await,
        "listReviewComments" => list_review_comments(api, ws, args).await,
        "replyToReviewComment" => reply_to_review_comment(api, ws, args).await,
        "resolveThread" => resolve_thread(api, ws, args).await,
        "listComments" => list_comments(api, ws, args).await,
        "postComment" => post_comment(api, ws, args).await,
        other => Err(format!("host: unknown method `pr.{other}`")),
    }
}

/// Peel the required `repo` ("owner/name" string) + `prNumber` (positive
/// number) pair every `ws.pr.*` method takes. Validation errors surface
/// before any trait call; slug-shape validation lives in the engine.
fn req_repo_and_pr(args: &Value) -> Result<(String, u64), String> {
    let repo = match args.get("repo") {
        Some(Value::String(s)) if !s.trim().is_empty() => s.clone(),
        _ => return Err("repo is required and must be an \"owner/name\" string".to_string()),
    };
    let pr_number =
        req_i64(args, "prNumber").map_err(|_| "prNumber is required and must be a number")?;
    if pr_number <= 0 {
        return Err("prNumber is required and must be a number".to_string());
    }
    Ok((repo, pr_number as u64))
}

async fn status(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let (repo, pr_number) = req_repo_and_pr(args)?;
    api.pr_status(ws.clone(), repo, pr_number)
        .await
        .map_err(map_err)
}

async fn snapshot(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let (repo, pr_number) = req_repo_and_pr(args)?;
    api.pr_state(ws.clone(), repo, pr_number)
        .await
        .map_err(map_err)
}

async fn merge(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let (repo, pr_number) = req_repo_and_pr(args)?;
    let merge_method = match opt_str(args, "mergeMethod") {
        Some(m) if !matches!(m.as_str(), "merge" | "squash" | "rebase") => {
            return Err("mergeMethod must be one of: merge, squash, rebase".to_string())
        }
        m => m,
    };
    let commit_title = opt_str(args, "commitTitle");
    let commit_message = opt_str(args, "commitMessage");
    // Idempotency-wrapped in `intent-services`: pass the caller-supplied key
    // through when present, otherwise mint a UUID so agent-initiated retries
    // dedupe and the `with_idempotency` soft-launch warn never fires. Blank /
    // whitespace-only keys are treated as absent (parity with `comment.add`)
    // so an accidental empty string cannot collapse dedupe across unrelated
    // requests.
    let idempotency_key = opt_str(args, "idempotencyKey")
        .filter(|k| !k.trim().is_empty())
        .or_else(|| Some(uuid::Uuid::new_v4().to_string()));
    api.pr_merge(
        ws.clone(),
        repo,
        pr_number,
        merge_method,
        commit_title,
        commit_message,
        idempotency_key,
    )
    .await
    .map_err(map_err)
}

async fn update_branch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let (repo, pr_number) = req_repo_and_pr(args)?;
    api.pr_update_branch(ws.clone(), repo, pr_number)
        .await
        .map_err(map_err)
}

async fn list_review_comments(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let (repo, pr_number) = req_repo_and_pr(args)?;
    let path = opt_str(args, "path");
    let status = match opt_str(args, "status") {
        Some(s) if !matches!(s.as_str(), "unresolved" | "resolved" | "all") => {
            return Err("status must be one of: unresolved, resolved, all".to_string())
        }
        s => s,
    };
    api.pr_list_review_comments(ws.clone(), repo, pr_number, path, status)
        .await
        .map_err(map_err)
}

async fn reply_to_review_comment(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let (repo, pr_number) = req_repo_and_pr(args)?;
    let comment_id =
        req_i64(args, "commentId").map_err(|_| "commentId is required and must be a number")?;
    if comment_id < 0 {
        return Err("commentId is required and must be a number".to_string());
    }
    let body = req_str(args, "body").map_err(|_| "body is required and must be a string")?;
    if body.is_empty() {
        return Err("body is required and must be a string".to_string());
    }
    api.pr_reply_to_review_comment(ws.clone(), repo, pr_number, comment_id as u64, body)
        .await
        .map_err(map_err)
}

async fn resolve_thread(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let (repo, pr_number) = req_repo_and_pr(args)?;
    let thread_id =
        req_str(args, "threadId").map_err(|_| "threadId is required and must be a string")?;
    if thread_id.is_empty() {
        return Err("threadId is required and must be a string".to_string());
    }
    let action = match opt_str(args, "action") {
        Some(a) if !matches!(a.as_str(), "resolve" | "unresolve") => {
            return Err("action must be one of: resolve, unresolve".to_string())
        }
        a => a,
    };
    api.pr_resolve_thread(ws.clone(), repo, pr_number, thread_id, action)
        .await
        .map_err(map_err)
}

async fn list_comments(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let (repo, pr_number) = req_repo_and_pr(args)?;
    let count = args.get("count").and_then(Value::as_i64);
    api.pr_list_comments(ws.clone(), repo, pr_number, count)
        .await
        .map_err(map_err)
}

async fn post_comment(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let (repo, pr_number) = req_repo_and_pr(args)?;
    let body = req_str(args, "body").map_err(|_| "body is required and must be a string")?;
    if body.is_empty() {
        return Err("body is required and must be a string".to_string());
    }
    api.pr_post_comment(ws.clone(), repo, pr_number, body)
        .await
        .map_err(map_err)
}

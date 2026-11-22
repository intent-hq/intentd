//! The checkout a machine's git calls reach on the head: where a
//! workspace's agents work, the submodules pinned in it, and the pins a home
//! push keeps.

use std::path::PathBuf;

use intent_core::node_link::SubmodulePin;
use intent_core::remote_launch::RemotePlacementView;
use intent_core::{AgentId, Error, Result, Workspace};
use intent_git::hub::AgentRef;
use intent_git::node_git::AgentRemote;
use intent_git::ref_storage::{GitStorage, RefExpect};

/// The checkout a workspace's agents work in: its worktree, or the
/// repository it was opened on.
pub(crate) fn checkout_path(workspace: &Workspace) -> Option<PathBuf> {
    workspace
        .worktree_path
        .as_ref()
        .or(workspace.repository_path.as_ref())
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
}

/// The pins of the checkout `agent_id` works in on its machine: its own,
/// or its checkout owner's when it shares one. Empty for an agent without
/// pins or in a home checkout.
pub(crate) async fn checkout_pins(
    store: &intent_store::Store,
    agent_id: &AgentId,
) -> Result<Vec<SubmodulePin>> {
    let view_of = |summary: &intent_store::AgentSessionSummary| {
        RemotePlacementView::from_metadata(summary.metadata.as_ref())
    };
    let Some(view) = view_of(&store.get_agent_session_summary(agent_id).await?) else {
        return Ok(Vec::new());
    };
    if view.home_checkout.is_some() {
        return Ok(Vec::new());
    }
    let view = match &view.shared_checkout_of {
        Some(owner) => match view_of(&store.get_agent_session_summary(owner).await?) {
            Some(owner) => owner,
            None => return Ok(Vec::new()),
        },
        None => view,
    };
    Ok(view.pins().to_vec())
}

/// `home/rewritten/<sha>` of `remote`'s workspace.
fn rewritten_ref(remote: &AgentRemote, sha: &str) -> String {
    format!("{}rewritten/{sha}", remote.ref_prefix())
}

pub(crate) fn git_error(what: &str) -> impl Fn(git2::Error) -> Error + '_ {
    move |e| Error::Internal(format!("{what}: {e}"))
}

/// Before a home push: pin the current `home/head` as
/// `home/rewritten/<sha>` unless that pin exists. The sha of a pin made
/// here; `None` when there is nothing to pin or it was pinned already.
///
/// # Errors
///
/// When the repository cannot be opened or the pin cannot be written.
pub(crate) fn pin_home_head(
    git: &dyn GitStorage,
    repository: &std::path::Path,
    remote: &AgentRemote,
) -> Result<Option<String>> {
    let repo = git
        .open(repository)
        .map_err(git_error("open the workspace repository"))?;
    let Ok(head) = repo.find_ref(&remote.ref_name(AgentRef::Head)) else {
        return Ok(None);
    };
    let Some(oid) = head else {
        return Ok(None);
    };
    let sha = oid.to_string();
    let pin = rewritten_ref(remote, &sha);
    if repo.find_ref(&pin).is_ok() {
        return Ok(None);
    }
    repo.update_ref(
        &pin,
        RefExpect::Absent,
        Some(oid),
        "pin the home's head before a push",
    )
    .map_err(git_error("pin the home's head"))?;
    Ok(Some(sha))
}

/// After a home push: drop each pin of `pins` the new `home/head` still
/// contains (the push only added commits), and move the orchestrator's
/// worktree, its read-only mirror, to the new `home/head`.
///
/// # Errors
///
/// When the repository cannot be opened, a pin cannot be dropped or the
/// worktree cannot be moved.
pub(crate) fn settle_home_push(
    git: &dyn GitStorage,
    checkout: &std::path::Path,
    remote: &AgentRemote,
    pins: &[String],
) -> Result<()> {
    let refs = git
        .open(checkout)
        .map_err(git_error("open the workspace repository"))?;
    let Some(head) = refs
        .find_ref(&remote.ref_name(AgentRef::Head))
        .ok()
        .flatten()
    else {
        return Ok(());
    };
    for sha in pins {
        let Ok(old) = git2::Oid::from_str(sha) else {
            continue;
        };
        let kept = old == head || refs.is_descendant_of(head, old).unwrap_or(false);
        if kept {
            let pin = rewritten_ref(remote, sha);
            if let Ok(target) = refs.find_ref(&pin) {
                if target == Some(old) {
                    refs.update_ref(&pin, RefExpect::Is(old), None, "drop a rewritten pin")
                        .map_err(git_error("drop a rewritten pin"))?;
                }
            }
        }
    }
    // The worktree is the orchestrator's mirror, not the home's refs. With
    // refs kept elsewhere than this repository, `head` must be fetched into
    // it before the reset.
    let repo =
        git2::Repository::open(checkout).map_err(git_error("open the workspace repository"))?;
    let at = repo.head().ok().and_then(|head| head.target());
    if at != Some(head) {
        let commit = repo
            .find_object(head, Some(git2::ObjectType::Commit))
            .map_err(git_error("read the home's head"))?;
        repo.reset(&commit, git2::ResetType::Hard, None)
            .map_err(git_error("move the mirror to the home's head"))?;
    }
    Ok(())
}

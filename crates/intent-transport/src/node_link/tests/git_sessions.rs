//! Git sessions on a real git connection. Git-level confinement and the
//! helper are covered by intent-git's `node_git` tests and intentd's
//! `e2e_node_git`.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use intent_core::{BoxFuture, Error, Result};
use intent_git::node_git::{AgentRemote, GitService};

use super::*;

struct AllowAgent {
    repository: PathBuf,
}

impl NodeGitAccess for AllowAgent {
    fn repository<'a>(
        &'a self,
        binding: &'a LeaseBinding,
        remote: &'a AgentRemote,
        _service: GitService,
    ) -> BoxFuture<'a, Result<PathBuf>> {
        Box::pin(async move {
            binding.ensure_current(&super::binding(1))?;
            if remote.agent_id() == "agent-1" {
                Ok(self.repository.clone())
            } else if remote.agent_id() == "agent-gone" {
                // A final stop whose workspace repository was removed.
                Err(Error::Internal(format!(
                    "{}: the workspace's repository is gone",
                    git_codes::UNAVAILABLE
                )))
            } else if remote.agent_id() == "agent-stale" {
                Err(Error::InvalidParams("node-link-stale".into()))
            } else {
                Err(Error::Forbidden("not on this lease".into()))
            }
        })
    }
}

fn repository(dir: &Path) -> PathBuf {
    let path = dir.join("head");
    let status = Command::new("git")
        .args(["init", "--quiet"])
        .arg(&path)
        .status()
        .unwrap();
    assert!(status.success());
    path
}

/// A node with head serving git sessions for `agent-1`, and the node's
/// sessions attached to the git connection.
async fn connected() -> (
    Node,
    DialedLink,
    NodeGitSessions,
    tokio::task::JoinHandle<()>,
) {
    let mut node = Node::start(token("t")).await;
    let link = dial(&node, 1).await;
    let _ = node.accept_link().await;
    let head_git = dial_git(&node.endpoint(), &binding(1)).await.unwrap();
    let node_git = node.accept_git().await;
    let access = AllowAgent {
        repository: repository(node.dir.path()),
    };
    let serving = tokio::spawn(serve_git(head_git, Arc::new(access)));
    let sessions = NodeGitSessions::new();
    sessions.attach(node_git);
    (node, link, sessions, serving)
}

fn remote(agent: &str) -> AgentRemote {
    AgentRemote::new("ws-1", agent).unwrap()
}

fn session_code(error: &Error) -> String {
    error.to_string()
}

/// Nothing attaches within [`ATTACH_WAIT`]: the session is unavailable.
#[tokio::test(start_paused = true)]
async fn open_without_a_connection_is_unavailable() {
    let sessions = NodeGitSessions::new();
    let started = tokio::time::Instant::now();
    let error = sessions
        .open(GitService::UploadPack, &remote("agent-1"))
        .await
        .unwrap_err();
    assert!(
        session_code(&error).contains(git_codes::UNAVAILABLE),
        "{error}"
    );
    assert!(started.elapsed() >= ATTACH_WAIT, "{:?}", started.elapsed());
}

/// intent-hq/intent#8383: head binds the link and launches the agent
/// before the node has attached the git connection, so the checkout opens
/// its session first. It waits for the connection instead of failing.
#[tokio::test]
async fn open_waits_for_a_connection_that_attaches_after_it() {
    let mut node = Node::start(token("t")).await;
    let _link = dial(&node, 1).await;
    let _ = node.accept_link().await;
    let sessions = NodeGitSessions::new();
    let opening = tokio::spawn({
        let sessions = sessions.clone();
        async move {
            sessions
                .open(GitService::UploadPack, &remote("agent-1"))
                .await
        }
    });
    tokio::task::yield_now().await;
    assert!(!opening.is_finished(), "open waits for a connection");
    let head_git = dial_git(&node.endpoint(), &binding(1)).await.unwrap();
    let node_git = node.accept_git().await;
    let access = AllowAgent {
        repository: repository(node.dir.path()),
    };
    let _serving = tokio::spawn(serve_git(head_git, Arc::new(access)));
    sessions.attach(node_git);
    let session = tokio::time::timeout(wait(), opening)
        .await
        .unwrap()
        .unwrap();
    assert!(session.is_ok(), "{:?}", session.err());
}

#[tokio::test]
async fn upload_pack_session_streams_and_finishes() {
    let (_node, _link, sessions, _serving) = connected().await;
    let mut session = tokio::time::timeout(
        wait(),
        sessions.open(GitService::UploadPack, &remote("agent-1")),
    )
    .await
    .unwrap()
    .unwrap();
    // An empty repository advertises nothing; a flush ends the session.
    session.send(b"0000").await.unwrap();
    session.finish().await.unwrap();
    let mut output = Vec::new();
    while let Some(bytes) = tokio::time::timeout(wait(), session.recv())
        .await
        .unwrap()
        .unwrap()
    {
        output.extend(bytes);
    }
    assert!(
        output.ends_with(b"0000"),
        "{}",
        String::from_utf8_lossy(&output)
    );
}

#[tokio::test]
async fn sessions_for_agents_off_the_lease_are_denied() {
    let (_node, _link, sessions, _serving) = connected().await;
    let error = tokio::time::timeout(
        wait(),
        sessions.open(GitService::ReceivePack, &remote("agent-2")),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(session_code(&error).contains(git_codes::DENIED), "{error}");
}

/// A refusal that carries its own session code keeps it: a final stop's
/// push whose repository is gone is `node-git-unavailable`, not denied.
#[tokio::test]
async fn a_missing_repository_is_unavailable_not_denied() {
    let (_node, _link, sessions, _serving) = connected().await;
    let error = tokio::time::timeout(
        wait(),
        sessions.open(GitService::ReceivePack, &remote("agent-gone")),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(
        session_code(&error).contains(git_codes::UNAVAILABLE),
        "{error}"
    );
}

/// A binding that is no longer current is refused as `node-git-denied`.
#[tokio::test]
async fn a_stale_binding_is_denied() {
    let (_node, _link, sessions, _serving) = connected().await;
    let error = tokio::time::timeout(
        wait(),
        sessions.open(GitService::ReceivePack, &remote("agent-stale")),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(session_code(&error).contains(git_codes::DENIED), "{error}");
}

#[tokio::test]
async fn head_caps_concurrent_sessions() {
    let (_node, _link, sessions, _serving) = connected().await;
    let mut open = Vec::new();
    for _ in 0..MAX_SESSIONS {
        // upload-pack waits for the client after advertising, so these
        // sessions stay open.
        open.push(
            tokio::time::timeout(
                wait(),
                sessions.open(GitService::UploadPack, &remote("agent-1")),
            )
            .await
            .unwrap()
            .unwrap(),
        );
    }
    let error = tokio::time::timeout(
        wait(),
        sessions.open(GitService::UploadPack, &remote("agent-1")),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(session_code(&error).contains(git_codes::BUSY), "{error}");
    // Dropping a session frees its slot.
    drop(open.pop());
    tokio::time::timeout(wait(), async {
        loop {
            if let Ok(session) = sessions
                .open(GitService::UploadPack, &remote("agent-1"))
                .await
            {
                break session;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

/// Give `repository` a commit and `count` refs under agent-1's prefix, so
/// its upload-pack advertisement spans many messages.
fn many_agent_refs(repository: &Path, count: usize) {
    let git = |args: &[&str]| {
        let status = Command::new("git")
            .arg("-C")
            .arg(repository)
            .args(args)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?}");
    };
    git(&[
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@t",
        "commit",
        "--quiet",
        "--allow-empty",
        "-m",
        "base",
    ]);
    let head = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    let head = String::from_utf8(head.stdout).unwrap();
    let mut input = String::new();
    for n in 0..count {
        input += "create refs/intent/ws/ws-1/agents/agent-1/r";
        input += &n.to_string();
        input += " ";
        input += head.trim();
        input += "\n";
    }
    let mut child = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(["update-ref", "--stdin"])
        .stdin(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    {
        use std::io::Write as _;
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
    }
    assert!(child.wait().unwrap().success());
}

#[tokio::test]
async fn replacing_an_idle_connection_closes_it() {
    let (node, _link, sessions, serving) = connected().await;
    let mut node = node;
    let _second_head = dial_git(&node.endpoint(), &binding(1)).await.unwrap();
    sessions.attach(node.accept_git().await);
    // Head's server for the first connection ends: the node closed it.
    tokio::time::timeout(wait(), serving)
        .await
        .unwrap()
        .unwrap();
    assert!(sessions.is_connected());
}

#[tokio::test]
async fn replacing_a_connection_ends_a_session_with_a_full_event_queue() {
    let (node, _link, sessions, serving) = connected().await;
    let mut node = node;
    many_agent_refs(&node.dir.path().join("head"), 20_000);
    // Never read: head's advertisement fills this session's event queue and
    // blocks the first connection's reader.
    let mut stalled = tokio::time::timeout(
        wait(),
        sessions.open(GitService::UploadPack, &remote("agent-1")),
    )
    .await
    .unwrap()
    .unwrap();
    // Wait until the shared reader is stuck on the stalled session's full
    // queue: it then can't deliver another session's acceptance.
    tokio::time::timeout(wait(), async {
        loop {
            let probe = tokio::time::timeout(
                intentd_test_support::test_timeout(Duration::from_millis(300)),
                sessions.open(GitService::UploadPack, &remote("agent-1")),
            )
            .await;
            if probe.is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let _second_head = dial_git(&node.endpoint(), &binding(1)).await.unwrap();
    sessions.attach(node.accept_git().await);
    tokio::time::timeout(wait(), serving)
        .await
        .unwrap()
        .unwrap();
    let error = tokio::time::timeout(wait(), async {
        loop {
            match stalled.recv().await {
                Ok(Some(_)) => {}
                Ok(None) => panic!("the replaced session must not end cleanly"),
                Err(error) => break error,
            }
        }
    })
    .await
    .unwrap();
    assert!(
        session_code(&error).contains(git_codes::UNAVAILABLE),
        "{error}"
    );
}

#[tokio::test]
async fn link_loss_ends_open_sessions() {
    let (_node, _link, sessions, serving) = connected().await;
    let mut session = tokio::time::timeout(
        wait(),
        sessions.open(GitService::UploadPack, &remote("agent-1")),
    )
    .await
    .unwrap()
    .unwrap();
    serving.abort();
    let error = tokio::time::timeout(wait(), async {
        loop {
            match session.recv().await {
                Ok(Some(_)) => {}
                Ok(None) => panic!("the session must not end cleanly"),
                Err(error) => break error,
            }
        }
    })
    .await
    .unwrap();
    assert!(
        session_code(&error).contains(git_codes::UNAVAILABLE),
        "{error}"
    );
    assert!(tokio::time::timeout(wait(), async {
        while sessions.is_connected() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok());
}

/// A one-commit repository in `dir` and that commit's id.
#[cfg(unix)]
fn source_commit(dir: &Path) -> (PathBuf, String) {
    let source = dir.join("source");
    let git = |args: &[&str]| {
        let out = Command::new("git")
            .arg("-C")
            .arg(&source)
            .args(["-c", "user.name=t", "-c", "user.email=t@t"])
            .args(args)
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?}: {out:?}");
        String::from_utf8(out.stdout).unwrap().trim().to_owned()
    };
    std::fs::create_dir_all(&source).unwrap();
    git(&["init", "--quiet"]);
    git(&["commit", "--quiet", "--allow-empty", "-m", "work"]);
    let commit = git(&["rev-parse", "HEAD"]);
    (source, commit)
}

/// The receive-pack request creating `ref_name` at `commit`: its command,
/// a flush and a pack of `commit` from `source`.
#[cfg(unix)]
fn push_request(source: &Path, commit: &str, ref_name: &str) -> Vec<u8> {
    let zero = "0".repeat(commit.len());
    let line = format!("{zero} {commit} {ref_name}\0report-status\n");
    let mut request = format!("{:04x}{line}0000", line.len() + 4).into_bytes();
    let mut pack = Command::new("git")
        .arg("-C")
        .arg(source)
        .args(["pack-objects", "--quiet", "--stdout", "--revs"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    {
        use std::io::Write as _;
        let mut stdin = pack.stdin.take().unwrap();
        writeln!(stdin, "{commit}").unwrap();
    }
    let out = pack.wait_with_output().unwrap();
    assert!(out.status.success());
    request.extend(out.stdout);
    request
}

/// Head's receive hooks in `dir`, plus a `reference-transaction` hook that
/// runs while receive-pack holds the ref locks: it creates the returned
/// `locked` file, then holds the locks until `release` exists.
#[cfg(unix)]
fn hold_ref_updates(dir: &Path) -> (PathBuf, PathBuf) {
    use std::os::unix::fs::PermissionsExt as _;
    intent_git::node_git::configure_receive_hooks(dir).unwrap();
    let (locked, release) = (dir.join("locked"), dir.join("release"));
    let hook = dir.join("node-git-hooks").join("reference-transaction");
    std::fs::write(
        &hook,
        format!(
            "#!/bin/sh\ncat >/dev/null\n[ \"$1\" = prepared ] || exit 0\n: > '{}'\n\
             n=0\nwhile [ ! -e '{}' ] && [ $n -lt 600 ]; do sleep 0.05; n=$((n+1)); done\n",
            locked.display(),
            release.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o700)).unwrap();
    (locked, release)
}

/// Push a new commit to agent-1's `head` over `sessions` and wait until
/// head's receive-pack holds its lock (see [`hold_ref_updates`]). Returns
/// the open session, the ref and the commit.
#[cfg(unix)]
async fn push_held_in_ref_update(
    dir: &Path,
    sessions: &NodeGitSessions,
    locked: &Path,
) -> (GitSession, String, String) {
    let (source, commit) = source_commit(dir);
    let ref_name = format!("{}head", remote("agent-1").ref_prefix());
    let session = tokio::time::timeout(
        wait(),
        sessions.open(GitService::ReceivePack, &remote("agent-1")),
    )
    .await
    .unwrap()
    .unwrap();
    session
        .send(&push_request(&source, &commit, &ref_name))
        .await
        .unwrap();
    session.finish().await.unwrap();
    tokio::time::timeout(wait(), async {
        while !locked.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("receive-pack reaches the ref update");
    (session, ref_name, commit)
}

/// A link that drops while head's receive-pack holds the pushed ref's lock
/// (a cloud sandbox paused during the agent's turn-end push) lets the push
/// finish: the ref lands and no `.lock` is left behind. Killing the
/// receive-pack there left `head.lock` for good, so every later write to
/// the agent's `head` failed.
#[cfg(unix)]
#[tokio::test]
async fn link_loss_during_a_ref_update_leaves_no_ref_lock() {
    let (node, _link, sessions, serving) = connected().await;
    let dir = node.dir.path();
    let (locked, release) = hold_ref_updates(dir);
    let (_session, ref_name, commit) = push_held_in_ref_update(dir, &sessions, &locked).await;

    serving.abort();
    let _ = serving.await;
    std::fs::write(&release, "").unwrap();

    let repository = dir.join("head");
    let lock = repository.join(".git").join(format!("{ref_name}.lock"));
    let landed = tokio::time::timeout(wait(), async {
        loop {
            let out = Command::new("git")
                .arg("-C")
                .arg(&repository)
                .args(["rev-parse", "--verify", "--quiet", &ref_name])
                .output()
                .unwrap();
            if String::from_utf8_lossy(&out.stdout).trim() == commit && !lock.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        landed.is_ok(),
        "the push did not land; lock left behind: {}",
        lock.exists()
    );
}

/// A push still running after its session closed keeps its session slot
/// until it exits, so a node cannot pile up services past
/// [`MAX_SESSIONS`] by closing sessions early.
#[cfg(unix)]
#[tokio::test]
async fn a_push_that_outlives_its_session_keeps_its_slot() {
    let (node, _link, sessions, _serving) = connected().await;
    let dir = node.dir.path();
    let (locked, release) = hold_ref_updates(dir);
    let (session, _, _) = push_held_in_ref_update(dir, &sessions, &locked).await;
    drop(session);

    let mut open = Vec::new();
    for _ in 1..MAX_SESSIONS {
        open.push(
            tokio::time::timeout(
                wait(),
                sessions.open(GitService::UploadPack, &remote("agent-1")),
            )
            .await
            .unwrap()
            .unwrap(),
        );
    }
    let error = tokio::time::timeout(
        wait(),
        sessions.open(GitService::UploadPack, &remote("agent-1")),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(session_code(&error).contains(git_codes::BUSY), "{error}");

    std::fs::write(&release, "").unwrap();
    tokio::time::timeout(wait(), async {
        loop {
            if let Ok(session) = sessions
                .open(GitService::UploadPack, &remote("agent-1"))
                .await
            {
                break session;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the slot frees once the push exits");
}

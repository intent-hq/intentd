//! Workspace homes on the machine (nodeProtocol 19) over a real link: the
//! opt-out, `homeOpen`, the backup journal, `homePush`, lease checks and
//! the bind-time sweep. The home checkouts are plain directories whose
//! `HEAD` is a file ([`checkout::TEST_HOME_HEAD`]); the git side of a home
//! is covered by the e2e tests.

use base64::Engine as _;
use intent_core::node_link::{
    HomeOpen, HomeOpenOutcome, HomePush, HomePushOutcome, HomeRef, PreparedCheckout, PtyAck,
    PtyExited, PtyInput, PtyKill, PtyMode, PtyOpen, PtyOutput, PtyResize, PtyStream, RecordOwner,
    SetupState, HOMES_PROTOCOL, MAX_PTY_OUTPUT_BYTES, NODE_HOME_UNKNOWN, NODE_HOME_UNSUPPORTED,
    PTY_UNACKED_BYTES,
};
use intent_core::HomeId;

use super::*;

/// [`FixedLaunch`] with `remoteAgents.allowHomes` (and the readiness gate)
/// answered by `allow`.
struct HomesLaunch(bool);

impl MachineLaunch for HomesLaunch {
    fn resolve(&self, _provider: &str) -> Result<ProviderLaunch, String> {
        Err("not installed".to_owned())
    }
    fn policy(&self) -> PermissionPolicy {
        PermissionPolicy::AllowAll
    }
    fn allows_homes(&self) -> bool {
        self.0
    }
}

async fn start(dir: &std::path::Path, allow: bool) -> Running {
    Running::start_with_launch(
        dir,
        MachineApi::new(),
        CountAdmission::new(4),
        Arc::new(HomesLaunch(allow)),
        |storage, _cache| checkout::PlainCheckouts::new(storage),
    )
    .await
}

fn home(id: &str) -> HomeRef {
    HomeRef {
        workspace_id: WorkspaceId::from("ws-1"),
        home_id: HomeId::from(id),
    }
}

fn open(home: &HomeRef) -> LinkFrame {
    LinkFrame::HomeOpen(HomeOpen {
        request_id: intent_core::node_link::LinkId::new_v4(),
        workspace_id: home.workspace_id.clone(),
        home_id: home.home_id.clone(),
        base_commit: "0".repeat(40),
        repo_key: "repo".into(),
        checkout: PreparedCheckout::default(),
        restore: None,
    })
}

fn push(home: &HomeRef) -> LinkFrame {
    LinkFrame::HomePush(HomePush {
        request_id: intent_core::node_link::LinkId::new_v4(),
        workspace_id: home.workspace_id.clone(),
        home_id: home.home_id.clone(),
    })
}

fn leasing(generation: u64, homes: Option<Vec<HomeRef>>) -> LinkHello {
    let mut hello = hello(generation, "lease-1", 0);
    hello.lease_homes = homes;
    hello
}

/// The next frame `pick` takes, skipping the others.
async fn next_of<T>(dialed: &mut DialedLink, pick: impl Fn(LinkFrame) -> Option<T>) -> T {
    loop {
        let frame = next_frame(dialed).await.expect("the link closed");
        if let Some(found) = pick(frame) {
            return found;
        }
    }
}

/// The home's next `homePushed` record: its `head`.
async fn next_pushed(dialed: &mut DialedLink, home: &HomeRef) -> String {
    next_of(dialed, |frame| match frame {
        LinkFrame::Journal(JournalRecord {
            owner: RecordOwner::Home(id),
            workspace_id,
            body: JournalBody::HomePushed { head, .. },
            ..
        }) if id == home.home_id && workspace_id == home.workspace_id => head,
        _ => None,
    })
    .await
}

fn worktree(dir: &std::path::Path, home: &HomeRef) -> std::path::PathBuf {
    dir.join("executor")
        .read_dir()
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path().join("homes").join(&home.home_id.0))
        .find(|path| path.is_dir())
        .unwrap_or_else(|| panic!("no directory for home {}", home.home_id))
        .join("work")
}

/// Past the replay of the records an earlier link left unacknowledged.
async fn replayed(dialed: &mut DialedLink) {
    next_of(dialed, |frame| {
        matches!(frame, LinkFrame::ReplayComplete { .. }).then_some(())
    })
    .await;
}

async fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + wait();
    while !done() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// With `remoteAgents.allowHomes` off (or this build's gate closed) the
/// machine says so in its capabilities and refuses every `homeOpen`.
#[tokio::test]
async fn a_machine_that_may_not_hold_homes_reports_it_and_refuses_home_open() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), false).await;
    let mut dialed = dial_link(&node.endpoint, leasing(1, Some(Vec::new())))
        .await
        .unwrap();
    assert_eq!(dialed.welcome.protocol, HOMES_PROTOCOL);
    assert_eq!(dialed.welcome.capabilities.homes, Some(false));
    replay_complete(&mut dialed).await;
    dialed.link.send(&open(&home("h1"))).await.unwrap();
    let reply = next_of(&mut dialed, |frame| match frame {
        LinkFrame::HomeOpenReply(reply) => Some(reply),
        _ => None,
    })
    .await;
    assert_eq!(reply.outcome, HomeOpenOutcome::Failed);
    assert!(
        reply
            .error
            .as_deref()
            .is_some_and(|e| e.starts_with(NODE_HOME_UNSUPPORTED)),
        "{reply:?}"
    );
    node.stop().await;
}

/// A home opens once: its setup result and every backup push are
/// journaled for the home (not an agent), a repeated `homeOpen` answers
/// `ready` without a second checkout, a commit is pushed by the watcher,
/// and `homePush` answers with the pushed refs.
#[tokio::test]
async fn a_home_opens_once_and_is_backed_up_by_home() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), true).await;
    let h1 = home("h1");
    let mut dialed = dial_link(&node.endpoint, leasing(1, Some(Vec::new())))
        .await
        .unwrap();
    assert_eq!(dialed.welcome.capabilities.homes, None);
    replay_complete(&mut dialed).await;

    dialed.link.send(&open(&h1)).await.unwrap();
    let reply = next_of(&mut dialed, |frame| match frame {
        LinkFrame::HomeOpenReply(reply) => Some(reply),
        _ => None,
    })
    .await;
    assert_eq!(
        (reply.outcome, &reply.home_id),
        (HomeOpenOutcome::Ready, &h1.home_id)
    );
    let setup = next_of(&mut dialed, |frame| match frame {
        LinkFrame::Journal(record) => match (&record.owner, &record.body) {
            (RecordOwner::Home(_), JournalBody::SetupFinished { state, .. }) => {
                Some((record.owner.clone(), *state))
            }
            _ => None,
        },
        _ => None,
    })
    .await;
    assert_eq!(
        setup,
        (RecordOwner::Home(h1.home_id.clone()), SetupState::Skipped)
    );
    let zero = "0".repeat(40);
    assert_eq!(next_pushed(&mut dialed, &h1).await, zero);

    dialed.link.send(&open(&h1)).await.unwrap();
    let again = next_of(&mut dialed, |frame| match frame {
        LinkFrame::HomeOpenReply(reply) => Some(reply),
        _ => None,
    })
    .await;
    assert_eq!(again.outcome, HomeOpenOutcome::Ready);
    let made = worktree(dir.path(), &h1).with_file_name(checkout::TEST_HOME_MADE);
    assert_eq!(
        std::fs::read_to_string(&made).unwrap(),
        "+",
        "a repeated homeOpen makes no second checkout"
    );

    // The same homeId named for another workspace is not this home.
    let elsewhere = HomeRef {
        workspace_id: WorkspaceId::from("ws-2"),
        home_id: h1.home_id.clone(),
    };
    dialed.link.send(&open(&elsewhere)).await.unwrap();
    let refused = next_of(&mut dialed, |frame| match frame {
        LinkFrame::HomeOpenReply(reply) => Some(reply),
        _ => None,
    })
    .await;
    assert_eq!(refused.outcome, HomeOpenOutcome::Failed);
    assert!(
        refused
            .error
            .as_deref()
            .is_some_and(|e| e.starts_with(NODE_HOME_UNKNOWN)),
        "{refused:?}"
    );

    let commit = "1".repeat(40);
    std::fs::write(
        worktree(dir.path(), &h1).join(checkout::TEST_HOME_HEAD),
        &commit,
    )
    .unwrap();
    assert_eq!(next_pushed(&mut dialed, &h1).await, commit);

    dialed.link.send(&push(&h1)).await.unwrap();
    let pushed = next_of(&mut dialed, |frame| match frame {
        LinkFrame::HomePushReply(reply) => Some(reply),
        _ => None,
    })
    .await;
    assert_eq!(pushed.outcome, HomePushOutcome::Pushed, "{pushed:?}");
    assert_eq!(pushed.head.as_deref(), Some(commit.as_str()));
    node.stop().await;
}

/// A home frame for a `homeId` the machine does not hold, or one of another
/// workspace, is refused with `node-home-unknown`.
#[tokio::test]
async fn home_push_for_an_unknown_home_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), true).await;
    let mut dialed = dial_link(&node.endpoint, leasing(1, Some(Vec::new())))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    dialed.link.send(&open(&home("h1"))).await.unwrap();
    next_of(&mut dialed, |frame| match frame {
        LinkFrame::HomeOpenReply(reply) => Some(reply),
        _ => None,
    })
    .await;
    let elsewhere = HomeRef {
        workspace_id: WorkspaceId::from("ws-2"),
        home_id: HomeId::from("h1"),
    };
    for unknown in [home("h2"), elsewhere] {
        dialed.link.send(&push(&unknown)).await.unwrap();
        let reply = next_of(&mut dialed, |frame| match frame {
            LinkFrame::HomePushReply(reply) => Some(reply),
            _ => None,
        })
        .await;
        assert_eq!(reply.outcome, HomePushOutcome::Failed);
        assert!(
            reply
                .error
                .as_deref()
                .is_some_and(|e| e.starts_with(NODE_HOME_UNKNOWN)),
            "{reply:?}"
        );
    }
    node.stop().await;
}

/// The bind-time sweep removes the home checkouts missing from the
/// hello's `leaseHomes` and keeps the listed ones; a hello without
/// `leaseHomes` (an older backend) removes none.
#[tokio::test]
async fn the_bind_sweep_keeps_exactly_the_leased_homes() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), true).await;
    let (kept, dropped) = (home("kept"), home("dropped"));
    let mut dialed = dial_link(&node.endpoint, leasing(1, Some(Vec::new())))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    for h in [&kept, &dropped] {
        dialed.link.send(&open(h)).await.unwrap();
        next_of(&mut dialed, |frame| match frame {
            LinkFrame::HomeOpenReply(reply) => Some(reply),
            _ => None,
        })
        .await;
    }
    let kept_dir = worktree(dir.path(), &kept);
    let dropped_dir = worktree(dir.path(), &dropped);
    drop(dialed);

    // Open homes are never swept: a new bind with no `leaseHomes` keeps
    // both, and the restarted machine below drops them from memory.
    let node_api = node.stop().await;
    let node = Running::start_with_launch(
        dir.path(),
        node_api,
        CountAdmission::new(4),
        Arc::new(HomesLaunch(true)),
        |storage, _cache| checkout::PlainCheckouts::new(storage),
    )
    .await;
    let mut dialed = dial_link(&node.endpoint, leasing(2, None)).await.unwrap();
    replayed(&mut dialed).await;
    assert!(kept_dir.is_dir() && dropped_dir.is_dir());
    drop(dialed);

    let mut dialed = dial_link(&node.endpoint, leasing(3, Some(vec![kept.clone()])))
        .await
        .unwrap();
    replayed(&mut dialed).await;
    until("the unleased home is swept", || !dropped_dir.exists()).await;
    assert!(kept_dir.is_dir());
    // The leased home reopens after the restart (on its own task, after
    // the sweep): it then answers pushes.
    let deadline = tokio::time::Instant::now() + wait();
    loop {
        dialed.link.send(&push(&kept)).await.unwrap();
        let reply = next_of(&mut dialed, |frame| match frame {
            LinkFrame::HomePushReply(reply) => Some(reply),
            _ => None,
        })
        .await;
        if reply.outcome == HomePushOutcome::Pushed {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the leased home never reopened: {reply:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    node.stop().await;
}

/// After a restart, a `homeOpen` the backend resends right after binding
/// races the reopen of the leased home: the home is opened once, so one
/// watcher backs it up and each commit is journaled once.
#[tokio::test]
async fn a_resent_home_open_after_a_restart_opens_the_home_once() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), true).await;
    let h1 = home("h1");
    let mut dialed = dial_link(&node.endpoint, leasing(1, Some(Vec::new())))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    dialed.link.send(&open(&h1)).await.unwrap();
    next_of(&mut dialed, |frame| match frame {
        LinkFrame::HomeOpenReply(reply) => Some(reply),
        _ => None,
    })
    .await;
    let head_file = worktree(dir.path(), &h1).join(checkout::TEST_HOME_HEAD);
    drop(dialed);
    let api = node.stop().await;

    let node = Running::start_with_launch(
        dir.path(),
        api,
        CountAdmission::new(4),
        Arc::new(HomesLaunch(true)),
        |storage, _cache| checkout::PlainCheckouts::new(storage),
    )
    .await;
    let mut dialed = dial_link(&node.endpoint, leasing(2, Some(vec![h1.clone()])))
        .await
        .unwrap();
    dialed.link.send(&open(&h1)).await.unwrap();
    let reply = next_of(&mut dialed, |frame| match frame {
        LinkFrame::HomeOpenReply(reply) => Some(reply),
        _ => None,
    })
    .await;
    assert_eq!(reply.outcome, HomeOpenOutcome::Ready, "{reply:?}");

    let (first, second) = ("1".repeat(40), "2".repeat(40));
    std::fs::write(&head_file, &first).unwrap();
    let mut heads = Vec::new();
    loop {
        let head = next_pushed(&mut dialed, &h1).await;
        if head == first && !heads.contains(&first) {
            std::fs::write(&head_file, &second).unwrap();
        }
        let done = head == second;
        heads.push(head);
        if done {
            break;
        }
    }
    assert_eq!(
        heads.iter().filter(|head| **head == first).count(),
        1,
        "one watcher: {heads:?}"
    );
    node.stop().await;
}

// Terminals on the home (node-link.md § Terminals on the home).

fn session_hello(generation: u64, session: &str, homes: Vec<HomeRef>) -> LinkHello {
    let mut hello = leasing(generation, Some(homes));
    hello.head_session = Some(session.to_owned());
    hello
}

fn pty_open(home: &HomeRef, terminal_id: &str, mode: PtyMode, command: Option<&str>) -> LinkFrame {
    LinkFrame::PtyOpen(PtyOpen {
        terminal_id: terminal_id.into(),
        workspace_id: home.workspace_id.clone(),
        home_id: home.home_id.clone(),
        mode,
        cwd: None,
        command: command.map(str::to_owned),
        env: Vec::new(),
        cols: Some(80),
        rows: Some(24),
    })
}

fn b64(text: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(text)
}

fn unb64(data: &str) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD
        .decode(data)
        .unwrap()
}

/// What a terminal sent until `stop` holds of the text: the output frames
/// (in arrival order) and the exit, if it came.
struct Seen {
    frames: Vec<PtyOutput>,
    exited: Option<PtyExited>,
}

impl Seen {
    fn text(&self) -> String {
        let mut bytes = Vec::new();
        for frame in &self.frames {
            bytes.extend(unb64(&frame.data));
        }
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

/// Read `terminal`'s frames until `stop(text so far, exited)` holds,
/// acknowledging each output frame when `ack`.
async fn watch_terminal(
    dialed: &mut DialedLink,
    terminal: &str,
    ack: bool,
    stop: impl Fn(&str, bool) -> bool,
) -> Seen {
    let mut seen = Seen {
        frames: Vec::new(),
        exited: None,
    };
    let deadline = tokio::time::Instant::now() + wait();
    loop {
        if stop(&seen.text(), seen.exited.is_some()) {
            return seen;
        }
        let frame = tokio::time::timeout_at(deadline, next_frame(dialed))
            .await
            .unwrap_or_else(|_| panic!("timed out; saw {:?}", seen.text()))
            .expect("the link closed");
        match frame {
            LinkFrame::PtyOutput(output) if output.terminal_id == terminal => {
                if ack {
                    dialed
                        .link
                        .send(&LinkFrame::PtyAck(PtyAck {
                            terminal_id: terminal.into(),
                            seq: output.seq,
                        }))
                        .await
                        .unwrap();
                }
                seen.frames.push(output);
            }
            LinkFrame::PtyExited(exited) if exited.terminal_id == terminal => {
                seen.exited = Some(exited);
            }
            _ => {}
        }
    }
}

async fn opened_home(dialed: &mut DialedLink, home: &HomeRef) {
    dialed.link.send(&open(home)).await.unwrap();
    let reply = next_of(dialed, |frame| match frame {
        LinkFrame::HomeOpenReply(reply) => Some(reply),
        _ => None,
    })
    .await;
    assert_eq!(reply.outcome, HomeOpenOutcome::Ready, "{reply:?}");
}

/// A terminal for a home the machine does not hold, or with a `cwd` that
/// leaves the checkout, ends at once with a typed error and runs nothing.
#[tokio::test]
async fn a_terminal_on_an_unknown_home_or_outside_the_checkout_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), true).await;
    let mut dialed = dial_link(&node.endpoint, session_hello(1, "s1", Vec::new()))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    dialed
        .link
        .send(&pty_open(&home("h9"), "t1", PtyMode::Terminal, None))
        .await
        .unwrap();
    let seen = watch_terminal(&mut dialed, "t1", true, |_, exited| exited).await;
    let exited = seen.exited.unwrap();
    assert_eq!(exited.exit_code, None);
    assert!(
        exited
            .error
            .as_deref()
            .is_some_and(|e| e.starts_with(NODE_HOME_UNKNOWN)),
        "{exited:?}"
    );

    let h1 = home("h1");
    opened_home(&mut dialed, &h1).await;
    let LinkFrame::PtyOpen(mut escape) = pty_open(&h1, "t2", PtyMode::Exec, Some("pwd")) else {
        unreachable!()
    };
    escape.cwd = Some("../..".into());
    dialed.link.send(&LinkFrame::PtyOpen(escape)).await.unwrap();
    let seen = watch_terminal(&mut dialed, "t2", true, |_, exited| exited).await;
    assert!(seen.frames.is_empty());
    assert!(seen
        .exited
        .unwrap()
        .error
        .is_some_and(|e| e.starts_with("node-pty-invalid-cwd")),);
    node.stop().await;
}

/// A shell runs in the home checkout: typed input reaches it, its output
/// arrives numbered from 0, a resize applies, and `ptyKill` ends it with
/// a `ptyExited`.
#[cfg(unix)]
#[tokio::test]
async fn a_home_terminal_runs_in_the_home_checkout_and_takes_input() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), true).await;
    let h1 = home("h1");
    let mut dialed = dial_link(&node.endpoint, session_hello(1, "s1", Vec::new()))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    opened_home(&mut dialed, &h1).await;
    let checkout = std::fs::canonicalize(worktree(dir.path(), &h1)).unwrap();

    dialed
        .link
        .send(&pty_open(&h1, "t1", PtyMode::Terminal, Some("/bin/sh")))
        .await
        .unwrap();
    dialed
        .link
        .send(&LinkFrame::PtyResize(PtyResize {
            terminal_id: "t1".into(),
            cols: 132,
            rows: 50,
        }))
        .await
        .unwrap();
    dialed
        .link
        .send(&LinkFrame::PtyInput(PtyInput {
            terminal_id: "t1".into(),
            data: b64("echo at:$(pwd -P):cols:$(stty size)\n"),
        }))
        .await
        .unwrap();
    let expected = format!("at:{}:cols:50 132", checkout.display());
    let seen = watch_terminal(&mut dialed, "t1", true, |text, _| text.contains(&expected)).await;
    let seqs: Vec<u64> = seen.frames.iter().map(|frame| frame.seq).collect();
    assert_eq!(seqs, (0..seqs.len() as u64).collect::<Vec<_>>());

    dialed
        .link
        .send(&LinkFrame::PtyKill(PtyKill {
            terminal_id: "t1".into(),
        }))
        .await
        .unwrap();
    let seen = watch_terminal(&mut dialed, "t1", true, |_, exited| exited).await;
    let exited = seen.exited.unwrap();
    assert_eq!(exited.error, None, "{exited:?}");
    assert_ne!(
        exited.exit_code,
        Some(0),
        "killed, not a clean exit: {exited:?}"
    );
    node.stop().await;
}

/// Exec mode runs the command on pipes: stdout and stderr arrive apart,
/// then the exit code.
#[cfg(unix)]
#[tokio::test]
async fn exec_mode_streams_stdout_and_stderr_apart_and_reports_the_exit_code() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), true).await;
    let h1 = home("h1");
    let mut dialed = dial_link(&node.endpoint, session_hello(1, "s1", Vec::new()))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    opened_home(&mut dialed, &h1).await;
    dialed
        .link
        .send(&pty_open(
            &h1,
            "x1",
            PtyMode::Exec,
            Some("echo out; echo err >&2; exit 3"),
        ))
        .await
        .unwrap();
    let seen = watch_terminal(&mut dialed, "x1", true, |_, exited| exited).await;
    assert_eq!(seen.exited.unwrap().exit_code, Some(3));
    let of = |stream| {
        seen.frames
            .iter()
            .filter(|frame| frame.stream == Some(stream))
            .flat_map(|frame| unb64(&frame.data))
            .collect::<Vec<u8>>()
    };
    assert_eq!(of(PtyStream::Stdout), b"out\n");
    assert_eq!(of(PtyStream::Stderr), b"err\n");
    node.stop().await;
}

/// Output the backend has not acknowledged is resent, with the same
/// `seq`, on the next link of the same backend session; the process keeps
/// running across the reconnect.
#[cfg(unix)]
#[tokio::test]
async fn unacknowledged_terminal_output_is_resent_after_a_reconnect() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), true).await;
    let h1 = home("h1");
    let mut dialed = dial_link(&node.endpoint, session_hello(1, "s1", Vec::new()))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    opened_home(&mut dialed, &h1).await;
    dialed
        .link
        .send(&pty_open(
            &h1,
            "t1",
            PtyMode::Exec,
            Some("echo first; read line; echo second:$line"),
        ))
        .await
        .unwrap();
    let before = watch_terminal(&mut dialed, "t1", false, |text, _| text.contains("first")).await;
    drop(dialed);

    let mut dialed = dial_link(&node.endpoint, session_hello(2, "s1", vec![h1.clone()]))
        .await
        .unwrap();
    let again = watch_terminal(&mut dialed, "t1", true, |text, _| text.contains("first")).await;
    assert_eq!(again.frames[0].seq, before.frames[0].seq);
    dialed
        .link
        .send(&LinkFrame::PtyInput(PtyInput {
            terminal_id: "t1".into(),
            data: b64("still-here\n"),
        }))
        .await
        .unwrap();
    let after = watch_terminal(&mut dialed, "t1", true, |_, exited| exited).await;
    assert!(
        after.text().contains("second:still-here"),
        "{}",
        after.text()
    );
    assert_eq!(after.exited.unwrap().exit_code, Some(0));
    node.stop().await;
}

/// A bind from a new backend session (the orchestrator restarted) ends
/// every terminal of the previous one; none of its output is resent.
#[cfg(unix)]
#[tokio::test]
async fn a_new_backend_session_ends_the_previous_sessions_terminals() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), true).await;
    let h1 = home("h1");
    let mut dialed = dial_link(&node.endpoint, session_hello(1, "s1", Vec::new()))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    opened_home(&mut dialed, &h1).await;
    let marker = worktree(dir.path(), &h1).join("still-running");
    dialed
        .link
        .send(&pty_open(
            &h1,
            "t1",
            PtyMode::Exec,
            Some("echo up; sleep 2; touch still-running"),
        ))
        .await
        .unwrap();
    watch_terminal(&mut dialed, "t1", false, |text, _| text.contains("up")).await;
    drop(dialed);

    let mut dialed = dial_link(&node.endpoint, session_hello(2, "s2", vec![h1.clone()]))
        .await
        .unwrap();
    replayed(&mut dialed).await;
    let quiet = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(3)),
        async {
            loop {
                match next_frame(&mut dialed).await {
                    Some(LinkFrame::PtyOutput(_) | LinkFrame::PtyExited(_)) => return true,
                    Some(_) => {}
                    None => return false,
                }
            }
        },
    )
    .await;
    assert!(quiet.is_err(), "no frame of the old session's terminal");
    assert!(!marker.exists(), "the old session's process was ended");
    node.stop().await;
}

/// Past 4 MiB of unacknowledged output the terminal waits: nothing more is
/// sent until the backend acknowledges, then the rest arrives in order.
#[cfg(unix)]
#[tokio::test]
async fn terminal_output_waits_for_acknowledgements_past_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), true).await;
    let h1 = home("h1");
    let mut dialed = dial_link(&node.endpoint, session_hello(1, "s1", Vec::new()))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    opened_home(&mut dialed, &h1).await;
    let total = PTY_UNACKED_BYTES * 2;
    dialed
        .link
        .send(&pty_open(
            &h1,
            "x1",
            PtyMode::Exec,
            Some(&format!("head -c {total} /dev/zero")),
        ))
        .await
        .unwrap();
    let mut received = 0usize;
    let mut last_seq = None;
    let stalled = loop {
        match tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(2)),
            next_frame(&mut dialed),
        )
        .await
        {
            Err(_) => break true,
            Ok(Some(LinkFrame::PtyOutput(output))) => {
                received += unb64(&output.data).len();
                last_seq = Some(output.seq);
            }
            Ok(Some(LinkFrame::PtyExited(_))) => break false,
            Ok(Some(_)) => {}
            Ok(None) => panic!("the link closed"),
        }
    };
    assert!(stalled, "the output did not wait for acknowledgements");
    assert!(
        (PTY_UNACKED_BYTES..PTY_UNACKED_BYTES + 2 * MAX_PTY_OUTPUT_BYTES).contains(&received),
        "{received}"
    );
    dialed
        .link
        .send(&LinkFrame::PtyAck(PtyAck {
            terminal_id: "x1".into(),
            seq: last_seq.unwrap(),
        }))
        .await
        .unwrap();
    let rest = watch_terminal(&mut dialed, "x1", true, |_, exited| exited).await;
    assert_eq!(rest.frames[0].seq, last_seq.unwrap() + 1);
    let rest_bytes: usize = rest.frames.iter().map(|f| unb64(&f.data).len()).sum();
    assert_eq!(received + rest_bytes, total);
    assert_eq!(rest.exited.unwrap().exit_code, Some(0));
    node.stop().await;
}

/// A frame for a terminal the machine does not run (after a restart of
/// the machine, say) is answered with `ptyExited` `node-pty-unknown`, so
/// the backend ends its copy.
#[tokio::test]
async fn a_frame_for_an_unknown_terminal_is_answered_with_its_end() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), true).await;
    let mut dialed = dial_link(&node.endpoint, session_hello(1, "s1", Vec::new()))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    dialed
        .link
        .send(&LinkFrame::PtyAck(PtyAck {
            terminal_id: "gone".into(),
            seq: 4,
        }))
        .await
        .unwrap();
    let seen = watch_terminal(&mut dialed, "gone", false, |_, exited| exited).await;
    let exited = seen.exited.unwrap();
    assert_eq!(exited.exit_code, None);
    assert!(exited
        .error
        .is_some_and(|e| e.starts_with("node-pty-unknown")),);
    node.stop().await;
}

/// Input frames reach an exec process's stdin in order, none dropped,
/// also when they arrive faster than it reads.
#[cfg(unix)]
#[tokio::test]
async fn exec_input_arrives_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), true).await;
    let h1 = home("h1");
    let mut dialed = dial_link(&node.endpoint, session_hello(1, "s1", Vec::new()))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    opened_home(&mut dialed, &h1).await;
    dialed
        .link
        .send(&pty_open(
            &h1,
            "x1",
            PtyMode::Exec,
            Some("sleep 1; head -n 3"),
        ))
        .await
        .unwrap();
    for line in ["one\n", "two\n", "three\n"] {
        dialed
            .link
            .send(&LinkFrame::PtyInput(PtyInput {
                terminal_id: "x1".into(),
                data: b64(line),
            }))
            .await
            .unwrap();
    }
    let seen = watch_terminal(&mut dialed, "x1", true, |_, exited| exited).await;
    assert_eq!(seen.text(), "one\ntwo\nthree\n");
    assert_eq!(seen.exited.unwrap().exit_code, Some(0));
    node.stop().await;
}

/// A large paste into a terminal whose output waits for acknowledgements
/// does not hold up the link: the acknowledgement that frees the output
/// still arrives, and the program finishes.
#[cfg(unix)]
#[tokio::test]
async fn a_paste_while_the_output_waits_does_not_block_the_link() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), true).await;
    let h1 = home("h1");
    let mut dialed = dial_link(&node.endpoint, session_hello(1, "s1", Vec::new()))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    opened_home(&mut dialed, &h1).await;
    let total = PTY_UNACKED_BYTES * 2;
    dialed
        .link
        .send(&pty_open(
            &h1,
            "t1",
            PtyMode::Terminal,
            Some(&format!(
                "head -c {total} /dev/zero; stty -echo; cat > /dev/null"
            )),
        ))
        .await
        .unwrap();
    // Let the output fill the window without acknowledging it.
    let mut last_seq = 0;
    let mut received = 0usize;
    let filled = tokio::time::Instant::now() + wait();
    while received < PTY_UNACKED_BYTES {
        let frame = tokio::time::timeout_at(filled, next_frame(&mut dialed))
            .await
            .expect("the output never filled the window");
        if let Some(LinkFrame::PtyOutput(output)) = frame {
            received += unb64(&output.data).len();
            last_seq = output.seq;
        }
    }
    let paste = "x".repeat(256 * 1024);
    for _ in 0..4 {
        dialed
            .link
            .send(&LinkFrame::PtyInput(PtyInput {
                terminal_id: "t1".into(),
                data: b64(&paste),
            }))
            .await
            .unwrap();
    }
    dialed
        .link
        .send(&LinkFrame::PtyAck(PtyAck {
            terminal_id: "t1".into(),
            seq: last_seq,
        }))
        .await
        .unwrap();
    // The rest of the output arrives (the ack was read), acknowledged as
    // it comes; the input reached the program, which still runs.
    let deadline = tokio::time::Instant::now() + wait();
    while received < total {
        let frame = tokio::time::timeout_at(deadline, next_frame(&mut dialed))
            .await
            .unwrap_or_else(|_| panic!("stuck after {received} bytes"))
            .expect("the link closed");
        if let LinkFrame::PtyOutput(output) = frame {
            received += unb64(&output.data).len();
            dialed
                .link
                .send(&LinkFrame::PtyAck(PtyAck {
                    terminal_id: "t1".into(),
                    seq: output.seq,
                }))
                .await
                .unwrap();
        }
    }
    dialed
        .link
        .send(&LinkFrame::PtyKill(PtyKill {
            terminal_id: "t1".into(),
        }))
        .await
        .unwrap();
    let seen = watch_terminal(&mut dialed, "t1", true, |_, exited| exited).await;
    assert_eq!(seen.exited.unwrap().error, None);
    node.stop().await;
}

/// A `cwd` that is a symlink out of the home checkout is refused.
#[cfg(unix)]
#[tokio::test]
async fn a_cwd_symlinked_out_of_the_checkout_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), true).await;
    let h1 = home("h1");
    let mut dialed = dial_link(&node.endpoint, session_hello(1, "s1", Vec::new()))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    opened_home(&mut dialed, &h1).await;
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), worktree(dir.path(), &h1).join("out")).unwrap();
    let LinkFrame::PtyOpen(mut open) = pty_open(&h1, "x1", PtyMode::Exec, Some("pwd")) else {
        unreachable!()
    };
    open.cwd = Some("out".into());
    dialed.link.send(&LinkFrame::PtyOpen(open)).await.unwrap();
    let seen = watch_terminal(&mut dialed, "x1", true, |_, exited| exited).await;
    assert!(seen.frames.is_empty());
    assert!(seen
        .exited
        .unwrap()
        .error
        .is_some_and(|e| e.starts_with("node-pty-invalid-cwd")));
    node.stop().await;
}

/// A kill ends a terminal whose output is paused at the acknowledgement
/// window, in both modes: its `ptyExited` still arrives.
#[cfg(unix)]
#[tokio::test]
async fn a_kill_ends_a_terminal_whose_output_is_paused() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), true).await;
    let h1 = home("h1");
    let mut dialed = dial_link(&node.endpoint, session_hello(1, "s1", Vec::new()))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    opened_home(&mut dialed, &h1).await;
    for (terminal, mode) in [("t1", PtyMode::Terminal), ("x1", PtyMode::Exec)] {
        dialed
            .link
            .send(&pty_open(
                &h1,
                terminal,
                mode,
                Some(&format!(
                    "head -c {} /dev/zero; sleep 600",
                    PTY_UNACKED_BYTES * 2
                )),
            ))
            .await
            .unwrap();
        let filled = tokio::time::Instant::now() + wait();
        let mut received = 0usize;
        while received < PTY_UNACKED_BYTES {
            let frame = tokio::time::timeout_at(filled, next_frame(&mut dialed))
                .await
                .expect("the output never filled the window");
            if let Some(LinkFrame::PtyOutput(output)) = frame {
                if output.terminal_id == terminal {
                    received += unb64(&output.data).len();
                }
            }
        }
        dialed
            .link
            .send(&LinkFrame::PtyKill(PtyKill {
                terminal_id: terminal.into(),
            }))
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + wait();
        loop {
            let frame = tokio::time::timeout_at(deadline, next_frame(&mut dialed))
                .await
                .unwrap_or_else(|_| panic!("{terminal} never ended after the kill"))
                .expect("the link closed");
            match frame {
                LinkFrame::PtyExited(exited) if exited.terminal_id == terminal => break,
                LinkFrame::PtyOutput(output) if output.terminal_id == terminal => {
                    dialed
                        .link
                        .send(&LinkFrame::PtyAck(PtyAck {
                            terminal_id: terminal.into(),
                            seq: output.seq,
                        }))
                        .await
                        .unwrap();
                }
                _ => {}
            }
        }
    }
    node.stop().await;
}

fn home_script(home: &HomeRef, spawn: &str, command: &str, cwd: Option<&str>) -> LinkFrame {
    LinkFrame::ScriptStart(intent_core::node_link::ScriptStartCommand {
        owner: intent_core::node_link::CheckoutOwner::Home(home.clone()),
        script_id: "s1".into(),
        spawn_id: spawn.into(),
        command: command.into(),
        mode: intent_core::ScriptMode::Command,
        cwd: cwd.map(str::to_owned),
        env: vec![intent_core::node_link::ScriptEnvVar {
            key: "HOME_RUN_VAR".into(),
            value: "from-the-script".into(),
        }],
    })
}

/// What one home run reported: whether it started, its output in `seq`
/// order and its exit.
#[derive(Default)]
struct HomeRun {
    started: bool,
    seqs: Vec<u64>,
    output: String,
    exited: Option<intent_core::node_link::ScriptExited>,
}

/// Collect `spawn`'s script frames until its `scriptExited`, checking that
/// each names `home`.
async fn watch_home_run(dialed: &mut DialedLink, home: &HomeRef, spawn: &str) -> HomeRun {
    let owner = intent_core::node_link::CheckoutOwner::Home(home.clone());
    let mut run = HomeRun::default();
    while run.exited.is_none() {
        let frame = next_frame(dialed).await.expect("the link closed");
        if frame.script_owner().is_none() {
            continue;
        }
        assert_eq!(frame.script_owner(), Some(&owner), "{frame:?}");
        match frame {
            LinkFrame::ScriptStarted(started) if started.spawn_id == spawn => run.started = true,
            LinkFrame::ScriptOutput(output) if output.spawn_id == spawn => {
                run.seqs.push(output.seq);
                run.output.push_str(&output.data);
            }
            LinkFrame::ScriptExited(exited) if exited.spawn_id == spawn => {
                run.exited = Some(exited);
            }
            _ => {}
        }
    }
    run
}

/// A home run (nodeProtocol 19) runs in the home checkout with its saved
/// environment, streams its output numbered from 0 and reports its exit
/// code, every frame naming the home.
#[cfg(unix)]
#[tokio::test]
async fn a_home_run_runs_in_the_home_checkout_and_reports_its_exit() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), true).await;
    let h1 = home("h1");
    let mut dialed = dial_link(&node.endpoint, session_hello(1, "s1", Vec::new()))
        .await
        .unwrap();
    assert_eq!(dialed.link.protocol(), HOMES_PROTOCOL);
    replay_complete(&mut dialed).await;
    opened_home(&mut dialed, &h1).await;
    let checkout = std::fs::canonicalize(worktree(dir.path(), &h1)).unwrap();
    std::fs::create_dir_all(checkout.join("web")).unwrap();

    dialed
        .link
        .send(&home_script(
            &h1,
            "p1",
            "echo at:$(pwd -P):$HOME_RUN_VAR; exit 4",
            Some("web"),
        ))
        .await
        .unwrap();
    let run = watch_home_run(&mut dialed, &h1, "p1").await;
    assert!(run.started);
    assert!(
        run.output.contains(&format!(
            "at:{}:from-the-script",
            checkout.join("web").display()
        )),
        "{}",
        run.output
    );
    assert_eq!(run.seqs, (0..run.seqs.len() as u64).collect::<Vec<_>>());
    let exited = run.exited.unwrap();
    assert_eq!(exited.exit_code, Some(4), "{exited:?}");
    assert_eq!(exited.error, None);
    node.stop().await;
}

/// A home run for a home this machine does not hold, or with a `cwd` that
/// leaves the home checkout, is refused with a typed `scriptExited` and
/// runs nothing.
#[tokio::test]
async fn a_home_run_on_an_unknown_home_or_outside_the_checkout_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), true).await;
    let mut dialed = dial_link(&node.endpoint, session_hello(1, "s1", Vec::new()))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;

    let unknown = home("h9");
    dialed
        .link
        .send(&home_script(&unknown, "p1", "echo ran", None))
        .await
        .unwrap();
    let run = watch_home_run(&mut dialed, &unknown, "p1").await;
    assert!(!run.started);
    let exited = run.exited.unwrap();
    assert_eq!(exited.exit_code, None);
    assert!(
        exited
            .error
            .as_deref()
            .is_some_and(|e| e.starts_with(NODE_HOME_UNKNOWN)),
        "{exited:?}"
    );

    let h1 = home("h1");
    opened_home(&mut dialed, &h1).await;
    dialed
        .link
        .send(&home_script(&h1, "p2", "echo ran", Some("../outside")))
        .await
        .unwrap();
    let run = watch_home_run(&mut dialed, &h1, "p2").await;
    assert!(!run.started);
    assert!(run
        .exited
        .unwrap()
        .error
        .is_some_and(|e| e.starts_with("node-script-invalid-cwd")),);
    node.stop().await;
}

/// `scriptStop` naming the home ends its run, answered by the run's own
/// `scriptExited`; a stop naming an agent with the same script and spawn
/// does not touch it.
#[cfg(unix)]
#[tokio::test]
async fn stopping_a_home_run_ends_it_and_an_agent_stop_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(dir.path(), true).await;
    let h1 = home("h1");
    let mut dialed = dial_link(&node.endpoint, session_hello(1, "s1", Vec::new()))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    opened_home(&mut dialed, &h1).await;
    dialed
        .link
        .send(&home_script(&h1, "p1", "echo up; sleep 600", None))
        .await
        .unwrap();
    next_of(&mut dialed, |frame| match frame {
        LinkFrame::ScriptOutput(output) if output.data.contains("up") => Some(()),
        _ => None,
    })
    .await;

    // Same script and spawn, but an agent's: no such process.
    dialed
        .link
        .send(&LinkFrame::ScriptStop(
            intent_core::node_link::ScriptProcessRef {
                owner: AgentId::from("a1").into(),
                script_id: "s1".into(),
                spawn_id: "p1".into(),
            },
        ))
        .await
        .unwrap();
    let refused = next_of(&mut dialed, |frame| match frame {
        LinkFrame::ScriptExited(exited) => Some(exited),
        _ => None,
    })
    .await;
    assert_eq!(refused.owner, AgentId::from("a1").into());
    assert!(refused
        .error
        .is_some_and(|e| e.starts_with("node-script-not-running")));

    dialed
        .link
        .send(&LinkFrame::ScriptStop(
            intent_core::node_link::ScriptProcessRef {
                owner: intent_core::node_link::CheckoutOwner::Home(h1.clone()),
                script_id: "s1".into(),
                spawn_id: "p1".into(),
            },
        ))
        .await
        .unwrap();
    let run = watch_home_run(&mut dialed, &h1, "p1").await;
    let exited = run.exited.unwrap();
    assert_eq!(exited.error, None, "{exited:?}");
    assert_ne!(exited.exit_code, Some(0), "killed: {exited:?}");
    node.stop().await;
}

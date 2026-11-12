//! Workspace homes on the machine (nodeProtocol 19) over a real link: the
//! opt-out, `homeOpen`, the backup journal, `homePush`, lease checks and
//! the bind-time sweep. The home checkouts are plain directories whose
//! `HEAD` is a file ([`checkout::TEST_HOME_HEAD`]); the git side of a home
//! is covered by the e2e tests.

use intent_core::node_link::{
    HomeOpen, HomeOpenOutcome, HomePush, HomePushOutcome, HomeRef, PreparedCheckout, RecordOwner,
    SetupState, HOMES_PROTOCOL, NODE_HOME_UNKNOWN, NODE_HOME_UNSUPPORTED,
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

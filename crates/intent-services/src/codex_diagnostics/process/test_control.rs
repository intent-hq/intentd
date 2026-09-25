//! In-suite barriers around the existing cleanup path. Platform teardown still
//! runs normally; simulated confirmation failure occurs only after it returns.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::process::Command;
use tokio::sync::{mpsc, Semaphore};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Acp,
    Metadata,
    AdapterVersion,
    RuntimeVersion,
    Raw,
}

pub struct Event {
    pub stage: Stage,
    pub home: PathBuf,
    pub finished: bool,
}

struct State {
    target: Stage,
    fail: bool,
    release: Semaphore,
    events: mpsc::UnboundedSender<Event>,
}

tokio::task_local! {
    static CONTROL: Arc<State>;
}

#[derive(Clone)]
pub struct Control(Arc<State>);

impl Control {
    pub fn new(target: Stage, fail: bool) -> (Self, mpsc::UnboundedReceiver<Event>) {
        let (events, receiver) = mpsc::unbounded_channel();
        (
            Self(Arc::new(State {
                target,
                fail,
                release: Semaphore::new(0),
                events,
            })),
            receiver,
        )
    }

    pub async fn scope<F: std::future::Future>(&self, future: F) -> F::Output {
        CONTROL.scope(self.0.clone(), future).await
    }

    pub fn release(&self) {
        self.0.release.add_permits(1);
    }
}

pub struct CleanupHook {
    state: Arc<State>,
    stage: Stage,
    home: PathBuf,
}

impl CleanupHook {
    pub async fn before(&self) {
        self.event(false);
        if self.stage == self.state.target {
            let _permit = self.state.release.acquire().await.unwrap();
        }
    }

    pub fn fail(&self) -> bool {
        self.state.fail && self.stage == self.state.target
    }

    pub fn after(&self) {
        self.event(true);
    }

    fn event(&self, finished: bool) {
        let _ = self.state.events.send(Event {
            stage: self.stage,
            home: self.home.clone(),
            finished,
        });
    }
}

pub fn capture(command: &Command, home: &Path) -> Option<CleanupHook> {
    let control = CONTROL.try_with(Arc::clone).ok()?;
    let args: Vec<_> = command.as_std().get_args().collect();
    let stage = if args.contains(&std::ffi::OsStr::new("app-server")) {
        Stage::Raw
    } else if args.contains(&std::ffi::OsStr::new("--version")) {
        if args
            .iter()
            .any(|arg| Path::new(arg).file_name().is_some_and(|n| n == "codex.js"))
        {
            Stage::RuntimeVersion
        } else {
            Stage::AdapterVersion
        }
    } else if args.first().is_some_and(|arg| *arg == "-e") {
        Stage::Metadata
    } else {
        Stage::Acp
    };
    Some(CleanupHook {
        state: control,
        stage,
        home: home.to_owned(),
    })
}

pub async fn next(
    receiver: &mut mpsc::UnboundedReceiver<Event>,
    stage: Stage,
    finished: bool,
) -> Event {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let event = receiver.recv().await.expect("cleanup event stream closed");
            if event.stage == stage && event.finished == finished {
                return event;
            }
        }
    })
    .await
    .expect("cleanup reached expected barrier")
}

//! Turn-lifecycle interleavings. Starting, re-driving, retrying,
//! interrupting and stopping a turn kept undoing one another when two of
//! them overlapped (intentd#3694, #3723, #3739, #3742, #3747, #3749, #3754).
//!
//! Each case parks one action at a named [`TurnStep`] (or lets it finish),
//! runs more actions to their end, resumes the first, and then checks the
//! same invariants for every order:
//!
//! 1. A prompt that is still running holds the agent's slot, and at most one
//!    prompt runs at a time.
//! 2. With no interrupt or stop in the order, no turn is cut short, each
//!    message reaches a runtime once, and the failed message goes first.
//! 3. The agent is never stuck: a final message is delivered, and the agent
//!    then settles with nothing queued.

use super::*;
use crate::agent_manager::turn_schedule::{TurnFault, TurnStep};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    Send(&'static str),
    Retry,
    Redrive,
    Interrupt,
    Stop,
}

impl Action {
    const THEN: [Self; 5] = [
        Self::Send(BETA),
        Self::Retry,
        Self::Redrive,
        Self::Interrupt,
        Self::Stop,
    ];

    fn cancels(self) -> bool {
        matches!(self, Self::Interrupt | Self::Stop)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scenario {
    /// The first turn failed with no output; its message waits for Try again.
    FailedPark,
    /// The same, with the failed message gone: a new message sends directly.
    FailedNoQueue,
    /// A turn is running.
    Running,
}

#[derive(Clone, Debug)]
struct Case {
    scenario: Scenario,
    fault: Option<TurnFault>,
    first: Action,
    at: Option<TurnStep>,
    then: Vec<Action>,
}

impl Case {
    fn cancels(&self) -> bool {
        self.first.cancels() || self.then.iter().any(|a| a.cancels())
    }
}

/// `first` parked at `at` (or run to its end), followed by every second
/// action.
fn pairs(
    scenario: Scenario,
    fault: Option<TurnFault>,
    first: Action,
    at: Option<TurnStep>,
) -> Vec<Case> {
    Action::THEN
        .into_iter()
        .map(|then| Case {
            scenario,
            fault,
            first,
            at,
            then: vec![then],
        })
        .collect()
}

const SPARE_RUNS: usize = 4;
const ALPHA: &str = "interleaved message alpha";
const BETA: &str = "interleaved message beta";
const FINAL_MESSAGE: &str = "interleaved final message";
const RUNNING_MESSAGE: &str = "interleaved running message";

/// The agent under test and every runtime its launches hand out.
#[derive(Clone)]
struct Actor {
    mgr: Arc<AgentManager>,
    ws: WorkspaceId,
    id: AgentId,
    runs: Vec<Arc<FakeRuntime>>,
}

impl Actor {
    async fn act(self, action: Action) -> intent_core::Result<Value> {
        match action {
            Action::Send(text) => {
                self.mgr
                    .send_message(
                        self.id.clone(),
                        self.ws.clone(),
                        text.into(),
                        None,
                        super::super::super::super::TurnOptions::default(),
                    )
                    .await
            }
            Action::Retry => self.mgr.agent_retry(self.id.clone(), self.ws.clone()).await,
            Action::Redrive => self
                .mgr
                .redrive_failed_turn(&self.id, &self.ws)
                .await
                .map(|()| Value::Null),
            Action::Interrupt => Ok(json!(self.mgr.interrupt(&self.id).await)),
            Action::Stop => Ok(json!(self.mgr.stop(&self.id).await)),
        }
    }

    fn in_flight(&self) -> usize {
        self.runs
            .iter()
            .map(|run| run.in_flight.load(Ordering::SeqCst))
            .sum()
    }

    fn snapshot(&self) -> (bool, usize, usize, usize, usize, usize) {
        let prompts = self
            .runs
            .iter()
            .map(|run| run.prompts.lock().unwrap().len())
            .sum();
        let responses = self
            .runs
            .iter()
            .map(|run| run.responses.load(Ordering::SeqCst))
            .sum();
        (
            self.mgr.is_busy(&self.id),
            self.mgr.live_worker_count_for_test(),
            self.in_flight(),
            prompts,
            responses,
            self.mgr.services.queue_snapshot(&self.id).len(),
        )
    }

    /// Wait until nothing has moved for a while, then check invariant 1.
    async fn settle(&self, label: &str) {
        timeout(
            intentd_test_support::test_timeout(Duration::from_secs(20)),
            async {
                let mut last = self.snapshot();
                let mut quiet = 0;
                while quiet < 25 {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    let now = self.snapshot();
                    if now == last {
                        quiet += 1;
                    } else {
                        (last, quiet) = (now, 0);
                    }
                }
            },
        )
        .await
        .unwrap_or_else(|_| panic!("{label}: the agent never settled"));
        let in_flight = self.in_flight();
        assert!(
            in_flight <= 1,
            "{label}: {in_flight} prompts run at once: {}",
            self.prompts()
        );
        assert!(
            in_flight == 0 || self.mgr.is_busy(&self.id),
            "{label}: a running prompt lost its slot: {}",
            self.prompts()
        );
    }

    fn prompts(&self) -> String {
        self.runs
            .iter()
            .enumerate()
            .map(|(n, run)| format!("[run {n}]\n{}", all_prompts(run)))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

async fn fixture(scenario: Scenario, name: &str) -> (TempDb, Actor, Vec<&'static str>) {
    let runs: Vec<_> = (0..SPARE_RUNS).map(|_| FakeRuntime::new()).collect();
    let launches = runs.iter().map(|run| Launch::Runs(run.clone())).collect();
    match scenario {
        Scenario::FailedPark | Scenario::FailedNoQueue => {
            let (tmp, mgr, ws, id) = remote_failed_first_turn(name, launches).await;
            let mut sent = vec![FIRST_MESSAGE];
            if scenario == Scenario::FailedNoQueue {
                assert!(
                    mgr.services.clear_queue(&id),
                    "the failed message was queued"
                );
                sent.clear();
            }
            (tmp, Actor { mgr, ws, id, runs }, sent)
        }
        Scenario::Running => {
            let ws = WorkspaceId::from(format!("ws-{name}"));
            let id = AgentId::from(format!("agent-{name}"));
            let (tmp, mgr, _backup) =
                remote_agent_with_backup(ScriptedRemote::new(launches), &id, &ws).await;
            send(&mgr, &id, &ws, RUNNING_MESSAGE).await;
            runs[0].wait_prompts(1).await;
            (tmp, Actor { mgr, ws, id, runs }, vec![RUNNING_MESSAGE])
        }
    }
}

async fn run_case(case: &Case, n: usize) {
    let label = format!("case {n} {case:?}");
    let name = format!("{:?}-{n}", case.scenario).to_lowercase();
    let (_tmp, actor, mut sent) = fixture(case.scenario, &name).await;
    if let Some(fault) = case.fault {
        actor.mgr.turn_schedule.inject(fault);
    }

    let pause = case.at.map(|step| actor.mgr.turn_schedule.pause(step));
    let mut first = tokio::spawn(actor.clone().act(case.first));
    let mut parked = None;
    if let Some(pause) = pause {
        tokio::select! {
            () = pause.reached.notified() => parked = Some(pause),
            done = &mut first => {
                record_sent(&mut sent, case.first, &done.unwrap());
                actor.mgr.turn_schedule.disarm(case.at.unwrap());
            }
        }
    } else {
        record_sent(&mut sent, case.first, &(&mut first).await.unwrap());
    }
    actor.settle(&label).await;

    for &then in &case.then {
        let done = actor.clone().act(then).await;
        record_sent(&mut sent, then, &done);
        actor.settle(&label).await;
    }

    if let Some(pause) = parked {
        pause.resume.notify_one();
        record_sent(&mut sent, case.first, &first.await.unwrap());
        actor.settle(&label).await;
    }

    finish(&actor, &label, case.cancels(), &mut sent).await;
}

fn record_sent(sent: &mut Vec<&'static str>, action: Action, done: &intent_core::Result<Value>) {
    if let (Action::Send(text), Ok(_)) = (action, done) {
        sent.push(text);
    }
}

/// Invariants 2 and 3, after a final message.
async fn finish(actor: &Actor, label: &str, cancels: bool, sent: &mut Vec<&'static str>) {
    actor.mgr.turn_schedule.clear_faults();
    for run in &actor.runs {
        run.finish.add_permits(64);
    }
    actor.settle(label).await;
    let done = actor.clone().act(Action::Send(FINAL_MESSAGE)).await;
    record_sent(sent, Action::Send(FINAL_MESSAGE), &done);
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(20)),
        async {
            while !actor.prompts().contains(FINAL_MESSAGE) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        },
    )
    .await
    .unwrap_or_else(|_| panic!("{label}: the agent is stuck: {}", actor.prompts()));
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(20)),
        async {
            while actor.mgr.is_busy(&actor.id)
                || actor.mgr.live_worker_count_for_test() > 0
                || !actor.mgr.services.queue_snapshot(&actor.id).is_empty()
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        },
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "{label}: the agent never settled (busy {}, workers {}, queued {}): {}",
            actor.mgr.is_busy(&actor.id),
            actor.mgr.live_worker_count_for_test(),
            actor.mgr.services.queue_snapshot(&actor.id).len(),
            actor.prompts()
        )
    });
    actor.settle(label).await;

    let prompts = actor.prompts();
    // A stop drops the message whose turn it ends; only orders without one
    // must deliver every message.
    if cancels {
        return;
    }
    for text in sent.iter() {
        assert!(
            prompts.contains(text),
            "{label}: {text} was lost: {prompts}"
        );
    }
    for (n, run) in actor.runs.iter().enumerate() {
        let run_prompts = all_prompts(run);
        for text in sent.iter() {
            assert!(
                run_prompts.matches(text).count() <= 1,
                "{label}: {text} reached run {n} twice: {prompts}"
            );
        }
        if let Some(failed) = run_prompts.find(FIRST_MESSAGE) {
            for text in sent.iter().filter(|text| **text != FIRST_MESSAGE) {
                if let Some(at) = run_prompts.find(text) {
                    assert!(
                        failed < at,
                        "{label}: {text} ran before the failed message: {prompts}"
                    );
                }
            }
        }
        assert_eq!(
            run.cancels.load(Ordering::SeqCst),
            0,
            "{label}: run {n} was cancelled: {prompts}"
        );
        assert_eq!(
            run.responses.load(Ordering::SeqCst),
            run.prompts.lock().unwrap().len(),
            "{label}: a turn on run {n} was cut short: {prompts}"
        );
    }
}

async fn run_cases(cases: Vec<Case>) {
    for (n, case) in cases.iter().enumerate() {
        run_case(case, n).await;
    }
}

/// One test per scenario, fault, first action and step, so the orders run
/// in parallel and each test stays well inside its time limit.
macro_rules! interleavings {
    ($($name:ident: $scenario:ident, $fault:expr, $first:expr, $at:expr;)*) => {$(
        #[intent_test_macros::daemon_test]
        async fn $name() {
            run_cases(pairs(Scenario::$scenario, $fault, $first, $at)).await;
        }
    )*};
}

interleavings! {
    failed_turn_then_send_at_user_persist: FailedPark, None, Action::Send(ALPHA), Some(TurnStep::UserPersist);
    failed_turn_then_send_to_its_end: FailedPark, None, Action::Send(ALPHA), None;
    failed_turn_then_retry_at_retry_checked: FailedPark, None, Action::Retry, Some(TurnStep::RetryChecked);
    failed_turn_then_retry_at_retry_lifted: FailedPark, None, Action::Retry, Some(TurnStep::RetryLifted);
    failed_turn_then_retry_to_its_end: FailedPark, None, Action::Retry, None;
    failed_turn_then_redrive_at_redrive_checked: FailedPark, None, Action::Redrive, Some(TurnStep::RedriveChecked);
    failed_turn_then_redrive_to_its_end: FailedPark, None, Action::Redrive, None;
    failed_turn_then_interrupt_at_interrupt_releasing: FailedPark, None, Action::Interrupt, Some(TurnStep::InterruptReleasing);
    failed_turn_then_interrupt_to_its_end: FailedPark, None, Action::Interrupt, None;
    failed_turn_then_stop_at_detach_releasing: FailedPark, None, Action::Stop, Some(TurnStep::DetachReleasing);
    failed_turn_then_stop_to_its_end: FailedPark, None, Action::Stop, None;
    failed_turn_redrive_store_error_then_send_at_user_persist: FailedPark, Some(TurnFault::RedriveStore), Action::Send(ALPHA), Some(TurnStep::UserPersist);
    failed_turn_redrive_store_error_then_send_to_its_end: FailedPark, Some(TurnFault::RedriveStore), Action::Send(ALPHA), None;
    failed_turn_redrive_store_error_then_redrive_at_redrive_checked: FailedPark, Some(TurnFault::RedriveStore), Action::Redrive, Some(TurnStep::RedriveChecked);
    failed_turn_redrive_store_error_then_redrive_to_its_end: FailedPark, Some(TurnFault::RedriveStore), Action::Redrive, None;
    failed_active_write_then_send_at_user_persist: FailedNoQueue, Some(TurnFault::ActiveWrite), Action::Send(ALPHA), Some(TurnStep::UserPersist);
    failed_active_write_then_send_to_its_end: FailedNoQueue, Some(TurnFault::ActiveWrite), Action::Send(ALPHA), None;
    failed_active_write_then_retry_at_retry_checked: FailedNoQueue, Some(TurnFault::ActiveWrite), Action::Retry, Some(TurnStep::RetryChecked);
    failed_active_write_then_retry_at_retry_lifted: FailedNoQueue, Some(TurnFault::ActiveWrite), Action::Retry, Some(TurnStep::RetryLifted);
    failed_active_write_then_retry_to_its_end: FailedNoQueue, Some(TurnFault::ActiveWrite), Action::Retry, None;
    failed_active_write_then_redrive_at_redrive_checked: FailedNoQueue, Some(TurnFault::ActiveWrite), Action::Redrive, Some(TurnStep::RedriveChecked);
    failed_active_write_then_redrive_to_its_end: FailedNoQueue, Some(TurnFault::ActiveWrite), Action::Redrive, None;
    failed_active_write_then_interrupt_at_interrupt_releasing: FailedNoQueue, Some(TurnFault::ActiveWrite), Action::Interrupt, Some(TurnStep::InterruptReleasing);
    failed_active_write_then_interrupt_to_its_end: FailedNoQueue, Some(TurnFault::ActiveWrite), Action::Interrupt, None;
    failed_active_write_then_stop_at_detach_releasing: FailedNoQueue, Some(TurnFault::ActiveWrite), Action::Stop, Some(TurnStep::DetachReleasing);
    failed_active_write_then_stop_to_its_end: FailedNoQueue, Some(TurnFault::ActiveWrite), Action::Stop, None;
    running_turn_then_send_at_user_persist: Running, None, Action::Send(ALPHA), Some(TurnStep::UserPersist);
    running_turn_then_send_to_its_end: Running, None, Action::Send(ALPHA), None;
    running_turn_then_retry_at_retry_checked: Running, None, Action::Retry, Some(TurnStep::RetryChecked);
    running_turn_then_retry_at_retry_lifted: Running, None, Action::Retry, Some(TurnStep::RetryLifted);
    running_turn_then_retry_to_its_end: Running, None, Action::Retry, None;
    running_turn_then_redrive_at_redrive_checked: Running, None, Action::Redrive, Some(TurnStep::RedriveChecked);
    running_turn_then_redrive_to_its_end: Running, None, Action::Redrive, None;
    running_turn_then_interrupt_at_interrupt_releasing: Running, None, Action::Interrupt, Some(TurnStep::InterruptReleasing);
    running_turn_then_interrupt_to_its_end: Running, None, Action::Interrupt, None;
    running_turn_then_stop_at_detach_releasing: Running, None, Action::Stop, Some(TurnStep::DetachReleasing);
    running_turn_then_stop_to_its_end: Running, None, Action::Stop, None;
}

/// A stop or interrupt parked before it frees the slot, while another one
/// frees it and a new message starts a turn.
#[intent_test_macros::daemon_test]
async fn running_turn_two_stops_then_a_new_message() {
    let mut cases = Vec::new();
    for (first, at, then) in [
        (
            Action::Interrupt,
            TurnStep::InterruptReleasing,
            [Action::Stop, Action::Send(BETA)],
        ),
        (
            Action::Interrupt,
            TurnStep::InterruptReleasing,
            [Action::Interrupt, Action::Send(BETA)],
        ),
        (
            Action::Stop,
            TurnStep::DetachReleasing,
            [Action::Interrupt, Action::Send(BETA)],
        ),
    ] {
        cases.push(Case {
            scenario: Scenario::Running,
            fault: None,
            first,
            at: Some(at),
            then: then.to_vec(),
        });
    }
    run_cases(cases).await;
}

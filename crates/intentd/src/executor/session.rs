//! One bound link from the backend: replay the journal after the backend's
//! acknowledged sequence, then stream new records live, turn the backend's
//! control frames into runner calls, and answer capabilities requests.
//!
//! When head binds a newer generation, the listener closes the older link,
//! but its session may still hold frames it already received, or be about
//! to adopt its lease. Every session therefore takes the runner's link gate
//! (the binding of the newest session) around adoption and around each
//! frame's side effect, and acts only while its link is open and no newer
//! generation holds the gate. A replaced session can launch, stop or
//! acknowledge nothing, and cannot move the journal back to its lease.

use intent_core::node_link::{
    FinalPush, LeaseBinding, LinkChannel, LinkFrame, LinkHello, LinkOutcome, LinkRpcError,
    MachineCapabilities, PushWipResult, SkillsReply, StopId, StopOutcome, CHECKOUT_FILES_PROTOCOL,
    FINAL_STOP_PROTOCOL, FORGE_TOKEN_PROTOCOL, HOMES_PROTOCOL, NODE_LINK_PROTOCOLS,
    ONE_SHOT_PROTOCOL, SKILLS_PROTOCOL,
};
use intent_core::nodes::NodeCounter;
use intent_core::AgentId;
use intent_core::BoxFuture;
use intent_transport::node_link::{LinkEvent, LinkEvents, NodeLink};

use super::runner::{Runner, StopWait};
use super::skills;

/// JSON-RPC "method not found": the node serves no head-to-node requests yet.
const METHOD_NOT_FOUND: i32 = -32601;

/// What a session may still do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Admission {
    /// This session holds the newest binding.
    Current,
    /// The link closed or a newer generation took over: drop the frame.
    Replaced,
}

/// Whether `binding`'s session may act, given the gate's binding.
fn admit(gate: Option<&LeaseBinding>, binding: &LeaseBinding, closed: bool) -> Admission {
    if closed || gate.is_some_and(|current| current.generation > binding.generation) {
        Admission::Replaced
    } else {
        Admission::Current
    }
}

/// The outcome of adopting a link's lease and acknowledgement.
#[derive(Debug, PartialEq, Eq)]
enum Adopted {
    Ready,
    /// A newer session already took over.
    Replaced,
    /// The journal refused (head acknowledges unwritten records, or IO).
    Refused(String),
}

/// Make `binding` the gate's binding and resync the journal to its lease
/// and acknowledged sequence, unless a newer session got there first.
async fn adopt(
    runner: &Runner,
    binding: &LeaseBinding,
    acked: NodeCounter,
    closed: impl Fn() -> bool,
) -> Adopted {
    let mut gate = runner.link_gate().lock().await;
    if admit(gate.as_ref(), binding, closed()) == Admission::Replaced {
        return Adopted::Replaced;
    }
    *gate = Some(binding.clone());
    let lease_id = binding.lease_id.clone();
    let result = runner
        .with_journal(move |journal| {
            if journal.use_lease(&lease_id)? {
                tracing::info!("node: new lease; journal sequence restarted");
            }
            journal.acknowledge(acked)
        })
        .await;
    match result {
        Ok(()) => Adopted::Ready,
        Err(error) => Adopted::Refused(error.to_string()),
    }
}

/// What applying a frame asks of the link.
#[derive(Debug, PartialEq, Eq)]
enum Applied {
    Done,
    /// Dropped: the session was replaced.
    Stale,
    /// The frame was invalid for this link; close it.
    Close,
    /// A stop was applied for the current session: send its
    /// acknowledgement once [`PendingAck::ack`] resolves.
    Stopping(PendingAck),
}

/// The acknowledgement owed for one stop, sent once the stop is applied.
pub(super) struct PendingAck {
    stop_id: StopId,
    agent_id: AgentId,
    wait: AckWait,
}

/// What a stop's acknowledgement waits for.
enum AckWait {
    Plain(StopWait),
    /// A final stop: the stop, the final push and the checkout's removal.
    Final(BoxFuture<'static, Option<(StopOutcome, FinalPush)>>),
}

impl PendingAck {
    /// Wait until the stop is applied, then build its `stopAck`; `None`
    /// when it may not be acknowledged (the exit record was not journaled,
    /// or a final stop's checkout could not be removed).
    pub(super) async fn ack(self) -> Option<LinkFrame> {
        let (outcome, final_push) = match self.wait {
            AckWait::Plain(wait) => (wait.wait().await?, None),
            AckWait::Final(wait) => {
                let (outcome, push) = wait.await?;
                (outcome, Some(push))
            }
        };
        Some(LinkFrame::StopAck {
            stop_id: self.stop_id,
            agent_id: self.agent_id,
            outcome,
            final_push,
        })
    }
}

impl std::fmt::Debug for PendingAck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingAck")
            .field("stop_id", &self.stop_id)
            .field("agent_id", &self.agent_id)
            .finish_non_exhaustive()
    }
}

impl PartialEq for PendingAck {
    fn eq(&self, other: &Self) -> bool {
        (self.stop_id, &self.agent_id) == (other.stop_id, &other.agent_id)
    }
}

impl Eq for PendingAck {}

/// Apply one control or journal frame for `binding`'s session.
async fn apply(
    runner: &Runner,
    binding: &LeaseBinding,
    closed: impl Fn() -> bool,
    frame: LinkFrame,
) -> Applied {
    let gate = runner.link_gate().lock().await;
    if admit(gate.as_ref(), binding, closed()) == Admission::Replaced {
        tracing::debug!(frame = ?frame.channel(), "node: dropping a frame from a replaced link");
        return Applied::Stale;
    }
    match frame {
        LinkFrame::Launch(command) => runner.launch(command).await,
        LinkFrame::Prompt {
            agent_id,
            turn_id,
            prompt,
        } => runner.prompt(&agent_id, turn_id, prompt).await,
        LinkFrame::Cancel { agent_id } => runner.cancel(&agent_id).await,
        LinkFrame::Stop {
            agent_id,
            stop_id,
            final_stop,
        } => {
            // Under the gate, so a replaced session stops nothing; the
            // acknowledgement waits outside it, so a slow teardown (or a
            // final stop's push and removal) holds up no other frame.
            let wait = if final_stop {
                AckWait::Final(runner.final_stop(&agent_id))
            } else {
                AckWait::Plain(runner.stop(&agent_id))
            };
            return Applied::Stopping(PendingAck {
                stop_id,
                agent_id,
                wait,
            });
        }
        LinkFrame::Ack { seq } => {
            if let Err(error) = runner
                .with_journal(move |journal| journal.acknowledge(seq))
                .await
            {
                tracing::warn!(%error, "node: closing link after a bad acknowledgement");
                return Applied::Close;
            }
        }
        other => {
            tracing::debug!(frame = ?other.channel(), "node: ignoring unexpected frame");
        }
    }
    drop(gate);
    Applied::Done
}

/// Apply one script-lane frame (nodeProtocol 8) for `binding`'s session, on
/// `link`. A replaced session starts and stops nothing.
async fn apply_script(runner: &Runner, binding: &LeaseBinding, link: &NodeLink, frame: LinkFrame) {
    let gate = runner.link_gate().lock().await;
    if admit(gate.as_ref(), binding, link.is_closed()) == Admission::Replaced {
        tracing::debug!("node: dropping a script frame from a replaced link");
        return;
    }
    match frame {
        LinkFrame::ScriptStart(command) => {
            runner.script_start(link, binding.generation, command).await;
        }
        LinkFrame::ScriptStop(process) => runner.script_stop(link, &process),
        other => {
            tracing::debug!(frame = ?other.channel(), "node: ignoring unexpected script frame");
        }
    }
}

/// Apply one secret frame (nodeProtocol 14, node-link.md § Secrets on the
/// machine) for `binding`'s session and build its one `secretReply`. A
/// replaced session holds, drops and answers nothing.
async fn apply_secret(
    runner: &Runner,
    binding: &LeaseBinding,
    closed: impl Fn() -> bool,
    frame: LinkFrame,
) -> Option<LinkFrame> {
    let gate = runner.link_gate().lock().await;
    if admit(gate.as_ref(), binding, closed()) == Admission::Replaced {
        tracing::debug!("node: dropping a secret frame from a replaced link");
        return None;
    }
    let reply = runner.secret_frame(frame)?;
    drop(gate);
    Some(LinkFrame::SecretReply(reply))
}

/// Apply a `forgeToken` frame (nodeProtocol 15, node-link.md § Forge token
/// on the machine) for `binding`'s session and build its one
/// `forgeTokenReply`. A replaced session holds, drops and answers nothing.
async fn apply_forge(
    runner: &Runner,
    binding: &LeaseBinding,
    closed: impl Fn() -> bool,
    frame: intent_core::node_link::ForgeToken,
) -> Option<LinkFrame> {
    let gate = runner.link_gate().lock().await;
    if admit(gate.as_ref(), binding, closed()) == Admission::Replaced {
        tracing::debug!("node: dropping a forge token frame from a replaced link");
        return None;
    }
    let reply = runner.forge_frame(frame);
    drop(gate);
    Some(LinkFrame::ForgeTokenReply(reply))
}

/// A fresh capabilities report, for [`LinkFrame::Capabilities`].
pub(super) type Report =
    std::sync::Arc<dyn Fn() -> BoxFuture<'static, MachineCapabilities> + Send + Sync>;

/// Serve `link` until it closes.
pub(super) async fn serve_link(
    runner: Runner,
    link: NodeLink,
    mut events: LinkEvents,
    hello: LinkHello,
    report: Report,
) {
    let binding = hello.binding;
    let acked = hello.acked_seq;
    let protocol = NODE_LINK_PROTOCOLS
        .highest_common(hello.protocol)
        .unwrap_or_default();
    match adopt(&runner, &binding, acked, || link.is_closed()).await {
        Adopted::Ready => {
            // Before any frame of this link: a restarted backend's hello
            // drops the secrets the previous process handed over.
            runner.adopt_head(hello.head_session.as_deref(), &hello.lease_agents);
            if protocol < FORGE_TOKEN_PROTOCOL {
                runner.drop_forge();
            }
            runner.head_link().set(link.clone());
            runner.resend_setup_output(&link);
            if protocol >= HOMES_PROTOCOL {
                runner.resend_home_setup_output(&link);
            }
            // The backend listed the agents it still records: the
            // checkouts of any others are orphans.
            // The list is taken now, before any frame of this link (a
            // launch included) is handled.
            if protocol >= FINAL_STOP_PROTOCOL {
                let candidates = runner.sweep_candidates(&hello.lease_agents);
                if !candidates.is_empty() {
                    let runner = runner.clone();
                    // caller-binding: allow — node-local checkout sweep for one bound backend
                    tokio::spawn(async move {
                        runner.sweep_checkouts(candidates).await;
                    });
                }
            }
            // The homes this backend expects here: the others' checkouts
            // are orphans, and leased ones left by a restart reopen.
            let lease_homes = (protocol >= HOMES_PROTOCOL)
                .then_some(hello.lease_homes.as_deref())
                .flatten();
            let swept = runner.adopt_home_lease(lease_homes);
            if let Some(lease) = lease_homes.map(<[_]>::to_vec) {
                let runner = runner.clone();
                // caller-binding: allow — node-local home sweep and resume for one bound backend
                tokio::spawn(async move {
                    runner.sweep_homes(swept).await;
                    runner.resume_leased_homes(&lease).await;
                });
            }
        }
        Adopted::Replaced => {
            tracing::info!("node: link replaced before it was adopted");
            link.close();
            return;
        }
        Adopted::Refused(error) => {
            // Head claims records the node never wrote, or the journal is
            // unusable: refuse the link rather than replay a wrong sequence.
            tracing::warn!(%error, "node: link refused at journal resync");
            link.close();
            return;
        }
    }
    let streamer = {
        let runner = runner.clone();
        let link = link.clone();
        // caller-binding: allow — node-local journal streaming; the node has no service layer
        tokio::spawn(async move { stream_journal(runner, link, acked).await })
    };
    while let Some(event) = events.recv().await {
        match event {
            // A read-only report; it neither needs the link gate nor waits
            // in line with control frames.
            LinkEvent::Frame(LinkFrame::Capabilities) => {
                let link = link.clone();
                let report = report.clone();
                // caller-binding: allow — answers one capabilities request on its link
                tokio::spawn(async move {
                    let capabilities = report().await;
                    let _ = link
                        .send(&LinkFrame::CapabilitiesReply { capabilities })
                        .await;
                });
            }
            // Read-only like the capabilities report: the machine's user
            // skills for a profile, before any checkout exists.
            LinkEvent::Frame(LinkFrame::Skills(query)) if protocol >= SKILLS_PROTOCOL => {
                let link = link.clone();
                let home = runner.home();
                // caller-binding: allow — answers one skills query on its link
                tokio::spawn(async move {
                    let skills = skills::discover(None, home).await;
                    let _ = link
                        .send(&LinkFrame::SkillsReply(SkillsReply {
                            query_id: query.query_id,
                            skills,
                        }))
                        .await;
                });
            }
            // Pushes the agent's checkout now (nodeProtocol 13); the push
            // waits in line with the checkout's other pushes, not with
            // control frames.
            LinkEvent::Frame(LinkFrame::PushWip(request)) => {
                let link = link.clone();
                let runner = runner.clone();
                // caller-binding: allow — answers one push request on its link
                tokio::spawn(async move {
                    let (outcome, error) = runner.push_now(&request.agent_id).await;
                    let _ = link
                        .send(&LinkFrame::PushWipReply(PushWipResult {
                            request_id: request.request_id,
                            agent_id: request.agent_id,
                            outcome,
                            error,
                        }))
                        .await;
                });
            }
            // Reads a piece of an agent's checkout for another agent's pull
            // (nodeProtocol 16); the reply rides the script lane.
            LinkEvent::Frame(LinkFrame::CheckoutRead(read))
                if protocol >= CHECKOUT_FILES_PROTOCOL =>
            {
                let link = link.clone();
                let runner = runner.clone();
                // caller-binding: allow — answers one checkout read on its link
                tokio::spawn(async move {
                    let reply = runner.checkout_read(read).await;
                    let _ = link.send(&LinkFrame::CheckoutReadReply(reply)).await;
                });
            }
            // Quick actions for an orchestrator (nodeProtocol 18): each runs
            // on its own task and answers once; a cancel ends it.
            LinkEvent::Frame(LinkFrame::OneShot(frame)) if protocol >= ONE_SHOT_PROTOCOL => {
                // A replaced session starts nothing, as for script frames.
                let replaced = {
                    let gate = runner.link_gate().lock().await;
                    admit(gate.as_ref(), &binding, link.is_closed()) == Admission::Replaced
                };
                if replaced {
                    tracing::debug!("node: dropping a quick action from a replaced link");
                    continue;
                }
                let link = link.clone();
                let runner = runner.clone();
                // Registered here, before the task runs and under this link's
                // generation, so this link's end always cancels it and never
                // a newer link's.
                let cancelled = runner.begin_one_shot(frame.request_id, binding.generation);
                // caller-binding: allow — answers one quick action on its link
                tokio::spawn(async move {
                    let reply = runner.one_shot(frame, cancelled).await;
                    let _ = link.send(&LinkFrame::OneShotReply(reply)).await;
                });
            }
            LinkEvent::Frame(LinkFrame::OneShotCancel(cancel)) if protocol >= ONE_SHOT_PROTOCOL => {
                runner.cancel_one_shot(cancel.request_id);
            }
            // Workspace homes (nodeProtocol 19): each answers once on its
            // own task; a replaced session starts nothing.
            LinkEvent::Frame(frame @ (LinkFrame::HomeOpen(_) | LinkFrame::HomePush(_)))
                if protocol >= HOMES_PROTOCOL =>
            {
                let replaced = {
                    let gate = runner.link_gate().lock().await;
                    admit(gate.as_ref(), &binding, link.is_closed()) == Admission::Replaced
                };
                if replaced {
                    tracing::debug!("node: dropping a home frame from a replaced link");
                    continue;
                }
                let link = link.clone();
                let runner = runner.clone();
                // caller-binding: allow — answers one home frame on its link
                tokio::spawn(async move {
                    let reply = match frame {
                        LinkFrame::HomeOpen(open) => {
                            LinkFrame::HomeOpenReply(runner.home_open(open).await)
                        }
                        LinkFrame::HomePush(push) => {
                            LinkFrame::HomePushReply(runner.home_push(push).await)
                        }
                        _ => return,
                    };
                    let _ = link.send(&reply).await;
                });
            }
            LinkEvent::Frame(LinkFrame::ForgeToken(frame)) => {
                // Handled in order, so it precedes any launch sent after it.
                if let Some(reply) =
                    apply_forge(&runner, &binding, || link.is_closed(), frame).await
                {
                    let _ = link.send(&reply).await;
                }
            }
            LinkEvent::Frame(
                frame @ (LinkFrame::SecretPut(_)
                | LinkFrame::SecretForget(_)
                | LinkFrame::SecretList(_)),
            ) => {
                if let Some(reply) =
                    apply_secret(&runner, &binding, || link.is_closed(), frame).await
                {
                    let _ = link.send(&reply).await;
                }
            }
            LinkEvent::Frame(frame) if frame.channel() == LinkChannel::Script => {
                apply_script(&runner, &binding, &link, frame).await;
            }
            LinkEvent::Frame(frame) => {
                match apply(&runner, &binding, || link.is_closed(), frame).await {
                    Applied::Close => link.close(),
                    Applied::Stopping(pending) => {
                        let link = link.clone();
                        // caller-binding: allow — acknowledges one applied stop on its link
                        tokio::spawn(async move {
                            // No ack, or a link that closed meanwhile: the
                            // backend resends the stop.
                            if let Some(ack) = pending.ack().await {
                                let _ = link.send(&ack).await;
                            }
                        });
                    }
                    Applied::Done | Applied::Stale => {}
                }
            }
            LinkEvent::Request(request) => {
                let _ = request
                    .respond(LinkOutcome::Err(LinkRpcError {
                        code: METHOD_NOT_FOUND,
                        message: "method not supported by this node".into(),
                        data: None,
                    }))
                    .await;
            }
        }
    }
    streamer.abort();
    // No quick action outlives the link that asked for it.
    runner.cancel_binding_one_shots(binding.generation);
    // No script outlives the link that started it.
    runner.end_binding_scripts(binding.generation).await;
}

/// Send every record after `acked`, then `ReplayComplete`, then each new
/// record as it is appended, until the link closes.
async fn stream_journal(runner: Runner, link: NodeLink, acked: NodeCounter) {
    // Subscribe before the first read, so an append racing it still wakes
    // the loop below.
    let mut appended = runner.appended();
    let mut cursor = acked;
    let mut replaying = true;
    loop {
        let read = runner
            .with_journal(move |journal| Ok((journal.records_after(cursor)?, journal.last_seq())))
            .await;
        let (records, last) = match read {
            Ok(read) => read,
            Err(error) => {
                tracing::error!(%error, "node: journal read failed; closing link");
                link.close();
                return;
            }
        };
        for record in records {
            cursor = record.seq;
            if link.send(&LinkFrame::Journal(record)).await.is_err() {
                return;
            }
        }
        if replaying {
            replaying = false;
            let through_seq = last.max(acked);
            if link
                .send(&LinkFrame::ReplayComplete { through_seq })
                .await
                .is_err()
            {
                return;
            }
        }
        tokio::select! {
            changed = appended.changed() => if changed.is_err() { return },
            () = link.closed() => return,
        }
    }
}

#[cfg(test)]
mod tests;

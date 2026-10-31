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
    LeaseBinding, LinkFrame, LinkHello, LinkOutcome, LinkRpcError, MachineCapabilities,
};
use intent_core::nodes::NodeCounter;
use intent_core::BoxFuture;
use intent_transport::node_link::{LinkEvent, LinkEvents, NodeLink};

use super::runner::Runner;

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
}

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
        LinkFrame::Stop { agent_id } => runner.stop(&agent_id),
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
    match adopt(&runner, &binding, acked, || link.is_closed()).await {
        Adopted::Ready => runner.head_link().set(link.clone()),
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
            LinkEvent::Frame(frame) => {
                if apply(&runner, &binding, || link.is_closed(), frame).await == Applied::Close {
                    link.close();
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

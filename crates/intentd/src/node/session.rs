//! One bound link from head: replay the journal after head's acknowledged
//! sequence, then stream new records live, and turn head's control frames
//! into runner calls.

use intent_core::node_link::{LinkFrame, LinkHello, LinkOutcome, LinkRpcError};
use intent_core::nodes::NodeCounter;
use intent_transport::node_link::{LinkEvent, LinkEvents, NodeLink};

use super::runner::Runner;

/// JSON-RPC "method not found": the node serves no head-to-node requests yet.
const METHOD_NOT_FOUND: i32 = -32601;

/// Serve `link` until it closes.
pub(super) async fn serve_link(
    runner: Runner,
    link: NodeLink,
    mut events: LinkEvents,
    hello: LinkHello,
) {
    let lease_id = hello.binding.lease_id.clone();
    let acked = hello.acked_seq;
    let adopted = runner
        .with_journal(move |journal| {
            if journal.use_lease(&lease_id)? {
                tracing::info!("node: new lease; journal sequence restarted");
            }
            journal.acknowledge(acked)
        })
        .await;
    if let Err(error) = adopted {
        // Head claims records the node never wrote, or the journal is
        // unusable: refuse the link rather than replay a wrong sequence.
        tracing::warn!(%error, "node: link refused at journal resync");
        link.close();
        return;
    }
    let streamer = {
        let runner = runner.clone();
        let link = link.clone();
        // caller-binding: allow — node-local journal streaming; the node has no service layer
        tokio::spawn(async move { stream_journal(runner, link, acked).await })
    };
    while let Some(event) = events.recv().await {
        match event {
            LinkEvent::Frame(frame) => handle_frame(&runner, &link, frame).await,
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

async fn handle_frame(runner: &Runner, link: &NodeLink, frame: LinkFrame) {
    match frame {
        LinkFrame::Launch(command) => runner.launch(command),
        LinkFrame::Prompt {
            agent_id,
            turn_id,
            prompt,
        } => runner.prompt(&agent_id, turn_id, prompt),
        LinkFrame::Cancel { agent_id } => runner.cancel(&agent_id).await,
        LinkFrame::Stop { agent_id } => runner.stop(&agent_id),
        LinkFrame::Ack { seq } => {
            if let Err(error) = runner
                .with_journal(move |journal| journal.acknowledge(seq))
                .await
            {
                tracing::warn!(%error, "node: closing link after a bad acknowledgement");
                link.close();
            }
        }
        other => {
            tracing::debug!(frame = ?other.channel(), "node: ignoring unexpected frame");
        }
    }
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

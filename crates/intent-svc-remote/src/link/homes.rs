//! Workspace homes, link side: the machine's answers to home frames and
//! the resend of its pending home stops while it is linked. The domain side
//! (`crate::homes`) opens, records and settles homes.

use std::sync::Arc;

use intent_core::node_link::{
    HomeOpenReply, HomePushReply, HomeStopRecord, LinkFrame, HOMES_PROTOCOL,
};

use crate::connector::Shared;

/// The machine's answer to a home frame.
#[derive(Debug)]
pub(crate) enum HomeReply {
    Open(HomeOpenReply),
    Push(HomePushReply),
}

/// Send `stop` on the machine's current link, when it speaks
/// nodeProtocol 19; otherwise it goes after the next bind.
pub(crate) async fn send_stop(shared: &Shared, stop: &HomeStopRecord) {
    if shared.current_link().is_none() || shared.protocol() < HOMES_PROTOCOL {
        return;
    }
    if let Err(error) = shared.send(&LinkFrame::HomeStop(stop.frame())).await {
        tracing::info!(home = %stop.home_id, %error, "remote machine: could not send a home stop; it goes after the next bind");
    }
}

/// Make sure one worker resends the machine's pending home stops every
/// `tuning.home_stop_resend` while it has a link, until none is left: a
/// `failed` ack (a push that did not land) or one that never came is
/// retried without a reconnect. Called whenever a stop is sent or fails;
/// a worker that ended (no stop left, or no link) is started again.
pub(crate) fn ensure_stop_resends(shared: &Arc<Shared>) {
    use std::sync::atomic::Ordering;
    if shared.home_stop_resends.swap(true, Ordering::SeqCst) {
        return;
    }
    let shared = shared.clone();
    // caller-binding: allow — resends one machine's pending home stops while it is linked
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(shared.tuning.home_stop_resend).await;
            let stops = pending_while_linked(&shared).await;
            if stops.is_empty() {
                shared.home_stop_resends.store(false, Ordering::SeqCst);
                // A stop sent meanwhile found this worker still running:
                // keep going for it, unless another worker took over.
                let again = !pending_while_linked(&shared).await.is_empty();
                if again && !shared.home_stop_resends.swap(true, Ordering::SeqCst) {
                    continue;
                }
                return;
            }
            for stop in stops {
                send_stop(&shared, &stop).await;
            }
        }
    });
}

/// The machine's pending home stops while it has a link; none without
/// one (the next bind sends them and starts a worker again).
async fn pending_while_linked(shared: &Shared) -> Vec<HomeStopRecord> {
    if shared.current_link().is_none() {
        return Vec::new();
    }
    shared
        .store
        .list_home_stops(&shared.machine_id())
        .await
        .unwrap_or_else(|error| {
            tracing::warn!(%error, "remote machine: could not read its home stops");
            Vec::new()
        })
}

impl Shared {
    /// Whether the machine holds a home of this backend (keeps the link up).
    pub(crate) fn set_holds_homes(&self, holds: bool) {
        self.holds_homes
            .store(holds, std::sync::atomic::Ordering::SeqCst);
    }
}

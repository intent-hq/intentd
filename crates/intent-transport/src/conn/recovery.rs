//! Negotiated generic recovery: ordered controls and physical writer receipts.
use super::{
    conflate, events, json, spawn_forwarder, Arc, ConflationBuffer, ConnSubs, Delivery, Enqueue,
    Event, EventItem, Future, HashMap, HashSet, OutboundSender, Subscription, Value,
};
use futures::FutureExt as _;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::{oneshot, watch};

const DEADLINE: Duration = Duration::from_secs(5);
const INVALID_SET: &str = "Invalid recovery subscription set";

#[derive(Clone)]
pub(super) struct Lifetime(watch::Sender<bool>);
impl Lifetime {
    pub(super) fn new() -> Self {
        Self(watch::channel(false).0)
    }
    fn retire(&self) {
        self.0.send_replace(true);
    }
    fn retired(&self) -> bool {
        *self.0.borrow()
    }
    async fn cancelled(&self) {
        let _ = self.0.subscribe().wait_for(|closed| *closed).await;
    }
}

pub(super) struct Lease {
    pub(super) fences: Vec<intent_services::FenceHandle>,
    pub(super) lifetime: Lifetime,
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.lifetime.retire();
    }
}

struct Receipt {
    identity: Arc<()>,
    ack: oneshot::Sender<()>,
    leases: Vec<Lifetime>,
}

struct ReceiptGuard {
    identity: Arc<()>,
    output: Output,
    frame: String,
}
impl Drop for ReceiptGuard {
    fn drop(&mut self) {
        self.output.cancel_receipt(&self.frame, &self.identity);
    }
}

/// Shared by reader, writer, forwarders and owned fence tasks. Cancellation is
/// out of band: neither it nor a physical-write acknowledgement uses a lane.
#[derive(Clone)]
pub(crate) struct Output {
    closed: Lifetime,
    receipts: Arc<Mutex<HashMap<String, Receipt>>>,
}
impl Default for Output {
    fn default() -> Self {
        Self {
            closed: Lifetime::new(),
            receipts: Arc::default(),
        }
    }
}
impl Output {
    pub(crate) fn close(&self) {
        self.closed.retire();
        self.receipts.lock().expect("receipts").clear();
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.retired()
    }
    pub(crate) async fn cancelled(&self) {
        self.closed.cancelled().await;
    }
    /// Capture control metadata before the writer takes ownership of the frame.
    /// Ordinary traffic does not copy its payload.
    pub(crate) fn prepare_frame(&self, frame: &crate::OutboundFrame) -> PendingWrite {
        frame.public_text().map_or_else(
            || PendingWrite {
                output: self.clone(),
                frame: None,
                leases: Vec::new(),
            },
            |text| self.prepare(text),
        )
    }
    pub(crate) fn prepare(&self, frame: &str) -> PendingWrite {
        let receipts = self.receipts.lock().expect("receipts");
        let (frame, leases) = receipts.get(frame).map_or((None, Vec::new()), |receipt| {
            (Some(frame.to_owned()), receipt.leases.clone())
        });
        PendingWrite {
            output: self.clone(),
            frame,
            leases,
        }
    }
    #[cfg(test)]
    pub(crate) async fn write<F: Future<Output = bool>>(&self, frame: &str, write: F) -> bool {
        self.prepare(frame).finish(write).await
    }
    fn register(
        &self,
        frame: String,
        leases: Vec<Lifetime>,
    ) -> Option<(oneshot::Receiver<()>, ReceiptGuard)> {
        let mut receipts = self.receipts.lock().expect("receipts");
        if receipts.contains_key(&frame) {
            return None;
        }
        let (ack, rx) = oneshot::channel();
        let identity = Arc::new(());
        receipts.insert(
            frame.clone(),
            Receipt {
                identity: identity.clone(),
                ack,
                leases,
            },
        );
        Some((
            rx,
            ReceiptGuard {
                identity,
                output: self.clone(),
                frame,
            },
        ))
    }
    fn cancel_receipt(&self, frame: &str, identity: &Arc<()>) {
        let mut receipts = self.receipts.lock().expect("receipts");
        let Some(receipt) = receipts
            .get(frame)
            .filter(|r| Arc::ptr_eq(&r.identity, identity))
        else {
            // Request IDs may be reused after a physical response. An older
            // coordinator must never remove or cancel the newer receipt.
            return;
        };
        if receipt.leases.is_empty() {
            // A queued loss marker may outlive its retired subscription. It
            // carries no lease authority and must not close its replacement.
            receipts.remove(frame);
        } else {
            // Publish cancellation BEFORE removing a pending fence's guards.
            // Otherwise the writer could prepare it as an ordinary response.
            self.closed.retire();
            receipts.clear();
        }
    }
}

/// Losing a named lease during physical send/flush cancels the writer and its
/// connection; neither the pending frame nor queued frames may survive it.
pub(crate) struct PendingWrite {
    output: Output,
    frame: Option<String>,
    leases: Vec<Lifetime>,
}
impl PendingWrite {
    pub(crate) async fn finish<F: Future<Output = bool>>(self, write: F) -> bool {
        let result = tokio::select! {
            biased;
            () = self.output.cancelled() => false,
            () = retired(&self.leases) => false,
            result = write => result,
        };
        if result {
            if let Some(frame) = self.frame {
                if let Some(receipt) = self
                    .output
                    .receipts
                    .lock()
                    .expect("receipts")
                    .remove(&frame)
                {
                    let _ = receipt.ack.send(());
                }
            }
        } else {
            self.output.close();
        }
        result
    }
}

async fn retired(leases: &[Lifetime]) {
    if leases.is_empty() {
        return std::future::pending().await;
    }
    futures::future::select_all(leases.iter().map(|l| Box::pin(l.cancelled()))).await;
}

pub(super) fn fence(
    id: events::IdInfo,
    params: &serde_json::Map<String, Value>,
    out: &OutboundSender,
    subs: &mut ConnSubs,
) -> bool {
    if !id.present {
        return true;
    }
    let ids = params
        .get("subscriptionIds")
        .and_then(Value::as_array)
        .filter(|ids| !ids.is_empty() && ids.len() <= 64)
        .and_then(|ids| {
            ids.iter()
                .map(|v| v.as_str().filter(|s| !s.is_empty()).map(str::to_owned))
                .collect::<Option<Vec<_>>>()
        });
    let set = ids.as_ref().and_then(|ids| {
        let mut seen = HashSet::new();
        ids.iter()
            .map(|sid| {
                if !seen.insert(sid) {
                    return None;
                }
                let sub = subs.subs.get(sid)?;
                let lease = sub.recovery.as_ref()?;
                (!lease.lifetime.retired() && !sub.handle.is_finished())
                    .then(|| (lease.fences.clone(), lease.lifetime.clone()))
            })
            .collect::<Option<Vec<_>>>()
    });
    subs.fences.retain(|task| !task.is_finished());
    // Bound coordinator ownership independently of the per-receiver controls.
    if subs.fences.len() >= 64 {
        out.recovery.close();
        return false;
    }
    let out = out.clone();
    let started = tokio::time::Instant::now();
    let receipts = set.as_ref().and_then(|set| {
        set.iter()
            .flat_map(|(fences, _)| fences.iter().map(|f| f.request().ok()))
            .collect::<Option<Vec<_>>>()
    });
    let task = spawn_forwarder(async move {
        let work = async {
            let (Some(ids), Some(set), Some(receipts)) = (ids, set, receipts) else {
                let _ = out
                    .send_priority(events::error_frame(&id.echo, -32602, INVALID_SET))
                    .await;
                return;
            };
            let lifetimes: Vec<_> = set.into_iter().map(|(_, life)| life).collect();
            let drained = tokio::select! {
                biased;
                () = retired(&lifetimes) => false,
                result = futures::future::try_join_all(receipts) => result.is_ok(),
            };
            if !drained || lifetimes.iter().any(Lifetime::retired) {
                let _ = out
                    .send_priority(events::error_frame(&id.echo, -32602, INVALID_SET))
                    .await;
                return;
            }
            let frame = events::success_frame(&id.echo, &json!({"subscriptionIds":ids}));
            let Some((receipt, _receipt_guard)) =
                out.recovery.register(frame.clone(), lifetimes.clone())
            else {
                out.recovery.close();
                return;
            };
            let enqueued = tokio::select! {
                biased;
                () = retired(&lifetimes) => false,
                result = out.send_bulk(frame.clone()) => result.is_ok(),
            };
            if !enqueued {
                out.recovery.close();
                return;
            }
            let flushed = tokio::select! {
                biased;
                () = retired(&lifetimes) => false,
                result = receipt => result.is_ok(),
            };
            if !flushed {
                out.recovery.close();
            }
        };
        tokio::select! {
            biased;
            () = out.recovery.cancelled() => {},
            () = tokio::time::sleep_until(started + DEADLINE) => out.recovery.close(),
            () = work => {},
        }
    });
    subs.fences.push(task);
    true
}

/// A membership delivery always wins over an observable completed operation.
/// Capacity waits and authorization reads are cancel-safe and retried after
/// membership invalidation. Nothing is admitted between recheck and enqueue.
enum Guarded<T> {
    Ready(T),
    Membership(Option<Delivery>),
}
async fn guarded<T>(
    future: impl Future<Output = T>,
    membership: &mut Option<Subscription>,
) -> Guarded<T> {
    tokio::select! {
        biased;
        delivery = async { membership.as_mut().expect("guarded").recv_delivery().await }, if membership.is_some() => Guarded::Membership(delivery),
        value = future => Guarded::Ready(value),
    }
}

struct Forwarder {
    gate: Option<events::MembershipGate>,
    membership: Option<Subscription>,
    scope: Option<String>,
    id: String,
    out: OutboundSender,
    buffer: ConflationBuffer<EventItem>,
    generation: u64,
    recovery: bool,
}
impl Forwarder {
    fn membership(&mut self, delivery: Option<Delivery>) -> bool {
        match delivery {
            Some(Delivery::Fence(ack)) => {
                let _ = ack.send(());
                true
            }
            Some(Delivery::Batch(batch)) => {
                let gate = self.gate.as_mut().expect("guarded");
                for event in batch {
                    gate.observe_membership_event(&event);
                    if gate.is_own_unshare(&event)
                        && self.scope.as_deref() == Some(event.workspace_id.as_str())
                    {
                        return false;
                    }
                }
                true
            }
            // Membership continuity is a security input even for legacy leases.
            _ => {
                if let Some(gate) = &mut self.gate {
                    gate.clear();
                }
                self.buffer = ConflationBuffer::new();
                self.out.recovery.close();
                false
            }
        }
    }
    async fn allowed(&mut self, event: &Event) -> Option<bool> {
        loop {
            let Some(gate) = &mut self.gate else {
                return Some(true);
            };
            match guarded(gate.allows(event), &mut self.membership).await {
                Guarded::Ready(allowed) => {
                    // The authorization future may have yielded. Check again
                    // after it completes, before admitting or buffering data.
                    if let Some(delivery) = self
                        .membership
                        .as_mut()
                        .and_then(|sub| sub.recv_delivery().now_or_never())
                    {
                        if !self.membership(delivery) {
                            return None;
                        }
                        continue;
                    }
                    return (!self.out.recovery.is_closed()).then_some(allowed);
                }
                Guarded::Membership(delivery) => {
                    if !self.membership(delivery) {
                        return None;
                    }
                }
            }
        }
    }
    async fn send_item(&mut self, item: EventItem) -> bool {
        let lane = self.out.bulk_sender();
        loop {
            let permit = match guarded(lane.reserve(), &mut self.membership).await {
                Guarded::Ready(Ok(p)) => p,
                Guarded::Ready(Err(_)) => return false,
                Guarded::Membership(d) => {
                    if !self.membership(d) {
                        return false;
                    }
                    continue;
                }
            };
            // Re-authorize buffered frames AFTER acquiring output capacity.
            match self.allowed(item.event()).await {
                Some(true) if !self.out.recovery.is_closed() => {
                    permit.send(item.into_frame(&self.id).into());
                    return true;
                }
                Some(false) => return true,
                _ => return false,
            }
        }
    }
    async fn drain(&mut self) -> bool {
        while let Some(item) = self.buffer.pop() {
            if !self.send_item(item).await {
                return false;
            }
        }
        true
    }
    async fn control(&mut self, frame: crate::OutboundFrame) -> bool {
        let lane = self.out.bulk_sender();
        loop {
            match guarded(lane.reserve(), &mut self.membership).await {
                Guarded::Ready(Ok(p)) if !self.out.recovery.is_closed() => {
                    if let Some(delivery) = self
                        .membership
                        .as_mut()
                        .and_then(|sub| sub.recv_delivery().now_or_never())
                    {
                        if !self.membership(delivery) {
                            return false;
                        }
                        continue;
                    }
                    p.send(frame);
                    return true;
                }
                Guarded::Ready(_) => return false,
                Guarded::Membership(d) => {
                    if !self.membership(d) {
                        return false;
                    }
                }
            }
        }
    }
    async fn loss(&mut self) -> bool {
        let started = tokio::time::Instant::now();
        let Some(generation) = self.generation.checked_add(1) else {
            self.out.recovery.close();
            return false;
        };
        self.generation = generation;
        let frame = json!({"jsonrpc":"2.0","method":"events.lost","params":{
            "subscriptionId":self.id,"generation":generation.to_string(),"scope":{"workspaceId":self.scope}
        }}).to_string();
        let output = self.out.recovery.clone();
        let Some((mut receipt, _receipt_guard)) = output.register(frame.clone(), vec![]) else {
            output.close();
            return false;
        };
        let work = async {
            if !self.drain().await || !self.control(frame.clone().into()).await {
                return false;
            }
            loop {
                match guarded(&mut receipt, &mut self.membership).await {
                    Guarded::Ready(result) => return result.is_ok(),
                    Guarded::Membership(d) => {
                        if !self.membership(d) {
                            return false;
                        }
                    }
                }
            }
        };
        let success = tokio::select! {
            biased;
            () = output.cancelled() => false,
            () = tokio::time::sleep_until(started + DEADLINE) => false,
            result = work => result,
        };
        if !success {
            output.close();
        }
        success
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "connection admission passes the independent subscription policy and lifetime"
)]
pub(super) async fn forward(
    mut subscription: Subscription,
    membership: Option<(events::MembershipGate, Subscription)>,
    scope: Option<String>,
    id: String,
    out: OutboundSender,
    host_removal_control: bool,
    recovery: bool,
    lifetime: Lifetime,
) {
    struct Retire(Lifetime);
    impl Drop for Retire {
        fn drop(&mut self) {
            self.0.retire();
        }
    }
    let _retire = Retire(lifetime.clone());
    let (gate, membership) = membership.map_or((None, None), |(g, s)| (Some(g), Some(s)));
    let output = out.recovery.clone();
    let mut f = Forwarder {
        gate,
        membership,
        scope,
        id,
        out,
        buffer: ConflationBuffer::new(),
        generation: 0,
        recovery,
    };
    let run = async {
        loop {
            enum Input {
                Delivery(Option<Delivery>),
                Drain,
            }
            let lane = f.out.bulk_sender();
            let next = async {
                tokio::select! {
                    biased;
                    permit = lane.reserve(), if !f.buffer.is_empty() => { drop(permit); Input::Drain },
                    delivery = subscription.recv_delivery() => Input::Delivery(delivery),
                }
            };
            let input = match guarded(next, &mut f.membership).await {
                Guarded::Membership(d) => {
                    if !f.membership(d) {
                        return;
                    }
                    continue;
                }
                Guarded::Ready(input) => input,
            };
            match input {
                Input::Drain => {
                    if let Some(item) = f.buffer.pop() {
                        if !f.send_item(item).await {
                            return;
                        }
                    }
                }
                Input::Delivery(None) => {
                    let _ = f.drain().await;
                    return;
                }
                Input::Delivery(Some(Delivery::Fence(ack))) => {
                    if !f.drain().await {
                        return;
                    }
                    let _ = ack.send(());
                }
                Input::Delivery(Some(Delivery::Lagged(_))) => {
                    if f.recovery && !f.loss().await {
                        return;
                    }
                }
                Input::Delivery(Some(Delivery::HistoryChanged)) => {}
                Input::Delivery(Some(Delivery::Monitor(output))) => {
                    if !f.drain().await
                        || !f
                            .control(crate::OutboundFrame::private(output.into_output()))
                            .await
                    {
                        return;
                    }
                }
                Input::Delivery(Some(Delivery::Batch(batch))) => {
                    for mut event in batch {
                        if host_removal_control
                            && event.event_type == intent_core::events::HOST_MEMBERS_CHANGED
                            && event.data["action"] == "removed"
                            && crate::context::current_caller()
                                .and_then(|c| c.principal_id().cloned())
                                .is_some_and(|id| event.data["principalId"] == id.0)
                        {
                            continue;
                        }
                        match f.allowed(&event).await {
                            Some(true) => {}
                            Some(false) => continue,
                            None => return,
                        }
                        if f.out.recovery.is_closed() {
                            return;
                        }
                        if f.gate.is_some() {
                            events::project_queue_event_for_current_caller(&mut event);
                        }
                        if let Some(key) = conflate::event_key(&event) {
                            let item = EventItem::new(&key, event);
                            match conflate::offer(&mut f.buffer, key, item, &lane, |item| {
                                item.into_frame(&f.id)
                            }) {
                                Enqueue::Closed => return,
                                Enqueue::Sent | Enqueue::Buffered => {}
                                Enqueue::Overflow(item) => {
                                    if !f.drain().await || !f.send_item(item).await {
                                        return;
                                    }
                                }
                            }
                        } else if !f.drain().await || !f.send_item(EventItem::Latest(event)).await {
                            return;
                        }
                    }
                }
            }
        }
    };
    tokio::select! {
        biased;
        () = output.cancelled() => {},
        () = lifetime.cancelled() => {},
        () = run => {},
    }
}

#[cfg(test)]
mod tests;

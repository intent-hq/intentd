//! In-process ownership of qualified MCP results. No runtime installs a policy
//! by default. These mechanics neither discover a target nor grant authority.
//!
//! A real producer must reserve BEFORE acquiring private data, bind the actual
//! original read evidence, and retain its own request/source authority. Each
//! admission must validate the complete supplied set atomically. Calling several
//! independent fences, or checking today's owner, does not meet that contract.

use std::any::Any;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use intent_js::{
    BoxFuture, GuardedHostFn, HostAdmissionOutcome, HostCallId, HostFn, HostReply,
    HostReplyAdmission, HostTransferReceipt, PreparedHostTransfer,
};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use super::repository_guidance::{BridgeResponse, ConnectionToken, PreparedBridgeLine};
use super::request_context::CapturedRequestContext;
use super::request_context::McpContextFuture;

/// Maximum original qualified acquisitions in one invocation. Ordinary calls
/// consume no records. Records are never evicted, even if JS ignores a reply.
pub const MAX_PRIVATE_READS: usize = 64;
pub(crate) const REFUSAL: &str = "Private result delivery refused";

/// Trusted, original request policy. Implementations must not detach any body
/// or admission. The default endpoint factory returns `None`.
pub trait McpPrivatePolicy: Send + Sync {
    /// Capture the original host scope synchronously, before permission checks
    /// or any host await. Its qualified entry must use this call's reservation.
    fn capture_host(&self, call: McpHostCall) -> Box<dyn McpPrivateHostScope>;

    /// Capture only an optional local leaf of this original request. Absence
    /// disables qualified preparation before its source is invoked. A scope
    /// must restore the original service context without the required request's
    /// cancellation wrapper: optional drop must not retire that parent.
    fn capture_optional_context(&self) -> Option<Box<dyn McpOptionalContextScope>> {
        None
    }

    /// One final output admission. The default omits guidance, preserving the
    /// required admission for a nonempty seal and ordinary output for an empty
    /// seal. Real implementations must jointly validate original required and
    /// optional facts, never nest independent same-parent fences.
    fn admit_optional<'a>(
        &'a self,
        boundary: &'a McpPrivateBoundary,
        sealed: McpSealedReads<'a>,
        _optional: &'a McpOptionalEvidence,
        packet: PreparedMcpVariants<'a>,
    ) -> BoxFuture<'a, McpPrivateAdmission> {
        let base = packet.without_guidance();
        if sealed.is_empty() {
            Box::pin(async move { base.transfer(boundary) })
        } else {
            self.admit(boundary, sealed.records(), base)
        }
    }

    /// Admit one prebuilt effect against EVERY supplied original read. Release
    /// all guards before resolving. `packet.transfer(boundary)` is the sole
    /// consuming action: it performs only ownership transfer. A borrowed packet
    /// cannot be detached, and leaves an unused original slot with its owner.
    ///
    /// The real authority implementation must open a fresh child of the SAME
    /// request, retain source leases, acquire shared ancestors once, order the
    /// distinct operations, and use the provider's atomic batch eligibility.
    /// This crate supplies none of those authority checks.
    fn admit<'a>(
        &'a self,
        boundary: &'a McpPrivateBoundary,
        originals: &'a [McpReadEvidence],
        packet: PreparedMcpTransfer<'a>,
    ) -> BoxFuture<'a, McpPrivateAdmission>;
}

/// Optional-only execution context. Construct/drop must retain the original
/// allocation and cancel only its optional leaf. No authority is supplied here.
pub trait McpOptionalContextScope: Send + Sync {
    fn scope<'a>(&'a self, body: McpContextFuture<'a>) -> McpContextFuture<'a>;
}

/// Producer-owned original evidence, never serialized or placed in a writer
/// packet. Packaging a value is not a grant; the original policy validates it.
pub struct McpOptionalEvidence(Box<dyn Any + Send + Sync>);

impl McpOptionalEvidence {
    #[must_use]
    pub fn new<T: Any + Send + Sync>(original: T) -> Self {
        Self(Box::new(original))
    }

    #[must_use]
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.0.downcast_ref()
    }
}

/// ACP-created proof of a complete successful seal. An empty slice is distinct
/// from a failed seal; there is no public constructor or subset operation.
pub struct McpSealedReads<'a> {
    _original: &'a Invocation,
    records: &'a [McpReadEvidence],
}

impl<'a> McpSealedReads<'a> {
    #[must_use]
    pub fn records(&self) -> &'a [McpReadEvidence] {
        self.records
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// A decision made only inside the original consuming admission.
pub enum McpGuidanceChoice {
    WithoutGuidance,
    WithGuidance,
}

/// One borrowed original slot and one prebuilt choice. The policy cannot
/// replace either payload, serialize, reserve again or detach the action.
#[must_use]
pub struct PreparedMcpVariants<'a> {
    identity: Arc<()>,
    action: Box<dyn FnOnce(McpGuidanceChoice) -> Result<Effect, McpPrivateAdmission> + Send + 'a>,
}

impl<'a> PreparedMcpVariants<'a> {
    /// Permanently consume the ability to enrich before required admission.
    pub fn without_guidance(self) -> PreparedMcpTransfer<'a> {
        PreparedMcpTransfer {
            identity: self.identity,
            action: Box::new(move || (self.action)(McpGuidanceChoice::WithoutGuidance)),
        }
    }

    /// Move a prebuilt variant once. This action performs no acquisition,
    /// serialization, I/O, await, spawn or authority-bearing destruction.
    #[must_use]
    pub fn transfer(
        self,
        original: &McpPrivateBoundary,
        choice: McpGuidanceChoice,
    ) -> McpPrivateAdmission {
        if !Arc::ptr_eq(&self.identity, &original.identity) {
            return McpPrivateAdmission::ForeignBoundary;
        }
        match (self.action)(choice) {
            Ok(effect) => McpPrivateAdmission::Transferred(McpTransferReceipt {
                identity: self.identity,
                effect,
            }),
            Err(result) => result,
        }
    }
}

/// Scope exactly one original host body; never skip, replace, retry or spawn it.
pub trait McpPrivateHostScope: Send + Sync {
    /// The producer can bind its private task-local context around this body.
    fn scope<'a>(&'a self, body: McpContextFuture<'a>) -> McpContextFuture<'a>;
}

/// Distinct effect boundaries. These labels are not authorization or identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpPrivateBoundaryKind {
    HostPromise,
    ArtifactStart,
    Attachments,
    TcpResponse,
    DirectResponse,
}

/// Original allocation, not a caller-selected string or reusable numeric ID.
pub struct McpPrivateBoundary {
    identity: Arc<()>,
    kind: McpPrivateBoundaryKind,
}

impl McpPrivateBoundary {
    /// Name the effect being admitted, without exposing its private payload.
    #[must_use]
    pub const fn kind(&self) -> McpPrivateBoundaryKind {
        self.kind
    }
}

/// Opaque original evidence, constructed only by consuming a reservation.
pub struct McpReadEvidence(Arc<dyn Any + Send + Sync>);

impl McpReadEvidence {
    /// Retrieve the trusted producer's own type; no JSON interpretation occurs.
    #[must_use]
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.0.downcast_ref()
    }
}

enum Effect {
    Host(HostTransferReceipt),
    Output,
}

/// Receipt created only by the original consuming packet. Not cloneable.
pub struct McpTransferReceipt {
    identity: Arc<()>,
    effect: Effect,
}

/// Admission and consumer closure remain separate from the actual host outcome.
pub enum McpPrivateAdmission {
    Transferred(McpTransferReceipt),
    ConsumerClosed,
    ForeignBoundary,
    Refused,
}

/// Prebuilt, one-use ownership transfer. No payload accessor or constructor is
/// available to policy implementations, and the original slot cannot change.
#[must_use]
pub struct PreparedMcpTransfer<'a> {
    identity: Arc<()>,
    action: Box<dyn FnOnce() -> Result<Effect, McpPrivateAdmission> + Send + 'a>,
}

impl PreparedMcpTransfer<'_> {
    /// Pure move into the original slot. Do not execute JS, serialize, await,
    /// reserve a queue slot, do I/O, spawn, or acquire another lock here.
    #[must_use]
    pub fn transfer(self, original: &McpPrivateBoundary) -> McpPrivateAdmission {
        if !Arc::ptr_eq(&self.identity, &original.identity) {
            return McpPrivateAdmission::ForeignBoundary;
        }
        match (self.action)() {
            Ok(effect) => McpPrivateAdmission::Transferred(McpTransferReceipt {
                identity: self.identity,
                effect,
            }),
            Err(reason) => reason,
        }
    }
}

/// Reservation failures are trusted control diagnostics, never provider errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrivateReadRefusal {
    Limit,
    Closed,
}

struct Record {
    host: Arc<AtomicU8>,
    evidence: Option<McpReadEvidence>,
}

#[derive(Default)]
struct Ledger {
    closed: bool,
    failed: bool,
    records: Vec<Record>,
    sealed: Option<Arc<[McpReadEvidence]>>,
}

struct Invocation {
    policy: Arc<dyn McpPrivatePolicy>,
    ledger: Mutex<Ledger>,
    deadline: Instant,
    artifact: ArtifactAccounting,
}

/// One original writer job's factual outcome, separate from output eligibility.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactOutcome {
    NotStarted = 0,
    Unknown = 1,
    Written = 2,
    Failed = 3,
}

#[derive(Clone, Default)]
pub(crate) struct ArtifactAccounting(Arc<AtomicU8>);

impl ArtifactAccounting {
    pub(crate) fn record(&self, outcome: ArtifactOutcome) {
        self.0.store(outcome as u8, Ordering::Release);
    }

    #[cfg(test)]
    fn outcome(&self) -> ArtifactOutcome {
        match self.0.load(Ordering::Acquire) {
            0 => ArtifactOutcome::NotStarted,
            2 => ArtifactOutcome::Written,
            3 => ArtifactOutcome::Failed,
            _ => ArtifactOutcome::Unknown,
        }
    }
}

/// Handle minted by ACP for one engine-created host call. Clones refer to the
/// same original call; neither constructors nor identity serialization exist.
#[derive(Clone)]
pub struct McpHostCall {
    invocation: Arc<Invocation>,
    status: Arc<AtomicU8>,
}

impl McpHostCall {
    /// Reserve BEFORE any possibly private read (including cache/subreads).
    /// The 65th reservation refuses without acquiring or evicting anything.
    ///
    /// # Errors
    /// Returns `Limit` at the bound, or `Closed` for a closed/failed invocation.
    ///
    /// # Panics
    /// Panics if an internal ledger invariant poisoned its mutex. Producer code
    /// is never called while holding that mutex.
    pub fn reserve(&self) -> Result<McpReadReservation, PrivateReadRefusal> {
        let mut ledger = self.invocation.ledger.lock().unwrap();
        if ledger.closed || ledger.failed || self.status.load(Ordering::Acquire) != 0 {
            return Err(PrivateReadRefusal::Closed);
        }
        if ledger.records.len() == MAX_PRIVATE_READS {
            ledger.failed = true;
            return Err(PrivateReadRefusal::Limit);
        }
        let index = ledger.records.len();
        ledger.records.push(Record {
            host: self.status.clone(),
            evidence: None,
        });
        Ok(McpReadReservation {
            call: self.clone(),
            index,
            bound: false,
        })
    }
}

/// Original nonclone acquisition slot. Dropping it unbound prevents successful
/// sealing. Error outcomes also need evidence; they can contain private bytes.
#[must_use]
pub struct McpReadReservation {
    call: McpHostCall,
    index: usize,
    bound: bool,
}

impl McpReadReservation {
    /// Bind fixed original facts/handles, not credentials or provider payloads.
    ///
    /// # Errors
    /// Returns `Closed` if the original invocation or host call has ended.
    ///
    /// # Panics
    /// Panics if an internal ledger invariant poisoned its mutex.
    pub fn bind(mut self, evidence: Arc<dyn Any + Send + Sync>) -> Result<(), PrivateReadRefusal> {
        let mut ledger = self.call.invocation.ledger.lock().unwrap();
        if ledger.closed || ledger.failed || self.call.status.load(Ordering::Acquire) != 0 {
            return Err(PrivateReadRefusal::Closed);
        }
        ledger.records[self.index].evidence = Some(McpReadEvidence(evidence));
        self.bound = true;
        Ok(())
    }
}

impl Drop for McpReadReservation {
    fn drop(&mut self) {
        if !self.bound {
            self.call.invocation.ledger.lock().unwrap().failed = true;
        }
    }
}

struct HostCompletion(McpHostCall);

enum HostDisposition {
    Ordinary,
    Qualified(Vec<McpReadEvidence>),
    Refused,
}

impl HostCompletion {
    fn finish(&self) -> HostDisposition {
        let mut ledger = self.0.invocation.ledger.lock().unwrap();
        let mut originals = Vec::new();
        for record in &ledger.records {
            if Arc::ptr_eq(&record.host, &self.0.status) {
                let Some(evidence) = &record.evidence else {
                    ledger.failed = true;
                    return HostDisposition::Refused;
                };
                originals.push(McpReadEvidence(evidence.0.clone()));
            }
        }
        if ledger.failed {
            HostDisposition::Refused
        } else if originals.is_empty() {
            self.0.status.store(1, Ordering::Release);
            HostDisposition::Ordinary
        } else {
            self.0.status.store(3, Ordering::Release);
            HostDisposition::Qualified(originals)
        }
    }
}

impl Drop for HostCompletion {
    fn drop(&mut self) {
        // A reserved call dropped before its actual completion cannot disappear
        // from the sealed set. Ordinary unpolled calls have no obligations.
        let _ = self
            .0
            .status
            .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire);
    }
}

#[derive(Clone)]
pub(crate) struct McpPrivateInvocation(Arc<Invocation>);

tokio::task_local! {
    static CURRENT: Option<McpPrivateInvocation>;
}

impl McpPrivateInvocation {
    pub(crate) fn new(policy: Arc<dyn McpPrivatePolicy>, budget: Duration) -> Self {
        Self(Arc::new(Invocation {
            policy,
            ledger: Mutex::new(Ledger::default()),
            deadline: Instant::now() + budget,
            artifact: ArtifactAccounting::default(),
        }))
    }

    pub(crate) fn current() -> Option<Self> {
        CURRENT.try_with(Clone::clone).ok().flatten()
    }

    pub(crate) async fn scope<T>(invocation: Option<Self>, body: impl Future<Output = T>) -> T {
        CURRENT.scope(invocation, body).await
    }

    fn fail(&self) {
        self.0.ledger.lock().unwrap().failed = true;
    }

    pub(super) fn capture_optional_context(
        &self,
        preparation_budget: Duration,
    ) -> Option<Box<dyn McpOptionalContextScope>> {
        // Omit before preparation when its full separate budget no longer fits.
        // Required delivery still performs its own deadline/authority check.
        if Instant::now() + preparation_budget >= self.0.deadline {
            return None;
        }
        catch_unwind(AssertUnwindSafe(|| {
            self.0.policy.capture_optional_context()
        }))
        .ok()
        .flatten()
    }

    async fn admit_optional<'a>(
        &'a self,
        boundary: &'a McpPrivateBoundary,
        records: &'a [McpReadEvidence],
        evidence: &'a McpOptionalEvidence,
        action: impl FnOnce(McpGuidanceChoice) -> Result<Effect, McpPrivateAdmission> + Send + 'a,
    ) -> McpPrivateAdmission {
        if self.0.ledger.lock().unwrap().failed || Instant::now() >= self.0.deadline {
            return McpPrivateAdmission::Refused;
        }
        let sealed = McpSealedReads {
            _original: &self.0,
            records,
        };
        let packet = PreparedMcpVariants {
            identity: boundary.identity.clone(),
            action: Box::new(action),
        };
        let pending = async {
            self.0
                .policy
                .admit_optional(boundary, sealed, evidence, packet)
                .await
        };
        match tokio::time::timeout_at(self.0.deadline, catch_future(pending)).await {
            Ok(Ok(McpPrivateAdmission::Transferred(receipt)))
                if Arc::ptr_eq(&receipt.identity, &boundary.identity) =>
            {
                McpPrivateAdmission::Transferred(receipt)
            }
            Ok(Ok(McpPrivateAdmission::ConsumerClosed)) => McpPrivateAdmission::ConsumerClosed,
            _ => McpPrivateAdmission::Refused,
        }
    }

    pub(crate) fn guarded_host(&self, ordinary: HostFn) -> GuardedHostFn {
        let invocation = self.clone();
        Arc::new(move |arg, identity| {
            let call = McpHostCall {
                invocation: invocation.0.clone(),
                status: Arc::new(AtomicU8::new(0)),
            };
            let completion = HostCompletion(call.clone());
            // This capture precedes creation/polling of the ordinary host future
            // and therefore agent_is_retired and every other permission await.
            let captured =
                catch_unwind(AssertUnwindSafe(|| invocation.0.policy.capture_host(call)));
            let ordinary = ordinary.clone();
            let invocation = invocation.clone();
            Box::pin(async move {
                let Ok(scope) = captured else {
                    invocation.fail();
                    return HostReply::Ordinary(Err(REFUSAL.into()));
                };
                let mut outcome = None;
                let scoped = async {
                    scope
                        .scope(Box::pin(async { outcome = Some(ordinary(arg).await) }))
                        .await;
                };
                if catch_future(scoped).await.is_err() || outcome.is_none() {
                    invocation.fail();
                    return HostReply::Ordinary(Err(REFUSAL.into()));
                }
                let outcome = outcome.expect("checked original host completion");
                match completion.finish() {
                    HostDisposition::Ordinary => HostReply::Ordinary(outcome),
                    HostDisposition::Refused => HostReply::Ordinary(Err(REFUSAL.into())),
                    HostDisposition::Qualified(originals) => HostReply::Guarded {
                        outcome,
                        admission: Box::new(HostAdmission {
                            invocation,
                            originals,
                            identity,
                            status: completion.0.status.clone(),
                        }),
                    },
                }
            })
        })
    }

    /// Called after engine cleanup. All bound records survive JS transforms;
    /// outstanding reservations/calls and late appends fail closed.
    pub(crate) fn seal(&self) -> Result<Option<SealedPrivateOutput>, ()> {
        let mut ledger = self.0.ledger.lock().unwrap();
        ledger.closed = true;
        if ledger.failed
            || ledger
                .records
                .iter()
                .any(|record| record.evidence.is_none() || record.host.load(Ordering::Acquire) != 1)
        {
            ledger.failed = true;
            return Err(());
        }
        if ledger.records.is_empty() {
            return Ok(None);
        }
        let originals = ledger.sealed.get_or_insert_with(|| Arc::from([])).clone();
        let originals = if originals.is_empty() {
            let originals: Arc<[McpReadEvidence]> = ledger
                .records
                .iter()
                .map(|record| McpReadEvidence(record.evidence.as_ref().unwrap().0.clone()))
                .collect();
            ledger.sealed = Some(originals.clone());
            originals
        } else {
            originals
        };
        Ok(Some(SealedPrivateOutput {
            invocation: self.clone(),
            originals,
        }))
    }

    async fn admit<'a>(
        &'a self,
        boundary: &'a McpPrivateBoundary,
        originals: &'a [McpReadEvidence],
        action: impl FnOnce() -> Result<Effect, McpPrivateAdmission> + Send + 'a,
    ) -> McpPrivateAdmission {
        if self.0.ledger.lock().unwrap().failed || Instant::now() >= self.0.deadline {
            return McpPrivateAdmission::Refused;
        }
        let packet = PreparedMcpTransfer {
            identity: boundary.identity.clone(),
            action: Box::new(action),
        };
        let pending = async { self.0.policy.admit(boundary, originals, packet).await };
        match tokio::time::timeout_at(self.0.deadline, catch_future(pending)).await {
            Ok(Ok(McpPrivateAdmission::Transferred(receipt)))
                if Arc::ptr_eq(&receipt.identity, &boundary.identity) =>
            {
                McpPrivateAdmission::Transferred(receipt)
            }
            Ok(Ok(McpPrivateAdmission::ConsumerClosed)) => McpPrivateAdmission::ConsumerClosed,
            _ => McpPrivateAdmission::Refused,
        }
    }
}

struct HostAdmission {
    invocation: McpPrivateInvocation,
    originals: Vec<McpReadEvidence>,
    identity: HostCallId,
    status: Arc<AtomicU8>,
}

impl Drop for HostAdmission {
    fn drop(&mut self) {
        let _ = self
            .status
            .compare_exchange(3, 2, Ordering::AcqRel, Ordering::Acquire);
    }
}

impl HostReplyAdmission for HostAdmission {
    fn admit(
        self: Box<Self>,
        packet: PreparedHostTransfer,
    ) -> BoxFuture<'static, HostAdmissionOutcome> {
        Box::pin(async move {
            let boundary = boundary(McpPrivateBoundaryKind::HostPromise);
            let result = self
                .invocation
                .admit(&boundary, &self.originals, || {
                    match packet.transfer(&self.identity) {
                        HostAdmissionOutcome::Transferred(receipt) => Ok(Effect::Host(receipt)),
                        HostAdmissionOutcome::ConsumerClosed => {
                            Err(McpPrivateAdmission::ConsumerClosed)
                        }
                        HostAdmissionOutcome::ForeignCall => {
                            Err(McpPrivateAdmission::ForeignBoundary)
                        }
                        HostAdmissionOutcome::Refused => Err(McpPrivateAdmission::Refused),
                    }
                })
                .await;
            match result {
                McpPrivateAdmission::Transferred(McpTransferReceipt {
                    effect: Effect::Host(receipt),
                    ..
                }) => {
                    self.status.store(1, Ordering::Release);
                    HostAdmissionOutcome::Transferred(receipt)
                }
                McpPrivateAdmission::ConsumerClosed => {
                    self.invocation.fail();
                    HostAdmissionOutcome::ConsumerClosed
                }
                _ => {
                    self.invocation.fail();
                    HostAdmissionOutcome::Refused
                }
            }
        })
    }
}

fn boundary(kind: McpPrivateBoundaryKind) -> McpPrivateBoundary {
    McpPrivateBoundary {
        identity: Arc::new(()),
        kind,
    }
}

/// A sealed original set. No subset, overwrite, append, replacement owner, or
/// last-result API exists. Each call below asks for a fresh admission.
#[derive(Clone)]
pub(crate) struct SealedPrivateOutput {
    invocation: McpPrivateInvocation,
    originals: Arc<[McpReadEvidence]>,
}

impl SealedPrivateOutput {
    pub(crate) fn artifact_accounting(&self) -> ArtifactAccounting {
        self.invocation.0.artifact.clone()
    }

    pub(crate) async fn transfer<T: Send>(
        &self,
        kind: McpPrivateBoundaryKind,
        value: T,
    ) -> Result<T, ()> {
        self.transfer_prepared(kind, || value).await
    }

    /// `prepare` may only move an already prepared packet into its owning
    /// envelope (for example Prepared -> Started), with no effectful work.
    pub(crate) async fn transfer_prepared<T: Send>(
        &self,
        kind: McpPrivateBoundaryKind,
        prepare: impl FnOnce() -> T + Send,
    ) -> Result<T, ()> {
        let (tx, rx) = oneshot::channel();
        let boundary = boundary(kind);
        let result = self
            .invocation
            .admit(&boundary, &self.originals, || {
                tx.send(prepare())
                    .map(|()| Effect::Output)
                    .map_err(|_| McpPrivateAdmission::ConsumerClosed)
            })
            .await;
        if matches!(result, McpPrivateAdmission::Transferred(_)) {
            rx.await.map_err(|_| ())
        } else {
            self.invocation.fail();
            Err(())
        }
    }
}

/// Separate carrier preserves the existing `BridgeResponse` shape, including
/// optional guidance. Typed state, never JSON content, chooses mandatory gating.
pub(crate) struct DeliveryResponse {
    response: BridgeResponse,
    output: Option<SealedPrivateOutput>,
    context: Option<CapturedRequestContext>,
    // Constructed only after a successful empty seal, only for qualified
    // optional output. Failed seals use the fixed control response instead.
    sealed_empty: Option<McpPrivateInvocation>,
    // Ownership only: successful-empty output must keep its original scope
    // alive after guidance preparation without re-entering required execution.
    retained_empty_context: Option<CapturedRequestContext>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeliveryOutcome {
    Admitted,
    Refused,
    ConsumerClosed,
}

impl DeliveryResponse {
    pub(crate) fn ordinary(response: BridgeResponse) -> Self {
        Self {
            response,
            output: None,
            context: None,
            sealed_empty: None,
            retained_empty_context: None,
        }
    }

    pub(crate) fn from_context(response: BridgeResponse, context: &CapturedRequestContext) -> Self {
        match context
            .private_invocation
            .as_ref()
            .map(McpPrivateInvocation::seal)
            .transpose()
        {
            Ok(output) => {
                let output = output.flatten();
                let sealed_empty = (output.is_none() && response.requires_qualified_guidance())
                    .then(|| context.private_invocation.clone())
                    .flatten();
                let retained_empty_context = sealed_empty.as_ref().map(|_| context.clone());
                let context = output.is_some().then(|| context.clone());
                Self {
                    response,
                    output,
                    context,
                    sealed_empty,
                    retained_empty_context,
                }
            }
            Err(()) => Self::ordinary(BridgeResponse::plain(refusal_response(&response.value))),
        }
    }

    pub(crate) async fn into_direct(mut self) -> BridgeResponse {
        if let Some(context) = self.context.take() {
            context.run(self.direct_inner()).await
        } else {
            self.direct_inner().await
        }
    }

    async fn direct_inner(self) -> BridgeResponse {
        let Some(output) = self.output else {
            return self.response;
        };
        let control = refusal_response(&self.response.value);
        output
            .transfer(McpPrivateBoundaryKind::DirectResponse, self.response)
            .await
            .unwrap_or_else(|()| BridgeResponse::plain(control))
    }

    pub(crate) async fn enqueue(
        mut self,
        sender: mpsc::Sender<PreparedBridgeLine>,
        connection: &ConnectionToken,
    ) -> DeliveryOutcome {
        // Drop only after the original decision (or its cancellation), outside
        // admission/transfer guards. Never move this owner into the writer packet.
        let _retained_empty_context = self.retained_empty_context.take();
        if let Some(context) = self.context.take() {
            context.run(self.enqueue_inner(sender, connection)).await
        } else if let Some(invocation) = self.sealed_empty.clone() {
            // Empty ordinary output never re-enters a required scope. Its
            // original policy/caller remain available solely for optional
            // admission, whose failure can only omit the sidecar.
            let caller = self.response.qualified_caller();
            let scoped = McpPrivateInvocation::scope(
                Some(invocation),
                self.enqueue_inner(sender, connection),
            );
            match caller {
                Some(caller) => intent_core::with_caller(caller, scoped).await,
                None => scoped.await,
            }
        } else {
            self.enqueue_inner(sender, connection).await
        }
    }

    async fn enqueue_inner(
        self,
        sender: mpsc::Sender<PreparedBridgeLine>,
        connection: &ConnectionToken,
    ) -> DeliveryOutcome {
        let control = PreparedBridgeLine::plain(refusal_response(&self.response.value));
        let (prepared, evidence) = self
            .response
            .prepare_guidance()
            .await
            .prepare_delivery_line();
        if let Some(evidence) = evidence {
            return Self::enqueue_optional(
                self.output.as_ref(),
                self.sealed_empty.as_ref(),
                prepared,
                &evidence,
                control,
                sender,
                connection,
            )
            .await;
        }
        let Some(output) = self.output else {
            return if sender.send(prepared).await.is_ok() {
                DeliveryOutcome::Admitted
            } else {
                DeliveryOutcome::ConsumerClosed
            };
        };
        // Keep ordinary queue backpressure and reserve once, outside any fence.
        // If the original budget expires while waiting, admission refuses and
        // this SAME slot carries the control error instead of losing the reply.
        // Cancellation/closure discards only this original packet/reservation.
        let Ok(permit) = sender.reserve_owned().await else {
            return DeliveryOutcome::ConsumerClosed;
        };
        let mut original_slot = Some(permit);
        let boundary = boundary(McpPrivateBoundaryKind::TcpResponse);
        let result = output
            .invocation
            .admit(&boundary, &output.originals, || {
                if !connection.is_live() {
                    return Err(McpPrivateAdmission::ConsumerClosed);
                }
                original_slot
                    .take()
                    .expect("one original response permit")
                    .send(prepared);
                Ok(Effect::Output)
            })
            .await;
        if !matches!(result, McpPrivateAdmission::Transferred(_)) {
            // If an effect already happened it cannot be recalled. Otherwise
            // the SAME unused permit carries only the prebuilt control error.
            if let Some(permit) = original_slot.take() {
                if connection.is_live() {
                    permit.send(control);
                    return DeliveryOutcome::Refused;
                }
                return DeliveryOutcome::ConsumerClosed;
            }
        }
        DeliveryOutcome::Admitted
    }

    async fn enqueue_optional(
        output: Option<&SealedPrivateOutput>,
        sealed_empty: Option<&McpPrivateInvocation>,
        prepared: PreparedBridgeLine,
        evidence: &McpOptionalEvidence,
        control: PreparedBridgeLine,
        sender: mpsc::Sender<PreparedBridgeLine>,
        connection: &ConnectionToken,
    ) -> DeliveryOutcome {
        let invocation = output.map(|o| &o.invocation).or(sealed_empty);
        let Some(invocation) = invocation else {
            // No successful original policy capture: never publish qualified
            // guidance through an ordinary/legacy helper.
            return if sender.send(prepared.without_guidance()).await.is_ok() {
                DeliveryOutcome::Admitted
            } else {
                DeliveryOutcome::ConsumerClosed
            };
        };
        let Ok(permit) = sender.reserve_owned().await else {
            return DeliveryOutcome::ConsumerClosed;
        };
        let mut original_slot = Some(permit);
        let mut original_packet = Some(prepared);
        let records = output.map_or(&[][..], |o| o.originals.as_ref());
        let boundary = boundary(McpPrivateBoundaryKind::TcpResponse);
        let result = invocation
            .admit_optional(&boundary, records, evidence, |choice| {
                if output.is_some() && !connection.is_live() {
                    return Err(McpPrivateAdmission::ConsumerClosed);
                }
                let packet = original_packet.take().expect("one original variant packet");
                let packet = match choice {
                    McpGuidanceChoice::WithoutGuidance => packet.without_guidance(),
                    McpGuidanceChoice::WithGuidance => packet,
                };
                original_slot
                    .take()
                    .expect("one original response permit")
                    .send(packet);
                Ok(Effect::Output)
            })
            .await;
        if !matches!(result, McpPrivateAdmission::Transferred(_)) {
            if let Some(permit) = original_slot.take() {
                if output.is_none() {
                    // ACP proved empty; optional failure cannot gate the
                    // ordinary result or impose endpoint/required-read gates.
                    permit.send(original_packet.take().unwrap().without_guidance());
                    return DeliveryOutcome::Admitted;
                }
                if connection.is_live() {
                    permit.send(control);
                    return DeliveryOutcome::Refused;
                }
                return DeliveryOutcome::ConsumerClosed;
            }
        }
        // Already-consumed bytes are factual even if a later panic, invalid
        // receipt or cancellation prevented a normal policy return.
        DeliveryOutcome::Admitted
    }
}

pub(crate) fn refusal_tool_result() -> Value {
    json!({"content":[{"type":"text","text":REFUSAL}],"isError":true})
}

fn refusal_response(original: &Value) -> Value {
    super::ok(&original["id"], refusal_tool_result())
}

// Poll the original future in place. Unlike spawn+join, unwinding/cancellation
// cannot detach it, and all original task-local scopes remain in force.
async fn catch_future<T>(future: impl Future<Output = T> + Send) -> Result<T, ()> {
    struct Catch<F>(Pin<Box<F>>);
    impl<F: Future> Future for Catch<F> {
        type Output = Result<F::Output, ()>;
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            match catch_unwind(AssertUnwindSafe(|| self.0.as_mut().poll(cx))) {
                Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
                Ok(Poll::Pending) => Poll::Pending,
                Err(_) => Poll::Ready(Err(())),
            }
        }
    }
    Catch(Box::pin(future)).await
}

#[cfg(test)]
pub(crate) mod tests;

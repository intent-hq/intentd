//! Opt-in recipient evidence from actual initialize/session/registration replies.
//! This evidence never grants repository or account authority.
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::transport::Connection;
use agent_client_protocol::schema::v1::Meta;

pub mod history;
pub mod payload;

pub(crate) const META_KEY: &str = "reviewMonitoring";
const CONTEXT_KEY: &str = "reviewMonitoringContext";

/// Original daemon-owned startup correlation. This is never account authority.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReviewMonitoringContext {
    version: u8,
    owner_key: String,
    load_attempt_id: String,
}
impl ReviewMonitoringContext {
    /// Allocate once from the original trusted agent/workspace owner, before
    /// preparation or startup. Never reconstruct this from a model/tool value.
    #[must_use]
    pub fn new(owner_key: String) -> Option<Self> {
        (!owner_key.is_empty() && owner_key.len() <= 256).then(|| Self {
            version: 1,
            owner_key,
            load_attempt_id: uuid::Uuid::new_v4().to_string(),
        })
    }
    /// Preserve ordinary sibling metadata while selecting this exact attempt.
    ///
    /// # Panics
    /// Panics if serialization of the string-only context unexpectedly fails.
    pub fn insert_into(&self, meta: &mut Meta) {
        meta.insert(
            CONTEXT_KEY.into(),
            serde_json::to_value(self).expect("context is serializable"),
        );
    }
    pub(crate) fn parse(meta: Option<&Meta>) -> Option<Self> {
        let context: Self = serde_json::from_value(meta?.get(CONTEXT_KEY)?.clone()).ok()?;
        (context.version == 1
            && !context.owner_key.is_empty()
            && context.owner_key.len() <= 256
            && !context.load_attempt_id.is_empty()
            && context.load_attempt_id.len() <= 256)
            .then_some(context)
    }
}

/// Explicit local support. Ordinary handshakes never offer this extension.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReviewMonitoringOffer {
    /// Preserve ordinary behavior and withhold qualified monitor content.
    #[default]
    Disabled,
    /// Offer the implemented version one consumer contract.
    V1,
}

pub(crate) fn supported(meta: Option<&Meta>) -> bool {
    meta.and_then(|m| m.get(META_KEY))
        .and_then(serde_json::Value::as_u64)
        .is_some_and(|version| version >= 1)
}

#[derive(Default)]
pub(crate) struct ConnectionState {
    closed: bool,
    current: Option<Arc<Generation>>,
}
impl ConnectionState {
    pub(crate) fn begin(&mut self) -> Arc<Generation> {
        self.retire();
        let generation = Arc::new(Generation(
            Mutex::new(State {
                next: 0,
                sessions: HashMap::new(),
                attempts: HashSet::new(),
                retired_attempts: HashSet::new(),
                retired_contexts: HashSet::new(),
                remote_retirements: HashMap::new(),
            }),
            AtomicBool::new(self.closed),
        ));
        self.current = Some(generation.clone());
        generation
    }
    pub(crate) fn retire(&mut self) {
        if let Some(generation) = self.current.take() {
            generation.retire();
        }
    }
    pub(crate) fn close(&mut self) {
        self.closed = true;
        self.retire();
    }
}

struct State {
    next: u64,
    attempts: HashSet<ReviewMonitoringContext>,
    retired_attempts: HashSet<ReviewMonitoringContext>,
    retired_contexts: HashSet<(String, ReviewMonitoringContext)>,
    remote_retirements: HashMap<(String, ReviewMonitoringContext), Arc<history::SharedRetirement>>,
    sessions: HashMap<String, (u64, Option<Weak<Leaf>>)>,
}
pub(crate) struct Generation(Mutex<State>, AtomicBool);
impl Generation {
    pub(crate) fn current(&self) -> bool {
        !self.1.load(Ordering::SeqCst)
    }

    pub(crate) fn transfer_prepared<T>(
        &self,
        session: &str,
        context: &ReviewMonitoringContext,
        transfer: impl FnOnce() -> crate::error::AcpResult<T>,
    ) -> crate::error::AcpResult<T> {
        let state = self.0.lock().unwrap();
        if !self.current()
            || state.retired_attempts.contains(context)
            || state
                .retired_contexts
                .contains(&(session.into(), context.clone()))
        {
            return Err(crate::error::AcpError::Protocol(
                "Review monitoring context unavailable".into(),
            ));
        }
        transfer()
    }

    fn context_current(&self, session: &str, context: &ReviewMonitoringContext) -> bool {
        let state = self.0.lock().unwrap();
        self.current()
            && !state.retired_attempts.contains(context)
            && !state
                .retired_contexts
                .contains(&(session.into(), context.clone()))
    }

    fn retire(&self) {
        // Close every allocation before waiting for any in-flight transfer.
        self.1.store(true, Ordering::SeqCst);
        let leaves: Vec<_> = self
            .0
            .lock()
            .unwrap()
            .sessions
            .values()
            .filter_map(|(_, leaf)| leaf.as_ref().and_then(Weak::upgrade))
            .collect();
        for leaf in leaves {
            leaf.retire();
        }
    }

    pub(crate) fn attempt(self: &Arc<Self>, session: Option<&str>, meta: Option<&Meta>) -> Attempt {
        let mut state = self.0.lock().unwrap();
        state.next = state
            .next
            .checked_add(1)
            .expect("ACP session attempt exhausted");
        let sequence = state.next;
        let requested = ReviewMonitoringContext::parse(meta);
        if requested
            .as_ref()
            .is_some_and(|context| state.retired_attempts.contains(context))
            || session
                .zip(requested.as_ref())
                .is_some_and(|(session, context)| {
                    state
                        .retired_contexts
                        .contains(&(session.into(), context.clone()))
                })
        {
            // Preserve the exact rejected attempt for capture, without replacing
            // or retiring a newer allocation that reused the public session ID.
            return Attempt {
                generation: self.clone(),
                sequence,
                context: requested,
            };
        }
        if let Some(session) = session {
            if let Some((_, Some(old))) = state.sessions.insert(session.into(), (sequence, None)) {
                if let Some(old) = old.upgrade() {
                    old.retire();
                }
            }
        }
        let context = requested.filter(|context| state.attempts.insert(context.clone()));
        Attempt {
            generation: self.clone(),
            sequence,
            context,
        }
    }
}
pub(crate) struct Attempt {
    generation: Arc<Generation>,
    sequence: u64,
    context: Option<ReviewMonitoringContext>,
}
impl Attempt {
    pub(crate) fn capture(
        self,
        connection: &Arc<Connection>,
        session: &str,
        meta: Option<&Meta>,
    ) -> Option<PendingReviewMonitoring> {
        let mut state = self.generation.0.lock().unwrap();
        if !self.generation.current()
            || self.context.as_ref().is_some_and(|context| {
                state.retired_attempts.contains(context)
                    || state
                        .retired_contexts
                        .contains(&(session.into(), context.clone()))
            })
        {
            return None;
        }
        if let Some((sequence, old)) = state.sessions.get(session) {
            if *sequence > self.sequence {
                return None;
            }
            if let Some(old) = old.as_ref().and_then(Weak::upgrade) {
                old.retire();
            }
        }
        state.sessions.insert(session.into(), (self.sequence, None));
        if !supported(meta) {
            return None;
        }
        let context = self.context?;
        if ReviewMonitoringContext::parse(meta).as_ref() != Some(&context) {
            return None;
        }
        let leaf = Arc::new(Leaf {
            transfer: Mutex::new(()),
            retired: AtomicBool::new(false),
            acknowledged: AtomicBool::new(false),
            context,
            session: session.to_owned(),
        });
        state
            .sessions
            .insert(session.into(), (self.sequence, Some(Arc::downgrade(&leaf))));
        drop(state);
        Some(PendingReviewMonitoring {
            connection: Arc::downgrade(connection),
            generation: self.generation,
            leaf,
        })
    }
}
struct Leaf {
    transfer: Mutex<()>,
    retired: AtomicBool,
    acknowledged: AtomicBool,
    context: ReviewMonitoringContext,
    session: String,
}
impl Leaf {
    fn retire(&self) {
        self.retired.store(true, Ordering::SeqCst);
        // A transfer already linearized may finish; no queued transfer can start.
        drop(self.transfer.lock().unwrap());
    }
}

/// A pending actual session declaration. It cannot authorize a read or prompt.
#[derive(Clone)]
pub struct PendingReviewMonitoring {
    connection: Weak<Connection>,
    generation: Arc<Generation>,
    leaf: Arc<Leaf>,
}
impl PendingReviewMonitoring {
    /// Installation must retain the exact successful physical ACP session.
    /// Public IDs or an acknowledged receipt from another connection cannot bind it.
    #[must_use]
    pub fn is_for(&self, connection: &Arc<Connection>, session: &str) -> bool {
        self.generation.current()
            && !self.leaf.retired.load(Ordering::SeqCst)
            && self.leaf.session == session
            && self
                .connection
                .upgrade()
                .is_some_and(|original| Arc::ptr_eq(&original, connection) && original.is_alive())
    }

    /// Check the original echoed startup correlation without upgrading it to ACK.
    #[must_use]
    pub fn matches_context(&self, context: &ReviewMonitoringContext) -> bool {
        &self.leaf.context == context
            && self.generation.current()
            && !self.leaf.retired.load(Ordering::SeqCst)
            && self
                .connection
                .upgrade()
                .is_some_and(|connection| connection.is_alive())
    }

    pub(crate) fn acknowledge(&self) {
        if self.generation.current() && !self.leaf.retired.load(Ordering::SeqCst) {
            self.leaf.acknowledged.store(true, Ordering::SeqCst);
        }
    }
    /// Snapshot support only after the exact callback registration acknowledged.
    /// A pre-ack request receives no receipt and cannot become capable later.
    #[must_use]
    pub fn acknowledged(&self) -> Option<ReviewMonitoringReceipt> {
        let receipt = ReviewMonitoringReceipt(self.clone());
        receipt.is_current().then_some(receipt)
    }
}

/// Actual original connection/session support, separate from physical ownership
/// and repository authority, which callers must additionally retain and fence.
#[derive(Clone)]
pub struct ReviewMonitoringReceipt(PendingReviewMonitoring);
impl ReviewMonitoringReceipt {
    /// Exact original recipient allocation, not equality of public identifiers.
    #[must_use]
    pub fn same_recipient(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0.generation, &other.0.generation)
            && Arc::ptr_eq(&self.0.leaf, &other.0.leaf)
            && Weak::ptr_eq(&self.0.connection, &other.0.connection)
    }

    /// Exact acknowledged startup allocation for scoped history and retirement.
    #[must_use]
    pub fn context(&self) -> &ReviewMonitoringContext {
        &self.0.leaf.context
    }

    /// A later initialize/session replacement or connection closure retires this
    /// evidence permanently, including when an old successful reply arrives late.
    #[must_use]
    pub fn is_current(&self) -> bool {
        self.0.generation.current()
            && !self.0.leaf.retired.load(Ordering::SeqCst)
            && self.0.leaf.acknowledged.load(Ordering::SeqCst)
            && self
                .0
                .connection
                .upgrade()
                .is_some_and(|connection| connection.is_alive())
    }
    /// Retire this exact session allocation without touching another session.
    pub fn retire(&self) {
        self.0.leaf.retire();
    }

    /// Retain the original generation through one synchronous final operation.
    /// Callers additionally hold their physical and source/account fences.
    ///
    /// # Panics
    /// Panics if the transfer mutex is poisoned or the supplied action panics.
    pub fn with_current<T>(&self, action: impl FnOnce() -> T) -> Option<T> {
        let _transfer = self.0.leaf.transfer.lock().unwrap();
        if !self.0.generation.current()
            || self.0.leaf.retired.load(Ordering::SeqCst)
            || !self.0.leaf.acknowledged.load(Ordering::SeqCst)
            || !self
                .0
                .connection
                .upgrade()
                .is_some_and(|connection| connection.is_alive())
        {
            return None;
        }
        Some(action())
    }
}

impl Generation {
    pub(crate) fn retire_attempt(&self, context: &ReviewMonitoringContext) {
        let mut state = self.0.lock().unwrap();
        state.retired_attempts.insert(context.clone());
        for (_, leaf) in state.sessions.values() {
            if let Some(leaf) = leaf.as_ref().and_then(Weak::upgrade) {
                if &leaf.context == context {
                    leaf.retire();
                }
            }
        }
    }

    fn retire_context(
        &self,
        session: &str,
        context: &ReviewMonitoringContext,
    ) -> Arc<history::SharedRetirement> {
        let mut state = self.0.lock().unwrap();
        // Preserve retirement even before a late session response creates a leaf.
        // This original attempt cannot be revived by capture or queued history.
        state
            .retired_contexts
            .insert((session.into(), context.clone()));
        if let Some(leaf) = state
            .sessions
            .get(session)
            .and_then(|(_, leaf)| leaf.as_ref())
            .and_then(Weak::upgrade)
        {
            if &leaf.context == context {
                leaf.retire();
            }
        }
        state
            .remote_retirements
            .entry((session.into(), context.clone()))
            .or_insert_with(|| Arc::new(history::SharedRetirement::default()))
            .clone()
    }
}

#[cfg(test)]
mod retirement_tests {
    use super::*;

    #[tokio::test]
    async fn recipient_retirement_closes_eligibility_before_joining_a_held_transfer() {
        let (socket, _peer) = tokio::io::duplex(8192);
        let (read, write) = tokio::io::split(socket);
        let connection = Arc::new(Connection::new(
            write,
            read,
            None,
            crate::ConnectionHooks::default(),
        ));
        let mut state = ConnectionState::default();
        let generation = state.begin();
        let context = ReviewMonitoringContext::new("original".into()).unwrap();
        let leaf = Arc::new(Leaf {
            transfer: Mutex::new(()),
            retired: AtomicBool::new(false),
            acknowledged: AtomicBool::new(true),
            context,
            session: "original".into(),
        });
        let receipt = ReviewMonitoringReceipt(PendingReviewMonitoring {
            connection: Arc::downgrade(&connection),
            generation: generation.clone(),
            leaf: leaf.clone(),
        });
        let transferring = receipt.clone();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let transfer = std::thread::spawn(move || {
            transferring.with_current(|| {
                entered_tx.send(()).unwrap();
                release_rx
                    .recv_timeout(intentd_test_support::test_timeout(
                        std::time::Duration::from_secs(5),
                    ))
                    .unwrap();
                42
            })
        });
        entered_rx
            .recv_timeout(intentd_test_support::test_timeout(
                std::time::Duration::from_secs(5),
            ))
            .unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let retiring = receipt.clone();
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            retiring.retire();
            done_tx.send(()).unwrap();
        });
        started_rx
            .recv_timeout(intentd_test_support::test_timeout(
                std::time::Duration::from_secs(5),
            ))
            .unwrap();
        let deadline = std::time::Instant::now()
            + intentd_test_support::test_timeout(std::time::Duration::from_secs(1));
        while !leaf.retired.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        let closed_before_join = leaf.retired.load(Ordering::SeqCst);
        let joined_early = done_rx.try_recv().is_ok();
        release_tx.send(()).unwrap();
        assert_eq!(transfer.join().unwrap(), Some(42));
        worker.join().unwrap();
        assert!(
            closed_before_join,
            "retirement must close eligibility before waiting for the original transfer"
        );
        assert!(
            !joined_early,
            "retirement must join the transfer before reporting completion"
        );
        assert_eq!(receipt.with_current(|| 7), None);
    }
    #[tokio::test]
    async fn generation_and_same_id_retirement_close_before_join_and_preserve_sibling_scope() {
        for whole_generation in [false, true] {
            let (socket, _peer) = tokio::io::duplex(8192);
            let (read, write) = tokio::io::split(socket);
            let connection = Arc::new(Connection::new(
                write,
                read,
                None,
                crate::ConnectionHooks::default(),
            ));
            let mut state = ConnectionState::default();
            let generation = state.begin();
            let receipt = |session: &str| {
                let mut meta = Meta::new();
                ReviewMonitoringContext::new("original".into())
                    .unwrap()
                    .insert_into(&mut meta);
                meta.insert(META_KEY.into(), serde_json::json!(1));
                let pending = generation
                    .attempt(None, Some(&meta))
                    .capture(&connection, session, Some(&meta))
                    .unwrap();
                pending.acknowledge();
                pending.acknowledged().unwrap()
            };
            let original = receipt("original");
            let sibling = receipt("sibling");
            let transferring = original.clone();
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let transfer = std::thread::spawn(move || {
                transferring.with_current(|| {
                    entered_tx.send(()).unwrap();
                    release_rx
                        .recv_timeout(intentd_test_support::test_timeout(
                            std::time::Duration::from_secs(5),
                        ))
                        .unwrap();
                    42
                })
            });
            entered_rx
                .recv_timeout(intentd_test_support::test_timeout(
                    std::time::Duration::from_secs(5),
                ))
                .unwrap();
            let retiring = generation.clone();
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let worker = std::thread::spawn(move || {
                if whole_generation {
                    retiring.retire();
                } else {
                    let _attempt = retiring.attempt(Some("original"), None);
                }
                done_tx.send(()).unwrap();
            });
            let deadline = std::time::Instant::now()
                + intentd_test_support::test_timeout(std::time::Duration::from_secs(1));
            while original.is_current() && std::time::Instant::now() < deadline {
                std::thread::yield_now();
            }
            let closed = !original.is_current();
            let sibling_result = sibling.with_current(|| 7);
            let joined_early = done_rx.try_recv().is_ok();
            release_tx.send(()).unwrap();
            assert_eq!(transfer.join().unwrap(), Some(42));
            worker.join().unwrap();
            assert!(closed);
            assert!(!joined_early);
            assert_eq!(
                sibling_result,
                if whole_generation { None } else { Some(7) }
            );
            assert_eq!(original.with_current(|| 9), None);
        }
    }
}

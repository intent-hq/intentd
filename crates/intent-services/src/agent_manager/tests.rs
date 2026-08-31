//! Unit tests for the agent process registry (cap + LRU + lifecycle) and the
//! [`AgentManager`] multiplexing/teardown — parity-checked against
//! `agent-process-registry`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use intent_acp::permission::{PermissionOptionView, RiskLevel};
use intent_acp::{
    AcpError, Connection, ConnectionHooks, EventSink, IncomingNotification, JsonRpcError,
    PermissionOutcome, PermissionPolicy, PermissionRequestData,
};
use intent_core::{
    now_iso, AgentId, AgentSession, AgentStatus, Error, Workspace, WorkspaceActivity, WorkspaceApi,
    WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};
use intent_store::Store;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration};

use super::{
    budget_admits, charged_bytes, compute_process_cap, derive_agent_type, derive_is_orchestrator,
    is_cancel_transport_closed, recommended_memory_budget_bytes, resolve_npx_only, resolve_spawn,
    text_prompt, AgentHandle, AgentManager, BusEventSink, KillFn, ProcessRegistry, ResolvedSpawn,
    TreeMemoryProbe, DEFAULT_AGENT_TYPE, PROVISIONAL_AGENT_BYTES,
};
use crate::agent_ops::user_message_blocks;
use crate::events::{EventBus, SubscriptionFilter};
use crate::Services;

/// `SQLite` db inside an RAII temp dir: the dir sweep (on drop, including on
/// panic) also covers `-wal`/`-shm` sidecars, and a background task that
/// lazily reopens a pool connection after drop cannot recreate the file at
/// the TMPDIR root. Set `INTENTD_TEST_KEEP_TMP` (non-empty) to keep the dir.
struct TempDb {
    path: PathBuf,
    _dir: tempfile::TempDir,
}

impl TempDb {
    fn new() -> Self {
        let mut dir = tempfile::Builder::new()
            .prefix("intentd-mgr-")
            .tempdir()
            .expect("create test tempdir");
        if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
            dir.disable_cleanup(true);
        }
        let path = dir.path().join("mgr.db");
        Self { path, _dir: dir }
    }
}

/// Serializes env-mutating tests: process env is global, so a test that SETS
/// `MOCK_AGENT_SCRIPT_PATH` (or a retry-backoff knob) must not interleave
/// with one that unsets it. Crate-wide (via the `pub(crate)` [`EnvGuard`]):
/// tests in OTHER modules that mutate the same vars (e.g. `agent_session`'s
/// idle-timeout tests and this module's worker redrive tests both pin
/// `INTENTD_PROMPT_IDLE_TIMEOUT_MS`) must serialize through the same lock or
/// one test's Drop-restore races another's live window.
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Pins env vars for the guard's lifetime — holding [`ENV_LOCK`] so
/// env-mutating tests serialize — and restores the prior values on drop so
/// tests stay hermetic (mirrors `intent-acp`'s test `EnvGuard`).
///
/// The lock is held until the guard drops, so a test must use exactly ONE
/// guard: constructing a second before the first drops deadlocks the test
/// thread. Mutate every var (sets AND unsets) through a single
/// [`EnvGuard::apply`] call instead.
pub(crate) struct EnvGuard {
    saved: Vec<(&'static str, Option<String>)>,
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl EnvGuard {
    fn acquire() -> std::sync::MutexGuard<'static, ()> {
        // A panicked test already restored its env in Drop before poisoning.
        ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Apply every `(key, value)` mutation under one guard: `Some(v)` sets
    /// the var, `None` unsets it. Prior values are restored on drop.
    pub(crate) fn apply(pairs: &[(&'static str, Option<&str>)]) -> Self {
        let lock = Self::acquire();
        let mut saved = Vec::new();
        for (key, value) in pairs {
            saved.push((*key, std::env::var(key).ok()));
            match value {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
        }
        Self { saved, _lock: lock }
    }

    fn unset(key: &'static str) -> Self {
        Self::apply(&[(key, None)])
    }

    /// Set every `(key, value)` pair for the guard's lifetime.
    pub(crate) fn set_all(pairs: &[(&'static str, &str)]) -> Self {
        let pairs: Vec<(&'static str, Option<&str>)> =
            pairs.iter().map(|(k, v)| (*k, Some(*v))).collect();
        Self::apply(&pairs)
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, prev) in &self.saved {
            match prev {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
        }
    }
}

/// A kill callback that records the agents it was invoked for (the registry
/// itself performs the follow-up `deregister`).
fn recording_kill(id: AgentId, log: Arc<Mutex<Vec<AgentId>>>) -> KillFn {
    Arc::new(move || {
        let log = log.clone();
        let id = id.clone();
        Box::pin(async move {
            log.lock().unwrap().push(id);
        })
    })
}

/// An always-winning claim (registry-level tests without a manager: no
/// `try_begin` contention to guard against).
fn claim_all(_: &AgentId) -> bool {
    true
}

/// A no-op claim release, paired with [`claim_all`].
fn release_none(_: &AgentId) {}

#[test]
fn compute_process_cap_reserves_8gb_and_budgets_1gb_per_agent() {
    assert_eq!(compute_process_cap(8 * super::GB), 4);
    assert_eq!(compute_process_cap(16 * super::GB), 8);
    assert_eq!(compute_process_cap(32 * super::GB), 24);
    assert_eq!(compute_process_cap(64 * super::GB), 56);
    assert_eq!(compute_process_cap(128 * super::GB), 100);
}

#[test]
fn compute_process_cap_lower_clamp_floors_at_4() {
    // Below the 8 GB reserve the subtraction saturates to 0 → clamp to 4.
    assert_eq!(compute_process_cap(0), 4);
    assert_eq!(compute_process_cap(4 * super::GB), 4);
    // Just past the reserve, the raw budget (1..=4 GB) is still at or below the floor.
    assert_eq!(compute_process_cap(12 * super::GB), 4);
    // First value above the floor.
    assert_eq!(compute_process_cap(13 * super::GB), 5);
}

#[test]
fn compute_process_cap_upper_clamp_caps_at_100() {
    // 108 GB is the first size to hit the ceiling: (108 - 8) / 1 = 100.
    assert_eq!(compute_process_cap(107 * super::GB), 99);
    assert_eq!(compute_process_cap(108 * super::GB), 100);
    assert_eq!(compute_process_cap(256 * super::GB), 100);
    assert_eq!(compute_process_cap(u64::MAX), 100);
}

#[test]
fn compute_process_cap_is_monotonic_with_no_cliffs() {
    // The old step table jumped 30 → 100 between 64 and 65 GB; the smooth
    // formula must grow by at most 1 per GB.
    let mut prev = compute_process_cap(0);
    for gb in 1..=160 {
        let cap = compute_process_cap(gb * super::GB);
        assert!(cap >= prev, "cap must be monotonic at {gb} GB");
        assert!(cap - prev <= 1, "cap must not cliff at {gb} GB");
        prev = cap;
    }
}

#[test]
#[cfg(target_os = "macos")]
fn total_memory_bytes_macos_returns_physical_ram() {
    let mem = super::total_memory_bytes();
    assert!(mem.is_some(), "macOS should detect physical RAM");
    let bytes = mem.unwrap();
    assert!(bytes > 0, "detected RAM should be > 0");
    // Sanity check: physical RAM on modern machines is at least 1 GB.
    assert!(bytes >= super::GB, "detected RAM should be >= 1 GB");
}

#[tokio::test]
async fn tracks_concurrent_processes_and_deregisters() {
    let reg = ProcessRegistry::new(8);
    let log = Arc::new(Mutex::new(Vec::new()));
    for name in ["a", "b", "c"] {
        let id = AgentId::from(name);
        reg.register(id.clone(), recording_kill(id, log.clone()));
    }
    assert_eq!(reg.size(), 3);
    assert!(reg.is_registered(&AgentId::from("b")));
    assert!(reg.deregister(&AgentId::from("b")));
    assert_eq!(reg.size(), 2);
    assert!(!reg.is_registered(&AgentId::from("b")));
}

#[tokio::test]
async fn acquire_evicts_lru_idle_when_full() {
    let reg = ProcessRegistry::new(2);
    let log = Arc::new(Mutex::new(Vec::new()));
    let (a, b, c) = (AgentId::from("a"), AgentId::from("b"), AgentId::from("c"));
    reg.register(a.clone(), recording_kill(a.clone(), log.clone()));
    reg.register(b.clone(), recording_kill(b.clone(), log.clone()));
    // `a` is the least-recently-used idle process.
    reg.set_last_active(&a, 100);
    reg.set_last_active(&b, 200);

    reg.acquire(&c, claim_all, release_none).await;

    assert_eq!(*log.lock().unwrap(), vec![a.clone()], "evicts LRU idle");
    assert!(!reg.is_registered(&a));
    assert!(reg.is_registered(&b));
    assert_eq!(reg.size(), 1);
}

#[tokio::test]
async fn acquire_queues_until_a_process_goes_idle() {
    let reg = Arc::new(ProcessRegistry::new(1));
    let log = Arc::new(Mutex::new(Vec::new()));
    let (a, b) = (AgentId::from("a"), AgentId::from("b"));
    reg.register(a.clone(), recording_kill(a.clone(), log.clone()));
    reg.mark_active(&a);

    let reg2 = reg.clone();
    let acquired = tokio::spawn(async move { reg2.acquire(&b, claim_all, release_none).await });
    // All processes active → the acquire must block.
    assert!(timeout(Duration::from_millis(50), async {}).await.is_ok());
    assert!(!acquired.is_finished(), "acquire blocks while all active");

    // Becoming idle wakes the queued spawn, which evicts `a` and proceeds.
    reg.mark_idle(&a);
    timeout(Duration::from_secs(2), acquired)
        .await
        .expect("acquire resolves once a slot frees")
        .expect("task ok");
    assert_eq!(*log.lock().unwrap(), vec![a]);
    assert_eq!(reg.size(), 0);
}

/// Tree-memory probe whose reading tests set by hand. Every `set` bumps the
/// sample id, which is exactly what the real 5 s sampler does.
struct FakeProbe(
    Mutex<(u64, u64)>,
    Mutex<std::collections::HashMap<AgentId, u64>>,
);

impl FakeProbe {
    fn new(bytes: u64) -> Arc<Self> {
        Arc::new(Self(
            Mutex::new((bytes, 1)),
            Mutex::new(std::collections::HashMap::new()),
        ))
    }

    /// Publish a freshly measured reading (new sample id → the registry drops
    /// the provisional correction it accumulated against the previous one).
    fn set(&self, bytes: u64) {
        let mut guard = self.0.lock().unwrap();
        guard.0 = bytes;
        guard.1 += 1;
    }

    /// Publish per-agent attribution buckets (monorepo#2063 Phase A) alongside
    /// the aggregate reading.
    fn set_agents(&self, pairs: &[(&AgentId, u64)]) {
        *self.1.lock().unwrap() = pairs.iter().map(|(id, b)| ((*id).clone(), *b)).collect();
    }
}

impl TreeMemoryProbe for FakeProbe {
    fn sample(&self) -> Option<(u64, u64)> {
        Some(*self.0.lock().unwrap())
    }

    fn agent_samples(&self) -> std::collections::HashMap<AgentId, u64> {
        self.1.lock().unwrap().clone()
    }
}

/// A probe that has never produced a sample must leave admission untouched —
/// a daemon in its first seconds behaves exactly as if no budget existed.
struct NeverSampled;

impl TreeMemoryProbe for NeverSampled {
    fn sample(&self) -> Option<(u64, u64)> {
        None
    }
}

#[test]
fn recommended_budget_halves_ram_left_after_the_8gb_reserve() {
    let gb = super::GB;
    // The reporter's 48 GB seat: 20 GB, which the measured 21.5 GB tree crosses.
    assert_eq!(recommended_memory_budget_bytes(48 * gb), 20 * gb);
    assert_eq!(recommended_memory_budget_bytes(32 * gb), 12 * gb);
    // Floor: never below 4 GB, however small (or undetectable) the host.
    assert_eq!(recommended_memory_budget_bytes(16 * gb), 4 * gb);
    assert_eq!(recommended_memory_budget_bytes(8 * gb), 4 * gb);
    assert_eq!(recommended_memory_budget_bytes(0), 4 * gb);
    // Always strictly under the slot cap's implied budget, which spends all of
    // the same post-reserve RAM at 1 GB per agent.
    for gb_total in 1..=160u64 {
        let implied = compute_process_cap(gb_total * gb) as u64 * gb;
        let budget = recommended_memory_budget_bytes(gb_total * gb);
        assert!(
            budget <= implied,
            "budget {budget} must not exceed the slot cap's implied {implied} at {gb_total} GB"
        );
    }
}

#[test]
fn charged_bytes_applies_a_signed_correction_and_saturates_at_zero() {
    assert_eq!(charged_bytes(1_000, 250), 1_250);
    assert_eq!(charged_bytes(1_000, -250), 750);
    assert_eq!(charged_bytes(1_000, -5_000), 0, "credits cannot underflow");
    assert_eq!(charged_bytes(u64::MAX, 1), u64::MAX, "charges cannot wrap");
}

#[test]
fn budget_admits_an_empty_registry_however_fat_the_tree() {
    // Over budget with nothing registered: the tree is one-shot adapters or
    // simply another process the daemon does not own, and refusing forever
    // would wedge the daemon.
    assert!(budget_admits(u64::MAX, 1_000, 0));
    assert!(!budget_admits(1_000, 1_000, 1), "at budget denies");
    assert!(budget_admits(999, 1_000, 1));
}

#[tokio::test]
async fn budget_is_off_by_default_and_inert_before_the_first_sample() {
    // A 1-byte budget is unmeetable, so anything but "inert" would queue here.
    let probes: [Option<Arc<dyn TreeMemoryProbe>>; 2] = [None, Some(Arc::new(NeverSampled))];
    for probe in probes {
        let reg = ProcessRegistry::new(8);
        if let Some(probe) = probe {
            assert!(reg.set_memory_budget(1, probe), "budget installs once");
        }
        let log = Arc::new(Mutex::new(Vec::new()));
        let (a, b) = (AgentId::from("a"), AgentId::from("b"));
        reg.register(a.clone(), recording_kill(a.clone(), log.clone()));

        timeout(
            Duration::from_millis(200),
            reg.acquire(&b, claim_all, release_none),
        )
        .await
        .expect("acquire must not queue without a usable budget");
        assert!(reg.is_registered(&a), "nothing evicted");
    }
}

#[tokio::test]
async fn budget_reclaims_idle_processes_before_queueing_a_spawn() {
    let gb = super::GB;
    let reg = Arc::new(ProcessRegistry::new(8)); // Slots free; only memory binds.
    let probe = FakeProbe::new(10 * gb);
    assert!(reg.set_memory_budget(4 * gb, probe.clone()));

    let log = Arc::new(Mutex::new(Vec::new()));
    let (idle, active, spawning) = (
        AgentId::from("idle"),
        AgentId::from("active"),
        AgentId::from("spawning"),
    );
    reg.register(idle.clone(), recording_kill(idle.clone(), log.clone()));
    reg.register(active.clone(), recording_kill(active.clone(), log.clone()));
    reg.mark_active(&active);

    // 10 GB charged against a 4 GB budget with a slot free: the idle process is
    // reclaimed rather than the spawn being admitted.
    let reg2 = reg.clone();
    let handle =
        tokio::spawn(async move { reg2.acquire(&spawning, claim_all, release_none).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        *log.lock().unwrap(),
        vec![idle.clone()],
        "reclaims the idle process, never the active one"
    );
    assert!(
        !handle.is_finished(),
        "still over budget after the one eviction available → the spawn waits"
    );

    // Once the tree actually drains, the queued spawn proceeds without any
    // registry event to wake it — nothing was deregistered or marked idle.
    probe.set(gb);
    timeout(Duration::from_secs(30), handle)
        .await
        .expect("the memory waiter must re-check on its own timer")
        .expect("task ok");
    assert!(
        reg.is_registered(&active),
        "an active process is never evicted by the budget"
    );
}

/// Budget-driven queue/evict/resume events must carry `reason: "memory-budget"`
/// (monorepo#2063) — the slot-cap variant (`"slots"`) is asserted end-to-end in
/// [`process_cap_events_queued_resumed_evicted`].
#[tokio::test]
async fn budget_driven_process_events_carry_memory_budget_reason() {
    let gb = super::GB;
    let events: Arc<Mutex<Vec<(AgentId, String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();
    let event_fn: super::ProcessEventFn =
        Arc::new(move |agent_id, event_type, _used, _cap, reason| {
            let events = events_clone.clone();
            let agent_id = agent_id.clone();
            let event_type = event_type.to_string();
            let reason = reason.to_string();
            Box::pin(async move {
                events.lock().unwrap().push((agent_id, event_type, reason));
            })
        });

    // Slots free; only memory binds — every event below is budget-driven.
    let reg = Arc::new(ProcessRegistry::new(8).with_event_fn(event_fn));
    let probe = FakeProbe::new(10 * gb);
    assert!(reg.set_memory_budget(4 * gb, probe.clone()));

    let log = Arc::new(Mutex::new(Vec::new()));
    let (idle, active, spawning) = (
        AgentId::from("idle"),
        AgentId::from("active"),
        AgentId::from("spawning"),
    );
    reg.register(idle.clone(), recording_kill(idle.clone(), log.clone()));
    reg.register(active.clone(), recording_kill(active.clone(), log.clone()));
    reg.mark_active(&active);

    // Over budget with a slot free: evicts the idle process, then queues.
    let reg2 = reg.clone();
    let spawning2 = spawning.clone();
    let handle =
        tokio::spawn(async move { reg2.acquire(&spawning2, claim_all, release_none).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Under budget again + a deregister → wakes the queued spawn (resumed).
    probe.set(gb);
    reg.deregister(&active);
    timeout(Duration::from_secs(2), handle)
        .await
        .expect("acquire resolves once woken under budget")
        .expect("task ok");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let events = events.lock().unwrap().clone();
    let find = |event_type: &str, agent: &AgentId| {
        events
            .iter()
            .find(|(a, e, _)| e == event_type && a == agent)
            .unwrap_or_else(|| panic!("{event_type} event for {agent} recorded"))
            .2
            .clone()
    };
    assert_eq!(
        find("agent:process:evicted", &idle),
        "memory-budget",
        "budget-driven eviction reason"
    );
    assert_eq!(
        find("agent:process:queued", &spawning),
        "memory-budget",
        "budget-driven queue reason"
    );
    assert_eq!(
        find("agent:process:resumed", &spawning),
        "memory-budget",
        "budget-driven resume reason"
    );
}

/// When both constraints bind at once (all slots active AND over budget), the
/// budget wins the reason label — a freed slot alone cannot clear it.
#[tokio::test]
async fn both_constraints_binding_labels_reason_memory_budget() {
    let gb = super::GB;
    let events: Arc<Mutex<Vec<(AgentId, String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();
    let event_fn: super::ProcessEventFn =
        Arc::new(move |agent_id, event_type, _used, _cap, reason| {
            let events = events_clone.clone();
            let agent_id = agent_id.clone();
            let event_type = event_type.to_string();
            let reason = reason.to_string();
            Box::pin(async move {
                events.lock().unwrap().push((agent_id, event_type, reason));
            })
        });

    // Cap of 1 with one ACTIVE process (no idle to evict) AND over budget:
    // both constraints bind, so the queued reason must be memory-budget.
    let reg = Arc::new(ProcessRegistry::new(1).with_event_fn(event_fn));
    let probe = FakeProbe::new(10 * gb);
    assert!(reg.set_memory_budget(4 * gb, probe.clone()));

    let log = Arc::new(Mutex::new(Vec::new()));
    let (active, spawning) = (AgentId::from("active"), AgentId::from("spawning"));
    reg.register(active.clone(), recording_kill(active.clone(), log.clone()));
    reg.mark_active(&active);

    let reg2 = reg.clone();
    let spawning2 = spawning.clone();
    let handle =
        tokio::spawn(async move { reg2.acquire(&spawning2, claim_all, release_none).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    {
        let events = events.lock().unwrap();
        let queued = events
            .iter()
            .find(|(a, e, _)| e == "agent:process:queued" && a == &spawning)
            .expect("queued event recorded");
        assert_eq!(queued.2, "memory-budget", "budget wins when both bind");
    }

    // Release both constraints so the queued spawn resolves and the task ends.
    probe.set(gb);
    reg.deregister(&active);
    timeout(Duration::from_secs(2), handle)
        .await
        .expect("acquire resolves once both constraints clear")
        .expect("task ok");
}

#[tokio::test]
async fn provisional_charge_stops_a_burst_clearing_one_stale_sample() {
    let gb = super::GB;
    // Budget leaves room for exactly two provisional charges beyond the sample.
    let budget = gb + 2 * PROVISIONAL_AGENT_BYTES;
    let reg = Arc::new(ProcessRegistry::new(64));
    let probe = FakeProbe::new(gb);
    assert!(reg.set_memory_budget(budget, probe.clone()));

    let log = Arc::new(Mutex::new(Vec::new()));
    // The registry must be non-empty or the always-admit rule applies.
    let seed = AgentId::from("seed");
    reg.register(seed.clone(), recording_kill(seed.clone(), log.clone()));
    reg.mark_active(&seed);

    let mut admitted = 0usize;
    for n in 0..6 {
        let id = AgentId::from(format!("burst-{n}").as_str());
        if timeout(
            Duration::from_millis(50),
            reg.acquire(&id, claim_all, release_none),
        )
        .await
        .is_err()
        {
            break;
        }
        reg.register(id.clone(), recording_kill(id.clone(), log.clone()));
        // Active, so the next acquire cannot simply reclaim this one instead of
        // being held by the budget — the burst is what is under test here.
        reg.mark_active(&id);
        admitted += 1;
    }
    assert_eq!(
        admitted, 2,
        "without the provisional charge every spawn would clear the same stale sample"
    );

    // A fresh sample supersedes the correction: the reading now reflects the
    // admitted spawns, and headroom is judged on measurement alone.
    probe.set(gb);
    let next = AgentId::from("after-fresh-sample");
    timeout(
        Duration::from_millis(50),
        reg.acquire(&next, claim_all, release_none),
    )
    .await
    .expect("a fresh under-budget sample admits again");
}

#[tokio::test]
async fn deregister_credits_back_the_provisional_charge() {
    let gb = super::GB;
    let reg = Arc::new(ProcessRegistry::new(64));
    let probe = FakeProbe::new(gb);
    assert!(reg.set_memory_budget(gb + PROVISIONAL_AGENT_BYTES, probe));

    let log = Arc::new(Mutex::new(Vec::new()));
    let seed = AgentId::from("seed");
    reg.register(seed.clone(), recording_kill(seed.clone(), log.clone()));
    reg.mark_active(&seed);

    let first = AgentId::from("first");
    timeout(
        Duration::from_millis(50),
        reg.acquire(&first, claim_all, release_none),
    )
    .await
    .expect("the single unit of headroom admits one spawn");
    reg.register(first.clone(), recording_kill(first.clone(), log.clone()));
    // Active, so the budget cannot reclaim it — the credit path is under test.
    reg.mark_active(&first);

    // Headroom is now spent against the same sample.
    let second = AgentId::from("second");
    assert!(
        timeout(
            Duration::from_millis(50),
            reg.acquire(&second, claim_all, release_none),
        )
        .await
        .is_err(),
        "no headroom left against this sample"
    );

    // The dead process is still inside that sample, so its cost is credited
    // back rather than waiting a full sampling period for the truth.
    reg.deregister(&first);
    timeout(
        Duration::from_millis(50),
        reg.acquire(&second, claim_all, release_none),
    )
    .await
    .expect("the credited charge frees the headroom immediately");
}

/// `budget_status` (monorepo#2063 A3): the `system.status` visibility read —
/// `None` without a budget; with one, the installed bytes, the charged bytes
/// admission compares (sample + pending correction; `None` before the first
/// sample), and the live waiter count. Admissions keep the pending correction
/// nonzero throughout, so both branches of the sample-seq check are pinned by
/// value, not merely exercised.
#[tokio::test]
async fn budget_status_reports_budget_charged_and_queued() {
    let gb = super::GB;
    let provisional = PROVISIONAL_AGENT_BYTES;
    // No budget installed → no visibility fields at all.
    assert!(ProcessRegistry::new(8).budget_status().is_none());

    // Installed but never sampled → the ceiling serves, charged is absent.
    let reg = ProcessRegistry::new(8);
    assert!(reg.set_memory_budget(4 * gb, Arc::new(NeverSampled)));
    assert_eq!(reg.budget_status(), Some((4 * gb, None, 0)));

    // An admission under budget charges its provisional cost against the
    // sample, and charged reports the sum — a raw-sample regression would
    // read 2 GB here.
    let reg = Arc::new(ProcessRegistry::new(8));
    let probe = FakeProbe::new(2 * gb);
    assert!(reg.set_memory_budget(4 * gb, probe.clone()));
    let log = Arc::new(Mutex::new(Vec::new()));
    let active = AgentId::from("active");
    reg.acquire(&active, claim_all, release_none).await;
    reg.register(active.clone(), recording_kill(active.clone(), log.clone()));
    reg.mark_active(&active);
    let (budget, charged, queued) = reg.budget_status().expect("budget installed");
    assert_eq!(budget, 4 * gb);
    assert_eq!(
        charged,
        Some(2 * gb + provisional),
        "charged is the sample plus the pending correction"
    );
    assert_eq!(queued, 0);

    // A fresh sample the correction has not been reset against is served
    // uncorrected — exactly the sample, not sample + correction — and the
    // read must not perturb admission state.
    probe.set(3 * gb);
    let (_, charged, _) = reg.budget_status().expect("budget installed");
    assert_eq!(
        charged,
        Some(3 * gb),
        "a fresh sample is served uncorrected"
    );

    // The next admission proves the status read mutated nothing: it resets
    // the correction against the fresh sample and charges itself. Had the
    // read recorded the new seq while keeping the stale correction, this
    // would report 3 GB + 2 × provisional.
    let second = AgentId::from("second");
    reg.acquire(&second, claim_all, release_none).await;
    reg.register(second.clone(), recording_kill(second.clone(), log.clone()));
    reg.mark_active(&second);
    let (_, charged, _) = reg.budget_status().expect("budget installed");
    assert_eq!(charged, Some(3 * gb + provisional));

    // Over budget with a spawn queued behind the gate: the waiter is counted,
    // and the queueing acquire re-checked against the new sample (correction
    // reset, nothing yet admitted against it).
    probe.set(10 * gb);
    let reg2 = reg.clone();
    let handle = tokio::spawn(async move {
        reg2.acquire(&AgentId::from("spawning"), claim_all, release_none)
            .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (_, charged, queued) = reg.budget_status().expect("budget installed");
    assert_eq!(charged, Some(10 * gb), "charged is what admission compares");
    assert_eq!(queued, 1, "the queued spawn is visible");

    // Draining the tree admits the queued spawn on its timed re-check.
    probe.set(gb);
    timeout(Duration::from_secs(30), handle)
        .await
        .expect("the drained tree admits the queued spawn")
        .expect("task ok");
    let (_, _, queued) = reg.budget_status().expect("budget installed");
    assert_eq!(queued, 0, "the admitted spawn left the queue");
}

/// Budget-triggered idle drain (monorepo#2063 level 2): an over-budget state
/// drains idle processes with NO spawn attempt, largest-attributed-subtree
/// first, and stops the moment the charge clears the budget — the smaller
/// idle survivor is kept. The probe never publishes a fresh sample here: the
/// stop relies on the drain crediting the victim's ATTRIBUTED bytes (7 GB),
/// not just the fixed provisional cost, so it holds within a single sample
/// period.
#[tokio::test]
async fn over_budget_drain_evicts_largest_attributed_idle_first() {
    let gb = super::GB;
    let reg = ProcessRegistry::new(8);
    let probe = FakeProbe::new(10 * gb);
    assert!(reg.set_memory_budget(4 * gb, probe.clone()));

    let log = Arc::new(Mutex::new(Vec::new()));
    let (small, big) = (AgentId::from("small"), AgentId::from("big"));
    reg.register(small.clone(), recording_kill(small.clone(), log.clone()));
    reg.register(big.clone(), recording_kill(big.clone(), log.clone()));
    // LRU alone would pick `small` (older); attribution must pick `big`.
    reg.set_last_active(&small, 100);
    reg.set_last_active(&big, 200);
    probe.set_agents(&[(&small, gb), (&big, 7 * gb)]);

    let evicted = reg.evict_while_over_budget(|_| true, |_| {}).await;

    assert_eq!(evicted, 1, "the drain stops once the charge clears");
    assert_eq!(
        *log.lock().unwrap(),
        vec![big.clone()],
        "largest attributed idle subtree goes first"
    );
    assert!(reg.is_registered(&small), "under budget again → small kept");
}

/// With no attribution at all (probe predates Phase A buckets / first sweep
/// not landed), the over-budget drain degrades to plain LRU order.
#[tokio::test]
async fn over_budget_drain_falls_back_to_lru_without_attribution() {
    let gb = super::GB;
    let reg = ProcessRegistry::new(8);
    let probe = FakeProbe::new(10 * gb);
    assert!(reg.set_memory_budget(4 * gb, probe.clone()));

    let log = Arc::new(Mutex::new(Vec::new()));
    let (older, newer) = (AgentId::from("older"), AgentId::from("newer"));
    reg.register(older.clone(), recording_kill(older.clone(), log.clone()));
    reg.register(newer.clone(), recording_kill(newer.clone(), log.clone()));
    reg.set_last_active(&older, 100);
    reg.set_last_active(&newer, 200);

    let evicted = {
        let probe = probe.clone();
        reg.evict_while_over_budget(|_| true, move |_| probe.set(2 * gb))
            .await
    };

    assert_eq!(evicted, 1);
    assert_eq!(
        *log.lock().unwrap(),
        vec![older.clone()],
        "no attribution → least-recently-used first"
    );
    assert!(reg.is_registered(&newer));
}

/// The over-budget drain never touches active processes: with every process
/// active it evicts nothing, however far over budget the tree is — admission
/// (and level 4, opt-in) own that case, not the reap sweep.
#[tokio::test]
async fn over_budget_drain_never_touches_active_processes() {
    let gb = super::GB;
    let reg = ProcessRegistry::new(8);
    let probe = FakeProbe::new(10 * gb);
    assert!(reg.set_memory_budget(4 * gb, probe.clone()));

    let log = Arc::new(Mutex::new(Vec::new()));
    let (a, b) = (AgentId::from("a"), AgentId::from("b"));
    for id in [&a, &b] {
        reg.register(id.clone(), recording_kill(id.clone(), log.clone()));
        reg.mark_active(id);
    }
    probe.set_agents(&[(&a, 7 * gb), (&b, 2 * gb)]);

    assert_eq!(reg.evict_while_over_budget(|_| true, |_| {}).await, 0);
    assert!(log.lock().unwrap().is_empty(), "no kill ran");
    assert!(reg.is_registered(&a) && reg.is_registered(&b));
}

/// Under budget (or no budget installed, or no sample yet) the drain is a
/// no-op — the sweep must not evict a single idle process without pressure.
#[tokio::test]
async fn over_budget_drain_is_inert_without_pressure() {
    let gb = super::GB;
    let log = Arc::new(Mutex::new(Vec::new()));

    // No budget installed.
    let reg = ProcessRegistry::new(8);
    let id = AgentId::from("a");
    reg.register(id.clone(), recording_kill(id.clone(), log.clone()));
    assert_eq!(reg.evict_while_over_budget(|_| true, |_| {}).await, 0);

    // Installed but never sampled.
    let reg = ProcessRegistry::new(8);
    assert!(reg.set_memory_budget(4 * gb, Arc::new(NeverSampled)));
    reg.register(id.clone(), recording_kill(id.clone(), log.clone()));
    assert_eq!(reg.evict_while_over_budget(|_| true, |_| {}).await, 0);

    // Sampled under budget.
    let reg = ProcessRegistry::new(8);
    assert!(reg.set_memory_budget(4 * gb, FakeProbe::new(gb)));
    reg.register(id.clone(), recording_kill(id.clone(), log.clone()));
    assert_eq!(reg.evict_while_over_budget(|_| true, |_| {}).await, 0);

    assert!(log.lock().unwrap().is_empty(), "no kill ever ran");
}

/// A candidate whose `try_claim` loses (busy, or another sweep holds it) is
/// skipped, and the drain moves on to the next-largest candidate.
#[tokio::test]
async fn over_budget_drain_skips_candidates_whose_claim_loses() {
    let gb = super::GB;
    let reg = ProcessRegistry::new(8);
    let probe = FakeProbe::new(10 * gb);
    assert!(reg.set_memory_budget(4 * gb, probe.clone()));

    let log = Arc::new(Mutex::new(Vec::new()));
    let (held, next) = (AgentId::from("held"), AgentId::from("next"));
    reg.register(held.clone(), recording_kill(held.clone(), log.clone()));
    reg.register(next.clone(), recording_kill(next.clone(), log.clone()));
    probe.set_agents(&[(&held, 7 * gb), (&next, 2 * gb)]);

    let evicted = {
        let held = held.clone();
        let probe = probe.clone();
        reg.evict_while_over_budget(move |id| *id != held, move |_| probe.set(2 * gb))
            .await
    };

    assert_eq!(evicted, 1);
    assert_eq!(
        *log.lock().unwrap(),
        vec![next.clone()],
        "the lost claim is skipped, not killed"
    );
    assert!(reg.is_registered(&held), "claim holder's candidate kept");
}

/// Turn-start budget re-check (monorepo#2063 B8): an idle registered agent's
/// next turn gates on the budget like a spawn — reclaim by evicting another
/// idle process (never its own), else queue until the tree drains. Never
/// refused.
#[tokio::test]
async fn turn_start_gates_idle_agent_and_reclaims_other_idle_first() {
    let gb = super::GB;
    let reg = Arc::new(ProcessRegistry::new(8));
    let probe = FakeProbe::new(10 * gb);
    assert!(reg.set_memory_budget(4 * gb, probe.clone()));

    let log = Arc::new(Mutex::new(Vec::new()));
    let (warm, other) = (AgentId::from("warm"), AgentId::from("other"));
    reg.register(warm.clone(), recording_kill(warm.clone(), log.clone()));
    reg.register(other.clone(), recording_kill(other.clone(), log.clone()));
    // `other` is the LRU idle process — but even if `warm` were older, its
    // own process must never be the victim (asserted below).
    reg.set_last_active(&warm, 100);
    reg.set_last_active(&other, 200);

    let reg2 = reg.clone();
    let warm2 = warm.clone();
    let handle = tokio::spawn(async move {
        reg2.acquire_turn_start(&warm2, claim_all, release_none)
            .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        *log.lock().unwrap(),
        vec![other.clone()],
        "reclaims the OTHER idle process, never the gated agent's own"
    );
    assert!(
        reg.is_registered(&warm),
        "the gated agent's process survives"
    );
    assert!(
        !handle.is_finished(),
        "still over budget after the one eviction available → the turn waits"
    );

    // Once the tree drains, the queued turn proceeds on its own timed
    // re-check — no registry event fires here.
    probe.set(gb);
    timeout(Duration::from_secs(30), handle)
        .await
        .expect("the turn-start waiter must re-check on its own timer")
        .expect("task ok");
}

/// Busy agents are never gated mid-turn (monorepo#2063 B8 regression): a
/// process marked ACTIVE admits immediately however far over budget the tree
/// is, and so does an agent with no registered process at all (its spawn is
/// gated by `acquire` instead).
#[tokio::test]
async fn turn_start_never_gates_active_or_unregistered_agents() {
    let gb = super::GB;
    let reg = ProcessRegistry::new(8);
    let probe = FakeProbe::new(100 * gb);
    assert!(reg.set_memory_budget(4 * gb, probe.clone()));

    let log = Arc::new(Mutex::new(Vec::new()));
    let busy = AgentId::from("busy");
    reg.register(busy.clone(), recording_kill(busy.clone(), log.clone()));
    reg.mark_active(&busy);

    timeout(
        Duration::from_millis(200),
        reg.acquire_turn_start(&busy, claim_all, release_none),
    )
    .await
    .expect("an active process is never gated mid-turn");

    timeout(
        Duration::from_millis(200),
        reg.acquire_turn_start(&AgentId::from("unregistered"), claim_all, release_none),
    )
    .await
    .expect("an unregistered agent admits immediately (spawn path gates it)");
    assert!(log.lock().unwrap().is_empty(), "nothing was evicted");
}

/// The turn-start gate's queue/evict events carry `reason: "memory-budget"`,
/// and admission charges NO provisional cost — the warm process is already in
/// the tree sample, so a charge would double-count it.
#[tokio::test]
async fn turn_start_gate_events_carry_memory_budget_reason_without_charge() {
    let gb = super::GB;
    let events: Arc<Mutex<Vec<(AgentId, String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();
    let event_fn: super::ProcessEventFn =
        Arc::new(move |agent_id, event_type, _used, _cap, reason| {
            let events = events_clone.clone();
            let agent_id = agent_id.clone();
            let event_type = event_type.to_string();
            let reason = reason.to_string();
            Box::pin(async move {
                events.lock().unwrap().push((agent_id, event_type, reason));
            })
        });

    let reg = Arc::new(ProcessRegistry::new(8).with_event_fn(event_fn));
    let probe = FakeProbe::new(10 * gb);
    assert!(reg.set_memory_budget(4 * gb, probe.clone()));

    let log = Arc::new(Mutex::new(Vec::new()));
    let warm = AgentId::from("warm");
    reg.register(warm.clone(), recording_kill(warm.clone(), log.clone())); // No other idle to evict.

    let reg2 = reg.clone();
    let warm2 = warm.clone();
    let handle = tokio::spawn(async move {
        reg2.acquire_turn_start(&warm2, claim_all, release_none)
            .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    {
        let events = events.lock().unwrap();
        let queued = events
            .iter()
            .find(|(a, e, _)| e == "agent:process:queued" && a == &warm)
            .expect("queued event recorded");
        assert_eq!(queued.2, "memory-budget", "turn-start gate reason");
    }

    probe.set(gb);
    timeout(Duration::from_secs(30), handle)
        .await
        .expect("the drained tree admits the queued turn")
        .expect("task ok");
    // Admission left the charge exactly at the sample — no provisional cost.
    let (_, charged, _) = reg.budget_status().expect("budget installed");
    assert_eq!(charged, Some(gb), "turn-start admission charges nothing");
}

#[tokio::test]
async fn lifecycle_active_processes_are_not_reaped() {
    let reg = ProcessRegistry::new(8);
    let log = Arc::new(Mutex::new(Vec::new()));
    let (a, b) = (AgentId::from("a"), AgentId::from("b"));
    reg.register(a.clone(), recording_kill(a.clone(), log.clone()));
    reg.register(b.clone(), recording_kill(b.clone(), log.clone()));
    reg.mark_active(&a);
    assert!(reg.is_active(&a));

    let evicted = reg.evict_idle(None, claim_all, release_none).await;
    assert_eq!(evicted, 1);
    assert_eq!(
        *log.lock().unwrap(),
        vec![b.clone()],
        "skips the active one"
    );
    assert!(reg.is_registered(&a));
    assert!(!reg.is_registered(&b));
}

#[tokio::test]
async fn process_cap_events_queued_resumed_evicted() {
    use intent_core::events::{AGENT_PROCESS_EVICTED, AGENT_PROCESS_QUEUED, AGENT_PROCESS_RESUMED};

    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone()).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let mgr = AgentManager::new(services, sink, 2); // cap=2 to test eviction and queueing

    // Subscribe to process-cap events with no batching for determinism.
    let mut filter = SubscriptionFilter {
        event_types: vec![
            AGENT_PROCESS_QUEUED.to_string(),
            AGENT_PROCESS_RESUMED.to_string(),
            AGENT_PROCESS_EVICTED.to_string(),
        ],
        ..Default::default()
    };
    filter.batch_window = None;
    let mut sub = bus.subscribe(filter);

    // Insert workspace + session rows so the event callback can resolve workspace_id.
    let ws_id = WorkspaceId::from("test-ws");
    let ts = now_iso();
    let ws = Workspace {
        id: ws_id.clone(),
        title: "Test".into(),
        branch: "main".into(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts,
        last_activity: None,
        tags: vec![],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    };
    store.insert_workspace(&ws).await.unwrap();
    let (a, b) = (AgentId::from("a"), AgentId::from("b"));
    let ts = now_iso();
    store
        .insert_agent_session(&AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: a.clone(),
            workspace_id: ws_id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "A".into(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: AgentStatus::Pending,
            is_active: false,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            created_at: ts.clone(),
            updated_at: ts.clone(),
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
            retired_at: None,
        })
        .await
        .unwrap();
    store
        .insert_agent_session(&AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: b.clone(),
            workspace_id: ws_id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "B".into(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: AgentStatus::Pending,
            is_active: false,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            created_at: ts.clone(),
            updated_at: ts,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
            retired_at: None,
        })
        .await
        .unwrap();

    // Scenario: cap=2, register A and B (idle), then acquire for C (should evict LRU idle A).
    let log = Arc::new(Mutex::new(Vec::new()));
    mgr.registry
        .register(a.clone(), recording_kill(a.clone(), log.clone()));
    mgr.registry.set_last_active(&a, 100); // A is older
    mgr.registry
        .register(b.clone(), recording_kill(b.clone(), log.clone()));
    mgr.registry.set_last_active(&b, 200); // B is newer

    // Acquire for C — cap is full (A and B registered), so should evict LRU idle (A).
    let c = AgentId::from("c");
    let c_clone = c.clone();
    let ts = now_iso();
    store
        .insert_agent_session(&AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: c_clone.clone(),
            workspace_id: ws_id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "C".into(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: AgentStatus::Pending,
            is_active: false,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            created_at: ts.clone(),
            updated_at: ts,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
            retired_at: None,
        })
        .await
        .unwrap();

    mgr.registry.acquire(&c, claim_all, release_none).await;
    // After acquiring, register C.
    mgr.registry
        .register(c.clone(), recording_kill(c.clone(), log.clone()));

    // Collect the eviction event with bounded wait (event emission is async).
    let mut evict_event = None;
    for _ in 0..50 {
        match timeout(Duration::from_millis(100), sub.recv()).await {
            Ok(Some(batch)) => {
                for ev in batch {
                    if ev.event_type == AGENT_PROCESS_EVICTED && ev.data["agentId"] == a.0 {
                        evict_event = Some(ev);
                        break;
                    }
                }
                if evict_event.is_some() {
                    break;
                }
            }
            Ok(None) => break, // subscription closed
            Err(_) => {}       // timeout, try again
        }
    }
    let ev = evict_event.expect("eviction event published");
    assert_eq!(
        ev.event_type, AGENT_PROCESS_EVICTED,
        "eviction path emits agent:process:evicted"
    );
    assert_eq!(
        ev.data["agentId"], a.0,
        "eviction event carries evicted agent id"
    );
    assert_eq!(ev.data["used"], 2, "used count at eviction");
    assert_eq!(ev.data["cap"], 2, "cap value");
    assert_eq!(ev.data["reason"], "slots", "slot-cap eviction reason");

    // Scenario: cap=2, B and C now registered, make both active, acquire for D → should queue.
    mgr.registry.mark_active(&b);
    mgr.registry.mark_active(&c);

    let d = AgentId::from("d");
    let ts = now_iso();
    store
        .insert_agent_session(&AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: d.clone(),
            workspace_id: ws_id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "D".into(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: AgentStatus::Pending,
            is_active: false,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            created_at: ts.clone(),
            updated_at: ts,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
            retired_at: None,
        })
        .await
        .unwrap();

    let reg2 = mgr.registry.clone();
    let d_clone = d.clone();
    let queued_acquire =
        tokio::spawn(async move { reg2.acquire(&d_clone, claim_all, release_none).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !queued_acquire.is_finished(),
        "acquire blocks when all active"
    );

    // Collect queue event with bounded wait (event emission is async).
    let mut queue_event = None;
    for _ in 0..50 {
        match timeout(Duration::from_millis(100), sub.recv()).await {
            Ok(Some(batch)) => {
                for ev in batch {
                    if ev.event_type == AGENT_PROCESS_QUEUED && ev.data["agentId"] == d.0 {
                        queue_event = Some(ev);
                        break;
                    }
                }
                if queue_event.is_some() {
                    break;
                }
            }
            Ok(None) => break, // subscription closed
            Err(_) => {}       // timeout, try again
        }
    }
    let ev = queue_event.expect("queue event published");
    assert_eq!(
        ev.event_type, AGENT_PROCESS_QUEUED,
        "queueing path emits agent:process:queued"
    );
    assert_eq!(
        ev.data["agentId"], d.0,
        "queued event carries queued agent id"
    );
    assert_eq!(ev.data["used"], 2, "used count at queue");
    assert_eq!(ev.data["cap"], 2, "cap value");
    assert_eq!(ev.data["reason"], "slots", "slot-cap queue reason");

    // Mark B idle → should resume D.
    mgr.registry.mark_idle(&b);
    queued_acquire.await.expect("task ok");

    // Collect resume event with bounded wait (event emission is async).
    let mut resume_event = None;
    for _ in 0..50 {
        match timeout(Duration::from_millis(100), sub.recv()).await {
            Ok(Some(batch)) => {
                for ev in batch {
                    if ev.event_type == AGENT_PROCESS_RESUMED && ev.data["agentId"] == d.0 {
                        resume_event = Some(ev);
                        break;
                    }
                }
                if resume_event.is_some() {
                    break;
                }
            }
            Ok(None) => break, // subscription closed
            Err(_) => {}       // timeout, try again
        }
    }
    let ev = resume_event.expect("resume event published");
    assert_eq!(
        ev.event_type, AGENT_PROCESS_RESUMED,
        "resume path emits agent:process:resumed"
    );
    assert_eq!(
        ev.data["agentId"], d.0,
        "resumed event carries resumed agent id"
    );
    assert_eq!(ev.data["used"], 2, "used count after resume");
    assert_eq!(ev.data["cap"], 2, "cap value");
    assert_eq!(ev.data["reason"], "slots", "slot-cap resume reason");
}

async fn manager() -> (TempDb, AgentManager) {
    let (tmp, mgr, _bus) = manager_with_bus().await;
    (tmp, mgr)
}

/// Like [`manager`] but also returns the [`EventBus`] so a test can subscribe and
/// assert which lifecycle events the manager publishes.
async fn manager_with_bus() -> (TempDb, AgentManager, EventBus) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    (tmp, AgentManager::new(services, sink, 8), bus)
}

/// A passive agent handle over an in-memory duplex connection (no child).
fn mock_handle() -> AgentHandle {
    let (client_w, _agent_r) = tokio::io::duplex(1024);
    let (_agent_w, client_r) = tokio::io::duplex(1024);
    let (note_tx, note_rx) = mpsc::unbounded_channel();
    let connection = Arc::new(Connection::new(
        client_w,
        client_r,
        None,
        ConnectionHooks {
            notifications: Some(note_tx),
            ..ConnectionHooks::default()
        },
    ));
    AgentHandle {
        connection,
        notifications: Arc::new(TokioMutex::new(note_rx)),
        serve_task: tokio::spawn(async {}),
        child: None,
        child_pid: None,
        _mcp_bridge: None,
        _mcp_config: None,
        _rules_config: None,
        _pi_extension: None,
        session_mcp_servers: Vec::new(),
        spawned_model: None,
        spawned_provider: "auggie".to_string(),
        thought_level: None,
        wake_gate: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        wake_listener: None,
    }
}

/// Track a mock agent in the manager + registry the way `create_agent` would.
fn track(mgr: &AgentManager, id: &AgentId) {
    mgr.handles
        .lock()
        .unwrap()
        .insert(id.clone(), mock_handle());
    mgr.registry.register(id.clone(), mgr.make_kill(id.clone()));
}

#[tokio::test]
async fn manager_tracks_lookup_stop_and_shuts_down() {
    let (_tmp, mgr) = manager().await;
    let (a, b) = (AgentId::from("a"), AgentId::from("b"));
    track(&mgr, &a);
    track(&mgr, &b);

    assert_eq!(mgr.len(), 2);
    assert_eq!(mgr.registry().size(), 2);
    assert!(mgr.contains(&a));

    assert!(mgr.stop(&a).await);
    assert_eq!(mgr.len(), 1);
    assert!(!mgr.contains(&a));
    assert!(!mgr.registry().is_registered(&a));

    mgr.shutdown().await;
    assert!(mgr.is_empty(), "shutdown tears down every tracked agent");
    assert_eq!(mgr.registry().size(), 0);
}

/// Graceful shutdown flushes a busy agent's partial in-flight assistant content
/// (the live-turn slot) as an `assistant` row tagged with the FE
/// terminal-message convention (`metadata.interrupted = true` +
/// `stopReason = "interrupted"`) — reusing the turn's minted message id so
/// block ids match what streamed — alongside the `interrupted_agent` row. A busy
/// agent with no live-turn slot gets only the interrupted row (no phantom
/// assistant message).
#[tokio::test]
async fn shutdown_flushes_partial_live_turn_as_interrupted_assistant_row() {
    let (_tmp, mgr) = manager().await;
    let ws = WorkspaceId::from("ws-shutdown-flush");
    let with_partial = AgentId::from("a-partial");
    let without_partial = AgentId::from("a-no-partial");
    seed_agent(&mgr, &ws, &with_partial).await;
    insert_extra_session(&mgr, &ws, &without_partial).await;
    track(&mgr, &with_partial);
    track(&mgr, &without_partial);
    assert!(mgr.try_begin(&with_partial, &ws).await);
    assert!(mgr.try_begin(&without_partial, &ws).await);

    // Simulate a mid-stream turn: the live-turn slot holds coalesced blocks.
    let blocks = vec![
        json!({ "type": "text", "id": "msg-flush:0", "text": "partial answer…" }),
        json!({
            "type": "tool_use",
            "id": "msg-flush:1",
            "name": "read_file",
            "input": {},
            "toolCallId": "call-1"
        }),
    ];
    mgr.services
        .set_live_turn(&with_partial, "msg-flush", blocks.clone());

    mgr.shutdown().await;

    // The partial content persisted as an assistant row with the turn's
    // message id and metadata.status = "interrupted".
    let messages = mgr
        .services
        .store
        .get_agent_messages(&with_partial, None)
        .await
        .expect("messages");
    assert_eq!(messages.len(), 1, "exactly one flushed assistant row");
    let msg = &messages[0];
    assert_eq!(msg.id, "msg-flush");
    assert_eq!(msg.role, "assistant");
    assert_eq!(msg.content, Value::Array(blocks));
    let metadata = msg.metadata.as_ref().expect("metadata");
    assert_eq!(metadata["interrupted"], true);
    assert_eq!(metadata["stopReason"], "interrupted");
    assert_eq!(metadata["status"], "interrupted");
    assert_eq!(
        metadata["interruptReason"], "daemon_shutdown",
        "shutdown flush stamps the machine-readable reason"
    );
    assert!(
        metadata.get("interruptedBy").is_none(),
        "no sender attribution outside message preemption"
    );

    // Both busy agents got interrupted_agent rows; the one without a live-turn
    // slot got no assistant row.
    for id in [&with_partial, &without_partial] {
        assert!(
            mgr.services
                .store
                .get_interrupted_agent(id)
                .await
                .expect("get interrupted")
                .is_some(),
            "interrupted row for {id}"
        );
    }
    let other = mgr
        .services
        .store
        .get_agent_messages(&without_partial, None)
        .await
        .expect("messages");
    assert!(
        other.is_empty(),
        "no phantom assistant row without live turn"
    );
}

#[tokio::test]
async fn reap_idle_evicts_handles_and_deregisters() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (a, b) = (AgentId::from("a"), AgentId::from("b"));
    track(&mgr, &a);
    track(&mgr, &b);

    let reaped = mgr.reap_idle(None).await;
    assert_eq!(reaped, 2);
    assert!(mgr.is_empty(), "reap drops the manager handles");
    assert_eq!(mgr.registry().size(), 0);
}

#[tokio::test]
async fn evict_idle_older_than_evicts_only_stale_idle() {
    let reg = ProcessRegistry::new(8);
    let log = Arc::new(Mutex::new(Vec::new()));
    let (old, fresh, active) = (
        AgentId::from("old"),
        AgentId::from("fresh"),
        AgentId::from("active"),
    );
    for id in [&old, &fresh, &active] {
        reg.register(id.clone(), recording_kill(id.clone(), log.clone()));
    }
    // `old` last streamed at the epoch (well past any TTL); `fresh` just now;
    // `active` is streaming (protected regardless of its timestamp).
    reg.set_last_active(&old, 1);
    reg.set_last_active(&fresh, super::now_ms());
    reg.set_last_active(&active, 1);
    reg.mark_active(&active);

    let evicted = reg
        .evict_idle_older_than(Duration::from_secs(60), |_| true, |_| {})
        .await;

    assert_eq!(evicted, 1, "only the stale idle process is reaped");
    assert_eq!(*log.lock().unwrap(), vec![old.clone()]);
    assert!(!reg.is_registered(&old));
    assert!(reg.is_registered(&fresh), "within-TTL idle kept");
    assert!(reg.is_registered(&active), "active process kept");
}

/// monorepo#3040 — the TTL idle sweep was the ONE eviction path that emitted
/// no `agent:process:evicted`, so an FE chat session bound to the reaped agent
/// had no signal it was gone. The sweep must fire the registry event callback
/// with the additive reason `"idle-ttl"` for every process it evicts.
#[tokio::test]
async fn ttl_eviction_emits_evicted_event_with_idle_ttl_reason() {
    let events: Arc<Mutex<Vec<(AgentId, String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();
    let event_fn: super::ProcessEventFn =
        Arc::new(move |agent_id, event_type, _used, _cap, reason| {
            let events = events_clone.clone();
            let agent_id = agent_id.clone();
            let event_type = event_type.to_string();
            let reason = reason.to_string();
            Box::pin(async move {
                events.lock().unwrap().push((agent_id, event_type, reason));
            })
        });
    let reg = ProcessRegistry::new(8).with_event_fn(event_fn);
    let log = Arc::new(Mutex::new(Vec::new()));
    let (stale, fresh) = (AgentId::from("stale"), AgentId::from("fresh"));
    reg.register(stale.clone(), recording_kill(stale.clone(), log.clone()));
    reg.register(fresh.clone(), recording_kill(fresh.clone(), log.clone()));
    reg.set_last_active(&stale, 1);
    reg.set_last_active(&fresh, super::now_ms());

    let evicted = reg
        .evict_idle_older_than(Duration::from_secs(60), |_| true, |_| {})
        .await;
    assert_eq!(evicted, 1);

    // The callback future is spawned; give it a bounded window to record.
    let mut recorded = Vec::new();
    for _ in 0..50 {
        recorded = events.lock().unwrap().clone();
        if !recorded.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        recorded,
        vec![(
            stale.clone(),
            "agent:process:evicted".to_string(),
            "idle-ttl".to_string()
        )],
        "the TTL sweep emits agent:process:evicted with reason idle-ttl for the stale process only"
    );
}

/// Regression for monorepo#3040 — a send to an agent the TTL sweep just
/// reaped must NOT be silently dropped. The reap only kills the child process
/// (the session row survives), so `send_message` must take the auto-restore
/// path: claim the turn slot, persist the user row, and start a turn worker
/// (which respawns the child on demand). The eviction itself must be
/// observable on the event bus as `agent:process:evicted` with reason
/// `idle-ttl` so a bound FE session can react.
#[tokio::test]
async fn send_after_ttl_reap_restores_instead_of_silently_dropping() {
    use intent_core::events::AGENT_PROCESS_EVICTED;

    let (_tmp, mgr, bus) = manager_with_bus().await;
    let mgr = Arc::new(mgr);
    let ws = WorkspaceId::from("ws-reap-send");
    let id = AgentId::from("a-reap-send");
    seed_agent(&mgr, &ws, &id).await;
    track(&mgr, &id);
    mgr.registry().set_last_active(&id, 1);

    let mut filter = SubscriptionFilter {
        event_types: vec![AGENT_PROCESS_EVICTED.to_string()],
        ..Default::default()
    };
    filter.batch_window = None;
    let mut sub = bus.subscribe(filter);

    assert_eq!(mgr.reap_idle_older_than(Duration::from_secs(60)).await, 1);
    assert!(!mgr.contains(&id), "the reap dropped the child handle");
    assert!(
        mgr.services.store.get_agent_session(&id).await.is_ok(),
        "the reap keeps the session row (only the process dies)"
    );

    // The eviction is observable: agent:process:evicted with reason idle-ttl.
    let mut evict_event = None;
    for _ in 0..50 {
        match timeout(Duration::from_millis(100), sub.recv()).await {
            Ok(Some(batch)) => {
                if let Some(ev) = batch
                    .into_iter()
                    .find(|ev| ev.event_type == AGENT_PROCESS_EVICTED && ev.data["agentId"] == id.0)
                {
                    evict_event = Some(ev);
                    break;
                }
            }
            Ok(None) => break,
            Err(_) => {}
        }
    }
    let ev = evict_event.expect("TTL reap publishes agent:process:evicted");
    assert_eq!(ev.data["reason"], "idle-ttl", "TTL sweep eviction reason");

    // A user send to the reaped agent restores: the RPC succeeds, the user
    // row is persisted (nothing silently dropped), and `queued == false`
    // proves `try_begin` won the turn slot (restore path engaged) — the
    // spawned worker respawns the child on demand. No `is_busy` assertion
    // here: the worker's spawn attempt fails in this env and releases the
    // slot, so reading it after the send races that release (#1356 review).
    let result = mgr
        .send_message(
            id.clone(),
            ws.clone(),
            "are you still there?".to_string(),
            None,
            super::TurnOptions {
                origin: intent_core::MessageOrigin::User,
                ..super::TurnOptions::default()
            },
        )
        .await
        .expect("send to a reaped agent must not error");
    assert_eq!(result["success"], json!(true));
    assert_eq!(
        result["queued"],
        json!(false),
        "the reap claim is released, so the send starts a turn: {result}"
    );

    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    assert!(
        messages
            .iter()
            .any(|m| m.role == "user" && m.content.to_string().contains("are you still there?")),
        "the post-reap send's user row is persisted, not dropped"
    );
}

/// monorepo#2104 (#1161 review) — a live-turn slot that outlived its turn must
/// not outlive it INTO the next turn. `flush_partial_turn_on_interruption`
/// deliberately keeps the slot on a non-UNIQUE store error (it is the only copy
/// of the streamed content), and `end_turn` then releases busy. The next turn
/// claims busy here, but its worker replaces the slot only in `begin_live_turn`
/// — after the user row INSERT, the task spawn and the ACP session setup. For
/// that whole window the pair (busy = true, slot = the PREVIOUS turn's content)
/// would otherwise be readable, and `chat_snapshot` would serve the old content
/// as `isStreaming: true`. Claiming the slot drops it.
#[tokio::test]
async fn try_begin_drops_a_live_turn_slot_that_outlived_its_turn() {
    let (_tmp, mgr) = manager().await;
    let ws = WorkspaceId::from("ws-stale-slot");
    let id = AgentId::from("a-stale-slot");
    seed_agent(&mgr, &ws, &id).await;
    track(&mgr, &id);

    // The orphan a failed flush leaves behind: content, no busy claim.
    mgr.services.set_live_turn(
        &id,
        "msg-previous-turn",
        vec![json!({ "type": "text", "id": "msg-previous-turn:0", "text": "I'll run " })],
    );
    assert!(
        mgr.services.live_turn(&id).is_some(),
        "precondition: the orphan slot is published while the agent is idle"
    );

    assert!(mgr.try_begin(&id, &ws).await, "the next turn claims busy");

    assert!(
        mgr.services.live_turn(&id).is_none(),
        "the previous turn's slot must not survive the new turn's claim"
    );
    assert!(mgr.is_busy(&id), "…and the claim itself still stands");
}

/// intentd#1157 composition corner: the claim must leave a slot whose teardown
/// flush is IN FLIGHT alone.
///
/// `interrupt_inner` pins WITHOUT a busy claim — its gates are a live connection
/// handle and a persisted `acpSessionId` — so a stop against an idle agent can
/// have a pin+flush in flight while a concurrent message wins `try_begin`. Since
/// monorepo#2110 the flush re-reads the slot AS OF FLUSH TIME, so clearing it
/// here would do double damage: drop the content, and make the flush read the
/// pinned slot as vanished — which it interprets as "the worker already
/// persisted the full row", silently losing the turn instead of recording it.
#[tokio::test]
async fn try_begin_leaves_a_slot_whose_teardown_flush_is_in_flight() {
    let (_tmp, mgr) = manager().await;
    let ws = WorkspaceId::from("ws-pinned-slot");
    let id = AgentId::from("a-pinned-slot");
    seed_agent(&mgr, &ws, &id).await;
    track(&mgr, &id);

    mgr.services.set_live_turn(
        &id,
        "msg-being-flushed",
        vec![json!({ "type": "text", "id": "msg-being-flushed:0", "text": "I'll run " })],
    );
    // The teardown path pins immediately before aborting the worker; its flush
    // has not run yet.
    mgr.services.pin_live_turn(&id);

    assert!(mgr.try_begin(&id, &ws).await, "the new message claims busy");

    let live = mgr
        .services
        .live_turn(&id)
        .expect("a pin with a flush still in flight owns the slot: the claim must not clear it");
    assert_eq!(live.message_id, "msg-being-flushed");
    assert!(live.flush_pending, "…and the pin is untouched");
}

/// The other side of that guard, and the reason it cannot simply be "skip
/// anything pinned": the deliberate flush-failure keep stays pinned FOREVER, so
/// a blanket pin-skip would sail it into the next turn and re-expose the
/// monorepo#2138 stale-gilding this clear exists to prevent.
///
/// Driven through the real give-up arm — the flush targets an agent with no
/// `agent_session` row, so the append fails the `agent_message.agent_id` foreign
/// key (a genuine store error, NOT the UNIQUE-id collision), which is the arm
/// that keeps the slot.
#[tokio::test]
async fn try_begin_drops_a_slot_whose_flush_already_gave_up() {
    let (_tmp, mgr) = manager().await;
    let ws = WorkspaceId::from("ws-flush-gave-up");
    // Deliberately NOT seeded: no agent_session row, so the flush's INSERT
    // violates agent_message's foreign key.
    let id = AgentId::from("a-flush-gave-up");
    track(&mgr, &id);

    mgr.services.set_live_turn(
        &id,
        "msg-stranded",
        vec![json!({ "type": "text", "id": "msg-stranded:0", "text": "I'll run " })],
    );
    mgr.services.pin_live_turn(&id);

    let flushed = mgr
        .services
        .flush_pinned_turn_on_interruption(
            &id,
            crate::agent_session::InterruptReason::UserStop,
            None,
        )
        .await
        .expect("the pinned slot was there to flush");
    assert!(
        flushed.message_id.is_none(),
        "precondition: the store rejected the append, so nothing was persisted"
    );
    let kept = mgr
        .services
        .live_turn(&id)
        .expect("the give-up arm keeps the slot as the only copy of the content");
    assert!(
        kept.flush_pending,
        "the pin is kept too, so the guard drop and the normal turn-end clear still leave it alone"
    );
    assert!(
        kept.flush_failed,
        "…but it is marked abandoned: no flush is coming back for it"
    );

    assert!(mgr.try_begin(&id, &ws).await, "the next turn claims busy");

    assert!(
        mgr.services.live_turn(&id).is_none(),
        "an abandoned slot must not outlive its turn into the next one (monorepo#2138)"
    );
}

/// A later teardown re-pinning an abandoned slot gives it a real second chance:
/// the fresh pin clears the abandoned mark, so the claim guard protects that
/// flush exactly as it protects a first attempt.
#[tokio::test]
async fn re_pinning_an_abandoned_slot_makes_it_in_flight_again() {
    let (_tmp, mgr) = manager().await;
    let ws = WorkspaceId::from("ws-repin");
    let id = AgentId::from("a-repin");
    track(&mgr, &id);

    mgr.services.set_live_turn(
        &id,
        "msg-stranded",
        vec![json!({ "type": "text", "id": "msg-stranded:0", "text": "I'll run " })],
    );
    mgr.services.pin_live_turn(&id);
    let _ = mgr
        .services
        .flush_pinned_turn_on_interruption(
            &id,
            crate::agent_session::InterruptReason::UserStop,
            None,
        )
        .await;
    assert!(mgr.services.live_turn(&id).is_some_and(|s| s.flush_failed));

    // A second teardown (e.g. graceful shutdown) reaches the same stranded content.
    mgr.services.pin_live_turn(&id);

    assert!(
        mgr.services.live_turn(&id).is_some_and(|s| !s.flush_failed),
        "a fresh pin means a fresh attempt is in flight"
    );
    assert!(mgr.try_begin(&id, &ws).await);
    assert!(
        mgr.services.live_turn(&id).is_some(),
        "…so the claim leaves it to that flush again"
    );
}

/// The claim must not disturb a slot when it does NOT win: a second `try_begin`
/// against an agent whose turn is already running is a no-op, so a live turn's
/// own slot survives a losing claim.
#[tokio::test]
async fn losing_try_begin_leaves_the_running_turns_slot_intact() {
    let (_tmp, mgr) = manager().await;
    let ws = WorkspaceId::from("ws-losing-claim");
    let id = AgentId::from("a-losing-claim");
    seed_agent(&mgr, &ws, &id).await;
    track(&mgr, &id);

    assert!(mgr.try_begin(&id, &ws).await);
    mgr.services.set_live_turn(
        &id,
        "msg-running",
        vec![json!({ "type": "text", "id": "msg-running:0", "text": "streaming…" })],
    );

    assert!(!mgr.try_begin(&id, &ws).await, "the second claim loses");

    let live = mgr
        .services
        .live_turn(&id)
        .expect("the running turn's slot survives a losing claim");
    assert_eq!(live.message_id, "msg-running");
}

/// A winning `try_begin` in an ARCHIVED workspace auto-unarchives it: the
/// row flips to Active and the §6.5 `workspace:updated` delta carries the
/// additive `autoUnarchive` stamp naming the triggering agent. The claim
/// itself still succeeds — the unarchive is a side effect of the turn
/// start, never a gate on it.
#[tokio::test]
async fn winning_try_begin_auto_unarchives_the_workspace() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let ws = WorkspaceId::from("ws-auto-unarchive");
    let id = AgentId::from("a-auto-unarchive");
    seed_agent(&mgr, &ws, &id).await;
    let mut row = mgr.services.store.get_workspace(&ws).await.unwrap();
    row.status = WorkspaceStatus::Archived;
    row.archived = true;
    row.archived_at = Some(now_iso());
    mgr.services
        .store
        .update_workspace(&row)
        .await
        .expect("archive row");

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    assert!(
        mgr.try_begin(&id, &ws).await,
        "the claim wins despite the archived workspace"
    );

    let after = mgr.services.store.get_workspace(&ws).await.unwrap();
    assert!(!after.archived, "turn start flipped the row to Active");
    assert_eq!(after.status, WorkspaceStatus::Active);
    assert!(after.archived_at.is_none());

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let delta = events
        .iter()
        .find(|e| e.event_type == "workspace:updated")
        .expect("auto-unarchive published workspace:updated");
    assert_eq!(
        delta.data["changes"],
        json!({
            "archived": false,
            "status": "Active",
            "archivedAt": null,
            "autoUnarchive": {
                "reason": "agent_activity",
                "agentId": id.0,
                "agentName": "Builder",
            },
        }),
        "stamped §6.5 delta"
    );

    // The confirmed flip persisted exactly one `auto_unarchived` system row
    // (spec Contract text + metadata) and emitted `agent:message`.
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .unwrap();
    assert_eq!(messages.len(), 1, "exactly one notice row");
    let notice = &messages[0];
    assert_eq!(notice.role, "system");
    assert_eq!(
        notice.content,
        json!([{ "type": "text", "text": super::AUTO_UNARCHIVE_NOTICE_TEXT }])
    );
    assert_eq!(
        notice.metadata,
        Some(json!({ "type": "auto_unarchived", "reason": "agent_activity" }))
    );
    let msg_event = events
        .iter()
        .find(|e| e.event_type == "agent:message")
        .expect("notice emitted agent:message");
    assert_eq!(msg_event.data["role"], json!("system"));
    assert_eq!(msg_event.data["messageId"], json!(notice.id));

    // THIS turn's outbound prompt carries the trailing notice block…
    let prompt = mgr
        .build_turn_prompt(&id, &ws, "hello", &super::TurnOptions::default())
        .await;
    let last = serde_json::to_value(prompt.last().expect("non-empty prompt")).unwrap();
    assert_eq!(last["text"], json!(super::AUTO_UNARCHIVE_PROMPT_NOTICE));
    mgr.end_turn(&id).await;

    // …and the NEXT turn's does not (the flag was consumed).
    assert!(mgr.try_begin(&id, &ws).await, "second claim wins");
    let next = mgr
        .build_turn_prompt(&id, &ws, "hello", &super::TurnOptions::default())
        .await;
    assert!(
        !serde_json::to_string(&next)
            .unwrap()
            .contains("automatically unarchived"),
        "the notice must not replay on later turns"
    );
    mgr.end_turn(&id).await;
}

/// A claim in a NON-archived workspace persists no `auto_unarchived` notice
/// and injects nothing into the turn's prompt.
#[tokio::test]
async fn winning_try_begin_in_active_workspace_persists_no_notice() {
    let (_tmp, mgr) = manager().await;
    let ws = WorkspaceId::from("ws-active-no-notice");
    let id = AgentId::from("a-active-no-notice");
    seed_agent(&mgr, &ws, &id).await;

    assert!(mgr.try_begin(&id, &ws).await);
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .unwrap();
    assert!(messages.is_empty(), "no notice row on an active workspace");
    let prompt = mgr
        .build_turn_prompt(&id, &ws, "hello", &super::TurnOptions::default())
        .await;
    assert!(
        !serde_json::to_string(&prompt)
            .unwrap()
            .contains("automatically unarchived"),
        "no prompt notice on an active workspace"
    );
    mgr.end_turn(&id).await;
}

/// The suppressed re-claim (`auto_unarchive = false`, the worker's raced
/// end-of-turn re-check) never persists the notice nor arms the prompt flag
/// — the workspace stays archived and the transcript stays clean.
#[tokio::test]
async fn suppressed_reclaim_persists_no_notice() {
    let (_tmp, mgr) = manager().await;
    let ws = WorkspaceId::from("ws-suppressed-reclaim");
    let id = AgentId::from("a-suppressed-reclaim");
    seed_agent(&mgr, &ws, &id).await;
    let mut row = mgr.services.store.get_workspace(&ws).await.unwrap();
    row.status = WorkspaceStatus::Archived;
    row.archived = true;
    row.archived_at = Some(now_iso());
    mgr.services
        .store
        .update_workspace(&row)
        .await
        .expect("archive row");

    assert_eq!(
        mgr.try_begin_outcome(&id, &ws, false).await,
        super::TryBeginOutcome::Started
    );
    let after = mgr.services.store.get_workspace(&ws).await.unwrap();
    assert!(
        after.archived,
        "suppressed claim leaves the workspace archived"
    );
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .unwrap();
    assert!(
        messages.is_empty(),
        "no notice row on the suppressed re-claim"
    );
    let prompt = mgr
        .build_turn_prompt(&id, &ws, "hello", &super::TurnOptions::default())
        .await;
    assert!(
        !serde_json::to_string(&prompt)
            .unwrap()
            .contains("automatically unarchived"),
        "no prompt notice on the suppressed re-claim"
    );
    mgr.end_turn(&id).await;
}

/// A stale prompt flag never leaks across a slot release: a claim whose turn
/// never built a prompt clears the flag when the slot releases, so the next
/// claim's prompt is clean.
#[tokio::test]
async fn auto_unarchive_prompt_flag_cleared_on_slot_release() {
    let (_tmp, mgr) = manager().await;
    let ws = WorkspaceId::from("ws-flag-hygiene");
    let id = AgentId::from("a-flag-hygiene");
    seed_agent(&mgr, &ws, &id).await;
    let mut row = mgr.services.store.get_workspace(&ws).await.unwrap();
    row.status = WorkspaceStatus::Archived;
    row.archived = true;
    row.archived_at = Some(now_iso());
    mgr.services
        .store
        .update_workspace(&row)
        .await
        .expect("archive row");

    // The claim flips the workspace and arms the flag, but the turn ends
    // without ever building a prompt (e.g. a harness wake turn).
    assert!(mgr.try_begin(&id, &ws).await);
    mgr.end_turn(&id).await;

    assert!(mgr.try_begin(&id, &ws).await, "next claim wins");
    let prompt = mgr
        .build_turn_prompt(&id, &ws, "hello", &super::TurnOptions::default())
        .await;
    assert!(
        !serde_json::to_string(&prompt)
            .unwrap()
            .contains("automatically unarchived"),
        "the stale flag must not leak into the next turn's prompt"
    );
    mgr.end_turn(&id).await;
}

/// The arm-skipped-after-release interleaving: `try_begin_outcome` arms the
/// prompt flag only while THIS claim still holds the slot, decided under the
/// `busy` lock. A concurrent stop/teardown releasing the slot during the
/// awaits between the claim and the arm (`release_slot_sync` clears nothing)
/// must cause the late arm to be SKIPPED — an unconditional insert would
/// leave a stale flag that leaks the notice into a later, non-triggering
/// turn. Drives `arm_auto_unarchive_flag_if_slot_held` at both edges of the
/// race window.
#[tokio::test]
async fn auto_unarchive_flag_arm_skipped_when_slot_released() {
    let (_tmp, mgr) = manager().await;
    let ws = WorkspaceId::from("ws-arm-race");
    let id = AgentId::from("a-arm-race");
    seed_agent(&mgr, &ws, &id).await;

    // Slot held: the arm lands.
    assert!(mgr.try_begin(&id, &ws).await);
    mgr.arm_auto_unarchive_flag_if_slot_held(&id);
    assert!(
        mgr.auto_unarchived.lock().unwrap().contains(&id),
        "the arm lands while the claim holds the slot"
    );
    mgr.end_turn(&id).await;
    assert!(
        !mgr.auto_unarchived.lock().unwrap().contains(&id),
        "the release clears the armed flag"
    );

    // The race: the slot was released (concurrent stop/teardown) before the
    // arm ran — the late arm must be skipped, not inserted after the clear.
    mgr.arm_auto_unarchive_flag_if_slot_held(&id);
    assert!(
        !mgr.auto_unarchived.lock().unwrap().contains(&id),
        "a late arm after the slot release must be skipped"
    );

    // The next turn's prompt stays clean.
    assert!(mgr.try_begin(&id, &ws).await, "next claim wins");
    let prompt = mgr
        .build_turn_prompt(&id, &ws, "hello", &super::TurnOptions::default())
        .await;
    assert!(
        !serde_json::to_string(&prompt)
            .unwrap()
            .contains("automatically unarchived"),
        "no stale notice leaks into the later, non-triggering turn"
    );
    mgr.end_turn(&id).await;
}

/// A winning `try_begin` whose workspace row is MISSING (unarchivable) must
/// still start the turn — the auto-unarchive is best-effort and never
/// blocks or fails the claim.
#[tokio::test]
async fn winning_try_begin_survives_auto_unarchive_failure() {
    let (_tmp, mgr) = manager().await;
    let ws = WorkspaceId::from("ws-missing-row");
    let id = AgentId::from("a-orphan-turn");
    // No workspace/session rows seeded: the workspace read inside the
    // auto-unarchive fails, and the claim must still succeed.
    assert!(
        mgr.try_begin(&id, &ws).await,
        "a failed auto-unarchive must not block the turn"
    );
    assert!(mgr.is_busy(&id), "the slot is held");
    mgr.end_turn(&id).await;
}

#[tokio::test]
async fn reap_idle_older_than_skips_in_flight_agents() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (busy, idle) = (AgentId::from("busy"), AgentId::from("idle"));
    track(&mgr, &busy);
    track(&mgr, &idle);
    // Both stale past the TTL, but `busy` has an in-flight prompt.
    mgr.registry().set_last_active(&busy, 1);
    mgr.registry().set_last_active(&idle, 1);
    assert!(mgr.try_begin(&busy, &WorkspaceId::new()).await);

    let reaped = mgr.reap_idle_older_than(Duration::from_secs(60)).await;

    assert_eq!(reaped, 1, "only the idle agent is reaped");
    assert!(
        mgr.contains(&busy),
        "agent with an in-flight prompt is kept"
    );
    assert!(!mgr.contains(&idle));
    assert_eq!(mgr.registry().size(), 1);
}

/// monorepo#2118 — the idle-reap TOCTOU. The sweep must CLAIM a candidate
/// atomically against `try_begin` before killing it: with the old bare busy
/// check, a turn starting between the eligibility check and the `kill().await`
/// had its freshly-spawned child tree killed mid-turn. This drives the exact
/// interleaving deterministically: the kill blocks on a channel, and while it
/// is mid-kill a `try_begin` must LOSE (the message parks on the queue) rather
/// than start a turn. After the sweep the claim is gone and `try_begin` wins
/// again.
#[tokio::test]
async fn reap_claim_blocks_try_begin_during_kill_window() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let ws = WorkspaceId::from("ws-reap-claim");
    let id = AgentId::from("a-reap-claim");
    seed_agent(&mgr, &ws, &id).await;

    // A kill that signals entry and then blocks until the test releases it,
    // exposing the mid-kill window.
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel::<()>();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let kill: KillFn = Arc::new(move || {
        let entered_tx = entered_tx.clone();
        let release_rx = release_rx.clone();
        Box::pin(async move {
            let _ = entered_tx.send(());
            let rx = release_rx.lock().unwrap().take();
            if let Some(rx) = rx {
                let _ = rx.await;
            }
        })
    });
    mgr.registry().register(id.clone(), kill);
    mgr.registry().set_last_active(&id, 1);

    let sweep = {
        let mgr = mgr.clone();
        tokio::spawn(async move { mgr.reap_idle_older_than(Duration::from_secs(60)).await })
    };
    entered_rx.recv().await.expect("kill entered");

    // Mid-kill: the claim must make this turn LOSE, exactly like busy.
    assert!(
        !mgr.try_begin(&id, &ws).await,
        "try_begin must lose against a held reap claim (the TOCTOU)"
    );

    release_tx.send(()).unwrap();
    assert_eq!(sweep.await.unwrap(), 1);
    assert!(!mgr.registry().is_registered(&id), "candidate was reaped");
    assert!(
        mgr.try_begin(&id, &ws).await,
        "claim is released after the kill; the next turn starts normally"
    );
}

/// monorepo#2118 — the candidate snapshot is stale by the time earlier kills
/// in the same sweep have awaited, so each claimed candidate is re-validated
/// under the registry lock before its kill. An agent that ran a whole turn
/// mid-sweep (fresh `last_active_ms`) must be released and kept, not killed
/// off the stale snapshot.
#[tokio::test]
async fn reap_revalidates_candidate_after_earlier_kills_await() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let ws = WorkspaceId::from("ws-reap-revalidate");
    let (a, b) = (AgentId::from("a-first"), AgentId::from("b-second"));
    seed_agent(&mgr, &ws, &b).await;

    // `a` kills first (older timestamp) and blocks; while it blocks, `b`
    // runs a turn (fresh `last_active_ms`), invalidating `b`'s candidacy.
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel::<()>();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let kill_a: KillFn = Arc::new(move || {
        let entered_tx = entered_tx.clone();
        let release_rx = release_rx.clone();
        Box::pin(async move {
            let _ = entered_tx.send(());
            let rx = release_rx.lock().unwrap().take();
            if let Some(rx) = rx {
                let _ = rx.await;
            }
        })
    });
    mgr.registry().register(a.clone(), kill_a);
    mgr.registry().set_last_active(&a, 1);
    let log = Arc::new(Mutex::new(Vec::new()));
    mgr.registry()
        .register(b.clone(), recording_kill(b.clone(), log.clone()));
    mgr.registry().set_last_active(&b, 2);

    let sweep = {
        let mgr = mgr.clone();
        tokio::spawn(async move { mgr.reap_idle_older_than(Duration::from_secs(60)).await })
    };
    entered_rx.recv().await.expect("a's kill entered");

    // While `a`'s kill blocks, `b` runs a whole turn and goes idle again.
    mgr.registry().mark_active(&b);
    mgr.registry().mark_idle(&b);

    release_tx.send(()).unwrap();
    assert_eq!(sweep.await.unwrap(), 1, "only `a` is reaped");
    assert!(!mgr.registry().is_registered(&a));
    assert!(
        mgr.registry().is_registered(&b),
        "`b` re-validated fresh and kept"
    );
    assert!(log.lock().unwrap().is_empty(), "`b`'s kill never ran");
    assert!(
        mgr.try_begin(&b, &ws).await,
        "`b`'s claim was released on the re-validation reject"
    );
}

/// monorepo#2118 (PR review) — an overlapping sweep must not double-claim: a
/// candidate whose id is ALREADY in `reap_claims` (another sweep holds it
/// mid-kill) is skipped, not killed. With the old unconditional-`true` claim,
/// the second sweep would kill it and its release would drop the first
/// sweep's still-needed claim, reopening the window mid-kill.
#[tokio::test]
async fn overlapping_sweep_does_not_double_claim() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let ws = WorkspaceId::from("ws-reap-overlap");
    let id = AgentId::from("a-reap-overlap");
    seed_agent(&mgr, &ws, &id).await;

    let log = Arc::new(Mutex::new(Vec::new()));
    mgr.registry()
        .register(id.clone(), recording_kill(id.clone(), log.clone()));
    mgr.registry().set_last_active(&id, 1);

    // Another sweep holds the claim (mid-kill).
    mgr.reap_claims.lock().unwrap().insert(id.clone());
    assert_eq!(mgr.reap_idle_older_than(Duration::from_secs(60)).await, 0);
    assert!(log.lock().unwrap().is_empty(), "no kill under a held claim");
    assert!(mgr.registry().is_registered(&id), "candidate kept");
    assert!(
        mgr.reap_claims.lock().unwrap().contains(&id),
        "the holder's claim is untouched (a lost try_claim never releases)"
    );

    // Holder releases: the next sweep claims and reaps normally.
    mgr.reap_claims.lock().unwrap().remove(&id);
    assert_eq!(mgr.reap_idle_older_than(Duration::from_secs(60)).await, 1);
    assert!(!mgr.registry().is_registered(&id));
}

/// A kill that signals entry on a channel and then blocks until the test
/// releases it, exposing the mid-kill window (the monorepo#2118/#2247 races).
fn blocking_kill() -> (
    KillFn,
    mpsc::UnboundedReceiver<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (entered_tx, entered_rx) = mpsc::unbounded_channel::<()>();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let kill: KillFn = Arc::new(move || {
        let entered_tx = entered_tx.clone();
        let release_rx = release_rx.clone();
        Box::pin(async move {
            let _ = entered_tx.send(());
            let rx = release_rx.lock().unwrap().take();
            if let Some(rx) = rx {
                let _ = rx.await;
            }
        })
    });
    (kill, entered_rx, release_tx)
}

/// Fill the registry's remaining slots with ACTIVE processes so the eviction
/// paths under test have exactly the candidates the test registered.
fn fill_active(mgr: &AgentManager, n: usize, log: &Arc<Mutex<Vec<AgentId>>>) {
    for i in 0..n {
        let id = AgentId::from(format!("filler-{i}").as_str());
        mgr.registry
            .register(id.clone(), recording_kill(id.clone(), log.clone()));
        mgr.registry.mark_active(&id);
    }
}

/// monorepo#2247 — the slot-cap eviction TOCTOU. `acquire`'s evict arm must
/// CLAIM its victim atomically against `try_begin` before killing it, exactly
/// like the TTL sweep (monorepo#2118): with the old bare LRU pick, a turn
/// starting between candidate selection and the `kill().await` had its
/// freshly-spawned child tree killed mid-turn. Mid-kill a `try_begin` must
/// LOSE (the message parks on the queue); after the eviction the claim is
/// gone and `try_begin` wins again.
#[tokio::test]
async fn acquire_evict_claim_blocks_try_begin_during_kill_window() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let ws = WorkspaceId::from("ws-acquire-claim");
    let victim = AgentId::from("a-acquire-claim");
    seed_agent(&mgr, &ws, &victim).await;

    let (kill, mut entered_rx, release_tx) = blocking_kill();
    mgr.registry.register(victim.clone(), kill);
    mgr.registry.set_last_active(&victim, 1);
    // At cap (8) with the victim as the only idle candidate.
    let log = Arc::new(Mutex::new(Vec::new()));
    fill_active(&mgr, 7, &log);

    let acquire = {
        let mgr = mgr.clone();
        tokio::spawn(async move {
            let (try_claim, release, _released) = mgr.reap_claim_fns();
            mgr.registry
                .acquire(&AgentId::from("spawning"), try_claim, release)
                .await;
        })
    };
    entered_rx.recv().await.expect("kill entered");

    // Mid-kill: the claim must make this turn LOSE, exactly like busy.
    assert!(
        !mgr.try_begin(&victim, &ws).await,
        "try_begin must lose against a held eviction claim (the TOCTOU)"
    );

    release_tx.send(()).unwrap();
    acquire.await.unwrap();
    assert!(
        !mgr.registry.is_registered(&victim),
        "the victim was evicted"
    );
    assert!(
        mgr.try_begin(&victim, &ws).await,
        "claim is released after the kill; the next turn starts normally"
    );
    assert!(log.lock().unwrap().is_empty(), "no filler was killed");
}

/// monorepo#2247 — an eviction candidate whose id is ALREADY in `reap_claims`
/// (a sweep holds it mid-kill) is skipped for the next-LRU idle candidate,
/// and the holder's claim is left untouched.
#[tokio::test]
async fn acquire_evict_skips_candidate_claimed_by_a_sweep() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (held, next) = (AgentId::from("held"), AgentId::from("next"));
    let log = Arc::new(Mutex::new(Vec::new()));
    mgr.registry
        .register(held.clone(), recording_kill(held.clone(), log.clone()));
    mgr.registry.set_last_active(&held, 1);
    mgr.registry
        .register(next.clone(), recording_kill(next.clone(), log.clone()));
    mgr.registry.set_last_active(&next, 2);
    fill_active(&mgr, 6, &log);

    // Another sweep holds the LRU candidate's claim (mid-kill).
    mgr.reap_claims.lock().unwrap().insert(held.clone());

    let (try_claim, release, _released) = mgr.reap_claim_fns();
    timeout(
        Duration::from_secs(2),
        mgr.registry
            .acquire(&AgentId::from("spawning"), try_claim, release),
    )
    .await
    .expect("acquire proceeds via the next-LRU candidate");

    assert_eq!(*log.lock().unwrap(), vec![next.clone()], "next-LRU killed");
    assert!(mgr.registry.is_registered(&held), "held candidate kept");
    assert!(
        mgr.reap_claims.lock().unwrap().contains(&held),
        "the holder's claim is untouched (a lost try_claim never releases)"
    );
}

/// monorepo#2247 — aborting the acquire future mid-kill (the abortable
/// worker case) must release the claim via the drop guard, or the victim's
/// queue is wedged until daemon restart.
#[tokio::test]
async fn acquire_abort_at_kill_releases_claim() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let ws = WorkspaceId::from("ws-abort-claim");
    let victim = AgentId::from("a-abort-claim");
    seed_agent(&mgr, &ws, &victim).await;

    let (kill, mut entered_rx, _release_tx) = blocking_kill();
    mgr.registry.register(victim.clone(), kill);
    mgr.registry.set_last_active(&victim, 1);
    let log = Arc::new(Mutex::new(Vec::new()));
    fill_active(&mgr, 7, &log);

    let acquire = {
        let mgr = mgr.clone();
        tokio::spawn(async move {
            let (try_claim, release, _released) = mgr.reap_claim_fns();
            mgr.registry
                .acquire(&AgentId::from("spawning"), try_claim, release)
                .await;
        })
    };
    entered_rx.recv().await.expect("kill entered");
    assert!(mgr.reap_claims.lock().unwrap().contains(&victim));

    acquire.abort();
    let _ = acquire.await;
    assert!(
        !mgr.reap_claims.lock().unwrap().contains(&victim),
        "claim released by the drop guard on abort"
    );
    assert!(
        mgr.try_begin(&victim, &ws).await,
        "try_begin wins after abort"
    );
}

/// monorepo#2247 — when EVERY candidate is claimed by a concurrent sweep,
/// `acquire` queues as a (timed) waiter instead of spinning on the same
/// unclaimable snapshot, and proceeds once the holder releases.
#[tokio::test]
async fn acquire_waits_when_every_candidate_is_claimed() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let held = AgentId::from("held");
    let log = Arc::new(Mutex::new(Vec::new()));
    mgr.registry
        .register(held.clone(), recording_kill(held.clone(), log.clone()));
    mgr.registry.set_last_active(&held, 1);
    fill_active(&mgr, 7, &log);

    mgr.reap_claims.lock().unwrap().insert(held.clone());

    let acquire = {
        let mgr = mgr.clone();
        tokio::spawn(async move {
            let (try_claim, release, _released) = mgr.reap_claim_fns();
            mgr.registry
                .acquire(&AgentId::from("spawning"), try_claim, release)
                .await;
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !acquire.is_finished(),
        "acquire waits while the only candidate is claimed"
    );
    assert!(mgr.registry.is_registered(&held), "no kill under a claim");

    // Holder releases (no deregister fires): the waiter's timed re-check
    // claims and evicts normally.
    mgr.reap_claims.lock().unwrap().remove(&held);
    timeout(Duration::from_secs(30), acquire)
        .await
        .expect("the waiter re-checks on its own timer")
        .expect("task ok");
    assert!(!mgr.registry.is_registered(&held));
}

/// monorepo#2247 — the spawning agent's OWN stale entry is evicted without a
/// claim: the busy slot held by the worker driving the spawn already makes
/// `try_begin` lose for it, and a claim attempt against that busy slot would
/// deadlock the spawn forever.
#[tokio::test]
async fn acquire_evicts_own_stale_entry_without_claim() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let ws = WorkspaceId::from("ws-own-entry");
    let id = AgentId::from("a-own-entry");
    seed_agent(&mgr, &ws, &id).await;

    let log = Arc::new(Mutex::new(Vec::new()));
    mgr.registry
        .register(id.clone(), recording_kill(id.clone(), log.clone()));
    mgr.registry.set_last_active(&id, 1);
    fill_active(&mgr, 7, &log);

    // The agent holds its own busy slot (the worker driving this spawn).
    assert!(mgr.try_begin(&id, &ws).await);

    let (try_claim, release, _released) = mgr.reap_claim_fns();
    timeout(
        Duration::from_secs(2),
        mgr.registry.acquire(&id, try_claim, release),
    )
    .await
    .expect("own stale entry evicts without deadlocking against the own busy slot");
    assert_eq!(*log.lock().unwrap(), vec![id.clone()], "own entry killed");
    assert!(!mgr.registry.is_registered(&id));
}

/// monorepo#2247 — after a claim wins, the candidate is re-validated under
/// the registry lock before its kill: one that went active in the window is
/// released and skipped, and the next-LRU candidate is evicted instead.
#[tokio::test]
async fn acquire_revalidates_candidate_after_claim() {
    let reg = Arc::new(ProcessRegistry::new(2));
    let log = Arc::new(Mutex::new(Vec::new()));
    let (a, b) = (AgentId::from("a"), AgentId::from("b"));
    reg.register(a.clone(), recording_kill(a.clone(), log.clone()));
    reg.set_last_active(&a, 1);
    reg.register(b.clone(), recording_kill(b.clone(), log.clone()));
    reg.set_last_active(&b, 2);

    // A turn races the claim: the moment `a` is claimed it goes active, so
    // the re-validation must reject and release it.
    let released: Arc<Mutex<Vec<AgentId>>> = Arc::new(Mutex::new(Vec::new()));
    let try_claim = {
        let reg = reg.clone();
        let a = a.clone();
        move |id: &AgentId| {
            if *id == a {
                reg.mark_active(&a);
            }
            true
        }
    };
    let release = {
        let released = released.clone();
        move |id: &AgentId| released.lock().unwrap().push(id.clone())
    };
    reg.acquire(&AgentId::from("c"), try_claim, release).await;

    assert_eq!(*log.lock().unwrap(), vec![b.clone()], "only `b` is killed");
    assert!(reg.is_registered(&a), "`a` re-validated active and kept");
    assert!(
        released.lock().unwrap().contains(&a),
        "`a`'s claim was released on the re-validation reject"
    );
}

/// monorepo#2247 (PR review) — the admission paths run inside ABORTABLE
/// worker futures: when the future is dropped at the `kill().await`, the
/// `ClaimGuard` releases the claim, and that release itself must kick the
/// drain — otherwise a message that parked behind the claim (`try_begin`
/// lost `ReapClaimed` mid-kill) strands until an unrelated queue event.
#[tokio::test]
async fn aborted_acquire_release_still_kicks_drain_for_parked_message() {
    let script = mock_agent_script();
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let mgr = Arc::new(AgentManager::new(services.clone(), sink, 8));
    services.attach_agent_manager(&mgr);

    let ws = WorkspaceId::from("ws-abort-kick");
    let victim = AgentId::from("a-abort-kick");
    seed_agent(&mgr, &ws, &victim).await;
    let mut session = mgr.services.store.get_agent_session(&victim).await.unwrap();
    session.provider = Some("mock".to_string());
    mgr.services
        .store
        .update_agent_session(&ws, &session)
        .await
        .expect("set mock provider");

    // Keep `release_tx` alive: dropping it would complete the blocked kill
    // and defeat the mid-kill abort below.
    let (kill, mut entered_rx, _release_tx) = blocking_kill();
    mgr.registry.register(victim.clone(), kill);
    mgr.registry.set_last_active(&victim, 1);
    let log = Arc::new(Mutex::new(Vec::new()));
    fill_active(&mgr, 7, &log);

    let acquire = {
        let mgr = mgr.clone();
        tokio::spawn(async move {
            let (try_claim, release) = mgr.admission_claim_fns();
            mgr.registry
                .acquire(&AgentId::from("spawning"), try_claim, release)
                .await;
        })
    };
    entered_rx.recv().await.expect("kill entered");

    // Mid-kill, a send parks behind the claim (its inline drain kick loses
    // `try_begin` against the held claim).
    mgr.services
        .agent_queue_message_op(victim.clone(), "parked behind claim".into(), None, None)
        .await
        .expect("queue message");
    assert_eq!(services.queue_snapshot(&victim).len(), 1, "message parked");

    // Abort the admission future at the `kill().await`: the `ClaimGuard`
    // releases the claim, and the release's own drain kick delivers the
    // parked message — no tail call after the await ever runs here.
    acquire.abort();
    let _ = acquire.await;
    timeout(Duration::from_secs(30), async {
        loop {
            if services.queue_snapshot(&victim).is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the aborted admission's claim release kicks the parked drain");
}

/// monorepo#2247 — the turn-start budget gate's eviction claims its victim
/// against `try_begin` on the same terms as `acquire`'s evict arm.
#[tokio::test]
async fn turn_start_evict_claim_blocks_try_begin_during_kill_window() {
    let gb = super::GB;
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let ws = WorkspaceId::from("ws-turn-start-claim");
    let victim = AgentId::from("a-turn-start-claim");
    seed_agent(&mgr, &ws, &victim).await;

    let probe = FakeProbe::new(10 * gb);
    assert!(mgr.registry.set_memory_budget(4 * gb, probe.clone()));

    let (kill, mut entered_rx, release_tx) = blocking_kill();
    mgr.registry.register(victim.clone(), kill);
    mgr.registry.set_last_active(&victim, 1);
    // The gated warm agent (never its own gate's victim).
    let warm = AgentId::from("warm");
    let log = Arc::new(Mutex::new(Vec::new()));
    mgr.registry
        .register(warm.clone(), recording_kill(warm.clone(), log.clone()));
    mgr.registry.set_last_active(&warm, 1000);

    let gate = {
        let mgr = mgr.clone();
        let warm = warm.clone();
        tokio::spawn(async move {
            let (try_claim, release, _released) = mgr.reap_claim_fns();
            mgr.registry
                .acquire_turn_start(&warm, try_claim, release)
                .await;
        })
    };
    entered_rx.recv().await.expect("kill entered");

    assert!(
        !mgr.try_begin(&victim, &ws).await,
        "try_begin must lose against the turn-start gate's held claim"
    );

    release_tx.send(()).unwrap();
    // Under budget again → the gate admits.
    probe.set(gb);
    timeout(Duration::from_secs(30), gate)
        .await
        .expect("gate admits once the tree drains")
        .expect("task ok");
    assert!(!mgr.registry.is_registered(&victim), "victim evicted");
    assert!(
        mgr.try_begin(&victim, &ws).await,
        "claim is released after the kill"
    );
    assert!(log.lock().unwrap().is_empty(), "the warm agent survives");
}

/// monorepo#2247 (PR review) — `acquire_turn_start` duplicates `acquire`'s
/// candidate-walk machinery, so its skip-claimed behavior is pinned on its
/// own copy: a candidate held by another sweep is skipped for the next-LRU
/// one, and the holder's claim is left untouched.
#[tokio::test]
async fn turn_start_evict_skips_candidate_claimed_by_a_sweep() {
    let gb = super::GB;
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);

    let probe = FakeProbe::new(10 * gb);
    assert!(mgr.registry.set_memory_budget(4 * gb, probe.clone()));

    let (held, next) = (AgentId::from("held"), AgentId::from("next"));
    let log = Arc::new(Mutex::new(Vec::new()));
    mgr.registry
        .register(held.clone(), recording_kill(held.clone(), log.clone()));
    mgr.registry.set_last_active(&held, 1);
    mgr.registry
        .register(next.clone(), recording_kill(next.clone(), log.clone()));
    mgr.registry.set_last_active(&next, 2);
    let warm = AgentId::from("warm");
    mgr.registry
        .register(warm.clone(), recording_kill(warm.clone(), log.clone()));
    mgr.registry.set_last_active(&warm, 1000);

    // Another sweep holds the LRU candidate's claim (mid-kill).
    mgr.reap_claims.lock().unwrap().insert(held.clone());

    let (try_claim, release, _released) = mgr.reap_claim_fns();
    let gate = mgr.registry.acquire_turn_start(&warm, try_claim, release);
    tokio::pin!(gate);
    // The eviction of `next` drains the tree below budget mid-walk.
    let advance = async {
        loop {
            if log.lock().unwrap().contains(&next) {
                probe.set(gb);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    timeout(Duration::from_secs(30), async {
        tokio::join!(&mut gate, advance)
    })
    .await
    .expect("gate proceeds via the next-LRU candidate");

    assert_eq!(*log.lock().unwrap(), vec![next.clone()], "next-LRU killed");
    assert!(mgr.registry.is_registered(&held), "held candidate kept");
    assert!(
        mgr.reap_claims.lock().unwrap().contains(&held),
        "the holder's claim is untouched (a lost try_claim never releases)"
    );
}

/// monorepo#2247 (PR review) — `acquire_turn_start`'s copy of the
/// all-candidates-claimed fallback: the gate queues as a (timed) waiter
/// instead of spinning, and proceeds once the holder releases.
#[tokio::test]
async fn turn_start_waits_when_every_candidate_is_claimed() {
    let gb = super::GB;
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);

    let probe = FakeProbe::new(10 * gb);
    assert!(mgr.registry.set_memory_budget(4 * gb, probe.clone()));

    let held = AgentId::from("held");
    let log = Arc::new(Mutex::new(Vec::new()));
    mgr.registry
        .register(held.clone(), recording_kill(held.clone(), log.clone()));
    mgr.registry.set_last_active(&held, 1);
    let warm = AgentId::from("warm");
    mgr.registry
        .register(warm.clone(), recording_kill(warm.clone(), log.clone()));
    mgr.registry.set_last_active(&warm, 1000);

    mgr.reap_claims.lock().unwrap().insert(held.clone());

    let gate = {
        let mgr = mgr.clone();
        let warm = warm.clone();
        tokio::spawn(async move {
            let (try_claim, release, _released) = mgr.reap_claim_fns();
            mgr.registry
                .acquire_turn_start(&warm, try_claim, release)
                .await;
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !gate.is_finished(),
        "the gate waits while the only candidate is claimed"
    );
    assert!(mgr.registry.is_registered(&held), "no kill under a claim");

    // Holder releases and the tree drains (no deregister fires): the
    // waiter's timed re-check admits.
    mgr.reap_claims.lock().unwrap().remove(&held);
    probe.set(gb);
    timeout(Duration::from_secs(30), gate)
        .await
        .expect("the waiter re-checks on its own timer")
        .expect("task ok");
    assert!(mgr.registry.is_registered(&held), "under budget: no evict");
    assert!(log.lock().unwrap().is_empty(), "nothing was killed");
}

/// monorepo#2247 — the count-based LRU reap (`reap_idle` / `evict_idle`)
/// claims each candidate before its kill: mid-kill a `try_begin` must LOSE,
/// and a candidate claimed by another sweep is skipped, not killed.
#[tokio::test]
async fn reap_idle_claim_blocks_try_begin_during_kill_window() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let ws = WorkspaceId::from("ws-reap-max-claim");
    let id = AgentId::from("a-reap-max-claim");
    seed_agent(&mgr, &ws, &id).await;

    let (kill, mut entered_rx, release_tx) = blocking_kill();
    mgr.registry.register(id.clone(), kill);

    let sweep = {
        let mgr = mgr.clone();
        tokio::spawn(async move { mgr.reap_idle(None).await })
    };
    entered_rx.recv().await.expect("kill entered");

    assert!(
        !mgr.try_begin(&id, &ws).await,
        "try_begin must lose against the count-based reap's held claim"
    );

    release_tx.send(()).unwrap();
    assert_eq!(sweep.await.unwrap(), 1);
    assert!(!mgr.registry.is_registered(&id), "candidate was reaped");
    assert!(
        mgr.try_begin(&id, &ws).await,
        "claim is released after the kill; the next turn starts normally"
    );
}

/// monorepo#2247 — `reap_idle` honors a claim another sweep already holds:
/// the candidate is skipped (not killed), the holder's claim untouched.
#[tokio::test]
async fn reap_idle_skips_candidate_claimed_by_a_sweep() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let id = AgentId::from("a-reap-max-held");
    let log = Arc::new(Mutex::new(Vec::new()));
    mgr.registry
        .register(id.clone(), recording_kill(id.clone(), log.clone()));

    mgr.reap_claims.lock().unwrap().insert(id.clone());
    assert_eq!(mgr.reap_idle(None).await, 0);
    assert!(log.lock().unwrap().is_empty(), "no kill under a held claim");
    assert!(mgr.registry.is_registered(&id), "candidate kept");
    assert!(
        mgr.reap_claims.lock().unwrap().contains(&id),
        "the holder's claim is untouched"
    );

    mgr.reap_claims.lock().unwrap().remove(&id);
    assert_eq!(mgr.reap_idle(None).await, 1);
    assert!(!mgr.registry.is_registered(&id));
}

/// monorepo#2063 B7 — the manager's over-budget reap drains idle agents with
/// fresh timestamps (no TTL involved), skips agents with an in-flight prompt
/// via the same claim wiring as the TTL sweep, and honors a claim another
/// sweep already holds.
#[tokio::test]
async fn reap_over_budget_drains_idle_and_respects_claims() {
    let gb = super::GB;
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let ws = WorkspaceId::from("ws-budget-reap");
    let (busy, idle) = (AgentId::from("busy"), AgentId::from("idle"));
    seed_agent(&mgr, &ws, &busy).await;
    track(&mgr, &busy);
    track(&mgr, &idle);
    // Both freshly active — the TTL sweep would touch neither.
    assert!(mgr.try_begin(&busy, &ws).await, "busy claims its turn");

    let probe = FakeProbe::new(10 * gb);
    probe.set_agents(&[(&busy, 7 * gb), (&idle, 2 * gb)]);
    assert!(mgr.registry().set_memory_budget(4 * gb, probe.clone()));

    // Still over budget after the eviction: the drain must stop anyway once
    // only the busy agent (claim-protected) remains.
    assert_eq!(Arc::clone(&mgr).reap_over_budget().await, 1);
    assert!(!mgr.contains(&idle), "idle agent drained without a TTL");
    assert!(mgr.contains(&busy), "in-flight agent survives the drain");

    // A claim held by another sweep protects its agent too.
    track(&mgr, &idle);
    mgr.end_turn(&busy).await;
    mgr.registry().mark_idle(&busy);
    mgr.reap_claims.lock().unwrap().insert(busy.clone());
    probe.set(10 * gb);
    let evicted = {
        let probe = probe.clone();
        let mgr = Arc::clone(&mgr);
        tokio::spawn(async move {
            let n = mgr.reap_over_budget().await;
            probe.set(gb);
            n
        })
        .await
        .unwrap()
    };
    assert_eq!(evicted, 1, "only the unclaimed idle agent is drained");
    assert!(mgr.contains(&busy), "held claim protects the agent");
    assert!(
        mgr.reap_claims.lock().unwrap().contains(&busy),
        "the holder's claim is untouched"
    );
}

/// monorepo#2063 B7 — under budget the manager's over-budget reap is a no-op:
/// no agent is touched however stale its timestamp.
#[tokio::test]
async fn reap_over_budget_is_inert_under_budget() {
    let gb = super::GB;
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let id = AgentId::from("a-under-budget");
    track(&mgr, &id);
    mgr.registry().set_last_active(&id, 1);
    assert!(mgr.registry().set_memory_budget(4 * gb, FakeProbe::new(gb)));

    assert_eq!(Arc::clone(&mgr).reap_over_budget().await, 0);
    assert!(mgr.contains(&id), "no pressure → nothing drained");
}

/// Track a mock agent whose handle owns a REAL child process (a long sleep),
/// the way `create_agent` installs one, and arm the child-exit watcher for it.
/// Returns the child's pid and the watcher task.
#[cfg(unix)]
fn track_with_child(mgr: &AgentManager, id: &AgentId) -> (u32, tokio::task::JoinHandle<bool>) {
    let mut cmd = tokio::process::Command::new("sh");
    cmd.args(["-c", "sleep 300"]);
    cmd.kill_on_drop(true);
    cmd.process_group(0);
    let child = cmd.spawn().expect("spawn sleeper child");
    let pid = child.id().expect("live child has a pid");
    let mut handle = mock_handle();
    handle.child = Some(child);
    handle.child_pid = Some(pid);
    mgr.handles.lock().unwrap().insert(id.clone(), handle);
    mgr.registry.register(id.clone(), mgr.make_kill(id.clone()));
    let watcher = mgr.arm_child_exit_watcher(id.clone(), Some(pid));
    (pid, watcher)
}

/// Proactive dead-child detection (monorepo#764): an idle agent's child that
/// is killed EXTERNALLY (SIGKILL) is reaped by the watcher within a bounded
/// delay — handle removed, registry deregistered — and the watcher reports it
/// fired the unexpected-exit cleanup.
#[cfg(unix)]
#[tokio::test]
async fn child_exit_watcher_reaps_idle_agent_on_external_kill() {
    use nix::sys::signal::{kill, Signal};
    use nix::unistd::Pid;

    let (_tmp, mgr) = manager().await;
    let id = AgentId::from("a-watched");
    let (pid, watcher) = track_with_child(&mgr, &id);
    assert!(mgr.contains(&id));
    assert!(mgr.registry().is_registered(&id));

    kill(Pid::from_raw(pid.cast_signed()), Signal::SIGKILL).expect("external SIGKILL");

    let fired = timeout(Duration::from_secs(5), watcher)
        .await
        .expect("watcher fires within a bounded delay")
        .expect("watcher task completes");
    assert!(fired, "watcher fired the unexpected-exit cleanup");
    assert!(!mgr.contains(&id), "handle removed");
    assert!(!mgr.registry().is_registered(&id), "registry deregistered");
}

/// Deliberate teardown must NOT fire the watcher: `stop()` removes the handle
/// before killing the child, so the watcher observes the missing handle and
/// stands down (returns `false`, no "unexpected exit" cleanup/log).
#[cfg(unix)]
#[tokio::test]
async fn child_exit_watcher_stands_down_on_deliberate_stop() {
    let (_tmp, mgr) = manager().await;
    let id = AgentId::from("a-stopped");
    let (_pid, watcher) = track_with_child(&mgr, &id);

    assert!(mgr.stop(&id).await, "stop removes the tracked handle");

    let fired = timeout(Duration::from_secs(5), watcher)
        .await
        .expect("watcher stands down within a bounded delay")
        .expect("watcher task completes");
    assert!(!fired, "deliberate stop must not fire the watcher");
}

/// `kill_child_only` (the retry/respawn teardown) likewise removes the handle
/// before the kill, so the watcher stands down without firing.
#[cfg(unix)]
#[tokio::test]
async fn child_exit_watcher_stands_down_on_kill_child_only() {
    let (_tmp, mgr) = manager().await;
    let id = AgentId::from("a-killed-deliberately");
    let (_pid, watcher) = track_with_child(&mgr, &id);

    mgr.kill_child_only(&id).await;

    let fired = timeout(Duration::from_secs(5), watcher)
        .await
        .expect("watcher stands down within a bounded delay")
        .expect("watcher task completes");
    assert!(
        !fired,
        "deliberate kill_child_only must not fire the watcher"
    );
}

/// Process-tree teardown (§5.6): a provider's whole process group is signalled,
/// so a grandchild spawned by the direct child is terminated too — `kill_on_drop`
/// alone would leave it orphaned.
#[cfg(unix)]
#[tokio::test]
async fn kill_child_tree_terminates_grandchild() {
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::process::Command;

    // The shell becomes the group leader (`process_group(0)`), backgrounds a
    // grandchild `sleep`, prints its pid, then sleeps so the group stays alive.
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg("sleep 300 & echo $!; sleep 300");
    cmd.stdout(Stdio::piped());
    cmd.kill_on_drop(true);
    cmd.process_group(0);
    let mut child = cmd.spawn().expect("spawn sleep tree");

    let stdout = child.stdout.take().expect("piped stdout");
    let mut lines = BufReader::new(stdout).lines();
    let line = timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("grandchild pid line in time")
        .expect("read ok")
        .expect("a pid line");
    let grandchild: u32 = line.trim().parse().expect("grandchild pid");
    assert!(pid_alive(grandchild), "grandchild alive before teardown");

    super::kill_child_tree(child, None).await;

    let mut dead = false;
    for _ in 0..100 {
        if !pid_alive(grandchild) {
            dead = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(dead, "grandchild terminated with the process group");
}

/// Regression (monorepo#764): after a `try_wait` liveness probe reaps the
/// direct child (`Child::id()` reads `None`), the pgid teardown must still
/// sweep same-group descendants via the spawn-time pid — this is the
/// child-exit-watcher path, where the leader is reaped before the kill.
#[cfg(unix)]
#[tokio::test]
async fn kill_child_tree_sweeps_group_after_leader_reaped() {
    use nix::sys::signal::{kill, Signal};
    use nix::unistd::Pid;
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::process::Command;

    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg("sleep 300 & echo $!; sleep 300");
    cmd.stdout(Stdio::piped());
    cmd.kill_on_drop(true);
    cmd.process_group(0);
    let mut child = cmd.spawn().expect("spawn sleep tree");
    let spawn_pid = child.id().expect("live child has a pid");

    let stdout = child.stdout.take().expect("piped stdout");
    let mut lines = BufReader::new(stdout).lines();
    let line = timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("grandchild pid line in time")
        .expect("read ok")
        .expect("a pid line");
    let grandchild: u32 = line.trim().parse().expect("grandchild pid");
    assert!(pid_alive(grandchild), "grandchild alive before teardown");

    // Kill ONLY the leader, then reap it via `try_wait` — same-group
    // grandchild survives and `Child::id()` reads `None` afterwards.
    kill(Pid::from_raw(spawn_pid.cast_signed()), Signal::SIGKILL).expect("kill leader");
    let mut reaped = false;
    for _ in 0..100 {
        if child.try_wait().expect("try_wait ok").is_some() {
            reaped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(reaped, "leader reaped by try_wait");
    assert!(child.id().is_none(), "id() cleared after reap");
    assert!(pid_alive(grandchild), "grandchild outlives the leader");

    super::kill_child_tree(child, Some(spawn_pid)).await;

    let mut dead = false;
    for _ in 0..100 {
        if !pid_alive(grandchild) {
            dead = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(dead, "grandchild swept via the spawn-time pid");
}

/// Timing proof for the batch stop path (the `workspace.delete` sweep): N
/// tracked agents whose children ignore SIGTERM tear down in ~ONE shared
/// grace window via `stop_many` — mirroring `kill_sweep_tests` — with the
/// per-agent `stop()` semantics (handle removal, deregistration) applied to
/// every agent.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn stop_many_tears_down_slow_children_in_one_shared_grace_window() {
    const N: usize = 4;
    let grace = super::PROCESS_GROUP_TERM_GRACE;
    let (_tmp, mgr) = manager().await;
    let mut ids = Vec::with_capacity(N);
    let mut pids = Vec::with_capacity(N);
    for i in 0..N {
        let id = AgentId::from(format!("a-batch-{i}").as_str());
        let mut cmd = tokio::process::Command::new("sh");
        // Ignore SIGTERM so each child only dies on the SIGKILL sweep,
        // forcing the full grace window to elapse.
        cmd.args(["-c", "trap '' TERM; sleep 30"]);
        cmd.process_group(0);
        cmd.kill_on_drop(true);
        let child = cmd.spawn().expect("spawn slow child");
        let pid = child.id().expect("live child has a pid");
        let mut handle = mock_handle();
        handle.child = Some(child);
        handle.child_pid = Some(pid);
        mgr.handles.lock().unwrap().insert(id.clone(), handle);
        mgr.registry.register(id.clone(), mgr.make_kill(id.clone()));
        ids.push(id);
        pids.push(pid);
    }
    // Let each sh install its trap before SIGTERM arrives.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let start = std::time::Instant::now();
    let fence = mgr.stop_many(&ids).await;
    let elapsed = start.elapsed();

    // Serial teardown would take ~N * grace (8s for 4 children); the shared
    // window must finish in ~one grace period (<4s total).
    assert!(
        elapsed < grace * 2,
        "batch stop took {elapsed:?}, expected ~one {grace:?} grace window"
    );
    // The children ignored SIGTERM, so the full shared grace must have
    // elapsed (proves the window ran once, not that children died early).
    assert!(
        elapsed >= grace.checked_sub(Duration::from_millis(500)).unwrap(),
        "batch stop returned after {elapsed:?}, before the shared grace window elapsed"
    );
    // Per-agent `stop()` semantics applied to every agent in the batch.
    assert!(mgr.is_empty(), "all handles removed");
    assert_eq!(mgr.registry().size(), 0, "all agents deregistered");
    // The kill itself, not just the elapsed window: every child must be
    // dead. Short retry loop — a SIGKILLed child stays signal-0-visible as
    // a zombie until its wait task reaps it (stragglers past
    // KILL_SWEEP_REAP_GRACE reap in the background).
    for &pid in &pids {
        let mut dead = !pid_alive(pid);
        for _ in 0..100 {
            if dead {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            dead = !pid_alive(pid);
        }
        assert!(dead, "child {pid} killed by the batch sweep");
    }
    // The returned fence keeps every swept agent blocked from the
    // lazy-spawn paths until dropped, then re-opens them.
    for id in &ids {
        assert!(
            mgr.stopping.lock().unwrap().contains(id),
            "{id} fenced while the TeardownFence is held"
        );
    }
    drop(fence);
    assert!(
        mgr.stopping.lock().unwrap().is_empty(),
        "fence drop clears the stopping set"
    );
}

/// Ghost-agent race regression (PR #1038 review): while a `stop_many`
/// teardown fence is held, the lazy-spawn path (`ensure_started`) must
/// refuse to spawn a replacement child for a swept agent — otherwise a
/// concurrent `agent.sendMessage` racing `workspace.delete`'s shared grace
/// wait could leave a live process whose session row the cascade then
/// deletes. Once the fence drops, the spawn path is open again.
#[tokio::test]
async fn stop_many_fence_blocks_lazy_respawn_until_dropped() {
    let (_tmp, mgr) = manager().await;
    let ws = WorkspaceId::from("ws-fence");
    let agent_id = AgentId::from("a-fence-respawn");
    mgr.services
        .store
        .insert_workspace(&super::role_reminder_tests::workspace(&ws))
        .await
        .expect("insert workspace");
    let session = super::role_reminder_tests::session(&agent_id, &ws, None);
    mgr.services
        .store
        .insert_agent_session(&session)
        .await
        .expect("insert session");

    let fence = mgr.stop_many(std::slice::from_ref(&agent_id)).await;
    // Session row still present (the delete cascade has not run), yet the
    // spawn is refused: the fence, not the store check, blocks it.
    let err = mgr
        .ensure_started(&agent_id, &ws)
        .await
        .expect_err("fenced agent must not respawn");
    assert!(
        matches!(err, Error::NotFound(_)),
        "fence surfaces NotFound (non-retryable for retry_spawn), got: {err:?}"
    );
    assert!(mgr.is_empty(), "no handle installed for the fenced agent");

    drop(fence);
    // Fence lifted, session row deleted (as the workspace.delete cascade
    // does): the spawn path proceeds past the teardown guard and now fails
    // on its own store read — proving the fence no longer fires.
    mgr.services
        .store
        .delete_agent_session(&ws, &agent_id)
        .await
        .expect("delete session");
    let err = mgr
        .ensure_started(&agent_id, &ws)
        .await
        .expect_err("session row gone");
    assert!(
        !err.to_string().contains("is being deleted"),
        "unfenced agent proceeds past the teardown guard, got: {err:?}"
    );
}

/// Signal-0 liveness probe used by the process-group teardown test.
#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    use nix::sys::signal::kill;
    use nix::unistd::Pid;
    matches!(
        kill(Pid::from_raw(pid.cast_signed()), None),
        Ok(()) | Err(nix::errno::Errno::EPERM)
    )
}

/// A self-cleaning temp git repo with one committed file modified in the workdir.
struct TempRepo {
    dir: PathBuf,
}

impl Drop for TempRepo {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Seed `a.txt`, commit it, then leave an unstaged modification (2 adds / 1 del).
fn seed_repo() -> TempRepo {
    use git2::{Repository, Signature};
    let dir = std::env::temp_dir().join(format!("intentd-ft-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let repo = Repository::init(&dir).unwrap();
    {
        let mut cfg = repo.config().unwrap();
        cfg.set_str("user.name", "Test").unwrap();
        cfg.set_str("user.email", "test@example.com").unwrap();
    }
    std::fs::write(dir.join("a.txt"), "line1\nline2\nline3\n").unwrap();
    {
        let mut index = repo.index().unwrap();
        index.add_path(std::path::Path::new("a.txt")).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = repo.find_tree(tree_oid).unwrap();
        let sig = Signature::now("Test", "test@example.com").unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "seed", &tree, &[])
            .unwrap();
    }
    std::fs::write(dir.join("a.txt"), "line1\nCHANGED\nline3\nline4\n").unwrap();
    TempRepo { dir }
}

/// An agent `file:changed` runs the BE-internal review pipeline (§17.1): the
/// sink records both the diff (§17.3) and the attribution row (§17.4) for the
/// edited file, with the agent's stats and lazy blob SHAs.
#[tokio::test]
async fn agent_file_change_records_tracked_change_and_diff() {
    use intent_acp::SinkEvent;
    use intent_core::{
        events::FILE_CHANGED, now_iso, ActorType, EventActor, Workspace, WorkspaceActivity,
        WorkspaceAttention, WorkspaceId, WorkspaceStatus,
    };

    let repo = seed_repo();
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let ws_id = WorkspaceId::from("ws-ft");
    let ts = now_iso();
    let ws = Workspace {
        id: ws_id.clone(),
        title: "WS".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts,
        last_activity: None,
        tags: vec![],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: Some(repo.dir.display().to_string()),
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    };
    store.insert_workspace(&ws).await.unwrap();

    let bus = EventBus::new(store.clone());
    let sink = BusEventSink::new(bus);
    sink.publish(SinkEvent {
        workspace_id: ws_id.clone(),
        event_type: FILE_CHANGED.to_string(),
        actor: EventActor {
            actor_type: ActorType::Agent,
            id: Some("agent-1".to_string()),
            name: Some("Agent".to_string()),
            ..Default::default()
        },
        session_id: Some("agent-1".to_string()),
        data: serde_json::json!({
            "path": "a.txt",
            "relativePath": "a.txt",
            "action": "modify",
        }),
    })
    .await;

    let changes = store.list_tracked_changes(&ws_id).await.unwrap();
    assert_eq!(changes.len(), 1, "one attribution row for the edited file");
    let c = &changes[0];
    assert_eq!(c.path, "a.txt");
    assert_eq!(c.stage, "unstaged");
    assert_eq!(c.status, "modified");
    assert_eq!(c.agent_id.as_deref(), Some("agent-1"));
    assert_eq!(c.session_id.as_deref(), Some("agent-1"));
    assert_eq!(c.additions, 2);
    assert_eq!(c.deletions, 1);
    assert!(
        c.new_blob_sha.is_some(),
        "content recoverable lazily via SHA"
    );

    let diffs = store.list_diffs(&ws_id).await.unwrap();
    assert_eq!(diffs.len(), 1, "one diff row for the edited file");
    assert_eq!(diffs[0].file_path, "a.txt");
    assert!(!diffs[0].staged);
    assert!(
        diffs[0].old_content.is_none(),
        "content stays lazy, not inlined"
    );
    assert!(
        diffs[0].hunks_json.contains("CHANGED"),
        "extracted hunks carry the new line"
    );
}

const MGR_ACP_SID: &str = "mgr-acp-new";

/// Shared capture of `(method, params)` for every request the manager sends to
/// a mock agent; opt-in per [`spawn_cfg_mock_agent`] so tests that need it can
/// assert on the exact request sequence.
type MockCallLog = Arc<Mutex<Vec<(String, Value)>>>;

/// The `availableModes` list a mock agent advertises in its `session/new` /
/// `session/load` response. Defaults to a set that includes `bypassPermissions`
/// so tests exercising the "`set_mode` was attempted" assertions keep working;
/// tests can substitute a bypass-free set (e.g. `default`+`ask`, matching
/// auggie today) to exercise the skip path.
#[derive(Clone)]
struct MockModes {
    current_mode_id: &'static str,
    available_modes: &'static [&'static str],
}

impl MockModes {
    const fn with_bypass() -> Self {
        Self {
            current_mode_id: "default",
            available_modes: &["default", "bypassPermissions"],
        }
    }

    const fn no_bypass() -> Self {
        // Matches auggie's real advertised set today: no bypass-equivalent, so
        // the manager must skip `session/set_mode` rather than trigger `-32602`.
        Self {
            current_mode_id: "default",
            available_modes: &["default", "ask"],
        }
    }

    fn to_json(&self) -> Value {
        let available: Vec<Value> = self
            .available_modes
            .iter()
            .map(|id| json!({ "id": id, "name": id }))
            .collect();
        json!({
            "currentModeId": self.current_mode_id,
            "availableModes": available,
        })
    }
}

/// Configurable mock agent: `initialize` advertises `loadSession` per `load_cap`;
/// `session/new` mints [`MGR_ACP_SID`] and advertises the caller-chosen
/// `availableModes` (so tests can flip between the bypass-advertised and
/// bypass-absent shapes); `session/load` echoes the same modes; everything else
/// (e.g. `authenticate`) resolves with `{}`. When `log` is `Some`, every request
/// method (and its params) is recorded so tests can assert what the manager sent
/// after handshake / session setup.
fn spawn_cfg_mock_agent_with_modes<R, W>(
    read: R,
    write: W,
    load_cap: bool,
    log: Option<MockCallLog>,
    modes: MockModes,
) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(read).lines();
        let mut write = write;
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(&line).expect("valid JSON");
            let (Some(id), Some(method)) =
                (value.get("id"), value.get("method").and_then(Value::as_str))
            else {
                continue;
            };
            if let Some(log) = &log {
                let params = value.get("params").cloned().unwrap_or(Value::Null);
                log.lock().unwrap().push((method.to_string(), params));
            }
            let result = match method {
                "initialize" => {
                    json!({ "protocolVersion": 1, "agentCapabilities": { "loadSession": load_cap } })
                }
                "session/new" => {
                    json!({ "sessionId": MGR_ACP_SID, "modes": modes.to_json() })
                }
                "session/load" => json!({ "modes": modes.to_json() }),
                // A valid stop reason: an invalid/empty prompt response would
                // now be classified as a terminal mid-turn failure (killing
                // the tracked handle), not a benign warn-and-continue.
                "session/prompt" => json!({ "stopReason": "end_turn" }),
                _ => json!({}),
            };
            let resp = json!({ "jsonrpc": "2.0", "id": id, "result": result });
            write
                .write_all(format!("{resp}\n").as_bytes())
                .await
                .unwrap();
            write.flush().await.unwrap();
        }
    })
}

/// Track a handle wired to a configurable mock agent (parity with `create_agent`
/// minus a real child), returning the agent task handle.
fn track_mock_agent(mgr: &AgentManager, id: &AgentId, load_cap: bool) -> JoinHandle<()> {
    track_mock_agent_inner(mgr, id, load_cap, None, MockModes::with_bypass()).0
}

/// Like [`track_mock_agent`] but also returns a shared log capturing every
/// request the manager sent to the mock (method + params), so tests can assert
/// e.g. that `session/set_mode bypassPermissions` was attempted after session
/// setup.
fn track_mock_agent_with_log(
    mgr: &AgentManager,
    id: &AgentId,
    load_cap: bool,
) -> (JoinHandle<()>, MockCallLog) {
    let log: MockCallLog = Arc::new(Mutex::new(Vec::new()));
    let (handle, ()) = track_mock_agent_inner(
        mgr,
        id,
        load_cap,
        Some(log.clone()),
        MockModes::with_bypass(),
    );
    (handle, log)
}

/// Like [`track_mock_agent_with_log`] but with a caller-chosen advertised-modes
/// set (e.g. `MockModes::no_bypass()` to exercise the "provider offers no
/// bypass-equivalent" skip path).
fn track_mock_agent_with_log_modes(
    mgr: &AgentManager,
    id: &AgentId,
    load_cap: bool,
    modes: MockModes,
) -> (JoinHandle<()>, MockCallLog) {
    let log: MockCallLog = Arc::new(Mutex::new(Vec::new()));
    let (handle, ()) = track_mock_agent_inner(mgr, id, load_cap, Some(log.clone()), modes);
    (handle, log)
}

fn track_mock_agent_inner(
    mgr: &AgentManager,
    id: &AgentId,
    load_cap: bool,
    log: Option<MockCallLog>,
    modes: MockModes,
) -> (JoinHandle<()>, ()) {
    let (c2a_client, c2a_agent) = tokio::io::duplex(16 * 1024);
    let (a2c_agent, a2c_client) = tokio::io::duplex(16 * 1024);
    let agent = spawn_cfg_mock_agent_with_modes(c2a_agent, a2c_agent, load_cap, log, modes);
    let (note_tx, note_rx) = mpsc::unbounded_channel::<IncomingNotification>();
    let connection = Arc::new(Connection::new(
        c2a_client,
        a2c_client,
        None,
        ConnectionHooks {
            notifications: Some(note_tx),
            ..ConnectionHooks::default()
        },
    ));
    mgr.handles.lock().unwrap().insert(
        id.clone(),
        AgentHandle {
            connection,
            notifications: Arc::new(TokioMutex::new(note_rx)),
            serve_task: tokio::spawn(async {}),
            child: None,
            child_pid: None,
            _mcp_bridge: None,
            _mcp_config: None,
            _rules_config: None,
            _pi_extension: None,
            session_mcp_servers: Vec::new(),
            spawned_model: None,
            spawned_provider: "auggie".to_string(),
            thought_level: None,
            wake_gate: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_listener: None,
        },
    );
    mgr.registry.register(id.clone(), mgr.make_kill(id.clone()));
    (agent, ())
}

/// Mock agent that streams one `agent_message_chunk`, then answers
/// `session/prompt` with a JSON-RPC ERROR carrying `error_message` (a
/// transient-shaped `-32603`), while answering the lifecycle methods normally.
/// Drives the suspend-enrollment turn worker path: a suspend-overlapping
/// transient disconnect that `run_prompt_turn` enrolls for wake-resume.
fn spawn_mock_agent_erroring_on_prompt<R, W>(
    read: R,
    write: W,
    error_message: String,
) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(read).lines();
        let mut write = write;
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(&line).expect("valid JSON");
            let (Some(id), Some(method)) =
                (value.get("id"), value.get("method").and_then(Value::as_str))
            else {
                continue;
            };
            if method == "session/prompt" {
                // A pre-failure warning chunk, then the transient error.
                let note = json!({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": "existing-id",
                        "update": { "sessionUpdate": "agent_message_chunk",
                            "content": { "type": "text", "text": "partial " } }
                    }
                });
                write
                    .write_all(format!("{note}\n").as_bytes())
                    .await
                    .unwrap();
                let resp = json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": { "code": -32603, "message": error_message },
                });
                write
                    .write_all(format!("{resp}\n").as_bytes())
                    .await
                    .unwrap();
                write.flush().await.unwrap();
                continue;
            }
            let result = match method {
                "initialize" => {
                    json!({ "protocolVersion": 1, "agentCapabilities": { "loadSession": true } })
                }
                "session/new" => json!({ "sessionId": MGR_ACP_SID }),
                _ => json!({}),
            };
            let resp = json!({ "jsonrpc": "2.0", "id": id, "result": result });
            write
                .write_all(format!("{resp}\n").as_bytes())
                .await
                .unwrap();
            write.flush().await.unwrap();
        }
    })
}

/// Track a live handle wired to [`spawn_mock_agent_erroring_on_prompt`]: the
/// child is alive and reusable, but its `session/prompt` fails transiently.
fn track_mock_agent_prompt_rpc_error(
    mgr: &AgentManager,
    id: &AgentId,
    error_message: &str,
) -> JoinHandle<()> {
    let (c2a_client, c2a_agent) = tokio::io::duplex(16 * 1024);
    let (a2c_agent, a2c_client) = tokio::io::duplex(16 * 1024);
    let agent =
        spawn_mock_agent_erroring_on_prompt(c2a_agent, a2c_agent, error_message.to_string());
    let (note_tx, note_rx) = mpsc::unbounded_channel::<IncomingNotification>();
    let connection = Arc::new(Connection::new(
        c2a_client,
        a2c_client,
        None,
        ConnectionHooks {
            notifications: Some(note_tx),
            ..ConnectionHooks::default()
        },
    ));
    mgr.handles.lock().unwrap().insert(
        id.clone(),
        AgentHandle {
            connection,
            notifications: Arc::new(TokioMutex::new(note_rx)),
            serve_task: tokio::spawn(async {}),
            child: None,
            child_pid: None,
            _mcp_bridge: None,
            _mcp_config: None,
            _rules_config: None,
            _pi_extension: None,
            session_mcp_servers: Vec::new(),
            spawned_model: None,
            spawned_provider: "auggie".to_string(),
            thought_level: None,
            wake_gate: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_listener: None,
        },
    );
    mgr.registry.register(id.clone(), mgr.make_kill(id.clone()));
    agent
}

/// Injectable overlap query that reports a fixed suspend overlap for ANY window,
/// so a transient turn failure is always classified as suspend-induced.
struct AlwaysSuspended(Duration);

impl crate::agent_session::SuspendOverlapQuery for AlwaysSuspended {
    fn did_suspend_overlap(
        &self,
        _start: std::time::Instant,
        _end: std::time::Instant,
    ) -> Option<Duration> {
        Some(self.0)
    }
}

/// Finding 1 (suspend-enrollment tears down the provider child): the suspend
/// branch of the turn worker must `kill_child_only` on enrollment, exactly like
/// the adjacent silent-redrive branch. Otherwise the live child + IPC connection
/// survive the upstream disconnect and the resume routes through
/// `ensure_started`'s live-child reuse — returning the stale `acpSessionId`
/// WITHOUT a `session/load`. This drives a real suspend-overlapping turn failure
/// through the worker and asserts (a) the handle is torn down, and (b) the
/// resume then issues `session/load` against the persisted id.
#[tokio::test]
async fn suspend_enrollment_kills_child_so_resume_issues_session_load() {
    // Keep the enrollment self-heal (finding 2) from firing mid-test: we observe
    // the suspend branch's own teardown and drive the resume manually.
    let _env = EnvGuard::set_all(&[("INTENTD_WAKE_RESUME_SELF_HEAL_MS", "600000")]);

    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store)
        .with_event_bus(bus.clone())
        .with_suspend_tracker(Arc::new(AlwaysSuspended(Duration::from_secs(120))));
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let mgr = Arc::new(AgentManager::new(services, sink, 8));

    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-suspend-kill"));
    seed_agent(&mgr, &ws, &id).await;
    // A persisted ACP session id makes the mid-turn child reusable (the
    // live-child reuse hazard the fix guards against) and reloadable on resume.
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "existing-id")
        .await
        .unwrap();

    // A live child that fails its prompt transiently while a suspend overlaps.
    let _agent = track_mock_agent_prompt_rpc_error(&mgr, &id, "Connection reset by peer");
    assert!(
        mgr.contains(&id),
        "the child handle is live before the turn"
    );

    // Drive the turn: the worker reuses the live child, streams, then the prompt
    // fails → suspend-overlap classifier enrolls it → suspend branch runs.
    mgr.send_message(
        id.clone(),
        ws.clone(),
        "hi".to_string(),
        None,
        super::TurnOptions::default(),
    )
    .await
    .expect("send_message starts the turn worker");

    // (a) The suspend branch tore down the provider child — no live handle
    // survives for the resume to reuse.
    let mut torn_down = false;
    for _ in 0..60 {
        if !mgr.contains(&id) {
            torn_down = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        torn_down,
        "suspend enrollment must kill_child_only so the resume cannot reuse the live child"
    );

    // The turn was enrolled as system_suspend (not surfaced terminally).
    let row = mgr
        .services
        .store
        .get_interrupted_agent(&id)
        .await
        .unwrap()
        .expect("interrupted_agent row enrolled");
    assert_eq!(row.reason.as_deref(), Some("system_suspend"));

    // (b) Resume: with the child gone, session establishment issues `session/load`
    // against the persisted id (the recovery the enrollment promises) — proven by
    // a fresh child's request log, NOT the live-child reuse path.
    let (_agent2, log) = track_mock_agent_with_log(&mgr, &id, true);
    let sid = mgr
        .start_session(&id, PathBuf::from("/tmp/ws"), &test_provider())
        .await
        .expect("resume establishes a session");
    assert_eq!(
        sid, "existing-id",
        "resume reloads the persisted session id"
    );
    let sent: Vec<String> = log.lock().unwrap().iter().map(|(m, _)| m.clone()).collect();
    assert!(
        sent.iter().any(|m| m == "session/load"),
        "resume goes through session/load (fresh spawn), not live-child reuse: {sent:?}"
    );
}

/// A test provider that skips `authenticate` (deterministic handshake).
fn test_provider() -> intent_providers::ProviderConfig {
    intent_providers::ProviderConfig {
        supports_authenticate: false,
        ..*intent_providers::provider_config(intent_providers::first_provider_id())
    }
}

async fn seed_agent(mgr: &AgentManager, ws: &WorkspaceId, id: &AgentId) {
    seed_agent_with_task_graph(mgr, ws, id, false).await;
}

async fn seed_agent_with_task_graph(
    mgr: &AgentManager,
    ws: &WorkspaceId,
    id: &AgentId,
    task_graph_enabled: bool,
) {
    let ts = now_iso();
    let workspace = Workspace {
        id: ws.clone(),
        title: "WS".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts.clone(),
        last_activity: None,
        tags: vec![],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    };
    let session = AgentSession {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: id.clone(),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: "Builder".to_string(),
        name_explicitly_set: false,
        model: None,
        reasoning_effort: None,
        effort_levels: None,
        // Pinned explicitly: seeded sessions predate monorepo#3044, when a
        // `None` provider still resolved positionally to auggie; resolution
        // now fails loudly with no provider and no configured default.
        provider: Some("auggie".to_string()),
        system_prompt: None,
        specialist: None,
        status: AgentStatus::Pending,
        is_active: true,
        messages: Vec::new(),
        stats: None,
        task_note_id: None,
        skip_auto_commit: false,
        completion_report: None,
        completion_report_timestamp: None,
        attention_request_kind: None,
        attention_request_reason: None,
        attention_request_timestamp: None,
        delegation_depth: None,
        initial_message: None,
        context_references: None,
        image_blocks: None,
        file_blocks: None,
        is_background: false,
        metadata: None,
        created_at: ts.clone(),
        updated_at: ts,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
        pending_delete_at: None,
        retired_at: None,
    };
    mgr.services
        .store
        .insert_workspace(&workspace)
        .await
        .expect("insert ws");
    mgr.services
        .store
        .insert_agent_session_with_task_graph(&session, task_graph_enabled)
        .await
        .expect("insert session");
}

#[tokio::test]
async fn start_session_opens_first_session_without_recreate_flag() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-new"));
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);

    let sid = mgr
        .start_session(&id, PathBuf::from("/tmp/ws"), &test_provider())
        .await
        .expect("first session");
    assert_eq!(sid, MGR_ACP_SID);
    assert!(!mgr.take_recreated(&id), "brand-new agent is not flagged");
    let stored = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(stored.acp_session_id.as_deref(), Some(MGR_ACP_SID));
}

#[tokio::test]
async fn start_session_resumes_when_load_supported() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-resume"));
    seed_agent(&mgr, &ws, &id).await;
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "existing-id")
        .await
        .unwrap();
    let _agent = track_mock_agent(&mgr, &id, true);

    let sid = mgr
        .start_session(&id, PathBuf::from("/tmp/ws"), &test_provider())
        .await
        .expect("resume");
    assert_eq!(sid, "existing-id", "session/load resumes the stored id");
    assert!(!mgr.take_recreated(&id), "resume needs no history resend");
    let stored = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(stored.acp_session_id.as_deref(), Some("existing-id"));
}

#[tokio::test]
async fn start_session_recreates_and_flags_when_load_unsupported() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-recreate"));
    seed_agent(&mgr, &ws, &id).await;
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "stale-id")
        .await
        .unwrap();
    let _agent = track_mock_agent(&mgr, &id, false);

    let sid = mgr
        .start_session(&id, PathBuf::from("/tmp/ws"), &test_provider())
        .await
        .expect("recreate");
    assert_eq!(sid, MGR_ACP_SID, "fresh session replaces the lost id");
    let stored = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(stored.acp_session_id.as_deref(), Some(MGR_ACP_SID));
    // The recreate flag is set so the next turn resends history; take() clears it.
    assert!(mgr.take_recreated(&id), "recreate flags a history resend");
    assert!(!mgr.take_recreated(&id), "flag is cleared once taken");
}

/// The typed workspace-MCP bridge entry stashed on the handle (STAB-156):
/// built from the same normalized-server shape `create_agent` uses for
/// `supports_session_mcp_servers` providers (claude-code, codex, droid, grok).
fn test_session_mcp_servers() -> Vec<intent_acp::session::McpServer> {
    let mut servers = intent_acp::NormalizedMcpServers::new();
    servers.insert(
        "workspace-mcp".to_string(),
        intent_acp::NormalizedMcpServer::Stdio {
            command: "/usr/local/bin/intentd".into(),
            args: vec![
                "mcp-bridge".to_string(),
                "--connect".to_string(),
                "127.0.0.1:9999".to_string(),
            ],
            env: intent_acp::EnvMap::new(),
        },
    );
    intent_acp::to_acp_session_mcp_servers(&servers)
}

/// For providers that consume MCP servers from the ACP session setup
/// (claude-code, codex, droid, grok — STAB-156), the handle's stashed server list
/// rides the `session/new` request's `mcpServers` field, with the stdio bridge
/// entry serialized untagged (no `type`) as the ACP schema requires.
#[tokio::test]
async fn start_session_carries_session_mcp_servers_on_session_new() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-mcp-new"));
    seed_agent(&mgr, &ws, &id).await;
    let (_agent, log) = track_mock_agent_with_log(&mgr, &id, false);
    mgr.handles
        .lock()
        .unwrap()
        .get_mut(&id)
        .unwrap()
        .session_mcp_servers = test_session_mcp_servers();

    mgr.start_session(&id, PathBuf::from("/tmp/ws"), &test_provider())
        .await
        .expect("first session");

    let log = log.lock().unwrap();
    let (_, params) = log
        .iter()
        .find(|(m, _)| m == "session/new")
        .expect("session/new sent");
    let servers = params["mcpServers"].as_array().expect("mcpServers array");
    assert_eq!(servers.len(), 1);
    let bridge = &servers[0];
    assert_eq!(bridge["name"], json!("workspace-mcp"));
    assert_eq!(bridge["command"], json!("/usr/local/bin/intentd"));
    assert_eq!(
        bridge["args"],
        json!(["mcp-bridge", "--connect", "127.0.0.1:9999"])
    );
    assert!(
        bridge.get("type").is_none(),
        "stdio bridge entry must serialize untagged: {bridge}"
    );
}

/// Per the ACP schema, http/sse `McpServer` entries are only valid when the
/// agent advertised `mcpCapabilities.http`/`sse` in `initialize`. The scripted
/// mock advertises neither (serde defaults ⇒ false), so `start_session` must
/// drop the http/sse entries and keep only the mandatory-per-spec stdio bridge
/// — a user-configured remote catalog entry can't break agent spawn.
#[tokio::test]
async fn start_session_filters_remote_mcp_servers_without_capability() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-mcp-filter"));
    seed_agent(&mgr, &ws, &id).await;
    let (_agent, log) = track_mock_agent_with_log(&mgr, &id, false);
    let mut servers = intent_acp::NormalizedMcpServers::new();
    servers.insert(
        "remote-http".to_string(),
        intent_acp::NormalizedMcpServer::Http {
            url: "https://h".into(),
            headers: None,
        },
    );
    servers.insert(
        "remote-sse".to_string(),
        intent_acp::NormalizedMcpServer::Sse {
            url: "https://s".into(),
            headers: None,
        },
    );
    let mut stash = test_session_mcp_servers();
    stash.extend(intent_acp::to_acp_session_mcp_servers(&servers));
    mgr.handles
        .lock()
        .unwrap()
        .get_mut(&id)
        .unwrap()
        .session_mcp_servers = stash;

    mgr.start_session(&id, PathBuf::from("/tmp/ws"), &test_provider())
        .await
        .expect("first session");

    let log = log.lock().unwrap();
    let (_, params) = log
        .iter()
        .find(|(m, _)| m == "session/new")
        .expect("session/new sent");
    let sent = params["mcpServers"].as_array().expect("mcpServers array");
    assert_eq!(
        sent.len(),
        1,
        "http/sse entries dropped without advertised capability: {sent:?}"
    );
    assert_eq!(sent[0]["name"], json!("workspace-mcp"));
}

/// The same stashed server list rides `session/load` on the resume path, so a
/// resumed session reconnects the workspace-MCP bridge of the NEW child (the
/// old child's bridge died with it).
#[tokio::test]
async fn start_session_carries_session_mcp_servers_on_session_load() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-mcp-load"));
    seed_agent(&mgr, &ws, &id).await;
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "existing-id")
        .await
        .unwrap();
    let (_agent, log) = track_mock_agent_with_log(&mgr, &id, true);
    mgr.handles
        .lock()
        .unwrap()
        .get_mut(&id)
        .unwrap()
        .session_mcp_servers = test_session_mcp_servers();

    mgr.start_session(&id, PathBuf::from("/tmp/ws"), &test_provider())
        .await
        .expect("resume");

    let log = log.lock().unwrap();
    let (_, params) = log
        .iter()
        .find(|(m, _)| m == "session/load")
        .expect("session/load sent");
    let servers = params["mcpServers"].as_array().expect("mcpServers array");
    assert_eq!(servers.len(), 1);
    assert_eq!(servers[0]["name"], json!("workspace-mcp"));
}

/// Under the shipped `AllowAll` default, `start_session` best-effort asks the
/// provider to run in `bypassPermissions` mode (parity with the TS acp-provider)
/// once a session id is minted. Providers that don't advertise `set_mode` skip
/// the call; providers that do (auggie today) see it after `session/new`,
/// `session/load`, or the recreate path.
#[tokio::test]
async fn start_session_sends_bypass_permissions_under_allow_all() {
    let (_tmp, mgr) = manager().await;
    // `manager()` builds an AllowAll manager (the shipped default), which is
    // what wires `maybe_bypass_permissions` on the three session paths.
    assert_eq!(mgr.policy(), PermissionPolicy::AllowAll);

    // 1) Brand-new session: bypass follows `session/new`.
    let new_id = AgentId::from("a-bypass-new");
    seed_agent(&mgr, &WorkspaceId::from("ws-bypass-new"), &new_id).await;
    let (_agent_new, new_log) = track_mock_agent_with_log(&mgr, &new_id, false);
    let sid = mgr
        .start_session(&new_id, PathBuf::from("/tmp/ws"), &test_provider())
        .await
        .expect("first session");
    assert_eq!(sid, MGR_ACP_SID);
    let new_calls = new_log.lock().unwrap().clone();
    let set_mode = new_calls
        .iter()
        .find(|(m, _)| m == "session/set_mode")
        .expect("session/set_mode called after session/new");
    assert_eq!(set_mode.1["sessionId"], MGR_ACP_SID);
    assert_eq!(set_mode.1["modeId"], "bypassPermissions");
    // Ordering: `session/new` precedes the bypass attempt.
    let new_idx = new_calls
        .iter()
        .position(|(m, _)| m == "session/new")
        .expect("session/new in log");
    let set_idx = new_calls
        .iter()
        .position(|(m, _)| m == "session/set_mode")
        .expect("session/set_mode in log");
    assert!(new_idx < set_idx, "bypass attempted after session/new");

    // 2) Resume path: bypass follows `session/load`.
    let resume_id = AgentId::from("a-bypass-resume");
    let resume_ws = WorkspaceId::from("ws-bypass-resume");
    seed_agent(&mgr, &resume_ws, &resume_id).await;
    mgr.services
        .store
        .set_acp_session_id(&resume_ws, &resume_id, "existing-id")
        .await
        .unwrap();
    let (_agent_r, resume_log) = track_mock_agent_with_log(&mgr, &resume_id, true);
    let sid = mgr
        .start_session(&resume_id, PathBuf::from("/tmp/ws"), &test_provider())
        .await
        .expect("resume");
    assert_eq!(sid, "existing-id");
    let resume_calls = resume_log.lock().unwrap().clone();
    let set_mode = resume_calls
        .iter()
        .find(|(m, _)| m == "session/set_mode")
        .expect("session/set_mode called after session/load");
    assert_eq!(set_mode.1["sessionId"], "existing-id");
    assert_eq!(set_mode.1["modeId"], "bypassPermissions");

    // 3) Recreate path: bypass follows the fallback `session/new`.
    let recreate_id = AgentId::from("a-bypass-recreate");
    let recreate_ws = WorkspaceId::from("ws-bypass-recreate");
    seed_agent(&mgr, &recreate_ws, &recreate_id).await;
    mgr.services
        .store
        .set_acp_session_id(&recreate_ws, &recreate_id, "stale-id")
        .await
        .unwrap();
    let (_agent_rc, recreate_log) = track_mock_agent_with_log(&mgr, &recreate_id, false);
    let sid = mgr
        .start_session(&recreate_id, PathBuf::from("/tmp/ws"), &test_provider())
        .await
        .expect("recreate");
    assert_eq!(sid, MGR_ACP_SID);
    let recreate_calls = recreate_log.lock().unwrap().clone();
    let set_mode = recreate_calls
        .iter()
        .find(|(m, _)| m == "session/set_mode")
        .expect("session/set_mode called on recreate path");
    assert_eq!(set_mode.1["sessionId"], MGR_ACP_SID);
    assert_eq!(set_mode.1["modeId"], "bypassPermissions");
}

/// Every non-`AllowAll` policy leaves the provider alone: `Interactive` drives
/// the FE round-trip, `AutoByRisk` / `DenyAll` apply local decisions, and none
/// of them should ask the provider to disable its own prompts.
#[tokio::test]
async fn start_session_skips_bypass_under_non_allow_all_policies() {
    for policy in [
        PermissionPolicy::Interactive,
        PermissionPolicy::AutoByRisk,
        PermissionPolicy::DenyAll,
    ] {
        let (_tmp, mgr) = manager().await;
        let mgr = mgr.with_policy(policy);
        let (ws, id) = (
            WorkspaceId::from("ws-1"),
            AgentId::from(format!("a-no-bypass-{policy:?}").as_str()),
        );
        seed_agent(&mgr, &ws, &id).await;
        let (_agent, log) = track_mock_agent_with_log(&mgr, &id, false);
        mgr.start_session(&id, PathBuf::from("/tmp/ws"), &test_provider())
            .await
            .expect("session");
        let calls = log.lock().unwrap().clone();
        assert!(
            calls.iter().all(|(m, _)| m != "session/set_mode"),
            "policy {policy:?} must not attempt bypassPermissions; got {calls:?}"
        );
    }
}

/// A provider that doesn't advertise a bypass-equivalent in `availableModes`
/// (auggie today: `default`+`ask`) is left alone under `AllowAll` rather than
/// being hit with `session/set_mode bypassPermissions` and getting `-32602`.
/// The local `AllowAll` auto-approve carries the parity contract by itself.
#[tokio::test]
async fn start_session_skips_bypass_when_provider_doesnt_advertise_bypass_mode() {
    let (_tmp, mgr) = manager().await;
    assert_eq!(mgr.policy(), PermissionPolicy::AllowAll);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-no-bypass-cap"));
    seed_agent(&mgr, &ws, &id).await;
    // Mock advertises `default`+`ask` only, mirroring what auggie returns today.
    let (_agent, log) = track_mock_agent_with_log_modes(&mgr, &id, false, MockModes::no_bypass());
    mgr.start_session(&id, PathBuf::from("/tmp/ws"), &test_provider())
        .await
        .expect("session");
    let calls = log.lock().unwrap().clone();
    assert!(
        calls.iter().all(|(m, _)| m != "session/set_mode"),
        "no session/set_mode when provider doesn't advertise a bypass-equivalent; got {calls:?}"
    );
}

#[tokio::test]
async fn build_turn_prompt_prepends_history_once_after_recreate() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-hist"));
    seed_agent(&mgr, &ws, &id).await;
    // Prior transcript + the just-persisted current user message (the last row).
    for (role, text) in [
        ("user", "first question"),
        ("assistant", "first answer"),
        ("user", "current message"),
    ] {
        mgr.services
            .store
            .append_agent_message(
                &id,
                role,
                &json!([{ "type": "text", "text": text }]),
                &now_iso(),
            )
            .await
            .unwrap();
    }
    mgr.recreated.lock().unwrap().insert(id.clone());

    let prompt = mgr
        .build_turn_prompt(&id, &ws, "current message", &super::TurnOptions::default())
        .await;
    let text = serde_json::to_value(&prompt).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(text.contains("<supervisor>"), "history XML is prepended");
    assert!(text.contains("first question"));
    assert!(text.contains("first answer"));
    assert!(
        !text.contains("<text>current message</text>"),
        "current message is excluded from the rendered history"
    );
    assert!(
        text.trim_end().ends_with("current message"),
        "ends with the live prompt"
    );

    // The flag is consumed: a follow-up turn sends only the message text.
    let plain = mgr
        .build_turn_prompt(&id, &ws, "next message", &super::TurnOptions::default())
        .await;
    let plain_text = serde_json::to_value(&plain).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(plain_text, "next message");
}

// --- Attachment blocks (image + file) ----------------------------------------

/// FE-supplied `imageBlocks` become ACP `image` content blocks appended after
/// the text prompt (reference-parity `acp-provider.ts`), preserving `data`
/// and `mimeType` verbatim in the camelCase wire shape.
#[tokio::test]
async fn build_turn_prompt_appends_image_blocks_after_text() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-img"), AgentId::from("a-img"));
    seed_agent(&mgr, &ws, &id).await;

    let options = super::TurnOptions {
        image_blocks: Some(json!([
            {"data": "AAAA", "mimeType": "image/png"},
            {"data": "BBBB", "mimeType": "image/jpeg"},
        ])),
        ..super::TurnOptions::default()
    };
    let prompt = mgr.build_turn_prompt(&id, &ws, "hi", &options).await;
    let wire = serde_json::to_value(&prompt).unwrap();
    let arr = wire.as_array().unwrap();
    assert_eq!(arr.len(), 3, "text + 2 image blocks");
    assert_eq!(arr[0]["type"], json!("text"));
    assert_eq!(arr[1]["type"], json!("image"));
    assert_eq!(arr[1]["data"], json!("AAAA"));
    assert_eq!(arr[1]["mimeType"], json!("image/png"));
    assert_eq!(arr[2]["type"], json!("image"));
    assert_eq!(arr[2]["data"], json!("BBBB"));
    assert_eq!(arr[2]["mimeType"], json!("image/jpeg"));
}

/// FE-supplied `fileBlocks` become ACP `resource` content blocks with a
/// `BlobResourceContents` carrying the file name lifted into the resource
/// `uri` (`file:///<fileName>`), appended after any image blocks.
#[tokio::test]
async fn build_turn_prompt_appends_file_blocks_after_text_and_images() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-file"), AgentId::from("a-file"));
    seed_agent(&mgr, &ws, &id).await;

    let options = super::TurnOptions {
        image_blocks: Some(json!([{"data": "IMG", "mimeType": "image/png"}])),
        file_blocks: Some(json!([
            {"data": "Zm9v", "mimeType": "text/plain", "fileName": "notes.txt"},
            {"data": "YmFy", "mimeType": "application/pdf", "fileName": "spec.pdf"},
        ])),
        ..super::TurnOptions::default()
    };
    let prompt = mgr.build_turn_prompt(&id, &ws, "hi", &options).await;
    let wire = serde_json::to_value(&prompt).unwrap();
    let arr = wire.as_array().unwrap();
    assert_eq!(arr.len(), 4, "text + 1 image + 2 file blocks");
    assert_eq!(arr[0]["type"], json!("text"));
    assert_eq!(arr[1]["type"], json!("image"));
    // Images come before files, files come in caller order.
    assert_eq!(arr[2]["type"], json!("resource"));
    assert_eq!(arr[2]["resource"]["blob"], json!("Zm9v"));
    assert_eq!(arr[2]["resource"]["mimeType"], json!("text/plain"));
    assert_eq!(arr[2]["resource"]["uri"], json!("file:///notes.txt"));
    assert_eq!(arr[3]["type"], json!("resource"));
    assert_eq!(arr[3]["resource"]["blob"], json!("YmFy"));
    assert_eq!(arr[3]["resource"]["mimeType"], json!("application/pdf"));
    assert_eq!(arr[3]["resource"]["uri"], json!("file:///spec.pdf"));
}

/// Malformed attachment entries (missing required fields, wrong types) are
/// silently dropped so a partial array can never poison the whole turn — only
/// the well-formed sibling blocks reach the prompt.
#[tokio::test]
async fn build_turn_prompt_skips_malformed_attachments() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-bad"), AgentId::from("a-bad"));
    seed_agent(&mgr, &ws, &id).await;

    let options = super::TurnOptions {
        image_blocks: Some(json!([
            {"data": "OK"},                        // missing mimeType
            {"data": 42, "mimeType": "image/png"}, // wrong type
            {"data": "GOOD", "mimeType": "image/png"},
        ])),
        file_blocks: Some(json!([
            {"mimeType": "text/plain", "fileName": "x.txt"},   // missing data
            {"data": "d", "fileName": "x.txt"},                 // missing mimeType
            {"data": "d", "mimeType": "text/plain"},            // missing fileName
            {"data": "d", "mimeType": "text/plain", "fileName": "keep.txt"},
        ])),
        ..super::TurnOptions::default()
    };
    let prompt = mgr.build_turn_prompt(&id, &ws, "hi", &options).await;
    let wire = serde_json::to_value(&prompt).unwrap();
    let arr = wire.as_array().unwrap();
    // text + 1 well-formed image + 1 well-formed file.
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[1]["data"], json!("GOOD"));
    assert_eq!(arr[2]["resource"]["uri"], json!("file:///keep.txt"));
}

/// Combined interrupt delivery (STAB-114 / monorepo#1014): `prepend_content`
/// precedes the turn's own `content` inside the single text block, and the
/// preempted message's attachments (`prepend_image_blocks` /
/// `prepend_file_blocks`) come BEFORE this turn's own attachments.
#[tokio::test]
async fn build_turn_prompt_prepends_preempted_content_and_attachments_first() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-prep"), AgentId::from("a-prep"));
    seed_agent(&mgr, &ws, &id).await;

    let options = super::TurnOptions {
        prepend_content: Some("original ask".to_string()),
        prepend_image_blocks: Some(json!([{"data": "ORIG_IMG", "mimeType": "image/png"}])),
        prepend_file_blocks: Some(json!([
            {"data": "b3JpZw==", "mimeType": "text/plain", "fileName": "orig.txt"},
        ])),
        image_blocks: Some(json!([{"data": "NEW_IMG", "mimeType": "image/jpeg"}])),
        file_blocks: Some(json!([
            {"data": "bmV3", "mimeType": "text/plain", "fileName": "new.txt"},
        ])),
        ..super::TurnOptions::default()
    };
    let prompt = mgr
        .build_turn_prompt(&id, &ws, "urgent update", &options)
        .await;
    let wire = serde_json::to_value(&prompt).unwrap();
    let arr = wire.as_array().unwrap();
    assert_eq!(
        arr.len(),
        5,
        "text + orig img + orig file + new img + new file"
    );
    // The single text block carries both messages, original first.
    assert_eq!(arr[0]["type"], json!("text"));
    let text = arr[0]["text"].as_str().unwrap();
    let orig_pos = text.find("original ask").expect("preempted text present");
    let new_pos = text.find("urgent update").expect("interrupt text present");
    assert!(
        orig_pos < new_pos,
        "preempted text precedes interrupt: {text:?}"
    );
    // Preempted attachments precede this turn's own.
    assert_eq!(arr[1]["type"], json!("image"));
    assert_eq!(arr[1]["data"], json!("ORIG_IMG"));
    assert_eq!(arr[2]["type"], json!("resource"));
    assert_eq!(arr[2]["resource"]["uri"], json!("file:///orig.txt"));
    assert_eq!(arr[3]["type"], json!("image"));
    assert_eq!(arr[3]["data"], json!("NEW_IMG"));
    assert_eq!(arr[4]["type"], json!("resource"));
    assert_eq!(arr[4]["resource"]["uri"], json!("file:///new.txt"));
}

/// Recreated-session interaction (monorepo#1014): when the ACP session was
/// recreated, `build_turn_body`'s history replay already renders the
/// preempted user row, so `prepend_content` must NOT be injected a second
/// time. The prepend ATTACHMENTS still ride (the history XML is text-only).
#[tokio::test]
async fn build_turn_prompt_skips_prepend_text_when_session_recreated() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (
        WorkspaceId::from("ws-prep-rec"),
        AgentId::from("a-prep-rec"),
    );
    seed_agent(&mgr, &ws, &id).await;
    // Both user rows are persisted, as `interrupt_send_message` leaves them:
    // the preempted "original ask" and the interrupt turn's own "urgent update".
    mgr.services
        .store
        .append_agent_message(
            &id,
            "user",
            &json!([{ "type": "text", "text": "original ask" }]),
            &now_iso(),
        )
        .await
        .unwrap();
    mgr.services
        .store
        .append_agent_message(
            &id,
            "user",
            &json!([{ "type": "text", "text": "urgent update" }]),
            &now_iso(),
        )
        .await
        .unwrap();
    // Arm the recreate flag: the next prompt replays prior history.
    mgr.recreated.lock().unwrap().insert(id.clone());

    let options = super::TurnOptions {
        prepend_content: Some("original ask".to_string()),
        prepend_image_blocks: Some(json!([{"data": "ORIG_IMG", "mimeType": "image/png"}])),
        ..super::TurnOptions::default()
    };
    let prompt = mgr
        .build_turn_prompt(&id, &ws, "urgent update", &options)
        .await;
    let wire = serde_json::to_value(&prompt).unwrap();
    let arr = wire.as_array().unwrap();
    let text = arr[0]["text"].as_str().unwrap();
    // Exactly one copy of the preempted text: the history replay's. A second
    // copy would mean the prepend was injected on top of the replay.
    assert_eq!(
        text.matches("original ask").count(),
        1,
        "preempted text delivered once (via history replay): {text:?}"
    );
    // The recreate flag was consumed by `build_turn_body` (not left armed).
    assert!(!mgr.recreated.lock().unwrap().contains(&id));
    // Prepend attachments still delivered: history XML carries no blocks.
    assert_eq!(arr[1]["type"], json!("image"));
    assert_eq!(arr[1]["data"], json!("ORIG_IMG"));
}

/// Terminal-failure requeue interaction (monorepo#1014): a zero-output
/// interrupt turn that fails terminally is requeued via
/// `persist_error_and_requeue`; the rebuilt entry must carry the `prepend_*`
/// fields so the retry still delivers the preempted message combined.
#[tokio::test]
async fn terminal_failure_requeue_carries_prepend_fields() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-prep-rq"), AgentId::from("a-prep-rq"));
    seed_agent(&mgr, &ws, &id).await;

    let options = super::TurnOptions {
        prepend_content: Some("original ask".to_string()),
        prepend_image_blocks: Some(json!([{"data": "ORIG_IMG", "mimeType": "image/png"}])),
        prepend_file_blocks: Some(json!([
            {"data": "b3JpZw==", "mimeType": "text/plain", "fileName": "orig.txt"},
        ])),
        ..super::TurnOptions::default()
    };
    super::persist_error_and_requeue(&mgr, &id, &ws, "urgent update", &options, true, "boom").await;

    let queued = mgr
        .services
        .dequeue_message(&id)
        .expect("failed message requeued");
    assert_eq!(queued.content, "urgent update");
    assert_eq!(queued.prepend_content.as_deref(), Some("original ask"));
    assert_eq!(
        queued.prepend_image_blocks,
        Some(json!([{"data": "ORIG_IMG", "mimeType": "image/png"}]))
    );
    assert_eq!(
        queued.prepend_file_blocks,
        Some(json!([
            {"data": "b3JpZw==", "mimeType": "text/plain", "fileName": "orig.txt"},
        ]))
    );
    // The wire shape (`agent.getQueue`) does not leak the prompt-only fields.
    let wire = queued.to_value(0);
    assert!(wire.get("prependContent").is_none());
    assert!(wire.get("prependImageBlocks").is_none());
    assert!(wire.get("prependFileBlocks").is_none());
}

/// Quarantine-park interaction (monorepo#1034): `send_message` to a poisoned
/// session parks the message in the queue; the parked entry must carry the
/// caller's combined-delivery `prepend_*` fields so the `agent.retry` redrive
/// still delivers the preempted message ahead of the interrupt message.
#[tokio::test]
async fn quarantine_park_preserves_prepend_fields() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-prep-park"),
        AgentId::from("a-prep-park"),
    );
    seed_agent(&mgr, &ws, &id).await;
    mgr.services
        .store
        .set_agent_session_status(
            &ws,
            &id,
            AgentStatus::Error,
            false,
            &now_iso(),
            Some(Some(
                "The model provider blocked this response for safety reasons. \
                 Please start a new session"
                    .into(),
            )),
        )
        .await
        .expect("park session poisoned");

    let options = super::TurnOptions {
        prepend_content: Some("original ask".to_string()),
        prepend_image_blocks: Some(json!([{"data": "ORIG_IMG", "mimeType": "image/png"}])),
        prepend_file_blocks: Some(json!([
            {"data": "b3JpZw==", "mimeType": "text/plain", "fileName": "orig.txt"},
        ])),
        ..super::TurnOptions::default()
    };
    let result = mgr
        .send_message(
            id.clone(),
            ws.clone(),
            "urgent update".to_string(),
            None,
            options,
        )
        .await
        .expect("quarantined send parks in queue");
    assert_eq!(result["queued"], json!(true));
    assert_eq!(result["quarantined"], json!(true));

    let queued = mgr.services.dequeue_message(&id).expect("parked message");
    assert_eq!(queued.content, "urgent update");
    assert_eq!(queued.prepend_content.as_deref(), Some("original ask"));
    assert_eq!(
        queued.prepend_image_blocks,
        Some(json!([{"data": "ORIG_IMG", "mimeType": "image/png"}]))
    );
    assert_eq!(
        queued.prepend_file_blocks,
        Some(json!([
            {"data": "b3JpZw==", "mimeType": "text/plain", "fileName": "orig.txt"},
        ]))
    );
    // The wire shape (`agent.getQueue`) does not leak the prompt-only fields.
    let wire = queued.to_value(0);
    assert!(wire.get("prependContent").is_none());
    assert!(wire.get("prependImageBlocks").is_none());
    assert!(wire.get("prependFileBlocks").is_none());
}

/// Concurrent-send slot race (monorepo#1034): an interrupt during the "slot
/// claimed but no cancellable session yet" window falls through to
/// `send_message`, which loses `try_begin` and queues instead. The queued
/// entry must carry the `prepend_*` fields — before the fix they were
/// hard-coded `None` and the preempted message silently vanished from the
/// eventual drain's prompt.
#[tokio::test]
async fn busy_queue_fallback_preserves_prepend_fields() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-prep-busy"),
        AgentId::from("a-prep-busy"),
    );
    seed_agent(&mgr, &ws, &id).await;
    // Claim the slot without a session handle: `is_busy` is true but the
    // turn is not cancellable (no handle + no acpSessionId), so the
    // interrupt skips preemption and the inner send loses `try_begin`.
    assert!(mgr.try_begin(&id, &ws).await);

    let options = super::TurnOptions {
        prepend_content: Some("original ask".to_string()),
        prepend_image_blocks: Some(json!([{"data": "ORIG_IMG", "mimeType": "image/png"}])),
        prepend_file_blocks: Some(json!([
            {"data": "b3JpZw==", "mimeType": "text/plain", "fileName": "orig.txt"},
        ])),
        ..super::TurnOptions::default()
    };
    let result = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "urgent update".to_string(),
            None,
            options,
        )
        .await
        .expect("busy interrupt queues behind the starting turn");
    assert_eq!(result["queued"], json!(true), "parked, not streamed");

    let queued = mgr.services.dequeue_message(&id).expect("queued message");
    assert_eq!(queued.content, "urgent update");
    assert_eq!(queued.prepend_content.as_deref(), Some("original ask"));
    assert_eq!(
        queued.prepend_image_blocks,
        Some(json!([{"data": "ORIG_IMG", "mimeType": "image/png"}]))
    );
    assert_eq!(
        queued.prepend_file_blocks,
        Some(json!([
            {"data": "b3JpZw==", "mimeType": "text/plain", "fileName": "orig.txt"},
        ]))
    );

    mgr.end_turn(&id).await;
}

/// Append-failure auto-queue interaction (monorepo#1034 / #1056): when the
/// mid-send `append_agent_message_with_id` fails on a validated agent,
/// `send_message` falls back to the auto-queue — the queued entry must carry
/// the caller's combined-delivery `prepend_*` fields so the eventual drain
/// still delivers the preempted message ahead of the interrupt message.
///
/// The bare `ALTER TABLE agent_message RENAME` trick from the
/// `failed_drain_persist_*` tests cannot be reused verbatim here: the
/// up-front `require_agent_session` validation (monorepo#564) and the
/// vanished-session race guard inside the append-failure arm both READ the
/// transcript table, so hiding it entirely fails the send closed instead of
/// auto-queueing. Shadowing the renamed table with a read-only VIEW keeps
/// every read working while every INSERT fails ("cannot modify ... it is a
/// view"), forcing exactly the append to fail.
#[tokio::test]
async fn append_failure_queue_fallback_preserves_prepend_fields() {
    let script = mock_agent_script();
    // The rule keys on the exact "original\n\nnew" adjacency that
    // `build_turn_prompt` renders, so the assistant response below proves the
    // redriven drain delivered ONE combined prompt, original first.
    let behavior = json!({
        "rules": [{
            "ifPromptContains": "original ask\n\nurgent update",
            "response": "combined-original-first",
        }],
        "response": "prepend missing from prompt",
    })
    .to_string();
    let _env = EnvGuard::set_all(&[
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
        ("INTENTD_PERSIST_RETRY_BACKOFF_MS", "10,10"),
    ]);
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-prep-app"),
        AgentId::from("a-prep-app"),
    );
    seed_agent(&mgr, &ws, &id).await;
    set_session_provider(&mgr, &ws, &id, "mock").await;

    // Shadow the transcript table with a read-only VIEW: session validation
    // and transcript reads keep working, but the mid-send append INSERT
    // fails, taking the auto-queue arm.
    sqlx::query("ALTER TABLE agent_message RENAME TO agent_message_broken")
        .execute(mgr.services.store.write_pool())
        .await
        .expect("hide agent_message table");
    sqlx::query("CREATE VIEW agent_message AS SELECT * FROM agent_message_broken")
        .execute(mgr.services.store.write_pool())
        .await
        .expect("shadow agent_message with a read-only view");

    let options = super::TurnOptions {
        prepend_content: Some("original ask".to_string()),
        prepend_image_blocks: Some(json!([{"data": "ORIG_IMG", "mimeType": "image/png"}])),
        prepend_file_blocks: Some(json!([
            {"data": "b3JpZw==", "mimeType": "text/plain", "fileName": "orig.txt"},
        ])),
        ..super::TurnOptions::default()
    };
    let result = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "urgent update".to_string(),
            None,
            options,
        )
        .await
        .expect("append failure falls back to the auto-queue");
    assert_eq!(result["queued"], json!(true), "parked, not streamed");
    // Wire-only semantics: the RPC snapshot of the auto-queued entry does not
    // leak the prompt-only fields. The positive anchor guards against a
    // vacuous pass — `.get(...)` on a missing/non-object `queuedMessage`
    // would also return `None`.
    let wire = &result["queuedMessage"];
    assert_eq!(
        wire["content"],
        json!("urgent update"),
        "queuedMessage snapshot present on the fallback response: {result}"
    );
    assert!(wire.get("prependContent").is_none());
    assert!(wire.get("prependImageBlocks").is_none());
    assert!(wire.get("prependFileBlocks").is_none());

    // The auto-queue's synchronous self-drain dequeued the entry, hit the
    // same broken store in `persist_user`, and requeued it front (fail-closed
    // drain, #547) — threading the entry's `prepend_*` fields through
    // `TurnOptions` and back. The surviving entry proves the auto-queue arm
    // captured them: had `send_message` dropped them (the pre-fix bug), the
    // requeued entry would carry `None`.
    assert_eq!(
        mgr.services
            .store
            .get_agent_session_status(&id)
            .await
            .unwrap(),
        AgentStatus::Error,
        "failed self-drain parked the session for agent.retry"
    );
    let queued = mgr
        .services
        .dequeue_message(&id)
        .expect("auto-queued message survives the failed self-drain");
    assert!(
        queued.content.starts_with("urgent update"),
        "original content first (dequeue-wait note may follow): {}",
        queued.content
    );
    assert_eq!(queued.prepend_content.as_deref(), Some("original ask"));
    assert_eq!(
        queued.prepend_image_blocks,
        Some(json!([{"data": "ORIG_IMG", "mimeType": "image/png"}]))
    );
    assert_eq!(
        queued.prepend_file_blocks,
        Some(json!([
            {"data": "b3JpZw==", "mimeType": "text/plain", "fileName": "orig.txt"},
        ]))
    );
    assert!(!queued.persisted, "user row never reached the transcript");
    // The wire shape (`agent.getQueue`) does not leak the prompt-only fields.
    let wire = queued.to_value(0);
    assert!(wire.get("prependContent").is_none());
    assert!(wire.get("prependImageBlocks").is_none());
    assert!(wire.get("prependFileBlocks").is_none());
    mgr.services.requeue_front(&id, queued);

    // Restore the store and redrive: the drain must deliver ONE combined
    // prompt with the preempted text first (block ordering itself is covered
    // by `build_turn_prompt_prepends_preempted_content_and_attachments_first`).
    sqlx::query("DROP VIEW agent_message")
        .execute(mgr.services.store.write_pool())
        .await
        .expect("drop shadow view");
    sqlx::query("ALTER TABLE agent_message_broken RENAME TO agent_message")
        .execute(mgr.services.store.write_pool())
        .await
        .expect("restore agent_message table");
    let result = mgr
        .agent_retry(id.clone(), ws.clone())
        .await
        .expect("agent.retry");
    assert_eq!(result["redriven"], json!(true));
    timeout(Duration::from_secs(10), async {
        loop {
            let session = mgr.services.store.get_agent_session(&id).await.unwrap();
            if session.status == AgentStatus::RuntimeIdle
                && !mgr.is_busy(&id)
                && mgr.workers.lock().unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("retry turn completes and the agent goes idle");

    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    let user_rows: Vec<_> = messages
        .iter()
        .filter(|m| {
            m.role == "user"
                && m.content[0]["text"]
                    .as_str()
                    .is_some_and(|t| t.starts_with("urgent update"))
        })
        .collect();
    assert_eq!(
        user_rows.len(),
        1,
        "the interrupt message lands in the transcript exactly once: {messages:?}"
    );
    // Wire-only for the transcript too: the prepend content rides the prompt,
    // never persisting as its own user row.
    assert!(
        !messages.iter().any(|m| m.role == "user"
            && serde_json::to_string(&m.content)
                .unwrap()
                .contains("original ask")),
        "the prepend content must not persist as a user row: {messages:?}"
    );
    let assistant = messages
        .iter()
        .find(|m| m.role == "assistant")
        .expect("retried turn produced assistant output");
    assert!(
        serde_json::to_string(&assistant.content)
            .unwrap()
            .contains("combined-original-first"),
        "drain delivered the combined prompt original-first: {messages:?}"
    );
}

/// Turn correlation across retries (monorepo#1022): a terminal-failure
/// requeue mints a NEW entry `id` but preserves the failed turn's ORIGINAL
/// `turn_id`, so the retry redrives the same logical turn.
#[tokio::test]
async fn terminal_failure_requeue_preserves_turn_id() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-tid-rq"), AgentId::from("a-tid-rq"));
    seed_agent(&mgr, &ws, &id).await;

    let options = super::TurnOptions {
        turn_id: Some("turn-original".to_string()),
        ..super::TurnOptions::default()
    };
    super::persist_error_and_requeue(&mgr, &id, &ws, "retry me", &options, true, "boom").await;

    let queued = mgr
        .services
        .dequeue_message(&id)
        .expect("failed message requeued");
    assert_eq!(
        queued.turn_id, "turn-original",
        "original turn_id preserved"
    );
    assert_ne!(queued.id, queued.turn_id, "entry id is newly minted");
    // Wire shape (`agent.getQueue`) carries the correlation id.
    assert_eq!(queued.to_value(0)["turnId"], json!("turn-original"));
}

/// Bare wiring without a minted `turn_id` (options constructed directly in
/// tests): the requeue falls back to the new entry `id` so every entry always
/// carries a correlation id.
#[tokio::test]
async fn terminal_failure_requeue_defaults_turn_id_to_new_id() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-tid-def"), AgentId::from("a-tid-def"));
    seed_agent(&mgr, &ws, &id).await;

    let options = super::TurnOptions::default();
    super::persist_error_and_requeue(&mgr, &id, &ws, "retry me", &options, true, "boom").await;

    let queued = mgr
        .services
        .dequeue_message(&id)
        .expect("failed message requeued");
    assert_eq!(
        queued.turn_id, queued.id,
        "fallback turn_id equals entry id"
    );
}

/// Wire surface (monorepo#1022): the terminal `agent:failed` +
/// `agent:stream:end` pair carries the failed turn's `turnId` when present,
/// and omits the key entirely when absent (never `null`).
#[tokio::test]
async fn terminal_failure_events_carry_turn_id() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let (ws, id) = (WorkspaceId::from("ws-tfe-tid"), AgentId::from("a-tfe-tid"));
    seed_agent(&mgr, &ws, &id).await;

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    super::publish_terminal_failure_events(&mgr, &id, &ws, "boom", Some("turn-tfe-1")).await;

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let failed = events
        .iter()
        .find(|e| e.event_type == "agent:failed")
        .expect("agent:failed event");
    assert_eq!(failed.data["turnId"], json!("turn-tfe-1"));
    assert_eq!(failed.data["error"], json!("boom"));
    let end = events
        .iter()
        .find(|e| e.event_type == "agent:stream:end")
        .expect("agent:stream:end event");
    assert_eq!(end.data["turnId"], json!("turn-tfe-1"));

    // Omit-when-absent: a None turn id leaves both payloads without the key.
    let mut sub = bus.subscribe(SubscriptionFilter::default());
    super::publish_terminal_failure_events(&mgr, &id, &ws, "boom2", None).await;
    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    for ty in ["agent:failed", "agent:stream:end"] {
        let ev = events
            .iter()
            .find(|e| e.event_type == ty)
            .unwrap_or_else(|| panic!("{ty} event"));
        assert!(
            ev.data.get("turnId").is_none(),
            "{ty} must omit turnId when absent: {:?}",
            ev.data
        );
    }
}

/// Durable-before-observable (monorepo#2009): the terminal-failure handlers
/// complete the `status = error` + `stop_reason` store write BEFORE the
/// terminal `agent:failed`/`agent:stream:end` pair is published, so a client
/// that reads `agent.getSession` upon observing either event is guaranteed
/// the persisted Error. Runs the spawn handler on its own task and reads the
/// store the moment `agent:failed` arrives — with the write ordered after the
/// publish this read races the persist (the pre-#2009 flake in
/// `pi_spawn_fails_fast_on_old_cli_over_wss`); with the write ordered first
/// the read deterministically sees `error`. Also pins the unchanged wire
/// order: agent:failed → agent:stream:end → agent:status-changed →
/// agent:queue:updated.
#[tokio::test]
async fn terminal_failure_persists_error_before_publishing_events() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let (ws, id) = (WorkspaceId::from("ws-dbo"), AgentId::from("a-dbo"));
    seed_agent(&mgr, &ws, &id).await;

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let mgr = Arc::new(mgr);
    let handler = {
        let (mgr, ws, id) = (mgr.clone(), ws.clone(), id.clone());
        tokio::spawn(async move {
            let options = super::TurnOptions::default();
            super::handle_terminal_spawn_failure(
                &mgr,
                &id,
                &ws,
                "retry me",
                &options,
                true,
                &Error::Internal("boom".to_string()),
            )
            .await;
        })
    };

    // Read the store the moment agent:failed lands on the bus: the persisted
    // Error must already be visible.
    let mut events = Vec::new();
    'observed: loop {
        let batch = timeout(Duration::from_secs(10), sub.recv())
            .await
            .expect("agent:failed within 10s")
            .expect("bus open");
        for event in batch {
            let failed = event.event_type == "agent:failed";
            events.push(event);
            if failed {
                break 'observed;
            }
        }
    }
    let stored = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(
        stored.status,
        AgentStatus::Error,
        "status is durably error when agent:failed is observable (monorepo#2009)"
    );
    assert!(
        stored
            .stop_reason
            .as_deref()
            .is_some_and(|r| r.contains("boom")),
        "stop_reason persisted alongside the status: {:?}",
        stored.stop_reason
    );
    handler.await.unwrap();

    // Wire order is unchanged by the persist reorder.
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let order: Vec<&str> = events
        .iter()
        .map(|e| e.event_type.as_str())
        .filter(|t| {
            matches!(
                *t,
                "agent:failed"
                    | "agent:stream:end"
                    | "agent:status-changed"
                    | "agent:queue:updated"
            )
        })
        .collect();
    assert_eq!(
        order,
        vec![
            "agent:failed",
            "agent:stream:end",
            "agent:status-changed",
            "agent:queue:updated"
        ],
        "terminal wire order unchanged"
    );
}

/// Teardown paths that abort the turn worker can land between the streaming
/// path's terminal-error stash and the terminal-failure handler's take
/// (monorepo#2050): the orphaned entry describes the aborted turn, so a LATER
/// failure must not consume it (its streak / `stop_reason` would mis-describe
/// the new failure). Every worker-abort path — stop/detach, interrupt, retry,
/// delete — must discard the slot.
#[tokio::test]
async fn worker_abort_paths_discard_stale_pending_terminal_error() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-stale-stash"),
        AgentId::from("a-stale-stash"),
    );
    seed_agent(&mgr, &ws, &id).await;

    let stash = |services: &crate::Services| {
        let (services, id, ws) = (services.clone(), id.clone(), ws.clone());
        async move {
            let persist = super::persist_terminal_error_status_via_services(
                &services,
                &id,
                &ws,
                "session/prompt failed: aborted turn",
            )
            .await;
            services.stash_pending_terminal_error(&id, persist);
        }
    };

    // stop() → detach: discards the stash.
    stash(&mgr.services).await;
    mgr.stop(&id).await;
    assert!(
        mgr.services.take_pending_terminal_error(&id).is_none(),
        "stop discards a stale pending terminal error"
    );

    // interrupt() (keep-alive; no handle → falls back through the stop arm,
    // and the live-handle path discards at the worker abort): discards.
    stash(&mgr.services).await;
    mgr.interrupt(&id).await;
    assert!(
        mgr.services.take_pending_terminal_error(&id).is_none(),
        "interrupt discards a stale pending terminal error"
    );

    // agent.retry (the clean-slate escape hatch): discards alongside the
    // failure streak. Park the session in Error first so retry proceeds.
    mgr.services
        .store
        .set_agent_session_status(
            &ws,
            &id,
            AgentStatus::Error,
            false,
            &now_iso(),
            Some(Some("session/prompt failed: aborted turn".into())),
        )
        .await
        .expect("park in error");
    stash(&mgr.services).await;
    mgr.agent_retry(id.clone(), ws.clone())
        .await
        .expect("retry");
    assert!(
        mgr.services.take_pending_terminal_error(&id).is_none(),
        "retry discards a stale pending terminal error"
    );
}

/// Wire surface (monorepo#1022): the drain loop's `agent:queue:processing`
/// names the entry being flipped to in-flight, including its `turnId` — the
/// drain-start signal that covers `persisted: true` redrives which skip the
/// user-row echo. Uses the quarantine park path to enqueue via
/// `send_message` so the entry carries a daemon-minted turn id.
#[tokio::test]
async fn drain_emits_queue_processing_with_turn_id() {
    let script = mock_agent_script();
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-qp-tid"), AgentId::from("a-qp-tid"));
    seed_agent(&mgr, &ws, &id).await;
    let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
    session.provider = Some("mock".to_string());
    mgr.services
        .store
        .update_agent_session(&ws, &session)
        .await
        .expect("set mock provider");

    let (enqueued, _) = mgr.services.enqueue_message(
        &id,
        "queued work".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    let mut sub = bus.subscribe(SubscriptionFilter::default());
    mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;

    // Wait for the drained turn to finish so the worker exits cleanly.
    timeout(Duration::from_secs(10), async {
        loop {
            if !mgr.is_busy(&id) && mgr.workers.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("drained turn completes");

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let processing = events
        .iter()
        .find(|e| e.event_type == intent_core::events::AGENT_QUEUE_PROCESSING)
        .expect("agent:queue:processing event");
    assert_eq!(processing.data["agentId"], json!(id.0));
    assert_eq!(processing.data["messageId"], json!(enqueued.id));
    // The payload's content matches what is persisted/sent to the provider,
    // which includes the dequeue-wait annotation appended at drain time.
    assert!(
        processing.data["content"]
            .as_str()
            .is_some_and(|c| c.starts_with("queued work")),
        "queue:processing carries the drained content: {:?}",
        processing.data
    );
    assert_eq!(
        processing.data["turnId"],
        json!(enqueued.turn_id),
        "queue:processing names the drained entry's turn: {:?}",
        processing.data
    );
    // The user-row echo for the drained message carries the same turnId.
    let echo = events
        .iter()
        .find(|e| {
            e.event_type == intent_core::events::AGENT_MESSAGE && e.data["role"] == json!("user")
        })
        .expect("user-row agent:message echo");
    assert_eq!(
        echo.data["turnId"],
        json!(enqueued.turn_id),
        "user-row echo carries the drained turn id: {:?}",
        echo.data
    );
}

// --- First-turn workspace-naming instruction ---------------------------------

/// Seed an agent whose workspace already carries `title` (used by naming-instruction
/// tests to distinguish slug-shaped vs custom titles).
async fn seed_agent_with_title(mgr: &AgentManager, ws: &WorkspaceId, id: &AgentId, title: &str) {
    seed_agent(mgr, ws, id).await;
    let mut session = mgr.services.store.get_agent_session(id).await.unwrap();
    session.name_explicitly_set = true;
    mgr.services
        .store
        .update_agent_session(ws, &session)
        .await
        .expect("mark agent name explicit");
    let mut workspace = mgr.services.store.get_workspace(ws).await.unwrap();
    workspace.title = title.to_string();
    mgr.services
        .store
        .update_workspace(&workspace)
        .await
        .expect("update ws title");
}

/// Point the seeded agent session at a specific provider (allowed while
/// `acp_session_id` is unset — provider immutability only kicks in after
/// first real use).
async fn set_session_provider(mgr: &AgentManager, ws: &WorkspaceId, id: &AgentId, provider: &str) {
    let mut session = mgr.services.store.get_agent_session(id).await.unwrap();
    session.provider = Some(provider.to_string());
    mgr.services
        .store
        .update_agent_session(ws, &session)
        .await
        .expect("update session provider");
}

/// Slug-shaped workspace title on an agent's first turn → the naming instruction
/// is prepended as a `<system>` block naming the daemon MCP tool as the
/// session's provider (auggie) surfaces it (`set_workspace_title_workspace-mcp`),
/// not the FE `workspace_api` surface.
#[tokio::test]
async fn build_turn_prompt_injects_naming_instruction_for_slug_title() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-slug"), AgentId::from("a-slug"));
    seed_agent_with_title(&mgr, &ws, &id, "amber-fox").await;
    set_session_provider(&mgr, &ws, &id, "auggie").await;
    // Persist the current user turn so `build_turn_prompt` sees the "first
    // turn" shape (one user message, zero assistant messages).
    mgr.services
        .store
        .append_agent_message(
            &id,
            "user",
            &json!([{ "type": "text", "text": "hello" }]),
            &now_iso(),
        )
        .await
        .unwrap();

    let prompt = mgr
        .build_turn_prompt(&id, &ws, "hello", &super::TurnOptions::default())
        .await;
    let text = serde_json::to_value(&prompt).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        text.starts_with("<system>"),
        "naming instruction prepends the prompt: {text:?}"
    );
    assert!(
        text.contains("`set_workspace_title_workspace-mcp`"),
        "instruction names the daemon MCP tool: {text:?}"
    );
    assert!(
        !text.contains("workspace_api"),
        "instruction must not reference the FE workspace_api surface: {text:?}"
    );
    assert!(text.trim_end().ends_with("hello"));
}

/// Empty workspace title on an agent's first turn → the naming instruction
/// still fires (Untitled parity: `create_workspace` now stores `""` when the
/// caller omits a title, and `needsWorkspaceRename` treats empty/whitespace
/// titles as "needs rename").
#[tokio::test]
async fn build_turn_prompt_injects_naming_instruction_for_empty_title() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-empty"), AgentId::from("a-empty"));
    seed_agent_with_title(&mgr, &ws, &id, "").await;
    set_session_provider(&mgr, &ws, &id, "auggie").await;
    mgr.services
        .store
        .append_agent_message(
            &id,
            "user",
            &json!([{ "type": "text", "text": "hello" }]),
            &now_iso(),
        )
        .await
        .unwrap();

    let prompt = mgr
        .build_turn_prompt(&id, &ws, "hello", &super::TurnOptions::default())
        .await;
    let text = serde_json::to_value(&prompt).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        text.starts_with("<system>"),
        "empty title still triggers the naming instruction: {text:?}"
    );
    assert!(text.contains("`set_workspace_title_workspace-mcp`"));
}

/// opencode session → the naming instruction spells the tool with opencode's
/// LEADING server prefix (`workspace-mcp_set_workspace_title`), the mirror of
/// auggie's trailing `_workspace-mcp` suffix.
#[tokio::test]
async fn build_turn_prompt_naming_instruction_uses_opencode_tool_name() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-oc"), AgentId::from("a-oc"));
    seed_agent_with_title(&mgr, &ws, &id, "amber-fox").await;
    set_session_provider(&mgr, &ws, &id, "opencode").await;
    mgr.services
        .store
        .append_agent_message(
            &id,
            "user",
            &json!([{ "type": "text", "text": "hello" }]),
            &now_iso(),
        )
        .await
        .unwrap();

    let prompt = mgr
        .build_turn_prompt(&id, &ws, "hello", &super::TurnOptions::default())
        .await;
    let text = serde_json::to_value(&prompt).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        text.contains("`workspace-mcp_set_workspace_title`"),
        "opencode nudge uses the leading server prefix: {text:?}"
    );
    assert!(
        !text.contains("set_workspace_title_workspace-mcp"),
        "opencode nudge must not use auggie's trailing suffix: {text:?}"
    );
}

/// A compound `provider:model` id wins over `session.provider` for the nudge
/// spelling (same precedence as `resolve_spawn`): an auggie-flagged session
/// whose model targets opencode gets the opencode tool name.
#[tokio::test]
async fn build_turn_prompt_naming_instruction_prefers_model_provider_prefix() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (
        WorkspaceId::from("ws-compound"),
        AgentId::from("a-compound"),
    );
    seed_agent_with_title(&mgr, &ws, &id, "amber-fox").await;
    let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
    session.provider = Some("auggie".to_string());
    session.model = Some("opencode:kimi-k3".to_string());
    mgr.services
        .store
        .update_agent_session(&ws, &session)
        .await
        .unwrap();
    mgr.services
        .store
        .append_agent_message(
            &id,
            "user",
            &json!([{ "type": "text", "text": "hello" }]),
            &now_iso(),
        )
        .await
        .unwrap();

    let prompt = mgr
        .build_turn_prompt(&id, &ws, "hello", &super::TurnOptions::default())
        .await;
    let text = serde_json::to_value(&prompt).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        text.contains("`workspace-mcp_set_workspace_title`"),
        "model provider prefix wins over session.provider: {text:?}"
    );
}

/// A malformed compound model id (`:sonnet` — empty provider prefix) must not
/// shadow `session.provider`: the empty prefix falls through and the session's
/// provider spelling is used (guard in `agent_session::resolve_provider_id`).
#[tokio::test]
async fn build_turn_prompt_naming_instruction_ignores_empty_compound_prefix() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (
        WorkspaceId::from("ws-malformed"),
        AgentId::from("a-malformed"),
    );
    seed_agent_with_title(&mgr, &ws, &id, "amber-fox").await;
    let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
    session.provider = Some("auggie".to_string());
    session.model = Some(":sonnet".to_string());
    mgr.services
        .store
        .update_agent_session(&ws, &session)
        .await
        .unwrap();
    mgr.services
        .store
        .append_agent_message(
            &id,
            "user",
            &json!([{ "type": "text", "text": "hello" }]),
            &now_iso(),
        )
        .await
        .unwrap();

    let prompt = mgr
        .build_turn_prompt(&id, &ws, "hello", &super::TurnOptions::default())
        .await;
    let text = serde_json::to_value(&prompt).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        text.contains("`set_workspace_title_workspace-mcp`"),
        "empty compound prefix must fall through to session.provider: {text:?}"
    );
}

/// Providers with unknown MCP tool naming (claude-code here; also
/// codex/droid/grok until their MCP tool spellings are empirically captured)
/// → the nudge falls back to the generic phrasing instead of guessing an
/// affixed tool name.
#[tokio::test]
async fn build_turn_prompt_naming_instruction_generic_for_unknown_provider() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-cc"), AgentId::from("a-cc"));
    seed_agent_with_title(&mgr, &ws, &id, "amber-fox").await;
    set_session_provider(&mgr, &ws, &id, "claude-code").await;
    mgr.services
        .store
        .append_agent_message(
            &id,
            "user",
            &json!([{ "type": "text", "text": "hello" }]),
            &now_iso(),
        )
        .await
        .unwrap();

    let prompt = mgr
        .build_turn_prompt(&id, &ws, "hello", &super::TurnOptions::default())
        .await;
    let text = serde_json::to_value(&prompt).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        text.contains("call the `set_workspace_title` tool from the workspace MCP server"),
        "unknown-provider nudge uses the generic phrasing: {text:?}"
    );
    assert!(
        !text.contains("set_workspace_title_workspace-mcp")
            && !text.contains("workspace-mcp_set_workspace_title"),
        "unknown-provider nudge must not carry an affixed tool name: {text:?}"
    );
}

/// Custom workspace title on an agent's first turn → no naming instruction is
/// injected (the reference `needsWorkspaceRename` guard skips already-titled
/// workspaces).
#[tokio::test]
async fn build_turn_prompt_skips_naming_instruction_for_custom_title() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-custom"), AgentId::from("a-custom"));
    seed_agent_with_title(&mgr, &ws, &id, "Add dark mode support").await;
    mgr.services
        .store
        .append_agent_message(
            &id,
            "user",
            &json!([{ "type": "text", "text": "hi" }]),
            &now_iso(),
        )
        .await
        .unwrap();

    let prompt = mgr
        .build_turn_prompt(&id, &ws, "hi", &super::TurnOptions::default())
        .await;
    let text = serde_json::to_value(&prompt).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(text, "hi", "no naming block for already-titled workspaces");
}

/// Slug-shaped title but an assistant message already exists → the naming
/// instruction fires only on the FIRST turn and stays absent for every turn
/// after (reference `!messages.some(m => m.role === 'assistant')`).
#[tokio::test]
async fn build_turn_prompt_skips_naming_instruction_after_first_turn() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-second"), AgentId::from("a-second"));
    seed_agent_with_title(&mgr, &ws, &id, "amber-fox").await;
    for (role, text) in [
        ("user", "first question"),
        ("assistant", "first answer"),
        ("user", "follow-up"),
    ] {
        mgr.services
            .store
            .append_agent_message(
                &id,
                role,
                &json!([{ "type": "text", "text": text }]),
                &now_iso(),
            )
            .await
            .unwrap();
    }

    let prompt = mgr
        .build_turn_prompt(&id, &ws, "follow-up", &super::TurnOptions::default())
        .await;
    let text = serde_json::to_value(&prompt).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        !text.contains("<system>"),
        "second-turn prompt carries no naming instruction: {text:?}"
    );
    assert_eq!(text, "follow-up");
}

/// STAB-28: The keep-alive interrupt path emits `agent:stream:end` and NOW
/// ALSO emits `agent:idle` when the agent has no queued ready-to-send messages.
/// This fixes the bug where a parent that re-messages via agent.send after a
/// child settles registers a completion watch that never fires (the aborted
/// worker never reaches `run_prompt_turn`'s idle-emit path). When the agent DOES
/// have queued messages, idle is suppressed (the agent will resume immediately).
#[tokio::test]
async fn interrupt_emits_terminal_stream_end_and_idle_when_no_queue() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-int"));
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);
    // An `acpSessionId` is required for the keep-alive interrupt (otherwise
    // `interrupt` falls back to the hard `stop` kill path).
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-int")
        .await
        .unwrap();
    // Claim the in-flight slot so the interrupt exercises the busy turn path.
    assert!(mgr.try_begin(&id, &ws).await);

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    assert!(mgr.interrupt(&id).await, "interrupt finds the live agent");

    // Drain the published events within a bounded window.
    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    assert!(
        types.contains(&"agent:stream:end"),
        "interrupt emits the terminal stream:end (got {types:?})"
    );
    assert!(
        types.contains(&"agent:idle"),
        "STAB-28: interrupt NOW emits agent:idle when queue is empty (got {types:?})"
    );
    // The synthetic idle carries the session enrichment — `isBackground`
    // reflects the session row's flag (`false` for this foreground agent).
    let idle = events
        .iter()
        .find(|e| e.event_type == "agent:idle")
        .expect("agent:idle event");
    assert_eq!(
        idle.data["isBackground"],
        json!(false),
        "interrupt-path agent:idle carries isBackground (got {:?})",
        idle.data
    );
    // The interrupt terminal is distinguishable from a normal turn end: it
    // carries `stopReason: "interrupted"` plus the machine-readable
    // `interruptReason`. No live-turn slot existed here, so no interrupted
    // row was persisted and `messageId` is absent.
    let end = events
        .iter()
        .find(|e| e.event_type == "agent:stream:end")
        .expect("stream:end event");
    assert_eq!(
        end.data["stopReason"], "interrupted",
        "interrupt stream:end carries stopReason (got {:?})",
        end.data
    );
    assert_eq!(
        end.data["interruptReason"], "user_stop",
        "plain agent.stop stamps user_stop on stream:end (got {:?})",
        end.data
    );
    assert!(
        end.data.get("interruptedBy").is_none(),
        "no sender attribution outside message preemption (got {:?})",
        end.data
    );
    assert!(
        end.data.get("messageId").is_none(),
        "no live-turn slot → no messageId on stream:end (got {:?})",
        end.data
    );
}

/// STAB-28 suppression case: interrupt must NOT emit agent:idle when the agent
/// has queued ready-to-send messages — the agent will resume immediately, so
/// waking parents on idle would be premature. This regression guard ensures the
/// gating logic stays correct (the empty-queue case is exercised above).
#[tokio::test]
async fn interrupt_suppresses_idle_when_queue_has_ready_to_send() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-int"));
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-int")
        .await
        .unwrap();
    assert!(mgr.try_begin(&id, &ws).await);

    // Queue a ready-to-send message via agent_queue_message_op (mirroring
    // agent.queueMessage over the wire). New messages are always editing=false
    // so they're immediately ready to send.
    let _ = mgr
        .services
        .agent_queue_message_op(id.clone(), "follow-up".into(), None, None)
        .await
        .expect("queue");

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    assert!(mgr.interrupt(&id).await, "interrupt finds the live agent");

    // Drain the published events within a bounded window.
    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    assert!(
        types.contains(&"agent:stream:end"),
        "interrupt still emits the terminal stream:end (got {types:?})"
    );
    assert!(
        !types.contains(&"agent:idle"),
        "STAB-28: interrupt suppresses agent:idle when queue has ready-to-send (got {types:?})"
    );
}

/// A WEDGED transport (intent-hq/monorepo#3039): the child stopped draining
/// its stdin — e.g. after ingesting a multi-MB tool result — so the writer
/// task is blocked mid-`write_all` on the full pipe and the outbound writer
/// channel is saturated. `Connection::notify` then awaits channel capacity
/// forever. Returns the connection plus the KEPT far-end duplex halves
/// (dropping them would error the writes instead of blocking, turning the
/// wedge into a plain transport-closed).
async fn wedged_connection() -> (
    Arc<Connection>,
    tokio::io::DuplexStream,
    tokio::io::DuplexStream,
) {
    // Tiny stdin pipe the far end never reads: the FIRST oversized line
    // blocks the writer task mid-write_all.
    let (client_w, agent_r) = tokio::io::duplex(64);
    let (agent_w, client_r) = tokio::io::duplex(64);
    let conn = Arc::new(Connection::new(
        client_w,
        client_r,
        None,
        ConnectionHooks::default(),
    ));
    // Saturate the writer channel (capacity 256): one line wedges the writer
    // task, 256 more fill the channel, the rest park awaiting capacity.
    let payload = json!({ "pad": "x".repeat(256) });
    for _ in 0..300 {
        let conn = Arc::clone(&conn);
        let payload = payload.clone();
        tokio::spawn(async move {
            let _ = conn.notify("wedge/fill", payload).await;
        });
    }
    // The channel is full once a fresh notify no longer completes promptly.
    timeout(Duration::from_secs(5), async {
        loop {
            let conn = Arc::clone(&conn);
            let payload = payload.clone();
            let probe = tokio::spawn(async move {
                let _ = conn.notify("wedge/probe", payload).await;
            });
            if timeout(Duration::from_millis(100), probe).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("writer channel saturates");
    // Confirm the saturation with a longer window: under heavy scheduling
    // starvation the 100ms probe above could break early on a send that
    // merely wasn't polled in time (residual capacity), and the tests would
    // then fail on a confusing downstream assert. A fresh notify still
    // pending after 500ms fails loudly HERE instead.
    let confirm_conn = Arc::clone(&conn);
    let confirm_payload = payload.clone();
    let confirm = tokio::spawn(async move {
        let _ = confirm_conn.notify("wedge/confirm", confirm_payload).await;
    });
    assert!(
        timeout(Duration::from_millis(500), confirm).await.is_err(),
        "saturation loop broke early: the confirming notify still found channel capacity"
    );
    (conn, agent_r, agent_w)
}

/// intent-hq/monorepo#3039: `agent.stop` against a WEDGED transport must not
/// hang. Before the fix, `interrupt` awaited `session/cancel`'s unbounded
/// channel send forever — Stop never reached the terminal emits, the FE spun
/// on "Thinking", and the idle sweep later reaped the agent silently. Now the
/// cancel enqueue is time-bounded: on timeout the child is torn down and the
/// terminal `agent:stream:end` + `agent:idle` still reach the bus.
#[tokio::test]
async fn interrupt_on_wedged_transport_still_emits_terminal_events() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-wedged"));
    seed_agent(&mgr, &ws, &id).await;
    let (conn, _agent_r, _agent_w) = wedged_connection().await;
    let (_note_tx, note_rx) = mpsc::unbounded_channel::<IncomingNotification>();
    mgr.handles.lock().unwrap().insert(
        id.clone(),
        AgentHandle {
            connection: conn,
            notifications: Arc::new(TokioMutex::new(note_rx)),
            serve_task: tokio::spawn(async {}),
            child: None,
            child_pid: None,
            _mcp_bridge: None,
            _mcp_config: None,
            _rules_config: None,
            _pi_extension: None,
            session_mcp_servers: Vec::new(),
            spawned_model: None,
            spawned_provider: "auggie".to_string(),
            thought_level: None,
            wake_gate: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_listener: None,
        },
    );
    mgr.registry.register(id.clone(), mgr.make_kill(id.clone()));
    // A live `acpSessionId` keeps the stop on the keep-alive interrupt path
    // (the one that sends `session/cancel` over the wedged wire).
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-wedged")
        .await
        .unwrap();
    // Claim the in-flight slot: the incident agent was mid-turn.
    assert!(mgr.try_begin(&id, &ws).await);

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    // The stop must settle within the bounded cancel window (+ margin), not
    // hang forever on the wedged writer channel.
    let found = timeout(Duration::from_secs(10), mgr.interrupt(&id))
        .await
        .expect("interrupt returns despite the wedged transport (monorepo#3039)");
    assert!(found, "interrupt finds the live agent");

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    assert!(
        types.contains(&"agent:stream:end"),
        "wedged-transport stop still emits the terminal stream:end (got {types:?})"
    );
    assert!(
        types.contains(&"agent:idle"),
        "wedged-transport stop still emits agent:idle so completion watches fire (got {types:?})"
    );
    // The undeliverable cancel voided the keep-alive contract: the wedged
    // child was torn down instead of being left for the silent idle reap.
    assert!(
        !mgr.contains(&id),
        "wedged child handle is removed (not resumable)"
    );
    assert!(!mgr.is_busy(&id), "busy slot released");
}

/// intent-hq/monorepo#3039 (idle-timeout half): the warn-and-continue path's
/// `session/cancel` settle against a WEDGED transport must report "not
/// settled" within the bounded window instead of hanging the turn worker —
/// the caller then tears the child down and the warning turn spawns fresh.
#[tokio::test]
async fn cancel_and_settle_idle_prompt_times_out_on_wedged_transport() {
    let (conn, _agent_r, _agent_w) = wedged_connection().await;
    let id = AgentId::from("a-wedged-settle");
    let settled = timeout(
        Duration::from_secs(10),
        super::cancel_and_settle_idle_prompt(&conn, &id, "acp-wedged"),
    )
    .await
    .expect("settle returns despite the wedged transport (monorepo#3039)");
    assert!(!settled, "a wedged child is reported as unsettleable");
}

/// `priority: "interrupt"` delivery to a BUSY agent preempts the turn
/// keep-alive: the message streams immediately (`queued: false`) instead of
/// queueing behind the turn, the preemption emits the terminal
/// `agent:stream:end`, and the child handle survives — the agent is never
/// killed (contrast the hard `stop`, which tears the child down).
#[tokio::test]
async fn interrupt_send_message_preempts_busy_turn_without_kill() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-int-send"));
    seed_agent(&mgr, &ws, &id).await;
    // Keep-alive semantics under test require a provider WITHOUT the
    // `kills_child_on_interrupt` quirk (seed_agent's auggie now has it —
    // monorepo#2763; the flagged teardown is covered by
    // `interrupt_kills_child_for_provider_with_kill_quirk`). The env var +
    // spawned_provider alignment keep the follow-up send on the live-child
    // reuse path (no respawn, no resolve_spawn failure).
    let script = mock_agent_script();
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
    set_session_provider(&mgr, &ws, &id, "mock").await;
    let _agent = track_mock_agent(&mgr, &id, false);
    mgr.handles
        .lock()
        .unwrap()
        .get_mut(&id)
        .unwrap()
        .spawned_provider = "node".to_string();
    // A live `acpSessionId` keeps the preemption on the keep-alive interrupt
    // path (no session → `interrupt` would fall back to the kill path).
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-int-send")
        .await
        .unwrap();
    // Claim the in-flight slot so the send sees a busy (mid-turn) agent.
    assert!(mgr.try_begin(&id, &ws).await);

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let result = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "urgent".to_string(),
            None,
            super::TurnOptions::default(),
        )
        .await
        .expect("interrupt send");
    assert_eq!(result["success"], json!(true));
    assert_eq!(
        result["queued"],
        json!(false),
        "interrupt priority streams immediately, never queues: {result}"
    );
    assert!(result["messageId"].is_string());

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    let end = events
        .iter()
        .find(|e| e.event_type == "agent:stream:end")
        .unwrap_or_else(|| panic!("preemption emits the terminal stream:end (got {types:?})"));
    assert_eq!(
        end.data["interruptReason"], "preempted_by_message",
        "preemption stream:end carries the machine-readable reason (got {:?})",
        end.data
    );
    assert!(
        end.data.get("interruptedBy").is_none(),
        "automatic send with no sender attribution stamps no interruptedBy (got {:?})",
        end.data
    );
    assert!(
        mgr.handles.lock().unwrap().contains_key(&id),
        "the child handle survives the interrupt (never killed)"
    );
    // The interrupt message was persisted as the next user turn.
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    let last = session.messages.last().expect("message persisted");
    assert_eq!(last.role, "user");
    assert!(serde_json::to_string(&last.content)
        .unwrap()
        .contains("urgent"));
}

/// Provider `kills_child_on_interrupt` quirk (intent-hq/monorepo#2763): a
/// keep-alive interrupt on a session whose provider carries the flag (auggie
/// — `seed_agent`'s default) tears the child down AFTER the polite
/// `session/cancel` — the handle is removed — while the persisted
/// `acpSessionId` survives, keeping the agent resumable via respawn +
/// the `start_session` resume ladder on the next send.
#[tokio::test]
async fn interrupt_kills_child_for_provider_with_kill_quirk() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-int-quirk"));
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);
    // A live `acpSessionId` keeps the interrupt on the keep-alive path (no
    // session → `interrupt` falls back to the hard kill path anyway).
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-int-quirk")
        .await
        .unwrap();
    assert!(mgr.try_begin(&id, &ws).await);

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    assert!(mgr.interrupt(&id).await, "interrupt finds the live agent");

    // The terminal stream:end is still emitted (the teardown happens after
    // the cancel, before the terminal emits — same shape as keep-alive).
    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    assert!(
        events.iter().any(|e| e.event_type == "agent:stream:end"),
        "interrupt emits the terminal stream:end (got {types:?})"
    );
    // The quirk tore the child down: no live handle survives.
    assert!(
        !mgr.handles.lock().unwrap().contains_key(&id),
        "kills_child_on_interrupt tears the child handle down"
    );
    // The persisted acpSessionId survives the teardown, so the next
    // `agent.sendMessage` respawns the child and resumes the session.
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(
        session.acp_session_id.as_deref(),
        Some("acp-int-quirk"),
        "acpSessionId survives the quirk teardown (agent stays resumable)"
    );
    assert!(!mgr.is_busy(&id), "the in-flight slot was released");
}

/// Sender attribution on preemption: an agent-to-agent interrupt send (the
/// `messageMetadata` carries `fromAgentId`/`fromAgentName`, PROTOCOL §5.5)
/// stamps `interruptedBy: { kind: "agent", agentId, name }` on both the
/// persisted interrupted row and the terminal `agent:stream:end`; a
/// user-origin send stamps `{ kind: "user" }` (covered by the zero-output
/// combined-delivery test above).
#[tokio::test]
async fn preemption_by_agent_sender_stamps_agent_attribution() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-int-attr"));
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-int-attr")
        .await
        .unwrap();
    assert!(mgr.try_begin(&id, &ws).await);
    // Mid-stream turn: the live-turn slot holds a streamed block, so the
    // preemption flushes a NON-empty interrupted row.
    let blocks = vec![json!({ "type": "text", "id": "msg-int-attr:0", "text": "partial…" })];
    mgr.services
        .set_live_turn(&id, "msg-int-attr", blocks.clone());

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let result = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "urgent from sibling".to_string(),
            None,
            super::TurnOptions {
                message_metadata: Some(json!({
                    "fromAgentId": "agent-sender-1",
                    "fromAgentName": "Coordinator",
                })),
                ..super::TurnOptions::default()
            },
        )
        .await
        .expect("interrupt send");
    assert_eq!(result["success"], json!(true));

    let expected_by = json!({
        "kind": "agent",
        "agentId": "agent-sender-1",
        "name": "Coordinator",
    });
    // The persisted interrupted row carries reason + agent attribution.
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    let marker = messages
        .iter()
        .find(|m| m.id == "msg-int-attr")
        .expect("interrupted row persisted");
    let metadata = marker.metadata.as_ref().expect("metadata");
    assert_eq!(metadata["interruptReason"], "preempted_by_message");
    assert_eq!(metadata["interruptedBy"], expected_by);

    // The terminal stream:end mirrors both fields.
    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let end = events
        .iter()
        .find(|e| e.event_type == "agent:stream:end")
        .expect("stream:end event");
    assert_eq!(end.data["interruptReason"], "preempted_by_message");
    assert_eq!(end.data["interruptedBy"], expected_by);
    assert_eq!(end.data["messageId"], "msg-int-attr");
}

/// Regression: the keep-alive interrupt (`agent.stop` mid-turn) must persist
/// the streamed-so-far assistant content instead of dropping it. The live-turn
/// slot is pinned BEFORE the worker abort (the abort drops `LiveTurnGuard`,
/// which would otherwise clear the slot) and flushed after it via
/// `flush_pinned_turn_on_interruption` — same convention as the graceful
/// shutdown flush: the turn's minted message id, `metadata.interrupted = true`
/// + `stopReason = "interrupted"`.
#[tokio::test]
async fn interrupt_flushes_partial_live_turn_as_interrupted_assistant_row() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-int-flush"));
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);
    // An `acpSessionId` keeps the interrupt on the keep-alive path.
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-int-flush")
        .await
        .unwrap();
    assert!(mgr.try_begin(&id, &ws).await);
    // Simulate a mid-stream turn: the live-turn slot holds coalesced blocks.
    let blocks = vec![json!({ "type": "text", "id": "msg-int-flush:0", "text": "partial…" })];
    mgr.services
        .set_live_turn(&id, "msg-int-flush", blocks.clone());

    assert!(mgr.interrupt(&id).await, "interrupt finds the live agent");

    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    assert_eq!(
        messages.len(),
        1,
        "the partial turn persisted: {messages:?}"
    );
    let msg = &messages[0];
    assert_eq!(msg.id, "msg-int-flush");
    assert_eq!(msg.role, "assistant");
    assert_eq!(msg.content, Value::Array(blocks));
    let metadata = msg.metadata.as_ref().expect("metadata");
    assert_eq!(metadata["interrupted"], true);
    assert_eq!(metadata["stopReason"], "interrupted");
    assert_eq!(
        metadata["interruptReason"], "user_stop",
        "plain agent.stop stamps user_stop on the persisted row"
    );
    assert!(
        metadata.get("interruptedBy").is_none(),
        "no sender attribution outside message preemption"
    );
    assert!(
        mgr.services.live_turn(&id).is_none(),
        "live-turn slot cleared after the flush"
    );
}

/// Pre-first-token stop: a plain `agent.stop` landing after the turn started
/// (live-turn slot open) but before any block streamed persists an EMPTY
/// interrupted assistant row (every interruption persists the marker row,
/// empty blocks allowed), and the terminal `agent:stream:end` carries both
/// `stopReason: "interrupted"` and the persisted row's `messageId`.
#[tokio::test]
async fn interrupt_zero_output_persists_empty_interrupted_row_with_message_id() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-int-empty"));
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-int-empty")
        .await
        .unwrap();
    assert!(mgr.try_begin(&id, &ws).await);
    // The turn started (slot open, message id minted) but nothing streamed yet.
    mgr.services.set_live_turn(&id, "msg-int-empty", Vec::new());

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    assert!(mgr.interrupt(&id).await, "interrupt finds the live agent");

    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    assert_eq!(
        messages.len(),
        1,
        "the empty synthetic row persisted: {messages:?}"
    );
    let msg = &messages[0];
    assert_eq!(msg.id, "msg-int-empty");
    assert_eq!(msg.role, "assistant");
    assert_eq!(msg.content, Value::Array(Vec::new()));
    let metadata = msg.metadata.as_ref().expect("metadata");
    assert_eq!(metadata["interrupted"], true);
    assert_eq!(metadata["stopReason"], "interrupted");
    assert_eq!(metadata["interruptReason"], "user_stop");
    assert!(
        mgr.services.live_turn(&id).is_none(),
        "live-turn slot cleared after the flush"
    );

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let end = events
        .iter()
        .find(|e| e.event_type == "agent:stream:end")
        .expect("stream:end event");
    assert_eq!(end.data["stopReason"], "interrupted");
    assert_eq!(end.data["interruptReason"], "user_stop");
    assert_eq!(
        end.data["messageId"], "msg-int-empty",
        "stream:end targets the persisted synthetic row (got {:?})",
        end.data
    );
}

/// Regression companion: the hard `stop()` path (interrupt fallback / kill)
/// flushes the partial live turn the same way as the keep-alive interrupt, so
/// a mid-stream `agent.stop` that lands on the kill path (no `acpSessionId`)
/// still keeps the streamed-so-far content.
#[tokio::test]
async fn stop_flushes_partial_live_turn_as_interrupted_assistant_row() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-stop-flush"));
    seed_agent(&mgr, &ws, &id).await;
    track(&mgr, &id);
    assert!(mgr.try_begin(&id, &ws).await);
    let blocks = vec![json!({ "type": "text", "id": "msg-stop-flush:0", "text": "partial…" })];
    mgr.services
        .set_live_turn(&id, "msg-stop-flush", blocks.clone());

    assert!(mgr.stop(&id).await, "stop finds the live agent");

    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    assert_eq!(
        messages.len(),
        1,
        "the partial turn persisted: {messages:?}"
    );
    let msg = &messages[0];
    assert_eq!(msg.id, "msg-stop-flush");
    assert_eq!(msg.role, "assistant");
    assert_eq!(msg.content, Value::Array(blocks));
    let metadata = msg.metadata.as_ref().expect("metadata");
    assert_eq!(metadata["interrupted"], true);
    assert_eq!(metadata["stopReason"], "interrupted");
    assert_eq!(
        metadata["interruptReason"], "agent_stopped",
        "hard-stop teardown stamps agent_stopped on the persisted row"
    );
}

/// Every interruption persists the marker row, even with ZERO streamed
/// output: a hard `stop()` landing while the live-turn slot is open but
/// empty (turn started, nothing streamed) persists the EMPTY interrupted
/// row stamped `agent_stopped`, with no sender attribution.
#[tokio::test]
async fn stop_with_empty_live_turn_persists_empty_interrupted_row() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-stop-empty"));
    seed_agent(&mgr, &ws, &id).await;
    track(&mgr, &id);
    assert!(mgr.try_begin(&id, &ws).await);
    mgr.services
        .set_live_turn(&id, "msg-stop-empty", Vec::new());

    assert!(mgr.stop(&id).await, "stop finds the live agent");

    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    assert_eq!(
        messages.len(),
        1,
        "empty marker row persisted: {messages:?}"
    );
    let msg = &messages[0];
    assert_eq!(msg.id, "msg-stop-empty");
    assert_eq!(msg.role, "assistant");
    assert_eq!(msg.content, Value::Array(Vec::new()));
    let metadata = msg.metadata.as_ref().expect("metadata");
    assert_eq!(metadata["interrupted"], true);
    assert_eq!(metadata["interruptReason"], "agent_stopped");
    assert!(
        metadata.get("interruptedBy").is_none(),
        "no sender attribution outside message preemption"
    );
}

/// Regression for intent-hq/monorepo#1757: a plain `agent.stop` (keep-alive
/// `UserStop` interrupt) landing on a ZERO-output turn drops the turn's user
/// message provider-side (`session/cancel` discards the in-flight prompt),
/// so the NEXT plain follow-up send must redeliver the stopped message's
/// text AND attachments ahead of the follow-up content in ONE
/// `session/prompt` — same combined-delivery semantics as the
/// interrupt-priority preemption path (monorepo#1014). The transcript is
/// untouched (prompt-only redelivery, no duplicate user rows).
#[tokio::test]
async fn stop_zero_output_then_follow_up_redelivers_stopped_message_and_attachments() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-stop-redeliver"));
    seed_agent(&mgr, &ws, &id).await;
    // Keep-alive semantics under test: use a provider without the
    // `kills_child_on_interrupt` quirk (auggie now kills — monorepo#2763).
    // Env var + spawned_provider alignment keep the follow-up send on the
    // live-child reuse path (no respawn, no resolve_spawn failure).
    let script = mock_agent_script();
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
    set_session_provider(&mgr, &ws, &id, "mock").await;
    let (_agent, log) = track_mock_agent_with_log(&mgr, &id, false);
    mgr.handles
        .lock()
        .unwrap()
        .get_mut(&id)
        .unwrap()
        .spawned_provider = "node".to_string();
    // A live `acpSessionId` keeps the stop on the keep-alive interrupt path.
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-stop-redeliver")
        .await
        .unwrap();
    assert!(mgr.try_begin(&id, &ws).await);
    // The in-flight turn's user message (text + image attachment) is already
    // persisted; the live-turn slot is open with ZERO streamed blocks.
    mgr.services
        .store
        .append_agent_message(
            &id,
            "user",
            &json!([
                { "type": "text", "text": "first with screenshot" },
                { "type": "image", "data": "aGVsbG8=", "mimeType": "image/png" },
            ]),
            &now_iso(),
        )
        .await
        .unwrap();
    mgr.services
        .set_live_turn(&id, "msg-stop-redeliver", Vec::new());

    assert!(
        mgr.interrupt(&id).await,
        "keep-alive stop finds the live agent"
    );
    assert!(
        mgr.handles.lock().unwrap().contains_key(&id),
        "the child handle survives the keep-alive stop"
    );

    // Plain follow-up send (NOT interrupt-priority — the agent is idle now).
    let result = mgr
        .send_message(
            id.clone(),
            ws.clone(),
            "follow-up".to_string(),
            None,
            super::TurnOptions {
                origin: intent_core::MessageOrigin::User,
                ..super::TurnOptions::default()
            },
        )
        .await
        .expect("follow-up send");
    assert_eq!(result["success"], json!(true));
    assert_eq!(
        result["queued"],
        json!(false),
        "idle agent streams: {result}"
    );

    // The follow-up turn's `session/prompt` carries the stopped message's
    // text ahead of the follow-up text, plus its image attachment.
    let mut prompt_params = None;
    for _ in 0..50 {
        {
            let log = log.lock().unwrap();
            if let Some((_, params)) = log.iter().find(|(m, _)| m == "session/prompt") {
                prompt_params = Some(params.clone());
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let params = prompt_params.expect("session/prompt sent for the follow-up turn");
    let blocks = params["prompt"].as_array().expect("prompt blocks");
    let prompt_text = blocks
        .iter()
        .filter(|b| b["type"] == json!("text"))
        .filter_map(|b| b["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let first_pos = prompt_text
        .find("first with screenshot")
        .expect("follow-up prompt redelivers the stopped message's text");
    let follow_pos = prompt_text
        .find("follow-up")
        .expect("follow-up prompt carries its own content");
    assert!(
        first_pos < follow_pos,
        "stopped message precedes the follow-up: {prompt_text:?}"
    );
    assert!(
        blocks
            .iter()
            .any(|b| b["type"] == json!("image") && b["data"] == json!("aGVsbG8=")),
        "follow-up prompt redelivers the stopped message's image attachment: {blocks:?}"
    );

    // Prompt-only redelivery: the transcript keeps exactly the two user rows
    // (stopped message + follow-up) — nothing re-appended.
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    let user_rows: Vec<_> = messages.iter().filter(|m| m.role == "user").collect();
    assert_eq!(
        user_rows.len(),
        2,
        "no duplicate user rows appended: {messages:?}"
    );
}

/// Companion gate: a `agent.stop` landing on a turn that already STREAMED
/// output must NOT redeliver the stopped message on the follow-up turn — the
/// provider saw the message (it produced output for it), so redelivery would
/// duplicate context and risk re-running side effects.
#[tokio::test]
async fn stop_with_streamed_output_does_not_redeliver_on_follow_up() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-stop-progress"));
    seed_agent(&mgr, &ws, &id).await;
    // Keep-alive semantics under test: use a provider without the
    // `kills_child_on_interrupt` quirk (auggie now kills — monorepo#2763).
    // Env var + spawned_provider alignment keep the follow-up send on the
    // live-child reuse path (no respawn, no resolve_spawn failure).
    let script = mock_agent_script();
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
    set_session_provider(&mgr, &ws, &id, "mock").await;
    let (_agent, log) = track_mock_agent_with_log(&mgr, &id, false);
    mgr.handles
        .lock()
        .unwrap()
        .get_mut(&id)
        .unwrap()
        .spawned_provider = "node".to_string();
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-stop-progress")
        .await
        .unwrap();
    assert!(mgr.try_begin(&id, &ws).await);
    mgr.services
        .store
        .append_agent_message(
            &id,
            "user",
            &json!([{ "type": "text", "text": "first with screenshot" }]),
            &now_iso(),
        )
        .await
        .unwrap();
    // The turn streamed a block before the stop → the provider received and
    // acted on the message.
    mgr.services.set_live_turn(
        &id,
        "msg-stop-progress",
        vec![json!({ "type": "text", "id": "msg-stop-progress:0", "text": "partial…" })],
    );

    assert!(
        mgr.interrupt(&id).await,
        "keep-alive stop finds the live agent"
    );

    let result = mgr
        .send_message(
            id.clone(),
            ws.clone(),
            "follow-up".to_string(),
            None,
            super::TurnOptions {
                origin: intent_core::MessageOrigin::User,
                ..super::TurnOptions::default()
            },
        )
        .await
        .expect("follow-up send");
    assert_eq!(
        result["queued"],
        json!(false),
        "idle agent streams: {result}"
    );

    let mut prompt_text = None;
    for _ in 0..50 {
        {
            let log = log.lock().unwrap();
            if let Some((_, params)) = log.iter().find(|(m, _)| m == "session/prompt") {
                let text = params["prompt"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|b| b["type"] == json!("text"))
                    .filter_map(|b| b["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n");
                prompt_text = Some(text);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let prompt_text = prompt_text.expect("session/prompt sent for the follow-up turn");
    assert!(
        !prompt_text.contains("first with screenshot"),
        "a progressed (output-producing) stopped turn is NOT redelivered: {prompt_text:?}"
    );
    assert!(prompt_text.contains("follow-up"));
}

/// Spawn-window fallback: a user stop landing with no `acpSessionId` yet
/// falls back to the hard kill path — the redelivery payload must still be
/// armed (post-`stop`, which clears stale per-agent flags) so the next turn
/// redelivers the stopped message's text + attachments.
#[tokio::test]
async fn stop_fallback_kill_path_arms_redelivery_for_next_turn() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-stop-fb"));
    seed_agent(&mgr, &ws, &id).await;
    track(&mgr, &id);
    // NO acpSessionId → `interrupt` falls back to the kill path.
    assert!(mgr.try_begin(&id, &ws).await);
    mgr.services
        .store
        .append_agent_message(
            &id,
            "user",
            &json!([
                { "type": "text", "text": "first with screenshot" },
                { "type": "image", "data": "aGVsbG8=", "mimeType": "image/png" },
            ]),
            &now_iso(),
        )
        .await
        .unwrap();
    mgr.services.set_live_turn(&id, "msg-stop-fb", Vec::new());

    assert!(mgr.interrupt(&id).await, "fallback stop finds the agent");

    let armed = mgr.stop_redelivery.lock().unwrap().get(&id).cloned();
    let armed = armed.expect("zero-output fallback stop arms the redelivery payload");
    assert_eq!(armed.content.as_deref(), Some("first with screenshot"));
    assert_eq!(
        armed.image_blocks,
        Some(json!([{ "type": "image", "data": "aGVsbG8=", "mimeType": "image/png" }]))
    );
    assert!(armed.file_blocks.is_none());
}

/// A second [`AgentManager`] over the SAME store, standing in for the daemon
/// process that replaces the stopped one in the restart regression tests.
fn restarted_manager(mgr: &AgentManager) -> AgentManager {
    let store = mgr.services.store.clone();
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus));
    AgentManager::new(services, sink, 8)
}

/// Drive a zero-output user stop that arms the redelivery payload (the
/// spawn-window fallback path — no `acpSessionId`), shared by the
/// persistence regression tests below.
async fn arm_redelivery_via_fallback_stop(mgr: &AgentManager, ws: &WorkspaceId, id: &AgentId) {
    seed_agent(mgr, ws, id).await;
    track(mgr, id);
    assert!(mgr.try_begin(id, ws).await);
    mgr.services
        .store
        .append_agent_message(
            id,
            "user",
            &json!([
                { "type": "text", "text": "first with screenshot" },
                { "type": "image", "data": "aGVsbG8=", "mimeType": "image/png" },
            ]),
            &now_iso(),
        )
        .await
        .unwrap();
    mgr.services
        .set_live_turn(id, "msg-stop-persist", Vec::new());
    assert!(mgr.interrupt(id).await, "fallback stop finds the agent");
    assert!(
        mgr.stop_redelivery.lock().unwrap().contains_key(id),
        "zero-output fallback stop arms the redelivery payload"
    );
}

/// Regression for intent-hq/monorepo#1899: the armed zero-output
/// stop-redelivery payload is mirrored to the store, survives a daemon
/// restart (graceful shutdown → fresh manager over the same store →
/// rehydration), and the follow-up turn on the NEW manager still redelivers
/// the stopped message's text + attachments — then clears the persisted row
/// so a second restart cannot redeliver it again.
#[tokio::test]
async fn stop_redelivery_survives_daemon_restart_and_redelivers_once() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-stop-restart"));
    arm_redelivery_via_fallback_stop(&mgr, &ws, &id).await;

    // The arm wrote through to the durable mirror.
    let rows = mgr
        .services
        .store
        .load_all_stop_redeliveries()
        .await
        .expect("load stop redeliveries");
    assert_eq!(rows.len(), 1, "armed payload persisted: {rows:?}");
    assert_eq!(rows[0].agent_id, id);
    assert_eq!(rows[0].payload["content"], json!("first with screenshot"));

    // Graceful daemon shutdown must NOT clear the persisted row.
    mgr.shutdown().await;
    let rows = mgr
        .services
        .store
        .load_all_stop_redeliveries()
        .await
        .expect("load stop redeliveries");
    assert_eq!(
        rows.len(),
        1,
        "graceful shutdown preserves the persisted payload: {rows:?}"
    );

    // "Restart": a fresh manager over the same store rehydrates the payload.
    let mgr2 = Arc::new(restarted_manager(&mgr));
    let rehydrated = mgr2
        .rehydrate_stop_redeliveries()
        .await
        .expect("rehydrate stop redeliveries");
    assert_eq!(rehydrated, 1);
    let armed = mgr2.stop_redelivery.lock().unwrap().get(&id).cloned();
    let armed = armed.expect("rehydrated payload lands in the in-memory map");
    assert_eq!(armed.content.as_deref(), Some("first with screenshot"));
    assert_eq!(
        armed.image_blocks,
        Some(json!([{ "type": "image", "data": "aGVsbG8=", "mimeType": "image/png" }]))
    );

    // Follow-up send on the restarted manager: the prompt redelivers the
    // stopped message's text ahead of the follow-up, plus its attachment.
    let (_agent, log) = track_mock_agent_with_log(&mgr2, &id, false);
    let result = mgr2
        .send_message(
            id.clone(),
            ws.clone(),
            "follow-up".to_string(),
            None,
            super::TurnOptions {
                origin: intent_core::MessageOrigin::User,
                ..super::TurnOptions::default()
            },
        )
        .await
        .expect("follow-up send");
    assert_eq!(
        result["queued"],
        json!(false),
        "idle agent streams: {result}"
    );
    let mut prompt_params = None;
    for _ in 0..50 {
        {
            let log = log.lock().unwrap();
            if let Some((_, params)) = log.iter().find(|(m, _)| m == "session/prompt") {
                prompt_params = Some(params.clone());
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let params = prompt_params.expect("session/prompt sent for the follow-up turn");
    let blocks = params["prompt"].as_array().expect("prompt blocks");
    let prompt_text = blocks
        .iter()
        .filter(|b| b["type"] == json!("text"))
        .filter_map(|b| b["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let first_pos = prompt_text
        .find("first with screenshot")
        .expect("restarted follow-up redelivers the stopped message's text");
    let follow_pos = prompt_text
        .find("follow-up")
        .expect("follow-up prompt carries its own content");
    assert!(
        first_pos < follow_pos,
        "stopped message precedes the follow-up: {prompt_text:?}"
    );
    assert!(
        blocks
            .iter()
            .any(|b| b["type"] == json!("image") && b["data"] == json!("aGVsbG8=")),
        "restarted follow-up redelivers the stopped message's image attachment: {blocks:?}"
    );

    // Consumption clears the durable mirror (async, from the worker task):
    // a restart after the follow-up turn must not redeliver a second time.
    let mut cleared = false;
    for _ in 0..50 {
        let rows = mgr2
            .services
            .store
            .load_all_stop_redeliveries()
            .await
            .expect("load stop redeliveries");
        if rows.is_empty() {
            cleared = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(cleared, "consumed payload's persisted row is cleared");
}

/// Companion gate for intent-hq/monorepo#1899: a hard `agent.stop` landing
/// while a payload is armed drops BOTH the in-memory payload and its durable
/// mirror — a restart after the stop must not resurrect it.
#[tokio::test]
async fn hard_stop_clears_persisted_stop_redelivery() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-stop-clear"));
    arm_redelivery_via_fallback_stop(&mgr, &ws, &id).await;

    // Re-track (the fallback stop removed the handle) and hard-stop.
    track(&mgr, &id);
    assert!(mgr.stop(&id).await);

    assert!(
        !mgr.stop_redelivery.lock().unwrap().contains_key(&id),
        "hard stop drops the in-memory payload"
    );
    let rows = mgr
        .services
        .store
        .load_all_stop_redeliveries()
        .await
        .expect("load stop redeliveries");
    assert!(
        rows.is_empty(),
        "hard stop clears the persisted payload: {rows:?}"
    );
}

/// Regression for the consume-gap race (intent-hq/monorepo#1899 review):
/// `spawn_worker` removes the map entry synchronously but clears the durable
/// row from the spawned (abortable) worker task. A hard stop landing in that
/// gap sees no map entry, so it must sync the store UNCONDITIONALLY — a
/// change-gated sync would skip the clear, leaving a stale row that a later
/// restart rehydrates (double redelivery of an already-consumed message).
#[tokio::test]
async fn hard_stop_clears_stale_persisted_row_without_map_entry() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-stop-gap"));
    arm_redelivery_via_fallback_stop(&mgr, &ws, &id).await;

    // Simulate the consume gap: the map entry is gone (spawn_worker removed
    // it) but the durable row still exists (the worker task's clear never
    // ran because the hard stop aborts it).
    mgr.stop_redelivery.lock().unwrap().remove(&id);
    let rows = mgr
        .services
        .store
        .load_all_stop_redeliveries()
        .await
        .expect("load stop redeliveries");
    assert_eq!(rows.len(), 1, "stale persisted row set up: {rows:?}");

    track(&mgr, &id);
    assert!(mgr.stop(&id).await);

    let rows = mgr
        .services
        .store
        .load_all_stop_redeliveries()
        .await
        .expect("load stop redeliveries");
    assert!(
        rows.is_empty(),
        "hard stop clears the stale persisted row even with no map entry: {rows:?}"
    );
}

/// Shutdown-capture companion: a graceful daemon shutdown landing while the
/// live-turn slot is open but EMPTY persists the empty interrupted row
/// stamped `daemon_shutdown` (every interruption leaves a marker row).
#[tokio::test]
async fn shutdown_with_empty_live_turn_persists_empty_interrupted_row() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (
        WorkspaceId::from("ws-shutdown-empty"),
        AgentId::from("a-shutdown-empty"),
    );
    seed_agent(&mgr, &ws, &id).await;
    track(&mgr, &id);
    assert!(mgr.try_begin(&id, &ws).await);
    mgr.services
        .set_live_turn(&id, "msg-shutdown-empty", Vec::new());

    mgr.shutdown().await;

    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    assert_eq!(
        messages.len(),
        1,
        "empty marker row persisted: {messages:?}"
    );
    let msg = &messages[0];
    assert_eq!(msg.id, "msg-shutdown-empty");
    assert_eq!(msg.role, "assistant");
    assert_eq!(msg.content, Value::Array(Vec::new()));
    let metadata = msg.metadata.as_ref().expect("metadata");
    assert_eq!(metadata["interrupted"], true);
    assert_eq!(metadata["interruptReason"], "daemon_shutdown");
    assert!(
        metadata.get("interruptedBy").is_none(),
        "no sender attribution outside message preemption"
    );
}

/// `agent.sendQueuedMessageNow` on an IDLE agent: the entry is atomically
/// dequeued, the REST of the queue is preserved, the user row persists under
/// the ENTRY id (the RPC result's `messageId`), and the turn starts
/// (`queued: false`).
#[tokio::test]
async fn send_queued_message_now_delivers_entry_and_preserves_rest_of_queue() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-sqmn-idle"));
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);
    let first = mgr
        .services
        .agent_queue_message_op(id.clone(), "first queued".into(), None, None)
        .await
        .expect("queue first");
    let first_id = first["queuedMessage"]["id"].as_str().unwrap().to_string();
    let second = mgr
        .services
        .agent_queue_message_op(id.clone(), "second queued".into(), None, None)
        .await
        .expect("queue second");
    let second_id = second["queuedMessage"]["id"].as_str().unwrap().to_string();

    // Send the SECOND entry now — the first must stay queued.
    let result = mgr
        .send_queued_message_now(id.clone(), ws.clone(), second_id.clone())
        .await
        .expect("send queued now");
    assert_eq!(result["success"], json!(true));
    assert_eq!(result["queued"], json!(false));
    assert_eq!(result["messageId"], json!(second_id));

    let queue = mgr.services.queue_snapshot(&id);
    assert_eq!(queue.len(), 1, "rest of queue preserved: {queue:?}");
    assert_eq!(queue[0]["id"], json!(first_id));

    // The user row persists under the entry id.
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    let row = messages
        .iter()
        .find(|m| m.id == second_id)
        .expect("user row persisted under the entry id");
    assert_eq!(row.role, "user");
    assert!(serde_json::to_string(&row.content)
        .unwrap()
        .contains("second queued"));
}

/// `agent.sendQueuedMessageNow` with an unknown `messageId` is `-32602` with
/// NO side effects: the queue is untouched and no transcript row appears
/// (deliberately NOT idempotent, unlike `agent.removeQueuedMessage`).
#[tokio::test]
async fn send_queued_message_now_not_found_has_no_side_effects() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-sqmn-missing"));
    seed_agent(&mgr, &ws, &id).await;
    mgr.services
        .agent_queue_message_op(id.clone(), "still queued".into(), None, None)
        .await
        .expect("queue");

    let err = mgr
        .send_queued_message_now(id.clone(), ws.clone(), "no-such-entry".into())
        .await
        .expect_err("absent entry must error");
    assert!(
        matches!(err, Error::InvalidParams(ref m) if m.contains("queued message not found")),
        "got {err:?}"
    );
    assert_eq!(mgr.services.queue_snapshot(&id).len(), 1, "queue untouched");
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    assert!(messages.is_empty(), "no transcript row appended");
    assert!(!mgr.is_busy(&id), "no slot claimed");
}

/// `agent.sendQueuedMessageNow` on a BUSY agent preempts the turn keep-alive
/// (same semantics as `agent.sendMessage` with `priority: "interrupt"`): the
/// entry streams immediately (`queued: false`) and the child handle survives
/// — the agent is never killed.
#[tokio::test]
async fn send_queued_message_now_preempts_busy_turn_without_kill() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-sqmn-busy"));
    seed_agent(&mgr, &ws, &id).await;
    // Keep-alive semantics under test: use a provider without the
    // `kills_child_on_interrupt` quirk (auggie now kills — monorepo#2763).
    // Env var + spawned_provider alignment keep the delivered send on the
    // live-child reuse path (no respawn, no resolve_spawn failure).
    let script = mock_agent_script();
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
    set_session_provider(&mgr, &ws, &id, "mock").await;
    let _agent = track_mock_agent(&mgr, &id, false);
    mgr.handles
        .lock()
        .unwrap()
        .get_mut(&id)
        .unwrap()
        .spawned_provider = "node".to_string();
    // A live `acpSessionId` keeps the preemption on the keep-alive interrupt
    // path (no session → the preemption would be skipped).
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-sqmn-busy")
        .await
        .unwrap();
    let queued = mgr
        .services
        .agent_queue_message_op(id.clone(), "urgent queued".into(), None, None)
        .await
        .expect("queue");
    let entry_id = queued["queuedMessage"]["id"].as_str().unwrap().to_string();
    // Claim the in-flight slot so the send sees a busy (mid-turn) agent.
    assert!(mgr.try_begin(&id, &ws).await);

    let result = mgr
        .send_queued_message_now(id.clone(), ws.clone(), entry_id.clone())
        .await
        .expect("send queued now");
    assert_eq!(result["success"], json!(true));
    assert_eq!(
        result["queued"],
        json!(false),
        "preemption streams immediately, never queues: {result}"
    );
    assert_eq!(result["messageId"], json!(entry_id));
    assert!(
        mgr.handles.lock().unwrap().contains_key(&id),
        "the child handle survives the preemption (never killed)"
    );
    assert!(mgr.services.queue_snapshot(&id).is_empty());
}

/// Transactional guarantee: when the slot cannot be claimed (turn startup —
/// busy but no cancellable turn), the dequeued entry is restored at the
/// FRONT of the queue and the RPC reports `queued: true`; the message is
/// never lost.
#[tokio::test]
async fn send_queued_message_now_restores_entry_when_slot_unavailable() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-sqmn-slot"));
    seed_agent(&mgr, &ws, &id).await;
    // Busy WITHOUT a live handle/`acpSessionId`: the turn-startup window
    // where preemption is skipped and `try_begin` fails.
    assert!(mgr.try_begin(&id, &ws).await);
    let other = mgr
        .services
        .agent_queue_message_op(id.clone(), "ahead".into(), None, None)
        .await
        .expect("queue other");
    let other_id = other["queuedMessage"]["id"].as_str().unwrap().to_string();
    let queued = mgr
        .services
        .agent_queue_message_op(id.clone(), "send me now".into(), None, None)
        .await
        .expect("queue target");
    let entry_id = queued["queuedMessage"]["id"].as_str().unwrap().to_string();

    let result = mgr
        .send_queued_message_now(id.clone(), ws.clone(), entry_id.clone())
        .await
        .expect("send queued now");
    assert_eq!(result["success"], json!(true));
    assert_eq!(result["queued"], json!(true), "honest queued outcome");
    assert_eq!(result["queuedMessage"]["id"], json!(entry_id));

    let queue = mgr.services.queue_snapshot(&id);
    assert_eq!(queue.len(), 2, "nothing lost: {queue:?}");
    assert_eq!(
        queue[0]["id"],
        json!(entry_id),
        "restored entry is at the FRONT (next to deliver)"
    );
    assert_eq!(queue[1]["id"], json!(other_id));
}

/// Transactional guarantee: a user-persist failure (duplicate row id)
/// restores the entry at the FRONT of the queue and surfaces the error — the
/// message is never lost and the slot is released.
#[tokio::test]
async fn send_queued_message_now_persist_failure_requeues_front() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-sqmn-persist"));
    seed_agent(&mgr, &ws, &id).await;
    let queued = mgr
        .services
        .agent_queue_message_op(id.clone(), "doomed append".into(), None, None)
        .await
        .expect("queue");
    let entry_id = queued["queuedMessage"]["id"].as_str().unwrap().to_string();
    // Pre-insert a transcript row under the SAME id so the append hits the
    // `agent_message.id` PK and fails.
    mgr.services
        .store
        .append_agent_message_with_id(
            &id,
            &entry_id,
            "user",
            &json!([{ "type": "text", "text": "occupies the id" }]),
            None,
            &now_iso(),
        )
        .await
        .expect("seed conflicting row");

    let err = mgr
        .send_queued_message_now(id.clone(), ws.clone(), entry_id.clone())
        .await
        .expect_err("append failure must surface");
    let _ = err;

    let queue = mgr.services.queue_snapshot(&id);
    assert_eq!(queue.len(), 1, "entry restored, never lost: {queue:?}");
    assert_eq!(queue[0]["id"], json!(entry_id));
    assert!(!mgr.is_busy(&id), "the slot was released");
}

/// monorepo#840 quarantine gate: `send_queued_message_now` on a poisoned
/// session (Error + session-fatal provider block) must NOT redrive — the
/// entry stays in the queue and the result reports
/// `queued: true, quarantined: true`. An unknown entry id on a poisoned
/// session is still `-32602`.
#[tokio::test]
async fn send_queued_message_now_leaves_entry_queued_when_quarantined() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-sqmn-poison"));
    seed_agent(&mgr, &ws, &id).await;
    let queued = mgr
        .services
        .agent_queue_message_op(id.clone(), "parked".into(), None, None)
        .await
        .expect("queue");
    let entry_id = queued["queuedMessage"]["id"].as_str().unwrap().to_string();
    mgr.services
        .store
        .set_agent_session_status(
            &ws,
            &id,
            AgentStatus::Error,
            false,
            &now_iso(),
            Some(Some(
                "The model provider blocked this response for safety reasons. \
                 Please start a new session"
                    .into(),
            )),
        )
        .await
        .expect("park session poisoned");

    let result = mgr
        .send_queued_message_now(id.clone(), ws.clone(), entry_id.clone())
        .await
        .expect("quarantined send-now succeeds as a no-op park");
    assert_eq!(result["success"], json!(true));
    assert_eq!(result["queued"], json!(true), "entry NOT delivered");
    assert_eq!(result["quarantined"], json!(true));
    assert_eq!(result["queuedMessage"]["id"], json!(entry_id));
    assert_eq!(
        mgr.services.queue_snapshot(&id).len(),
        1,
        "entry stays in the queue for agent.retry"
    );
    assert!(!mgr.is_busy(&id), "no slot claim for a poisoned session");

    let err = mgr
        .send_queued_message_now(id.clone(), ws.clone(), "no-such-entry".into())
        .await
        .expect_err("absent entry is still -32602 while quarantined");
    assert!(
        matches!(err, Error::InvalidParams(ref m) if m.contains("queued message not found")),
        "got {err:?}"
    );
}

/// Stale-redrive parity with the drain paths (#576): a delegated agent's
/// entry whose `queued_at` predates the delivered completion report is
/// annotated with the stale-redrive note before delivery.
#[tokio::test]
async fn send_queued_message_now_annotates_stale_redrive() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-sqmn-stale"));
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);
    let queued = mgr
        .services
        .agent_queue_message_op(id.clone(), "queued before report".into(), None, None)
        .await
        .expect("queue");
    let entry_id = queued["queuedMessage"]["id"].as_str().unwrap().to_string();
    // The completion report lands AFTER the enqueue → the entry is stale.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
    session.parent_agent_id = Some(AgentId::from("agent-parent"));
    session.completion_report = Some("work done".to_string());
    session.completion_report_timestamp = Some(now_iso());
    mgr.services
        .store
        .update_agent_session(&ws, &session)
        .await
        .expect("set delegated report");

    let result = mgr
        .send_queued_message_now(id.clone(), ws.clone(), entry_id.clone())
        .await
        .expect("send queued now");
    assert_eq!(result["queued"], json!(false));

    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    let row = messages
        .iter()
        .find(|m| m.id == entry_id)
        .expect("user row persisted under the entry id");
    let text = serde_json::to_string(&row.content).unwrap();
    assert!(
        text.contains("queued before report"),
        "original content preserved: {text}"
    );
    assert!(
        text.contains("was already delivered"),
        "stale-redrive note appended: {text}"
    );
}

/// STAB-114/126 guard, combined-delivery semantics (monorepo#1014): a
/// zero-output interrupt (live-turn slot open but no assistant blocks
/// streamed yet) persists an EMPTY interrupted marker row (stamped
/// `preempted_by_message` + user attribution) but the marker never counts as
/// turn progress — the preempted message is still delivered TOGETHER with the
/// interrupt message in ONE `session/prompt` (original first), leaving the
/// queue empty instead of re-queueing the original behind the interrupt.
#[tokio::test]
async fn interrupt_send_message_zero_output_delivers_combined_prompt() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-int-zero"));
    seed_agent(&mgr, &ws, &id).await;
    // Keep-alive in-place resume under test: use a provider without the
    // `kills_child_on_interrupt` quirk (auggie now kills — monorepo#2763).
    // Env var + spawned_provider alignment keep the follow-up send on the
    // live-child reuse path (no respawn, no resolve_spawn failure).
    let script = mock_agent_script();
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
    set_session_provider(&mgr, &ws, &id, "mock").await;
    let (_agent, log) = track_mock_agent_with_log(&mgr, &id, false);
    mgr.handles
        .lock()
        .unwrap()
        .get_mut(&id)
        .unwrap()
        .spawned_provider = "node".to_string();
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-int-zero")
        .await
        .unwrap();
    assert!(mgr.try_begin(&id, &ws).await);
    // The in-flight turn's user message is already persisted, and its live-turn
    // slot is open with ZERO streamed assistant blocks.
    mgr.services
        .store
        .append_agent_message(
            &id,
            "user",
            &json!([{ "type": "text", "text": "first" }]),
            &now_iso(),
        )
        .await
        .unwrap();
    mgr.services.set_live_turn(&id, "msg-int-zero", Vec::new());

    let result = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "urgent".to_string(),
            None,
            super::TurnOptions {
                origin: intent_core::MessageOrigin::User,
                ..super::TurnOptions::default()
            },
        )
        .await
        .expect("interrupt send");
    assert_eq!(result["success"], json!(true));
    assert_eq!(
        result["queued"],
        json!(false),
        "combined delivery streams immediately: {result}"
    );

    // The preemption persisted the EMPTY interrupted marker row (before the
    // interrupt message's own user row), stamped with the reason + the user
    // sender attribution — and nothing else.
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    let assistant_rows: Vec<_> = messages.iter().filter(|m| m.role == "assistant").collect();
    assert_eq!(
        assistant_rows.len(),
        1,
        "zero-output preemption persists exactly the empty marker row: {messages:?}"
    );
    let marker = assistant_rows[0];
    assert_eq!(marker.id, "msg-int-zero");
    assert_eq!(marker.content, Value::Array(Vec::new()));
    let metadata = marker.metadata.as_ref().expect("metadata");
    assert_eq!(metadata["interrupted"], true);
    assert_eq!(metadata["interruptReason"], "preempted_by_message");
    assert_eq!(metadata["interruptedBy"], json!({ "kind": "user" }));
    // The marker row lands BEFORE the interrupt message's user row.
    let marker_idx = messages
        .iter()
        .position(|m| m.id == "msg-int-zero")
        .unwrap();
    let urgent_idx = messages
        .iter()
        .position(|m| {
            serde_json::to_string(&m.content)
                .unwrap()
                .contains("urgent")
        })
        .expect("interrupt user row persisted");
    assert!(
        marker_idx < urgent_idx,
        "marker row precedes the interrupt message row: {messages:?}"
    );
    // The queue stays EMPTY: the preempted message is delivered in the
    // combined prompt, never re-queued behind the interrupt.
    let queue = mgr.services.queue_snapshot(&id);
    assert!(
        queue.is_empty(),
        "combined delivery leaves the queue empty: {queue:?}"
    );

    // The interrupt turn's `session/prompt` carries BOTH messages in original
    // order: the preempted "first" precedes the interrupt "urgent" within the
    // same text block. Poll the mock call log within a bounded window (the
    // worker sends the prompt asynchronously).
    let mut prompt_text = None;
    for _ in 0..50 {
        {
            let log = log.lock().unwrap();
            if let Some((_, params)) = log.iter().find(|(m, _)| m == "session/prompt") {
                let text = params["prompt"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|b| b["type"] == json!("text"))
                    .filter_map(|b| b["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n");
                prompt_text = Some(text);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let prompt_text = prompt_text.expect("session/prompt sent for the interrupt turn");
    let first_pos = prompt_text
        .find("first")
        .expect("combined prompt carries the preempted message");
    let urgent_pos = prompt_text
        .find("urgent")
        .expect("combined prompt carries the interrupt message");
    assert!(
        first_pos < urgent_pos,
        "preempted message precedes the interrupt message: {prompt_text:?}"
    );
}

/// Race guard on the STAB-114 combined-delivery progress check: the marker
/// row exclusion applies ONLY while the flushed row is actually empty. The
/// zero-output snapshot in `preempt_busy_turn` is taken several awaits before
/// `interrupt_inner` re-reads the live-turn slot, so a first block streaming
/// in that window lands in the flushed row — a NON-empty marker row must
/// count as progress (blocking combined re-delivery), while an empty one
/// must not.
#[test]
fn turn_progress_check_excludes_only_empty_marker_row() {
    fn msg(id: &str, role: &str, content: Value) -> intent_core::AgentMessage {
        intent_core::AgentMessage {
            id: id.into(),
            agent_id: AgentId::from("a-progress"),
            seq: 0,
            role: role.into(),
            content,
            metadata: None,
            app_message_id: None,
            created_at: now_iso(),
        }
    }
    let user = msg("u1", "user", json!([{ "type": "text", "text": "first" }]));
    let marker_id = "marker".to_string();

    // Empty marker row → excluded → no progress (combined delivery fires).
    let empty_marker = msg("marker", "assistant", json!([]));
    assert!(!super::turn_progressed_after(
        &[user.clone(), empty_marker],
        0,
        Some(&marker_id)
    ));

    // NON-empty marker row (a block streamed between the snapshot and the
    // flush) → counts as progress despite the id match.
    let raced_marker = msg(
        "marker",
        "assistant",
        json!([{ "type": "text", "text": "raced-in block" }]),
    );
    assert!(super::turn_progressed_after(
        &[user.clone(), raced_marker],
        0,
        Some(&marker_id)
    ));

    // Nothing after the user row → no progress.
    assert!(!super::turn_progressed_after(&[user], 0, None));
    // Any OTHER non-user row after the last user message is progress, even
    // an empty one whose id does not match the marker.
    let other = msg("a1", "assistant", json!([]));
    assert!(super::turn_progressed_after(
        &[msg("u1", "user", json!([])), other],
        0,
        Some(&marker_id)
    ));
}

/// Agent-sender attribution with an ABSENT name: `fromAgentId` without
/// `fromAgentName` stamps `{ kind: "agent", agentId }` with NO `name` key on
/// row metadata and `stream:end` (benign wire deviation — FE handles it).
#[tokio::test]
async fn preemption_by_agent_sender_without_name_omits_name_field() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-int-noname"));
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-int-noname")
        .await
        .unwrap();
    assert!(mgr.try_begin(&id, &ws).await);
    let blocks = vec![json!({ "type": "text", "id": "msg-int-noname:0", "text": "partial…" })];
    mgr.services.set_live_turn(&id, "msg-int-noname", blocks);

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let result = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "urgent nameless".to_string(),
            None,
            super::TurnOptions {
                message_metadata: Some(json!({ "fromAgentId": "agent-sender-2" })),
                ..super::TurnOptions::default()
            },
        )
        .await
        .expect("interrupt send");
    assert_eq!(result["success"], json!(true));

    let expected_by = json!({ "kind": "agent", "agentId": "agent-sender-2" });
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    let marker = messages
        .iter()
        .find(|m| m.id == "msg-int-noname")
        .expect("interrupted row persisted");
    let metadata = marker.metadata.as_ref().expect("metadata");
    assert_eq!(metadata["interruptedBy"], expected_by);
    assert!(
        metadata["interruptedBy"].get("name").is_none(),
        "no name key when the sender name is unknown: {metadata}"
    );

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let end = events
        .iter()
        .find(|e| e.event_type == "agent:stream:end")
        .expect("stream:end event");
    assert_eq!(end.data["interruptedBy"], expected_by);
}

/// Regression: interrupt-WITH-message preemption must NOT emit the STAB-28
/// synthetic `agent:idle`. The child is not settling — it is about to run the
/// interrupt turn — so an idle emit here would fire completion watches and
/// deliver a spurious "child settled" wake to the parent mid-preemption.
/// Contrast `interrupt_emits_terminal_stream_end_and_idle_when_no_queue`
/// above: the plain `interrupt()` / `agent.stop` path still emits idle so
/// watches fire on a real cancellation. The ready-to-send queue is EMPTY here
/// (the follow-up content is not queued yet at interrupt time), so only the
/// `PreemptedByMessage` reason — not the queue check — prevents the emit.
#[tokio::test]
async fn interrupt_send_message_suppresses_synthetic_idle() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-int-noidle"));
    seed_agent(&mgr, &ws, &id).await;
    // Keep-alive in-place resume under test: use a provider without the
    // `kills_child_on_interrupt` quirk (auggie now kills — monorepo#2763).
    // Env var + spawned_provider alignment keep the follow-up send on the
    // live-child reuse path (no respawn, no resolve_spawn failure).
    let script = mock_agent_script();
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
    set_session_provider(&mgr, &ws, &id, "mock").await;
    let _agent = track_mock_agent(&mgr, &id, false);
    mgr.handles
        .lock()
        .unwrap()
        .get_mut(&id)
        .unwrap()
        .spawned_provider = "node".to_string();
    // Keep the preemption on the keep-alive interrupt path (no session →
    // `interrupt` would fall back to the kill path).
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-int-noidle")
        .await
        .unwrap();
    // Claim the in-flight slot so the send preempts a busy (mid-turn) agent.
    assert!(mgr.try_begin(&id, &ws).await);

    // Prime intent-core's process-wide login-shell PATH capture (OnceLock;
    // on Unix the first use spawns `$SHELL -ilc`, up to 5s — a no-op
    // elsewhere) so the requeued turn's `resolve_spawn` →
    // `find_provider_binary` doesn't eat into the event-collection deadline
    // below. Called directly (not via `find_provider_binary`) so the
    // priming can't short-circuit at an earlier resolution tier (e.g. an
    // installed `~/.augment/bin/auggie`), and via `spawn_blocking` so the
    // capture's blocking poll loop doesn't stall the test runtime's worker
    // thread.
    tokio::task::spawn_blocking(intent_core::path_utils::enhanced_path_dirs)
        .await
        .expect("prime enhanced PATH dirs");

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let result = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "urgent follow-up".to_string(),
            None,
            super::TurnOptions::default(),
        )
        .await
        .expect("interrupt send");
    assert_eq!(result["success"], json!(true));
    assert_eq!(result["queued"], json!(false));

    // Collect until the requeued interrupt turn settles (its terminal
    // `stream_complete` idle) instead of stopping on a fixed quiet gap: the
    // settlement idle is published by the spawned turn worker, so under full
    // parallel test load it can trail the preemption events by more than any
    // fixed gap (monorepo#2007). Everything up to and including settlement is
    // captured, so the suppression assertion below still sees any synthetic
    // `interrupted` idle (that emit would precede settlement). A final short
    // drain keeps the exactly-one-settlement-idle guard meaningful.
    let mut events = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let batch = tokio::time::timeout_at(deadline, sub.recv())
            .await
            .expect("settlement idle within the 10s collection deadline")
            .expect("event bus stays open");
        let settled = batch
            .iter()
            .any(|e| e.event_type == "agent:idle" && e.data["reason"] == json!("stream_complete"));
        events.extend(batch);
        if settled {
            break;
        }
    }
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    assert!(
        types.contains(&"agent:stream:end"),
        "preemption still emits the terminal stream:end (got {types:?})"
    );
    // Split idles by reason: the synthetic `interrupted` idle must be gone,
    // while the interrupt turn (run against the mock agent, which resolves
    // `session/prompt` immediately) still settles with exactly one normal
    // `stream_complete` idle — the parent wake is deferred, not dropped.
    let idle_reasons: Vec<&str> = events
        .iter()
        .filter(|e| e.event_type == "agent:idle")
        .map(|e| e.data["reason"].as_str().unwrap_or(""))
        .collect();
    assert!(
        !idle_reasons.contains(&"interrupted"),
        "interrupt-with-message must NOT emit the synthetic agent:idle — the \
         child is being preempted, not settling (got idles {idle_reasons:?})"
    );
    assert_eq!(
        idle_reasons,
        vec!["stream_complete"],
        "exactly one settlement idle after the interrupt turn completes"
    );
}

/// Interrupt-priority delivery to an IDLE agent falls through to the plain
/// `send_message` path unchanged: `{ success, queued: false, messageId }`.
#[tokio::test]
async fn interrupt_send_message_idle_agent_falls_through_to_send() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-int-idle"));
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-int-idle")
        .await
        .unwrap();

    let result = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "hello".to_string(),
            None,
            super::TurnOptions::default(),
        )
        .await
        .expect("idle interrupt send");
    assert_eq!(result["success"], json!(true));
    assert_eq!(result["queued"], json!(false));
    assert!(result["messageId"].is_string());
    assert!(
        mgr.handles.lock().unwrap().contains_key(&id),
        "idle fall-through never touches the handle"
    );
}

/// The SAME interrupt-priority message (same `messageId`) delivered twice in
/// quick succession preempts exactly once: the duplicate is acknowledged
/// idempotently (`deduplicated: true`) without cancelling the interrupt turn
/// it raced and without re-persisting the message; the child handle survives
/// and the agent never reaches a failed status. A DISTINCT `messageId` is a
/// genuinely new interrupt and still preempts.
#[tokio::test]
async fn duplicate_interrupt_send_same_message_id_preempts_once() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-int-dup"));
    seed_agent(&mgr, &ws, &id).await;
    // Keep-alive semantics under test: use a provider without the
    // `kills_child_on_interrupt` quirk (auggie now kills — monorepo#2763).
    // Env var + spawned_provider alignment keep the delivered sends on the
    // live-child reuse path (no respawn, no resolve_spawn failure).
    let script = mock_agent_script();
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
    set_session_provider(&mgr, &ws, &id, "mock").await;
    let _agent = track_mock_agent(&mgr, &id, false);
    mgr.handles
        .lock()
        .unwrap()
        .get_mut(&id)
        .unwrap()
        .spawned_provider = "node".to_string();
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-int-dup")
        .await
        .unwrap();
    // Claim the in-flight slot so the first delivery preempts a busy turn.
    assert!(mgr.try_begin(&id, &ws).await);

    let first = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "dup-urgent".to_string(),
            Some("user-msg-dup".to_string()),
            super::TurnOptions::default(),
        )
        .await
        .expect("first interrupt send");
    assert_eq!(first["success"], json!(true));
    assert_eq!(first["queued"], json!(false));
    assert_eq!(first["messageId"], json!("user-msg-dup"));

    // Duplicate delivery of the SAME message id, racing the interrupt turn the
    // first delivery just started: acknowledged, no second preemption/persist.
    let second = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "dup-urgent".to_string(),
            Some("user-msg-dup".to_string()),
            super::TurnOptions::default(),
        )
        .await
        .expect("duplicate interrupt send");
    assert_eq!(second["success"], json!(true));
    assert_eq!(
        second["deduplicated"],
        json!(true),
        "duplicate is acknowledged idempotently: {second}"
    );
    assert_eq!(second["messageId"], json!("user-msg-dup"));

    // Not double-persisted: exactly ONE user message carries the content.
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    let dup_count = session
        .messages
        .iter()
        .filter(|m| {
            m.role == "user"
                && serde_json::to_string(&m.content)
                    .unwrap()
                    .contains("dup-urgent")
        })
        .count();
    assert_eq!(dup_count, 1, "duplicate delivery must not double-persist");
    assert!(
        mgr.handles.lock().unwrap().contains_key(&id),
        "the child handle survives the duplicate (never killed)"
    );
    assert_ne!(
        session.status,
        AgentStatus::Error,
        "the agent never transitions to a failed status"
    );

    // A DISTINCT message id is a new interrupt, not a duplicate: it preempts
    // (or claims the idle slot) and persists normally.
    let third = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "next-urgent".to_string(),
            Some("user-msg-next".to_string()),
            super::TurnOptions::default(),
        )
        .await
        .expect("distinct interrupt send");
    assert_eq!(third["success"], json!(true));
    assert!(
        third.get("deduplicated").is_none(),
        "a new messageId is never deduplicated: {third}"
    );
}

/// Interrupt-priority delivery during TURN STARTUP (busy slot claimed but no
/// cancellable turn yet — no `acpSessionId` persisted, the spawn/`session/new`
/// window): preemption is skipped (a keep-alive interrupt is impossible and
/// falling back to `stop` would kill the child) and the message queues behind
/// the starting turn instead. The child handle survives, the starting turn is
/// left intact, and the agent never reaches a failed status.
#[tokio::test]
async fn interrupt_send_during_turn_startup_queues_keep_alive() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-int-startup"));
    seed_agent(&mgr, &ws, &id).await;
    // Child handle live but NO `acpSessionId` yet — `session/new` in flight.
    let _agent = track_mock_agent(&mgr, &id, false);
    assert!(mgr.try_begin(&id, &ws).await);

    let result = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "early interrupt".to_string(),
            Some("user-msg-early".to_string()),
            super::TurnOptions::default(),
        )
        .await
        .expect("startup-window interrupt send");
    assert_eq!(result["success"], json!(true));
    assert_eq!(
        result["queued"],
        json!(true),
        "startup window queues keep-alive instead of preempting: {result}"
    );
    assert_eq!(result["queuedMessage"]["content"], json!("early interrupt"));
    assert!(
        mgr.handles.lock().unwrap().contains_key(&id),
        "the child handle survives (no stop-kill fallback)"
    );
    assert!(
        mgr.is_busy(&id),
        "the starting turn keeps its in-flight slot"
    );
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_ne!(
        session.status,
        AgentStatus::Error,
        "the agent never transitions to a failed status"
    );
}

// --- SP-B: spawn `agent_type` derived from the specialist's `agentType` -------

/// Self-cleaning temp directory for hermetic specialist-file fixtures.
struct TempSpecialistsDir(PathBuf);

impl TempSpecialistsDir {
    fn new() -> Self {
        let dir =
            std::env::temp_dir().join(format!("intentd-spb-specialists-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("create specialists dir");
        Self(dir)
    }

    /// Write `<id>.md` with the given raw markdown-with-frontmatter content.
    fn write(&self, id: &str, content: &str) {
        std::fs::write(self.0.join(format!("{id}.md")), content).expect("write specialist file");
    }
}

impl Drop for TempSpecialistsDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Build a `Services` whose user specialists tier is `dir` (bundled tier left to
/// the env default, which is irrelevant for these ids).
async fn services_with_specialists(dir: &TempSpecialistsDir) -> (TempDb, Services) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let services = Services::new(store).with_specialist_dirs(Some(dir.0.clone()), None);
    (tmp, services)
}

/// Like [`services_with_specialists`] but also wires a writable settings
/// registry (TOML-backed, temp config dir) so a test can `apply()` provider/
/// background-agent settings and see them reflected in resolution.
async fn services_with_specialists_and_registry(
    dir: &TempSpecialistsDir,
) -> (TempDb, Services, tempfile::TempDir) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let config_dir = tempfile::tempdir().expect("temp config dir");
    let registry = Arc::new(
        crate::SettingsRegistry::load(config_dir.path().join("config.toml"))
            .expect("load registry"),
    );
    let services = Services::new(store)
        .with_settings_registry(registry)
        .with_specialist_dirs(Some(dir.0.clone()), None);
    (tmp, services, config_dir)
}

/// An otherwise-empty session carrying just the `specialist` under test.
fn session_with_specialist(specialist: Option<&str>) -> AgentSession {
    AgentSession {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: AgentId::from("agent-spb"),
        workspace_id: WorkspaceId::from("ws-spb"),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: "SpB".to_string(),
        name_explicitly_set: false,
        model: None,
        reasoning_effort: None,
        effort_levels: None,
        provider: None,
        system_prompt: None,
        specialist: specialist.map(str::to_string),
        status: AgentStatus::Pending,
        is_active: false,
        messages: Vec::new(),
        stats: None,
        task_note_id: None,
        skip_auto_commit: false,
        completion_report: None,
        completion_report_timestamp: None,
        attention_request_kind: None,
        attention_request_reason: None,
        attention_request_timestamp: None,
        delegation_depth: None,
        initial_message: None,
        context_references: None,
        image_blocks: None,
        file_blocks: None,
        is_background: false,
        metadata: None,
        created_at: now_iso(),
        updated_at: now_iso(),
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
        pending_delete_at: None,
        retired_at: None,
    }
}

#[tokio::test]
async fn derive_agent_type_uses_specialist_agent_type_and_engages_denylist() {
    use intent_acp::{get_tool_denylist_for_agent_type, SUBAGENT_TOOLS};

    let dir = TempSpecialistsDir::new();
    dir.write(
        "ralph",
        "---\nname: \"Ralph\"\ndescription: \"Loops\"\nagentType: \"ralph-loop\"\n---\n\nYou loop.",
    );
    let (_tmp, services) = services_with_specialists(&dir).await;

    let session = session_with_specialist(Some("ralph"));
    let agent_type = derive_agent_type(&services, &session, None);
    assert_eq!(agent_type, "ralph-loop");

    // The derived type drives the §18.4 denylist: ralph-loop denies the
    // sub-agent orchestration tools (but not the full text-only denylist).
    let denylist = get_tool_denylist_for_agent_type(&agent_type);
    assert!(!denylist.is_empty(), "ralph-loop engages a denylist");
    for tool in SUBAGENT_TOOLS {
        assert!(
            denylist.contains(tool),
            "ralph-loop denylist removes {tool}"
        );
    }
}

#[tokio::test]
async fn derive_agent_type_falls_back_to_default_without_agent_type() {
    use intent_acp::get_tool_denylist_for_agent_type;

    let dir = TempSpecialistsDir::new();
    // A specialist that declares no `agentType` frontmatter.
    dir.write(
        "plain",
        "---\nname: \"Plain\"\ndescription: \"No agentType\"\n---\n\nbody",
    );
    let (_tmp, services) = services_with_specialists(&dir).await;

    let with_specialist = session_with_specialist(Some("plain"));
    assert_eq!(
        derive_agent_type(&services, &with_specialist, None),
        DEFAULT_AGENT_TYPE,
    );

    // A plain agent with no specialist at all keeps the default too.
    let no_specialist = session_with_specialist(None);
    assert_eq!(
        derive_agent_type(&services, &no_specialist, None),
        DEFAULT_AGENT_TYPE,
    );

    // The default (interactive) type is unrestricted — no regression.
    assert!(get_tool_denylist_for_agent_type(DEFAULT_AGENT_TYPE).is_empty());
}

#[tokio::test]
async fn specialist_model_options_lists_only_visible_specialists_with_options() {
    let dir = TempSpecialistsDir::new();
    // Carries options (with and without hints) → listed in order.
    dir.write(
        "chooser",
        "---\nname: \"Chooser\"\ndescription: \"Has options\"\nmodelOptions: [{\"model\":\"opencode:kimi-k3\",\"hint\":\"cheap\"},{\"model\":\"auggie:opus\"}]\n---\n\nbody",
    );
    // Carries options plus a frontmatter `model` on the default provider →
    // the resolved default is reported alongside them.
    let default_provider = intent_providers::first_provider_id();
    dir.write(
        "pinned",
        &format!(
            "---\nname: \"Pinned\"\ndescription: \"Pinned default\"\nmodel: \"{default_provider}:pinned-model\"\nmodelOptions: [{{\"model\":\"opencode:kimi-k3\",\"hint\":\"cheap\"}}]\n---\n\nbody"
        ),
    );
    // No options → omitted.
    dir.write(
        "plain",
        "---\nname: \"Plain\"\ndescription: \"No options\"\n---\n\nbody",
    );
    // Hidden → omitted even though it carries options.
    dir.write(
        "ghost",
        "---\nname: \"Ghost\"\ndescription: \"Hidden\"\nhidden: true\nmodelOptions: [{\"model\":\"grok:grok-5\",\"hint\":\"fast\"}]\n---\n\nbody",
    );
    let (_tmp, services) = services_with_specialists(&dir).await;

    let listed = services.specialist_model_options(None);
    let chooser = listed
        .iter()
        .find(|s| s.specialist == "chooser")
        .expect("chooser listed");
    assert_eq!(chooser.options.len(), 2);
    assert_eq!(chooser.options[0].model, "opencode:kimi-k3");
    assert_eq!(chooser.options[0].hint, "cheap");
    assert_eq!(chooser.options[1].model, "auggie:opus");
    assert_eq!(chooser.options[1].hint, "");
    // No frontmatter model and no configured default → provider CLI default.
    assert_eq!(chooser.default_model, None);
    let pinned = listed
        .iter()
        .find(|s| s.specialist == "pinned")
        .expect("pinned listed");
    assert_eq!(
        pinned.default_model.as_deref(),
        Some(format!("{default_provider}:pinned-model").as_str()),
        "the frontmatter default must be reported as the specialist's default"
    );
    assert!(
        !listed.iter().any(|s| s.specialist == "plain"),
        "specialists without options are omitted"
    );
    assert!(
        !listed.iter().any(|s| s.specialist == "ghost"),
        "hidden specialists are omitted"
    );
}

/// A specialist pinned to another provider via frontmatter `codingAgent`
/// reports the default THAT provider would pin, not the settings-derived
/// default shared by every other specialist (PR #958 review: `agent.delegate`
/// resolves the specialist's own provider override before falling back to the
/// settings-derived provider).
#[tokio::test]
async fn specialist_model_options_default_honors_specialist_coding_agent_override() {
    let dir = TempSpecialistsDir::new();
    let default_provider = intent_providers::first_provider_id();
    // Pinned to a DIFFERENT (non-default) known provider than the settings
    // default, with a provider-default configured for THAT provider only.
    dir.write(
        "opencode-pinned",
        "---\nname: \"OpenCode pinned\"\ndescription: \"Pinned to opencode\"\ncodingAgent: \"opencode\"\nmodelOptions: [{\"model\":\"opencode:kimi-k3\",\"hint\":\"cheap\"}]\n---\n\nbody",
    );
    let (_tmp, services, _cfg) = services_with_specialists_and_registry(&dir).await;
    services
        .settings_registry()
        .expect("registry wired")
        .apply(&[(
            "model.providerDefaults".to_string(),
            json!({ "opencode": "opencode:default-model" }),
        )])
        .expect("set opencode provider default");

    let listed = services.specialist_model_options(None);
    let pinned = listed
        .iter()
        .find(|s| s.specialist == "opencode-pinned")
        .expect("opencode-pinned listed");
    assert_eq!(
        pinned.default_model.as_deref(),
        Some("opencode:default-model"),
        "default must be resolved against the specialist's own codingAgent \
         override ({default_provider} is the settings-derived default, not opencode)"
    );
}

/// monorepo#1729: the quick-action model settings are scoped to single-shot
/// quick actions, so the delegation hint's default must ignore them and show
/// the settings default a real delegate would actually pin.
#[tokio::test]
async fn specialist_model_options_default_ignores_quick_action_settings() {
    let dir = TempSpecialistsDir::new();
    dir.write(
        "chooser",
        "---\nname: \"Chooser\"\ndescription: \"Has options\"\nmodelOptions: [{\"model\":\"opencode:kimi-k3\",\"hint\":\"cheap\"}]\n---\n\nbody",
    );
    let (_tmp, services, _cfg) = services_with_specialists_and_registry(&dir).await;
    services
        .settings_registry()
        .expect("registry wired")
        .apply(&[
            (
                "quickActions.typeOverrides".to_string(),
                json!({ "chooser": "auggie:quick-action-model" }),
            ),
            ("model.default".to_string(), json!("auggie:settings-model")),
        ])
        .expect("set quick-action type override + default model");

    let listed = services.specialist_model_options(None);
    let chooser = listed
        .iter()
        .find(|s| s.specialist == "chooser")
        .expect("chooser listed");
    assert_eq!(
        chooser.default_model.as_deref(),
        Some("auggie:settings-model"),
        "quick-action type override must not apply to a delegated specialist"
    );
}

/// Build a normalized prompt for `session_id` keyed by `request_id`.
fn prompt(request_id: &str, session_id: &str) -> PermissionRequestData {
    PermissionRequestData {
        request_id: request_id.to_string(),
        session_id: session_id.to_string(),
        title: "Write file".to_string(),
        description: None,
        options: vec![PermissionOptionView {
            id: "allow_once".to_string(),
            label: "Allow".to_string(),
            description: None,
            destructive: false,
        }],
        agent_name: "auggie".to_string(),
        risk_level: RiskLevel::High,
        timestamp: 0,
    }
}

#[tokio::test]
async fn default_policy_is_allow_all_and_overridable() {
    let (_tmp, mgr) = manager().await;
    // Shipped default (§6.7/M3.5): reference parity with the TS acp-provider —
    // `start_session` best-effort sets `bypassPermissions` on providers that
    // advertise set-mode, and `AllowAll` auto-approves anything the provider
    // still surfaces.
    assert_eq!(mgr.policy(), PermissionPolicy::AllowAll);
    // `with_policy` selects an FE-driven interactive deployment.
    let (_tmp2, mgr2, _bus) = manager_with_bus().await;
    let mgr2 = mgr2.with_policy(PermissionPolicy::Interactive);
    assert_eq!(mgr2.policy(), PermissionPolicy::Interactive);
}

#[tokio::test]
async fn pending_permissions_snapshots_and_respond_unblocks() {
    let (_tmp, mgr) = manager().await;
    // Register two outstanding prompts directly in the registry the way a
    // surfaced (interactive) prompt would.
    let mut rx = mgr.permissions.register(prompt("perm_1", "agent-a"));
    let _rx2 = mgr.permissions.register(prompt("perm_2", "agent-b"));

    let pending = mgr.pending_permissions();
    assert_eq!(pending.len(), 2);
    assert!(pending.iter().any(|p| p.request_id == "perm_1"));
    assert!(pending.iter().any(|p| p.request_id == "perm_2"));

    // Resolving delivers the outcome to the blocked waiter and drops the prompt.
    assert!(mgr.respond_permission(
        "perm_1",
        PermissionOutcome::Selected {
            option_id: "allow_once".to_string()
        }
    ));
    assert_eq!(
        rx.try_recv().expect("waiter receives the resolved outcome"),
        PermissionOutcome::Selected {
            option_id: "allow_once".to_string()
        }
    );
    assert_eq!(mgr.pending_permissions().len(), 1);

    // A second resolve (or an unknown id) finds nothing outstanding.
    assert!(!mgr.respond_permission("perm_1", PermissionOutcome::Cancelled));
    assert!(!mgr.respond_permission("nope", PermissionOutcome::Cancelled));
}

#[tokio::test]
async fn services_pending_and_respond_rpcs_drive_the_registry() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let manager = Arc::new(AgentManager::new(services.clone(), sink, 8));
    services.attach_agent_manager(&manager);

    let mut rx = manager.permissions.register(prompt("perm_1", "agent-a"));
    let _rx2 = manager.permissions.register(prompt("perm_2", "agent-b"));

    // Unfiltered snapshot returns both prompts as `{ requests: [...] }`.
    let all = services
        .agent_pending_permissions(None)
        .await
        .expect("pending");
    assert_eq!(all["requests"].as_array().unwrap().len(), 2);

    // Filtering by agentId (= sessionId) keeps only that session's prompt.
    let filtered = services
        .agent_pending_permissions(Some(AgentId::from("agent-a")))
        .await
        .expect("pending filtered");
    let reqs = filtered["requests"].as_array().unwrap();
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0]["requestId"], json!("perm_1"));
    assert_eq!(reqs[0]["sessionId"], json!("agent-a"));

    // Resolving over the RPC unblocks the waiter and reports `{ resolved: true }`.
    let resolved = services
        .agent_respond_permission(
            "perm_1".to_string(),
            json!({ "outcome": "selected", "optionId": "allow_once" }),
        )
        .await
        .expect("respond");
    assert_eq!(resolved, json!({ "resolved": true }));
    assert_eq!(
        rx.try_recv().expect("waiter unblocked"),
        PermissionOutcome::Selected {
            option_id: "allow_once".to_string()
        }
    );

    // An unknown request id is `{ resolved: false }`, not an error.
    let missing = services
        .agent_respond_permission("perm_1".to_string(), json!({ "outcome": "cancelled" }))
        .await
        .expect("respond missing");
    assert_eq!(missing, json!({ "resolved": false }));

    // A malformed `outcome` shape is rejected as invalid params.
    let err = services
        .agent_respond_permission("perm_2".to_string(), json!({ "outcome": "approved" }))
        .await
        .expect_err("malformed outcome rejected");
    assert!(matches!(err, Error::InvalidParams(_)));
}

#[tokio::test]
async fn services_permission_rpcs_are_inert_without_a_manager() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    // No AgentManager attached → no registry to consult.
    let services = Services::new(store);

    let pending = services
        .agent_pending_permissions(None)
        .await
        .expect("pending");
    assert_eq!(pending["requests"].as_array().unwrap().len(), 0);

    let resolved = services
        .agent_respond_permission("perm_1".to_string(), json!({ "outcome": "cancelled" }))
        .await
        .expect("respond");
    assert_eq!(resolved, json!({ "resolved": false }));
}

// --- Lifecycle plumbing -------------------------------------------------------

#[tokio::test]
async fn stop_returns_false_for_unknown_agent() {
    let (_tmp, mgr) = manager().await;
    assert!(!mgr.stop(&AgentId::from("missing")).await);
}

/// Insert an additional session row into an existing workspace (companion to
/// [`seed_agent`], which also inserts the workspace and so cannot be called
/// twice with the same `ws`).
async fn insert_extra_session(mgr: &AgentManager, ws: &WorkspaceId, id: &AgentId) {
    let ts = now_iso();
    let session = AgentSession {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: id.clone(),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: "Extra".to_string(),
        name_explicitly_set: false,
        model: None,
        reasoning_effort: None,
        effort_levels: None,
        provider: None,
        system_prompt: None,
        specialist: None,
        status: AgentStatus::Pending,
        is_active: true,
        messages: Vec::new(),
        stats: None,
        task_note_id: None,
        skip_auto_commit: false,
        completion_report: None,
        completion_report_timestamp: None,
        attention_request_kind: None,
        attention_request_reason: None,
        attention_request_timestamp: None,
        delegation_depth: None,
        initial_message: None,
        context_references: None,
        image_blocks: None,
        file_blocks: None,
        is_background: false,
        metadata: None,
        created_at: ts.clone(),
        updated_at: ts,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
        pending_delete_at: None,
        retired_at: None,
    };
    mgr.services
        .store
        .insert_agent_session(&session)
        .await
        .expect("insert extra session");
}

/// `workspace.delete` walks every session in the workspace through
/// `AgentManager::stop`: the tracked handles, workers, in-flight busy set, and
/// `agent_ws` map all drain, and the workspace insert itself is idempotent —
/// a same-slug recreate observes zero pre-existing agents.
#[tokio::test]
async fn delete_workspace_stops_live_agents_and_leaves_no_ghost_state() {
    // Build the manager inline so we can pin a hermetic `workspaces_root` on
    // Services — the delete path walks it to unlink the daemon-owned
    // workspace dir and must never fall through to the real user home.
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store)
        .with_event_bus(bus.clone())
        .with_workspaces_root(tmp.path.with_extension("workspaces"));
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let mgr = Arc::new(AgentManager::new(services.clone(), sink, 8));
    services.attach_agent_manager(&mgr);

    let ws = WorkspaceId::from("ws-delete");
    let live = AgentId::from("a-live");
    let busy = AgentId::from("a-busy-mid-turn");
    // Seed the workspace once (`seed_agent` re-inserts it on every call).
    seed_agent(&mgr, &ws, &live).await;
    insert_extra_session(&mgr, &ws, &busy).await;
    track(&mgr, &live);
    track(&mgr, &busy);
    // Simulate a mid-turn worker: claim the in-flight slot AND register a
    // JoinHandle in the workers map (the two pieces of state `stop` clears).
    assert!(mgr.try_begin(&busy, &ws).await);
    let worker = tokio::spawn(async {
        // A never-ending worker; `stop` must abort it.
        std::future::pending::<()>().await;
    });
    mgr.workers.lock().unwrap().insert(busy.clone(), worker);
    assert!(mgr.is_busy(&busy));
    assert!(mgr.contains(&live) && mgr.contains(&busy));
    assert_eq!(mgr.registry().size(), 2);

    <Services as WorkspaceApi>::delete_workspace(&services, ws.clone())
        .await
        .expect("delete workspace");

    // Every tracked handle is gone; the process registry is empty; the
    // busy set + agent_ws map + workers map all drained.
    assert!(!mgr.contains(&live), "live handle removed");
    assert!(!mgr.contains(&busy), "busy handle removed");
    assert_eq!(mgr.registry().size(), 0, "registry emptied");
    assert!(!mgr.is_busy(&busy), "busy flag cleared");
    assert!(mgr.workers.lock().unwrap().is_empty(), "worker map cleared");
    assert!(mgr.agent_ws.lock().unwrap().is_empty(), "agent_ws cleared");

    // A same-slug recreate finds zero pre-existing agents.
    let store = Store::open(&tmp.path).await.expect("reopen store");
    let ts = now_iso();
    let workspace = Workspace {
        id: ws.clone(),
        title: "WS".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts,
        last_activity: None,
        tags: vec![],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    };
    store
        .insert_workspace(&workspace)
        .await
        .expect("re-insert same-slug workspace");
    let sessions = store
        .list_agent_sessions(&ws)
        .await
        .expect("list on recreated ws");
    assert!(sessions.is_empty(), "recreated workspace shows no ghosts");
}

/// `workspace.archive` gracefully interrupts every in-flight turn in the
/// workspace (the `agent.stop` keep-alive semantics of
/// `AgentManager::interrupt`): the draining worker is aborted and the terminal
/// `agent:stream:end` (`stopReason: "interrupted"`) is emitted — but NOTHING
/// is deleted. The tracked handle (provider child), registry entry, and
/// session row all survive so unarchive can resume the same session, and no
/// `agent:deleted` fires; `workspace:updated` still carries the archive delta.
#[tokio::test]
async fn archive_workspace_interrupts_in_flight_turns_keepalive() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let mgr = Arc::new(AgentManager::new(services.clone(), sink, 8));
    services.attach_agent_manager(&mgr);

    let ws = WorkspaceId::from("ws-archive");
    let id = AgentId::from("a-archive-busy");
    seed_agent(&mgr, &ws, &id).await;
    // Keep-alive semantics under test: use a provider without the
    // `kills_child_on_interrupt` quirk (auggie now kills — monorepo#2763).
    set_session_provider(&mgr, &ws, &id, "mock").await;
    let _agent = track_mock_agent(&mgr, &id, false);
    // An `acpSessionId` is required for the keep-alive interrupt (otherwise
    // `interrupt` falls back to the hard `stop` kill path).
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-archive")
        .await
        .unwrap();
    // Simulate a mid-turn worker: claim the in-flight slot AND register a
    // JoinHandle in the workers map (the interrupt must abort it).
    assert!(mgr.try_begin(&id, &ws).await);
    let worker = tokio::spawn(async {
        std::future::pending::<()>().await;
    });
    mgr.workers.lock().unwrap().insert(id.clone(), worker);

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let archived = <Services as WorkspaceApi>::archive_workspace(&services, ws.clone(), None)
        .await
        .expect("archive workspace");
    assert!(archived.archived, "workspace archived");

    // Drain the published events within a bounded window.
    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    let end = events
        .iter()
        .find(|e| e.event_type == "agent:stream:end")
        .unwrap_or_else(|| panic!("archive interrupt emits terminal stream:end (got {types:?})"));
    assert_eq!(
        end.data["stopReason"], "interrupted",
        "archive interrupt terminal carries stopReason (got {:?})",
        end.data
    );
    assert!(
        !types.contains(&"agent:deleted"),
        "archive deletes nothing (got {types:?})"
    );
    assert!(
        events
            .iter()
            .any(|e| e.event_type == "workspace:updated"
                && e.data["changes"]["archived"] == json!(true)),
        "workspace:updated carries the archive delta (got {types:?})"
    );

    // Keep-alive: the handle + registry entry survive (child stays alive for
    // resume), the in-flight slot and worker are released, and the persisted
    // session row is untouched.
    assert!(mgr.contains(&id), "tracked handle survives archive");
    assert!(
        mgr.registry().is_registered(&id),
        "process stays registered (idle, reapable)"
    );
    assert!(!mgr.is_busy(&id), "in-flight slot released");
    assert!(
        mgr.workers.lock().unwrap().is_empty(),
        "turn worker aborted"
    );
    mgr.services
        .store
        .get_agent_session(&id)
        .await
        .expect("session row preserved");
}

/// Regression (intent-hq/monorepo#1565): an agent archiving its OWN workspace
/// via `ws.workspace.archive` must not be interrupted by the sweep. The
/// caller is mid-turn — blocked awaiting the MCP tool result — so aborting
/// its worker orphans the tool call and leaks the busy slot (the workspace
/// stays `agent_running` forever). Every OTHER in-flight turn is still
/// interrupted keep-alive.
#[tokio::test]
async fn archive_workspace_skips_the_calling_agents_turn() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let mgr = Arc::new(AgentManager::new(services.clone(), sink, 8));
    services.attach_agent_manager(&mgr);

    let ws = WorkspaceId::from("ws-archive-self");
    let caller = AgentId::from("a-archive-caller");
    let other = AgentId::from("a-archive-other");
    seed_agent(&mgr, &ws, &caller).await;
    insert_extra_session(&mgr, &ws, &other).await;
    for id in [&caller, &other] {
        mgr.services
            .store
            .set_acp_session_id(&ws, id, &format!("acp-{}", id.as_str()))
            .await
            .unwrap();
        assert!(mgr.try_begin(id, &ws).await);
        let worker = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        mgr.workers.lock().unwrap().insert(id.clone(), worker);
    }
    let _caller_agent = track_mock_agent(&mgr, &caller, false);
    let _other_agent = track_mock_agent(&mgr, &other, false);

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let archived =
        <Services as WorkspaceApi>::archive_workspace(&services, ws.clone(), Some(caller.clone()))
            .await
            .expect("archive workspace");
    assert!(archived.archived, "workspace archived");

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let interrupted: Vec<&str> = events
        .iter()
        .filter(|e| e.event_type == "agent:stream:end")
        .filter_map(|e| e.data["agentId"].as_str())
        .collect();
    assert_eq!(
        interrupted,
        vec![other.as_str()],
        "only the non-calling agent is interrupted"
    );

    // The caller's turn is untouched: its slot is still held and its worker
    // still registered, so the in-flight MCP dispatch can return and the turn
    // ends normally through its own `end_turn`.
    assert!(mgr.is_busy(&caller), "caller keeps its in-flight slot");
    assert!(
        mgr.workers.lock().unwrap().contains_key(&caller),
        "caller's turn worker survives the sweep"
    );
    assert!(!mgr.is_busy(&other), "other agent's slot released");
    assert!(
        !mgr.workers.lock().unwrap().contains_key(&other),
        "other agent's worker aborted"
    );

    // The caller settles normally once its turn completes — no phantom
    // running agent left behind (the delete-time symptom in #1565).
    mgr.end_turn(&caller).await;
    assert!(!mgr.is_busy(&caller), "caller settles after its turn ends");
    assert!(
        mgr.list_busy().is_empty(),
        "no busy agents remain in the archived workspace"
    );
}

/// Pending queued messages survive `workspace.archive` untouched and are NOT
/// drained into a new turn while the workspace is archived (the archived gate
/// in `try_drain_queue`); `workspace.unarchive` itself kicks the drain and
/// delivers the parked queue — no organic follow-up kick required.
#[tokio::test]
async fn archive_workspace_parks_queue_until_unarchive() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let mgr = Arc::new(AgentManager::new(services.clone(), sink, 8));
    services.attach_agent_manager(&mgr);

    let ws = WorkspaceId::from("ws-archive-queue");
    let id = AgentId::from("a-archive-queued");
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-archive-q")
        .await
        .unwrap();
    // Busy agent with a pending queued message (the queue kick inside
    // `agent_queue_message_op` is a no-op while the slot is held).
    assert!(mgr.try_begin(&id, &ws).await);
    mgr.services
        .agent_queue_message_op(id.clone(), "follow-up".into(), None, None)
        .await
        .expect("queue message");
    assert_eq!(services.queue_snapshot(&id).len(), 1, "message queued");

    <Services as WorkspaceApi>::archive_workspace(&services, ws.clone(), None)
        .await
        .expect("archive workspace");

    // The interrupt released the slot but the queue is intact and undrained.
    assert!(!mgr.is_busy(&id), "interrupt released the slot");
    assert_eq!(
        services.queue_snapshot(&id).len(),
        1,
        "queue survives archive"
    );

    // An explicit drain kick while archived parks (archived gate): no slot
    // claim, no dequeue.
    mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;
    assert!(!mgr.is_busy(&id), "no queue-drain respawn while archived");
    assert_eq!(
        services.queue_snapshot(&id).len(),
        1,
        "queue stays parked while archived"
    );

    // Unarchive itself kicks the drain — the parked queue delivers without
    // any organic follow-up kick.
    <Services as WorkspaceApi>::unarchive_workspace(&services, ws.clone())
        .await
        .expect("unarchive workspace");
    assert!(
        services.queue_snapshot(&id).is_empty(),
        "unarchive's drain kick delivers the parked queue"
    );
}

/// Wake deliveries (`deliver_wake_message` — hook wakes, completion-watch
/// wakes, `agent.wakeOrCreate` context messages) must not start a turn while
/// the workspace is archived: the archived gate parks them in the queue
/// instead of claiming the slot, and unarchive's own drain kick delivers
/// the parked wake.
#[tokio::test]
async fn archive_workspace_parks_wake_deliveries() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let mgr = Arc::new(AgentManager::new(services.clone(), sink, 8));
    services.attach_agent_manager(&mgr);

    let ws = WorkspaceId::from("ws-archive-wake");
    let id = AgentId::from("a-archive-wake");
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);

    <Services as WorkspaceApi>::archive_workspace(&services, ws.clone(), None)
        .await
        .expect("archive workspace");

    // Idle agent + archived workspace: the wake queues instead of spawning
    // a turn.
    let out = services
        .deliver_wake_message(&ws, &id, "[Background hook \"w\"] cancelled", None)
        .await
        .expect("wake delivery");
    assert_eq!(out["queued"], json!(true), "wake parks while archived");
    assert_eq!(
        out["archivedParked"],
        json!(true),
        "archived park is distinguishable from a plain busy-queue fallback"
    );
    assert!(!mgr.is_busy(&id), "no turn spawned while archived");
    assert_eq!(
        services.queue_snapshot(&id).len(),
        1,
        "wake queued behind the archived gate"
    );

    // Unarchive itself kicks the drain and delivers the parked wake.
    <Services as WorkspaceApi>::unarchive_workspace(&services, ws.clone())
        .await
        .expect("unarchive workspace");
    assert!(
        services.queue_snapshot(&id).is_empty(),
        "unarchive's drain kick delivers the parked wake"
    );
}

/// Regression (intent-hq/monorepo#2739): the archived-check → enqueue window
/// in `deliver_wake_message` vs a concurrent `workspace.unarchive`. The
/// unarchive's drain kick fires against the still-empty queue before the
/// gate's enqueue lands, so without a post-enqueue re-check the parked wake
/// strands until the next organic drain trigger. The re-check must self-heal
/// by kicking the drain once it observes the workspace no longer archived
/// (mirroring `AgentManager::send_message`'s archived-gate re-check).
#[tokio::test]
async fn archived_wake_park_self_heals_when_unarchived_during_enqueue() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let park = Arc::new(crate::script_ops::SupervisePark::default());
    let services = Services::new(store)
        .with_event_bus(bus.clone())
        .with_wake_archived_park(park.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let mgr = Arc::new(AgentManager::new(services.clone(), sink, 8));
    services.attach_agent_manager(&mgr);

    let ws = WorkspaceId::from("ws-archive-wake-race");
    let id = AgentId::from("a-archive-wake-race");
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);

    <Services as WorkspaceApi>::archive_workspace(&services, ws.clone(), None)
        .await
        .expect("archive workspace");

    // The wake enters the archived gate and parks in the seam window with
    // the enqueue still pending.
    let deliver = {
        let services = services.clone();
        let (ws, id) = (ws.clone(), id.clone());
        tokio::spawn(async move {
            services
                .deliver_wake_message(&ws, &id, "[Agent Completed] raced wake", None)
                .await
        })
    };
    park.entered.notified().await;

    // The unarchive lands inside the window: its drain kick sees no
    // ready-to-send work (the enqueue has not happened yet) and does
    // nothing — the strand this regression guards against.
    <Services as WorkspaceApi>::unarchive_workspace(&services, ws.clone())
        .await
        .expect("unarchive workspace");
    assert!(
        services.queue_snapshot(&id).is_empty(),
        "unarchive's drain kick fired against a still-empty queue"
    );

    // Release the enqueue; the post-enqueue re-check observes the workspace
    // no longer archived and kicks the drain itself.
    park.release.notify_one();
    let out = deliver
        .await
        .expect("join wake delivery")
        .expect("wake delivery");
    assert_eq!(out["queued"], json!(true), "wake parked behind the gate");
    assert_eq!(out["archivedParked"], json!(true), "archived park marker");

    timeout(Duration::from_secs(10), async {
        loop {
            if services.queue_snapshot(&id).is_empty() && !services.has_ready_to_send(&id) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("re-check drain delivers the raced wake without an organic kick");
}

/// Soft-retire wake gate: `deliver_wake_message` must not start a turn on a
/// retired session — hook dispatches, PR-monitor wakes, and batch-delegate
/// advisory wakes can all still target one (retiring cancels neither hooks
/// nor monitors). An idle retired agent parks the wake in the queue
/// (`retiredParked`), and `agent.restore`'s own drain kick delivers it.
#[tokio::test]
async fn retired_session_parks_wake_deliveries_until_restore() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let mgr = Arc::new(AgentManager::new(services.clone(), sink, 8));
    services.attach_agent_manager(&mgr);

    let ws = WorkspaceId::from("ws-retired-wake");
    let id = AgentId::from("a-retired-wake");
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);

    services
        .agent_retire_op(id.clone(), Some(ws.clone()), None)
        .await
        .expect("retire");

    // Idle agent + retired session: the wake queues instead of spawning a
    // turn.
    let out = services
        .deliver_wake_message(&ws, &id, "[Background hook \"w\"] fired", None)
        .await
        .expect("wake delivery");
    assert_eq!(out["queued"], json!(true), "wake parks while retired");
    assert_eq!(
        out["retiredParked"],
        json!(true),
        "retired park is distinguishable from a plain busy-queue fallback"
    );
    assert!(!mgr.is_busy(&id), "no turn spawned while retired");
    assert_eq!(
        services.queue_snapshot(&id).len(),
        1,
        "wake queued behind the retired gate"
    );

    // Restore itself kicks the drain and delivers the parked wake.
    services
        .agent_restore_op(id.clone(), Some(ws.clone()))
        .await
        .expect("restore");
    timeout(Duration::from_secs(10), async {
        loop {
            if services.queue_snapshot(&id).is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("restore's drain kick delivers the parked wake");
}

/// Regression (intent-hq/monorepo#2732): an AUTOMATIC `send_message` into an
/// archived workspace must park in the queue — NOT claim the slot (whose
/// `try_begin` would auto-unarchive the workspace). The workspace stays
/// Archived with no `autoUnarchive` delta, and unarchive's own drain kick
/// delivers the parked message.
#[tokio::test]
async fn archived_workspace_parks_automatic_send_until_unarchive() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let mgr = Arc::new(AgentManager::new(services.clone(), sink, 8));
    services.attach_agent_manager(&mgr);

    let ws = WorkspaceId::from("ws-archive-auto-send");
    let id = AgentId::from("a-archive-auto-send");
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);

    <Services as WorkspaceApi>::archive_workspace(&services, ws.clone(), None)
        .await
        .expect("archive workspace");

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let result = mgr
        .send_message(
            id.clone(),
            ws.clone(),
            "automatic delivery".to_string(),
            None,
            super::TurnOptions::default(), // origin defaults to Automatic
        )
        .await
        .expect("automatic send parks");
    assert_eq!(
        result["queued"],
        json!(true),
        "parked, not driven: {result}"
    );
    assert_eq!(
        result["archivedParked"],
        json!(true),
        "archived park is distinguishable from a plain busy-queue fallback"
    );
    assert!(!mgr.is_busy(&id), "no slot claim while archived");
    assert_eq!(
        services.queue_snapshot(&id).len(),
        1,
        "message parked behind the archived gate"
    );

    // The workspace row stays Archived — the send never auto-unarchived it.
    let row = services.store.get_workspace(&ws).await.unwrap();
    assert!(row.archived, "workspace stays archived");
    assert_eq!(row.status, WorkspaceStatus::Archived);

    // No workspace:updated unarchive delta was published.
    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    assert!(
        !events.iter().any(|e| e.event_type == "workspace:updated"
            && e.data["changes"]["archived"] == json!(false)),
        "no auto-unarchive delta for an automatic delivery"
    );

    // Unarchive itself kicks the drain and delivers the parked message.
    <Services as WorkspaceApi>::unarchive_workspace(&services, ws.clone())
        .await
        .expect("unarchive workspace");
    assert!(
        services.queue_snapshot(&id).is_empty(),
        "unarchive's drain kick delivers the parked message"
    );
}

/// Regression (intent-hq/monorepo#2732) — the auto-unarchive loop: an
/// internal parent wake (`deliver_parent_wake`, the completion-watch /
/// event-subscription wake path) into an archived workspace parks in the
/// parent's queue instead of starting a turn that flips the workspace
/// straight back to Active.
#[tokio::test]
async fn archived_workspace_parks_internal_parent_wake() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let mgr = Arc::new(AgentManager::new(services.clone(), sink, 8));
    services.attach_agent_manager(&mgr);

    let ws = WorkspaceId::from("ws-archive-parent-wake");
    let parent = AgentId::from("a-archive-parent");
    seed_agent(&mgr, &ws, &parent).await;
    let _agent = track_mock_agent(&mgr, &parent, false);

    <Services as WorkspaceApi>::archive_workspace(&services, ws.clone(), None)
        .await
        .expect("archive workspace");

    let out = services
        .deliver_parent_wake(
            &ws,
            parent.clone(),
            "[Agent Completed] child settled".to_string(),
            None,
        )
        .await
        .expect("parent wake delivery");
    assert_eq!(out["queued"], json!(true), "wake parks while archived");
    assert_eq!(out["archivedParked"], json!(true), "archived park marker");
    assert!(!mgr.is_busy(&parent), "no turn spawned while archived");
    assert_eq!(
        services.queue_snapshot(&parent).len(),
        1,
        "wake queued behind the archived gate"
    );
    let row = services.store.get_workspace(&ws).await.unwrap();
    assert!(row.archived, "workspace stays archived after the wake");
}

/// Regression (intent-hq/monorepo#2732): an AUTOMATIC interrupt-priority
/// delivery (`interrupt_send_message`) into an archived workspace parks
/// front-of-queue instead of preempting/driving a turn; the workspace stays
/// Archived.
#[tokio::test]
async fn archived_workspace_parks_automatic_interrupt_send_front_of_queue() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let mgr = Arc::new(AgentManager::new(services.clone(), sink, 8));
    services.attach_agent_manager(&mgr);

    let ws = WorkspaceId::from("ws-archive-int-send");
    let id = AgentId::from("a-archive-int-send");
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);

    <Services as WorkspaceApi>::archive_workspace(&services, ws.clone(), None)
        .await
        .expect("archive workspace");

    // A normal automatic entry parked first, so the interrupt's
    // front-of-queue ordering is observable.
    let first = mgr
        .send_message(
            id.clone(),
            ws.clone(),
            "normal automatic".to_string(),
            None,
            super::TurnOptions::default(),
        )
        .await
        .expect("normal automatic send parks");
    assert_eq!(first["queued"], json!(true));

    let result = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "automatic interrupt".to_string(),
            None,
            super::TurnOptions::default(),
        )
        .await
        .expect("automatic interrupt parks");
    assert_eq!(result["queued"], json!(true), "parked: {result}");
    assert_eq!(
        result["archivedParked"],
        json!(true),
        "archived park marker"
    );
    assert!(!mgr.is_busy(&id), "no turn spawned while archived");

    let queue = services.queue_snapshot(&id);
    assert_eq!(queue.len(), 2, "both entries parked");
    assert_eq!(
        queue[0]["content"],
        json!("automatic interrupt"),
        "interrupt parks front-of-queue"
    );
    let row = services.store.get_workspace(&ws).await.unwrap();
    assert!(row.archived, "workspace stays archived");
}

/// Guard the revive path (intent-hq/monorepo#2732 non-goal): a USER-origin
/// `send_message` into an archived workspace still claims the slot and
/// auto-unarchives — only automatic deliveries park.
#[tokio::test]
async fn archived_workspace_user_send_still_auto_unarchives() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let mgr = Arc::new(AgentManager::new(services.clone(), sink, 8));
    services.attach_agent_manager(&mgr);

    let ws = WorkspaceId::from("ws-archive-user-send");
    let id = AgentId::from("a-archive-user-send");
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);

    <Services as WorkspaceApi>::archive_workspace(&services, ws.clone(), None)
        .await
        .expect("archive workspace");

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let result = mgr
        .send_message(
            id.clone(),
            ws.clone(),
            "user revive".to_string(),
            None,
            super::TurnOptions {
                origin: intent_core::MessageOrigin::User,
                ..super::TurnOptions::default()
            },
        )
        .await
        .expect("user send drives a turn");
    assert_eq!(result["queued"], json!(false), "direct send: {result}");

    let row = services.store.get_workspace(&ws).await.unwrap();
    assert!(!row.archived, "user send auto-unarchived the workspace");
    assert_eq!(row.status, WorkspaceStatus::Active);

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    assert!(
        events.iter().any(|e| e.event_type == "workspace:updated"
            && e.data["changes"]["autoUnarchive"]["reason"] == json!("agent_activity")),
        "stamped auto-unarchive delta published"
    );
}

/// Cross-workspace watchers are unaffected (intent-hq/monorepo#2732): the
/// archived gate keys on the workspace the message is DELIVERED in (the
/// target's home workspace), so a parent whose home workspace is Active
/// receives its wake immediately even when the watched child's workspace is
/// archived.
#[tokio::test]
async fn cross_workspace_parent_wake_unaffected_by_archived_child_workspace() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let mgr = Arc::new(AgentManager::new(services.clone(), sink, 8));
    services.attach_agent_manager(&mgr);

    // Parent home workspace (Active) and the child's workspace (Archived).
    let parent_ws = WorkspaceId::from("ws-cross-parent");
    let parent = AgentId::from("a-cross-parent");
    seed_agent(&mgr, &parent_ws, &parent).await;
    let _parent_agent = track_mock_agent(&mgr, &parent, false);

    let child_ws = WorkspaceId::from("ws-cross-child");
    let child = AgentId::from("a-cross-child");
    seed_agent(&mgr, &child_ws, &child).await;
    <Services as WorkspaceApi>::archive_workspace(&services, child_ws.clone(), None)
        .await
        .expect("archive the child's workspace");

    // The wake is delivered in the PARENT's home workspace (Active): it
    // drives a turn immediately instead of parking.
    let out = services
        .deliver_parent_wake(
            &parent_ws,
            parent.clone(),
            "[Agent Completed] cross-workspace child settled".to_string(),
            None,
        )
        .await
        .expect("cross-workspace wake delivery");
    assert_eq!(
        out["queued"],
        json!(false),
        "wake delivered immediately in the Active home workspace: {out}"
    );
    assert!(
        services.queue_snapshot(&parent).is_empty(),
        "nothing parked for the cross-workspace watcher"
    );
}

/// `stop` drops any pending `recreated` flag so a stale resend bit cannot
/// survive a teardown into a future spawn (parity with the `recreated` doc on
/// `AgentManager`).
#[tokio::test]
async fn stop_clears_pending_recreate_flag() {
    let (_tmp, mgr) = manager().await;
    let id = AgentId::from("a-stop-recreate");
    track(&mgr, &id);
    mgr.recreated.lock().unwrap().insert(id.clone());

    assert!(mgr.stop(&id).await);
    assert!(
        !mgr.recreated.lock().unwrap().contains(&id),
        "stop wipes the resend flag",
    );
}

#[tokio::test]
async fn is_busy_reflects_try_begin_and_end_turn() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-busy"), AgentId::from("a-busy"));
    assert!(!mgr.is_busy(&id), "fresh agent is not busy");
    assert!(mgr.try_begin(&id, &ws).await);
    assert!(mgr.is_busy(&id), "claim flips busy on");
    // Second `try_begin` is rejected — single-flight per agent (§5.5).
    assert!(
        !mgr.try_begin(&id, &ws).await,
        "single-flight rejects 2nd claim"
    );
    mgr.end_turn(&id).await;
    assert!(!mgr.is_busy(&id), "release flips busy off");
}

#[tokio::test]
async fn list_busy_reports_only_claimed_agents_with_their_workspace() {
    let (_tmp, mgr) = manager().await;
    let ws = WorkspaceId::from("ws-list-busy");
    let id = AgentId::from("agent-list-busy");

    assert!(
        mgr.list_busy().is_empty(),
        "fresh manager has no busy agents"
    );
    assert!(mgr.try_begin(&id, &ws).await);
    assert_eq!(mgr.list_busy(), vec![(id.clone(), ws)]);

    mgr.end_turn(&id).await;
    assert!(
        mgr.list_busy().is_empty(),
        "released agent is no longer listed"
    );
}

#[tokio::test]
async fn list_active_projects_busy_agent_with_workspace_and_epoch_timestamp() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus));
    let mgr = Arc::new(AgentManager::new(services.clone(), sink, 8));
    services.attach_agent_manager(&mgr);
    let ws = WorkspaceId::from("ws-list-active");
    let id = AgentId::from("agent-list-active");
    seed_agent(&mgr, &ws, &id).await;

    assert_eq!(
        services.agent_list_active_op().await.unwrap(),
        json!({ "streams": [] }),
        "idle agents are excluded"
    );

    assert!(mgr.try_begin(&id, &ws).await);
    let active = services.agent_list_active_op().await.unwrap();
    let streams = active["streams"].as_array().expect("streams array");
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0]["agentId"], json!(id));
    assert_eq!(streams[0]["sessionId"], json!(id));
    assert_eq!(streams[0]["workspaceId"], json!(ws));
    assert!(
        streams[0]["startTime"].as_i64().is_some_and(|ms| ms > 0),
        "updatedAt is converted to epoch milliseconds: {active}"
    );

    mgr.end_turn(&id).await;
    assert_eq!(
        services.agent_list_active_op().await.unwrap(),
        json!({ "streams": [] }),
        "settled agents are excluded"
    );
}

/// A busy agent whose session row is missing (e.g. deleted mid-turn by a
/// concurrent `agent.delete`) is skipped instead of failing the whole
/// `agent.listActive` response (PR #881 review).
#[tokio::test]
async fn list_active_skips_busy_agent_with_missing_session_row() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus));
    let mgr = Arc::new(AgentManager::new(services.clone(), sink, 8));
    services.attach_agent_manager(&mgr);
    let ws = WorkspaceId::from("ws-list-active-missing");
    let other_ws = WorkspaceId::from("ws-list-active-missing-2");
    let survivor = AgentId::from("agent-active-survivor");
    let deleted = AgentId::from("agent-active-deleted");
    seed_agent(&mgr, &ws, &survivor).await;
    seed_agent(&mgr, &other_ws, &deleted).await;

    assert!(mgr.try_begin(&survivor, &ws).await);
    assert!(mgr.try_begin(&deleted, &other_ws).await);
    // Simulate a concurrent agent.delete racing the busy snapshot: the row is
    // gone but the manager still lists the agent as busy.
    mgr.services
        .store
        .delete_agent_session(&other_ws, &deleted)
        .await
        .expect("delete session row");

    let active = services.agent_list_active_op().await.unwrap();
    let streams = active["streams"].as_array().expect("streams array");
    assert_eq!(
        streams.len(),
        1,
        "missing-row agent is skipped, not an endpoint error: {active}"
    );
    assert_eq!(streams[0]["agentId"], json!(survivor));
}

/// `try_begin` persists the runtime `Active` transition and publishes the
/// self-sufficient `agent:status-changed` event so a hydrated client reflects
/// the live runtime rather than the stored `Pending` placeholder.
#[tokio::test]
async fn try_begin_persists_active_status_and_emits_event() {
    use intent_core::events::AGENT_STATUS_CHANGED;
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let (ws, id) = (WorkspaceId::from("ws-begin"), AgentId::from("a-begin"));
    seed_agent(&mgr, &ws, &id).await;

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    assert!(mgr.try_begin(&id, &ws).await);

    let stored = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(stored.status, AgentStatus::Active);
    assert!(stored.is_active);

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let status_event = events
        .iter()
        .find(|e| e.event_type == AGENT_STATUS_CHANGED)
        .expect("agent:status-changed published");
    assert_eq!(status_event.data["status"], json!("active"));
    assert_eq!(status_event.data["isActive"], json!(true));
}

/// `end_turn` persists the `RuntimeIdle` transition and publishes
/// `agent:status-changed`. A no-op `end_turn` on an agent that was never busy
/// neither writes nor emits.
#[tokio::test]
async fn end_turn_persists_runtime_idle_and_emits_event() {
    use intent_core::events::AGENT_STATUS_CHANGED;
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let (ws, id) = (WorkspaceId::from("ws-end"), AgentId::from("a-end"));
    seed_agent(&mgr, &ws, &id).await;
    assert!(mgr.try_begin(&id, &ws).await);

    // Subscribe AFTER `try_begin` so we only capture the `end_turn` emission.
    let mut sub = bus.subscribe(SubscriptionFilter::default());
    mgr.end_turn(&id).await;

    let stored = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(stored.status, AgentStatus::RuntimeIdle);
    assert!(!stored.is_active);

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let status_event = events
        .iter()
        .find(|e| e.event_type == AGENT_STATUS_CHANGED)
        .expect("agent:status-changed published on end_turn");
    assert_eq!(status_event.data["status"], json!("idle"));
    assert_eq!(status_event.data["isActive"], json!(false));

    // Calling `end_turn` again on an already-idle agent is a no-op.
    mgr.end_turn(&id).await;
    assert!(!mgr.is_busy(&id));
}

#[tokio::test]
async fn interrupt_returns_false_for_unknown_agent() {
    let (_tmp, mgr) = manager().await;
    assert!(
        !mgr.interrupt(&AgentId::from("nope")).await,
        "no handle → fall through to stop, which reports no removal",
    );
}

/// Without an `acpSessionId` to cancel, `interrupt` falls back to the hard
/// `stop` path (which still tears the handle down).
#[tokio::test]
async fn interrupt_falls_back_to_stop_without_acp_session_id() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-int-fb"), AgentId::from("a-int-fb"));
    seed_agent(&mgr, &ws, &id).await;
    // Track the handle but leave `acp_session_id` unset.
    track(&mgr, &id);

    assert!(
        mgr.interrupt(&id).await,
        "stop fallback reports the removal"
    );
    assert!(!mgr.contains(&id), "fallback tore the handle down");
}

// --- Queue + drain ------------------------------------------------------------

#[tokio::test]
async fn try_drain_queue_no_op_when_already_busy() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-drain"), AgentId::from("a-drain"));
    // Queue a ready message so the only barrier is the busy flag.
    mgr.services
        .enqueue_message(&id, "queued".to_string(), None, None, None, None, false);
    assert!(mgr.try_begin(&id, &ws).await);

    mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;

    // Queue is left untouched (no dequeue happened) and the slot stays held.
    assert_eq!(mgr.services.queue_snapshot(&id).len(), 1);
    assert!(mgr.is_busy(&id));
}

#[tokio::test]
async fn try_drain_queue_no_op_without_ready_messages() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-empty"), AgentId::from("a-empty"));

    mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;

    assert!(
        !mgr.is_busy(&id),
        "no slot claim without ready-to-send work"
    );
    assert_eq!(mgr.services.queue_snapshot(&id).len(), 0);
}

/// STAB-52 regression: a session parked in `Error` must NOT be auto-redriven
/// by the self-drain path. The terminal-failure handler requeues the failed
/// message and persists `Error`, so any queue kick (queueMessage, edit-save,
/// wake delivery) that reached `try_drain_queue` used to re-claim the slot and
/// re-spawn the failing turn in a crash-loop, flapping `is_active`.
#[tokio::test]
async fn try_drain_queue_skips_agent_parked_in_error() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-err"), AgentId::from("a-err"));
    seed_agent(&mgr, &ws, &id).await;
    mgr.services
        .store
        .set_agent_session_status(&ws, &id, AgentStatus::Error, false, &now_iso(), None)
        .await
        .expect("park session in error");
    // A ready-to-send message is waiting (the terminal-failure requeue).
    mgr.services
        .enqueue_message(&id, "requeued".to_string(), None, None, None, None, false);

    mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;

    assert!(!mgr.is_busy(&id), "no slot claim while parked in error");
    assert!(
        mgr.workers.lock().unwrap().is_empty(),
        "no worker spawned for an errored session"
    );
    assert_eq!(
        mgr.services.queue_snapshot(&id).len(),
        1,
        "the message stays queued for agent.retry"
    );
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(session.status, AgentStatus::Error, "error status untouched");
    assert!(!session.is_active, "is_active stays 0");
}

/// monorepo#840 quarantine gate: `send_message` to a poisoned session (Error
/// + session-fatal provider block) parks the message in the queue —
/// `queued: true, quarantined: true` — without claiming the slot, spawning a
/// worker, or touching the Error status. `agent.retry` stays the redrive.
#[tokio::test]
async fn send_message_parks_message_for_poisoned_session() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-poison"), AgentId::from("a-poison"));
    seed_agent(&mgr, &ws, &id).await;
    mgr.services
        .store
        .set_agent_session_status(
            &ws,
            &id,
            AgentStatus::Error,
            false,
            &now_iso(),
            Some(Some(
                "The model provider blocked this response for safety reasons. \
                 Please start a new session"
                    .into(),
            )),
        )
        .await
        .expect("park session poisoned");

    let result = mgr
        .send_message(
            id.clone(),
            ws.clone(),
            "follow-up".to_string(),
            None,
            super::TurnOptions::default(),
        )
        .await
        .expect("quarantined send succeeds as a queue park");
    assert_eq!(result["success"], json!(true));
    assert_eq!(result["queued"], json!(true), "message parked, not driven");
    assert_eq!(result["quarantined"], json!(true));

    assert!(!mgr.is_busy(&id), "no slot claim for a poisoned session");
    assert!(
        mgr.workers.lock().unwrap().is_empty(),
        "no worker spawned for a poisoned session"
    );
    assert_eq!(
        mgr.services.queue_snapshot(&id).len(),
        1,
        "the message waits in the queue for agent.retry"
    );
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(session.status, AgentStatus::Error, "error status untouched");
    assert!(!session.is_active, "is_active stays 0");
}

/// Wire surface (monorepo#1022): every `agent.sendMessage` response arm
/// carries the turn correlation id. Queued arm: `turnId` matches the queue
/// entry's; quarantined arm likewise. The FE keys its retry record off this
/// value at send time.
#[tokio::test]
async fn send_message_queued_response_carries_turn_id() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-tid-q"), AgentId::from("a-tid-q"));
    seed_agent(&mgr, &ws, &id).await;
    // Claim the slot so the send queues behind the "running" turn.
    assert!(mgr.try_begin(&id, &ws).await);

    let result = mgr
        .send_message(
            id.clone(),
            ws.clone(),
            "queued".to_string(),
            None,
            super::TurnOptions::default(),
        )
        .await
        .expect("busy send queues");
    assert_eq!(result["queued"], json!(true));
    assert!(result["turnId"].is_string(), "turnId present: {result}");
    assert_eq!(
        result["turnId"], result["queuedMessage"]["turnId"],
        "response turnId matches the queue entry's"
    );
}

/// Wire surface (monorepo#1022): the direct (`queued: false`) `send_message`
/// arm returns the minted `turnId` and stamps the same id on the user-row
/// `agent:message` echo. Uses the deterministic mock agent for a real turn.
#[tokio::test]
async fn send_message_direct_response_and_user_echo_carry_turn_id() {
    let script = mock_agent_script();
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-tid-d"), AgentId::from("a-tid-d"));
    seed_agent(&mgr, &ws, &id).await;
    let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
    session.provider = Some("mock".to_string());
    mgr.services
        .store
        .update_agent_session(&ws, &session)
        .await
        .expect("set mock provider");

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let result = mgr
        .send_message(
            id.clone(),
            ws.clone(),
            "direct".to_string(),
            None,
            super::TurnOptions::default(),
        )
        .await
        .expect("direct send");
    assert_eq!(result["queued"], json!(false));
    let turn_id = result["turnId"]
        .as_str()
        .expect("direct response carries turnId")
        .to_string();

    // Wait for the turn to finish so the worker exits cleanly.
    timeout(Duration::from_secs(10), async {
        loop {
            if !mgr.is_busy(&id) && mgr.workers.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("direct turn completes");

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let echo = events
        .iter()
        .find(|e| {
            e.event_type == intent_core::events::AGENT_MESSAGE && e.data["role"] == json!("user")
        })
        .expect("user-row agent:message echo");
    assert_eq!(
        echo.data["turnId"],
        json!(turn_id),
        "user-row echo carries the send's turnId: {:?}",
        echo.data
    );
}

/// Wire surface (monorepo#1022): a redriving `agent.retry` response carries
/// the requeued entry's original `turnId`; a non-redriving retry (empty
/// queue) omits the field.
#[tokio::test]
async fn agent_retry_response_carries_redriven_turn_id() {
    // `mock` provider without its script path: the redriven spawn fails
    // deterministically, so the drained worker exits fast.
    let _env = EnvGuard::unset("MOCK_AGENT_SCRIPT_PATH");
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-tid-r"), AgentId::from("a-tid-r"));
    seed_agent(&mgr, &ws, &id).await;
    let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
    session.provider = Some("mock".to_string());
    mgr.services
        .store
        .update_agent_session(&ws, &session)
        .await
        .expect("set mock provider");
    mgr.services
        .store
        .set_agent_session_status(&ws, &id, AgentStatus::Error, false, &now_iso(), None)
        .await
        .expect("park session in error");
    // Simulate the terminal-failure requeue with a preserved turn id.
    let options = super::TurnOptions {
        turn_id: Some("turn-retry-1".to_string()),
        ..super::TurnOptions::default()
    };
    super::persist_error_and_requeue(&mgr, &id, &ws, "retry me", &options, true, "boom").await;

    let result = mgr
        .agent_retry(id.clone(), ws.clone())
        .await
        .expect("retry");
    assert_eq!(result["ok"], json!(true));
    assert_eq!(result["redriven"], json!(true));
    assert_eq!(
        result["turnId"],
        json!("turn-retry-1"),
        "retry response names the redriven turn: {result}"
    );
    // Let the redriven worker settle (spawn fails without a provider script,
    // parking the session back in Error — irrelevant to this assertion).
    let _ = timeout(Duration::from_secs(10), async {
        loop {
            if !mgr.is_busy(&id) && mgr.workers.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    // Empty-queue retry: no turnId key.
    mgr.services.clear_queue(&id);
    mgr.services
        .store
        .set_agent_session_status(&ws, &id, AgentStatus::Error, false, &now_iso(), None)
        .await
        .expect("re-park session in error");
    let result = mgr
        .agent_retry(id.clone(), ws.clone())
        .await
        .expect("retry with empty queue");
    assert_eq!(result["redriven"], json!(false));
    assert!(
        result.get("turnId").is_none(),
        "no turnId when nothing was redriven: {result}"
    );
}

/// monorepo#840: an ORDINARY Error session (no fatal `stop_reason`, no streak)
/// is NOT quarantined — `send_message` still redrives it (the documented
/// fresh-message recovery path). Guard against over-blocking. Uses the
/// `mock` provider without `MOCK_AGENT_SCRIPT_PATH` so the redriven spawn
/// fails deterministically and the worker exits.
#[tokio::test]
async fn send_message_still_redrives_ordinary_error_session() {
    let _env = EnvGuard::unset("MOCK_AGENT_SCRIPT_PATH");
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-plain-err"),
        AgentId::from("a-plain-err"),
    );
    seed_agent(&mgr, &ws, &id).await;
    let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
    session.provider = Some("mock".to_string());
    mgr.services
        .store
        .update_agent_session(&ws, &session)
        .await
        .expect("set mock provider");
    mgr.services
        .store
        .set_agent_session_status(
            &ws,
            &id,
            AgentStatus::Error,
            false,
            &now_iso(),
            Some(Some("connection reset by peer".into())),
        )
        .await
        .expect("park session in ordinary error");

    let result = mgr
        .send_message(
            id.clone(),
            ws.clone(),
            "try again".to_string(),
            None,
            super::TurnOptions::default(),
        )
        .await
        .expect("ordinary-error send proceeds");
    // Not quarantined: the send claimed the slot and drove the normal path
    // (the spawn then fails terminally in the worker; what matters here is
    // the gate did NOT park it).
    assert!(result.get("quarantined").is_none(), "no quarantine flag");
    assert_eq!(result["queued"], json!(false), "delivery drove a turn");

    // Let the worker hit its terminal spawn failure and exit so the test
    // leaves no dangling state.
    timeout(Duration::from_secs(10), async {
        loop {
            if !mgr.is_busy(&id) && mgr.workers.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("worker exits after the terminal spawn failure");

    // Streak below threshold also stays open: two identical failures…
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    mgr.services.clear_failure_streak(&id);
    mgr.services.record_terminal_failure(&id, "boom");
    mgr.services.record_terminal_failure(&id, "boom");
    assert!(
        !mgr.services.session_poisoned(&session),
        "below-threshold streak must not quarantine"
    );
}

/// monorepo#940: `agent.retry` of a POISONED session (Error + session-fatal
/// provider block) must arm `force_recreate` so the redrive's
/// `start_session` skips `session/load` and opens a fresh `session/new` —
/// resuming would replay the exact context the provider rejects.
#[tokio::test]
async fn retry_arms_force_recreate_for_poisoned_session() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-retry-poison"),
        AgentId::from("a-retry-poison"),
    );
    seed_agent(&mgr, &ws, &id).await;
    mgr.services
        .store
        .set_agent_session_status(
            &ws,
            &id,
            AgentStatus::Error,
            false,
            &now_iso(),
            Some(Some(
                "The model provider blocked this response for safety reasons. \
                 Please start a new session"
                    .into(),
            )),
        )
        .await
        .expect("park session poisoned");

    let result = mgr
        .agent_retry(id.clone(), ws.clone())
        .await
        .expect("retry");
    assert_eq!(result["ok"], json!(true));
    assert_eq!(result["redriven"], json!(false), "queue is empty");
    assert!(
        mgr.force_recreate.lock().unwrap().contains(&id),
        "poisoned retry arms force_recreate for a fresh session/new"
    );
}

/// monorepo#940 guard: `agent.retry` of an ORDINARY Error session (no fatal
/// `stop_reason`, no streak) must NOT arm `force_recreate` — the redrive keeps
/// today's `session/load` resume behavior exactly.
#[tokio::test]
async fn retry_does_not_arm_force_recreate_for_ordinary_error() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-retry-plain"),
        AgentId::from("a-retry-plain"),
    );
    seed_agent(&mgr, &ws, &id).await;
    mgr.services
        .store
        .set_agent_session_status(
            &ws,
            &id,
            AgentStatus::Error,
            false,
            &now_iso(),
            Some(Some("spawn failed: connection reset by peer".into())),
        )
        .await
        .expect("park session in ordinary error");

    let result = mgr
        .agent_retry(id.clone(), ws.clone())
        .await
        .expect("retry");
    assert_eq!(result["ok"], json!(true));
    assert!(
        !mgr.force_recreate.lock().unwrap().contains(&id),
        "ordinary error retry must keep the resume path (no force_recreate)"
    );
}

/// monorepo#940 ordering: the poisoned check in `agent_retry` must read the
/// identical-failure streak BEFORE `clear_failure_streak` wipes it — a
/// streak-poisoned session (no fatal `stop_reason`) still arms
/// `force_recreate`, and the streak is cleared afterwards as before.
#[tokio::test]
async fn retry_poison_check_reads_streak_before_clear() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-retry-streak"),
        AgentId::from("a-retry-streak"),
    );
    seed_agent(&mgr, &ws, &id).await;
    mgr.services
        .store
        .set_agent_session_status(&ws, &id, AgentStatus::Error, false, &now_iso(), None)
        .await
        .expect("park session in error");
    // Poisoned only via the streak: three consecutive identical failures.
    mgr.services.record_terminal_failure(&id, "boom");
    mgr.services.record_terminal_failure(&id, "boom");
    mgr.services.record_terminal_failure(&id, "boom");

    let result = mgr
        .agent_retry(id.clone(), ws.clone())
        .await
        .expect("retry");
    assert_eq!(result["ok"], json!(true));
    assert!(
        mgr.force_recreate.lock().unwrap().contains(&id),
        "streak-poisoned retry arms force_recreate (check ran before the clear)"
    );
    assert_eq!(
        mgr.services.failure_streak_count(&id),
        0,
        "retry still clears the streak after the poisoned check"
    );
}

/// monorepo#940: the `agent:status-changed` event emitted on a terminal
/// failure that classifies as session-fatal (here the #940-shaped
/// deterministic `session/prompt` 400 rejection) carries
/// `sessionCorrupted: true` alongside `stopReason`.
#[tokio::test]
async fn terminal_failure_event_carries_session_corrupted_when_poisoned() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let (ws, id) = (
        WorkspaceId::from("ws-940-corrupt"),
        AgentId::from("a-940-corrupt"),
    );
    seed_agent(&mgr, &ws, &id).await;

    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec!["agent:status-changed".to_string()],
        ..Default::default()
    });
    super::persist_error_and_requeue(
        &mgr,
        &id,
        &ws,
        "do work",
        &super::TurnOptions::default(),
        true,
        "session/prompt failed: 400 Bad Request {\"apiStatus\":\"invalidArgument\",\"requestId\":\"req-1\"}",
    )
    .await;

    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    let ev = batch
        .iter()
        .find(|e| e.event_type == "agent:status-changed")
        .expect("status-changed event");
    assert_eq!(ev.data["status"], json!("error"));
    assert_eq!(
        ev.data["sessionCorrupted"],
        json!(true),
        "poisoned failure carries sessionCorrupted (got {:?})",
        ev.data
    );
}

/// monorepo#940 guard: an ORDINARY terminal failure (unrecognized error,
/// streak below threshold) emits `agent:status-changed` WITHOUT the
/// `sessionCorrupted` field — absent, not `false`.
#[tokio::test]
async fn terminal_failure_event_omits_session_corrupted_for_ordinary_error() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let (ws, id) = (
        WorkspaceId::from("ws-940-plain"),
        AgentId::from("a-940-plain"),
    );
    seed_agent(&mgr, &ws, &id).await;

    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec!["agent:status-changed".to_string()],
        ..Default::default()
    });
    super::persist_error_and_requeue(
        &mgr,
        &id,
        &ws,
        "do work",
        &super::TurnOptions::default(),
        true,
        "connection reset by peer",
    )
    .await;

    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    let ev = batch
        .iter()
        .find(|e| e.event_type == "agent:status-changed")
        .expect("status-changed event");
    assert_eq!(ev.data["status"], json!("error"));
    assert_eq!(ev.data["stopReason"], json!("connection reset by peer"));
    assert!(
        ev.data.get("sessionCorrupted").is_none(),
        "ordinary error omits sessionCorrupted (got {:?})",
        ev.data
    );
}

/// A terminal failure appends a durable system-role transcript notice with a
/// single text block carrying the error text and `meta.kind = "turn-failure"`
/// (the `InterruptionNotice` shape), and emits `agent:message` (role=system)
/// for it. Persisting the error `status/stop_reason` must not depend on the
/// notice (best-effort append).
#[tokio::test]
async fn terminal_failure_appends_turn_failure_transcript_notice() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let (ws, id) = (WorkspaceId::from("ws-tfn"), AgentId::from("a-tfn"));
    seed_agent(&mgr, &ws, &id).await;

    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![intent_core::events::AGENT_MESSAGE.to_string()],
        ..Default::default()
    });
    super::persist_error_and_requeue(
        &mgr,
        &id,
        &ws,
        "do work",
        &super::TurnOptions::default(),
        true,
        "boom: provider exploded",
    )
    .await;

    // Transcript: exactly one system-role notice with the turn-failure meta.kind.
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    let notices: Vec<_> = messages
        .iter()
        .filter(|m| m.role == "system" && m.content[0]["meta"]["kind"] == json!("turn-failure"))
        .collect();
    assert_eq!(
        notices.len(),
        1,
        "one turn-failure notice in the transcript: {messages:?}"
    );
    assert_eq!(
        notices[0].content,
        json!([{
            "type": "text",
            "text": "boom: provider exploded",
            "meta": { "kind": "turn-failure" }
        }]),
        "notice carries a single text block with the error text"
    );

    // Wire: agent:message (role=system) for the appended notice.
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    let ev = batch
        .iter()
        .find(|e| {
            e.event_type == intent_core::events::AGENT_MESSAGE && e.data["role"] == json!("system")
        })
        .expect("agent:message (system) event");
    assert_eq!(ev.data["messageId"], json!(notices[0].id));
}

/// A repeat of the IDENTICAL terminal failure with no intervening
/// `agent.retry` or successful turn (streak > 1 — e.g. a fresh redrive of
/// the same message failing the same way again) does NOT append a
/// duplicate notice; a DISTINCT failure text appends a new one.
#[tokio::test]
async fn terminal_failure_notice_not_duplicated_on_identical_requeue() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-tfn-dup"), AgentId::from("a-tfn-dup"));
    seed_agent(&mgr, &ws, &id).await;

    let options = super::TurnOptions::default();
    super::persist_error_and_requeue(&mgr, &id, &ws, "do work", &options, true, "boom").await;
    super::persist_error_and_requeue(&mgr, &id, &ws, "do work", &options, true, "boom").await;

    let count_notices = |messages: &[intent_core::AgentMessage], text: &str| {
        messages
            .iter()
            .filter(|m| {
                m.role == "system"
                    && m.content[0]["meta"]["kind"] == json!("turn-failure")
                    && m.content[0]["text"] == json!(text)
            })
            .count()
    };
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    assert_eq!(
        count_notices(&messages, "boom"),
        1,
        "identical repeat failure appends no duplicate notice: {messages:?}"
    );

    // A distinct failure resets the streak and appends its own notice.
    super::persist_error_and_requeue(&mgr, &id, &ws, "do work", &options, true, "other error")
        .await;
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    assert_eq!(count_notices(&messages, "boom"), 1);
    assert_eq!(
        count_notices(&messages, "other error"),
        1,
        "a distinct failure text appends a new notice: {messages:?}"
    );
}

/// `agent.retry` clears the identical-failure streak (monorepo#840, the
/// deliberate quarantine escape hatch): a same-text failure immediately
/// after a retry restarts the streak at 1, so it DOES append its own
/// turn-failure notice — unlike a same-text failure with no intervening
/// retry, which is deduped (see
/// `terminal_failure_notice_not_duplicated_on_identical_requeue`).
#[tokio::test]
async fn terminal_failure_notice_not_deduped_across_agent_retry() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (
        WorkspaceId::from("ws-tfn-retry"),
        AgentId::from("a-tfn-retry"),
    );
    seed_agent(&mgr, &ws, &id).await;

    let options = super::TurnOptions::default();
    super::persist_error_and_requeue(&mgr, &id, &ws, "do work", &options, true, "boom").await;

    // agent.retry clears the streak — the deliberate quarantine escape hatch.
    mgr.services.clear_failure_streak(&id);
    mgr.services.clear_failure_wake_dedup(&id);

    // The identical failure text repeats right after the retry.
    super::persist_error_and_requeue(&mgr, &id, &ws, "do work", &options, true, "boom").await;

    let count_notices = |messages: &[intent_core::AgentMessage], text: &str| {
        messages
            .iter()
            .filter(|m| {
                m.role == "system"
                    && m.content[0]["meta"]["kind"] == json!("turn-failure")
                    && m.content[0]["text"] == json!(text)
            })
            .count()
    };
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    assert_eq!(
        count_notices(&messages, "boom"),
        2,
        "a same-text failure after agent.retry gets its own fresh notice: {messages:?}"
    );
}

/// A terminal failure persists `stop_reason_timestamp` alongside `stop_reason`
/// and the `agent:status-changed` error payload carries `stopReasonTimestamp`;
/// clearing the stop reason (turn begin / `agent.retry` — the `Some(None)`
/// path through `persist_status_with_stop_reason`) clears the persisted
/// timestamp and emits `stopReasonTimestamp: null`.
#[tokio::test]
async fn terminal_failure_persists_and_clears_stop_reason_timestamp() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let (ws, id) = (WorkspaceId::from("ws-srt"), AgentId::from("a-srt"));
    seed_agent(&mgr, &ws, &id).await;

    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec!["agent:status-changed".to_string()],
        ..Default::default()
    });
    super::persist_error_and_requeue(
        &mgr,
        &id,
        &ws,
        "do work",
        &super::TurnOptions::default(),
        true,
        "boom",
    )
    .await;

    let session = mgr
        .services
        .store
        .get_agent_session(&id)
        .await
        .expect("session");
    assert_eq!(session.stop_reason, Some("boom".to_string()));
    let persisted_ts = session
        .stop_reason_timestamp
        .expect("stop_reason_timestamp persisted with the error");

    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    let ev = batch
        .iter()
        .find(|e| e.event_type == "agent:status-changed")
        .expect("status-changed event");
    assert_eq!(ev.data["stopReason"], json!("boom"));
    assert_eq!(
        ev.data["stopReasonTimestamp"],
        json!(persisted_ts),
        "error event carries the persisted stopReasonTimestamp"
    );

    // Clearing the stop reason clears the timestamp (store) and emits null (wire).
    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec!["agent:status-changed".to_string()],
        ..Default::default()
    });
    mgr.persist_status_with_stop_reason(
        &id,
        &ws,
        AgentStatus::RuntimeIdle,
        false,
        Some(None),
        true,
    )
    .await;
    let session = mgr
        .services
        .store
        .get_agent_session(&id)
        .await
        .expect("session");
    assert_eq!(session.stop_reason, None, "stop_reason cleared");
    assert_eq!(
        session.stop_reason_timestamp, None,
        "stop_reason_timestamp cleared wherever stop_reason clears"
    );
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    let ev = batch
        .iter()
        .find(|e| e.event_type == "agent:status-changed")
        .expect("status-changed event");
    assert_eq!(ev.data["stopReason"], Value::Null);
    assert_eq!(
        ev.data["stopReasonTimestamp"],
        Value::Null,
        "clear event carries stopReasonTimestamp: null"
    );
}

/// monorepo#564 regression: `send_message` to a nonexistent agent id (e.g. a
/// truncated id) must fail closed with `InvalidParams` naming the id — NOT
/// claim the slot, NOT queue a phantom message, NOT persist a transcript row.
#[tokio::test]
async fn send_message_rejects_unknown_agent() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-ghost"),
        AgentId::from("agent-00000000-0000-0000-0000-000000000000"),
    );

    let err = mgr
        .send_message(
            id.clone(),
            ws.clone(),
            "hello?".to_string(),
            None,
            super::TurnOptions::default(),
        )
        .await
        .expect_err("unknown agent must be rejected");
    match &err {
        Error::InvalidParams(msg) => assert!(
            msg.contains(&id.0),
            "error must name the unknown agent id: {msg}"
        ),
        other => panic!("expected Error::InvalidParams, got {other:?}"),
    }
    assert!(!mgr.is_busy(&id), "no slot claim for an unknown agent");
    assert!(
        mgr.services.queue_snapshot(&id).is_empty(),
        "no phantom queue entry for an unknown agent"
    );
    assert!(
        mgr.workers.lock().unwrap().is_empty(),
        "no worker spawned for an unknown agent"
    );
}

/// monorepo#564 regression: the interrupt-priority path fails closed the same
/// way — `interrupt_send_message` to an unknown agent is rejected without
/// recording a duplicate-delivery interrupt id.
#[tokio::test]
async fn interrupt_send_message_rejects_unknown_agent() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-ghost"),
        AgentId::from("agent-00000000-0000-0000-0000-000000000000"),
    );

    let err = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "urgent!".to_string(),
            Some("msg-564".to_string()),
            super::TurnOptions::default(),
        )
        .await
        .expect_err("unknown agent must be rejected");
    assert!(
        matches!(err, Error::InvalidParams(_)),
        "expected Error::InvalidParams, got {err:?}"
    );
    assert!(
        !mgr.interrupt_ids.lock().unwrap().contains_key(&id),
        "no interrupt-dedup entry for an unknown agent"
    );
    assert!(
        mgr.services.queue_snapshot(&id).is_empty(),
        "no phantom queue entry for an unknown agent"
    );
}

/// STAB-52 regression (full loop): a message sent to an agent whose spawn
/// always fails terminally must land the row in exactly `status = Error,
/// is_active = 0` after a single failure — and a subsequent queue kick must
/// NOT re-spawn the failing turn. Uses the `mock` provider WITHOUT its
/// required `MOCK_AGENT_SCRIPT_PATH` env, so `resolve_spawn` fails
/// deterministically with a non-retryable error before any child exists.
#[tokio::test]
async fn terminal_spawn_failure_parks_error_without_crash_loop() {
    // Unset (and restore on drop) so the spawn-failure path is exercised even
    // in environments that export the mock script path globally.
    let _env = EnvGuard::unset("MOCK_AGENT_SCRIPT_PATH");
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-loop"), AgentId::from("a-loop"));
    seed_agent(&mgr, &ws, &id).await;
    // Point the session at the mock provider (immutable once set, but the
    // seeded session has none) so the spawn fails before launching a child.
    let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
    session.provider = Some("mock".to_string());
    mgr.services
        .store
        .update_agent_session(&ws, &session)
        .await
        .expect("set mock provider");

    let result = mgr
        .send_message(
            id.clone(),
            ws.clone(),
            "boom".to_string(),
            None,
            super::TurnOptions::default(),
        )
        .await
        .expect("send_message spawns the worker inline");
    assert_eq!(result["queued"], json!(false));

    // Wait for the worker to hit the terminal spawn failure and exit.
    timeout(Duration::from_secs(10), async {
        loop {
            let session = mgr.services.store.get_agent_session(&id).await.unwrap();
            if session.status == AgentStatus::Error
                && !mgr.is_busy(&id)
                && mgr.workers.lock().unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("worker parks the session in error and exits");

    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(session.status, AgentStatus::Error);
    assert!(
        !session.is_active,
        "terminal failure must not leak is_active=1"
    );
    // The failed message was requeued (already persisted) for agent.retry.
    let snap = mgr.services.queue_snapshot(&id);
    assert_eq!(snap.len(), 1, "failed message requeued exactly once");
    assert_eq!(snap[0]["content"], json!("boom"));

    // The crash-loop trigger: a queue kick lands on the requeued message.
    // Before the STAB-52 gate this re-claimed the slot and re-spawned the
    // failing turn indefinitely.
    mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;

    assert!(!mgr.is_busy(&id), "no re-claim of the in-flight slot");
    assert!(
        mgr.workers.lock().unwrap().is_empty(),
        "no re-spawned worker"
    );
    assert_eq!(mgr.services.queue_snapshot(&id).len(), 1);
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(
        session.status,
        AgentStatus::Error,
        "still parked in error awaiting agent.retry"
    );
    assert!(!session.is_active);
}

/// STAB-51 regression (intent-hq/monorepo#454): when the pre-turn
/// `persist_user` append fails for a drained message, the terminal-failure
/// requeue must carry `persisted: false` so the `agent.retry` drain
/// re-attempts the append and the message lands in the transcript. Before the
/// fix the requeue hard-coded `persisted: true`, so the retry skipped the
/// persist and the user message never reached the transcript.
#[tokio::test]
async fn failed_drain_persist_is_reattempted_by_retry_drain() {
    // Unset (and restore on drop) so every spawn fails terminally.
    let _env = EnvGuard::unset("MOCK_AGENT_SCRIPT_PATH");
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-p51"), AgentId::from("a-p51"));
    seed_agent(&mgr, &ws, &id).await;
    let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
    session.provider = Some("mock".to_string());
    mgr.services
        .store
        .update_agent_session(&ws, &session)
        .await
        .expect("set mock provider");

    // Queue an unpersisted message, then hide the transcript table so the
    // drain's pre-turn `persist_user` append fails.
    mgr.services
        .enqueue_message(&id, "boom".to_string(), None, None, None, None, false);
    sqlx::query("ALTER TABLE agent_message RENAME TO agent_message_broken")
        .execute(mgr.services.store.write_pool())
        .await
        .expect("hide agent_message table");

    mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;

    // Wait for the worker to hit the terminal spawn failure and exit. Poll
    // the status-only read: the full session read joins the (hidden)
    // transcript table.
    timeout(Duration::from_secs(10), async {
        loop {
            let status = mgr
                .services
                .store
                .get_agent_session_status(&id)
                .await
                .unwrap();
            if status == AgentStatus::Error
                && !mgr.is_busy(&id)
                && mgr.workers.lock().unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("worker parks the session in error and exits");
    assert_eq!(
        mgr.services.queue_snapshot(&id).len(),
        1,
        "failed message requeued for agent.retry"
    );

    // Restore the store, then retry: the drain must re-attempt the persist.
    sqlx::query("ALTER TABLE agent_message_broken RENAME TO agent_message")
        .execute(mgr.services.store.write_pool())
        .await
        .expect("restore agent_message table");
    let result = mgr
        .agent_retry(id.clone(), ws.clone())
        .await
        .expect("agent.retry");
    assert_eq!(result["redriven"], json!(true));

    // The retry turn fails terminally again (spawn still broken) and parks.
    timeout(Duration::from_secs(10), async {
        loop {
            let session = mgr.services.store.get_agent_session(&id).await.unwrap();
            if session.status == AgentStatus::Error
                && !mgr.is_busy(&id)
                && mgr.workers.lock().unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("retry turn parks the session in error and exits");

    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    let user_rows: Vec<_> = messages
        .iter()
        .filter(|m| {
            m.role == "user"
                && m.content[0]["text"]
                    .as_str()
                    .is_some_and(|t| t.starts_with("boom"))
        })
        .collect();
    assert_eq!(
        user_rows.len(),
        1,
        "the drained message lands in the transcript exactly once: {messages:?}"
    );
}

/// STAB-51 no-regression companion: when the pre-turn persist SUCCEEDED
/// (direct `send_message` path), the terminal-failure requeue keeps
/// `persisted: true` and the `agent.retry` drain must NOT append a duplicate
/// user row.
#[tokio::test]
async fn successful_persist_is_not_duplicated_by_retry_drain() {
    let _env = EnvGuard::unset("MOCK_AGENT_SCRIPT_PATH");
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-p51-ok"), AgentId::from("a-p51-ok"));
    seed_agent(&mgr, &ws, &id).await;
    let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
    session.provider = Some("mock".to_string());
    mgr.services
        .store
        .update_agent_session(&ws, &session)
        .await
        .expect("set mock provider");

    mgr.send_message(
        id.clone(),
        ws.clone(),
        "boom".to_string(),
        None,
        super::TurnOptions::default(),
    )
    .await
    .expect("send_message spawns the worker inline");

    timeout(Duration::from_secs(10), async {
        loop {
            let session = mgr.services.store.get_agent_session(&id).await.unwrap();
            if session.status == AgentStatus::Error
                && !mgr.is_busy(&id)
                && mgr.workers.lock().unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("worker parks the session in error and exits");

    let result = mgr
        .agent_retry(id.clone(), ws.clone())
        .await
        .expect("agent.retry");
    assert_eq!(result["redriven"], json!(true));

    timeout(Duration::from_secs(10), async {
        loop {
            let session = mgr.services.store.get_agent_session(&id).await.unwrap();
            if session.status == AgentStatus::Error
                && !mgr.is_busy(&id)
                && mgr.workers.lock().unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("retry turn parks the session in error and exits");

    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    let user_rows: Vec<_> = messages
        .iter()
        .filter(|m| m.role == "user" && m.content[0]["text"] == json!("boom"))
        .collect();
    assert_eq!(
        user_rows.len(),
        1,
        "retry must not duplicate an already-persisted user row: {messages:?}"
    );
}

/// intent-hq/monorepo#2762 regression: a queue drain against a VANISHED
/// session (concurrent `agent.delete` raced the drain trigger) must drop the
/// undeliverable queue instead of skipping silently — before the fix the
/// status-gate `Err` arm returned with the in-memory entries intact, leaving
/// a permanently wedged queue (every future kick re-skips or re-fails the
/// `agent_message.agent_id` FK, `SQLite` 787).
#[tokio::test]
async fn drain_against_vanished_session_drops_queue() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-2762-drain"),
        AgentId::from("a-2762-drain"),
    );
    seed_agent(&mgr, &ws, &id).await;

    mgr.services
        .enqueue_message(&id, "wedged".to_string(), None, None, None, None, false);
    mgr.services
        .store
        .delete_agent_session(&ws, &id)
        .await
        .expect("delete agent session");

    mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;

    assert!(
        mgr.services.queue_snapshot(&id).is_empty(),
        "queued messages for a vanished session are dropped, not wedged"
    );
    assert!(!mgr.is_busy(&id), "no slot claimed for a vanished session");
}

/// intent-hq/monorepo#2762 regression: a pre-turn persist failure whose
/// session row is GONE must discard the message — no Error park (there is no
/// row to park), no `agent:failed`/`agent:status-changed` ghost events for an
/// agent the FE already saw deleted, and NO front requeue (the requeue is
/// what wedged the queue: every retry re-failed the FK against the missing
/// session, reproducing the issue's "Retry failed" loop).
#[tokio::test]
async fn drain_persist_failure_for_vanished_session_discards_message() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-2762-persist"),
        AgentId::from("a-2762-persist"),
    );
    seed_agent(&mgr, &ws, &id).await;
    mgr.services
        .store
        .delete_agent_session(&ws, &id)
        .await
        .expect("delete agent session");
    // Simulate the drain arm's fail-closed handoff after `persist_user`
    // failed against the deleted row (the drain already dequeued the entry).
    let mut sub = bus.subscribe(SubscriptionFilter::default());
    super::handle_drain_persist_failure(
        &mgr,
        &id,
        &ws,
        "raced with delete",
        &super::TurnOptions::default(),
    )
    .await;

    assert!(
        mgr.services.queue_snapshot(&id).is_empty(),
        "no front requeue for a vanished session"
    );
    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    assert!(
        !events.iter().any(|e| e.event_type == "agent:failed"
            || e.event_type == "agent:status-changed"
            || e.event_type == "agent:queue:updated"),
        "no ghost failure events for a deleted agent: {events:?}"
    );
}

/// intent-hq/monorepo#2762 regression, terminal turn-failure arm: a turn that
/// dies mid-flight because the session was deleted under it (the assistant/
/// user append fails the `agent_message` FK) must NOT surface `agent:failed`
/// + requeue for the gone agent — that is the "Agent failed … Retry failed"
/// wedge from the issue. The message is discarded and any stashed streaming
/// terminal-error context is dropped with it.
#[tokio::test]
async fn terminal_turn_failure_for_vanished_session_discards_message() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-2762-turn"),
        AgentId::from("a-2762-turn"),
    );
    seed_agent(&mgr, &ws, &id).await;
    mgr.services
        .store
        .delete_agent_session(&ws, &id)
        .await
        .expect("delete agent session");

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let fk_err = Error::Internal(
        "append agent message failed: error returned from database: (code: 787) FOREIGN KEY constraint failed".to_string(),
    );
    super::handle_terminal_turn_failure(
        &mgr,
        &id,
        &ws,
        "raced with delete",
        &super::TurnOptions::default(),
        false,
        &fk_err,
    )
    .await;

    assert!(
        mgr.services.queue_snapshot(&id).is_empty(),
        "no front requeue for a vanished session"
    );
    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    assert!(
        !events.iter().any(|e| e.event_type == "agent:failed"
            || e.event_type == "agent:status-changed"
            || e.event_type == "agent:queue:updated"),
        "no ghost failure events for a deleted agent: {events:?}"
    );
}

/// intent-hq/monorepo#2762 regression, wake-delivery arm: a wake whose target
/// session vanished between validation and the user-row append must fail
/// closed with the monorepo#564 `unknown agent id` contract — before the fix
/// the append-failure fallback auto-queued the wake as a phantom entry no
/// drain could ever deliver.
#[tokio::test]
async fn wake_delivery_to_vanished_session_fails_closed() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    mgr.services.attach_agent_manager(&mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-2762-wake"),
        AgentId::from("a-2762-wake"),
    );
    seed_agent(&mgr, &ws, &id).await;
    mgr.services
        .store
        .delete_agent_session(&ws, &id)
        .await
        .expect("delete agent session");

    let err = mgr
        .services
        .deliver_wake_message(&ws, &id, "[Agent Completed] raced wake", None)
        .await
        .expect_err("wake against a vanished session is rejected");
    assert!(
        matches!(&err, Error::InvalidParams(msg) if msg.contains("unknown agent id")),
        "monorepo#564 fail-closed contract: {err:?}"
    );
    assert!(
        mgr.services.queue_snapshot(&id).is_empty(),
        "no phantom queue entry for a vanished session"
    );
    assert!(!mgr.is_busy(&id), "slot released after the rejected wake");
}

/// intent-hq/monorepo#2762 regression, wake enqueue-only route: a wake for a
/// BUSY agent whose session vanished never touches `agent_message` (the
/// busy-agent branch returns queued success without any append), so the
/// append-failure guard alone cannot catch it — the up-front vanished-session
/// gate must reject it before the phantom entry is parked.
#[tokio::test]
async fn wake_to_busy_vanished_session_fails_closed() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    mgr.services.attach_agent_manager(&mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-2762-busy"),
        AgentId::from("a-2762-busy"),
    );
    seed_agent(&mgr, &ws, &id).await;
    assert!(
        mgr.try_begin_turn(&id, &ws).await,
        "claim the in-flight slot so the wake takes the busy enqueue branch"
    );
    mgr.services
        .store
        .delete_agent_session(&ws, &id)
        .await
        .expect("delete agent session");

    let err = mgr
        .services
        .deliver_wake_message(&ws, &id, "[Agent Completed] raced wake", None)
        .await
        .expect_err("wake to a busy-but-vanished session is rejected");
    assert!(
        matches!(&err, Error::InvalidParams(msg) if msg.contains("unknown agent id")),
        "monorepo#564 fail-closed contract: {err:?}"
    );
    assert!(
        mgr.services.queue_snapshot(&id).is_empty(),
        "no phantom queue entry parked by the enqueue-only route"
    );
}

/// intent-hq/monorepo#2762 regression, batch flush interleaving: when a
/// flush persists an earlier entry and the session vanishes while persisting
/// a later one, the fail-closed restore must NOT put the already-persisted
/// head entries back — the vanished-session discard dropped the whole queue,
/// and restoring `head` would recreate a ghost in-memory queue (plus a
/// failed write-through) for the deleted agent.
#[tokio::test]
async fn flush_persist_failure_for_vanished_session_drops_whole_batch() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-2762-flush"),
        AgentId::from("a-2762-flush"),
    );
    seed_agent(&mgr, &ws, &id).await;
    mgr.services
        .store
        .delete_agent_session(&ws, &id)
        .await
        .expect("delete agent session");

    // Head entry already durable (`persisted: true`, e.g. a terminal-failure
    // requeue), second entry needs the row append — which fails NotFound
    // against the deleted session.
    let entry = |suffix: &str, persisted: bool| crate::agent_ops::QueuedMessage {
        id: format!("qm-2762-{suffix}"),
        turn_id: format!("qm-2762-{suffix}"),
        content: format!("entry {suffix}"),
        image_blocks: None,
        file_blocks: None,
        queued_at: now_iso(),
        editing: false,
        persisted,
        requeued_after_failure: false,
        message_metadata: None,
        prepend_content: None,
        prepend_image_blocks: None,
        prepend_file_blocks: None,
        interrupt_priority: false,
        user_origin: false,
    };
    let batch = vec![entry("head", true), entry("tail", false)];

    let prep = super::prepare_flush_turn(&mgr, &id, &ws, batch).await;
    assert!(
        matches!(prep, super::FlushPrep::Parked),
        "vanished-session flush parks instead of starting a turn"
    );
    assert!(
        mgr.services.queue_snapshot(&id).is_empty(),
        "no ghost queue restored from already-persisted head entries"
    );
}

/// Absolute path to the deterministic mock ACP agent fixture so a unit test
/// can exercise a REAL successful turn (node child, ACP handshake, default
/// "Mock agent completed." response) through the drain paths.
fn mock_agent_script() -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../intentd/tests/fixtures/mock-acp-agent.mjs")
        .canonicalize()
        .expect("mock-acp-agent.mjs fixture exists")
        .display()
        .to_string()
}

/// #547 regression (fail-closed drain): when the pre-turn `persist_user`
/// append fails for ALL bounded retry attempts, the drain must NOT start the
/// turn — even with a healthy provider that would succeed. The agent parks in
/// `Error` with the persist failure as `stop_reason` and the message requeued
/// `persisted: false`; `agent.retry` against a restored store then lands the
/// user row exactly once and the turn completes. Before the fix the drain
/// started the turn anyway, producing observable assistant output for a user
/// message that never reached the transcript.
#[tokio::test]
async fn failed_drain_persist_parks_error_without_starting_turn() {
    let script = mock_agent_script();
    let _env = EnvGuard::set_all(&[
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("INTENTD_PERSIST_RETRY_BACKOFF_MS", "10,10"),
    ]);
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-547"), AgentId::from("a-547"));
    seed_agent(&mgr, &ws, &id).await;
    let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
    session.provider = Some("mock".to_string());
    mgr.services
        .store
        .update_agent_session(&ws, &session)
        .await
        .expect("set mock provider");

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    // Queue an unpersisted message, then hide the transcript table so every
    // pre-turn `persist_user` attempt (initial + bounded retries) fails.
    let (enqueued, _) =
        mgr.services
            .enqueue_message(&id, "boom".to_string(), None, None, None, None, false);
    sqlx::query("ALTER TABLE agent_message RENAME TO agent_message_broken")
        .execute(mgr.services.store.write_pool())
        .await
        .expect("hide agent_message table");

    mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;

    // The drain must park the session in Error WITHOUT starting the turn.
    // Poll the status-only read: the full session read joins the (hidden)
    // transcript table.
    timeout(Duration::from_secs(10), async {
        loop {
            let status = mgr
                .services
                .store
                .get_agent_session_status(&id)
                .await
                .unwrap();
            if status == AgentStatus::Error
                && !mgr.is_busy(&id)
                && mgr.workers.lock().unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("drain parks the session in error without a worker");

    // The message is requeued front with CONFIRMED durability state.
    let requeued = mgr
        .services
        .dequeue_message(&id)
        .expect("failed message requeued for agent.retry");
    assert!(
        requeued.content.starts_with("boom"),
        "original content first (dequeue-wait note may follow): {}",
        requeued.content
    );
    assert!(
        !requeued.persisted,
        "requeue must carry persisted: false so the retry drain re-attempts the append"
    );
    assert!(
        requeued.requeued_after_failure,
        "requeue is a terminal-failure requeue (STAB-112)"
    );
    assert_eq!(
        requeued.turn_id, enqueued.turn_id,
        "drain → failure → requeue preserves the original turn_id (monorepo#1022)"
    );
    mgr.services.requeue_front(&id, requeued);

    // The turn never ran: no assistant chunks were streamed. Drain the bus
    // until quiet — pre-fix the turn started and the mock agent's response
    // chunk reached subscribers before the terminal failure parked the agent.
    let mut streamed_chunks = 0;
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        for ev in batch {
            if ev.event_type == intent_core::events::CHAT_STREAM_DELTA {
                streamed_chunks += 1;
            }
        }
    }
    assert_eq!(
        streamed_chunks, 0,
        "an unpersisted drained message must not produce assistant output"
    );

    // Restore the store: the stop_reason names the persist failure (not a
    // spawn/turn error) and the transcript holds NO rows for the failed drain.
    sqlx::query("ALTER TABLE agent_message_broken RENAME TO agent_message")
        .execute(mgr.services.store.write_pool())
        .await
        .expect("restore agent_message table");
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    let stop_reason = session.stop_reason.expect("stop_reason persisted");
    assert!(
        stop_reason.contains("persist") && stop_reason.contains("turn not started"),
        "stop_reason names the persist failure: {stop_reason}"
    );
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    assert!(
        messages.is_empty(),
        "no transcript rows before the retry: {messages:?}"
    );

    // agent.retry redrives: the persist is re-attempted against the healthy
    // store and the turn completes.
    let result = mgr
        .agent_retry(id.clone(), ws.clone())
        .await
        .expect("agent.retry");
    assert_eq!(result["redriven"], json!(true));
    timeout(Duration::from_secs(10), async {
        loop {
            let session = mgr.services.store.get_agent_session(&id).await.unwrap();
            if session.status == AgentStatus::RuntimeIdle
                && !mgr.is_busy(&id)
                && mgr.workers.lock().unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("retry turn completes and the agent goes idle");

    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    let user_rows: Vec<_> = messages
        .iter()
        .filter(|m| {
            m.role == "user"
                && m.content[0]["text"]
                    .as_str()
                    .is_some_and(|t| t.starts_with("boom"))
        })
        .collect();
    assert_eq!(
        user_rows.len(),
        1,
        "the drained message lands in the transcript exactly once: {messages:?}"
    );
    assert!(
        messages.iter().any(|m| m.role == "assistant"),
        "the retried turn completed with assistant output: {messages:?}"
    );
}

/// #547 companion (bounded retry): a TRANSIENT persist failure on the first
/// attempt self-heals inside `persist_user`'s bounded retry — the turn
/// proceeds normally with exactly one user row and no Error park. Before the
/// fix `persist_user` was single-attempt, so the blip lost the user row.
#[tokio::test]
async fn transient_drain_persist_blip_self_heals_via_bounded_retry() {
    let script = mock_agent_script();
    let _env = EnvGuard::set_all(&[
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("INTENTD_PERSIST_RETRY_BACKOFF_MS", "2000"),
    ]);
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-547-blip"),
        AgentId::from("a-547-blip"),
    );
    seed_agent(&mgr, &ws, &id).await;
    let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
    session.provider = Some("mock".to_string());
    mgr.services
        .store
        .update_agent_session(&ws, &session)
        .await
        .expect("set mock provider");

    mgr.services
        .enqueue_message(&id, "blip".to_string(), None, None, None, None, false);
    sqlx::query("ALTER TABLE agent_message RENAME TO agent_message_broken")
        .execute(mgr.services.store.write_pool())
        .await
        .expect("hide agent_message table");

    // Kick the drain in the background: the first persist attempt fails
    // immediately; restore the table well before the 2s-backoff retry runs.
    let drain = tokio::spawn(mgr.clone().try_drain_queue(id.clone(), ws.clone()));
    tokio::time::sleep(Duration::from_millis(250)).await;
    sqlx::query("ALTER TABLE agent_message_broken RENAME TO agent_message")
        .execute(mgr.services.store.write_pool())
        .await
        .expect("restore agent_message table");
    drain.await.expect("drain task");

    timeout(Duration::from_secs(10), async {
        loop {
            let session = mgr.services.store.get_agent_session(&id).await.unwrap();
            if session.status == AgentStatus::RuntimeIdle
                && !mgr.is_busy(&id)
                && mgr.workers.lock().unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("turn completes after the transient blip self-heals");

    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(session.status, AgentStatus::RuntimeIdle, "no Error park");
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    let user_rows: Vec<_> = messages
        .iter()
        .filter(|m| {
            m.role == "user"
                && m.content[0]["text"]
                    .as_str()
                    .is_some_and(|t| t.starts_with("blip"))
        })
        .collect();
    assert_eq!(
        user_rows.len(),
        1,
        "the blipped message lands in the transcript exactly once: {messages:?}"
    );
    assert!(
        messages.iter().any(|m| m.role == "assistant"),
        "the turn proceeded to completion: {messages:?}"
    );
}

/// monorepo#764: a transport-closed prompt failure BEFORE any streamed output
/// is silently redriven once on a fresh child — the turn completes with NO
/// `agent:failed`, no Error status, and no requeue (no Retry surface). The
/// mock child exits inside `session/prompt` on the first attempt only
/// (attempt counter persists across spawns), so the redrive succeeds.
#[tokio::test]
async fn pre_output_transport_failure_redrives_silently_once() {
    let script = mock_agent_script();
    let attempt_file = std::env::temp_dir().join(format!("itd-764-once-{}", uuid::Uuid::new_v4()));
    let attempt_file_s = attempt_file.to_string_lossy().into_owned();
    let behavior = json!({
        "exitDuringPromptAttempts": 1,
        "response": "recovered after silent redrive",
    })
    .to_string();
    let _env = EnvGuard::set_all(&[
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
        ("MOCK_AGENT_ATTEMPT_FILE", attempt_file_s.as_str()),
        ("INTENTD_SPAWN_RETRY_BACKOFF_MS", "10,20"),
    ]);
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-764-r"), AgentId::from("a-764-r"));
    seed_agent(&mgr, &ws, &id).await;
    let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
    session.provider = Some("mock".to_string());
    mgr.services
        .store
        .update_agent_session(&ws, &session)
        .await
        .expect("set mock provider");

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    mgr.send_message(
        id.clone(),
        ws.clone(),
        "dies pre-output once".to_string(),
        None,
        super::TurnOptions::default(),
    )
    .await
    .expect("send_message spawns the worker inline");

    // The redriven turn completes: the agent settles idle, not Error.
    timeout(Duration::from_secs(20), async {
        loop {
            let session = mgr.services.store.get_agent_session(&id).await.unwrap();
            if session.status == AgentStatus::RuntimeIdle
                && !mgr.is_busy(&id)
                && mgr.workers.lock().unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("silently redriven turn completes and the agent goes idle");

    // No user-visible failure surfaced for the redriven attempt.
    let mut saw_failed = false;
    let mut stream_ends = 0;
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        for ev in batch {
            match ev.event_type.as_str() {
                "agent:failed" => saw_failed = true,
                "agent:stream:end" => stream_ends += 1,
                _ => {}
            }
        }
    }
    assert!(!saw_failed, "no agent:failed for the silent redrive");
    assert_eq!(
        stream_ends, 1,
        "exactly one terminal stream:end for the message (the redriven attempt's)"
    );
    // Nothing requeued (no Retry surface) and no stale Error/stop_reason.
    assert!(
        mgr.services.queue_snapshot(&id).is_empty(),
        "no requeue for a silently redriven message"
    );
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert!(session.stop_reason.is_none(), "no error stop_reason");
    // The user row landed exactly once and the redriven turn produced output.
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    let user_rows: Vec<_> = messages
        .iter()
        .filter(|m| m.role == "user" && m.content[0]["text"] == json!("dies pre-output once"))
        .collect();
    assert_eq!(user_rows.len(), 1, "no duplicate user row: {messages:?}");
    assert!(
        messages.iter().any(|m| m.role == "assistant"),
        "the redriven turn completed with assistant output: {messages:?}"
    );
    let _ = std::fs::remove_file(&attempt_file);
}

/// The consuming half of the monorepo#2050 handoff: a full worker-driven
/// ordinary mid-turn failure — `run_prompt_turn` persists + stashes, then the
/// worker's `handle_terminal_turn_failure` CONSUMES the stash instead of
/// persisting again — leaves the identical-failure streak (monorepo#840) at
/// exactly 1, not 2. A regression that made both halves persist would double-
/// count the streak and halve the poison threshold.
#[tokio::test]
async fn worker_driven_streaming_failure_records_streak_exactly_once() {
    let script = mock_agent_script();
    let behavior = json!({
        "promptRpcError": { "code": -32603, "message": "backend exploded" },
        "response": "unreached",
    })
    .to_string();
    let _env = EnvGuard::set_all(&[
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
    ]);
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-2050-once"),
        AgentId::from("a-2050-once"),
    );
    seed_agent(&mgr, &ws, &id).await;
    set_session_provider(&mgr, &ws, &id, "mock").await;

    mgr.send_message(
        id.clone(),
        ws.clone(),
        "fails mid-turn".to_string(),
        None,
        super::TurnOptions::default(),
    )
    .await
    .expect("send_message spawns the worker inline");

    // The worker settles the terminal failure: Error persisted, worker gone.
    timeout(Duration::from_secs(20), async {
        loop {
            let status = mgr.services.store.get_agent_session_status(&id).await;
            if status.ok() == Some(AgentStatus::Error)
                && !mgr.is_busy(&id)
                && mgr.workers.lock().unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("worker settles the terminal streaming failure");

    // Exactly-once: the streaming persist recorded the streak; the handler
    // consumed the stash instead of recording it again.
    assert_eq!(
        mgr.services.failure_streak_count(&id),
        1,
        "one terminal failure records the streak exactly once (monorepo#840/#2050)"
    );
    // The stash was consumed by the handler — nothing lingers.
    assert!(
        mgr.services.take_pending_terminal_error(&id).is_none(),
        "the handler consumed the stashed terminal-error context"
    );
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert!(
        session
            .stop_reason
            .as_deref()
            .is_some_and(|r| r.contains("backend exploded")),
        "stop_reason carries the failure text: {:?}",
        session.stop_reason
    );
}

/// monorepo#764 one-retry bound: a SECOND consecutive pre-output transport
/// failure on the same message takes the existing terminal path — persisted
/// Error status, the terminal `agent:failed` + `agent:stream:end` pair
/// (emitted by the terminal-failure path, since `run_prompt_turn` suppressed
/// it for the marker error), and the message requeued for `agent.retry`.
#[tokio::test]
async fn second_pre_output_transport_failure_takes_terminal_path() {
    let script = mock_agent_script();
    let attempt_file = std::env::temp_dir().join(format!("itd-764-twice-{}", uuid::Uuid::new_v4()));
    let attempt_file_s = attempt_file.to_string_lossy().into_owned();
    let behavior = json!({
        "exitDuringPromptAttempts": 2,
        "response": "unreached",
    })
    .to_string();
    let _env = EnvGuard::set_all(&[
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
        ("MOCK_AGENT_ATTEMPT_FILE", attempt_file_s.as_str()),
        ("INTENTD_SPAWN_RETRY_BACKOFF_MS", "10,20"),
    ]);
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-764-t"), AgentId::from("a-764-t"));
    seed_agent(&mgr, &ws, &id).await;
    let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
    session.provider = Some("mock".to_string());
    mgr.services
        .store
        .update_agent_session(&ws, &session)
        .await
        .expect("set mock provider");

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    mgr.send_message(
        id.clone(),
        ws.clone(),
        "dies pre-output twice".to_string(),
        None,
        super::TurnOptions::default(),
    )
    .await
    .expect("send_message spawns the worker inline");

    // The second failure parks the agent in Error (one-retry bound).
    timeout(Duration::from_secs(20), async {
        loop {
            let session = mgr.services.store.get_agent_session(&id).await.unwrap();
            if session.status == AgentStatus::Error
                && !mgr.is_busy(&id)
                && mgr.workers.lock().unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("second pre-output failure parks the session in error");

    // The terminal pair reached the bus EXACTLY once (the second attempt's;
    // the first attempt stayed silent) — the STAB-6 Retry surface intact.
    let mut failed = 0;
    let mut stream_ends = 0;
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        for ev in batch {
            match ev.event_type.as_str() {
                "agent:failed" => failed += 1,
                "agent:stream:end" => stream_ends += 1,
                _ => {}
            }
        }
    }
    assert_eq!(failed, 1, "exactly one terminal agent:failed");
    assert_eq!(stream_ends, 1, "exactly one terminal agent:stream:end");
    // The message is requeued for agent.retry and the error is persisted.
    assert_eq!(
        mgr.services.queue_snapshot(&id).len(),
        1,
        "failed message requeued for agent.retry"
    );
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    let stop_reason = session.stop_reason.expect("error stop_reason persisted");
    assert!(
        stop_reason.contains("transport closed before output"),
        "stop_reason names the transport failure: {stop_reason}"
    );
    let _ = std::fs::remove_file(&attempt_file);
}

/// Warn-and-continue: a prompt idle timeout injects a persisted user-role
/// warning message and immediately drives a fresh turn — no `agent:failed`,
/// no Error park, no requeue — and a message queued during the hung turn is
/// NOT drained ahead of the warning turn. The child survives (keep-alive
/// cancel): the mock parks its FIRST prompt until `session/cancel`, then
/// serves the warning + queued turns from the SAME process.
#[tokio::test]
async fn idle_timeout_injects_warning_and_redrives() {
    let script = mock_agent_script();
    let behavior = json!({
        "parkIfPromptContains": "park-me",
        "rules": [
            { "ifPromptContains": "[SYSTEM WARNING]", "response": "continued after warning" },
            { "ifPromptContains": "queued follow-up", "response": "queued served" },
        ],
        "response": "unmatched",
    })
    .to_string();
    let _env = EnvGuard::set_all(&[
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
        // Short idle window; the prompt loop polls at 1s, so the first hung
        // turn times out on the first tick.
        ("INTENTD_PROMPT_IDLE_TIMEOUT_MS", "100"),
    ]);
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-idle-w"), AgentId::from("a-idle-w"));
    seed_agent(&mgr, &ws, &id).await;
    set_session_provider(&mgr, &ws, &id, "mock").await;

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    mgr.send_message(
        id.clone(),
        ws.clone(),
        "park-me and go silent".to_string(),
        None,
        super::TurnOptions::default(),
    )
    .await
    .expect("send_message spawns the worker inline");
    // While the first turn hangs, queue a follow-up: the warning turn must
    // run BEFORE it.
    mgr.services.enqueue_message(
        &id,
        "queued follow-up".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    // The warning redrive + queued drain complete: the agent settles idle.
    timeout(Duration::from_secs(30), async {
        loop {
            let session = mgr.services.store.get_agent_session(&id).await.unwrap();
            if session.status == AgentStatus::RuntimeIdle
                && !mgr.is_busy(&id)
                && mgr.workers.lock().unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("warning redrive + queued turn complete and the agent goes idle");

    // No failure surface anywhere in the sequence.
    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    assert!(
        !events.iter().any(|e| e.event_type == "agent:failed"),
        "no agent:failed on a warn-and-continue timeout"
    );
    // Every stream:end is the normal shape (none carries an error field).
    assert!(
        events
            .iter()
            .filter(|e| e.event_type == "agent:stream:end")
            .all(|e| e.data.get("error").is_none()),
        "stream:end events stay error-free"
    );
    // Nothing parked or requeued; no stale Error/stop_reason.
    assert!(mgr.services.queue_snapshot(&id).is_empty());
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert!(session.stop_reason.is_none(), "no error stop_reason");

    // Transcript ordering proves the warning turn ran BEFORE the queued
    // message: original user row → warning row → warning turn's assistant
    // response → queued user row → its response.
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    // The warning row is persisted, names the configured window, and reached
    // clients as a user-role `agent:message` echo (payload carries the row id).
    let warning_row = messages
        .iter()
        .find(|m| {
            m.role == "user"
                && m.content[0]["text"]
                    .as_str()
                    .is_some_and(|t| t.starts_with("[SYSTEM WARNING]"))
        })
        .expect("warning user row persisted");
    assert!(
        warning_row.content[0]["text"]
            .as_str()
            .unwrap()
            .contains("of silence"),
        "warning names the configured window"
    );
    assert!(
        events.iter().any(|e| {
            e.event_type == intent_core::events::AGENT_MESSAGE
                && e.data["role"] == json!("user")
                && e.data["messageId"] == json!(warning_row.id)
        }),
        "agent:message echo for the warning row reached the bus"
    );
    let texts: Vec<String> = messages
        .iter()
        .map(|m| {
            format!(
                "{}:{}",
                m.role,
                m.content[0]["text"].as_str().unwrap_or_default()
            )
        })
        .collect();
    let warning_idx = texts
        .iter()
        .position(|t| t.starts_with("user:[SYSTEM WARNING]"))
        .unwrap_or_else(|| panic!("warning user row persisted: {texts:?}"));
    let continued_idx = texts
        .iter()
        .position(|t| t == "assistant:continued after warning")
        .unwrap_or_else(|| panic!("warning turn streamed: {texts:?}"));
    let queued_idx = texts
        .iter()
        .position(|t| t.starts_with("user:queued follow-up"))
        .unwrap_or_else(|| panic!("queued user row persisted: {texts:?}"));
    assert!(
        warning_idx < continued_idx && continued_idx < queued_idx,
        "warning turn runs before the queued message: {texts:?}"
    );
}

/// Warn-and-continue cap: after [`super::MAX_CONSECUTIVE_IDLE_TIMEOUT_REDRIVES`]
/// back-to-back silent timeouts (each answered with a warning turn), the NEXT
/// timeout takes the terminal path — exactly one `agent:failed` (emitted by
/// the worker, since `run_prompt_turn` suppressed it), Error park with the
/// idle-timeout `stop_reason`, and a requeue for `agent.retry`. The mock parks
/// EVERY prompt ("of silence" matches the warning text too), so no turn ever
/// produces intervening activity.
#[tokio::test]
async fn idle_timeout_cap_exceeded_takes_terminal_path() {
    let script = mock_agent_script();
    let behavior = json!({ "parkIfPromptContains": "of silence" }).to_string();
    let _env = EnvGuard::set_all(&[
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
        ("INTENTD_PROMPT_IDLE_TIMEOUT_MS", "100"),
    ]);
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-idle-cap"),
        AgentId::from("a-idle-cap"),
    );
    seed_agent(&mgr, &ws, &id).await;
    set_session_provider(&mgr, &ws, &id, "mock").await;

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    mgr.send_message(
        id.clone(),
        ws.clone(),
        "a stretch of silence begins".to_string(),
        None,
        super::TurnOptions::default(),
    )
    .await
    .expect("send_message spawns the worker inline");

    // 4 consecutive silent timeouts at ~1s each (poll tick), 3 warning
    // redrives in between, then the terminal park.
    timeout(Duration::from_secs(60), async {
        loop {
            let session = mgr.services.store.get_agent_session(&id).await.unwrap();
            if session.status == AgentStatus::Error
                && !mgr.is_busy(&id)
                && mgr.workers.lock().unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("4th consecutive idle timeout parks the session in error");

    // Exactly MAX warnings were injected (cap boundary: the 4th timeout gets
    // no warning).
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    let warnings = messages
        .iter()
        .filter(|m| {
            m.role == "user"
                && m.content[0]["text"]
                    .as_str()
                    .is_some_and(|t| t.starts_with("[SYSTEM WARNING]"))
        })
        .count();
    assert_eq!(
        warnings,
        super::MAX_CONSECUTIVE_IDLE_TIMEOUT_REDRIVES as usize,
        "one warning per redrive, none for the terminal timeout: {messages:?}"
    );

    // Exactly one agent:failed (the worker's cap-exceeded emit) and it names
    // the idle timeout.
    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let failed: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "agent:failed")
        .collect();
    assert_eq!(failed.len(), 1, "exactly one terminal agent:failed");
    assert!(
        failed[0].data["error"]
            .as_str()
            .is_some_and(|e| e.contains("idle timeout")),
        "agent:failed names the idle timeout: {:?}",
        failed[0].data
    );
    // Error park + requeue for agent.retry (the last warning turn is the
    // failed message).
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    let stop_reason = session.stop_reason.expect("error stop_reason persisted");
    assert!(
        stop_reason.contains("idle timeout"),
        "stop_reason names the idle timeout: {stop_reason}"
    );
    assert_eq!(
        mgr.services.queue_snapshot(&id).len(),
        1,
        "failed turn requeued for agent.retry"
    );
}

#[tokio::test]
async fn send_message_queues_when_already_busy() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-q"), AgentId::from("a-q"));
    seed_agent(&mgr, &ws, &id).await;
    // Claim the in-flight slot so `send_message` must enqueue.
    assert!(mgr.try_begin(&id, &ws).await);

    let result = mgr
        .send_message(
            id.clone(),
            ws.clone(),
            "queued".to_string(),
            None,
            super::TurnOptions::default(),
        )
        .await
        .expect("send_message returns the queued envelope");
    assert_eq!(result["success"], json!(true));
    assert_eq!(result["queued"], json!(true));
    assert_eq!(result["queuedMessage"]["content"], json!("queued"));
    assert_eq!(result["queuedMessage"]["position"], json!(0));
    assert_eq!(mgr.services.queue_snapshot(&id).len(), 1);
}

/// The direct-send branch persists the user row UNDER the caller-supplied
/// `messageId` and publishes `agent:message` (role=user) with that id
/// (PROTOCOL §5.5) — previously it emitted nothing and the RPC result's
/// `messageId` named an id the store never used, so an
/// `agent.editAndRegenerate` regenerated user message never reached clients
/// until a full reload.
#[tokio::test]
async fn send_message_direct_branch_emits_agent_message_with_row_id() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-emit"), AgentId::from("a-emit"));
    seed_agent(&mgr, &ws, &id).await;

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let result = mgr
        .send_message(
            id.clone(),
            ws.clone(),
            "direct".to_string(),
            Some("user-msg-direct-1".to_string()),
            super::TurnOptions::default(),
        )
        .await
        .expect("direct send");
    assert_eq!(result["queued"], json!(false));
    assert_eq!(
        result["messageId"],
        json!("user-msg-direct-1"),
        "result messageId is the persisted row id"
    );

    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .unwrap();
    assert_eq!(
        messages.last().unwrap().id,
        "user-msg-direct-1",
        "row persisted under the client-supplied id"
    );

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let msg_event = events
        .iter()
        .find(|e| e.event_type == "agent:message")
        .expect("agent:message published on the direct-send branch");
    assert_eq!(msg_event.data["messageId"], json!("user-msg-direct-1"));
    assert_eq!(msg_event.data["role"], json!("user"));
    assert_eq!(msg_event.data["agentId"], json!(id.0));
}

/// Without a client-supplied `messageId` the direct-send branch mints one and
/// the RPC result names the ACTUAL persisted row id (they must match).
#[tokio::test]
async fn send_message_direct_branch_minted_id_matches_persisted_row() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-mint"), AgentId::from("a-mint"));
    seed_agent(&mgr, &ws, &id).await;

    let result = mgr
        .send_message(
            id.clone(),
            ws.clone(),
            "no id".to_string(),
            None,
            super::TurnOptions::default(),
        )
        .await
        .expect("direct send");
    assert_eq!(result["queued"], json!(false));
    let minted = result["messageId"].as_str().expect("minted id").to_string();
    assert!(minted.starts_with("user-msg-"), "TS-shaped default id");

    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .unwrap();
    assert_eq!(
        messages.last().unwrap().id,
        minted,
        "result messageId matches the persisted row"
    );
}

/// An oversized client-supplied `messageId` is rejected with `-32602` BEFORE
/// any state change (mirrors `agent_send_message_op`'s unconditional guard,
/// now that the row is keyed on the client id): no slot claim, no status
/// flap, nothing persisted.
#[tokio::test]
async fn send_message_rejects_oversized_message_id_before_any_state_change() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-len"), AgentId::from("a-len"));
    seed_agent(&mgr, &ws, &id).await;

    let err = mgr
        .send_message(
            id.clone(),
            ws.clone(),
            "too big".to_string(),
            Some("x".repeat(300)),
            super::TurnOptions::default(),
        )
        .await
        .expect_err("oversized id rejected");
    assert!(matches!(err, Error::InvalidParams(_)), "got {err:?}");
    assert!(
        !mgr.is_busy(&id),
        "slot never claimed on validation failure"
    );
    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .unwrap();
    assert!(messages.is_empty(), "nothing persisted");

    // The guard is unconditional: a BUSY agent also rejects instead of
    // silently queueing the oversized id.
    assert!(mgr.try_begin(&id, &ws).await);
    let err = mgr
        .send_message(
            id.clone(),
            ws.clone(),
            "still too big".to_string(),
            Some("y".repeat(300)),
            super::TurnOptions::default(),
        )
        .await
        .expect_err("oversized id rejected while busy");
    assert!(matches!(err, Error::InvalidParams(_)), "got {err:?}");
    assert_eq!(
        mgr.services.queue_snapshot(&id).len(),
        0,
        "nothing queued for an oversized id"
    );
}

/// When `send_message` hits the busy auto-queue fallback, the caller's
/// image + file blocks are preserved on the queued entry (the wire snapshot
/// includes both) so the eventual drain turn reaches the agent with the same
/// ACP content blocks.
#[tokio::test]
async fn send_message_auto_queue_preserves_image_and_file_blocks() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-q-blocks"),
        AgentId::from("a-q-blocks"),
    );
    seed_agent(&mgr, &ws, &id).await;
    assert!(mgr.try_begin(&id, &ws).await);

    let options = super::TurnOptions {
        image_blocks: Some(json!([{"data": "IMG", "mimeType": "image/png"}])),
        file_blocks: Some(json!([
            {"data": "FILE", "mimeType": "text/plain", "fileName": "n.txt"}
        ])),
        ..super::TurnOptions::default()
    };
    let result = mgr
        .send_message(id.clone(), ws.clone(), "hi".to_string(), None, options)
        .await
        .expect("queued");
    assert_eq!(result["queued"], json!(true));
    let snap = mgr.services.queue_snapshot(&id);
    assert_eq!(snap.len(), 1);
    assert_eq!(
        snap[0]["imageBlocks"],
        json!([{"data": "IMG", "mimeType": "image/png"}]),
        "image blocks land on the queued entry"
    );
    assert_eq!(
        snap[0]["fileBlocks"],
        json!([{"data": "FILE", "mimeType": "text/plain", "fileName": "n.txt"}]),
        "file blocks land on the queued entry"
    );
}

/// enqueue → dequeue round-trip preserves both attachment arrays so the drain
/// path can pipe them into the next turn's `TurnOptions`.
#[tokio::test]
async fn queue_dequeue_round_trip_preserves_image_and_file_blocks() {
    let (_tmp, mgr) = manager().await;
    let id = AgentId::from("a-rt");
    let images = Some(json!([{"data": "I", "mimeType": "image/png"}]));
    let files = Some(json!([
        {"data": "F", "mimeType": "text/plain", "fileName": "r.txt"}
    ]));
    mgr.services.enqueue_message(
        &id,
        "msg".to_string(),
        images.clone(),
        files.clone(),
        None,
        None,
        false,
    );
    let drained = mgr
        .services
        .dequeue_message(&id)
        .expect("dequeue returns the head");
    assert_eq!(drained.content, "msg");
    assert_eq!(drained.image_blocks, images);
    assert_eq!(drained.file_blocks, files);
}

// --- Recreate flag + history rendering ---------------------------------------

/// When the resend flag is set but the agent has no prior history (the just-
/// persisted current user message is excluded), `build_turn_body` just clears
/// the flag and returns the live content unchanged.
#[tokio::test]
async fn build_turn_body_clears_flag_when_only_current_message_exists() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-bt"), AgentId::from("a-bt"));
    seed_agent(&mgr, &ws, &id).await;
    mgr.services
        .store
        .append_agent_message(
            &id,
            "user",
            &json!([{ "type": "text", "text": "only message" }]),
            &now_iso(),
        )
        .await
        .unwrap();
    mgr.recreated.lock().unwrap().insert(id.clone());

    let body = mgr.build_turn_body(&id, "only message").await;

    assert_eq!(body, "only message", "no prior → live content unchanged");
    assert!(
        !mgr.recreated.lock().unwrap().contains(&id),
        "flag was consumed even though no XML was prepended",
    );
}

// --- resolve_spawn ------------------------------------------------------------

/// Regression (monorepo#3044): a bare session with no `provider`/`model` and
/// no settings-derived default fails loudly instead of silently spawning the
/// positional first registered provider (auggie), which may not be installed.
#[tokio::test]
async fn resolve_spawn_without_provider_or_default_fails_loudly() {
    let settings = intent_core::settings_file::SettingsFile::default();
    let session = session_with_specialist(None);
    let Err(err) = resolve_spawn(&session, None, &settings, None) else {
        panic!("no provider, no model, no configured default must not resolve")
    };
    assert!(
        matches!(&err, intent_core::Error::InvalidParams(m)
            if m.contains("no default provider/model is configured")),
        "loud no-default error, got: {err:?}"
    );
}

/// A bare session with no `provider`/`model` resolves via the settings-derived
/// default (`providers.active`), no model, and the temp dir as cwd (no
/// workspace path).
#[tokio::test]
async fn resolve_spawn_uses_configured_default_and_temp_cwd() {
    let mut settings = intent_core::settings_file::SettingsFile::default();
    settings.providers.active = Some("mock".to_string());
    let session = session_with_specialist(None);
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", "/tmp/mock-agent.js")]);
    let resolved = resolve_spawn(&session, None, &settings, None).expect("default resolves");
    assert_eq!(resolved.provider.id, "mock");
    assert!(resolved.model.is_none(), "no model selected");
    assert_eq!(resolved.cwd, std::env::temp_dir());
}

/// A persisted effective-model display name (legacy pre-monorepo#1534 row,
/// whitespace-bearing, e.g. `claude-code:Opus 4.8`) still selects the
/// provider via its compound prefix but never reaches `SpawnOptions.model` —
/// it is a stats/attribution value, not a spawnable model id; the spawn runs
/// on the provider default.
#[tokio::test]
async fn resolve_spawn_drops_effective_display_name_model() {
    if intent_providers::find_npx().is_none() {
        eprintln!("skipping: npx not available on this host");
        return;
    }
    let settings = intent_core::settings_file::SettingsFile::default();
    let mut session = session_with_specialist(None);
    session.model = Some("claude-code:Opus 4.8".to_string());
    let resolved = resolve_spawn(&session, None, &settings, None).expect("resolves");
    assert_eq!(resolved.provider.id, "claude-code", "prefix still wins");
    assert!(
        resolved.model.is_none(),
        "display name must not become a spawn model id"
    );
}

/// A compound `provider:model` id selects both the provider and the bare model
/// id, without needing an explicit `provider` on the session. claude-code is
/// npx-only, so a successful resolution always carries the pinned npx package
/// and never a locally-discovered provider binary.
#[tokio::test]
async fn resolve_spawn_parses_compound_model_id() {
    if intent_providers::find_npx().is_none() {
        eprintln!("skipping: npx not available on this host");
        return;
    }
    let settings = intent_core::settings_file::SettingsFile::default();
    let mut session = session_with_specialist(None);
    session.model = Some("claude-code:sonnet".to_string());
    let resolved = resolve_spawn(&session, None, &settings, None).expect("compound resolves");
    assert_eq!(resolved.provider.id, "claude-code");
    assert_eq!(resolved.model.as_deref(), Some("sonnet"));
    assert_eq!(
        resolved.provider_binary, None,
        "claude-code must never spawn a locally-discovered binary"
    );
    assert_eq!(
        resolved.npx_fallback_package,
        Some(intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE)
    );
    assert!(
        resolved.npx_fallback_binary.is_some(),
        "npx path must be resolved for npx-only providers"
    );
}

/// npx-only resolution: with npx present, the pinned package spec is returned;
/// with npx missing, resolution fails with the user-facing Node.js error.
#[test]
fn resolve_npx_only_returns_pinned_package_and_errors_without_npx() {
    let provider = intent_providers::provider_config("claude-code");

    let npx = PathBuf::from("/usr/local/bin/npx");
    let (bin, pkg) = resolve_npx_only(provider, Some(npx.clone())).expect("npx present resolves");
    assert_eq!(bin, npx);
    assert_eq!(pkg, intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE);

    let err = resolve_npx_only(provider, None).expect_err("missing npx is a hard error");
    assert!(
        matches!(err, intent_core::Error::InvalidInput(_)),
        "missing npx is an environment misconfiguration, not an internal error"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("npx not found")
            && msg.contains(intent_providers::CLAUDE_AGENT_ACP_NODE_REQUIREMENT),
        "error must explain the npx/Node.js requirement, got: {msg}"
    );
    assert!(
        msg.contains("Anthropic Claude Code"),
        "error must name the provider, got: {msg}"
    );
}

/// Non-npx-only providers reject npx-only resolution (defensive seam guard).
#[test]
fn resolve_npx_only_rejects_non_npx_only_provider() {
    let provider = intent_providers::provider_config("auggie");
    let err = resolve_npx_only(provider, Some(PathBuf::from("/usr/local/bin/npx")))
        .expect_err("auggie is not npx-only");
    assert!(err.to_string().contains("not configured for npx-only"));
}

/// When a model carries an explicit `provider:` prefix, that prefix wins over
/// session.provider. This is the fix for cross-provider model switches: the
/// compound prefix is the user's latest intent.
#[tokio::test]
async fn resolve_spawn_compound_prefix_wins_over_session_provider() {
    let settings = intent_core::settings_file::SettingsFile::default();
    let mut session = session_with_specialist(None);
    session.provider = Some("auggie".to_string());
    session.model = Some("opencode:opencode-go/kimi-k3".to_string());
    let resolved = resolve_spawn(&session, None, &settings, None).expect("compound prefix wins");
    // The compound prefix (opencode) should win over session.provider (auggie).
    assert_eq!(resolved.provider.id, "opencode");
    // The model string is the bare half.
    assert_eq!(resolved.model.as_deref(), Some("opencode-go/kimi-k3"));
}

/// Session.provider is used as a fallback for bare model ids (no `:` prefix).
#[tokio::test]
async fn resolve_spawn_session_provider_fallback_for_bare_model() {
    let settings = intent_core::settings_file::SettingsFile::default();
    let mut session = session_with_specialist(None);
    session.provider = Some("codex".to_string());
    session.model = Some("gpt-5.3-codex/high".to_string());
    let resolved =
        resolve_spawn(&session, None, &settings, None).expect("session provider fallback");
    // Bare model → session.provider is used.
    assert_eq!(resolved.provider.id, "codex");
    // The bare model is passed through as-is.
    assert_eq!(resolved.model.as_deref(), Some("gpt-5.3-codex/high"));
}

/// A workspace whose `path` exists on disk becomes the spawn cwd; a missing
/// path silently falls back to the temp dir.
#[tokio::test]
async fn resolve_spawn_prefers_existing_workspace_path() {
    let settings = intent_core::settings_file::SettingsFile::default();
    let mut session = session_with_specialist(None);
    session.provider = Some("auggie".to_string());
    let ws_dir = std::env::temp_dir().join(format!("intentd-rs-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&ws_dir).unwrap();
    let mut workspace = intent_core::Workspace {
        id: WorkspaceId::from("ws-rs"),
        title: "WS".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: now_iso(),
        updated_at: now_iso(),
        last_activity: None,
        tags: vec![],
        path: Some(ws_dir.display().to_string()),
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    };
    let resolved = resolve_spawn(&session, Some(&workspace), &settings, None)
        .expect("existing workspace path resolves");
    assert_eq!(resolved.cwd, ws_dir);

    // Switch to a non-existent path → fall back to temp.
    workspace.path = Some(
        std::env::temp_dir()
            .join(format!("intentd-missing-{}", uuid::Uuid::new_v4()))
            .display()
            .to_string(),
    );
    let resolved =
        resolve_spawn(&session, Some(&workspace), &settings, None).expect("falls back to temp");
    assert_eq!(resolved.cwd, std::env::temp_dir());

    let _ = std::fs::remove_dir_all(&ws_dir);
}

/// The chief workspace (no worktree on disk) spawns in the dedicated
/// `<data_dir>/chief-cwd` directory (STAB-50), created on demand — never
/// `/tmp`. Without a configured chief cwd root, chief falls back to the
/// temp dir.
#[tokio::test]
async fn resolve_spawn_chief_uses_dedicated_cwd() {
    let settings = intent_core::settings_file::SettingsFile::default();
    let mut session = session_with_specialist(None);
    session.provider = Some("auggie".to_string());
    let chief = intent_core::chief_workspace();

    // Fresh (not-yet-created) chief cwd root → created on demand and used.
    let data_dir = std::env::temp_dir().join(format!("intentd-chief-{}", uuid::Uuid::new_v4()));
    let chief_root = intent_core::chief_cwd_root(&data_dir);
    assert!(!chief_root.exists(), "fresh data dir: root must not exist");
    let resolved = resolve_spawn(&session, Some(&chief), &settings, Some(&chief_root))
        .expect("chief resolves");
    assert_eq!(resolved.cwd, chief_root);
    assert_ne!(resolved.cwd, PathBuf::from("/tmp"), "never /tmp");
    assert!(chief_root.is_dir(), "chief cwd created on demand");
    let entries = std::fs::read_dir(&chief_root).unwrap().count();
    assert_eq!(entries, 0, "dedicated chief cwd is empty");

    // Idempotent: resolving again with the dir already present still works.
    let resolved = resolve_spawn(&session, Some(&chief), &settings, Some(&chief_root))
        .expect("chief resolves again");
    assert_eq!(resolved.cwd, chief_root);

    // No chief cwd root configured (bare wiring) → temp-dir catch-all.
    let resolved = resolve_spawn(&session, Some(&chief), &settings, None)
        .expect("chief without root resolves");
    assert_eq!(resolved.cwd, std::env::temp_dir());

    // Creation failure (a regular FILE squats on the chief cwd path) →
    // the root is not a dir, so the spawn falls through to the temp-dir
    // catch-all instead of failing.
    let blocked_root = data_dir.join("blocked-chief-cwd");
    std::fs::write(&blocked_root, b"not a dir").unwrap();
    let resolved = resolve_spawn(&session, Some(&chief), &settings, Some(&blocked_root))
        .expect("chief resolves despite blocked root");
    assert_eq!(resolved.cwd, std::env::temp_dir());

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// A workspace with only `repository_path` on disk (an `isNewRepo`
/// direct-mode row missing `worktree_path`, intent-hq/monorepo#2611)
/// resolves its spawn cwd to the repository folder — never the temp dir.
#[tokio::test]
async fn resolve_spawn_falls_back_to_repository_path() {
    let settings = intent_core::settings_file::SettingsFile::default();
    let mut session = session_with_specialist(None);
    session.provider = Some("auggie".to_string());
    let repo_dir = std::env::temp_dir().join(format!("intentd-rs-repo-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut workspace = intent_core::Workspace {
        id: WorkspaceId::from("ws-repo-fb"),
        title: "WS".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: now_iso(),
        updated_at: now_iso(),
        last_activity: None,
        tags: vec![],
        path: None,
        repository_path: Some(repo_dir.display().to_string()),
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: Some(intent_core::CheckoutMode::Direct),
        disk_usage: None,
        pending_delete_at: None,
    };
    let resolved = resolve_spawn(&session, Some(&workspace), &settings, None)
        .expect("repository_path fallback resolves");
    assert_eq!(resolved.cwd, repo_dir, "cwd is the repository folder");
    assert_ne!(resolved.cwd, std::env::temp_dir(), "never the temp dir");

    // `worktree_path` still wins over `repository_path` when both exist.
    let wt_dir = std::env::temp_dir().join(format!("intentd-rs-wt-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&wt_dir).unwrap();
    workspace.worktree_path = Some(wt_dir.display().to_string());
    let resolved = resolve_spawn(&session, Some(&workspace), &settings, None)
        .expect("worktree_path still wins");
    assert_eq!(resolved.cwd, wt_dir);

    // A stale (non-directory) `path` must not suppress a live candidate
    // further down the chain — each entry is `is_dir()`-checked individually.
    workspace.path = Some(
        std::env::temp_dir()
            .join(format!("intentd-stale-path-{}", uuid::Uuid::new_v4()))
            .display()
            .to_string(),
    );
    let resolved = resolve_spawn(&session, Some(&workspace), &settings, None)
        .expect("stale path skipped, worktree_path resolves");
    assert_eq!(resolved.cwd, wt_dir, "stale `path` never wins the chain");
    workspace.path = None;

    // A missing repository_path directory falls through to the temp dir.
    workspace.worktree_path = None;
    workspace.repository_path = Some(
        std::env::temp_dir()
            .join(format!("intentd-missing-{}", uuid::Uuid::new_v4()))
            .display()
            .to_string(),
    );
    let resolved =
        resolve_spawn(&session, Some(&workspace), &settings, None).expect("falls back to temp");
    assert_eq!(resolved.cwd, std::env::temp_dir());

    let _ = std::fs::remove_dir_all(&repo_dir);
    let _ = std::fs::remove_dir_all(&wt_dir);
}

// --- Prompt block shape helpers ----------------------------------------------

/// The persisted/prompt wire shape for a user text message without attachments
/// is a single `{ type: "text", text }` block in an array (parity with
/// `agent.sendMessage`).
#[test]
fn user_message_blocks_emits_single_text_block_array_without_attachments() {
    let blocks = user_message_blocks("hello world", None, None);
    let arr = blocks.as_array().expect("array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["type"], json!("text"));
    assert_eq!(arr[0]["text"], json!("hello world"));
}

/// STAB-133: FE-supplied image and file attachments are appended after the
/// text block in the persisted user-message shape; malformed entries (missing
/// required fields) are skipped.
#[test]
fn user_message_blocks_appends_image_and_file_blocks() {
    let images = json!([
        { "type": "image", "data": "imgdata", "mimeType": "image/png" },
        { "type": "image", "mimeType": "image/png" }, // missing data → skipped
    ]);
    let files = json!([
        { "type": "file", "data": "filedata", "mimeType": "text/plain", "fileName": "a.txt" },
        { "type": "file", "data": "orphan" }, // missing fileName → skipped
        { "type": "file", "data": "x", "fileName": "b.txt" }, // missing mimeType → skipped
    ]);
    let blocks = user_message_blocks("look", Some(&images), Some(&files));
    let arr = blocks.as_array().expect("array");
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["type"], json!("text"));
    assert_eq!(arr[0]["text"], json!("look"));
    assert_eq!(arr[1]["type"], json!("image"));
    assert_eq!(arr[1]["data"], json!("imgdata"));
    assert_eq!(arr[1]["mimeType"], json!("image/png"));
    assert_eq!(arr[2]["type"], json!("file"));
    assert_eq!(arr[2]["data"], json!("filedata"));
    assert_eq!(arr[2]["fileName"], json!("a.txt"));
    assert_eq!(arr[2]["mimeType"], json!("text/plain"));
}

/// Attachment-reference file blocks (PROTOCOL §5.5) persist with their
/// `attachmentId` + metadata (no inline data) in the user-message shape;
/// mimeType/size are optional and pass through when present.
#[test]
fn user_message_blocks_persists_attachment_reference_file_blocks() {
    let files = json!([
        { "type": "file", "attachmentId": "att-1", "fileName": "spec.pdf",
          "mimeType": "application/pdf", "size": 123 },
        { "type": "file", "attachmentId": "att-2", "fileName": "raw.bin" },
    ]);
    let blocks = user_message_blocks("see attached", None, Some(&files));
    let arr = blocks.as_array().expect("array");
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[1]["type"], json!("file"));
    assert_eq!(arr[1]["attachmentId"], json!("att-1"));
    assert_eq!(arr[1]["fileName"], json!("spec.pdf"));
    assert_eq!(arr[1]["mimeType"], json!("application/pdf"));
    assert_eq!(arr[1]["size"], json!(123));
    assert!(arr[1].get("data").is_none());
    assert_eq!(arr[2]["attachmentId"], json!("att-2"));
    assert!(arr[2].get("mimeType").is_none());
    assert!(arr[2].get("size").is_none());
}

/// A blank `attachmentId` counts as absent everywhere: an entry carrying
/// inline `data` plus a whitespace `attachmentId` passes validation as
/// data-only, so persistence must keep the inline data rather than store a
/// dangling blank reference (same non-blank filter as `validate_file_blocks`
/// and prompt assembly).
#[test]
fn user_message_blocks_blank_attachment_id_persists_inline_data() {
    let files = json!([
        { "type": "file", "data": "aGk=", "mimeType": "text/plain",
          "attachmentId": "  ", "fileName": "hi.txt" },
    ]);
    let blocks = user_message_blocks("msg", None, Some(&files));
    let arr = blocks.as_array().expect("array");
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[1]["type"], json!("file"));
    assert_eq!(arr[1]["data"], json!("aGk="));
    assert_eq!(arr[1]["mimeType"], json!("text/plain"));
    assert!(arr[1].get("attachmentId").is_none());
}

/// `validate_file_blocks` (PROTOCOL §5.5): exactly one of `data` /
/// `attachmentId` per entry — both or neither is `-32602`; valid arrays,
/// non-arrays, and non-object entries pass.
#[test]
fn validate_file_blocks_rejects_both_or_neither() {
    use crate::agent_ops::validate_file_blocks;
    // Valid: inline-data entry and attachment-reference entry.
    let ok = json!([
        { "type": "file", "data": "d", "mimeType": "t/p", "fileName": "a.txt" },
        { "type": "file", "attachmentId": "att-1", "fileName": "b.pdf" },
    ]);
    assert!(validate_file_blocks("m", Some(&ok)).is_ok());
    // Neither.
    let neither = json!([{ "type": "file", "fileName": "x.txt" }]);
    let err = validate_file_blocks("agent.sendMessage", Some(&neither)).unwrap_err();
    assert!(
        matches!(err, intent_core::Error::InvalidParams(_)),
        "{err:?}"
    );
    // Both.
    let both = json!([{ "type": "file", "data": "d", "attachmentId": "att-1", "fileName": "x" }]);
    assert!(validate_file_blocks("m", Some(&both)).is_err());
    // Blank attachmentId counts as absent → data-only entry still valid.
    let blank = json!([{ "type": "file", "data": "d", "attachmentId": " ", "fileName": "x" }]);
    assert!(validate_file_blocks("m", Some(&blank)).is_ok());
    // Non-array / absent / non-object entries are tolerated.
    assert!(validate_file_blocks("m", None).is_ok());
    assert!(validate_file_blocks("m", Some(&json!("nope"))).is_ok());
    assert!(validate_file_blocks("m", Some(&json!(["str"]))).is_ok());
}

/// `validate_image_blocks` (monorepo#3338): exactly one of `data` /
/// `attachmentId` per image entry — both or neither is `-32602`; valid
/// arrays, non-arrays, and non-object entries pass.
#[test]
fn validate_image_blocks_rejects_both_or_neither() {
    use crate::agent_ops::validate_image_blocks;
    // Valid: inline-data entry and attachment-reference entry.
    let ok = json!([
        { "type": "image", "data": "iVBOR", "mimeType": "image/png" },
        { "type": "image", "attachmentId": "att-1", "mimeType": "image/png" },
    ]);
    assert!(validate_image_blocks("m", Some(&ok)).is_ok());
    // Neither.
    let neither = json!([{ "type": "image", "mimeType": "image/png" }]);
    let err = validate_image_blocks("agent.sendMessage", Some(&neither)).unwrap_err();
    assert!(
        matches!(err, intent_core::Error::InvalidParams(_)),
        "{err:?}"
    );
    let msg = format!("{err}");
    assert!(msg.contains("imageBlocks[0]"), "{msg}");
    // Both.
    let both = json!([{ "type": "image", "data": "d", "attachmentId": "att-1" }]);
    assert!(validate_image_blocks("m", Some(&both)).is_err());
    // Blank attachmentId counts as absent → data-only entry still valid.
    let blank = json!([{ "type": "image", "data": "d", "attachmentId": " " }]);
    assert!(validate_image_blocks("m", Some(&blank)).is_ok());
    // Non-array / absent / non-object entries are tolerated.
    assert!(validate_image_blocks("m", None).is_ok());
    assert!(validate_image_blocks("m", Some(&json!("nope"))).is_ok());
    assert!(validate_image_blocks("m", Some(&json!(["str"]))).is_ok());
}

/// `image_block_ref_ids` (monorepo#3338): only non-blank reference-arm
/// entries (no inline `data`) contribute ids.
#[test]
fn image_block_ref_ids_extracts_reference_arm_only() {
    use crate::agent_ops::image_block_ref_ids;
    let blocks = json!([
        { "type": "image", "data": "d", "mimeType": "image/png" },
        { "type": "image", "attachmentId": "att-1" },
        { "type": "image", "attachmentId": "  " },
        { "type": "image", "data": "d", "attachmentId": "att-2" },
        "not-an-object",
    ]);
    assert_eq!(image_block_ref_ids(Some(&blocks)), vec!["att-1"]);
    assert!(image_block_ref_ids(None).is_empty());
}

/// STAB-133 + monorepo#3338: an image-reference block persists on the user
/// transcript row AS a reference (no bytes), with `mimeType` carried when
/// present; inline entries keep the data shape.
#[test]
fn user_message_blocks_persists_image_reference() {
    let images = json!([
        { "type": "image", "attachmentId": "att-7", "mimeType": "image/png" },
        { "type": "image", "attachmentId": "att-8" },
        { "type": "image", "data": "imgdata", "mimeType": "image/jpeg" },
    ]);
    let blocks = user_message_blocks("msg", Some(&images), None);
    let arr = blocks.as_array().expect("array");
    assert_eq!(arr.len(), 4);
    assert_eq!(arr[1]["attachmentId"], json!("att-7"));
    assert_eq!(arr[1]["mimeType"], json!("image/png"));
    assert!(arr[1].get("data").is_none());
    assert_eq!(arr[2]["attachmentId"], json!("att-8"));
    assert!(arr[2].get("mimeType").is_none());
    assert_eq!(arr[3]["data"], json!("imgdata"));
    assert!(arr[3].get("attachmentId").is_none());
}

/// `validate_image_block_refs` (monorepo#3338): unknown attachment ids are
/// `-32602` naming the id; registered ids under the byte cap pass; a
/// recorded size over the cap is rejected.
#[tokio::test]
async fn validate_image_block_refs_checks_registry() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let services = Services::new(store.clone());
    let ws_id = WorkspaceId::from("ws-imgref");

    let unknown = json!([{ "type": "image", "attachmentId": "att-missing" }]);
    let err = services
        .validate_image_block_refs("agent.sendMessage", Some(&unknown))
        .await
        .unwrap_err();
    assert!(
        matches!(err, intent_core::Error::InvalidParams(_)),
        "{err:?}"
    );
    assert!(format!("{err}").contains("att-missing"), "{err}");

    store
        .insert_attachment(&intent_store::AttachmentRecord {
            id: "att-ok".into(),
            workspace_id: ws_id.clone(),
            file_name: "pic.png".into(),
            mime_type: Some("image/png".into()),
            size: 5,
            uploaded_at: now_iso(),
            stored_path: ".intent/attachments/att-ok_pic.png".into(),
        })
        .await
        .unwrap();
    let ok = json!([{ "type": "image", "attachmentId": "att-ok" }]);
    assert!(services
        .validate_image_block_refs("m", Some(&ok))
        .await
        .is_ok());

    store
        .insert_attachment(&intent_store::AttachmentRecord {
            id: "att-big".into(),
            workspace_id: ws_id,
            file_name: "huge.png".into(),
            mime_type: Some("image/png".into()),
            size: i64::try_from(crate::agent_ops::IMAGE_REF_MAX_BYTES + 1).expect("fits in i64"),
            uploaded_at: now_iso(),
            stored_path: ".intent/attachments/att-big_huge.png".into(),
        })
        .await
        .unwrap();
    let big = json!([{ "type": "image", "attachmentId": "att-big" }]);
    let err = services
        .validate_image_block_refs("m", Some(&big))
        .await
        .unwrap_err();
    assert!(
        matches!(err, intent_core::Error::InvalidParams(_)),
        "{err:?}"
    );
}

/// `validate_image_block_refs` (monorepo#3338): the byte cap is enforced in
/// AGGREGATE across all references in the array — two attachments each under
/// the cap individually are rejected when their recorded sizes sum past it,
/// so a small request cannot expand one prompt beyond the transport bound.
#[tokio::test]
async fn validate_image_block_refs_enforces_aggregate_cap() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let services = Services::new(store.clone());
    let ws_id = WorkspaceId::from("ws-imgagg");
    let over_half =
        i64::try_from(crate::agent_ops::IMAGE_REF_MAX_BYTES / 2 + 1).expect("fits in i64");
    for id in ["att-h1", "att-h2"] {
        store
            .insert_attachment(&intent_store::AttachmentRecord {
                id: id.into(),
                workspace_id: ws_id.clone(),
                file_name: format!("{id}.png"),
                mime_type: Some("image/png".into()),
                size: over_half,
                uploaded_at: now_iso(),
                stored_path: format!(".intent/attachments/{id}.png"),
            })
            .await
            .unwrap();
    }
    let one = json!([{ "type": "image", "attachmentId": "att-h1" }]);
    assert!(services
        .validate_image_block_refs("m", Some(&one))
        .await
        .is_ok());
    let both = json!([
        { "type": "image", "attachmentId": "att-h1" },
        { "type": "image", "attachmentId": "att-h2" }
    ]);
    let err = services
        .validate_image_block_refs("m", Some(&both))
        .await
        .unwrap_err();
    assert!(
        matches!(err, intent_core::Error::InvalidParams(_)),
        "{err:?}"
    );
    assert!(format!("{err}").contains("aggregate"), "{err}");
}

/// `resolve_image_block_refs` (monorepo#3338): a reference entry resolves to
/// inline base64 bytes read from the attachment's workspace root (MIME from
/// the block, else the registry row); inline entries pass through untouched;
/// a reference whose file vanished is skipped fail-soft.
#[tokio::test]
async fn resolve_image_block_refs_inlines_attachment_bytes() {
    use base64::Engine as _;
    use intent_core::{Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let root = tempfile::Builder::new()
        .prefix("intentd-imgref-root-")
        .tempdir()
        .expect("tempdir");
    let ws_id = WorkspaceId::from("ws-imgres");
    let ts = now_iso();
    let ws = Workspace {
        id: ws_id.clone(),
        title: "WS".into(),
        branch: "main".into(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts,
        last_activity: None,
        tags: vec![],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: Some(root.path().display().to_string()),
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    };
    store.insert_workspace(&ws).await.unwrap();

    let stored = ".intent/attachments/att-r_pic.png";
    let full = root.path().join(stored);
    std::fs::create_dir_all(full.parent().unwrap()).unwrap();
    std::fs::write(&full, b"png-bytes").unwrap();
    store
        .insert_attachment(&intent_store::AttachmentRecord {
            id: "att-r".into(),
            workspace_id: ws_id.clone(),
            file_name: "pic.png".into(),
            mime_type: Some("image/png".into()),
            size: 9,
            uploaded_at: now_iso(),
            stored_path: stored.into(),
        })
        .await
        .unwrap();
    // Registered row whose file was deleted out-of-band → skipped fail-soft.
    store
        .insert_attachment(&intent_store::AttachmentRecord {
            id: "att-gone".into(),
            workspace_id: ws_id,
            file_name: "gone.png".into(),
            mime_type: None,
            size: 1,
            uploaded_at: now_iso(),
            stored_path: ".intent/attachments/att-gone_gone.png".into(),
        })
        .await
        .unwrap();

    let services = Services::new(store);
    let input = json!([
        { "type": "image", "attachmentId": "att-r" },
        { "type": "image", "attachmentId": "att-gone" },
        { "type": "image", "data": "inline", "mimeType": "image/jpeg" },
    ]);
    let out = services
        .resolve_image_block_refs(Some(input))
        .await
        .expect("resolved array");
    let arr = out.as_array().expect("array");
    assert_eq!(arr.len(), 2, "vanished reference skipped: {arr:?}");
    let expected = base64::engine::general_purpose::STANDARD.encode(b"png-bytes");
    assert_eq!(arr[0]["data"], json!(expected));
    assert_eq!(arr[0]["mimeType"], json!("image/png"));
    assert!(arr[0].get("attachmentId").is_none());
    assert_eq!(arr[1]["data"], json!("inline"));
    assert_eq!(arr[1]["mimeType"], json!("image/jpeg"));

    // No references → input returned unchanged (no clone/rebuild).
    let inline_only = json!([{ "type": "image", "data": "x", "mimeType": "image/png" }]);
    let out = services
        .resolve_image_block_refs(Some(inline_only.clone()))
        .await;
    assert_eq!(out, Some(inline_only));
}

/// Prompt rendering (PROTOCOL §5.5): an attachment-reference file block
/// becomes a `text` attachment notice naming the metadata and directing the
/// model to `ws.file.getAttachment(attachmentId)`; inline-data file blocks
/// keep the `resource` blob shape.
#[test]
fn append_attachment_blocks_renders_attachment_reference_notice() {
    let options = super::TurnOptions {
        file_blocks: Some(json!([
            { "type": "file", "attachmentId": "att-9", "fileName": "big.har",
              "mimeType": "application/json", "size": 4096 },
            { "type": "file", "data": "aGk=", "mimeType": "text/plain", "fileName": "inline.txt" },
        ])),
        ..super::TurnOptions::default()
    };
    let mut blocks = Vec::new();
    super::append_attachment_blocks(&mut blocks, &options);
    assert_eq!(blocks.len(), 2);
    let notice = serde_json::to_value(&blocks[0]).unwrap();
    assert_eq!(notice["type"], json!("text"));
    let text = notice["text"].as_str().unwrap();
    assert!(text.contains("big.har"), "{text}");
    assert!(text.contains("application/json"), "{text}");
    assert!(text.contains("4096 bytes"), "{text}");
    assert!(text.contains("ws.file.getAttachment(\"att-9\")"), "{text}");
    let inline = serde_json::to_value(&blocks[1]).unwrap();
    assert_eq!(inline["type"], json!("resource"));
    assert_eq!(inline["resource"]["blob"], json!("aGk="));
}

/// STAB-133: the queue-drain `persist_user` path appends the FE-supplied
/// attachments captured at enqueue time to the persisted user row.
#[tokio::test]
async fn persist_user_appends_attachment_blocks_to_transcript_row() {
    let (_tmp, mgr) = manager().await;
    let ws = WorkspaceId::new();
    let id = AgentId::new();
    seed_agent(&mgr, &ws, &id).await;

    let images = json!([{ "type": "image", "data": "imgdata", "mimeType": "image/png" }]);
    let files = json!([{ "type": "file", "data": "filedata", "mimeType": "text/plain", "fileName": "f.txt" }]);
    super::persist_user(
        &mgr,
        &id,
        &ws,
        "drained",
        Some(&images),
        Some(&files),
        None,
        None,
        true,
    )
    .await;

    let messages = mgr
        .services
        .store
        .get_agent_messages(&id, None)
        .await
        .expect("messages");
    assert_eq!(messages.len(), 1);
    let blocks = messages[0].content.as_array().expect("blocks array");
    assert_eq!(
        blocks.len(),
        3,
        "text + image + file: {:?}",
        messages[0].content
    );
    assert_eq!(blocks[0]["type"], json!("text"));
    assert_eq!(blocks[0]["text"], json!("drained"));
    assert_eq!(blocks[1]["type"], json!("image"));
    assert_eq!(blocks[1]["data"], json!("imgdata"));
    assert_eq!(blocks[2]["type"], json!("file"));
    assert_eq!(blocks[2]["fileName"], json!("f.txt"));
}

/// Whether a debounced `lastActivity` derivation is pending for `ws` (the
/// schedule inserts into the debouncers map synchronously; the default 3s
/// window keeps the entry observable).
fn last_activity_pending(mgr: &AgentManager, ws: &WorkspaceId) -> bool {
    mgr.services
        .last_activity_debouncers
        .lock()
        .expect("debouncers lock")
        .contains_key(ws)
}

/// Turn-boundary gating (§10.1): a status persist that ENDS a turn
/// (non-active state) schedules the debounced `lastActivity` event; a
/// turn-start/mid-turn flip to an active state does not.
#[tokio::test]
async fn persist_status_schedules_last_activity_only_on_turn_end() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-la-status"), AgentId::from("a-la-s"));
    seed_agent(&mgr, &ws, &id).await;

    mgr.persist_status(&id, &ws, AgentStatus::Active, true)
        .await;
    assert!(
        !last_activity_pending(&mgr, &ws),
        "turn-start flip (is_active) must not schedule lastActivity"
    );

    mgr.persist_status(&id, &ws, AgentStatus::RuntimeIdle, false)
        .await;
    assert!(
        last_activity_pending(&mgr, &ws),
        "turn-end flip must schedule lastActivity"
    );
}

/// Turn-boundary gating (§10.1) on the stop-reason companion: same
/// non-active-only rule as [`AgentManager::persist_status`], plus the
/// `turn_boundary: false` opt-out for the no-turn `agent.retry` Error-clear.
#[tokio::test]
async fn persist_status_with_stop_reason_schedules_last_activity_only_on_turn_end() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-la-sr"), AgentId::from("a-la-sr"));
    seed_agent(&mgr, &ws, &id).await;

    mgr.persist_status_with_stop_reason(&id, &ws, AgentStatus::Active, true, Some(None), true)
        .await;
    assert!(
        !last_activity_pending(&mgr, &ws),
        "turn-start flip (is_active) must not schedule lastActivity"
    );

    // Non-active flip that is NOT a turn boundary (the agent.retry
    // Error-clear shape): must not schedule.
    mgr.persist_status_with_stop_reason(
        &id,
        &ws,
        AgentStatus::RuntimeIdle,
        false,
        Some(None),
        false,
    )
    .await;
    assert!(
        !last_activity_pending(&mgr, &ws),
        "no-turn retry-shaped flip must not schedule lastActivity"
    );

    mgr.persist_status_with_stop_reason(
        &id,
        &ws,
        AgentStatus::Error,
        false,
        Some(Some("boom".into())),
        true,
    )
    .await;
    assert!(
        last_activity_pending(&mgr, &ws),
        "terminal (turn-end) flip must schedule lastActivity"
    );
}

/// User-origin gating (§10.1): the queue-drain `persist_user` schedules the
/// debounced `lastActivity` only for user-origin entries — internal wakes and
/// agent-to-agent deliveries are not workspace-ordering activity.
#[tokio::test]
async fn persist_user_schedules_last_activity_only_for_user_origin() {
    let (_tmp, mgr) = manager().await;
    let ws = WorkspaceId::from("ws-la-user");
    let id = AgentId::from("a-la-u");
    seed_agent(&mgr, &ws, &id).await;

    super::persist_user(&mgr, &id, &ws, "wake", None, None, None, None, false).await;
    assert!(
        !last_activity_pending(&mgr, &ws),
        "agent-origin entry must not schedule lastActivity"
    );

    super::persist_user(&mgr, &id, &ws, "human", None, None, None, None, true).await;
    assert!(
        last_activity_pending(&mgr, &ws),
        "user-origin entry must schedule lastActivity"
    );
}

/// Direct-send origin gating (§10.1): [`AgentManager::send_message`]
/// schedules the debounced `lastActivity` only when
/// `TurnOptions::origin.is_user()` — an `Automatic`-origin direct send
/// (internal wake / agent-to-agent delivery) must not. Each half uses its
/// own workspace and asserts synchronously after the call returns: the gate
/// runs inline before `spawn_worker`, and on the current-thread test runtime
/// the spawned worker has not run yet, so a later turn-end schedule cannot
/// mask the read.
#[tokio::test]
async fn send_message_schedules_last_activity_only_for_user_origin() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);

    let (ws_auto, auto_id) = (WorkspaceId::from("ws-la-send-a"), AgentId::from("a-la-s-a"));
    seed_agent(&mgr, &ws_auto, &auto_id).await;
    let _agent_a = track_mock_agent(&mgr, &auto_id, false);
    mgr.send_message(
        auto_id,
        ws_auto.clone(),
        "wake".to_string(),
        None,
        super::TurnOptions {
            origin: intent_core::MessageOrigin::Automatic,
            ..super::TurnOptions::default()
        },
    )
    .await
    .expect("automatic-origin direct send");
    assert!(
        !last_activity_pending(&mgr, &ws_auto),
        "automatic-origin direct send must not schedule lastActivity"
    );

    let (ws_user, user_id) = (WorkspaceId::from("ws-la-send-u"), AgentId::from("a-la-s-u"));
    seed_agent(&mgr, &ws_user, &user_id).await;
    let _agent_u = track_mock_agent(&mgr, &user_id, false);
    mgr.send_message(
        user_id,
        ws_user.clone(),
        "human".to_string(),
        None,
        super::TurnOptions {
            origin: intent_core::MessageOrigin::User,
            ..super::TurnOptions::default()
        },
    )
    .await
    .expect("user-origin direct send");
    assert!(
        last_activity_pending(&mgr, &ws_user),
        "user-origin direct send must schedule lastActivity"
    );
}

#[test]
fn text_prompt_produces_one_acp_text_content_block() {
    let prompt = text_prompt("ping");
    assert_eq!(prompt.len(), 1);
    let rendered = serde_json::to_value(&prompt).unwrap();
    assert_eq!(rendered[0]["type"], json!("text"));
    assert_eq!(rendered[0]["text"], json!("ping"));
}

// --- derive_agent_type workspace path tier -----------------------------------

/// When a specialist sits under the workspace project tier
/// (`<ws>/.intent/specialists/<id>.md`), `derive_agent_type` discovers it via
/// the workspace path and returns its declared `agentType`.
#[tokio::test]
async fn derive_agent_type_uses_workspace_project_specialists_dir() {
    let ws_dir = std::env::temp_dir().join(format!("intentd-dat-{}", uuid::Uuid::new_v4()));
    let specialists_dir = ws_dir.join(".intent/specialists");
    std::fs::create_dir_all(&specialists_dir).unwrap();
    std::fs::write(
        specialists_dir.join("worker.md"),
        "---\nname: \"Worker\"\ndescription: \"d\"\nagentType: \"worker-loop\"\n---\n\nbody",
    )
    .unwrap();

    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    // No global specialists dirs — the only way to find `worker` is via the
    // workspace's project tier.
    let services = Services::new(store);

    let session = session_with_specialist(Some("worker"));
    let workspace = intent_core::Workspace {
        id: WorkspaceId::from("ws-dat"),
        title: "WS".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: now_iso(),
        updated_at: now_iso(),
        last_activity: None,
        tags: vec![],
        path: Some(ws_dir.display().to_string()),
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    };

    assert_eq!(
        derive_agent_type(&services, &session, Some(&workspace)),
        "worker-loop",
    );

    // A session with no specialist set keeps the default regardless of the
    // workspace tier (no lookup happens).
    let plain = session_with_specialist(None);
    assert_eq!(
        derive_agent_type(&services, &plain, Some(&workspace)),
        DEFAULT_AGENT_TYPE,
    );

    // Direct-checkout shape (monorepo#3778): only `repositoryPath` set — the
    // project tier must still resolve via the effective-path fallback, for
    // both spawn-time derivations.
    let mut repo_only = workspace.clone();
    repo_only.path = None;
    repo_only.repository_path = Some(ws_dir.display().to_string());
    assert_eq!(
        derive_agent_type(&services, &session, Some(&repo_only)),
        "worker-loop",
        "derive_agent_type must fall back to repositoryPath"
    );
    std::fs::write(
        specialists_dir.join("orch.md"),
        "---\nname: \"Orch\"\ndescription: \"d\"\nrole: \"orchestrator\"\n---\n\nbody",
    )
    .unwrap();
    let orch_session = session_with_specialist(Some("orch"));
    assert!(
        derive_is_orchestrator(&services, &orch_session, Some(&repo_only)),
        "derive_is_orchestrator must fall back to repositoryPath"
    );

    let _ = std::fs::remove_dir_all(&ws_dir);
}

// --- Context references → stdinContext builder (Fidelity B) ---------------

/// Port parity: the builder emits one entry per reference in order, with
/// the reference labels (`Selected text:`, `Task:`, `Code:`, `File <p>:`,
/// `Linear Issue:`, `GitHub Issue:`, `Sentry Issue:`, `Terminal ...`,
/// `Note: <id>`) and joins them with `\n\n`.
#[test]
fn build_stdin_context_from_context_references_ports_reference_shapes() {
    let refs = json!([
        {"type": "selection", "content": "hello"},
        {"type": "task", "taskText": "do the thing"},
        {"type": "code_chunk", "codeChunk": "fn foo() {}"},
        {"type": "file", "path": "src/a.rs", "content": "pub fn a() {}"},
        {"type": "file", "filePath": "src/only-path.rs"},
        {"type": "linear-issue", "content": "XYZ-1 title"},
        {"type": "github-issue", "content": "#42 title"},
        {"type": "sentry-issue", "content": "issue text"},
        {
            "type": "terminal",
            "content": "$ ls",
            "metadata": {"terminalId": "t1", "terminalName": "build"}
        },
        {"type": "note", "noteId": "note-1"},
        {"type": "note", "metadata": {"noteId": "note-2"}},
    ]);
    let out =
        super::build_stdin_context_from_context_references(Some(&refs)).expect("non-empty context");
    let parts: Vec<&str> = out.split("\n\n").collect();
    assert_eq!(parts.len(), 11);
    assert_eq!(parts[0], "Selected text:\nhello");
    assert_eq!(parts[1], "Task:\ndo the thing");
    assert_eq!(parts[2], "Code:\nfn foo() {}");
    assert_eq!(parts[3], "File src/a.rs:\npub fn a() {}");
    assert_eq!(parts[4], "File: src/only-path.rs");
    assert_eq!(parts[5], "Linear Issue:\nXYZ-1 title");
    assert_eq!(parts[6], "GitHub Issue:\n#42 title");
    assert_eq!(parts[7], "Sentry Issue:\nissue text");
    assert_eq!(parts[8], "Terminal \"build\" (terminal_id: t1):\n$ ls");
    assert_eq!(parts[9], "Note: note-1");
    assert_eq!(parts[10], "Note: note-2");
}

/// Empty / absent inputs collapse to `None` so the prompt is left unchanged.
#[test]
fn build_stdin_context_from_context_references_empty_is_none() {
    assert!(super::build_stdin_context_from_context_references(None).is_none());
    assert!(super::build_stdin_context_from_context_references(Some(&json!([]))).is_none());
    // Only-unsupported entries also collapse to None.
    assert!(
        super::build_stdin_context_from_context_references(Some(&json!([
            {"type": "note"}, {"type": "file"}
        ])))
        .is_none()
    );
}

/// End-to-end prompt shape: when `stdin_context` is absent but
/// `context_references` yield content, `build_turn_prompt` prepends a
/// `Context:` block synthesised by the builder; an explicit
/// `stdin_context` still wins over the fallback.
#[tokio::test]
async fn build_turn_prompt_uses_context_references_when_stdin_context_is_absent() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-ctx"), AgentId::from("a-ctx"));
    seed_agent(&mgr, &ws, &id).await;

    // Synthesised path.
    let options = super::TurnOptions {
        context_references: Some(json!([
            {"type": "selection", "content": "selected"},
            {"type": "file", "path": "a.rs", "content": "pub fn a() {}"},
        ])),
        ..super::TurnOptions::default()
    };
    let prompt = mgr.build_turn_prompt(&id, &ws, "do it", &options).await;
    let text = serde_json::to_value(&prompt).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        text.starts_with(
            "Context:\nSelected text:\nselected\n\nFile a.rs:\npub fn a() {}\n\n---\n\n"
        ),
        "unexpected prompt: {text:?}"
    );
    assert!(text.ends_with("do it"));

    // Explicit stdin_context wins over the synthesised fallback.
    let options = super::TurnOptions {
        stdin_context: Some("explicit".to_string()),
        context_references: Some(json!([
            {"type": "selection", "content": "ignored"}
        ])),
        ..super::TurnOptions::default()
    };
    let prompt = mgr.build_turn_prompt(&id, &ws, "do it", &options).await;
    let text = serde_json::to_value(&prompt).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(text.starts_with("Context:\nexplicit\n\n---\n\n"));
    assert!(!text.contains("ignored"));
}

/// `noteIds` (PROTOCOL §5.5): the resolver loads workspace-asset
/// images referenced by each note's markdown content, appends them as
/// ACP `image` content blocks, and adds a system text notice so the
/// agent knows the images are inlined (parity with the FE extraction
/// in `agent-backend-handler.service.ts`).
#[tokio::test]
async fn build_turn_prompt_resolves_note_ids_to_image_blocks() {
    use base64::Engine as _;
    use intent_core::{ContentType, Note, NoteId, NoteMetadata, NoteVisibility};

    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let assets_dir = crate::tests::test_tempdir("intentd-note-img-");
    let services = Services::new(store.clone())
        .with_event_bus(bus.clone())
        .with_assets_root(assets_dir.path().to_path_buf());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let mgr = AgentManager::new(services, sink, 8);

    let ws = WorkspaceId::from("ws-note-img");
    let id = AgentId::from("a-note-img");
    seed_agent(&mgr, &ws, &id).await;

    // Write an on-disk asset the note will reference.
    let asset_id = "asset-abc.png";
    let asset_bytes: &[u8] = b"pretend-png";
    let ws_dir = assets_dir.path().join(&ws.0);
    std::fs::create_dir_all(&ws_dir).expect("asset dir");
    std::fs::write(ws_dir.join(asset_id), asset_bytes).expect("write asset");

    // Persist a note whose markdown references the asset URL.
    let note_id = NoteId::new();
    let ts = now_iso();
    let note = Note {
        id: note_id.clone(),
        workspace_id: ws.clone(),
        title: "Spec".to_string(),
        content: format!(
            "# Screenshot\n\n![shot](workspace-asset://{ws}/{asset})\n",
            ws = ws.0,
            asset = asset_id,
        ),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata::default(),
        created_at: ts.clone(),
        rev: 0,
        updated_at: ts,
    };
    store.insert_note(&note).await.expect("insert note");

    let options = super::TurnOptions {
        note_ids: Some(json!([note_id.to_string()])),
        ..super::TurnOptions::default()
    };
    let prompt = mgr.build_turn_prompt(&id, &ws, "look", &options).await;
    let wire = serde_json::to_value(&prompt).unwrap();
    let arr = wire.as_array().unwrap();
    // Expect: original text prompt, image block, system notice.
    assert_eq!(arr.len(), 3, "text + image + notice");
    assert_eq!(arr[0]["type"], json!("text"));
    assert!(arr[0]["text"].as_str().unwrap().contains("look"));
    assert_eq!(arr[1]["type"], json!("image"));
    let expected_b64 = base64::engine::general_purpose::STANDARD.encode(asset_bytes);
    assert_eq!(arr[1]["data"], json!(expected_b64));
    assert_eq!(arr[1]["mimeType"], json!("image/png"));
    assert_eq!(arr[2]["type"], json!("text"));
    assert!(arr[2]["text"].as_str().unwrap().contains("1 image(s)"));

    // A cross-workspace URL is silently skipped (no image, no notice).
    let stray_id = NoteId::new();
    let stray = Note {
        id: stray_id.clone(),
        content: format!("![x](workspace-asset://other-ws/{asset_id})\n"),
        ..note.clone()
    };
    let mut stray = stray;
    stray.id = stray_id.clone();
    store.insert_note(&stray).await.expect("insert stray");
    let options = super::TurnOptions {
        note_ids: Some(json!([stray_id.to_string()])),
        ..super::TurnOptions::default()
    };
    let prompt = mgr.build_turn_prompt(&id, &ws, "look", &options).await;
    let arr_json = serde_json::to_value(&prompt).unwrap();
    let arr = arr_json.as_array().unwrap();
    assert_eq!(arr.len(), 1, "only text; stray URL is skipped");
}

/// A10 — daemon-side merge of user MCP servers into the agent spawn config.
/// Directly exercises [`AgentManager::merge_user_mcp_servers`] against an
/// [`InMemorySecretStore`] and a fresh store so the tests stay hermetic (no
/// keychain / real bridge involved).
mod merge_user_mcp_servers_tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use intent_acp::{NormalizedMcpServer, NormalizedMcpServers};
    use serde_json::json;

    use super::TempDb;
    use crate::agent_manager::AgentManager;
    use crate::agent_manager::BusEventSink;
    use crate::events::EventBus;
    use crate::settings::{InMemorySecretStore, SecretStore};
    use crate::Services;
    use intent_acp::EventSink;
    use intent_store::Store;

    async fn manager_with_secrets() -> (
        TempDb,
        AgentManager,
        Arc<InMemorySecretStore>,
        tempfile::TempDir,
    ) {
        let tmp = super::TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let bus = EventBus::new(store.clone());
        let secrets = Arc::new(InMemorySecretStore::default());
        let config_dir = tempfile::tempdir().expect("temp config dir");
        let registry = Arc::new(
            crate::SettingsRegistry::load(config_dir.path().join("config.toml"))
                .expect("load registry"),
        );
        let services = Services::new(store)
            .with_event_bus(bus.clone())
            .with_secret_store(secrets.clone() as Arc<dyn SecretStore>)
            .with_settings_registry(registry);
        let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
        (
            tmp,
            AgentManager::new(services, sink, 8),
            secrets,
            config_dir,
        )
    }

    fn write_servers(secrets: &InMemorySecretStore, servers: &serde_json::Value) {
        secrets
            .store("mcp.servers", &serde_json::to_string(&servers).unwrap())
            .expect("write mcp.servers");
    }

    #[tokio::test]
    async fn skips_when_enable_user_servers_disabled() {
        let (_tmp, mgr, secrets, _cfg) = manager_with_secrets().await;
        write_servers(
            &secrets,
            &json!({ "srv-1": { "id": "srv-1", "name": "u", "transport": "stdio",
                                 "command": "node", "enabled": true } }),
        );
        mgr.services
            .settings_registry()
            .unwrap()
            .apply(&[("mcp.enableUserServers".to_string(), json!(false))])
            .unwrap();
        let mut out = NormalizedMcpServers::new();
        mgr.merge_user_mcp_servers(&mut out).await.unwrap();
        assert!(out.is_empty(), "gate off → nothing merged: {out:?}");
    }

    #[tokio::test]
    async fn merges_enabled_stdio_server_by_name() {
        let (_tmp, mgr, secrets, _cfg) = manager_with_secrets().await;
        write_servers(
            &secrets,
            &json!({
                "srv-1": {
                    "id": "srv-1", "name": "my-tool", "transport": "stdio",
                    "command": "node", "args": ["srv.js"], "enabled": true,
                    "env": { "A": "1" }
                }
            }),
        );
        let mut out = NormalizedMcpServers::new();
        mgr.merge_user_mcp_servers(&mut out).await.unwrap();
        let entry = out.get("my-tool").expect("keyed by name, not id");
        match entry {
            NormalizedMcpServer::Stdio { command, args, env } => {
                assert_eq!(command, "node");
                assert_eq!(args, &vec!["srv.js".to_string()]);
                assert_eq!(env.get("A").map(String::as_str), Some("1"));
            }
            other => panic!("expected stdio, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn skips_disabled_and_globally_disabled_servers() {
        let (_tmp, mgr, secrets, _cfg) = manager_with_secrets().await;
        write_servers(
            &secrets,
            &json!({
                "srv-off": { "id": "srv-off", "name": "off", "transport": "stdio",
                              "command": "node", "enabled": false },
                "srv-glo": { "id": "srv-glo", "name": "glo", "transport": "stdio",
                              "command": "node", "enabled": true },
                "srv-on":  { "id": "srv-on",  "name": "on",  "transport": "stdio",
                              "command": "node", "enabled": true }
            }),
        );
        mgr.services
            .settings_registry()
            .unwrap()
            .apply(&[("mcp.disabledServers".to_string(), json!(["srv-glo"]))])
            .unwrap();
        let mut out = NormalizedMcpServers::new();
        mgr.merge_user_mcp_servers(&mut out).await.unwrap();
        let names: HashSet<_> = out.keys().cloned().collect();
        assert!(names.contains("on"), "enabled+not-disabled kept: {names:?}");
        assert!(!names.contains("off"), "enabled=false dropped");
        assert!(!names.contains("glo"), "globally-disabled dropped");
    }

    #[tokio::test]
    async fn injects_oauth_authorization_header_for_http() {
        let (_tmp, mgr, secrets, _cfg) = manager_with_secrets().await;
        write_servers(
            &secrets,
            &json!({
                "srv-remote": {
                    "id": "srv-remote", "name": "remote", "transport": "http",
                    "url": "https://example.test/mcp", "enabled": true
                }
            }),
        );
        mgr.services
            .store
            .set_mcp_oauth_token(
                "srv-remote",
                &serde_json::to_string(
                    &json!({ "access_token": "tok-xyz", "token_type": "bearer" }),
                )
                .unwrap(),
                "2026-07-05T00:00:00Z",
            )
            .await
            .unwrap();
        let mut out = NormalizedMcpServers::new();
        mgr.merge_user_mcp_servers(&mut out).await.unwrap();
        match out.get("remote").expect("http server merged") {
            NormalizedMcpServer::Http { url, headers } => {
                assert_eq!(url, "https://example.test/mcp");
                let headers = headers.as_ref().expect("auth header written");
                assert_eq!(
                    headers.get("Authorization").map(String::as_str),
                    Some("Bearer tok-xyz"),
                    "token_type title-cased, access_token appended",
                );
            }
            other => panic!("expected http, got {other:?}"),
        }
    }

    /// The reshape path builds its header via the shared
    /// `McpOauthService::authorization_header`, so an expired bag carrying
    /// refresh metadata is refreshed before the header is injected.
    #[tokio::test]
    async fn reshape_refreshes_expired_oauth_bag_via_shared_service() {
        use std::time::{SystemTime, UNIX_EPOCH};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoint = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf).await;
            let body = r#"{"access_token":"refreshed-tok","expires_in":3600}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len(),
            );
            let _ = sock.write_all(resp.as_bytes()).await;
        });

        let (_tmp, mgr, secrets, _cfg) = manager_with_secrets().await;
        // Server id unique to this test: refresh single-flight/cooldown state
        // lives in process-wide statics keyed by server id.
        let server_id = "srv-reshape-refresh";
        write_servers(
            &secrets,
            &json!({
                "srv-reshape-refresh": {
                    "id": server_id, "name": "reshape-refresh", "transport": "http",
                    "url": "https://example.test/mcp", "enabled": true
                }
            }),
        );
        let expired = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 100;
        mgr.services
            .store
            .set_mcp_oauth_token(
                server_id,
                &serde_json::to_string(&json!({
                    "access_token": "stale-tok",
                    "token_type": "bearer",
                    "expires_at": expired,
                    "refresh_token": "rt-1",
                    "token_endpoint": format!("http://{addr}/token"),
                    "client_id": "cid-1",
                }))
                .unwrap(),
                "2026-07-05T00:00:00Z",
            )
            .await
            .unwrap();
        let mut out = NormalizedMcpServers::new();
        mgr.merge_user_mcp_servers(&mut out).await.unwrap();
        match out.get("reshape-refresh").expect("http server merged") {
            NormalizedMcpServer::Http { headers, .. } => {
                let headers = headers.as_ref().expect("auth header written");
                assert_eq!(
                    headers.get("Authorization").map(String::as_str),
                    Some("Bearer refreshed-tok"),
                    "expired bag refreshed through the shared service",
                );
            }
            other => panic!("expected http, got {other:?}"),
        }
        endpoint.await.unwrap();
    }

    #[tokio::test]
    async fn preserves_existing_authorization_header() {
        let (_tmp, mgr, secrets, _cfg) = manager_with_secrets().await;
        write_servers(
            &secrets,
            &json!({
                "srv-remote": {
                    "id": "srv-remote", "name": "remote", "transport": "sse",
                    "url": "https://example.test/sse", "enabled": true,
                    "headers": { "Authorization": "Basic user:pass" }
                }
            }),
        );
        mgr.services
            .store
            .set_mcp_oauth_token(
                "srv-remote",
                &serde_json::to_string(&json!({ "access_token": "tok-xyz" })).unwrap(),
                "2026-07-05T00:00:00Z",
            )
            .await
            .unwrap();
        let mut out = NormalizedMcpServers::new();
        mgr.merge_user_mcp_servers(&mut out).await.unwrap();
        match out.get("remote").unwrap() {
            NormalizedMcpServer::Sse { headers, .. } => {
                let headers = headers.as_ref().unwrap();
                assert_eq!(
                    headers.get("Authorization").map(String::as_str),
                    Some("Basic user:pass"),
                );
            }
            other => panic!("expected sse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn does_not_overwrite_reserved_workspace_mcp() {
        let (_tmp, mgr, secrets, _cfg) = manager_with_secrets().await;
        write_servers(
            &secrets,
            &json!({
                "srv-x": { "id": "srv-x", "name": "workspace-mcp", "transport": "stdio",
                             "command": "evil", "enabled": true }
            }),
        );
        let mut out = NormalizedMcpServers::new();
        out.insert(
            "workspace-mcp".to_string(),
            NormalizedMcpServer::Stdio {
                command: "bridge".into(),
                args: vec![],
                env: std::collections::BTreeMap::default(),
            },
        );
        mgr.merge_user_mcp_servers(&mut out).await.unwrap();
        match out.get("workspace-mcp").unwrap() {
            NormalizedMcpServer::Stdio { command, .. } => {
                assert_eq!(command, "bridge", "reserved entry left intact");
            }
            other => panic!("expected stdio, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn empty_secret_is_a_noop() {
        let (_tmp, mgr, _secrets, _cfg) = manager_with_secrets().await;
        let mut out = NormalizedMcpServers::new();
        mgr.merge_user_mcp_servers(&mut out).await.unwrap();
        assert!(out.is_empty());
    }

    /// The opencode env config carries the same `workspace-mcp` bridge entry
    /// (in `OpenCode` `mcp` block shape) that the auggie `--mcp-config` path
    /// generates, pointing at the same bridge endpoint. The bridge exe is
    /// pinned to a space-free absolute path so the expectation does not
    /// depend on whether the host checkout path contains whitespace
    /// (monorepo#1367).
    #[tokio::test]
    async fn opencode_env_mcp_config_includes_bridge_server() {
        let (_tmp, mgr, _secrets, _cfg) = manager_with_secrets().await;
        let mgr = mgr.with_mcp_bridge_exe("/usr/local/bin/intentd");
        let json = mgr
            .opencode_env_mcp_config("127.0.0.1:9999".to_string())
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let bridge = &parsed["workspace-mcp"];
        assert_eq!(bridge["type"], "local");
        assert_eq!(bridge["enabled"], true);
        let command: Vec<String> =
            serde_json::from_value(bridge["command"].clone()).expect("command array");
        assert_eq!(
            &command[1..],
            ["mcp-bridge", "--connect", "127.0.0.1:9999"],
            "bridge args must match the auggie --mcp-config path"
        );
        assert_eq!(
            command[0], "/usr/local/bin/intentd",
            "space-free bridge exe stays absolute in the opencode command"
        );
    }

    /// monorepo#1367 — the opencode env config applies the same monorepo#1049
    /// normalization as `normalized_mcp_servers`: a whitespace-containing
    /// bridge path collapses to the executable basename and the entry's
    /// environment carries a PATH that prepends the parent dir to the
    /// inherited PATH.
    #[tokio::test]
    async fn opencode_env_mcp_config_normalizes_spaced_bridge_path() {
        let (_tmp, mgr, _secrets, _cfg) = manager_with_secrets().await;
        let mgr = mgr.with_mcp_bridge_exe("/opt/App Support/bin/intentd");
        let json = mgr
            .opencode_env_mcp_config("127.0.0.1:9999".to_string())
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let bridge = &parsed["workspace-mcp"];
        let command: Vec<String> =
            serde_json::from_value(bridge["command"].clone()).expect("command array");
        assert_eq!(
            command[0], "intentd",
            "spaced path collapses to the basename"
        );
        assert_eq!(&command[1..], ["mcp-bridge", "--connect", "127.0.0.1:9999"]);
        let inherited = std::env::var("PATH").expect("test process has PATH");
        let expected = std::env::join_paths(
            std::iter::once(std::path::PathBuf::from("/opt/App Support/bin"))
                .chain(std::env::split_paths(&inherited)),
        )
        .unwrap();
        assert_eq!(
            bridge["environment"]["PATH"],
            serde_json::json!(expected.to_string_lossy()),
            "entry PATH prepends the parent dir to the inherited PATH"
        );
    }

    /// monorepo#1049 — a whitespace-containing bridge path collapses to the
    /// executable basename, and the entry's PATH prepends the parent dir to
    /// the inherited PATH. Asserted on the final (post-baseline-merge) env:
    /// the server env wins that merge, so the override itself must carry the
    /// full inherited PATH rather than relying on the baseline's PATH.
    #[tokio::test]
    async fn spaced_bridge_path_normalizes_to_basename_with_path_prepend() {
        let (_tmp, mgr, _secrets, _cfg) = manager_with_secrets().await;
        let mgr = mgr.with_mcp_bridge_exe("/opt/App Support/bin/intentd");
        let servers = mgr
            .normalized_mcp_servers("127.0.0.1:9999".to_string())
            .await
            .unwrap();
        let NormalizedMcpServer::Stdio { command, args, env } =
            servers.get("workspace-mcp").unwrap()
        else {
            panic!("workspace-mcp is stdio");
        };
        assert_eq!(command, "intentd", "spaced path collapses to the basename");
        assert_eq!(args[..], ["mcp-bridge", "--connect", "127.0.0.1:9999"]);
        let inherited = std::env::var("PATH").expect("test process has PATH");
        let expected = std::env::join_paths(
            std::iter::once(std::path::PathBuf::from("/opt/App Support/bin"))
                .chain(std::env::split_paths(&inherited)),
        )
        .unwrap();
        assert_eq!(
            env.get("PATH").map(String::as_str),
            Some(&*expected.to_string_lossy()),
            "PATH starts with the parent dir and keeps the full inherited PATH"
        );
    }

    /// Whitespace-free bridge path is untouched: absolute command, and the
    /// entry's PATH is just the baseline (inherited) PATH — no injection.
    #[tokio::test]
    async fn unspaced_bridge_path_stays_absolute_without_path_override() {
        let (_tmp, mgr, _secrets, _cfg) = manager_with_secrets().await;
        let mgr = mgr.with_mcp_bridge_exe("/usr/local/bin/intentd");
        let servers = mgr
            .normalized_mcp_servers("127.0.0.1:9999".to_string())
            .await
            .unwrap();
        let NormalizedMcpServer::Stdio { command, env, .. } = servers.get("workspace-mcp").unwrap()
        else {
            panic!("workspace-mcp is stdio");
        };
        assert_eq!(command, "/usr/local/bin/intentd", "absolute path verbatim");
        let inherited = std::env::var("PATH").expect("test process has PATH");
        assert_eq!(
            env.get("PATH").map(String::as_str),
            Some(inherited.as_str()),
            "baseline PATH only — no injected override"
        );
    }
}

/// Classifier for the `session/cancel` log level in [`AgentManager::interrupt`]:
/// transport-closed errors (child already dead) are demoted to DEBUG; every
/// other `AcpError` keeps the WARN.
mod session_cancel_log_classifier_tests {
    use super::*;

    #[test]
    fn transport_closed_is_demoted_to_debug() {
        assert!(is_cancel_transport_closed(&AcpError::Transport(
            "writer task closed".to_string()
        )));
        assert!(is_cancel_transport_closed(&AcpError::Transport(
            "response channel dropped".to_string()
        )));
    }

    #[test]
    fn other_acp_errors_stay_at_warn() {
        assert!(!is_cancel_transport_closed(&AcpError::Timeout(
            "session/cancel".to_string()
        )));
        assert!(!is_cancel_transport_closed(&AcpError::Rpc(JsonRpcError {
            code: -32600,
            message: "invalid request".to_string(),
            data: None,
        })));
        assert!(!is_cancel_transport_closed(&AcpError::Protocol(
            "malformed response".to_string()
        )));
        assert!(!is_cancel_transport_closed(&AcpError::Serde(
            "bad payload".to_string()
        )));
    }
}

/// Stale queued-message redrive detection (#576):
/// [`AgentManager::annotate_stale_redrive`] must flag (and annotate) a
/// delegated agent's queued message whose `queued_at` predates the session's
/// `completion_report_timestamp`, and must treat everything else as fresh.
mod stale_redrive_tests {
    use super::*;
    use crate::agent_ops::QueuedMessage;

    fn queued_msg(content: &str, queued_at: &str, persisted: bool) -> QueuedMessage {
        QueuedMessage {
            id: "qm-stale-test".to_string(),
            turn_id: "qm-stale-test".to_string(),
            content: content.to_string(),
            image_blocks: None,
            file_blocks: None,
            queued_at: queued_at.to_string(),
            editing: false,
            persisted,
            requeued_after_failure: false,
            message_metadata: None,
            prepend_content: None,
            prepend_image_blocks: None,
            prepend_file_blocks: None,
            interrupt_priority: false,
            user_origin: false,
        }
    }

    /// Mark the seeded session as a delegated child that already delivered a
    /// completion report at `report_ts`.
    async fn set_delegated_report(
        mgr: &AgentManager,
        ws: &WorkspaceId,
        id: &AgentId,
        report_ts: &str,
    ) {
        let mut session = mgr.services.store.get_agent_session(id).await.unwrap();
        session.parent_agent_id = Some(AgentId::from("agent-parent"));
        session.completion_report = Some("work done".to_string());
        session.completion_report_timestamp = Some(report_ts.to_string());
        mgr.services
            .store
            .update_agent_session(ws, &session)
            .await
            .expect("set delegated report");
    }

    #[tokio::test]
    async fn stale_message_is_annotated_and_flagged() {
        let (_tmp, mgr) = manager().await;
        let (ws, id) = (WorkspaceId::from("ws-576-a"), AgentId::from("a-576-a"));
        seed_agent(&mgr, &ws, &id).await;

        let queued_at = now_iso();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let report_ts = now_iso();
        set_delegated_report(&mgr, &ws, &id, &report_ts).await;

        let mut msg = queued_msg("please continue", &queued_at, false);
        let stale = mgr.annotate_stale_redrive(&id, &mut msg).await;
        assert!(stale, "queued_at < completion_report_timestamp is stale");
        assert!(
            msg.content.starts_with("please continue"),
            "original content is preserved: {}",
            msg.content
        );
        assert!(
            msg.content
                .contains(super::super::STALE_REDRIVE_NOTE_PREFIX),
            "stale content carries the system note: {}",
            msg.content
        );
        assert!(
            msg.content.contains(&report_ts),
            "the note names the delivered report timestamp: {}",
            msg.content
        );
    }

    #[tokio::test]
    async fn fresh_message_after_report_is_not_stale() {
        let (_tmp, mgr) = manager().await;
        let (ws, id) = (WorkspaceId::from("ws-576-b"), AgentId::from("a-576-b"));
        seed_agent(&mgr, &ws, &id).await;

        let report_ts = now_iso();
        set_delegated_report(&mgr, &ws, &id, &report_ts).await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        let mut msg = queued_msg("new work", &now_iso(), false);
        let stale = mgr.annotate_stale_redrive(&id, &mut msg).await;
        assert!(!stale, "queued_at >= completion_report_timestamp is fresh");
        assert_eq!(msg.content, "new work", "fresh content is untouched");
    }

    #[tokio::test]
    async fn non_delegated_agent_is_never_stale() {
        let (_tmp, mgr) = manager().await;
        let (ws, id) = (WorkspaceId::from("ws-576-c"), AgentId::from("a-576-c"));
        seed_agent(&mgr, &ws, &id).await;

        // Report set but NO parent_agent_id: user-created agents keep today's
        // behavior even with a (vestigial) report on the session.
        let queued_at = now_iso();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
        session.completion_report = Some("work done".to_string());
        session.completion_report_timestamp = Some(now_iso());
        mgr.services
            .store
            .update_agent_session(&ws, &session)
            .await
            .expect("set report without parent");

        let mut msg = queued_msg("hello", &queued_at, false);
        let stale = mgr.annotate_stale_redrive(&id, &mut msg).await;
        assert!(!stale, "non-delegated agents are never stale");
        assert_eq!(msg.content, "hello");
    }

    #[tokio::test]
    async fn no_report_means_fresh() {
        let (_tmp, mgr) = manager().await;
        let (ws, id) = (WorkspaceId::from("ws-576-d"), AgentId::from("a-576-d"));
        seed_agent(&mgr, &ws, &id).await;
        let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
        session.parent_agent_id = Some(AgentId::from("agent-parent"));
        mgr.services
            .store
            .update_agent_session(&ws, &session)
            .await
            .expect("set parent without report");

        let mut msg = queued_msg("hi", &now_iso(), false);
        assert!(!mgr.annotate_stale_redrive(&id, &mut msg).await);
        assert_eq!(msg.content, "hi");
    }

    #[tokio::test]
    async fn persisted_requeue_is_flagged_but_not_annotated() {
        let (_tmp, mgr) = manager().await;
        let (ws, id) = (WorkspaceId::from("ws-576-e"), AgentId::from("a-576-e"));
        seed_agent(&mgr, &ws, &id).await;

        let queued_at = now_iso();
        tokio::time::sleep(Duration::from_millis(20)).await;
        set_delegated_report(&mgr, &ws, &id, &now_iso()).await;

        // A terminal-failure requeue whose transcript row is already durable:
        // the content must stay byte-identical to the persisted row, but the
        // clear-suppression flag still applies.
        let mut msg = queued_msg("already persisted", &queued_at, true);
        let stale = mgr.annotate_stale_redrive(&id, &mut msg).await;
        assert!(stale, "persisted stale requeue still suppresses the clear");
        assert_eq!(
            msg.content, "already persisted",
            "persisted rows are never rewritten"
        );
    }

    #[tokio::test]
    async fn stale_annotation_is_idempotent_across_requeues() {
        let (_tmp, mgr) = manager().await;
        let (ws, id) = (WorkspaceId::from("ws-576-f"), AgentId::from("a-576-f"));
        seed_agent(&mgr, &ws, &id).await;

        let queued_at = now_iso();
        tokio::time::sleep(Duration::from_millis(20)).await;
        set_delegated_report(&mgr, &ws, &id, &now_iso()).await;

        let mut msg = queued_msg("loop", &queued_at, false);
        assert!(mgr.annotate_stale_redrive(&id, &mut msg).await);
        let once = msg.content.clone();
        assert!(mgr.annotate_stale_redrive(&id, &mut msg).await);
        assert_eq!(
            msg.content, once,
            "a requeued stale entry is not double-annotated"
        );
    }

    /// Fail open: an unparseable `queued_at` (or report timestamp) disables
    /// the staleness verdict for that message — treated as fresh, content
    /// untouched (the `else` arm logs a warn for diagnosability).
    #[tokio::test]
    async fn unparseable_timestamps_fail_open_as_fresh() {
        let (_tmp, mgr) = manager().await;
        let (ws, id) = (WorkspaceId::from("ws-576-g"), AgentId::from("a-576-g"));
        seed_agent(&mgr, &ws, &id).await;
        set_delegated_report(&mgr, &ws, &id, &now_iso()).await;

        // Garbage queued_at.
        let mut msg = queued_msg("hello", "not-a-timestamp", false);
        assert!(!mgr.annotate_stale_redrive(&id, &mut msg).await);
        assert_eq!(msg.content, "hello");

        // Garbage report timestamp.
        set_delegated_report(&mgr, &ws, &id, "garbage-ts").await;
        let mut msg = queued_msg("hello", &now_iso(), false);
        assert!(!mgr.annotate_stale_redrive(&id, &mut msg).await);
        assert_eq!(msg.content, "hello");
    }

    /// STAB-112 requeue keeps staleness sticky (#576): a drained stale entry
    /// whose turn fails terminally is requeued with its ORIGINAL `queued_at`
    /// (threaded via [`TurnOptions::queued_at`]), so the retry drain still
    /// classifies it stale and keeps suppressing the report clear. Direct
    /// sends (`queued_at: None`) stamp a fresh timestamp as before.
    #[tokio::test]
    async fn terminal_failure_requeue_preserves_staleness() {
        let (_tmp, mgr) = manager().await;
        let (ws, id) = (WorkspaceId::from("ws-576-h"), AgentId::from("a-576-h"));
        seed_agent(&mgr, &ws, &id).await;

        let queued_at = now_iso();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let report_ts = now_iso();
        set_delegated_report(&mgr, &ws, &id, &report_ts).await;

        // Simulate the terminal-failure requeue of a drained stale entry.
        let options = super::super::TurnOptions {
            queued_at: Some(queued_at.clone()),
            ..super::super::TurnOptions::default()
        };
        super::super::persist_error_and_requeue(
            &mgr,
            &id,
            &ws,
            "stale work",
            &options,
            true,
            "boom",
        )
        .await;

        let mut requeued = mgr
            .services
            .dequeue_message(&id)
            .expect("requeued entry present");
        assert_eq!(
            requeued.queued_at, queued_at,
            "requeue carries the original queued_at, not now_iso()"
        );
        assert!(requeued.requeued_after_failure);
        assert!(
            mgr.annotate_stale_redrive(&id, &mut requeued).await,
            "the retry drain still classifies the requeued entry as stale"
        );
        assert_eq!(
            requeued.content, "stale work",
            "persisted requeues are never rewritten"
        );

        // Direct sends have no drained timestamp: the requeue stamps fresh.
        let options = super::super::TurnOptions::default();
        super::super::persist_error_and_requeue(
            &mgr,
            &id,
            &ws,
            "direct send",
            &options,
            true,
            "boom",
        )
        .await;
        let mut requeued = mgr
            .services
            .dequeue_message(&id)
            .expect("requeued entry present");
        assert!(
            !mgr.annotate_stale_redrive(&id, &mut requeued).await,
            "a direct-send requeue is stamped fresh (queued_at >= report_ts)"
        );
    }

    /// Full drain-path flow (#576): a delegated agent with a delivered
    /// completion report drains a STALE queued message through a real mock-ACP
    /// turn — the persisted user row carries the annotation and the report
    /// survives the turn (the start-of-turn clear is suppressed).
    #[tokio::test]
    async fn stale_drain_keeps_report_and_annotates_transcript() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let (_tmp, mgr) = manager().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (WorkspaceId::from("ws-576-g"), AgentId::from("a-576-g"));
        seed_agent(&mgr, &ws, &id).await;
        set_session_provider(&mgr, &ws, &id, "mock").await;

        // Enqueue FIRST, then persist the report: queued_at < report_ts, the
        // exact incident ordering (message queued while the reporting turn
        // was still in flight).
        mgr.services
            .enqueue_message(&id, "stale wake".to_string(), None, None, None, None, false);
        tokio::time::sleep(Duration::from_millis(20)).await;
        set_delegated_report(&mgr, &ws, &id, &now_iso()).await;

        mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;
        timeout(Duration::from_secs(10), async {
            loop {
                let session = mgr.services.store.get_agent_session(&id).await.unwrap();
                if session.status == AgentStatus::RuntimeIdle
                    && !mgr.is_busy(&id)
                    && mgr.workers.lock().unwrap().is_empty()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("stale drained turn completes");

        let session = mgr.services.store.get_agent_session(&id).await.unwrap();
        assert_eq!(
            session.completion_report.as_deref(),
            Some("work done"),
            "the delivered report survives a stale redrive"
        );
        assert!(
            session.completion_report_timestamp.is_some(),
            "report timestamp survives too"
        );

        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .expect("messages");
        let user_row = messages
            .iter()
            .find(|m| m.role == "user")
            .expect("drained user row persisted");
        let text = user_row.content[0]["text"].as_str().unwrap();
        assert!(
            text.starts_with("stale wake"),
            "original content first: {text}"
        );
        assert!(
            text.contains(super::super::STALE_REDRIVE_NOTE_PREFIX),
            "persisted row carries the annotation: {text}"
        );
    }

    /// Full drain-path flow (#576) counterpart: a FRESH queued message
    /// (enqueued after the report was delivered) keeps today's behavior — no
    /// annotation and the start-of-turn clear wipes the stale report.
    #[tokio::test]
    async fn fresh_drain_clears_report_without_annotation() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let (_tmp, mgr) = manager().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (WorkspaceId::from("ws-576-h"), AgentId::from("a-576-h"));
        seed_agent(&mgr, &ws, &id).await;
        set_session_provider(&mgr, &ws, &id, "mock").await;

        // Report lands 60s ago; the queued entry is backdated to 10s ago —
        // still AFTER the report (fresh, not a stale redrive) but past the
        // dequeue-wait annotation threshold (monorepo#2353) so the drained
        // row still carries the wait note.
        set_delegated_report(&mgr, &ws, &id, &super::dequeue_wait_tests::iso_secs_ago(60)).await;
        let (enqueued, _) = mgr.services.enqueue_message(
            &id,
            "fresh work".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        let mut entry = mgr
            .services
            .take_queued_message(&id, &enqueued.id)
            .expect("entry queued");
        entry.queued_at = super::dequeue_wait_tests::iso_secs_ago(10);
        mgr.services.requeue_front(&id, entry);

        mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;
        timeout(Duration::from_secs(10), async {
            loop {
                let session = mgr.services.store.get_agent_session(&id).await.unwrap();
                if session.status == AgentStatus::RuntimeIdle
                    && !mgr.is_busy(&id)
                    && mgr.workers.lock().unwrap().is_empty()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("fresh drained turn completes");

        let session = mgr.services.store.get_agent_session(&id).await.unwrap();
        assert_eq!(
            session.completion_report, None,
            "a fresh turn clears the prior report as today"
        );
        assert_eq!(session.completion_report_timestamp, None);

        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .expect("messages");
        let user_row = messages
            .iter()
            .find(|m| m.role == "user")
            .expect("drained user row persisted");
        let text = user_row.content[0]["text"].as_str().unwrap();
        assert!(
            text.starts_with("fresh work"),
            "fresh content is preserved: {text}"
        );
        assert!(
            !text.contains(super::super::STALE_REDRIVE_NOTE_PREFIX),
            "fresh content carries no stale-redrive note: {text}"
        );
        assert!(
            text.contains(super::super::DEQUEUE_WAIT_NOTE_PREFIX),
            "an above-threshold drained row carries the dequeue-wait note: {text}"
        );
    }
}

/// Dequeue-wait annotation: a drained queue entry tells the target when it
/// entered the queue and how long it waited before delivery —
/// [`super::annotate_dequeue_wait`] must annotate exactly once, never rewrite
/// a persisted requeue, never touch messages that were delivered immediately
/// (the direct-send path never constructs a queue entry), and skip waits
/// below [`super::DEQUEUE_WAIT_ANNOTATION_MIN_MS`] entirely (monorepo#2353).
mod dequeue_wait_tests {
    use super::*;
    use crate::agent_ops::QueuedMessage;

    /// RFC-3339 UTC timestamp `secs` seconds in the past — backdates
    /// `queued_at` so a test drain lands above the annotation threshold.
    pub(super) fn iso_secs_ago(secs: i64) -> String {
        (time::OffsetDateTime::now_utc() - time::Duration::seconds(secs))
            .format(&time::format_description::well_known::Rfc3339)
            .expect("format backdated timestamp")
    }

    pub(super) fn queued_msg(content: &str, queued_at: &str, persisted: bool) -> QueuedMessage {
        QueuedMessage {
            id: "qm-wait-test".to_string(),
            turn_id: "qm-wait-test".to_string(),
            content: content.to_string(),
            image_blocks: None,
            file_blocks: None,
            queued_at: queued_at.to_string(),
            editing: false,
            persisted,
            requeued_after_failure: false,
            message_metadata: None,
            prepend_content: None,
            prepend_image_blocks: None,
            prepend_file_blocks: None,
            interrupt_priority: false,
            user_origin: false,
        }
    }

    #[test]
    fn fresh_entry_is_annotated() {
        let queued_at = "2026-01-01T00:00:00Z";
        let mut msg = queued_msg("please continue", queued_at, false);
        super::super::annotate_dequeue_wait(&mut msg);
        assert!(
            msg.content.starts_with("please continue"),
            "original content is preserved: {}",
            msg.content
        );
        assert!(
            msg.content.contains(super::super::DEQUEUE_WAIT_NOTE_PREFIX),
            "drained content carries the dequeue-wait note: {}",
            msg.content
        );
        assert!(
            msg.content.contains(queued_at),
            "the note names the enqueue timestamp: {}",
            msg.content
        );
        assert!(
            msg.content.contains("before delivery."),
            "the note names the wait: {}",
            msg.content
        );
        // The structured stamp rides alongside the content note.
        let md = msg.message_metadata.as_ref().expect("queueInfo stamped");
        assert_eq!(md["queueInfo"]["queuedAt"], queued_at);
        assert!(
            md["queueInfo"]["waitedMs"].as_u64().is_some(),
            "waitedMs is a non-negative integer: {md}"
        );
    }

    #[test]
    fn annotation_is_idempotent_across_requeues() {
        let mut msg = queued_msg("loop", "2026-01-01T00:00:00Z", false);
        super::super::annotate_dequeue_wait(&mut msg);
        let once = msg.content.clone();
        let stamped_once = msg.message_metadata.clone();
        super::super::annotate_dequeue_wait(&mut msg);
        assert_eq!(
            msg.content, once,
            "a requeued annotated entry is not double-annotated"
        );
        assert_eq!(
            msg.message_metadata, stamped_once,
            "a requeued entry keeps its first-delivery queueInfo"
        );
    }

    #[test]
    fn queue_info_merges_into_existing_metadata() {
        // An entry enqueued with metadata (e.g. a parent wake's
        // `event_notification` tag) keeps its fields; queueInfo is additive.
        let mut msg = queued_msg("tagged", "2026-01-01T00:00:00Z", false);
        msg.message_metadata = Some(json!({ "type": "event_notification" }));
        super::super::annotate_dequeue_wait(&mut msg);
        let md = msg.message_metadata.as_ref().unwrap();
        assert_eq!(md["type"], "event_notification", "existing fields kept");
        assert_eq!(md["queueInfo"]["queuedAt"], "2026-01-01T00:00:00Z");
    }

    #[test]
    fn existing_queue_info_is_never_overwritten() {
        // Belt-and-braces beyond the content-prefix guard: even if the note
        // is absent, a pre-existing queueInfo keeps its original numbers.
        let original = json!({ "queuedAt": "2026-01-01T00:00:00Z", "waitedMs": 42 });
        let mut msg = queued_msg("kept", "2026-02-02T00:00:00Z", false);
        msg.message_metadata = Some(json!({ "queueInfo": original.clone() }));
        super::super::annotate_dequeue_wait(&mut msg);
        assert_eq!(
            msg.message_metadata.as_ref().unwrap()["queueInfo"],
            original,
            "first-delivery queueInfo stays"
        );
    }

    #[test]
    fn sub_threshold_wait_is_not_annotated() {
        // An incidental queue hop (monorepo#2353): an entry that waited less
        // than the threshold is treated like an immediate delivery — no
        // [SYSTEM NOTE], no queueInfo stamp.
        let mut msg = queued_msg("instant hop", &intent_core::now_iso(), false);
        super::super::annotate_dequeue_wait(&mut msg);
        assert_eq!(
            msg.content, "instant hop",
            "sub-threshold wait → no dequeue-wait note"
        );
        assert_eq!(
            msg.message_metadata, None,
            "sub-threshold wait → no queueInfo stamp"
        );
    }

    #[test]
    fn wait_at_or_above_threshold_is_annotated() {
        // Just past the threshold: the note and stamp apply unchanged.
        let queued_at = iso_secs_ago(6);
        let mut msg = queued_msg("parked a while", &queued_at, false);
        super::super::annotate_dequeue_wait(&mut msg);
        assert!(
            msg.content.contains(super::super::DEQUEUE_WAIT_NOTE_PREFIX),
            "above-threshold wait carries the note: {}",
            msg.content
        );
        let md = msg.message_metadata.as_ref().expect("queueInfo stamped");
        assert_eq!(md["queueInfo"]["queuedAt"], queued_at);
        assert!(
            md["queueInfo"]["waitedMs"].as_u64().is_some_and(
                |ms| ms >= u64::try_from(super::super::DEQUEUE_WAIT_ANNOTATION_MIN_MS).unwrap()
            ),
            "waitedMs reflects the above-threshold wait: {md}"
        );
    }

    #[test]
    fn negative_wait_is_sub_threshold_and_skipped() {
        // Clock skew: an entry "queued in the future" reads as a negative
        // wait, which sits below the threshold and skips the annotation.
        let mut msg = queued_msg("skewed", "2999-01-01T00:00:00Z", false);
        super::super::annotate_dequeue_wait(&mut msg);
        assert_eq!(msg.content, "skewed", "negative wait → no note");
        assert_eq!(
            msg.message_metadata, None,
            "negative wait → no queueInfo stamp"
        );
    }

    #[test]
    fn persisted_requeue_is_never_rewritten() {
        // A terminal-failure requeue whose transcript row is already durable:
        // the content must stay byte-identical to the persisted row, so the
        // note is skipped entirely for such entries.
        let mut msg = queued_msg("already persisted", "2026-01-01T00:00:00Z", true);
        super::super::annotate_dequeue_wait(&mut msg);
        assert_eq!(
            msg.content, "already persisted",
            "persisted rows are never rewritten"
        );
        assert_eq!(
            msg.message_metadata, None,
            "persisted rows are never stamped"
        );
    }

    #[test]
    fn unparseable_queued_at_fails_open() {
        let mut msg = queued_msg("hello", "not-a-timestamp", false);
        super::super::annotate_dequeue_wait(&mut msg);
        assert_eq!(msg.content, "hello", "unparseable queued_at → no note");
        assert_eq!(
            msg.message_metadata, None,
            "unparseable queued_at → no queueInfo stamp"
        );
    }

    #[test]
    fn wait_duration_formatting() {
        assert_eq!(super::super::format_wait_duration(-5), "0s");
        assert_eq!(super::super::format_wait_duration(0), "0s");
        assert_eq!(super::super::format_wait_duration(59), "59s");
        assert_eq!(super::super::format_wait_duration(60), "1m 0s");
        assert_eq!(super::super::format_wait_duration(125), "2m 5s");
        assert_eq!(super::super::format_wait_duration(3600), "1h 0m");
        assert_eq!(super::super::format_wait_duration(3750), "1h 2m");
    }

    /// `agent.sendQueuedMessageNow` runtime path: the delivered (and
    /// persisted) content carries the dequeue-wait note. The entry's
    /// `queued_at` is backdated past the threshold — a fresh enqueue would
    /// drain sub-threshold and skip the annotation (monorepo#2353).
    #[tokio::test]
    async fn send_queued_message_now_annotates_dequeue_wait() {
        let (_tmp, mgr) = manager().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (WorkspaceId::from("ws-wait-a"), AgentId::from("a-wait-a"));
        seed_agent(&mgr, &ws, &id).await;
        let _agent = track_mock_agent(&mgr, &id, false);
        let queued = mgr
            .services
            .agent_queue_message_op(id.clone(), "queued work".into(), None, None)
            .await
            .expect("queue");
        let entry_id = queued["queuedMessage"]["id"].as_str().unwrap().to_string();
        // Backdate the enqueue so the drain-time wait clears the threshold.
        let mut entry = mgr
            .services
            .take_queued_message(&id, &entry_id)
            .expect("entry queued");
        entry.queued_at = iso_secs_ago(10);
        mgr.services.requeue_front(&id, entry);

        let result = mgr
            .send_queued_message_now(id.clone(), ws.clone(), entry_id.clone())
            .await
            .expect("send queued now");
        assert_eq!(result["queued"], json!(false));

        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .expect("messages");
        let row = messages
            .iter()
            .find(|m| m.id == entry_id)
            .expect("user row persisted under the entry id");
        let text = serde_json::to_string(&row.content).unwrap();
        assert!(
            text.contains("queued work"),
            "original content preserved: {text}"
        );
        assert!(
            text.contains(super::super::DEQUEUE_WAIT_NOTE_PREFIX),
            "dequeue-wait note appended: {text}"
        );
        // The structured queueInfo stamp reaches the persisted row metadata.
        let md = row.metadata.as_ref().expect("row carries queueInfo");
        assert!(
            md["queueInfo"]["queuedAt"].as_str().is_some(),
            "queuedAt is the entry's ISO enqueue timestamp: {md}"
        );
        assert!(
            md["queueInfo"]["waitedMs"].as_u64().is_some(),
            "waitedMs is a non-negative integer: {md}"
        );
    }

    /// A message delivered immediately (never queued) is NOT annotated: the
    /// direct-send branch never constructs a queue entry, so its persisted
    /// user row is byte-identical to what the caller sent.
    #[tokio::test]
    async fn immediate_delivery_is_not_annotated() {
        let (_tmp, mgr) = manager().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (WorkspaceId::from("ws-wait-b"), AgentId::from("a-wait-b"));
        seed_agent(&mgr, &ws, &id).await;
        let _agent = track_mock_agent(&mgr, &id, false);

        let result = mgr
            .send_message(
                id.clone(),
                ws.clone(),
                "direct hello".to_string(),
                None,
                super::super::TurnOptions::default(),
            )
            .await
            .expect("direct send");
        assert_eq!(result["queued"], json!(false), "delivered immediately");

        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .expect("messages");
        let row = messages
            .iter()
            .find(|m| m.role == "user")
            .expect("direct user row persisted");
        let text = row.content[0]["text"].as_str().unwrap();
        assert_eq!(
            text, "direct hello",
            "immediate deliveries carry no dequeue-wait note"
        );
    }
}

/// A2A sender header on the delivery paths (monorepo#1015): the runtime
/// `send_message` front door prepends the header for agent-origin sends
/// (daemon-stamped `fromAgentId` in the metadata), user sends stay
/// byte-identical, and a queued agent-origin entry drains with the header on
/// top and the dequeue-wait note appended BELOW (the header rides the entry
/// content from enqueue time; the wait note is appended at drain).
mod sender_header_tests {
    use super::dequeue_wait_tests::{iso_secs_ago, queued_msg};
    use super::*;
    use crate::harness::v1::A2A_SENDER_NOTE_PREFIX;

    #[tokio::test]
    async fn agent_origin_send_persists_header() {
        let (_tmp, mgr) = manager().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (WorkspaceId::from("ws-a2a-a"), AgentId::from("a-a2a-a"));
        seed_agent(&mgr, &ws, &id).await;
        let _agent = track_mock_agent(&mgr, &id, false);

        let options = super::super::TurnOptions {
            message_metadata: Some(json!({
                "fromAgentId": "agent-sender",
                "fromAgentName": "Coordinator",
            })),
            ..super::super::TurnOptions::default()
        };
        let result = mgr
            .send_message(
                id.clone(),
                ws.clone(),
                "do the thing".to_string(),
                None,
                options,
            )
            .await
            .expect("send");
        assert_eq!(result["queued"], json!(false), "delivered immediately");

        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .expect("messages");
        let row = messages
            .iter()
            .find(|m| m.role == "user")
            .expect("user row persisted");
        let text = row.content[0]["text"].as_str().unwrap();
        assert_eq!(
            text, "[MESSAGE FROM AGENT Coordinator (agent-sender)]\n\ndo the thing",
            "agent-origin content carries the sender header"
        );
        // Metadata is unchanged — the attribution fields stay authoritative.
        let md = row.metadata.as_ref().expect("row metadata");
        assert_eq!(md["fromAgentId"], json!("agent-sender"));
    }

    #[tokio::test]
    async fn user_send_stays_byte_identical() {
        let (_tmp, mgr) = manager().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (WorkspaceId::from("ws-a2a-b"), AgentId::from("a-a2a-b"));
        seed_agent(&mgr, &ws, &id).await;
        let _agent = track_mock_agent(&mgr, &id, false);

        let result = mgr
            .send_message(
                id.clone(),
                ws.clone(),
                "plain user message".to_string(),
                None,
                super::super::TurnOptions::default(),
            )
            .await
            .expect("send");
        assert_eq!(result["queued"], json!(false));

        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .expect("messages");
        let row = messages
            .iter()
            .find(|m| m.role == "user")
            .expect("user row persisted");
        assert_eq!(
            row.content[0]["text"].as_str().unwrap(),
            "plain user message",
            "no fromAgentId → no header"
        );
    }

    /// A queued agent-origin entry: the header is already on the content
    /// (prepended at the send front door before the enqueue) and the
    /// drain-time dequeue-wait note appends BELOW it — header first, wait
    /// note last.
    #[test]
    fn queued_entry_drains_with_header_above_wait_note() {
        let mut content = "queued work".to_string();
        crate::agent_ops::annotate_sender_attribution(
            &mut content,
            Some(&json!({ "fromAgentId": "agent-sender", "fromAgentName": "Coordinator" })),
        );
        let mut msg = queued_msg(&content, &iso_secs_ago(10), false);
        super::super::annotate_dequeue_wait(&mut msg);
        assert!(
            msg.content.starts_with(A2A_SENDER_NOTE_PREFIX),
            "sender header stays on top: {}",
            msg.content
        );
        assert!(
            msg.content.contains("queued work"),
            "original content preserved: {}",
            msg.content
        );
        let header_pos = msg.content.find(A2A_SENDER_NOTE_PREFIX).unwrap();
        let wait_pos = msg
            .content
            .find(super::super::DEQUEUE_WAIT_NOTE_PREFIX)
            .expect("dequeue-wait note appended");
        assert!(
            header_pos < wait_pos,
            "wait note appends below the header: {}",
            msg.content
        );
    }
}

/// Batch-flush grouping stamp: [`super::stamp_flush_batch_id`] mints ONE
/// fresh `queueInfo.batchId` per multi-entry flush and stamps it on every
/// drained entry — including sub-threshold-wait entries that carry no other
/// queueInfo — without ever overwriting existing queueInfo fields, skipping
/// `persisted: true` requeues, and never stamping a single-entry drain.
#[cfg(test)]
mod flush_batch_id_tests {
    use super::dequeue_wait_tests::{iso_secs_ago, queued_msg};
    use super::*;

    #[test]
    fn flush_stamps_one_shared_batch_id_on_all_entries() {
        // Both entries waited past the annotation threshold, so the wait
        // stamp runs first (as in prepare_flush_turn) and batchId lands
        // NEXT TO queuedAt/waitedMs inside the same queueInfo object.
        let queued_at = iso_secs_ago(60);
        let mut entries = vec![
            queued_msg("first", &queued_at, false),
            queued_msg("second", &queued_at, false),
        ];
        for e in &mut entries {
            super::super::annotate_dequeue_wait(e);
        }
        super::super::stamp_flush_batch_id(&mut entries);
        let info = |i: usize| entries[i].message_metadata.as_ref().unwrap()["queueInfo"].clone();
        let batch_id = info(0)["batchId"]
            .as_str()
            .expect("batchId is a string")
            .to_string();
        assert!(!batch_id.is_empty(), "batchId is non-empty");
        assert_eq!(
            info(1)["batchId"].as_str(),
            Some(batch_id.as_str()),
            "every entry of the batch shares ONE batchId"
        );
        for i in [0, 1] {
            assert!(
                info(i)["queuedAt"].as_str().is_some() && info(i)["waitedMs"].as_u64().is_some(),
                "wait-stamp fields ride alongside batchId: {}",
                info(i)
            );
        }
    }

    #[test]
    fn single_entry_drain_gets_no_batch_id() {
        let mut entries = vec![queued_msg("solo", &iso_secs_ago(60), false)];
        super::super::stamp_flush_batch_id(&mut entries);
        assert_eq!(
            entries[0].message_metadata, None,
            "nothing to group — a single-entry drain is never stamped"
        );
    }

    #[test]
    fn sub_threshold_entry_in_batch_gets_batch_id_only() {
        // One entry drains sub-threshold (no wait note, no queuedAt/waitedMs
        // — monorepo#2353): it still gets the grouping stamp, as a queueInfo
        // carrying ONLY batchId.
        let mut entries = vec![
            queued_msg("waited", &iso_secs_ago(60), false),
            queued_msg("instant hop", &intent_core::now_iso(), false),
        ];
        for e in &mut entries {
            super::super::annotate_dequeue_wait(e);
        }
        assert_eq!(
            entries[1].message_metadata, None,
            "precondition: sub-threshold wait stamped no queueInfo"
        );
        super::super::stamp_flush_batch_id(&mut entries);
        let sub = &entries[1].message_metadata.as_ref().unwrap()["queueInfo"];
        assert!(sub["batchId"].as_str().is_some(), "batchId stamped: {sub}");
        assert_eq!(
            sub.as_object().unwrap().len(),
            1,
            "sub-threshold queueInfo carries ONLY batchId: {sub}"
        );
        assert_eq!(
            sub["batchId"],
            entries[0].message_metadata.as_ref().unwrap()["queueInfo"]["batchId"],
            "sub-threshold entry shares the batch's id"
        );
    }

    #[test]
    fn existing_queue_info_fields_are_never_overwritten() {
        // A requeued entry carrying its first-delivery queueInfo keeps
        // queuedAt/waitedMs untouched — batchId is purely additive.
        let mut entries = vec![
            queued_msg("fresh", &iso_secs_ago(60), false),
            queued_msg("requeued", &iso_secs_ago(60), false),
        ];
        entries[1].message_metadata = Some(json!({
            "queueInfo": { "queuedAt": "2026-01-01T00:00:00Z", "waitedMs": 42 }
        }));
        super::super::stamp_flush_batch_id(&mut entries);
        let info = &entries[1].message_metadata.as_ref().unwrap()["queueInfo"];
        assert_eq!(info["queuedAt"], "2026-01-01T00:00:00Z");
        assert_eq!(info["waitedMs"], 42);
        assert!(
            info["batchId"].as_str().is_some(),
            "batchId added alongside the kept fields: {info}"
        );
    }

    #[test]
    fn pre_existing_batch_id_is_kept() {
        // A mid-flush persist failure requeues entries already stamped;
        // keeping the first batch's id groups the retry's rows with the rows
        // the first attempt already persisted.
        let mut entries = vec![
            queued_msg("retried", &iso_secs_ago(60), false),
            queued_msg("fresh", &iso_secs_ago(60), false),
        ];
        entries[0].message_metadata = Some(json!({ "queueInfo": { "batchId": "batch-orig" } }));
        super::super::stamp_flush_batch_id(&mut entries);
        assert_eq!(
            entries[0].message_metadata.as_ref().unwrap()["queueInfo"]["batchId"],
            "batch-orig",
            "an already-stamped entry keeps its first batch's id"
        );
    }

    #[test]
    fn persisted_entries_are_never_stamped() {
        // The persisted entry's transcript row is already durable and never
        // rewritten; the fresh entry in the same flush is still stamped.
        let mut entries = vec![
            queued_msg("already durable", &iso_secs_ago(60), true),
            queued_msg("fresh", &iso_secs_ago(60), false),
        ];
        super::super::stamp_flush_batch_id(&mut entries);
        assert_eq!(
            entries[0].message_metadata, None,
            "persisted requeues are never stamped"
        );
        assert!(
            entries[1].message_metadata.as_ref().unwrap()["queueInfo"]["batchId"]
                .as_str()
                .is_some(),
            "the batch's fresh entries are still stamped"
        );
    }
}

/// Delivery-time "tasks now unblocked" annotation (intent-hq/monorepo#2044):
/// [`super::annotate_unblocked_hints`] resolves the stamped trigger ids
/// against CURRENT task state as a batch drains, coalescing all
/// trigger-carrying entries into one section on the last of them.
#[cfg(test)]
mod unblocked_hints_tests {
    use super::dequeue_wait_tests::queued_msg;
    use super::*;
    use crate::agent_ops::ready_delta::{stamp_trigger_tasks, UNBLOCKED_SECTION_PREFIX};
    use intent_core::{NoteCreate, NoteId, WorkspaceApi};

    async fn seed_task(services: &Services, ws: &WorkspaceId, title: &str, status: &str) -> NoteId {
        let note = services
            .create_note(
                ws.clone(),
                NoteCreate {
                    title: title.into(),
                    content: Some("body".into()),
                    tags: None,
                    parent_id: None,
                },
                None,
                None,
            )
            .await
            .expect("create note")
            .note;
        WorkspaceApi::mark_as_task(
            services,
            ws.clone(),
            note.id.clone(),
            status.into(),
            vec![],
            None,
            None,
            None,
            None,
        )
        .await
        .expect("mark as task");
        note.id
    }

    /// Two completion wakes draining in one batch coalesce into ONE section
    /// appended to the LAST trigger-carrying entry; the delta reflects task
    /// state at annotation time (both deps complete → the gated task rows
    /// once, not per-wake).
    #[tokio::test]
    async fn batch_coalesces_triggers_into_one_section_on_last_entry() {
        // The section is gated behind `agentFeatures.taskGraph`
        // (intent-hq/monorepo#2445), so wire a registry with it explicitly on.
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let bus = EventBus::new(store.clone());
        let cfg = tempfile::tempdir().expect("temp config dir");
        let cfg_path = cfg.path().join("config.toml");
        std::fs::write(&cfg_path, "[agentFeatures]\ntaskGraph = true\n").expect("write config");
        let registry = Arc::new(crate::SettingsRegistry::load(&cfg_path).expect("load registry"));
        let services = Services::new(store)
            .with_event_bus(bus.clone())
            .with_settings_registry(registry);
        let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus));
        let mgr = AgentManager::new(services, sink, 8);
        let ws = WorkspaceId::from("ws-unblocked");
        let parent = AgentId::from("seed-unblocked");
        seed_agent_with_task_graph(&mgr, &ws, &parent, true).await;
        let services = &mgr.services;
        let a = seed_task(services, &ws, "Task A", "complete").await;
        let b = seed_task(services, &ws, "Task B", "complete").await;
        let gated = seed_task(services, &ws, "Gated", "not_started").await;
        services
            .task_set_relations(
                ws.clone(),
                gated.clone(),
                Some(vec![a.clone(), b.clone()]),
                None,
            )
            .await
            .expect("gated dependsOn a+b");

        let mut wake_a = queued_msg("[WORKSPACE EVENTS] Child A completed.", &now_iso(), false);
        let mut md_a = json!({ "type": "event_notification" });
        stamp_trigger_tasks(&mut md_a, &[(ws.0.clone(), a.0.clone())]);
        wake_a.message_metadata = Some(md_a);
        let mut wake_b = queued_msg("[WORKSPACE EVENTS] Child B completed.", &now_iso(), false);
        let mut md_b = json!({ "type": "event_notification" });
        stamp_trigger_tasks(&mut md_b, &[(ws.0.clone(), b.0.clone())]);
        wake_b.message_metadata = Some(md_b);
        let plain = queued_msg("unrelated user message", &now_iso(), false);

        let mut entries = vec![wake_a, plain, wake_b];
        super::super::annotate_unblocked_hints(services, &parent, &mut entries).await;

        assert!(
            !entries[0].content.contains(UNBLOCKED_SECTION_PREFIX),
            "first wake is not annotated (coalesced onto the last): {}",
            entries[0].content
        );
        assert!(
            !entries[1].content.contains(UNBLOCKED_SECTION_PREFIX),
            "non-trigger entry untouched: {}",
            entries[1].content
        );
        let last = &entries[2].content;
        assert!(
            last.contains("Tasks now unblocked by these completions:"),
            "last trigger-carrying entry carries the plural section: {last}"
        );
        assert!(
            last.contains(&format!("[Gated](intent://local/task/{})", gated.0)),
            "section names the unblocked task once with a link: {last}"
        );
        assert_eq!(
            last.matches("[Gated]").count(),
            1,
            "coalesced delta lists the task once: {last}"
        );
    }

    /// Idempotency + persisted guards: an entry whose content already carries
    /// the section (terminal-failure requeue) and a `persisted: true` entry
    /// are never (re)annotated.
    #[tokio::test]
    async fn requeued_and_persisted_entries_are_not_reannotated() {
        let (_tmp, mgr) = manager().await;
        let ws = WorkspaceId::from("ws-unblocked-idem");
        let parent = AgentId::from("seed-unblocked-idem");
        seed_agent(&mgr, &ws, &parent).await;
        let services = &mgr.services;
        let a = seed_task(services, &ws, "Task A", "complete").await;
        let gated = seed_task(services, &ws, "Gated", "not_started").await;
        services
            .task_set_relations(ws.clone(), gated.clone(), Some(vec![a.clone()]), None)
            .await
            .expect("gated dependsOn a");
        let mut md = json!({ "type": "event_notification" });
        stamp_trigger_tasks(&mut md, &[(ws.0.clone(), a.0.clone())]);

        // Already-annotated content (requeue after a failed turn).
        let mut annotated = queued_msg(
            &format!("wake\n\n{UNBLOCKED_SECTION_PREFIX} this completion: x."),
            &now_iso(),
            false,
        );
        annotated.message_metadata = Some(md.clone());
        let before = annotated.content.clone();
        let mut entries = vec![annotated];
        super::super::annotate_unblocked_hints(services, &parent, &mut entries).await;
        assert_eq!(entries[0].content, before, "no double annotation");

        // Persisted rows stay byte-identical to the transcript.
        let mut persisted = queued_msg("wake", &now_iso(), true);
        persisted.message_metadata = Some(md);
        let mut entries = vec![persisted];
        super::super::annotate_unblocked_hints(services, &parent, &mut entries).await;
        assert_eq!(entries[0].content, "wake", "persisted rows never rewritten");
    }
}

#[cfg(test)]
mod harness_wake_tests {
    //! Implicit agent-initiated turns for out-of-turn `session/update`s
    //! (monorepo#855): the idle-listener tick opens a harness-wake turn,
    //! streams via the standard router, finalizes on quiescence, and
    //! coexists with racing prompt sends and the resume-replay gate.

    use super::*;

    /// A passive handle plus the notification sender that feeds its receiver,
    /// standing in for the child's out-of-turn `session/update` stream.
    fn wake_handle() -> (AgentHandle, mpsc::UnboundedSender<IncomingNotification>) {
        let (client_w, _agent_r) = tokio::io::duplex(1024);
        let (_agent_w, client_r) = tokio::io::duplex(1024);
        let (note_tx, note_rx) = mpsc::unbounded_channel();
        let connection = Arc::new(Connection::new(
            client_w,
            client_r,
            None,
            ConnectionHooks::default(),
        ));
        let handle = AgentHandle {
            connection,
            notifications: Arc::new(TokioMutex::new(note_rx)),
            serve_task: tokio::spawn(async {}),
            child: None,
            child_pid: None,
            _mcp_bridge: None,
            _mcp_config: None,
            _rules_config: None,
            _pi_extension: None,
            session_mcp_servers: Vec::new(),
            spawned_model: None,
            spawned_provider: "auggie".to_string(),
            thought_level: None,
            wake_gate: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_listener: None,
        };
        (handle, note_tx)
    }

    fn chunk_note(text: &str) -> IncomingNotification {
        IncomingNotification {
            method: "session/update".to_string(),
            params: json!({
                "sessionId": "acp-wake",
                "update": { "sessionUpdate": "agent_message_chunk",
                    "content": { "type": "text", "text": text } }
            }),
        }
    }

    /// An update variant with no canonical event mapping (dropped by the
    /// router in prompt turns; must not open an implicit turn).
    fn unmappable_note() -> IncomingNotification {
        IncomingNotification {
            method: "session/update".to_string(),
            params: json!({
                "sessionId": "acp-wake",
                "update": { "sessionUpdate": "plan", "entries": [] }
            }),
        }
    }

    /// A title-less `tool_call_update` first-sight — the STAB-124 late echo a
    /// cancelled child emits after the interrupt drain window. Mappable, but
    /// name-less: the transcript's `record_tool` drops it, so it must not
    /// open a phantom implicit turn.
    fn titleless_tool_update_note() -> IncomingNotification {
        IncomingNotification {
            method: "session/update".to_string(),
            params: json!({
                "sessionId": "acp-wake",
                "update": { "sessionUpdate": "tool_call_update",
                    "toolCallId": "tc_stale", "status": "failed" }
            }),
        }
    }

    async fn wake_setup() -> (
        TempDb,
        Arc<AgentManager>,
        EventBus,
        AgentId,
        WorkspaceId,
        mpsc::UnboundedSender<IncomingNotification>,
    ) {
        let (tmp, mgr, bus) = manager_with_bus().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("agent-wake"));
        seed_agent(&mgr, &ws, &id).await;
        let (handle, note_tx) = wake_handle();
        mgr.handles.lock().unwrap().insert(id.clone(), handle);
        mgr.registry.register(id.clone(), mgr.make_kill(id.clone()));
        (tmp, mgr, bus, id, ws, note_tx)
    }

    /// Collect bus events until `pred` matches or the timeout elapses.
    async fn collect_until(
        sub: &mut crate::events::Subscription,
        pred: impl Fn(&[intent_core::Event]) -> bool,
    ) -> Vec<intent_core::Event> {
        let mut seen: Vec<intent_core::Event> = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !pred(&seen) {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(remaining, sub.recv()).await {
                Ok(Some(batch)) => seen.extend(batch),
                _ => break,
            }
        }
        seen
    }

    /// Out-of-turn chunk burst → one implicit turn: `agent:stream:start`
    /// (reason `harness-wake`), streamed chunks, quiescence finalize with a
    /// persisted assistant row, one `agent:stream:end` carrying the
    /// `messageId`, and `agent:idle` (reason `harness_wake_complete`).
    #[tokio::test]
    async fn out_of_turn_burst_streams_as_implicit_turn() {
        let (_tmp, mgr, bus, id, ws, note_tx) = wake_setup().await;
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        note_tx.send(chunk_note("Hello ")).unwrap();
        note_tx.send(chunk_note("world")).unwrap();
        assert!(
            mgr.wake_listener_tick(&id, &ws).await,
            "handle alive → listener keeps running"
        );

        let events = collect_until(&mut sub, |seen| {
            seen.iter().any(|e| e.event_type == "agent:idle")
        })
        .await;
        let start = events
            .iter()
            .find(|e| e.event_type == "agent:stream:start")
            .expect("implicit turn emits stream:start");
        assert_eq!(start.data["agentId"], json!(id.0));
        assert_eq!(start.data["reason"], json!("harness-wake"));
        let message_id = start.data["messageId"].as_str().expect("messageId minted");
        let ends: Vec<_> = events
            .iter()
            .filter(|e| e.event_type == "agent:stream:end")
            .collect();
        assert_eq!(ends.len(), 1, "exactly one terminal stream:end");
        assert_eq!(ends[0].data["messageId"], json!(message_id));
        assert_eq!(
            ends[0].data["lastAgentResponse"],
            json!("Hello world"),
            "harness-wake terminal stream:end carries the final preview"
        );
        let idle = events
            .iter()
            .find(|e| e.event_type == "agent:idle")
            .expect("agent:idle after finalize");
        assert_eq!(idle.data["reason"], json!("harness_wake_complete"));

        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1, "one assistant row persisted");
        assert_eq!(messages[0].role, "assistant");
        assert_eq!(messages[0].id, message_id);
        assert_eq!(messages[0].content[0]["text"], json!("Hello world"));
        assert!(
            !mgr.is_busy(&id),
            "single-flight slot released after finalize"
        );
        assert!(
            mgr.services.live_turn(&id).is_none(),
            "live-turn slot cleared"
        );
    }

    /// The intent-hq/monorepo#3262 incident shape at the tick level: a wake
    /// burst whose whole output is one whitespace-only chunk (the bare "\n")
    /// must NOT be accepted as a successful recovery. The seeded agent is
    /// root/taskless (not redrive-eligible), so the recovery raises a
    /// `"blocker"` attention request and the wake idle carries the advisory
    /// `emptyWakeResponse: true` — the workspace visibly needs input instead
    /// of looking healthy.
    #[tokio::test]
    async fn empty_wake_burst_raises_attention_and_annotates_idle() {
        let (_tmp, mgr, bus, id, ws, note_tx) = wake_setup().await;
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        note_tx.send(chunk_note("\n")).unwrap();
        assert!(mgr.wake_listener_tick(&id, &ws).await);

        let events = collect_until(&mut sub, |seen| {
            seen.iter().any(|e| e.event_type == "agent:idle")
        })
        .await;
        let idle = events
            .iter()
            .find(|e| e.event_type == "agent:idle")
            .expect("wake idle still fires (attention arm enqueues nothing)");
        assert_eq!(idle.data["reason"], json!("harness_wake_complete"));
        assert_eq!(
            idle.data["emptyWakeResponse"],
            json!(true),
            "no-op recovery wake carries the advisory flag: {idle:?}"
        );
        assert!(
            events
                .iter()
                .any(|e| e.event_type == "agent:attention-requested"),
            "empty wake surfaces agent:attention-requested"
        );
        let session = mgr.services.store.get_agent_session(&id).await.unwrap();
        assert_eq!(
            session.attention_request_kind.as_deref(),
            Some("blocker"),
            "attention request persisted on the session"
        );
        assert!(!mgr.is_busy(&id), "slot released after finalize");
    }

    /// Healthy wake turns keep today's behavior end-to-end: meaningful chunk
    /// output finalizes with a plain `harness_wake_complete` idle — no
    /// `emptyWakeResponse` stamp, no attention request (no false positive on
    /// the recovery path).
    #[tokio::test]
    async fn meaningful_wake_burst_idles_without_recovery() {
        let (_tmp, mgr, bus, id, ws, note_tx) = wake_setup().await;
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        note_tx
            .send(chunk_note("[compaction] context compacted"))
            .unwrap();
        assert!(mgr.wake_listener_tick(&id, &ws).await);

        let events = collect_until(&mut sub, |seen| {
            seen.iter().any(|e| e.event_type == "agent:idle")
        })
        .await;
        let idle = events
            .iter()
            .find(|e| e.event_type == "agent:idle")
            .expect("agent:idle after finalize");
        assert!(
            idle.data.get("emptyWakeResponse").is_none(),
            "healthy wake omits the advisory flag: {idle:?}"
        );
        assert!(
            !events
                .iter()
                .any(|e| e.event_type == "agent:attention-requested"),
            "no attention raised for a meaningful wake"
        );
        let session = mgr.services.store.get_agent_session(&id).await.unwrap();
        assert!(
            session.attention_request_kind.is_none(),
            "no attention request persisted"
        );
    }

    /// A burst with no mappable update opens no turn: no events, no rows,
    /// slot untouched.
    #[tokio::test]
    async fn unmappable_burst_opens_no_turn() {
        let (_tmp, mgr, bus, id, ws, note_tx) = wake_setup().await;
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        note_tx.send(unmappable_note()).unwrap();
        assert!(mgr.wake_listener_tick(&id, &ws).await);

        assert!(
            timeout(Duration::from_millis(150), sub.recv())
                .await
                .is_err(),
            "no events published for an unmappable burst"
        );
        assert!(
            mgr.services
                .store
                .get_agent_messages(&id, None)
                .await
                .unwrap()
                .is_empty(),
            "no assistant row persisted"
        );
        assert!(!mgr.is_busy(&id), "slot never claimed");
    }

    /// A title-less `tool_call_update` first-sight (STAB-124 late echo) maps
    /// to `MappedUpdate::ToolCall` but produces no transcript content, so it
    /// must not open a phantom turn: no `stream:start`/`stream:end` pair, no
    /// rows, slot untouched.
    #[tokio::test]
    async fn titleless_tool_update_opens_no_phantom_turn() {
        let (_tmp, mgr, bus, id, ws, note_tx) = wake_setup().await;
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        note_tx.send(titleless_tool_update_note()).unwrap();
        assert!(mgr.wake_listener_tick(&id, &ws).await);

        assert!(
            timeout(Duration::from_millis(150), sub.recv())
                .await
                .is_err(),
            "no events published for a name-less tool-call first-sight"
        );
        assert!(
            mgr.services
                .store
                .get_agent_messages(&id, None)
                .await
                .unwrap()
                .is_empty(),
            "no assistant row persisted"
        );
        assert!(!mgr.is_busy(&id), "slot never claimed");
    }

    /// Soft-retire inertness at the listener level (PR review): a retired
    /// session's live handle can still receive a delayed out-of-turn
    /// `session/update`, but the tick must NOT open an implicit harness-wake
    /// turn — retiring removes event subscriptions, not the handle, so this
    /// gate is what keeps a retired session from persisting new turns. The
    /// buffered notification stays untouched; after `agent.restore` the next
    /// tick consumes it into a normal implicit turn.
    #[tokio::test]
    async fn retired_session_skips_wake_tick_until_restore() {
        let (_tmp, mgr, bus, id, ws, note_tx) = wake_setup().await;
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        mgr.services
            .agent_retire_op(id.clone(), Some(ws.clone()), None)
            .await
            .expect("retire");

        note_tx.send(chunk_note("late child tail")).unwrap();
        assert!(
            mgr.wake_listener_tick(&id, &ws).await,
            "listener keeps running (handle stays alive) while retired"
        );

        let events = collect_until(&mut sub, |seen| {
            seen.iter().any(|e| e.event_type == "agent:stream:start")
        })
        .await;
        assert!(
            !events.iter().any(|e| e.event_type == "agent:stream:start"),
            "no implicit turn driven on a retired session"
        );
        assert!(
            mgr.services
                .store
                .get_agent_messages(&id, None)
                .await
                .unwrap()
                .is_empty(),
            "no assistant row persisted while retired"
        );
        assert!(!mgr.is_busy(&id), "slot never claimed while retired");

        // Restore returns the session to service; the buffered notification
        // was left untouched and the next tick consumes it normally.
        mgr.services
            .agent_restore_op(id.clone(), Some(ws.clone()))
            .await
            .expect("restore");
        assert!(mgr.wake_listener_tick(&id, &ws).await);
        let events = collect_until(&mut sub, |seen| {
            seen.iter().any(|e| e.event_type == "agent:stream:end")
        })
        .await;
        assert!(
            events.iter().any(|e| e.event_type == "agent:stream:end"),
            "post-restore tick opens the implicit turn"
        );
        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .unwrap();
        assert_eq!(
            messages.len(),
            1,
            "the parked burst persisted after restore"
        );
        assert_eq!(messages[0].content[0]["text"], json!("late child tail"));
    }

    /// monorepo#2118 (PR review) — a tick losing the slot to a held REAP
    /// claim must NOT drive a harness wake turn: unlike a loss to a prompt
    /// worker (which owns the slot and finishes the turn), nobody owns the
    /// slot during a reap kill and the handle is being torn down, so driving
    /// the consumed notification would run unowned concurrent work in the
    /// kill window. The tick drops the notification: no events, no rows, no
    /// slot claim — and once the claim is released, a later burst opens an
    /// implicit turn normally.
    #[tokio::test]
    async fn tick_losing_to_reap_claim_drops_notification_without_wake_turn() {
        let (_tmp, mgr, bus, id, ws, note_tx) = wake_setup().await;
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        mgr.reap_claims.lock().unwrap().insert(id.clone());
        note_tx.send(chunk_note("dying child tail")).unwrap();
        assert!(mgr.wake_listener_tick(&id, &ws).await);

        assert!(
            timeout(Duration::from_millis(150), sub.recv())
                .await
                .is_err(),
            "no wake turn driven against the handle being killed"
        );
        assert!(
            mgr.services
                .store
                .get_agent_messages(&id, None)
                .await
                .unwrap()
                .is_empty(),
            "no assistant row persisted"
        );
        assert!(!mgr.is_busy(&id), "slot never claimed");

        // Claim released (kill done): the listener works normally again.
        mgr.reap_claims.lock().unwrap().remove(&id);
        note_tx.send(chunk_note("fresh burst")).unwrap();
        assert!(mgr.wake_listener_tick(&id, &ws).await);
        let events = collect_until(&mut sub, |seen| {
            seen.iter().any(|e| e.event_type == "agent:stream:end")
        })
        .await;
        assert!(
            events.iter().any(|e| e.event_type == "agent:stream:end"),
            "post-release burst opens an implicit turn normally"
        );
        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1, "only the post-release burst persisted");
        assert_eq!(messages[0].content[0]["text"], json!("fresh burst"));
    }

    /// `interrupt` aborts an open wake turn like a prompt turn: the drive
    /// task registered in `workers` is aborted, the streamed-so-far content
    /// is flushed as an interrupted row, the slot is released, and the single
    /// terminal `agent:stream:end` carries `stopReason: "interrupted"` — no
    /// duplicate finalize from the aborted wake turn.
    #[tokio::test]
    async fn interrupt_aborts_open_wake_turn() {
        let (_tmp, mgr, bus, id, ws, note_tx) = wake_setup().await;
        mgr.services
            .store
            .set_acp_session_id(&ws, &id, "acp-wake")
            .await
            .unwrap();
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        note_tx.send(chunk_note("partial wake output")).unwrap();
        assert!(mgr.wake_listener_tick(&id, &ws).await);
        assert!(
            mgr.workers.lock().unwrap().contains_key(&id),
            "wake-turn drive registered in workers"
        );

        // Wait until the chunk is routed (live-turn slot has content), then
        // interrupt mid-settle-window.
        let events = collect_until(&mut sub, |seen| {
            seen.iter().any(|e| e.event_type == "chat:stream:delta")
        })
        .await;
        assert!(
            events.iter().any(|e| e.event_type == "chat:stream:delta"),
            "wake turn streamed the chunk"
        );
        assert!(mgr.interrupt(&id).await, "interrupt found the agent");

        let events = collect_until(&mut sub, |seen| {
            seen.iter().any(|e| e.event_type == "agent:stream:end")
        })
        .await;
        let ends: Vec<_> = events
            .iter()
            .filter(|e| e.event_type == "agent:stream:end")
            .collect();
        assert_eq!(ends.len(), 1, "exactly one terminal stream:end");
        assert_eq!(ends[0].data["stopReason"], json!("interrupted"));
        assert_eq!(
            ends[0].data["lastAgentResponse"],
            json!("partial wake output"),
            "interrupt terminal stream:end carries the flushed partial preview"
        );
        let message_id = ends[0].data["messageId"]
            .as_str()
            .expect("interrupt flush persisted the partial row");

        // Give the aborted drive task time to have emitted a duplicate
        // finalize if the abort had not landed (its settle window elapses).
        tokio::time::sleep(crate::agent_manager::HARNESS_WAKE_SETTLE + Duration::from_millis(100))
            .await;
        let mut late: Vec<intent_core::Event> = Vec::new();
        while let Ok(Some(batch)) = timeout(Duration::from_millis(50), sub.recv()).await {
            late.extend(batch);
        }
        assert!(
            !late.iter().any(|e| e.event_type == "agent:stream:end"),
            "no duplicate stream:end from the aborted wake turn (got: {late:?})"
        );

        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1, "one interrupted assistant row");
        assert_eq!(messages[0].id, message_id);
        assert_eq!(messages[0].content[0]["text"], json!("partial wake output"));
        assert!(!mgr.is_busy(&id), "interrupt released the slot");
        assert!(
            mgr.workers.lock().unwrap().get(&id).is_none(),
            "aborted drive deregistered from workers"
        );
        assert!(
            mgr.services.live_turn(&id).is_none(),
            "live-turn slot cleared by the interrupt flush"
        );
    }

    /// A raised wake gate (resume-replay in flight) pauses the listener: the
    /// buffered notification is left untouched for the replay drain, and the
    /// tick opens nothing. Lowering the gate re-enables the listener.
    #[tokio::test]
    async fn wake_gate_pauses_listener_for_replay_drain() {
        let (_tmp, mgr, bus, id, ws, note_tx) = wake_setup().await;
        let gate = {
            let map = mgr.handles.lock().unwrap();
            map.get(&id).unwrap().wake_gate.clone()
        };
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        gate.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        note_tx.send(chunk_note("replay")).unwrap();
        assert!(mgr.wake_listener_tick(&id, &ws).await);
        assert!(
            timeout(Duration::from_millis(150), sub.recv())
                .await
                .is_err(),
            "gated tick consumes nothing and emits nothing"
        );

        // The replay drain (resume path) still sees the buffered burst.
        let notes = {
            let map = mgr.handles.lock().unwrap();
            map.get(&id).unwrap().notifications.clone()
        };
        {
            let mut guard = notes.lock().await;
            Services::drain_replay_notifications(&mut guard).await;
            assert!(guard.try_recv().is_err(), "replay burst drained");
        }
        gate.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

        // Gate lowered → a later out-of-turn burst opens a turn normally
        // (the tick spawns the drive task; wait for its finalize).
        note_tx.send(chunk_note("live")).unwrap();
        assert!(mgr.wake_listener_tick(&id, &ws).await);
        collect_until(&mut sub, |seen| {
            seen.iter().any(|e| e.event_type == "agent:stream:end")
        })
        .await;
        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1, "post-gate burst persisted");
        assert_eq!(messages[0].content[0]["text"], json!("live"));
    }

    /// The listener never consumes while a prompt turn owns the slot, and its
    /// tick reports the listener done once the handle is gone (stop path).
    #[tokio::test]
    async fn tick_skips_while_busy_and_exits_without_handle() {
        let (_tmp, mgr, _bus, id, ws, note_tx) = wake_setup().await;

        assert!(mgr.try_begin(&id, &ws).await, "claim slot as a prompt turn");
        note_tx.send(chunk_note("mid-prompt")).unwrap();
        assert!(mgr.wake_listener_tick(&id, &ws).await);
        {
            let notes = {
                let map = mgr.handles.lock().unwrap();
                map.get(&id).unwrap().notifications.clone()
            };
            let mut guard = notes.try_lock().expect("receiver not held");
            let buffered = guard.try_recv();
            assert!(
                buffered.is_ok(),
                "busy tick leaves the notification for the prompt turn"
            );
        }
        mgr.end_turn(&id).await;

        assert!(mgr.stop(&id).await, "stop removes the handle");
        assert!(
            !mgr.wake_listener_tick(&id, &ws).await,
            "tick reports listener shutdown once the handle is gone"
        );
    }

    /// Perf (PR review): with no notification buffered, the tick returns via
    /// the non-consuming peek without ever reaching `question_hold_active` —
    /// verified indirectly by arming a hold that would otherwise be a no-op
    /// gate (the tick is a no-op regardless, so this pins the "empty →
    /// short-circuit before any store read" contract by construction: an
    /// armed hold plus an empty channel must behave identically to no hold
    /// at all, and must not itself require a `question_hold_active` read to
    /// report that).
    #[tokio::test]
    async fn tick_with_no_buffered_notification_short_circuits_even_with_hold_armed() {
        let (_tmp, mgr, _bus, id, ws, _note_tx) = wake_setup().await;
        mgr.services
            .store
            .append_agent_message(
                &id,
                "assistant",
                &json!([
                    { "type": "text", "text": "Which scope?" },
                    {
                        "type": "resource",
                        "resource": {
                            "uri": "intent-question://q-1",
                            "name": "Scope",
                            "mimeType": "application/vnd.intent.question+json",
                            "text": "{\"question\":\"Which scope?\"}"
                        }
                    }
                ]),
                &now_iso(),
            )
            .await
            .expect("append question");
        assert!(mgr.services.question_hold_active(&id).await, "hold armed");

        // No notification sent: the tick must short-circuit on the peek and
        // leave the channel and hold state untouched.
        assert!(mgr.wake_listener_tick(&id, &ws).await);
        assert!(!mgr.is_busy(&id), "peek-only tick never claims the slot");
        assert!(
            mgr.services.question_hold_active(&id).await,
            "hold unaffected by the no-op tick"
        );
    }

    /// The hold still gates the tick once a notification IS buffered (the
    /// peek passes, and the existing `question_hold_active` check then
    /// blocks the implicit turn as before).
    #[tokio::test]
    async fn tick_with_buffered_notification_still_gated_by_hold() {
        let (_tmp, mgr, _bus, id, ws, note_tx) = wake_setup().await;
        mgr.services
            .store
            .append_agent_message(
                &id,
                "assistant",
                &json!([
                    { "type": "text", "text": "Which scope?" },
                    {
                        "type": "resource",
                        "resource": {
                            "uri": "intent-question://q-1",
                            "name": "Scope",
                            "mimeType": "application/vnd.intent.question+json",
                            "text": "{\"question\":\"Which scope?\"}"
                        }
                    }
                ]),
                &now_iso(),
            )
            .await
            .expect("append question");
        assert!(mgr.services.question_hold_active(&id).await, "hold armed");

        note_tx.send(chunk_note("auto wake output")).unwrap();
        assert!(mgr.wake_listener_tick(&id, &ws).await);
        assert!(!mgr.is_busy(&id), "hold blocks the implicit turn");

        // The buffered notification is left untouched (same contract as the
        // wake-gate-paused case) so it is not silently dropped.
        let notes = {
            let map = mgr.handles.lock().unwrap();
            map.get(&id).unwrap().notifications.clone()
        };
        let mut guard = notes.try_lock().expect("receiver not held");
        assert!(
            guard.try_recv().is_ok(),
            "notification left buffered for a later tick past the hold"
        );
    }

    /// A user send racing an active wake turn queues (slot is claimed) and
    /// the wake turn finalizes promptly — stream:end fires and the queued
    /// user row is persisted by the drain kick afterwards.
    #[tokio::test]
    async fn racing_send_queues_and_streams_after_wake_finalizes() {
        let (_tmp, mgr, bus, id, ws, note_tx) = wake_setup().await;
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        note_tx.send(chunk_note("wake output")).unwrap();
        let tick_mgr = mgr.clone();
        let (tick_id, tick_ws) = (id.clone(), ws.clone());
        let tick =
            tokio::spawn(async move { tick_mgr.wake_listener_tick(&tick_id, &tick_ws).await });

        // Wait until the implicit turn is live (stream:start observed).
        let events = collect_until(&mut sub, |seen| {
            seen.iter().any(|e| e.event_type == "agent:stream:start")
        })
        .await;
        assert!(
            events.iter().any(|e| e.event_type == "agent:stream:start"),
            "wake turn opened"
        );
        assert!(mgr.is_busy(&id), "wake turn holds the single-flight slot");

        // The racing user send goes to the queue (slot busy), which also
        // preempts the settle window — the wake turn finalizes first.
        mgr.services.enqueue_message(
            &id,
            "racing user message".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        assert!(mgr.services.has_ready_to_send(&id));

        let events = collect_until(&mut sub, |seen| {
            seen.iter().any(|e| e.event_type == "agent:stream:end")
        })
        .await;
        assert!(
            events.iter().any(|e| e.event_type == "agent:stream:end"),
            "wake turn finalized after the racing send"
        );
        assert!(
            !events.iter().any(|e| e.event_type == "agent:idle"),
            "idle suppressed — ready-to-send queue non-empty"
        );
        timeout(Duration::from_secs(5), tick)
            .await
            .expect("tick completes")
            .expect("tick task");

        // The wake turn's assistant row landed, and the drain kick persisted
        // the queued user row before spawning the follow-up prompt worker.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let messages = mgr
                .services
                .store
                .get_agent_messages(&id, None)
                .await
                .unwrap();
            let wake_row = messages
                .iter()
                .any(|m| m.role == "assistant" && m.content[0]["text"] == json!("wake output"));
            let user_row = messages.iter().any(|m| {
                m.role == "user"
                    && m.content[0]["text"]
                        .as_str()
                        .is_some_and(|t| t.starts_with("racing user message"))
            });
            if wake_row && user_row {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "wake assistant row + drained user row persisted (got: {messages:?})"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// The wake tick's race-loss path (a send claimed the slot between the
    /// busy check and `try_begin`) drives the already-consumed notification
    /// with a ZERO settle window: the implicit turn streams it, finalizes
    /// immediately without draining further, and leaves later buffered
    /// notifications untouched for the slot-owning prompt turn. No
    /// `agent:idle` is emitted — that duty stays with the slot's owner.
    #[tokio::test]
    async fn zero_settle_wake_turn_finalizes_immediately_leaving_buffer() {
        let (_tmp, mgr, bus, id, ws, note_tx) = wake_setup().await;
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        note_tx.send(chunk_note("pre-race output")).unwrap();
        note_tx.send(chunk_note("later burst")).unwrap();
        let notes = {
            let map = mgr.handles.lock().unwrap();
            map.get(&id).unwrap().notifications.clone()
        };
        let persisted_id = {
            let mut guard = notes.lock().await;
            let first = guard.try_recv().expect("first buffered note");
            let outcome = mgr
                .services
                .run_harness_wake_turn(&mut guard, first, &id, &ws, Duration::ZERO)
                .await;
            assert!(
                guard.try_recv().is_ok(),
                "zero-settle turn leaves the later note buffered for the slot owner"
            );
            assert!(
                !outcome.empty_response,
                "meaningful chunk output is not an empty wake response"
            );
            outcome.message_id.expect("assistant row persisted")
        };

        let events = collect_until(&mut sub, |seen| {
            seen.iter().any(|e| e.event_type == "agent:stream:end")
        })
        .await;
        let start = events
            .iter()
            .find(|e| e.event_type == "agent:stream:start")
            .expect("implicit turn emits stream:start");
        assert_eq!(start.data["reason"], json!("harness-wake"));
        let ends: Vec<_> = events
            .iter()
            .filter(|e| e.event_type == "agent:stream:end")
            .collect();
        assert_eq!(ends.len(), 1, "exactly one terminal stream:end");
        assert_eq!(ends[0].data["messageId"], json!(persisted_id));
        assert!(
            !events.iter().any(|e| e.event_type == "agent:idle"),
            "idle emit left to the slot's owner"
        );

        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1, "only the consumed note persisted");
        assert_eq!(messages[0].content[0]["text"], json!("pre-race output"));
    }
}

mod model_change_notice_tests {
    //! Unit tests for the model-change transcript notice: a turn starting
    //! under a different model/provider than the last committed turn persists
    //! one informational `role: "system"` row with
    //! `{ type: "model_changed", from, to, fromProvider, toProvider }`
    //! metadata and emits `agent:message`; first turns and unchanged turns
    //! persist nothing; picker toggles reverted before any message never
    //! produce a notice (the comparison is against the last COMMITTED turn).

    use super::*;
    use intent_core::events::AGENT_MESSAGE;

    fn resolved(provider_id: &str, model: Option<&str>) -> ResolvedSpawn {
        ResolvedSpawn {
            provider: *intent_providers::provider_config(provider_id),
            model: model.map(str::to_string),
            reasoning_effort: None,
            cwd: std::env::temp_dir(),
            provider_binary: None,
            extra_env: std::collections::BTreeMap::default(),
            npx_fallback_binary: None,
            npx_fallback_package: None,
            unsloth_endpoint: None,
        }
    }

    /// First turn: no committed prior identity → no notice, but the identity
    /// commits so the NEXT turn has a baseline to compare against.
    #[tokio::test]
    async fn first_turn_commits_baseline_without_notice() {
        let (_tmp, mgr) = manager().await;
        let (ws, id) = (WorkspaceId::from("ws-mc1"), AgentId::from("a-mc1"));
        seed_agent(&mgr, &ws, &id).await;

        mgr.maybe_persist_model_change_notice(&id, &ws, &resolved("auggie", Some("gpt-5")))
            .await;

        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .unwrap();
        assert!(messages.is_empty(), "first turn must not persist a notice");
        let (m, p) = mgr
            .services
            .store
            .get_agent_session_last_turn_model(&ws, &id)
            .await
            .unwrap();
        assert_eq!(m.as_deref(), Some("gpt-5"));
        assert_eq!(p.as_deref(), Some("auggie"));
    }

    /// A committed switch persists exactly one system row with the
    /// `model_changed` metadata shape and emits `agent:message`; a repeat
    /// turn under the same identity persists nothing further.
    #[tokio::test]
    async fn committed_switch_persists_notice_once_and_emits_event() {
        let (_tmp, mgr, bus) = manager_with_bus().await;
        let (ws, id) = (WorkspaceId::from("ws-mc2"), AgentId::from("a-mc2"));
        seed_agent(&mgr, &ws, &id).await;
        let mut sub = bus.subscribe(SubscriptionFilter {
            event_types: vec![AGENT_MESSAGE.to_string()],
            ..Default::default()
        });

        // Turn 1 commits the baseline; turn 2 switches model + provider.
        mgr.maybe_persist_model_change_notice(&id, &ws, &resolved("auggie", Some("gpt-5")))
            .await;
        mgr.maybe_persist_model_change_notice(&id, &ws, &resolved("claude-code", Some("sonnet")))
            .await;

        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1, "exactly one notice row");
        let notice = &messages[0];
        assert_eq!(notice.role, "system");
        let md = notice.metadata.as_ref().expect("notice carries metadata");
        assert_eq!(md["type"], json!("model_changed"));
        assert_eq!(md["from"], json!("gpt-5"));
        assert_eq!(md["to"], json!("sonnet"));
        assert_eq!(md["fromProvider"], json!("auggie"));
        assert_eq!(md["toProvider"], json!("claude-code"));
        assert!(notice.content[0]["text"]
            .as_str()
            .unwrap()
            .contains("Model changed"));

        // The persist emitted `agent:message` with the row id + system role.
        let batch = timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("recv")
            .expect("open");
        let event = batch
            .iter()
            .find(|e| e.event_type == AGENT_MESSAGE)
            .expect("agent:message event");
        assert_eq!(event.data["agentId"], json!(id.0));
        assert_eq!(event.data["role"], json!("system"));
        assert_eq!(event.data["messageId"], json!(notice.id));

        // Turn 3 under the unchanged identity: no second notice.
        mgr.maybe_persist_model_change_notice(&id, &ws, &resolved("claude-code", Some("sonnet")))
            .await;
        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1, "unchanged turn persists nothing");
    }

    /// Deferred commit: `agent.setModel` toggles that are reverted before any
    /// message never produce a notice — the next turn's identity equals the
    /// last committed one, and nothing was written in between.
    #[tokio::test]
    async fn switch_and_revert_before_send_produces_no_notice() {
        let (_tmp, mgr) = manager().await;
        let (ws, id) = (WorkspaceId::from("ws-mc3"), AgentId::from("a-mc3"));
        seed_agent(&mgr, &ws, &id).await;

        mgr.maybe_persist_model_change_notice(&id, &ws, &resolved("auggie", Some("gpt-5")))
            .await;
        // (setModel A→B→A happened between turns; nothing committed.)
        mgr.maybe_persist_model_change_notice(&id, &ws, &resolved("auggie", Some("gpt-5")))
            .await;

        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .unwrap();
        assert!(
            messages.is_empty(),
            "reverted toggle must not produce a notice"
        );
    }

    /// Default-model (None) ↔ explicit-model transitions are real switches:
    /// the `from`/`to` metadata carries `null` for the provider default.
    #[tokio::test]
    async fn default_model_transition_is_a_switch_with_null_metadata() {
        let (_tmp, mgr) = manager().await;
        let (ws, id) = (WorkspaceId::from("ws-mc4"), AgentId::from("a-mc4"));
        seed_agent(&mgr, &ws, &id).await;

        mgr.maybe_persist_model_change_notice(&id, &ws, &resolved("auggie", None))
            .await;
        mgr.maybe_persist_model_change_notice(&id, &ws, &resolved("auggie", Some("gpt-5")))
            .await;

        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);
        let md = messages[0].metadata.as_ref().unwrap();
        assert_eq!(md["from"], json!(null));
        assert_eq!(md["to"], json!("gpt-5"));
    }

    /// The recreate-replay body must exclude BOTH the current user message and
    /// the turn-start notice that trails it: `build_turn_body` truncates at
    /// the last user row, so a `model_changed` system row appended after the
    /// current user persist never reaches the provider prompt.
    #[tokio::test]
    async fn build_turn_body_excludes_trailing_notice_and_current_message() {
        let (_tmp, mgr) = manager().await;
        let (ws, id) = (WorkspaceId::from("ws-mc5"), AgentId::from("a-mc5"));
        seed_agent(&mgr, &ws, &id).await;
        let store = &mgr.services.store;
        for (role, text) in [
            ("user", "first ask"),
            ("assistant", "first answer"),
            ("user", "current ask"),
        ] {
            store
                .append_agent_message(
                    &id,
                    role,
                    &json!([{ "type": "text", "text": text }]),
                    &now_iso(),
                )
                .await
                .unwrap();
        }
        // The turn-start notice lands AFTER the current user row.
        mgr.services
            .store
            .set_agent_session_last_turn_model(&ws, &id, Some("gpt-5"), "auggie")
            .await
            .unwrap();
        mgr.maybe_persist_model_change_notice(&id, &ws, &resolved("claude-code", Some("sonnet")))
            .await;
        mgr.recreated.lock().unwrap().insert(id.clone());

        let body = mgr.build_turn_body(&id, "current ask").await;

        assert!(body.contains("first ask") && body.contains("first answer"));
        assert!(
            !body.contains("Model changed"),
            "notice must not reach the provider prompt"
        );
        // The current message appears once as the live content, never in the
        // replayed history.
        assert_eq!(body.matches("current ask").count(), 1);
    }
}

/// Question hold (PROTOCOL §5.5): the runtime delivery/drain gates. Uses the
/// manager without spawning provider turns — an active hold short-circuits
/// BEFORE `try_begin`, so no provider is needed for the held paths.
mod question_hold_gates {
    use super::*;
    use crate::agent_manager::TurnOptions;
    use intent_core::MessageOrigin;

    /// The same trailing-question-block shape as the `agent_ops` tests.
    fn question_blocks() -> Value {
        json!([
            { "type": "text", "text": "I have a clarifying question." },
            {
                "type": "resource",
                "resource": {
                    "uri": "intent-question://q-1",
                    "name": "Scope",
                    "mimeType": "application/vnd.intent.question+json",
                    "text": "{\"question\":\"Which scope?\"}"
                }
            }
        ])
    }

    /// Appends the trailing question-block assistant row AND persists the
    /// pending-questions marker for it (the stored-on-write contract the
    /// turn-end persist follows), returning its message id — for
    /// `agent_dismiss_questions_op` calls and answer-metadata tags.
    async fn arm_hold(mgr: &AgentManager, ws: &WorkspaceId, id: &AgentId) -> String {
        let asked = mgr
            .services
            .store
            .append_agent_message(id, "assistant", &question_blocks(), &now_iso())
            .await
            .expect("append question");
        mgr.services
            .record_pending_questions_marker(ws, id, &asked.id)
            .await;
        assert!(mgr.services.question_hold_active(id).await);
        asked.id
    }

    /// The `messageMetadata` tag the wizard's answer carries — the only user
    /// row shape that resolves a pending Q&A (spec §Decisions 3).
    fn answer_metadata(asked_id: &str) -> Value {
        json!({
            "type": "question_answers",
            "answeredQuestionsMessageId": asked_id,
        })
    }

    /// Automatic `send_message` parks in the queue with `heldForQuestions`
    /// and never claims the in-flight slot; a User send passes through the
    /// hold gate — and since monorepo#1791 a user send arriving while the
    /// hold has PARKED entries converts to a user-origin enqueue + drain
    /// kick, so the parked automatic backlog rides the user-led combined
    /// flush turn FIFO instead of being bypassed. A plain user send still
    /// does not RELEASE the hold (no answer tag): later automatic sends
    /// keep parking until the answer-tagged send clears the marker.
    #[tokio::test]
    async fn automatic_send_held_user_send_not() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let (_tmp, mgr, _bus) = manager_with_bus().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (WorkspaceId::from("ws-qh-send"), AgentId::from("a-qh-send"));
        seed_agent(&mgr, &ws, &id).await;
        let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
        session.provider = Some("mock".to_string());
        mgr.services
            .store
            .update_agent_session(&ws, &session)
            .await
            .expect("set mock provider");
        let asked = arm_hold(&mgr, &ws, &id).await;

        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                "auto wake".to_string(),
                None,
                TurnOptions::default(),
            )
            .await
            .expect("held send");
        assert_eq!(r["queued"], json!(true));
        assert_eq!(r["heldForQuestions"], json!(true));
        assert!(!mgr.is_busy(&id), "held send never claims the slot");
        assert_eq!(mgr.services.queue_snapshot(&id).len(), 1);

        // Hold still active (no user row was appended).
        assert!(mgr.services.question_hold_active(&id).await);

        // A PLAIN user-origin send is NOT held — but with a parked backlog
        // it converts to a user-origin enqueue + drain kick (monorepo#1791):
        // the batch flush delivers the parked automatic entry FIFO in the
        // SAME combined turn as the user message instead of bypassing it.
        // No answer tag rode along, so the pending-questions marker
        // survives the combined turn.
        let plain = TurnOptions {
            origin: MessageOrigin::User,
            ..TurnOptions::default()
        };
        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                "unrelated aside".to_string(),
                None,
                plain,
            )
            .await
            .expect("plain user send");
        assert_eq!(
            r.get("heldForQuestions"),
            None,
            "user sends bypass the hold gate"
        );
        assert_eq!(
            r["queued"],
            json!(true),
            "user send with a parked backlog converts to enqueue + flush"
        );
        timeout(Duration::from_secs(10), async {
            loop {
                if !mgr.is_busy(&id)
                    && mgr.workers.lock().unwrap().is_empty()
                    && !mgr.services.has_ready_to_send(&id)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("combined flush turn completes");
        assert!(
            mgr.services.question_hold_active(&id).await,
            "a plain user message does not resolve the pending Q&A"
        );
        assert!(
            mgr.services.queue_snapshot(&id).is_empty(),
            "the parked automatic entry rode the user-led flush turn"
        );
        // FIFO: the older parked wake's user row precedes the newer user
        // message's row in the transcript.
        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .expect("messages");
        let row_idx = |needle: &str| {
            messages.iter().position(|m| {
                m.role == "user"
                    && m.content.as_array().is_some_and(|blocks| {
                        blocks
                            .iter()
                            .any(|b| b["text"].as_str().is_some_and(|t| t.contains(needle)))
                    })
            })
        };
        let wake_idx = row_idx("auto wake").expect("parked wake row landed");
        let aside_idx = row_idx("unrelated aside").expect("user row landed");
        assert!(
            wake_idx < aside_idx,
            "older parked wake drains FIFO ahead of the newer user message: \
             wake={wake_idx} aside={aside_idx}"
        );

        // A LATER automatic send still parks — the hold is armed until the
        // answer or dismissal.
        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                "auto wake 2".to_string(),
                None,
                TurnOptions::default(),
            )
            .await
            .expect("second held send");
        assert_eq!(r["heldForQuestions"], json!(true));
        assert_eq!(mgr.services.queue_snapshot(&id).len(), 1);

        // The ANSWER releases the hold; with a parked backlog it converts to
        // the same enqueue + flush, draining the parked wake alongside it.
        let answer = TurnOptions {
            origin: MessageOrigin::User,
            message_metadata: Some(answer_metadata(&asked)),
            ..TurnOptions::default()
        };
        mgr.clone()
            .send_message(id.clone(), ws.clone(), "answer".to_string(), None, answer)
            .await
            .expect("answer send");
        timeout(Duration::from_secs(10), async {
            loop {
                if !mgr.is_busy(&id)
                    && mgr.workers.lock().unwrap().is_empty()
                    && !mgr.services.has_ready_to_send(&id)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("answer turn + released drain complete");
        assert!(
            !mgr.services.question_hold_active(&id).await,
            "hold cleared"
        );
    }

    /// monorepo#1791 regression: an automatic `after_all` settlement wake
    /// parked by the hold on an IDLE agent must not be bypassed by a newer
    /// plain user message — the user send converts to a user-origin enqueue
    /// and the flush delivers the wake FIFO in the same combined turn.
    #[tokio::test]
    async fn parked_settlement_wake_drains_with_newer_user_message() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let (_tmp, mgr, _bus) = manager_with_bus().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (WorkspaceId::from("ws-qh-1791"), AgentId::from("a-qh-1791"));
        seed_agent(&mgr, &ws, &id).await;
        let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
        session.provider = Some("mock".to_string());
        mgr.services
            .store
            .update_agent_session(&ws, &session)
            .await
            .expect("set mock provider");
        arm_hold(&mgr, &ws, &id).await;

        // The settlement wake (automatic) parks at position 0 on the idle
        // agent — the monorepo#1791 incident shape.
        let wake = "[WORKSPACE EVENTS] All 3 delegated child agent(s) settled";
        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                wake.to_string(),
                None,
                TurnOptions::default(),
            )
            .await
            .expect("wake send");
        assert_eq!(r["heldForQuestions"], json!(true));

        // The user's later "check" must NOT run past the parked wake.
        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                "check".to_string(),
                None,
                TurnOptions {
                    origin: MessageOrigin::User,
                    ..TurnOptions::default()
                },
            )
            .await
            .expect("check send");
        assert_eq!(r["queued"], json!(true), "converted to enqueue + flush");
        timeout(Duration::from_secs(10), async {
            loop {
                if !mgr.is_busy(&id)
                    && mgr.workers.lock().unwrap().is_empty()
                    && !mgr.services.has_ready_to_send(&id)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("combined turn completes");
        assert!(
            mgr.services.queue_snapshot(&id).is_empty(),
            "settlement wake no longer stuck in the queue"
        );
        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .expect("messages");
        let row_idx = |needle: &str| {
            messages.iter().position(|m| {
                m.role == "user"
                    && m.content.as_array().is_some_and(|blocks| {
                        blocks
                            .iter()
                            .any(|b| b["text"].as_str().is_some_and(|t| t.contains(needle)))
                    })
            })
        };
        let wake_idx = row_idx("delegated child agent(s) settled").expect("wake row landed");
        let check_idx = row_idx("check").expect("check row landed");
        assert!(
            wake_idx < check_idx,
            "settlement wake delivered FIFO ahead of the newer user message: \
             wake={wake_idx} check={check_idx}"
        );
    }

    /// The monorepo#1791 conversion requires the `all` flush mode: without
    /// batching no combined turn exists to carry the parked entries, so a
    /// user send under hold stays a DIRECT send (documented bypass) and the
    /// parked automatic entry stays parked — the pre-fix contract, pinned
    /// here for the non-default `off` mode.
    #[tokio::test]
    async fn user_send_under_hold_stays_direct_when_flush_mode_off() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let bus = EventBus::new(store.clone());
        let config_dir = tempfile::tempdir().expect("temp config dir");
        let registry = Arc::new(
            crate::SettingsRegistry::load(config_dir.path().join("config.toml"))
                .expect("load registry"),
        );
        registry
            .apply(&[("agents.flushQueuedMessages".to_string(), json!("off"))])
            .expect("disable flush");
        let services = Services::new(store)
            .with_event_bus(bus.clone())
            .with_settings_registry(registry);
        let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
        let mgr = Arc::new(AgentManager::new(services, sink, 8));
        let (ws, id) = (
            WorkspaceId::from("ws-qh-1791-off"),
            AgentId::from("a-qh-1791-off"),
        );
        seed_agent(&mgr, &ws, &id).await;
        let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
        session.provider = Some("mock".to_string());
        mgr.services
            .store
            .update_agent_session(&ws, &session)
            .await
            .expect("set mock provider");
        arm_hold(&mgr, &ws, &id).await;

        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                "auto wake".to_string(),
                None,
                TurnOptions::default(),
            )
            .await
            .expect("wake send");
        assert_eq!(r["heldForQuestions"], json!(true));

        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                "check".to_string(),
                None,
                TurnOptions {
                    origin: MessageOrigin::User,
                    ..TurnOptions::default()
                },
            )
            .await
            .expect("check send");
        assert_eq!(
            r["queued"],
            json!(false),
            "no combined turn exists in `off` mode — the direct send stands"
        );
        timeout(Duration::from_secs(10), async {
            loop {
                if !mgr.is_busy(&id) && mgr.workers.lock().unwrap().is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("direct turn completes");
        let snapshot = mgr.services.queue_snapshot(&id);
        assert_eq!(
            snapshot.len(),
            1,
            "the automatic entry stays parked under the hold (no batch to ride)"
        );
        assert_eq!(snapshot[0]["content"], json!("auto wake"));
    }

    /// `try_drain_queue` refuses to drain while the hold is active, and
    /// drains normally once the questions are dismissed.
    #[tokio::test]
    async fn drain_gated_until_dismiss() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let (_tmp, mgr, _bus) = manager_with_bus().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (
            WorkspaceId::from("ws-qh-drain"),
            AgentId::from("a-qh-drain"),
        );
        seed_agent(&mgr, &ws, &id).await;
        let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
        session.provider = Some("mock".to_string());
        mgr.services
            .store
            .update_agent_session(&ws, &session)
            .await
            .expect("set mock provider");

        mgr.services
            .enqueue_message(&id, "parked".to_string(), None, None, None, None, false);
        let asked = arm_hold(&mgr, &ws, &id).await;

        mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;
        assert!(!mgr.is_busy(&id), "hold blocks the drain");
        assert_eq!(
            mgr.services.queue_snapshot(&id).len(),
            1,
            "entry stays parked"
        );

        // A later question-FREE assistant turn does not release the hold —
        // the entry is still parked (pendingness survives the agent's own
        // turns until answered or dismissed).
        mgr.services
            .store
            .append_agent_message(
                &id,
                "assistant",
                &json!([{ "type": "text", "text": "still thinking" }]),
                &now_iso(),
            )
            .await
            .expect("append question-free tail");
        assert!(mgr.services.question_hold_active(&id).await);
        mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;
        assert!(!mgr.is_busy(&id), "hold survives a question-free turn");
        assert_eq!(mgr.services.queue_snapshot(&id).len(), 1);

        // Dismiss → drain proceeds (mirrors the RPC's dismiss-then-kick).
        mgr.services
            .agent_dismiss_questions_op(ws.clone(), id.clone(), asked.clone())
            .await
            .expect("dismiss");
        mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;
        timeout(Duration::from_secs(10), async {
            loop {
                if !mgr.is_busy(&id)
                    && mgr.workers.lock().unwrap().is_empty()
                    && !mgr.services.has_ready_to_send(&id)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("dismissed queue drains");
    }

    /// Automatic interrupt priority: held like any automatic delivery, and
    /// parked at the FRONT of the queue with the persisted marker.
    #[tokio::test]
    async fn automatic_interrupt_held_front_of_queue() {
        let (_tmp, mgr, _bus) = manager_with_bus().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (WorkspaceId::from("ws-qh-int"), AgentId::from("a-qh-int"));
        seed_agent(&mgr, &ws, &id).await;
        arm_hold(&mgr, &ws, &id).await;

        mgr.services.enqueue_message(
            &id,
            "earlier normal".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        let r = mgr
            .interrupt_send_message(
                id.clone(),
                ws.clone(),
                "urgent".to_string(),
                Some("m-int-1".to_string()),
                TurnOptions::default(),
            )
            .await
            .expect("held interrupt");
        assert_eq!(r["queued"], json!(true));
        assert_eq!(r["heldForQuestions"], json!(true));
        assert!(!mgr.is_busy(&id), "held interrupt never preempts/claims");

        let snapshot = mgr.services.queue_snapshot(&id);
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0]["content"], json!("urgent"));
        assert_eq!(snapshot[0]["interruptPriority"], json!(true));
        assert_eq!(snapshot[1]["content"], json!("earlier normal"));
    }

    /// PR review regression: held interrupts still record the dedup marker.
    /// A duplicate `message_id` arriving while the hold is active must be
    /// deduplicated (not double-enqueued), and a replay of that same id
    /// arriving after the hold releases must also be deduplicated — the
    /// held interrupt keeps the same at-most-once contract as one that
    /// streamed immediately.
    #[tokio::test]
    async fn held_interrupt_records_dedup_marker() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let (_tmp, mgr, _bus) = manager_with_bus().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (
            WorkspaceId::from("ws-qh-int-dedup"),
            AgentId::from("a-qh-int-dedup"),
        );
        seed_agent(&mgr, &ws, &id).await;
        let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
        session.provider = Some("mock".to_string());
        mgr.services
            .store
            .update_agent_session(&ws, &session)
            .await
            .expect("set mock provider");
        let asked = arm_hold(&mgr, &ws, &id).await;

        let r1 = mgr
            .interrupt_send_message(
                id.clone(),
                ws.clone(),
                "urgent".to_string(),
                Some("m-dedup-1".to_string()),
                TurnOptions::default(),
            )
            .await
            .expect("first held interrupt");
        assert_eq!(r1["queued"], json!(true));
        assert_eq!(r1["heldForQuestions"], json!(true));

        // A duplicate with the SAME message_id while still held must be
        // deduplicated, not enqueued a second time.
        let r2 = mgr
            .interrupt_send_message(
                id.clone(),
                ws.clone(),
                "urgent (duplicate)".to_string(),
                Some("m-dedup-1".to_string()),
                TurnOptions::default(),
            )
            .await
            .expect("duplicate held interrupt");
        assert_eq!(r2["deduplicated"], json!(true));
        assert_eq!(
            mgr.services.queue_snapshot(&id).len(),
            1,
            "no double-enqueue"
        );

        // Dismiss releases the hold; a replay of the SAME id must still be
        // deduplicated (the marker survived the hold window).
        mgr.services
            .agent_dismiss_questions_op(ws.clone(), id.clone(), asked)
            .await
            .expect("dismiss");
        let r3 = mgr
            .interrupt_send_message(
                id.clone(),
                ws.clone(),
                "urgent (replay)".to_string(),
                Some("m-dedup-1".to_string()),
                TurnOptions::default(),
            )
            .await
            .expect("post-release replay");
        assert_eq!(
            r3["deduplicated"],
            json!(true),
            "replay after release is still deduplicated"
        );
    }

    /// PR review regression: the hold-check → enqueue race against a
    /// concurrent `dismissQuestions`. Simulated by dismissing the questions
    /// AFTER `question_hold_active` would have observed `true` but the
    /// message is enqueued with the hold already cleared by the time the
    /// re-check inside `send_message` runs — the re-check must self-heal by
    /// kicking the drain instead of stranding the entry.
    #[tokio::test]
    async fn held_send_self_heals_when_dismissed_during_enqueue() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let (_tmp, mgr, _bus) = manager_with_bus().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (
            WorkspaceId::from("ws-qh-race2"),
            AgentId::from("a-qh-race2"),
        );
        seed_agent(&mgr, &ws, &id).await;
        let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
        session.provider = Some("mock".to_string());
        mgr.services
            .store
            .update_agent_session(&ws, &session)
            .await
            .expect("set mock provider");
        let asked = arm_hold(&mgr, &ws, &id).await;

        // Dismiss BEFORE the send: `question_hold_active` inside
        // `send_message` now observes `false`, so this exercises the
        // "already resolved" side, not the raw hold gate — the real value of
        // this test is asserting `try_drain_queue`'s own re-derivation is
        // safe to call twice in a row (once from the RPC's dismiss kick,
        // once from send's own post-enqueue re-check) without duplicating
        // delivery.
        mgr.services
            .agent_dismiss_questions_op(ws.clone(), id.clone(), asked)
            .await
            .expect("dismiss");

        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                "auto wake".to_string(),
                None,
                TurnOptions::default(),
            )
            .await
            .expect("send after dismiss");
        assert_eq!(
            r.get("heldForQuestions"),
            None,
            "hold already cleared before send"
        );
        timeout(Duration::from_secs(10), async {
            loop {
                if !mgr.is_busy(&id)
                    && mgr.workers.lock().unwrap().is_empty()
                    && !mgr.services.has_ready_to_send(&id)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("message delivered, no stranding");
    }

    /// Race regression (the WSS Q&A e2e flake): a USER answer that lands
    /// while the asking turn's worker still holds the in-flight slot is
    /// parked by the busy race — the worker's end-of-turn drain then sees
    /// the hold active (its own turn asked the questions). The parked
    /// user-origin entry must drain anyway (it IS the hold release);
    /// without the user-origin bypass the answer deadlocks: the hold waits
    /// for a user row while the user row waits in the queue.
    #[tokio::test]
    async fn user_send_parked_by_busy_race_drains_despite_hold() {
        let (_tmp, mgr, _bus) = manager_with_bus().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (WorkspaceId::from("ws-qh-race"), AgentId::from("a-qh-race"));
        seed_agent(&mgr, &ws, &id).await;
        arm_hold(&mgr, &ws, &id).await;

        // The user answer lost the busy race against the asking turn and
        // parked with the user-origin marker (send_message's busy branch).
        let opts = TurnOptions {
            origin: MessageOrigin::User,
            ..TurnOptions::default()
        };
        mgr.services.enqueue_message_with_origin(
            &id,
            "Q: Which scope?\nA: workspace".to_string(),
            None,
            None,
            None,
            opts.queued_prepend(),
            opts.interrupt_priority,
            opts.origin.is_user(),
        );
        // An automatic wake parked ahead of it must stay held.
        mgr.services.enqueue_message(
            &id,
            "auto report".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        assert!(mgr.services.question_hold_active(&id).await);
        assert!(mgr.services.has_user_origin_ready(&id));

        // The drain (kicked at the asking worker's turn end) proceeds for
        // the user entry ONLY: pops it despite the hold, leaves the
        // automatic entry parked.
        let popped = mgr
            .services
            .dequeue_user_origin_message(&id)
            .expect("user-origin entry drains under the hold");
        assert_eq!(popped.content, "Q: Which scope?\nA: workspace");
        assert!(popped.user_origin);
        let snapshot = mgr.services.queue_snapshot(&id);
        assert_eq!(snapshot.len(), 1, "automatic entry stays parked");
        assert_eq!(snapshot[0]["content"], json!("auto report"));

        // With no user entry left, the hold gate suspends the drain again.
        assert!(!mgr.services.has_user_origin_ready(&id));
        mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;
        assert!(!mgr.is_busy(&id), "hold still blocks automatic entries");
        assert_eq!(mgr.services.queue_snapshot(&id).len(), 1);
    }

    /// Answer-driven release (the persistent-pendingness contract): a parked
    /// automatic entry survives a plain user turn and only drains once a user
    /// row tagged `question_answers` for the marked message clears the
    /// pending-questions marker.
    #[tokio::test]
    async fn drain_gated_until_answer_metadata() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let (_tmp, mgr, _bus) = manager_with_bus().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (
            WorkspaceId::from("ws-qh-answer"),
            AgentId::from("a-qh-answer"),
        );
        seed_agent(&mgr, &ws, &id).await;
        let mut session = mgr.services.store.get_agent_session(&id).await.unwrap();
        session.provider = Some("mock".to_string());
        mgr.services
            .store
            .update_agent_session(&ws, &session)
            .await
            .expect("set mock provider");

        mgr.services
            .enqueue_message(&id, "parked".to_string(), None, None, None, None, false);
        let asked = arm_hold(&mgr, &ws, &id).await;

        // A FOREIGN answer tag (naming a message the marker does not point
        // at) is a no-op: the hold stays armed and the entry stays parked.
        mgr.services
            .resolve_pending_questions_for_answer(&ws, &id, Some(&answer_metadata("other-msg")))
            .await;
        assert!(mgr.services.question_hold_active(&id).await);
        mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;
        assert!(!mgr.is_busy(&id), "stale answer never releases the hold");
        assert_eq!(mgr.services.queue_snapshot(&id).len(), 1);

        // The matching answer clears the marker, and the drain proceeds.
        assert!(
            mgr.services
                .resolve_pending_questions_for_answer(&ws, &id, Some(&answer_metadata(&asked)))
                .await,
            "matching answer clears the marker"
        );
        assert!(!mgr.services.question_hold_active(&id).await);
        mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;
        timeout(Duration::from_secs(10), async {
            loop {
                if !mgr.is_busy(&id)
                    && mgr.workers.lock().unwrap().is_empty()
                    && !mgr.services.has_ready_to_send(&id)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("answered queue drains");
    }
}

/// Combined flush of parked archive notices on auto-unarchive
/// (intent-hq/intent#3883): a USER send into an ARCHIVED workspace whose
/// queue holds parked ready-to-send entries (hook/PR-monitor archive
/// cancellation wakes) converts to a user-origin enqueue + drain kick, so
/// the batch flush delivers the parked entries FIFO in ONE combined turn
/// with the user message — and the drain's `try_begin` performs the
/// auto-unarchive, so the same turn carries the one-shot prompt notice.
mod archived_flush_gates {
    use super::*;
    use crate::agent_manager::TurnOptions;
    use intent_core::MessageOrigin;

    async fn seed_mock_agent(mgr: &AgentManager, ws: &WorkspaceId, id: &AgentId) {
        seed_agent(mgr, ws, id).await;
        set_session_provider(mgr, ws, id, "mock").await;
    }

    async fn archive_row(mgr: &AgentManager, ws: &WorkspaceId) {
        let mut row = mgr.services.store.get_workspace(ws).await.unwrap();
        row.status = WorkspaceStatus::Archived;
        row.archived = true;
        row.archived_at = Some(now_iso());
        mgr.services
            .store
            .update_workspace(&row)
            .await
            .expect("archive row");
    }

    async fn await_settled(mgr: &Arc<AgentManager>, id: &AgentId) {
        timeout(Duration::from_secs(15), async {
            loop {
                if !mgr.is_busy(id)
                    && mgr.workers.lock().unwrap().is_empty()
                    && !mgr.services.has_ready_to_send(id)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("agent settles");
    }

    fn read_prompt_log(path: &std::path::Path) -> Vec<String> {
        std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .map(|l| {
                serde_json::from_str::<Value>(l).expect("prompt log line")["text"]
                    .as_str()
                    .expect("text")
                    .to_string()
            })
            .collect()
    }

    /// Regression (intent-hq/intent#3883): an archive-cancellation wake
    /// parked behind the archived gate must ride the SAME combined turn as
    /// the user message that auto-unarchives the workspace — wake row FIRST,
    /// user message after, the `auto_unarchived` system row persisted, and
    /// ONE outbound prompt ending with the one-shot unarchive notice.
    #[tokio::test]
    async fn parked_archive_wake_rides_the_unarchiving_user_turn() {
        let script = mock_agent_script();
        let prompt_log =
            std::env::temp_dir().join(format!("itd-ua-flush-{}.log", uuid::Uuid::new_v4()));
        let prompt_log_s = prompt_log.to_string_lossy().into_owned();
        let _env = EnvGuard::set_all(&[
            ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
            ("MOCK_AGENT_PROMPT_LOG", prompt_log_s.as_str()),
        ]);
        let (_tmp, mgr) = manager().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (
            WorkspaceId::from("ws-ua-flush"),
            AgentId::from("a-ua-flush"),
        );
        seed_mock_agent(&mgr, &ws, &id).await;
        archive_row(&mgr, &ws).await;

        // The hook-cancellation wake (automatic) parks behind the archived
        // gate on the idle agent.
        let wake = "[Background hook \"ci-watch\"] cancelled: workspace archived";
        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                wake.to_string(),
                None,
                TurnOptions::default(),
            )
            .await
            .expect("wake send");
        assert_eq!(r["archivedParked"], json!(true), "wake parks: {r}");

        // The user's later message converts to an enqueue + flush instead
        // of a direct turn that would bypass the parked wake.
        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                "back to work".to_string(),
                None,
                TurnOptions {
                    origin: MessageOrigin::User,
                    ..TurnOptions::default()
                },
            )
            .await
            .expect("user send");
        assert_eq!(
            r["queued"],
            json!(true),
            "converted to enqueue + flush: {r}"
        );
        await_settled(&mgr, &id).await;

        assert!(
            mgr.services.queue_snapshot(&id).is_empty(),
            "no parked leftovers"
        );
        let after = mgr.services.store.get_workspace(&ws).await.unwrap();
        assert!(!after.archived, "the drain's claim auto-unarchived");

        // Transcript order: wake row BEFORE the user message, and the
        // `auto_unarchived` system row persisted by the same claim.
        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .expect("messages");
        let row_idx = |needle: &str| {
            messages.iter().position(|m| {
                m.role == "user"
                    && m.content.as_array().is_some_and(|blocks| {
                        blocks
                            .iter()
                            .any(|b| b["text"].as_str().is_some_and(|t| t.contains(needle)))
                    })
            })
        };
        let wake_idx = row_idx("cancelled: workspace archived").expect("wake row landed");
        let user_idx = row_idx("back to work").expect("user row landed");
        assert!(
            wake_idx < user_idx,
            "parked wake delivered FIFO ahead of the user message: \
             wake={wake_idx} user={user_idx}"
        );
        assert!(
            messages.iter().any(|m| m.role == "system"
                && m.metadata
                    == Some(json!({ "type": "auto_unarchived", "reason": "agent_activity" }))),
            "the auto_unarchived system row persisted"
        );

        // ONE combined provider turn whose prompt carries the wake, the
        // user message, and the trailing one-shot unarchive notice.
        let prompts = read_prompt_log(&prompt_log);
        let _ = std::fs::remove_file(&prompt_log);
        assert_eq!(prompts.len(), 1, "one combined turn: {prompts:?}");
        let text = &prompts[0];
        let w = text
            .find("cancelled: workspace archived")
            .expect("wake in prompt");
        let u = text.find("back to work").expect("user msg in prompt");
        assert!(w < u, "prompt order wake → user: {text}");
        assert!(
            text.ends_with(super::super::AUTO_UNARCHIVE_PROMPT_NOTICE),
            "the combined prompt ends with the unarchive notice: {text}"
        );
    }

    /// EMPTY queue: a user send to an archived workspace keeps today's
    /// direct-turn path byte-for-byte (no queue hop).
    #[tokio::test]
    async fn user_send_with_empty_queue_stays_direct() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let (_tmp, mgr) = manager().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (
            WorkspaceId::from("ws-ua-empty"),
            AgentId::from("a-ua-empty"),
        );
        seed_mock_agent(&mgr, &ws, &id).await;
        archive_row(&mgr, &ws).await;

        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                "revive".to_string(),
                None,
                TurnOptions {
                    origin: MessageOrigin::User,
                    ..TurnOptions::default()
                },
            )
            .await
            .expect("user send");
        assert_eq!(r["queued"], json!(false), "direct turn, no queue hop: {r}");
        await_settled(&mgr, &id).await;
        let after = mgr.services.store.get_workspace(&ws).await.unwrap();
        assert!(!after.archived, "direct claim auto-unarchived");
    }

    /// The conversion requires the `all` flush mode: without batching no
    /// combined turn exists to carry the parked entries, so the user send
    /// stays a DIRECT send under `off` — the pre-fix contract.
    #[tokio::test]
    async fn user_send_stays_direct_when_flush_mode_off() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let bus = EventBus::new(store.clone());
        let config_dir = tempfile::tempdir().expect("temp config dir");
        let registry = Arc::new(
            crate::SettingsRegistry::load(config_dir.path().join("config.toml"))
                .expect("load registry"),
        );
        registry
            .apply(&[("agents.flushQueuedMessages".to_string(), json!("off"))])
            .expect("disable flush");
        let services = Services::new(store)
            .with_event_bus(bus.clone())
            .with_settings_registry(registry);
        let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
        let mgr = Arc::new(AgentManager::new(services, sink, 8));
        let (ws, id) = (WorkspaceId::from("ws-ua-off"), AgentId::from("a-ua-off"));
        seed_mock_agent(&mgr, &ws, &id).await;
        archive_row(&mgr, &ws).await;

        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                "auto wake".to_string(),
                None,
                TurnOptions::default(),
            )
            .await
            .expect("wake send");
        assert_eq!(r["archivedParked"], json!(true));

        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                "revive".to_string(),
                None,
                TurnOptions {
                    origin: MessageOrigin::User,
                    ..TurnOptions::default()
                },
            )
            .await
            .expect("user send");
        assert_eq!(
            r["queued"],
            json!(false),
            "no combined turn exists in `off` mode — the direct send stands: {r}"
        );
        await_settled(&mgr, &id).await;
    }

    /// A session parked in Error keeps the documented direct fresh-send
    /// recovery path — no conversion (the STAB-52 gate in `try_drain_queue`
    /// would strand a converted entry).
    #[tokio::test]
    async fn user_send_to_error_session_stays_direct() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let (_tmp, mgr) = manager().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (
            WorkspaceId::from("ws-ua-error"),
            AgentId::from("a-ua-error"),
        );
        seed_mock_agent(&mgr, &ws, &id).await;
        archive_row(&mgr, &ws).await;

        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                "auto wake".to_string(),
                None,
                TurnOptions::default(),
            )
            .await
            .expect("wake send");
        assert_eq!(r["archivedParked"], json!(true));

        mgr.services
            .store
            .set_agent_session_status(&ws, &id, AgentStatus::Error, false, &now_iso(), None)
            .await
            .expect("park session in Error");

        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                "recover".to_string(),
                None,
                TurnOptions {
                    origin: MessageOrigin::User,
                    ..TurnOptions::default()
                },
            )
            .await
            .expect("user send");
        assert_eq!(
            r["queued"],
            json!(false),
            "Error session keeps the direct fresh-send recovery: {r}"
        );
        await_settled(&mgr, &id).await;
    }

    /// `try_drain_queue`'s archived gate: a parked USER-origin entry exempts
    /// the drain (the user message is the explicit resurrection signal) —
    /// the claim auto-unarchives and the batch carries every parked entry
    /// FIFO; with NO user-origin entry ready the gate still parks everything
    /// (no regression on the archive/auto-unarchive loop, intentd#1293).
    #[tokio::test]
    async fn drain_archived_gate_exempts_user_origin_entries() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let (_tmp, mgr) = manager().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (
            WorkspaceId::from("ws-ua-drain"),
            AgentId::from("a-ua-drain"),
        );
        seed_mock_agent(&mgr, &ws, &id).await;
        archive_row(&mgr, &ws).await;

        // Automatic entries alone stay parked.
        mgr.services
            .enqueue_message(&id, "auto wake".to_string(), None, None, None, None, false);
        mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;
        assert!(!mgr.is_busy(&id), "automatic-only queue stays parked");
        assert_eq!(mgr.services.queue_snapshot(&id).len(), 1);
        let row = mgr.services.store.get_workspace(&ws).await.unwrap();
        assert!(row.archived, "no auto-unarchive without a user entry");

        // A user-origin entry queued INTO the archived workspace (at or
        // after `archivedAt`) exempts the gate: the next kick drains
        // everything and unarchives.
        mgr.services.enqueue_message_with_origin(
            &id,
            "user follow-up".to_string(),
            None,
            None,
            None,
            None,
            false,
            true,
        );
        mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;
        await_settled(&mgr, &id).await;
        assert!(
            mgr.services.queue_snapshot(&id).is_empty(),
            "both entries drained"
        );
        let after = mgr.services.store.get_workspace(&ws).await.unwrap();
        assert!(!after.archived, "the drain's claim auto-unarchived");
    }

    /// Regression (PR #1587 review): a user-origin entry parked by a busy
    /// race BEFORE archival is not a post-archive user action, so it must
    /// NOT release the archived gate — in the busy-user → archive flow the
    /// interrupted worker's end-of-turn re-kick would otherwise find that
    /// older entry and immediately auto-unarchive the freshly archived
    /// workspace. Only a user send made at or after `archivedAt` resurrects.
    #[tokio::test]
    async fn pre_archival_user_entry_does_not_release_the_gate() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let (_tmp, mgr) = manager().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (WorkspaceId::from("ws-ua-pre"), AgentId::from("a-ua-pre"));
        seed_mock_agent(&mgr, &ws, &id).await;

        // A user message parked by the busy race BEFORE the archive.
        mgr.services.enqueue_message_with_origin(
            &id,
            "pre-archival user leftover".to_string(),
            None,
            None,
            None,
            None,
            false,
            true,
        );
        // Ensure `queuedAt` strictly precedes `archivedAt` even at coarse
        // clock resolution.
        tokio::time::sleep(Duration::from_millis(5)).await;
        archive_row(&mgr, &ws).await;

        // The end-of-turn / unarchive-window re-kick path: the drain must
        // stay parked despite the ready user-origin entry.
        mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;
        assert!(!mgr.is_busy(&id), "pre-archival user entry stays parked");
        assert_eq!(mgr.services.queue_snapshot(&id).len(), 1);
        let row = mgr.services.store.get_workspace(&ws).await.unwrap();
        assert!(
            row.archived,
            "no auto-unarchive without a post-archive user send"
        );

        // A fresh user send INTO the archived workspace releases the park
        // and carries the older leftover FIFO in the same combined turn.
        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                "please resume".to_string(),
                None,
                TurnOptions {
                    origin: MessageOrigin::User,
                    ..TurnOptions::default()
                },
            )
            .await
            .expect("user send");
        assert_eq!(r["queued"], json!(true), "send converts to enqueue: {r}");
        await_settled(&mgr, &id).await;
        let after = mgr.services.store.get_workspace(&ws).await.unwrap();
        assert!(
            !after.archived,
            "the post-archive user send auto-unarchived"
        );
        assert!(
            mgr.services.queue_snapshot(&id).is_empty(),
            "both entries drained in the combined turn"
        );
    }
}

/// Attention-request clear gating (PROTOCOL §5.5): a pending
/// `ws.agent.requestDiscussion` / `ws.agent.reportBlocker` request retires on
/// a USER-ORIGIN delivery for every agent, and ALSO on an automatic delivery
/// (A2A sends, parent/subscription wakes, sendToTask, wakeOrCreate) when the
/// target session is a CHILD (`parent_agent_id` set) or BACKGROUND
/// (`is_background`) agent — the parent/coordinator is those agents'
/// attention surface. For a top-level foreground agent an automatic/system
/// message must never dismiss a request the user has not seen. The drain
/// handoffs restore each queue entry's captured origin, so a user message
/// that parked behind a busy turn still clears the request when it drains.
mod attention_request_clear_gates {
    use super::*;
    use crate::agent_manager::TurnOptions;
    use intent_core::MessageOrigin;

    /// Seed an agent on the mock provider with a pending attention request
    /// persisted on the session, shaped as a child (`parent`) and/or
    /// background (`background`) session.
    async fn seed_with_pending_request_shaped(
        mgr: &AgentManager,
        ws: &WorkspaceId,
        id: &AgentId,
        parent: Option<AgentId>,
        background: bool,
    ) {
        seed_agent(mgr, ws, id).await;
        let mut session = mgr.services.store.get_agent_session(id).await.unwrap();
        session.provider = Some("mock".to_string());
        session.parent_agent_id = parent;
        session.is_background = background;
        mgr.services
            .store
            .update_agent_session(ws, &session)
            .await
            .expect("seed session shape");
        mgr.services
            .store
            .set_attention_request(ws, id, "discussion", "need a decision", &now_iso())
            .await
            .expect("seed pending attention request");
    }

    /// Seed a TOP-LEVEL FOREGROUND agent on the mock provider with a pending
    /// attention request persisted on the session.
    async fn seed_with_pending_request(mgr: &AgentManager, ws: &WorkspaceId, id: &AgentId) {
        seed_with_pending_request_shaped(mgr, ws, id, None, false).await;
    }

    /// Wait for the in-flight turn + worker drain to finish.
    async fn await_worker_idle(mgr: &Arc<AgentManager>, id: &AgentId) {
        timeout(Duration::from_secs(10), async {
            loop {
                if !mgr.is_busy(id)
                    && mgr.workers.lock().unwrap().is_empty()
                    && !mgr.services.has_ready_to_send(id)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("turn completes and worker exits");
    }

    /// Drain the subscription and report whether an `agent:updated` with
    /// `attentionRequestCleared: true` was published.
    async fn saw_cleared_event(sub: &mut crate::events::Subscription) -> bool {
        let mut cleared = false;
        while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
            for e in batch {
                if e.event_type == intent_core::events::AGENT_UPDATED
                    && e.data["attentionRequestCleared"] == json!(true)
                {
                    cleared = true;
                }
            }
        }
        cleared
    }

    /// An automatic delivery (default `TurnOptions`, e.g. an A2A send or a
    /// parent wake) to a TOP-LEVEL FOREGROUND agent leaves the pending
    /// request intact and emits no `attentionRequestCleared`.
    #[tokio::test]
    async fn automatic_delivery_leaves_attention_request_pending() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let (_tmp, mgr, bus) = manager_with_bus().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (
            WorkspaceId::from("ws-attn-auto"),
            AgentId::from("a-attn-auto"),
        );
        seed_with_pending_request(&mgr, &ws, &id).await;

        let mut sub = bus.subscribe(SubscriptionFilter::default());
        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                "auto wake".to_string(),
                None,
                TurnOptions::default(),
            )
            .await
            .expect("automatic send");
        assert_eq!(r["queued"], json!(false), "idle agent delivers directly");
        await_worker_idle(&mgr, &id).await;

        assert!(
            !saw_cleared_event(&mut sub).await,
            "automatic delivery must not emit attentionRequestCleared"
        );
        let session = mgr.services.store.get_agent_session(&id).await.unwrap();
        assert_eq!(
            session.attention_request_kind.as_deref(),
            Some("discussion"),
            "request kind survives the automatic delivery"
        );
        assert_eq!(
            session.attention_request_reason.as_deref(),
            Some("need a decision")
        );
        assert!(session.attention_request_timestamp.is_some());
    }

    /// A user-origin delivery (`agent.sendMessage` front door) clears the
    /// pending request and emits `attentionRequestCleared: true`.
    #[tokio::test]
    async fn user_delivery_clears_attention_request() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let (_tmp, mgr, bus) = manager_with_bus().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (
            WorkspaceId::from("ws-attn-user"),
            AgentId::from("a-attn-user"),
        );
        seed_with_pending_request(&mgr, &ws, &id).await;

        let mut sub = bus.subscribe(SubscriptionFilter::default());
        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                "user follow-up".to_string(),
                None,
                TurnOptions {
                    origin: MessageOrigin::User,
                    ..TurnOptions::default()
                },
            )
            .await
            .expect("user send");
        assert_eq!(r["queued"], json!(false), "idle agent delivers directly");
        await_worker_idle(&mgr, &id).await;

        assert!(
            saw_cleared_event(&mut sub).await,
            "user delivery emits attentionRequestCleared"
        );
        let session = mgr.services.store.get_agent_session(&id).await.unwrap();
        assert_eq!(session.attention_request_kind, None);
        assert_eq!(session.attention_request_reason, None);
        assert_eq!(session.attention_request_timestamp, None);
    }

    /// A drained USER-ORIGIN queue entry (a user send that parked behind a
    /// busy turn) clears the request — the drain handoff restores the
    /// entry's origin instead of defaulting to `Automatic`.
    #[tokio::test]
    async fn drained_user_origin_entry_clears_attention_request() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let (_tmp, mgr, bus) = manager_with_bus().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (
            WorkspaceId::from("ws-attn-drain"),
            AgentId::from("a-attn-drain"),
        );
        seed_with_pending_request(&mgr, &ws, &id).await;

        let opts = TurnOptions {
            origin: MessageOrigin::User,
            ..TurnOptions::default()
        };
        mgr.services.enqueue_message_with_origin(
            &id,
            "parked user answer".to_string(),
            None,
            None,
            None,
            opts.queued_prepend(),
            opts.interrupt_priority,
            opts.origin.is_user(),
        );
        let mut sub = bus.subscribe(SubscriptionFilter::default());
        mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;
        await_worker_idle(&mgr, &id).await;

        assert!(
            saw_cleared_event(&mut sub).await,
            "drained user-origin entry emits attentionRequestCleared"
        );
        let session = mgr.services.store.get_agent_session(&id).await.unwrap();
        assert_eq!(session.attention_request_kind, None);
        assert_eq!(session.attention_request_reason, None);
        assert_eq!(session.attention_request_timestamp, None);
    }

    /// A drained AUTOMATIC queue entry (e.g. a parked A2A wake) leaves the
    /// request pending — the restored origin is `Automatic`.
    #[tokio::test]
    async fn drained_automatic_entry_leaves_attention_request_pending() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let (_tmp, mgr, bus) = manager_with_bus().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (
            WorkspaceId::from("ws-attn-drain-auto"),
            AgentId::from("a-attn-drain-auto"),
        );
        seed_with_pending_request(&mgr, &ws, &id).await;

        mgr.services.enqueue_message(
            &id,
            "parked auto wake".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        let mut sub = bus.subscribe(SubscriptionFilter::default());
        mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;
        await_worker_idle(&mgr, &id).await;

        assert!(
            !saw_cleared_event(&mut sub).await,
            "drained automatic entry must not emit attentionRequestCleared"
        );
        let session = mgr.services.store.get_agent_session(&id).await.unwrap();
        assert_eq!(
            session.attention_request_kind.as_deref(),
            Some("discussion"),
            "request survives the automatic drain"
        );
    }

    /// An automatic delivery to a CHILD agent (`parent_agent_id` set) clears
    /// the pending request and emits `attentionRequestCleared: true` — the
    /// parent is the child's attention surface, so its follow-up is the
    /// acknowledgement.
    #[tokio::test]
    async fn automatic_delivery_clears_attention_request_for_child_agent() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let (_tmp, mgr, bus) = manager_with_bus().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (
            WorkspaceId::from("ws-attn-child"),
            AgentId::from("a-attn-child"),
        );
        seed_with_pending_request_shaped(&mgr, &ws, &id, Some(AgentId::from("a-parent")), false)
            .await;

        let mut sub = bus.subscribe(SubscriptionFilter::default());
        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                "parent wake".to_string(),
                None,
                TurnOptions::default(),
            )
            .await
            .expect("automatic send");
        assert_eq!(r["queued"], json!(false), "idle agent delivers directly");
        await_worker_idle(&mgr, &id).await;

        assert!(
            saw_cleared_event(&mut sub).await,
            "automatic delivery to a child agent emits attentionRequestCleared"
        );
        let session = mgr.services.store.get_agent_session(&id).await.unwrap();
        assert_eq!(session.attention_request_kind, None);
        assert_eq!(session.attention_request_reason, None);
        assert_eq!(session.attention_request_timestamp, None);
    }

    /// An automatic delivery to a BACKGROUND (`is_background`, unparented)
    /// agent clears the pending request and emits
    /// `attentionRequestCleared: true`.
    #[tokio::test]
    async fn automatic_delivery_clears_attention_request_for_background_agent() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let (_tmp, mgr, bus) = manager_with_bus().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (WorkspaceId::from("ws-attn-bg"), AgentId::from("a-attn-bg"));
        seed_with_pending_request_shaped(&mgr, &ws, &id, None, true).await;

        let mut sub = bus.subscribe(SubscriptionFilter::default());
        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                "auto wake".to_string(),
                None,
                TurnOptions::default(),
            )
            .await
            .expect("automatic send");
        assert_eq!(r["queued"], json!(false), "idle agent delivers directly");
        await_worker_idle(&mgr, &id).await;

        assert!(
            saw_cleared_event(&mut sub).await,
            "automatic delivery to a background agent emits attentionRequestCleared"
        );
        let session = mgr.services.store.get_agent_session(&id).await.unwrap();
        assert_eq!(session.attention_request_kind, None);
        assert_eq!(session.attention_request_reason, None);
        assert_eq!(session.attention_request_timestamp, None);
    }

    /// A user-origin delivery to a CHILD agent still clears the pending
    /// request — user-origin dismissal is unchanged for all agent shapes.
    #[tokio::test]
    async fn user_delivery_clears_attention_request_for_child_agent() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", script.as_str())]);
        let (_tmp, mgr, bus) = manager_with_bus().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (
            WorkspaceId::from("ws-attn-child-user"),
            AgentId::from("a-attn-child-user"),
        );
        seed_with_pending_request_shaped(&mgr, &ws, &id, Some(AgentId::from("a-parent")), false)
            .await;

        let mut sub = bus.subscribe(SubscriptionFilter::default());
        let r = mgr
            .clone()
            .send_message(
                id.clone(),
                ws.clone(),
                "user follow-up".to_string(),
                None,
                TurnOptions {
                    origin: MessageOrigin::User,
                    ..TurnOptions::default()
                },
            )
            .await
            .expect("user send");
        assert_eq!(r["queued"], json!(false), "idle agent delivers directly");
        await_worker_idle(&mgr, &id).await;

        assert!(
            saw_cleared_event(&mut sub).await,
            "user delivery to a child agent emits attentionRequestCleared"
        );
        let session = mgr.services.store.get_agent_session(&id).await.unwrap();
        assert_eq!(session.attention_request_kind, None);
        assert_eq!(session.attention_request_reason, None);
        assert_eq!(session.attention_request_timestamp, None);
    }
}

/// Batch flush (`agents.flushQueuedMessages`, default on): ≥2 ready-to-send
/// entries drain into ONE combined provider turn (header + `Message #N:`
/// sections) while the transcript persists one user row per entry; the
/// setting off restores the one-turn-per-message behavior; a persist failure
/// mid-flush parks in Error with every entry requeued in original order
/// (never-lost).
mod flush_queued_messages_tests {
    use super::*;
    use crate::agent_ops::QueuedMessage;

    fn queued_msg(content: &str) -> QueuedMessage {
        QueuedMessage {
            id: "qm-flush-test".to_string(),
            turn_id: "qm-flush-test".to_string(),
            content: content.to_string(),
            image_blocks: None,
            file_blocks: None,
            queued_at: "2026-01-01T00:00:00Z".to_string(),
            editing: false,
            persisted: false,
            requeued_after_failure: false,
            message_metadata: None,
            prepend_content: None,
            prepend_image_blocks: None,
            prepend_file_blocks: None,
            interrupt_priority: false,
            user_origin: false,
        }
    }

    #[test]
    fn combined_prompt_carries_header_and_labeled_sections_in_order() {
        let entries = vec![queued_msg("first body"), queued_msg("second body")];
        let prompt = super::super::flush_combined_prompt(&entries);
        assert!(
            prompt.starts_with("2 queued messages while you were working"),
            "header names the flushed count: {prompt}"
        );
        let m1 = prompt.find("Message #1:\nfirst body").expect("entry 1");
        let m2 = prompt.find("Message #2:\nsecond body").expect("entry 2");
        assert!(m1 < m2, "delivery order preserved: {prompt}");
    }

    /// Read the mock fixture's prompt log: one JSON line per received prompt.
    fn read_prompt_log(path: &PathBuf) -> Vec<String> {
        std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .map(|l| {
                serde_json::from_str::<Value>(l).expect("prompt log line")["text"]
                    .as_str()
                    .expect("text")
                    .to_string()
            })
            .collect()
    }

    async fn seed_mock_agent(mgr: &AgentManager, ws: &WorkspaceId, id: &AgentId) {
        seed_agent(mgr, ws, id).await;
        let mut session = mgr.services.store.get_agent_session(id).await.unwrap();
        session.provider = Some("mock".to_string());
        mgr.services
            .store
            .update_agent_session(ws, &session)
            .await
            .expect("set mock provider");
    }

    #[tokio::test]
    async fn drain_flushes_two_ready_entries_into_one_combined_turn() {
        let script = mock_agent_script();
        let prompt_log =
            std::env::temp_dir().join(format!("itd-flush-on-{}.log", uuid::Uuid::new_v4()));
        let prompt_log_s = prompt_log.to_string_lossy().into_owned();
        let _env = EnvGuard::set_all(&[
            ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
            ("MOCK_AGENT_PROMPT_LOG", prompt_log_s.as_str()),
        ]);
        let (_tmp, mgr) = manager().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (
            WorkspaceId::from("ws-flush-on"),
            AgentId::from("a-flush-on"),
        );
        seed_mock_agent(&mgr, &ws, &id).await;

        mgr.services.enqueue_message(
            &id,
            "first message".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        mgr.services.enqueue_message(
            &id,
            "second message".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;
        timeout(Duration::from_secs(15), async {
            loop {
                if !mgr.is_busy(&id)
                    && mgr.workers.lock().unwrap().is_empty()
                    && !mgr.services.has_ready_to_send(&id)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("combined turn completes");

        // ONE provider turn carrying the combined prompt.
        let prompts = read_prompt_log(&prompt_log);
        let _ = std::fs::remove_file(&prompt_log);
        assert_eq!(prompts.len(), 1, "one combined turn: {prompts:?}");
        let text = &prompts[0];
        assert!(
            text.contains("2 queued messages while you were working"),
            "combined header: {text}"
        );
        let m1 = text.find("Message #1:").expect("label #1");
        let m2 = text.find("Message #2:").expect("label #2");
        let p1 = text.find("first message").expect("entry 1 content");
        let p2 = text.find("second message").expect("entry 2 content");
        assert!(m1 < p1 && p1 < m2 && m2 < p2, "labeled in order: {text}");

        // The transcript gains one user row PER entry — never the combined
        // prompt — plus the turn's assistant output.
        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .expect("messages");
        let user_texts: Vec<_> = messages
            .iter()
            .filter(|m| m.role == "user")
            .filter_map(|m| m.content[0]["text"].as_str())
            .collect();
        assert_eq!(user_texts.len(), 2, "one row per entry: {user_texts:?}");
        assert!(user_texts[0].starts_with("first message"), "{user_texts:?}");
        assert!(
            user_texts[1].starts_with("second message"),
            "{user_texts:?}"
        );
        assert!(
            user_texts
                .iter()
                .all(|t| !t.contains("queued messages while you were working")),
            "combined prompt is wire-only, never persisted: {user_texts:?}"
        );
        assert!(
            messages.iter().any(|m| m.role == "assistant"),
            "the combined turn completed: {messages:?}"
        );
    }

    #[tokio::test]
    async fn setting_off_keeps_one_turn_per_message() {
        let script = mock_agent_script();
        let prompt_log =
            std::env::temp_dir().join(format!("itd-flush-off-{}.log", uuid::Uuid::new_v4()));
        let prompt_log_s = prompt_log.to_string_lossy().into_owned();
        let _env = EnvGuard::set_all(&[
            ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
            ("MOCK_AGENT_PROMPT_LOG", prompt_log_s.as_str()),
        ]);
        // A manager whose settings registry has the flush setting OFF.
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let bus = EventBus::new(store.clone());
        let config_dir = tempfile::tempdir().expect("temp config dir");
        let registry = Arc::new(
            crate::SettingsRegistry::load(config_dir.path().join("config.toml"))
                .expect("load registry"),
        );
        registry
            .apply(&[("agents.flushQueuedMessages".to_string(), json!("off"))])
            .expect("disable flush");
        let services = Services::new(store)
            .with_event_bus(bus.clone())
            .with_settings_registry(registry);
        let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
        let mgr = Arc::new(AgentManager::new(services, sink, 8));
        let (ws, id) = (
            WorkspaceId::from("ws-flush-off"),
            AgentId::from("a-flush-off"),
        );
        seed_mock_agent(&mgr, &ws, &id).await;

        mgr.services.enqueue_message(
            &id,
            "first message".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        mgr.services.enqueue_message(
            &id,
            "second message".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;
        timeout(Duration::from_secs(15), async {
            loop {
                if !mgr.is_busy(&id)
                    && mgr.workers.lock().unwrap().is_empty()
                    && !mgr.services.has_ready_to_send(&id)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("both turns complete");

        let prompts = read_prompt_log(&prompt_log);
        let _ = std::fs::remove_file(&prompt_log);
        assert_eq!(
            prompts.len(),
            2,
            "setting off: one turn per message: {prompts:?}"
        );
        assert!(
            prompts
                .iter()
                .all(|t| !t.contains("queued messages while you were working")),
            "no combined header on the single-entry path: {prompts:?}"
        );
    }

    /// `systemOnly` mode: with ≥2 ready system-origin entries interleaved
    /// with a user-origin entry, the system entries batch into ONE combined
    /// turn while the user-origin entry stays queued and drains solo
    /// afterward (its own single-entry FIFO turn).
    #[tokio::test]
    async fn system_only_batches_system_entries_and_leaves_user_entry_for_solo_fifo_drain() {
        let script = mock_agent_script();
        let prompt_log =
            std::env::temp_dir().join(format!("itd-flush-systemonly-{}.log", uuid::Uuid::new_v4()));
        let prompt_log_s = prompt_log.to_string_lossy().into_owned();
        let _env = EnvGuard::set_all(&[
            ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
            ("MOCK_AGENT_PROMPT_LOG", prompt_log_s.as_str()),
        ]);
        // A manager whose settings registry has the flush setting `systemOnly`.
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let bus = EventBus::new(store.clone());
        let config_dir = tempfile::tempdir().expect("temp config dir");
        let registry = Arc::new(
            crate::SettingsRegistry::load(config_dir.path().join("config.toml"))
                .expect("load registry"),
        );
        registry
            .apply(&[(
                "agents.flushQueuedMessages".to_string(),
                json!("systemOnly"),
            )])
            .expect("set flush mode to systemOnly");
        let services = Services::new(store)
            .with_event_bus(bus.clone())
            .with_settings_registry(registry);
        let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
        let mgr = Arc::new(AgentManager::new(services, sink, 8));
        let (ws, id) = (
            WorkspaceId::from("ws-flush-systemonly"),
            AgentId::from("a-flush-systemonly"),
        );
        seed_mock_agent(&mgr, &ws, &id).await;

        mgr.services
            .enqueue_message(&id, "sys-1".to_string(), None, None, None, None, false);
        mgr.services.enqueue_message_with_origin(
            &id,
            "user-1".to_string(),
            None,
            None,
            None,
            None,
            false,
            true,
        );
        mgr.services
            .enqueue_message(&id, "sys-2".to_string(), None, None, None, None, false);

        mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;
        timeout(Duration::from_secs(15), async {
            loop {
                if !mgr.is_busy(&id)
                    && mgr.workers.lock().unwrap().is_empty()
                    && !mgr.services.has_ready_to_send(&id)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("both the combined system turn and the solo user turn complete");

        let prompts = read_prompt_log(&prompt_log);
        let _ = std::fs::remove_file(&prompt_log);
        assert_eq!(
            prompts.len(),
            2,
            "one combined system turn + one solo user turn: {prompts:?}"
        );
        let combined = &prompts[0];
        assert!(
            combined.contains("2 queued messages while you were working"),
            "first turn combines the two system entries: {combined}"
        );
        let s1 = combined.find("sys-1").expect("sys-1 in combined turn");
        let s2 = combined.find("sys-2").expect("sys-2 in combined turn");
        assert!(s1 < s2, "system entries in relative order: {combined}");
        assert!(
            !combined.contains("user-1"),
            "user entry excluded from the system-only batch: {combined}"
        );
        let solo = &prompts[1];
        assert!(
            solo.contains("user-1") && !solo.contains("queued messages while you were working"),
            "second turn is the solo FIFO drain of the user entry: {solo}"
        );
    }

    /// `systemOnly` mode with only ONE ready system entry (no batching
    /// partner) drains it solo via the single-entry FIFO path — no combined
    /// header.
    #[tokio::test]
    async fn system_only_single_system_entry_drains_solo() {
        let script = mock_agent_script();
        let prompt_log = std::env::temp_dir().join(format!(
            "itd-flush-systemonly-solo-{}.log",
            uuid::Uuid::new_v4()
        ));
        let prompt_log_s = prompt_log.to_string_lossy().into_owned();
        let _env = EnvGuard::set_all(&[
            ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
            ("MOCK_AGENT_PROMPT_LOG", prompt_log_s.as_str()),
        ]);
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let bus = EventBus::new(store.clone());
        let config_dir = tempfile::tempdir().expect("temp config dir");
        let registry = Arc::new(
            crate::SettingsRegistry::load(config_dir.path().join("config.toml"))
                .expect("load registry"),
        );
        registry
            .apply(&[(
                "agents.flushQueuedMessages".to_string(),
                json!("systemOnly"),
            )])
            .expect("set flush mode to systemOnly");
        let services = Services::new(store)
            .with_event_bus(bus.clone())
            .with_settings_registry(registry);
        let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
        let mgr = Arc::new(AgentManager::new(services, sink, 8));
        let (ws, id) = (
            WorkspaceId::from("ws-flush-systemonly-solo"),
            AgentId::from("a-flush-systemonly-solo"),
        );
        seed_mock_agent(&mgr, &ws, &id).await;

        mgr.services
            .enqueue_message(&id, "sys-only".to_string(), None, None, None, None, false);

        mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;
        timeout(Duration::from_secs(15), async {
            loop {
                if !mgr.is_busy(&id)
                    && mgr.workers.lock().unwrap().is_empty()
                    && !mgr.services.has_ready_to_send(&id)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("solo turn completes");

        let prompts = read_prompt_log(&prompt_log);
        let _ = std::fs::remove_file(&prompt_log);
        assert_eq!(prompts.len(), 1, "single solo turn: {prompts:?}");
        assert!(
            !prompts[0].contains("queued messages while you were working"),
            "no combined header for a lone system entry: {prompts:?}"
        );
    }

    /// Fail closed (#547) mid-flush: the head entry's row append fails
    /// terminally, so the agent parks in Error WITHOUT starting the turn and
    /// BOTH flushed entries are requeued in original order; `agent.retry`
    /// against a restored store then delivers both exactly once.
    #[tokio::test]
    async fn mid_flush_persist_failure_requeues_all_entries_in_order() {
        let script = mock_agent_script();
        let _env = EnvGuard::set_all(&[
            ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
            ("INTENTD_PERSIST_RETRY_BACKOFF_MS", "10,10"),
        ]);
        let (_tmp, mgr) = manager().await;
        let mgr = Arc::new(mgr);
        let (ws, id) = (
            WorkspaceId::from("ws-flush-547"),
            AgentId::from("a-flush-547"),
        );
        seed_mock_agent(&mgr, &ws, &id).await;

        mgr.services
            .enqueue_message(&id, "boom-1".to_string(), None, None, None, None, false);
        mgr.services
            .enqueue_message(&id, "boom-2".to_string(), None, None, None, None, false);
        sqlx::query("ALTER TABLE agent_message RENAME TO agent_message_broken")
            .execute(mgr.services.store.write_pool())
            .await
            .expect("hide agent_message table");

        mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;
        timeout(Duration::from_secs(10), async {
            loop {
                let status = mgr
                    .services
                    .store
                    .get_agent_session_status(&id)
                    .await
                    .unwrap();
                if status == AgentStatus::Error
                    && !mgr.is_busy(&id)
                    && mgr.workers.lock().unwrap().is_empty()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("flush parks the session in error without a worker");

        // Both entries back in the queue, original order, unpersisted.
        let snap = mgr.services.queue_snapshot(&id);
        assert_eq!(snap.len(), 2, "never-lost: both entries requeued: {snap:?}");
        assert!(
            snap[0]["content"].as_str().unwrap().starts_with("boom-1"),
            "original order preserved: {snap:?}"
        );
        assert!(
            snap[1]["content"].as_str().unwrap().starts_with("boom-2"),
            "original order preserved: {snap:?}"
        );

        // Restore the store; agent.retry redrives BOTH entries.
        sqlx::query("ALTER TABLE agent_message_broken RENAME TO agent_message")
            .execute(mgr.services.store.write_pool())
            .await
            .expect("restore agent_message table");
        let result = mgr
            .agent_retry(id.clone(), ws.clone())
            .await
            .expect("agent.retry");
        assert_eq!(result["redriven"], json!(true));
        timeout(Duration::from_secs(15), async {
            loop {
                let session = mgr.services.store.get_agent_session(&id).await.unwrap();
                if session.status == AgentStatus::RuntimeIdle
                    && !mgr.is_busy(&id)
                    && mgr.workers.lock().unwrap().is_empty()
                    && !mgr.services.has_ready_to_send(&id)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("retry delivers the requeued batch");

        let messages = mgr
            .services
            .store
            .get_agent_messages(&id, None)
            .await
            .expect("messages");
        for needle in ["boom-1", "boom-2"] {
            let rows = messages
                .iter()
                .filter(|m| {
                    m.role == "user"
                        && m.content[0]["text"]
                            .as_str()
                            .is_some_and(|t| t.starts_with(needle))
                })
                .count();
            assert_eq!(rows, 1, "{needle} lands exactly once: {messages:?}");
        }
    }
}

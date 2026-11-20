//! The sidecar's own PIN attempt limit (spec §5.3), on top of PAM's faillock:
//! after 5 wrong PINs each try waits 30 s, doubling; after 10, device passkeys stay
//! off until the user unlocks the screen again (or the machine restarts, which
//! also passes the lock screen). The counter is kept on disk.

use serde::{Deserialize, Serialize};

use super::store::StoreDir;

const LIMITER_FILE: &str = "limiter.json";
pub(crate) const MAX_FAILURES: u32 = 10;
const FREE_FAILURES: u32 = 5;
const FIRST_WAIT_MS: u64 = 30_000;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct State {
    failures: u32,
    last_failure_at: u64,
    boot_id: Option<String>,
    /// A lock screen was seen after the last wrong PIN; once it closes, the user
    /// has entered the right PIN there.
    saw_screen_lock: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Gate {
    Open {
        attempts_left: u32,
    },
    Wait {
        retry_after_ms: u64,
        attempts_left: u32,
    },
    Locked,
}

pub(crate) struct Limiter {
    dir: StoreDir,
    boot_id: Option<String>,
    state: State,
}

impl Limiter {
    /// A damaged counter file counts as locked (fail closed).
    pub fn load(dir: StoreDir, boot_id: Option<String>) -> Self {
        let state = match std::fs::read(dir.path(LIMITER_FILE)) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or(State {
                failures: MAX_FAILURES,
                boot_id: boot_id.clone(),
                ..State::default()
            }),
            Err(_) => State::default(),
        };
        let mut limiter = Self {
            dir,
            boot_id,
            state,
        };
        if limiter.state.failures > 0
            && limiter.boot_id.is_some()
            && limiter.state.boot_id != limiter.boot_id
        {
            limiter.reset();
        }
        limiter
    }

    pub fn gate(&self, now_ms: u64) -> Gate {
        let failures = self.state.failures;
        if failures >= MAX_FAILURES {
            return Gate::Locked;
        }
        let attempts_left = MAX_FAILURES - failures;
        if failures < FREE_FAILURES {
            return Gate::Open { attempts_left };
        }
        let wait = FIRST_WAIT_MS << (failures - FREE_FAILURES).min(16);
        let ready_at = self.state.last_failure_at.saturating_add(wait);
        if now_ms >= ready_at {
            Gate::Open { attempts_left }
        } else {
            Gate::Wait {
                retry_after_ms: ready_at - now_ms,
                attempts_left,
            }
        }
    }

    pub fn failed(&mut self, now_ms: u64) {
        self.state.failures = (self.state.failures + 1).min(MAX_FAILURES);
        self.state.last_failure_at = now_ms;
        self.state.boot_id.clone_from(&self.boot_id);
        self.state.saw_screen_lock = false;
        self.persist();
    }

    pub fn succeeded(&mut self) {
        if self.state != State::default() {
            self.reset();
        }
    }

    /// Feeds the lock screen state; returns whether the counter was cleared.
    pub fn observe_screen(&mut self, locked: bool) -> bool {
        if self.state.failures == 0 {
            return false;
        }
        if locked {
            if !self.state.saw_screen_lock {
                self.state.saw_screen_lock = true;
                self.persist();
            }
            false
        } else if self.state.saw_screen_lock {
            self.reset();
            true
        } else {
            false
        }
    }

    pub fn has_failures(&self) -> bool {
        self.state.failures > 0
    }

    fn reset(&mut self) {
        self.state = State::default();
        self.persist();
    }

    fn persist(&self) {
        let written = serde_json::to_vec(&self.state)
            .map_err(|error| error.to_string())
            .and_then(|bytes| {
                self.dir
                    .write_atomic(LIMITER_FILE, &bytes)
                    .map_err(|error| error.message)
            });
        if let Err(error) = written {
            tracing::warn!(%error, "could not save the PIN attempt counter");
        }
    }
}

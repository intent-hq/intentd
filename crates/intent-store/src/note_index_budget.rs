//! Cooperative limits around an isolated, database-free conversion.
//!
//! Third-party tree sinks cannot return our domain errors. A private unwind
//! exits those callbacks without invoking the panic hook; it is caught only at
//! the conversion boundary, before any SQL is submitted. Unrelated panics are
//! propagated. Ordinary canonical writers do not install this budget.
use intent_core::{note_page::NotePageError, Error, Result};
use std::{
    cell::RefCell,
    sync::Arc,
    time::{Duration, Instant},
};

pub(crate) const SOURCE_BYTES: usize = 32 * 1024 * 1024;
pub(crate) const AUX_BYTES: usize = 4 * 1024 * 1024;
const WORK_BYTES: usize = 512 * 1024 * 1024;
const STEPS: usize = 1_000_000;
const DEPTH: usize = 128;

struct Budget {
    remaining: usize,
    steps: usize,
    depth: usize,
    deadline: Instant,
    live: Arc<dyn Fn() -> bool + Send + Sync>,
}
thread_local! { static BUDGET: RefCell<Option<Budget>> = const { RefCell::new(None) }; }
struct Exhausted;
struct Cancelled;
struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        BUDGET.with(|slot| *slot.borrow_mut() = None);
    }
}

pub(crate) fn run<T>(
    live: Arc<dyn Fn() -> bool + Send + Sync>,
    f: impl FnOnce() -> T,
) -> Result<T> {
    BUDGET.with(|slot| {
        assert!(slot.borrow().is_none(), "conversion budgets cannot nest");
        *slot.borrow_mut() = Some(Budget {
            remaining: WORK_BYTES,
            steps: STEPS,
            depth: 0,
            deadline: Instant::now() + Duration::from_secs(60),
            live,
        });
    });
    let _reset = Reset;
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(value) => Ok(value),
        Err(error) if error.is::<Cancelled>() => Err(Error::NotePage(NotePageError::Preparing)),
        Err(error) if error.is::<Exhausted>() => Err(Error::NotePage(NotePageError::ResourceLimit)),
        Err(error) => std::panic::resume_unwind(error),
    }
}

pub(crate) fn charge(bytes: usize) {
    let exhausted = BUDGET.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(budget) = slot.as_mut() else {
            return false;
        };
        if !(budget.live)() {
            std::panic::resume_unwind(Box::new(Cancelled));
        }
        let Some(remaining) = budget.remaining.checked_sub(bytes) else {
            return true;
        };
        budget.remaining = remaining;
        budget.steps = budget.steps.saturating_sub(1);
        budget.steps == 0 || Instant::now() >= budget.deadline
    });
    if exhausted {
        std::panic::resume_unwind(Box::new(Exhausted));
    }
}

pub(crate) fn check_depth(depth: usize) {
    if depth > DEPTH && BUDGET.with(|slot| slot.borrow().is_some()) {
        std::panic::resume_unwind(Box::new(Exhausted));
    }
}
pub(crate) fn active() -> bool {
    BUDGET.with(|slot| slot.borrow().is_some())
}

pub(crate) struct Depth;
pub(crate) fn enter() -> Depth {
    charge(0);
    let exhausted = BUDGET.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(budget) = slot.as_mut() else {
            return false;
        };
        budget.depth += 1;
        budget.depth > DEPTH
    });
    if exhausted {
        std::panic::resume_unwind(Box::new(Exhausted));
    }
    Depth
}
impl Drop for Depth {
    fn drop(&mut self) {
        BUDGET.with(|slot| {
            if let Some(budget) = slot.borrow_mut().as_mut() {
                budget.depth -= 1;
            }
        });
    }
}

pub(crate) fn value(value: &serde_json::Value) {
    // Conservatively charge JSON container/key overhead as well as scalar bytes.
    fn size(value: &serde_json::Value) -> usize {
        match value {
            serde_json::Value::String(s) => 64 + s.capacity(),
            serde_json::Value::Array(items) => {
                64 + items.capacity() * 64 + items.iter().map(size).sum::<usize>()
            }
            serde_json::Value::Object(items) => {
                64 + items
                    .iter()
                    .map(|(key, v)| 256 + key.capacity() + size(v))
                    .sum::<usize>()
            }
            _ => 64,
        }
    }
    if BUDGET.with(|slot| slot.borrow().is_none()) {
        return;
    }
    charge(size(value));
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn limits_unwind_only_the_owned_conversion_and_reset() {
        assert!(matches!(
            run(Arc::new(|| true), || charge(WORK_BYTES + 1)),
            Err(Error::NotePage(NotePageError::ResourceLimit))
        ));
        assert!(matches!(
            run(Arc::new(|| false), || charge(0)),
            Err(Error::NotePage(NotePageError::Preparing))
        ));
        assert_eq!(run(Arc::new(|| true), || 7).unwrap(), 7);
        charge(usize::MAX); // No budget in an ordinary writer.
    }
}

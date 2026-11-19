//! Test seam for multi-statement reads: pause a reader between its first
//! statement and the rest, so a test can commit a write in the gap and check
//! that the reader still answers from one snapshot
//! ([`PgStore::begin_read_snapshot`](super::PgStore::begin_read_snapshot)).
//! Keyed by tenant and reader, so parallel tests on other tenants are
//! unaffected. A no-op outside tests.

#[cfg(any(test, feature = "test-support"))]
pub mod probe {
    use std::collections::HashMap;
    use std::sync::{Arc, LazyLock, Mutex, PoisonError};
    use tokio::sync::Notify;

    /// One armed pause: the reader notifies `entered` once it has run its
    /// first statement, then waits for `release`.
    #[derive(Clone, Default)]
    pub struct ReadPause {
        pub entered: Arc<Notify>,
        pub release: Arc<Notify>,
    }

    type Key = (String, &'static str);

    static PAUSES: LazyLock<Mutex<HashMap<Key, ReadPause>>> = LazyLock::new(Mutex::default);

    /// Arm a one-shot pause of `reader` (the store method's name) for
    /// `tenant`'s next call; returns the pause to wait on and release.
    #[must_use]
    pub fn pause(tenant: &str, reader: &'static str) -> ReadPause {
        let pause = ReadPause::default();
        PAUSES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((tenant.to_owned(), reader), pause.clone());
        pause
    }

    /// Called by `reader` after its first statement: waits in an armed pause.
    pub(crate) async fn between(tenant: &str, reader: &'static str) {
        let armed = PAUSES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(tenant.to_owned(), reader));
        if let Some(pause) = armed {
            pause.entered.notify_one();
            pause.release.notified().await;
        }
    }
}

#[cfg(not(any(test, feature = "test-support")))]
pub(crate) mod probe {
    #[expect(clippy::unused_async, reason = "same signature as the test probe")]
    #[inline]
    pub(crate) async fn between(_tenant: &str, _reader: &'static str) {}
}

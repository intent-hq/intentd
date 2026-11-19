//! Shared setup for the concurrency suites (spec §6.5.3): extra handles for
//! one tenant and a runner that starts N writers together.
//!
//! Each suite runs through `parity_test!`, so it also runs on `SQLite` and
//! under the leak trap. On Postgres the writers share the test pool (four
//! connections), so up to four transactions of one tenant are in flight at
//! once and every read-then-write method depends on the tenant writer lock
//! to stay atomic.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use intent_store::Store;
use tokio::sync::Barrier;

use crate::harness::{open_trapped, TestDb};

/// Concurrent writers per race.
pub const WRITERS: usize = 8;

/// Two handles for this run's tenant. On Postgres, `store` and a second
/// open: each open has its own process-local gates (note lanes, desktop
/// write gate), so only the database (the writer lock and row locks) orders
/// writes between them, as between two daemons. On `SQLite` the second is a
/// clone of `store`: one file has one writer.
pub async fn handles(store: &Store, db: Option<&TestDb>, tenant: &str) -> Vec<Store> {
    let second = match db {
        Some(db) => open_trapped(db, tenant).await,
        None => store.clone(),
    };
    vec![store.clone(), second]
}

/// Runs `writers` tasks that start together; task `i` gets handle
/// `i % handles.len()`. Returns the results in task order. A task that
/// panics fails the test; a deadlock fails the hang guard.
pub async fn race<T, F, Fut>(handles: &[Store], writers: usize, task: F) -> Vec<T>
where
    F: Fn(usize, Store) -> Fut,
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let start = Arc::new(Barrier::new(writers));
    let mut set = tokio::task::JoinSet::new();
    for i in 0..writers {
        let run = task(i, handles[i % handles.len()].clone());
        let start = start.clone();
        set.spawn(async move {
            start.wait().await;
            (i, run.await)
        });
    }
    let mut results: Vec<Option<T>> = (0..writers).map(|_| None).collect();
    tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(60)),
        async {
            while let Some(joined) = set.join_next().await {
                let (i, value) = joined.expect("writer task panicked");
                results[i] = Some(value);
            }
        },
    )
    .await
    .expect("the writers finish (no deadlock)");
    results
        .into_iter()
        .map(|r| r.expect("every writer reported"))
        .collect()
}

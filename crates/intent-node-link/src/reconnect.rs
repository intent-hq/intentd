//! Redial with exponential backoff.

use std::future::Future;
use std::time::Duration;

use tokio::sync::watch;

use intent_core::node_link::LinkHello;
use intent_core::{Error, Result};

use super::{dial, error_code, is_retryable, DialedLink, NodeEndpoint};

/// Exponential backoff with ±20% jitter between `min` and `max`.
#[derive(Debug, Clone)]
pub struct Backoff {
    min: Duration,
    max: Duration,
    next: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new(Duration::from_millis(500), Duration::from_secs(30))
    }
}

impl Backoff {
    #[must_use]
    pub fn new(min: Duration, max: Duration) -> Self {
        let max = max.max(min);
        Self {
            min,
            max,
            next: min,
        }
    }

    /// The delay before the next attempt; each call doubles the base delay
    /// up to `max`.
    pub fn next_delay(&mut self) -> Duration {
        let base = self.next;
        self.next = self.next.saturating_mul(2).min(self.max);
        jitter(base).min(self.max)
    }

    /// Start again from `min`, after a successful connection.
    pub fn reset(&mut self) {
        self.next = self.min;
    }
}

fn jitter(base: Duration) -> Duration {
    let Ok(random) = getrandom::u64() else {
        return base;
    };
    // A factor in [0.8, 1.2].
    let per_mille = 800 + random % 401;
    base.saturating_mul(u32::try_from(per_mille).unwrap_or(1000)) / 1000
}

/// Dial until a link is up, `shutdown` turns `true`, or a permanent error
/// (certificate, token, ownership or version) occurs. `next_hello` is called
/// before every attempt so head can durably advance the generation each
/// time; a stale rejection is retried with the next one.
///
/// # Errors
///
/// The permanent dial error, or `node-link-closed` on shutdown.
pub async fn dial_with_backoff<F, Fut>(
    endpoint: &NodeEndpoint,
    mut next_hello: F,
    backoff: &mut Backoff,
    mut shutdown: watch::Receiver<bool>,
) -> Result<DialedLink>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<LinkHello>>,
{
    loop {
        if *shutdown.borrow() {
            return Err(Error::Internal(super::codes::CLOSED.into()));
        }
        let attempt = async {
            let hello = next_hello().await?;
            dial::dial_link(endpoint, hello).await
        };
        let error = tokio::select! {
            result = attempt => match result {
                Ok(dialed) => {
                    backoff.reset();
                    return Ok(dialed);
                }
                Err(error) => error,
            },
            () = super::wait_true(&mut shutdown) => {
                return Err(Error::Internal(super::codes::CLOSED.into()));
            }
        };
        if !is_retryable(&error) {
            return Err(error);
        }
        let delay = backoff.next_delay();
        tracing::debug!(
            code = error_code(&error).unwrap_or("other"),
            delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            "node link: dial failed; retrying"
        );
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            () = super::wait_true(&mut shutdown) => {
                return Err(Error::Internal(super::codes::CLOSED.into()));
            }
        }
    }
}

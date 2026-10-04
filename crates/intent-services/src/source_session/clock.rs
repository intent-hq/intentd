//! Root-owned acceptance/history clock, independent of Store authority and retirement.
use intent_core::note_source_session::{Result, SessionError};

#[derive(Debug)]
pub struct AcceptanceClock {
    anchor: i128,
    start: i128,
    high: i128,
    last: i128,
}
impl AcceptanceClock {
    /// Establish one paired UTC/monotonic observation for a Services root.
    /// # Errors
    /// Rejects negative samples; replacing this clock also requires a new root incarnation.
    pub fn new(utc: i128, monotonic: i128) -> Result<Self> {
        if utc < 0 || monotonic < 0 {
            return Err(SessionError::Uncertain);
        }
        Ok(Self {
            anchor: utc,
            start: monotonic,
            high: utc,
            last: monotonic,
        })
    }
    /// Observe under the same registry lock as eligibility and history retirement.
    /// # Errors
    /// Negative/backward monotonic samples or overflow fail closed without changing state.
    pub fn observe(&mut self, utc: i128, monotonic: i128) -> Result<i128> {
        if utc < 0 || monotonic < self.last {
            return Err(SessionError::Uncertain);
        }
        let elapsed = monotonic
            .checked_sub(self.start)
            .ok_or(SessionError::Uncertain)?;
        let anchored = self
            .anchor
            .checked_add(elapsed)
            .ok_or(SessionError::Uncertain)?;
        self.high = self.high.max(utc).max(anchored);
        self.last = monotonic;
        Ok(self.high)
    }
    #[must_use]
    pub const fn high(&self) -> i128 {
        self.high
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn forward_then_rollback_never_reenables_removed_history() {
        let mut c = AcceptanceClock::new(100, 10).unwrap();
        assert_eq!(c.observe(150, 11).unwrap(), 150);
        assert_eq!(c.observe(80, 12).unwrap(), 150);
        assert_eq!(c.observe(80, 70).unwrap(), 160);
    }
    #[test]
    fn invalid_samples_do_not_change_shared_observation() {
        let mut c = AcceptanceClock::new(i128::MAX - 1, 10).unwrap();
        assert!(c.observe(0, 12).is_err());
        assert_eq!(c.high(), i128::MAX - 1);
        assert_eq!(c.observe(0, 11).unwrap(), i128::MAX);
        assert!(c.observe(1, 10).is_err());
        assert!(c.observe(-1, 11).is_err());
        assert_eq!(c.high(), i128::MAX);
    }
}

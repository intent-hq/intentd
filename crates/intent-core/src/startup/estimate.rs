//! Conservative measured-byte throughput, with a bounded stable sampling window.
use super::{StartupProgress, MAX_SAFE};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

const WINDOW: Duration = Duration::from_secs(30);
const MIN_WINDOW: Duration = Duration::from_secs(2);
const MAX_SAMPLES: usize = 32;

#[derive(Default)]
pub(super) struct Estimator(VecDeque<(Instant, StartupProgress)>);

impl Estimator {
    pub(super) fn record(&mut self, now: Instant, progress: StartupProgress) {
        self.0.push_back((now, progress));
        while self.0.len() > MAX_SAMPLES
            || self
                .0
                .front()
                .is_some_and(|(time, _)| now.saturating_duration_since(*time) > WINDOW)
        {
            self.0.pop_front();
        }
    }

    pub(super) fn estimate(&self, now: Instant) -> Option<u64> {
        let (start, first) = self.0.front()?;
        let (end, last) = self.0.back()?;
        if now.saturating_duration_since(*end) >= WINDOW
            || end.saturating_duration_since(*start) < MIN_WINDOW
            || last.completed_notes.checked_sub(first.completed_notes)? < 5
            || last.total_source_bytes == 0
            || last.processed_source_bytes >= last.total_source_bytes
        {
            return None;
        }
        // Every interval must have measured forward byte movement. Reject a
        // >2x spread in observed rates: unequal note expansion/deletion costs
        // make a single average misleading even when enough notes finished.
        let mut low = u128::MAX;
        let mut high = 0;
        for (a, b) in self.0.iter().zip(self.0.iter().skip(1)) {
            let ms = b.0.saturating_duration_since(a.0).as_millis();
            let bytes =
                b.1.processed_source_bytes
                    .checked_sub(a.1.processed_source_bytes)?;
            if ms == 0 || bytes == 0 {
                return None;
            }
            let scaled_rate = u128::from(bytes) * 1_000_000 / ms;
            if scaled_rate == 0 {
                return None;
            }
            low = low.min(scaled_rate);
            high = high.max(scaled_rate);
        }
        if high > low.saturating_mul(2) {
            return None;
        }
        let bytes = last
            .processed_source_bytes
            .checked_sub(first.processed_source_bytes)?;
        let remaining = last
            .total_source_bytes
            .checked_sub(last.processed_source_bytes)?;
        let ms = u128::from(remaining) * end.saturating_duration_since(*start).as_millis()
            / u128::from(bytes);
        Some(u64::try_from(ms).unwrap_or(MAX_SAFE).min(MAX_SAFE))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn progress(notes: u64, bytes: u64) -> StartupProgress {
        StartupProgress {
            completed_notes: notes,
            total_notes: 100,
            processed_source_bytes: bytes,
            total_source_bytes: 10_000,
        }
    }

    #[test]
    fn stable_work_estimates_and_stalls_reset_the_window() {
        let now = Instant::now();
        let mut e = Estimator::default();
        e.record(now, progress(0, 0));
        assert_eq!(e.estimate(now), None);
        for note in 1..=5 {
            let time = now + Duration::from_millis(note * 500);
            e.record(time, progress(note, note * 1000));
            assert_eq!(e.estimate(time), if note == 5 { Some(2500) } else { None });
        }
        assert_eq!(e.estimate(now + Duration::from_secs(33)), None);
        e.record(now + Duration::from_secs(34), progress(6, 6000));
        assert_eq!(e.estimate(now + Duration::from_secs(34)), None);
        assert_eq!(e.0.len(), 1);
    }

    #[test]
    fn zero_short_unstable_and_completed_work_have_no_estimate() {
        let now = Instant::now();
        for scenario in ["zero", "short", "unstable", "complete"] {
            let mut e = Estimator::default();
            for n in 0..=6 {
                let ms = if scenario == "short" {
                    n * 100
                } else {
                    n * 500
                };
                let bytes = match scenario {
                    "zero" => 0,
                    "unstable" => {
                        if n < 5 {
                            n * 100
                        } else {
                            n * 1000
                        }
                    }
                    _ => n * 1000,
                };
                let mut p = progress(n, bytes);
                if scenario == "complete" {
                    p.total_source_bytes = 6000;
                }
                e.record(now + Duration::from_millis(ms), p);
            }
            assert_eq!(e.estimate(now + Duration::from_secs(3)), None, "{scenario}");
        }
    }

    #[test]
    fn sample_storage_is_bounded() {
        let now = Instant::now();
        let mut e = Estimator::default();
        for n in 0..100 {
            e.record(now + Duration::from_millis(n * 250), progress(n, n * 10));
        }
        assert_eq!(e.0.len(), MAX_SAMPLES);
    }
}

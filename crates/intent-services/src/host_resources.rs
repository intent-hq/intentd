//! The whole host's CPU and memory use (node-link.md "Resources"): what a
//! machine reports with its load, and what a backend shows as its own.
//!
//! One sampler serves the daemon. CPU use is a delta between two
//! refreshes, so the first sample has none; later ones are smoothed so a
//! one-tick spike does not swing the reading.

use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use intent_core::node_link::HostResources;

/// Weight of the newest CPU reading in the moving average.
const CPU_SMOOTHING: f32 = 0.5;

/// Samples the host. Not shared: [`sample_host`] holds the daemon's one.
pub struct HostSampler {
    system: sysinfo::System,
    cpu_refreshed: Option<Instant>,
    cpu_average: Option<f32>,
}

impl Default for HostSampler {
    fn default() -> Self {
        Self::new()
    }
}

impl HostSampler {
    #[must_use]
    pub fn new() -> Self {
        Self {
            system: sysinfo::System::new(),
            cpu_refreshed: None,
            cpu_average: None,
        }
    }

    /// The host now: memory always (when the platform reports it), CPU
    /// once two refreshes are far enough apart. A call sooner than the
    /// platform's minimum interval keeps the last CPU reading.
    pub fn sample(&mut self) -> Option<HostResources> {
        let now = Instant::now();
        let due = self
            .cpu_refreshed
            .is_none_or(|at| now.duration_since(at) >= sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        if due {
            self.system.refresh_cpu_usage();
            if self.cpu_refreshed.is_some() {
                self.cpu_average = Some(smooth(self.cpu_average, self.system.global_cpu_usage()));
            }
            self.cpu_refreshed = Some(now);
        }
        self.system.refresh_memory();
        let total = self.system.total_memory();
        (total > 0).then(|| HostResources {
            cpu_percent: self.cpu_average.map(percent),
            memory_used_bytes: self.system.used_memory().min(total),
            memory_total_bytes: total,
        })
    }
}

/// The daemon's host sample, from its one shared sampler.
#[must_use]
pub fn sample_host() -> Option<HostResources> {
    static SAMPLER: OnceLock<Mutex<HostSampler>> = OnceLock::new();
    SAMPLER
        .get_or_init(|| Mutex::new(HostSampler::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .sample()
}

/// The moving average after `reading` (a percentage).
fn smooth(average: Option<f32>, reading: f32) -> f32 {
    let reading = if reading.is_finite() {
        reading.clamp(0.0, 100.0)
    } else {
        0.0
    };
    average.map_or(reading, |average| {
        average + CPU_SMOOTHING * (reading - average)
    })
}

/// A whole percentage, 0-100.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "clamped to 0-100 first"
)]
fn percent(value: f32) -> u8 {
    value.round().clamp(0.0, 100.0) as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_average_follows_readings_and_stays_in_range() {
        assert!((smooth(None, 40.0) - 40.0).abs() < f32::EPSILON);
        assert!((smooth(Some(40.0), 80.0) - 60.0).abs() < f32::EPSILON);
        assert!((smooth(Some(10.0), 250.0) - 55.0).abs() < f32::EPSILON);
        assert!((smooth(Some(10.0), f32::NAN) - 5.0).abs() < f32::EPSILON);
        assert_eq!(percent(99.6), 100);
        assert_eq!(percent(-3.0), 0);
        assert_eq!(percent(34.4), 34);
    }

    /// Memory comes with the first sample; CPU only from the second one
    /// taken after the platform's minimum interval.
    #[test]
    fn cpu_comes_from_the_second_sample() {
        let mut sampler = HostSampler::new();
        let Some(first) = sampler.sample() else {
            eprintln!("skipping: this platform reports no memory");
            return;
        };
        assert!(first.memory_total_bytes > 0);
        assert!(first.memory_used_bytes <= first.memory_total_bytes);
        assert_eq!(first.cpu_percent, None, "one refresh has no delta");
        let soon = sampler.sample().unwrap();
        assert_eq!(soon.cpu_percent, None, "too soon for a second CPU refresh");
        // timing-guard: CPU use is a delta over the platform's minimum interval
        std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        let later = sampler.sample().unwrap();
        assert!(later.cpu_percent.is_some_and(|cpu| cpu <= 100), "{later:?}");
    }
}

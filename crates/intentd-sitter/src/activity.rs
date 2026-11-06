//! Activity flags for the update-check endpoint: `d`, `w`, `m`, `i`, `a`.
//!
//! Each flag says whether this is the first successful check of the UTC
//! day (`d`), ISO week (`w`, weeks start Monday) or calendar month (`m`),
//! whether it is the first successful check ever on a genuinely fresh
//! install (`i`), and how old the install is (`a`, a coarse bucket).
//!
//! Only dates live in `state.json` (`last_check_day`, `last_week`,
//! `last_month`, `first_seen_day`); nothing identifying is sent. The dates
//! advance only after a successful endpoint check ([`record_success`]), so
//! a failed check retries with the same flags.
//!
//! First-seen date ([`resolve_first_seen`]): a fresh install records
//! today. An install that already had sitter state when it first ran this
//! code is marked estimated: its date comes from the creation time of
//! `<data_dir>/sitter` (falling back to the oldest file mtime below it, see
//! [`estimate_first_seen`]), or stays unknown. Estimated installs never
//! send `i=1`.

use std::fmt;
use std::fs;
use std::path::Path;
use std::time::SystemTime;

use time::{Date, Month, OffsetDateTime, UtcOffset};

use crate::state::SitterState;

/// Install age bucket (`a`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgeBucket {
    Day0,
    Days1To7,
    Days8To30,
    Days31To90,
    Days91To365,
    Over365,
    Unknown,
}

impl AgeBucket {
    /// Wire value for `a`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Day0 => "0d",
            Self::Days1To7 => "1-7d",
            Self::Days8To30 => "8-30d",
            Self::Days31To90 => "31-90d",
            Self::Days91To365 => "91-365d",
            Self::Over365 => "365d+",
            Self::Unknown => "unknown",
        }
    }

    /// Bucket for an install `days` old. Negative ages (a clock that went
    /// backwards) are unknown.
    #[must_use]
    pub fn from_days(days: i64) -> Self {
        match days {
            0 => Self::Day0,
            1..=7 => Self::Days1To7,
            8..=30 => Self::Days8To30,
            31..=90 => Self::Days31To90,
            91..=365 => Self::Days91To365,
            366.. => Self::Over365,
            _ => Self::Unknown,
        }
    }
}

impl fmt::Display for AgeBucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Flags sent with one endpoint check.
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent wire flags, one per query param"
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Activity {
    /// `d`: first successful check of the UTC day.
    pub first_of_day: bool,
    /// `w`: first successful check of the UTC ISO week.
    pub first_of_week: bool,
    /// `m`: first successful check of the UTC calendar month.
    pub first_of_month: bool,
    /// `i`: first successful check ever on a fresh install.
    pub install: bool,
    /// `a`: install age.
    pub age: AgeBucket,
}

impl Activity {
    /// Whether a successful check with these flags changes stored dates.
    #[must_use]
    pub fn needs_record(self) -> bool {
        self.first_of_day || self.first_of_week || self.first_of_month || self.install
    }
}

fn utc_date(now: OffsetDateTime) -> Date {
    now.to_offset(UtcOffset::UTC).date()
}

/// `YYYY-MM-DD` for `date`.
#[must_use]
pub fn format_day(date: Date) -> String {
    format!(
        "{:04}-{:02}-{:02}",
        date.year(),
        u8::from(date.month()),
        date.day()
    )
}

/// Parse a `YYYY-MM-DD` day written by [`format_day`].
#[must_use]
pub fn parse_day(day: &str) -> Option<Date> {
    let mut parts = day.splitn(3, '-');
    let year = parts.next()?.parse().ok()?;
    let month = Month::try_from(parts.next()?.parse::<u8>().ok()?).ok()?;
    let dom = parts.next()?.parse().ok()?;
    Date::from_calendar_date(year, month, dom).ok()
}

/// UTC ISO week of `now` as `YYYY-Www` (ISO week-numbering year).
#[must_use]
pub fn utc_week(now: OffsetDateTime) -> String {
    let (year, week, _) = utc_date(now).to_iso_week_date();
    format!("{year:04}-W{week:02}")
}

/// UTC calendar month of `now` as `YYYY-MM`.
#[must_use]
pub fn utc_month(now: OffsetDateTime) -> String {
    let date = utc_date(now);
    format!("{:04}-{:02}", date.year(), u8::from(date.month()))
}

/// Age bucket of an install first seen on `first_seen_day`, at `now`.
#[must_use]
pub fn age_bucket(first_seen_day: Option<&str>, now: OffsetDateTime) -> AgeBucket {
    first_seen_day
        .and_then(parse_day)
        .map_or(AgeBucket::Unknown, |first| {
            AgeBucket::from_days((utc_date(now) - first).whole_days())
        })
}

/// Flags for a check at `now` given the stored `state`.
#[must_use]
pub fn activity(state: &SitterState, now: OffsetDateTime) -> Activity {
    Activity {
        first_of_day: crate::telemetry::is_first_check_of_day(state.last_check_day.as_deref(), now),
        first_of_week: state.last_week.as_deref() != Some(utc_week(now).as_str()),
        first_of_month: state.last_month.as_deref() != Some(utc_month(now).as_str()),
        install: !state.first_seen_estimated
            && state.first_seen_day.is_some()
            && state.last_week.is_none()
            && state.last_month.is_none(),
        age: age_bucket(state.first_seen_day.as_deref(), now),
    }
}

/// Advance the stored day/week/month after a successful check at `now`.
pub fn record_success(state: &mut SitterState, now: OffsetDateTime) {
    state.last_check_day = Some(format_day(utc_date(now)));
    state.last_week = Some(utc_week(now));
    state.last_month = Some(utc_month(now));
}

/// Record the first-seen date once. `has_prior_install` says the sitter
/// already had state before this code first ran (an upgrade); then the
/// date comes from `estimate` and the install is marked estimated, so it
/// never sends `i=1`. Returns whether `state` changed.
pub fn resolve_first_seen(
    state: &mut SitterState,
    now: OffsetDateTime,
    has_prior_install: bool,
    estimate: impl FnOnce() -> Option<Date>,
) -> bool {
    if state.first_seen_day.is_some() || state.first_seen_estimated {
        return false;
    }
    if has_prior_install {
        state.first_seen_estimated = true;
        state.first_seen_day = estimate().map(format_day);
    } else {
        state.first_seen_day = Some(format_day(utc_date(now)));
    }
    true
}

/// Whether `state` shows this sitter had already run before (an installed
/// daemon, or a recorded check).
#[must_use]
pub fn state_shows_prior_install(state: &SitterState) -> bool {
    state.current_version.is_some()
        || state.last_check_at.is_some()
        || state.last_check_day.is_some()
}

/// Deepest level [`estimate_first_seen`] scans for the oldest mtime.
const MTIME_SCAN_DEPTH: usize = 3;

/// Estimated first-seen date for an existing install: the creation time
/// of `sitter_dir` (birthtime where the platform has one), else the oldest
/// file mtime below it, else `None`.
#[must_use]
pub fn estimate_first_seen(sitter_dir: &Path) -> Option<Date> {
    let created = fs::metadata(sitter_dir).and_then(|m| m.created()).ok();
    created
        .or_else(|| oldest_mtime(sitter_dir, MTIME_SCAN_DEPTH))
        .map(|t| utc_date(OffsetDateTime::from(t)))
}

fn oldest_mtime(dir: &Path, depth: usize) -> Option<SystemTime> {
    let mut oldest: Option<SystemTime> = None;
    for entry in fs::read_dir(dir).ok()?.flatten() {
        let Ok(meta) = entry.metadata() else { continue };
        let candidate = if meta.is_dir() {
            if depth == 0 {
                None
            } else {
                oldest_mtime(&entry.path(), depth - 1)
            }
        } else {
            meta.modified().ok()
        };
        if let Some(t) = candidate {
            oldest = Some(oldest.map_or(t, |o| o.min(t)));
        }
    }
    oldest
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::{date, datetime};

    #[test]
    fn age_bucket_boundaries() {
        let cases = [
            (0, "0d"),
            (1, "1-7d"),
            (7, "1-7d"),
            (8, "8-30d"),
            (30, "8-30d"),
            (31, "31-90d"),
            (90, "31-90d"),
            (91, "91-365d"),
            (365, "91-365d"),
            (366, "365d+"),
            (-1, "unknown"),
        ];
        for (days, expected) in cases {
            assert_eq!(AgeBucket::from_days(days).as_str(), expected, "{days}");
        }
    }

    #[test]
    fn age_bucket_from_first_seen_day() {
        let now = datetime!(2026-10-22 08:00 UTC);
        assert_eq!(age_bucket(Some("2026-10-22"), now), AgeBucket::Day0);
        assert_eq!(age_bucket(Some("2026-10-21"), now), AgeBucket::Days1To7);
        assert_eq!(age_bucket(Some("2025-10-21"), now), AgeBucket::Over365);
        assert_eq!(age_bucket(Some("2025-10-22"), now), AgeBucket::Days91To365);
        assert_eq!(age_bucket(None, now), AgeBucket::Unknown);
        assert_eq!(age_bucket(Some("garbage"), now), AgeBucket::Unknown);
        assert_eq!(age_bucket(Some("2026-10-23"), now), AgeBucket::Unknown);
    }

    #[test]
    fn day_round_trips() {
        assert_eq!(parse_day("2026-01-05"), Some(date!(2026 - 01 - 05)));
        assert_eq!(format_day(date!(2026 - 01 - 05)), "2026-01-05");
        assert_eq!(parse_day("2026-13-01"), None);
        assert_eq!(parse_day("2026-02-30"), None);
    }

    #[test]
    fn iso_week_rolls_over_sunday_to_monday_utc() {
        // 2026-10-25 is a Sunday; 2026-10-26 a Monday.
        let sunday = datetime!(2026-10-25 23:59:59 UTC);
        let monday = datetime!(2026-10-26 00:00:00 UTC);
        assert_eq!(utc_week(sunday), "2026-W43");
        assert_eq!(utc_week(monday), "2026-W44");
        // Late Sunday at UTC-5 is already Monday in UTC.
        assert_eq!(utc_week(datetime!(2026-10-25 20:00 -5)), "2026-W44");

        let mut state = SitterState::default();
        record_success(&mut state, sunday);
        assert!(!activity(&state, datetime!(2026-10-25 12:00 UTC)).first_of_week);
        assert!(activity(&state, monday).first_of_week);
    }

    #[test]
    fn month_rolls_over_utc() {
        let end = datetime!(2026-10-31 23:59:59 UTC);
        let start = datetime!(2026-11-01 00:00:00 UTC);
        assert_eq!(utc_month(end), "2026-10");
        assert_eq!(utc_month(start), "2026-11");
        let mut state = SitterState::default();
        record_success(&mut state, end);
        assert!(!activity(&state, datetime!(2026-10-01 00:00 UTC)).first_of_month);
        let next = activity(&state, start);
        assert!(next.first_of_month && next.first_of_day && !next.first_of_week);
    }

    #[test]
    fn year_boundary_uses_iso_week_year() {
        // 2026-12-31 (Thu) and 2027-01-01 (Fri) share ISO week 2026-W53;
        // 2027-01-04 (Mon) starts 2027-W01.
        let dec31 = datetime!(2026-12-31 12:00 UTC);
        let jan1 = datetime!(2027-01-01 12:00 UTC);
        let jan4 = datetime!(2027-01-04 00:00 UTC);
        assert_eq!(utc_week(dec31), "2026-W53");
        assert_eq!(utc_week(jan1), "2026-W53");
        assert_eq!(utc_week(jan4), "2027-W01");
        // 2025-12-29 (Mon) is already in ISO week 2026-W01.
        assert_eq!(utc_week(datetime!(2025-12-29 00:00 UTC)), "2026-W01");

        let mut state = SitterState::default();
        record_success(&mut state, dec31);
        let at_jan1 = activity(&state, jan1);
        assert!(at_jan1.first_of_day && at_jan1.first_of_month && !at_jan1.first_of_week);
        assert!(activity(&state, jan4).first_of_week);
    }

    #[test]
    fn fresh_install_sends_install_exactly_once() {
        let now = datetime!(2026-10-22 08:00 UTC);
        let mut state = SitterState::default();
        assert!(!state_shows_prior_install(&state));
        assert!(resolve_first_seen(&mut state, now, false, || {
            panic!("a fresh install must not be estimated")
        }));
        assert_eq!(state.first_seen_day.as_deref(), Some("2026-10-22"));
        assert!(!state.first_seen_estimated);

        let first = activity(&state, now);
        assert_eq!(
            first,
            Activity {
                first_of_day: true,
                first_of_week: true,
                first_of_month: true,
                install: true,
                age: AgeBucket::Day0,
            }
        );
        record_success(&mut state, now);

        let later = datetime!(2026-10-22 09:00 UTC);
        assert!(!resolve_first_seen(&mut state, later, true, || None));
        let second = activity(&state, later);
        assert!(!second.install && !second.needs_record());
        // Never again, even in a new month a year on.
        let next_year = datetime!(2027-11-02 09:00 UTC);
        let much_later = activity(&state, next_year);
        assert!(!much_later.install);
        assert_eq!(much_later.age, AgeBucket::Over365);
    }

    #[test]
    fn upgraded_install_never_sends_install() {
        let now = datetime!(2026-10-22 08:00 UTC);
        let mut state = SitterState {
            current_version: Some("0.11.20".to_string()),
            last_check_day: Some("2026-10-21".to_string()),
            ..SitterState::default()
        };
        assert!(state_shows_prior_install(&state));
        assert!(resolve_first_seen(&mut state, now, true, || Some(date!(
            2026 - 09 - 01
        ))));
        assert!(state.first_seen_estimated);
        assert_eq!(state.first_seen_day.as_deref(), Some("2026-09-01"));
        let flags = activity(&state, now);
        assert!(!flags.install);
        assert_eq!(flags.age, AgeBucket::Days31To90);
        assert!(flags.first_of_week && flags.first_of_month && flags.first_of_day);
        record_success(&mut state, now);
        assert!(!activity(&state, datetime!(2026-11-02 00:00 UTC)).install);
    }

    #[test]
    fn upgraded_install_without_an_estimate_is_unknown_and_never_installs() {
        let now = datetime!(2026-10-22 08:00 UTC);
        let mut state = SitterState::default();
        assert!(resolve_first_seen(&mut state, now, true, || None));
        assert!(state.first_seen_estimated);
        assert_eq!(state.first_seen_day, None);
        let flags = activity(&state, now);
        assert!(!flags.install);
        assert_eq!(flags.age, AgeBucket::Unknown);
        // Not re-resolved later as a fresh install.
        assert!(!resolve_first_seen(&mut state, now, false, || None));
        assert!(!activity(&state, now).install);
    }

    #[test]
    fn failed_check_does_not_advance_flags() {
        let now = datetime!(2026-10-22 08:00 UTC);
        let mut state = SitterState::default();
        resolve_first_seen(&mut state, now, false, || None);
        let before = activity(&state, now);
        // A failed check never calls record_success: the next attempt
        // carries the same flags.
        let retry = activity(&state, datetime!(2026-10-22 09:00 UTC));
        assert_eq!(before, retry);
        assert!(retry.install && retry.first_of_week && retry.first_of_month);
    }

    #[test]
    fn estimate_uses_sitter_dir_times() {
        let dir = tempfile::tempdir().unwrap();
        let sitter = dir.path().join("sitter");
        fs::create_dir_all(sitter.join("versions/1.0.0")).unwrap();
        fs::write(sitter.join("versions/1.0.0/intentd"), b"bin").unwrap();
        let today = OffsetDateTime::now_utc().date();
        let estimate = estimate_first_seen(&sitter).unwrap();
        // Either the birthtime or the oldest mtime: both are today here.
        assert!((today - estimate).whole_days().abs() <= 1, "{estimate}");
        assert_eq!(estimate_first_seen(&dir.path().join("missing")), None);
    }

    #[test]
    fn oldest_mtime_picks_the_oldest_file() {
        let dir = tempfile::tempdir().unwrap();
        let old = dir.path().join("a/old");
        fs::create_dir_all(old.parent().unwrap()).unwrap();
        fs::write(&old, b"x").unwrap();
        fs::write(dir.path().join("new"), b"y").unwrap();
        let past = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        fs::File::options()
            .write(true)
            .open(&old)
            .unwrap()
            .set_modified(past)
            .unwrap();
        assert_eq!(oldest_mtime(dir.path(), MTIME_SCAN_DEPTH), Some(past));
        assert_eq!(oldest_mtime(&dir.path().join("missing"), 1), None);
    }
}

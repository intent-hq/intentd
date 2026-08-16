//! Parsing of free-form `estimatedEffort` strings into minutes for the batch
//! `agent.delegate` scheduling math (PROTOCOL §5.5). The field stays a
//! free-form string on the wire; this is a best-effort reader for common
//! shapes — `"30 min"`, `"2h"`, `"1.5 hours"`, `"~45m"`, `"1h 30m"`,
//! `"1-2h"` (ranges → midpoint), `"1d"` (an 8-hour workday). Anything it
//! cannot read parses to `None` and the scheduler falls back to
//! [`DEFAULT_EFFORT_MINUTES`].

/// Fallback used by the batch-delegate scheduler when a task carries no
/// parseable estimate: every task still contributes to critical-path
/// lengths, just with a neutral weight.
pub(crate) const DEFAULT_EFFORT_MINUTES: u64 = 30;

/// Ceiling on a single parsed estimate (a year of 8-hour workdays). The
/// field is free-form, so an absurd number would otherwise saturate the
/// float→u64 cast at `u64::MAX` and overflow the critical-path sums
/// downstream; anything at or beyond the cap clamps to it.
pub(crate) const MAX_EFFORT_MINUTES: u64 = 365 * 480;

#[derive(Debug, Clone, Copy, PartialEq)]
enum Unit {
    Minute,
    Hour,
    /// A workday: 8 hours, not 24 — effort estimates measure work, not
    /// elapsed time.
    Day,
}

impl Unit {
    fn minutes(self) -> f64 {
        match self {
            Unit::Minute => 1.0,
            Unit::Hour => 60.0,
            Unit::Day => 480.0,
        }
    }
}

fn parse_unit(word: &str) -> Option<Unit> {
    match word {
        "m" | "min" | "mins" | "minute" | "minutes" => Some(Unit::Minute),
        "h" | "hr" | "hrs" | "hour" | "hours" => Some(Unit::Hour),
        "d" | "day" | "days" => Some(Unit::Day),
        _ => None,
    }
}

/// Tokenize one side into `(number, optional unit)` segments: `"1h 30m"` →
/// `[(1, Hour), (30, Minute)]`, `"45"` → `[(45, None)]`. Any character that
/// is not part of a number, a known unit word, or whitespace fails the parse.
fn segments(s: &str) -> Option<Vec<(f64, Option<Unit>)>> {
    let chars: Vec<char> = s.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i].is_whitespace() {
            i += 1;
            continue;
        }
        if !(chars[i].is_ascii_digit() || chars[i] == '.') {
            return None;
        }
        let start = i;
        while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
            i += 1;
        }
        let number: f64 = chars[start..i].iter().collect::<String>().parse().ok()?;
        let mut j = i;
        while j < chars.len() && chars[j].is_whitespace() {
            j += 1;
        }
        let mut unit = None;
        if j < chars.len() && chars[j].is_alphabetic() {
            let ustart = j;
            while j < chars.len() && chars[j].is_alphabetic() {
                j += 1;
            }
            unit = Some(parse_unit(&chars[ustart..j].iter().collect::<String>())?);
            i = j;
        }
        out.push((number, unit));
    }
    (!out.is_empty()).then_some(out)
}

/// Sum one side's segments into minutes, returning the last explicit unit so
/// a range's bare left side can borrow it (`"1-2h"` → left `1` is hours). A
/// lone bare number falls back to `fallback_unit` (minutes by default); a
/// bare trailing segment after a united one reads as minutes (`"1h 30"`).
fn side_minutes(s: &str, fallback_unit: Option<Unit>) -> Option<(f64, Option<Unit>)> {
    let segs = segments(s)?;
    let mut total = 0.0;
    let mut last_unit = None;
    let count = segs.len();
    for (i, (number, unit)) in segs.into_iter().enumerate() {
        let unit = match unit {
            Some(u) => {
                last_unit = Some(u);
                u
            }
            None if count == 1 => fallback_unit.unwrap_or(Unit::Minute),
            None if i == count - 1 => Unit::Minute,
            None => return None,
        };
        total += number * unit.minutes();
    }
    Some((total, last_unit))
}

fn to_minutes(value: f64) -> Option<u64> {
    (value.is_finite() && value >= 0.0).then(|| (value.round() as u64).min(MAX_EFFORT_MINUTES))
}

/// Parse an `estimatedEffort` string into minutes. `~` prefixes are
/// stripped, ranges (`-`, `–`, `—`, or `" to "`) resolve to their midpoint,
/// hyphen-joined units (`"90-minute"`) read as plain amounts, and a bare
/// number reads as minutes. Unparseable input → `None` (callers use
/// [`DEFAULT_EFFORT_MINUTES`] for scheduling math).
pub(crate) fn parse_effort_minutes(raw: &str) -> Option<u64> {
    let s = raw
        .trim()
        .trim_start_matches('~')
        .trim()
        .to_ascii_lowercase();
    if s.is_empty() {
        return None;
    }
    // A separator that fails to parse as a range falls through rather than
    // failing the whole parse — "90-minute" is a joiner, not a range.
    for separator in ["–", "—", " to ", "-"] {
        if let Some((left, right)) = s.split_once(separator) {
            let range = side_minutes(right, None).and_then(|(right_minutes, right_unit)| {
                let (left_minutes, _) = side_minutes(left, right_unit)?;
                to_minutes((left_minutes + right_minutes) / 2.0)
            });
            if range.is_some() {
                return range;
            }
        }
    }
    if let Some((minutes, _)) = side_minutes(&s, None) {
        return to_minutes(minutes);
    }
    // Hyphen-joined unit: "90-minute", "1.5-hour" — read the hyphen as a
    // space when the right side is a unit word.
    let (left, right) = s.split_once('-')?;
    if !right.chars().next().is_some_and(|c| c.is_alphabetic()) {
        return None;
    }
    let (minutes, _) = side_minutes(&format!("{left} {right}"), None)?;
    to_minutes(minutes)
}

#[cfg(test)]
mod tests {
    use super::parse_effort_minutes;

    #[test]
    fn plain_minutes_and_hours() {
        assert_eq!(parse_effort_minutes("30 min"), Some(30));
        assert_eq!(parse_effort_minutes("30 minutes"), Some(30));
        assert_eq!(parse_effort_minutes("45m"), Some(45));
        assert_eq!(parse_effort_minutes("2h"), Some(120));
        assert_eq!(parse_effort_minutes("2 hours"), Some(120));
        assert_eq!(parse_effort_minutes("1 hr"), Some(60));
    }

    #[test]
    fn days_are_eight_hour_workdays() {
        assert_eq!(parse_effort_minutes("1d"), Some(480));
        assert_eq!(parse_effort_minutes("2 days"), Some(960));
        assert_eq!(parse_effort_minutes("0.5 day"), Some(240));
    }

    #[test]
    fn fractions_compounds_and_bare_numbers() {
        assert_eq!(parse_effort_minutes("1.5h"), Some(90));
        assert_eq!(parse_effort_minutes("1h 30m"), Some(90));
        assert_eq!(parse_effort_minutes("1h 30min"), Some(90));
        assert_eq!(parse_effort_minutes("1h 30"), Some(90));
        assert_eq!(parse_effort_minutes("45"), Some(45));
    }

    #[test]
    fn tilde_prefix_is_stripped() {
        assert_eq!(parse_effort_minutes("~30 min"), Some(30));
        assert_eq!(parse_effort_minutes("~ 2h"), Some(120));
        assert_eq!(parse_effort_minutes("  ~1d "), Some(480));
    }

    #[test]
    fn ranges_resolve_to_midpoint() {
        assert_eq!(parse_effort_minutes("1-2h"), Some(90));
        assert_eq!(parse_effort_minutes("30-60 min"), Some(45));
        assert_eq!(parse_effort_minutes("1h - 2h"), Some(90));
        assert_eq!(parse_effort_minutes("1 to 2 hours"), Some(90));
        assert_eq!(parse_effort_minutes("~1–2h"), Some(90));
        // Midpoints round to the nearest minute.
        assert_eq!(parse_effort_minutes("30-45 min"), Some(38));
    }

    #[test]
    fn hyphen_joined_units_read_as_plain_amounts() {
        assert_eq!(parse_effort_minutes("90-minute"), Some(90));
        assert_eq!(parse_effort_minutes("1.5-hour"), Some(90));
        assert_eq!(parse_effort_minutes("2-day"), Some(960));
        // A hyphen with a numeric right side is still a range.
        assert_eq!(parse_effort_minutes("1-2h"), Some(90));
    }

    #[test]
    fn absurd_estimates_clamp_to_the_cap() {
        use super::MAX_EFFORT_MINUTES;
        assert_eq!(
            parse_effort_minutes("99999999999999999999d"),
            Some(MAX_EFFORT_MINUTES)
        );
        assert_eq!(
            parse_effort_minutes("1e308"),
            None,
            "exponent shapes stay unparseable"
        );
        assert_eq!(parse_effort_minutes("365d"), Some(MAX_EFFORT_MINUTES));
        assert_eq!(parse_effort_minutes("364d"), Some(364 * 480));
    }

    #[test]
    fn unparseable_shapes_return_none() {
        assert_eq!(parse_effort_minutes(""), None);
        assert_eq!(parse_effort_minutes("   "), None);
        assert_eq!(parse_effort_minutes("a while"), None);
        assert_eq!(parse_effort_minutes("2 fortnights"), None);
        assert_eq!(parse_effort_minutes("soon-ish"), None);
        assert_eq!(parse_effort_minutes("h30"), None);
    }
}

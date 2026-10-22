//! Getting a conversation back to its home provider after a quota failover
//! (PROTOCOL §5.54 Backup failover, "returning to the original provider").
//!
//! A conversation's *home* is the configuration it ran on before its first
//! automatic switch. Leaving home for quota records a return-after time: the
//! reset time the quota error reports when one can be parsed, otherwise
//! [`FALLBACK_RETURN_AFTER`]. The first user message after that time starts
//! on home; if home accepts it the conversation is back (one
//! `provider_returned` notice, state cleared), and if home rejects it for
//! quota the turn fails over through the rotation as usual and the next
//! return-after is the newly reported reset time or a doubling backoff
//! ([`backoff_delay`]). A manual model/provider change makes the new choice
//! home and clears the state.
//!
//! The state lives in the session row's `metadata` JSON under
//! [`PROVIDER_RETURN_METADATA_KEY`] (internal: never projected to clients),
//! so it survives a daemon restart without a schema migration. Absent (or
//! JSON `null`) means the conversation is on its home.

use chrono::{DateTime, Duration, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use intent_core::{now_iso, AgentId, AgentSession, WorkspaceId};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::{Arc, LazyLock, Mutex};

use crate::provider_profiles;
use crate::Services;

/// Session `metadata` key holding [`ProviderReturnState`].
pub(crate) const PROVIDER_RETURN_METADATA_KEY: &str = "providerReturn";

/// `metadata.type` of the notice persisted when a conversation is back home.
pub(crate) const PROVIDER_RETURNED_NOTICE_TYPE: &str = "provider_returned";

/// `reason` of the `provider_returned` notice.
pub(crate) const QUOTA_RESET_REASON: &str = "quota-reset";

/// Return-after used when the quota error reports no parseable reset time.
pub(crate) const FALLBACK_RETURN_AFTER: Duration = Duration::hours(5);

/// First step of the backoff after a failed return.
const BACKOFF_BASE: Duration = Duration::hours(1);

/// Longest backoff after repeated failed returns.
const BACKOFF_CAP: Duration = Duration::hours(24);

/// A reported reset time further out than this is not trusted (a misparse);
/// the fallback or backoff applies instead.
const MAX_REPORTED_HORIZON: Duration = Duration::days(7);

/// Agents whose current message is a timed return home not yet announced:
/// the `provider_returned` row is written at home's first output, before it
/// (see [`Services::announce_provider_return_if_pending`]). In-memory: a
/// message restarted after a daemon restart re-arms it at its start.
pub(crate) type ProviderReturnAnnouncements = Arc<Mutex<HashSet<AgentId>>>;

/// How the conversation is going back home on the current message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ReturnMode {
    /// The return-after time passed and the message started on home: a
    /// `provider_returned` notice is written once home accepts it.
    Timer,
    /// The failover rotation reached home: its `provider_failover` row
    /// already announced the switch, so no further notice.
    Rotation,
}

/// Persisted per-conversation return state (see the module docs).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProviderReturnState {
    /// The configuration to return to (canonical selection).
    pub(crate) home: String,
    /// RFC 3339 instant after which the next user message starts on home.
    pub(crate) return_after: String,
    /// Failed returns so far (0 until home first rejects a return).
    #[serde(default)]
    pub(crate) backoff_step: u32,
    /// The configuration the conversation is on while away (the notice's
    /// `fromProvider` when it gets back).
    pub(crate) away_on: String,
    /// Set while the current message is going back home.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) returning: Option<ReturnMode>,
}

impl ProviderReturnState {
    fn return_after(&self) -> Option<DateTime<Utc>> {
        DateTime::parse_from_rfc3339(&self.return_after)
            .ok()
            .map(|t| t.with_timezone(&Utc))
    }
}

/// The session's return state, when away from home. A missing, `null` or
/// unreadable value reads as "on home".
pub(crate) fn read_state(session: &AgentSession) -> Option<ProviderReturnState> {
    read_state_from(session.metadata.as_ref())
}

/// [`read_state`] on a raw session `metadata` value.
pub(crate) fn read_state_from(metadata: Option<&Value>) -> Option<ProviderReturnState> {
    metadata?
        .get(PROVIDER_RETURN_METADATA_KEY)
        .filter(|v| !v.is_null())
        .and_then(|v| serde_json::from_value(v.clone()).ok())
}

/// The backoff after the `step`-th failed return (1-based): 1h, 2h, 4h, …
/// capped at 24h.
pub(crate) fn backoff_delay(step: u32) -> Duration {
    let doublings = step.saturating_sub(1).min(16);
    let delay = BACKOFF_BASE * (1_i32 << doublings);
    delay.min(BACKOFF_CAP)
}

/// The state after a quota switch `from` → `to` lands. `reported` is the
/// reset time parsed from the quota error, if any.
///
/// - On home (no state): home becomes `from`; return after the reported
///   reset, else the fallback.
/// - Leaving home again while away (home rejected a return): one more failed
///   return — the reported reset, else the next backoff step.
/// - The rotation reaching home: the conversation is going back
///   ([`ReturnMode::Rotation`]); the timer stays until home accepts.
/// - A hop between backups: only `awayOn` moves.
pub(crate) fn on_switch(
    state: Option<ProviderReturnState>,
    from: &str,
    to: &str,
    reported: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> ProviderReturnState {
    let from = provider_profiles::canonical_selection(from);
    let to = provider_profiles::canonical_selection(to);
    let reported = reported.filter(|t| *t > now && *t <= now + MAX_REPORTED_HORIZON);
    let Some(mut state) = state else {
        return ProviderReturnState {
            home: from,
            return_after: rfc3339(reported.unwrap_or(now + FALLBACK_RETURN_AFTER)),
            backoff_step: 0,
            away_on: to,
            returning: None,
        };
    };
    if to == state.home {
        state.away_on = from;
        state.returning = Some(ReturnMode::Rotation);
    } else if from == state.home {
        state.backoff_step = state.backoff_step.saturating_add(1);
        state.return_after = rfc3339(reported.unwrap_or(now + backoff_delay(state.backoff_step)));
        state.away_on = to;
        state.returning = None;
    } else {
        state.away_on = to;
        state.returning = None;
    }
    state
}

/// Whether a return is due at `now`: the state is away and its return-after
/// passed (an unreadable time counts as due, so the conversation is never
/// stranded away).
pub(crate) fn return_due(state: &ProviderReturnState, now: DateTime<Utc>) -> bool {
    state.return_after().is_none_or(|t| now >= t)
}

fn rfc3339(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

static ISO_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?)\s*(Z|[+-]\d{2}:?\d{2})?")
        .expect("valid regex")
});
static EPOCH_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\|\s*(\d{10})\b").expect("valid regex"));
static RELATIVE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(?:try again|retry|resets?|available again|again)\s+(?:in|after)\s+(\d+(?:\.\d+)?)\s*(seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d)\b",
    )
    .expect("valid regex")
});
static CLOCK_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\bresets?\s+(?:at\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s*(utc|gmt|z)?\b")
        .expect("valid regex")
});

/// The quota reset time a provider error reports, if one can be read.
/// Handles ISO-8601 timestamps (a missing offset reads as UTC), a Unix epoch
/// after `|` (`usage limit reached|1760000000`), relative phrasings ("try
/// again in 3 hours", "resets in 45 minutes") and clock times ("resets at
/// 5pm", "resets 3:30 PM", "resets at 17:00 UTC"; without a zone the
/// daemon's local time; a time already past today means tomorrow). Anything
/// else, or a time not in the future, is `None`.
pub(crate) fn parse_reset_time(text: &str, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    parse_iso(text)
        .or_else(|| parse_epoch(text))
        .or_else(|| parse_relative(text, now))
        .or_else(|| parse_clock(text, now))
        .filter(|t| *t > now)
}

fn parse_iso(text: &str) -> Option<DateTime<Utc>> {
    let caps = ISO_RE.captures(text)?;
    let date = NaiveDate::parse_from_str(&caps[1], "%Y-%m-%d").ok()?;
    let time = NaiveTime::parse_from_str(&caps[2], "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(&caps[2], "%H:%M"))
        .ok()?;
    let naive = NaiveDateTime::new(date, time);
    match caps.get(3).map(|m| m.as_str()) {
        None | Some("Z") => Some(Utc.from_utc_datetime(&naive)),
        Some(offset) => {
            let offset = offset.replace(':', "");
            let stamp = format!("{} {offset}", naive.format("%Y-%m-%dT%H:%M:%S%.f"));
            DateTime::parse_from_str(&stamp, "%Y-%m-%dT%H:%M:%S%.f %z")
                .ok()
                .map(|t| t.with_timezone(&Utc))
        }
    }
}

fn parse_epoch(text: &str) -> Option<DateTime<Utc>> {
    let secs: i64 = EPOCH_RE.captures(text)?[1].parse().ok()?;
    Utc.timestamp_opt(secs, 0).single()
}

fn parse_relative(text: &str, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let caps = RELATIVE_RE.captures(text)?;
    let amount: f64 = caps[1].parse().ok()?;
    let unit_secs = match caps[2].to_ascii_lowercase().chars().next()? {
        's' => 1.0,
        'm' => 60.0,
        'h' => 3_600.0,
        'd' => 86_400.0,
        _ => return None,
    };
    let secs = amount * unit_secs;
    // The horizon (7 days in seconds) is exactly representable as an f64.
    if !secs.is_finite() || secs <= 0.0 || secs > 604_800.0 {
        return None;
    }
    #[expect(clippy::cast_possible_truncation, reason = "bounded above")]
    Some(now + Duration::seconds(secs.round() as i64))
}

fn parse_clock(text: &str, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let caps = CLOCK_RE.captures(text)?;
    let mut hour: u32 = caps[1].parse().ok()?;
    let minute: u32 = caps.get(2).map_or(Some(0), |m| m.as_str().parse().ok())?;
    let meridiem = caps.get(3).map(|m| m.as_str().to_ascii_lowercase());
    // A bare number with neither minutes nor am/pm is not a clock time
    // ("resets 3 times a day").
    if caps.get(2).is_none() && meridiem.is_none() {
        return None;
    }
    match meridiem.as_deref() {
        Some("am") if hour == 12 => hour = 0,
        Some("pm") if hour < 12 => hour += 12,
        Some(_) if hour > 12 => return None,
        _ => {}
    }
    let time = NaiveTime::from_hms_opt(hour, minute, 0)?;
    let utc = caps.get(4).is_some();
    let at = |date: NaiveDate| -> Option<DateTime<Utc>> {
        let naive = NaiveDateTime::new(date, time);
        if utc {
            Some(Utc.from_utc_datetime(&naive))
        } else {
            Local
                .from_local_datetime(&naive)
                .earliest()
                .map(|t| t.with_timezone(&Utc))
        }
    };
    let today = if utc {
        now.date_naive()
    } else {
        now.with_timezone(&Local).date_naive()
    };
    let candidate = at(today)?;
    if candidate > now {
        Some(candidate)
    } else {
        at(today.succ_opt()?)
    }
}

impl Services {
    /// Persist (or with `None`, clear) the agent's return state.
    /// Best-effort: a failed write is logged.
    pub(crate) async fn persist_provider_return_state(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        state: Option<&ProviderReturnState>,
    ) {
        let value = state.map_or(Value::Null, |s| json!(s));
        if let Err(e) = self
            .store
            .set_agent_session_metadata_key_json(
                workspace_id,
                agent_id,
                PROVIDER_RETURN_METADATA_KEY,
                &value.to_string(),
                &now_iso(),
            )
            .await
        {
            tracing::warn!(agent = %agent_id, error = %e, "persisting the provider return state failed");
        }
    }

    /// A manual model/provider choice makes it the new home: drop any
    /// return state. A no-op write when there is none.
    pub(crate) async fn clear_provider_return_state(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
    ) {
        let away = self
            .store
            .get_agent_session(agent_id)
            .await
            .ok()
            .and_then(|s| read_state(&s))
            .is_some();
        if away {
            self.persist_provider_return_state(workspace_id, agent_id, None)
                .await;
        }
    }

    /// At the start of a user message: when the conversation is away and its
    /// return-after passed, start this message on home. A home that can no
    /// longer run (profile removed, base disabled) is given up — the current
    /// configuration becomes home. A concurrent provider change wins.
    pub(crate) async fn begin_provider_return(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
    ) {
        let Ok(session) = self.store.get_agent_session(agent_id).await else {
            return;
        };
        let Some(mut state) = read_state(&session) else {
            return;
        };
        if !return_due(&state, Utc::now()) {
            return;
        }
        let settings = self.effective_settings();
        if self
            .ensure_provider_ref_selectable_in(&state.home, &settings.providers)
            .is_err()
            || crate::agent_ops::provider_is_disabled(
                intent_providers::base_provider_id(&state.home),
                settings.providers.enabled.as_ref(),
            )
        {
            tracing::info!(agent = %agent_id, home = %state.home, "home provider no longer available; staying on the current one");
            self.persist_provider_return_state(workspace_id, agent_id, None)
                .await;
            return;
        }
        let Some(current) = session.provider.clone().filter(|p| !p.trim().is_empty()) else {
            return;
        };
        let current_selection = provider_profiles::canonical_selection(&current);
        if current_selection != state.home {
            let landed = self
                .store
                .failover_agent_session_provider(
                    workspace_id,
                    agent_id,
                    &current,
                    session.model.as_deref(),
                    &state.home,
                    &now_iso(),
                )
                .await
                .unwrap_or(false);
            if !landed {
                return;
            }
            state.away_on = current_selection;
            self.invalidate_agent_list_cache(workspace_id);
            self.publish_agent_mutation_event(
                workspace_id,
                agent_id,
                intent_core::events::AGENT_UPDATED,
                json!({
                    "agentId": agent_id.0,
                    "modelId": session.model.clone().filter(|m| !m.is_empty()),
                    "providerId": state.home,
                }),
            )
            .await;
            tracing::info!(agent = %agent_id, home = %state.home, "quota reset time passed; starting this message on the home provider");
        }
        state.returning = Some(ReturnMode::Timer);
        self.persist_provider_return_state(workspace_id, agent_id, Some(&state))
            .await;
        self.provider_return_announcements
            .lock()
            .unwrap()
            .insert(agent_id.clone());
    }

    /// Drop the pending announcement (a new message, a teardown, or home
    /// rejecting the return before any output — then no `provider_returned`
    /// row is written).
    pub(crate) fn cancel_provider_return_announcement(&self, agent_id: &AgentId) {
        self.provider_return_announcements
            .lock()
            .unwrap()
            .remove(agent_id);
    }

    /// Home produced its first output on a timed return: write the one
    /// `provider_returned` row now, ahead of that output (the turn's
    /// assistant row is persisted after it). A no-op unless a return is
    /// pending for this message.
    pub(crate) async fn announce_provider_return_if_pending(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
    ) {
        if !self
            .provider_return_announcements
            .lock()
            .unwrap()
            .remove(agent_id)
        {
            return;
        }
        let Ok(session) = self.store.get_agent_session(agent_id).await else {
            return;
        };
        let Some(state) = read_state(&session) else {
            return;
        };
        let on_home = session
            .provider
            .as_deref()
            .is_some_and(|p| provider_profiles::canonical_selection(p) == state.home);
        if state.returning != Some(ReturnMode::Timer) || !on_home {
            return;
        }
        self.persist_provider_returned_notice(
            workspace_id,
            agent_id,
            &state.away_on,
            &state.home,
            session.model.as_deref().filter(|m| !m.is_empty()),
        )
        .await;
    }

    /// After a turn completes: a conversation going back home is home, and
    /// the state is cleared. A timed return whose turn produced no output at
    /// all is announced now; a return through the rotation was already
    /// announced by its failover row.
    pub(crate) async fn complete_provider_return(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
    ) {
        self.announce_provider_return_if_pending(workspace_id, agent_id)
            .await;
        let Ok(session) = self.store.get_agent_session(agent_id).await else {
            return;
        };
        let Some(state) = read_state(&session) else {
            return;
        };
        let Some(mode) = state.returning else {
            return;
        };
        let on_home = session
            .provider
            .as_deref()
            .is_some_and(|p| provider_profiles::canonical_selection(p) == state.home);
        if !on_home {
            return;
        }
        tracing::debug!(agent = %agent_id, ?mode, "back on the home provider");
        self.persist_provider_return_state(workspace_id, agent_id, None)
            .await;
    }

    async fn persist_provider_returned_notice(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        from: &str,
        to: &str,
        model: Option<&str>,
    ) {
        let profiles = self.provider_profiles.profiles().ok();
        let label = |s: &str| {
            provider_profiles::readable_selection(s, profiles.as_deref().map(Vec::as_slice))
        };
        let content = json!([{
            "type": "text",
            "text": format!("Quota on {} has reset. Switched back from {}.", label(to), label(from)),
        }]);
        let metadata = json!({
            "type": PROVIDER_RETURNED_NOTICE_TYPE,
            "fromProvider": from,
            "toProvider": to,
            "model": model,
            "reason": QUOTA_RESET_REASON,
        });
        match self
            .store
            .append_agent_message_with_metadata(
                agent_id,
                "system",
                &content,
                Some(&metadata),
                &now_iso(),
            )
            .await
        {
            Ok(message) => {
                self.invalidate_agent_list_cache(workspace_id);
                self.publish_agent_message_events(workspace_id, agent_id, &message, None)
                    .await;
            }
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "failed to persist provider returned notice");
            }
        }
    }

    /// Whether the session's return state says the model-change hop
    /// `from` → `to` is a timed return in progress (its notice is the
    /// `provider_returned` row, written once home accepts the turn).
    pub(crate) async fn is_announced_provider_return(
        &self,
        agent_id: &AgentId,
        from: &str,
        to: &str,
    ) -> bool {
        let Ok(session) = self.store.get_agent_session(agent_id).await else {
            return false;
        };
        read_state(&session).is_some_and(|s| {
            s.returning == Some(ReturnMode::Timer)
                && s.home == provider_profiles::canonical_selection(to)
                && s.away_on == provider_profiles::canonical_selection(from)
        })
    }
}

#[cfg(test)]
#[path = "provider_return_tests.rs"]
mod provider_return_tests;

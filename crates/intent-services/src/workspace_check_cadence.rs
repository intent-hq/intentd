//! Idle policy shared by automatic forge checks. Only meaningful persisted
//! content and live agent work affect this clock, never polling metadata.

use std::collections::HashMap;

use intent_core::{Result, WorkspaceActivity, WorkspaceId};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

use crate::Services;

/// The configured active interval remains a floor for every idle tier.
/// Missing content uses creation time. An absent, malformed, or future clock
/// conservatively uses the active interval rather than delaying unknown work.
pub(crate) fn workspace_check_interval_secs(
    running: bool,
    content: Option<&str>,
    created: Option<&str>,
    now: OffsetDateTime,
    active_secs: u64,
) -> u64 {
    if running {
        return active_secs;
    }
    let Some(at) = content
        .or(created)
        .and_then(|stamp| OffsetDateTime::parse(stamp, &Rfc3339).ok())
    else {
        return active_secs;
    };
    let idle_secs = (now - at).whole_seconds();
    let tier = match idle_secs {
        86_400.. => 900,
        21_600.. => 600,
        3_600.. => 300,
        900.. => 120,
        _ => active_secs,
    };
    active_secs.max(tier)
}

impl Services {
    /// One bounded metadata projection for a set of interested workspaces.
    /// No per-monitor queries, transcript reads, or mutable polling clock.
    pub(crate) async fn workspace_automatic_check_intervals(
        &self,
        workspace_ids: &[WorkspaceId],
        now: OffsetDateTime,
    ) -> Result<HashMap<WorkspaceId, u64>> {
        let rows = self.store.workspace_content_clocks(workspace_ids).await?;
        let active_secs = self.pr_monitor_poll_interval().as_secs();
        Ok(workspace_ids
            .iter()
            .map(|id| {
                let clock = rows.get(id);
                let seconds = workspace_check_interval_secs(
                    self.workspace_activity(id) == WorkspaceActivity::AgentRunning,
                    clock.and_then(|c| c.last_content_activity.as_deref()),
                    clock.map(|c| c.created_at.as_str()),
                    now,
                    active_secs,
                );
                (id.clone(), seconds)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_tier_boundaries_and_running_work() {
        let now = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
        for (age, seconds) in [
            (0, 60),
            (899, 60),
            (900, 120),
            (3599, 120),
            (3600, 300),
            (21599, 300),
            (21600, 600),
            (86399, 600),
            (86400, 900),
            (864_000, 900),
        ] {
            let stamp = (now - time::Duration::seconds(age))
                .format(&Rfc3339)
                .unwrap();
            assert_eq!(
                workspace_check_interval_secs(false, Some(&stamp), None, now, 60),
                seconds,
                "age {age}"
            );
            assert_eq!(
                workspace_check_interval_secs(true, Some(&stamp), None, now, 60),
                60
            );
            assert_eq!(
                workspace_check_interval_secs(false, Some(&stamp), None, now, 1800),
                1800
            );
        }
        assert_eq!(
            workspace_check_interval_secs(false, None, None, now, 30),
            30
        );
    }

    #[test]
    fn content_clock_precedence_and_conservative_fallbacks() {
        let now = OffsetDateTime::parse("2026-10-04T12:00:00Z", &Rfc3339).unwrap();
        let old = "2026-10-01T12:00:00Z";
        let recent = "2026-10-04T12:00:00Z";
        for (content, created, expected) in [
            (None, Some(old), 900),
            (Some(recent), Some(old), 60),
            (Some(old), Some(recent), 900),
            (Some("bad"), Some(old), 60),
            (Some("2027-01-01T00:00:00Z"), Some(old), 60),
            (None, Some("bad"), 60),
            (None, Some("2027-01-01T00:00:00Z"), 60),
            (None, None, 60),
            (Some("2026-10-04T12:45:00+01:00"), Some(old), 120),
        ] {
            assert_eq!(
                workspace_check_interval_secs(false, content, created, now, 60),
                expected,
                "content {content:?}, created {created:?}"
            );
        }
    }
}

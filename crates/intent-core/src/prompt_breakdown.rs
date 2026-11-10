//! What Intent put into an agent's context at spawn, by section: the
//! `promptBreakdown` field of `agent.getSession` (PROTOCOL §5.5, agents.md
//! "promptBreakdown"). Recorded only for sessions admitted to the
//! staged experiment, and stored next to the
//! session's `systemPrompt` so the two always describe the same spawn.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The first harness version whose spawns record a breakdown.
pub const FIRST_BREAKDOWN_HARNESS_VERSION: (u32, u32) = (3, 10);

/// Recording belongs to the staged 3.10 experiment, not every numerically
/// later harness. Released 3.11 only corrects visual guidance and does not
/// promote prompt-breakdown recording. Admit future versions explicitly.
#[must_use]
pub fn harness_records_prompt_breakdown(harness_version: &str) -> bool {
    harness_version == "3.10"
}

/// Display estimate for `chars`: `ceil(chars / 4)`. Not a provider token
/// count.
#[must_use]
pub fn est_tokens(chars: u64) -> u64 {
    chars.div_ceil(4)
}

/// Length in UTF-16 code units: the JavaScript string length the client
/// sees, which is what every `chars` field counts.
#[must_use]
pub fn utf16_len(text: &str) -> u64 {
    text.encode_utf16().count() as u64
}

/// Who writes a section's text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SectionOwner {
    /// Text Intent writes: doctrine, the `ws.*` reference, machine lists.
    Intent,
    /// Text from the user's own files or settings: repo rules, skills, the
    /// specialist prompt.
    User,
}

/// Where a section's text reaches the agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SectionLocation {
    /// Part of the stored `systemPrompt`.
    SystemPrompt,
    /// Part of the `workspace_api` tool description.
    ToolDescription,
    /// Loaded by the provider itself; counts toward neither sum.
    Provider,
}

/// The control a section's hint points at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ControlHintKind {
    EditRepoRules,
    ChangeSpecialist,
    AgentFeatures,
    McpSettings,
}

/// A pointer to a real control that changes a section.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlHint {
    pub kind: ControlHintKind,
    pub text: String,
}

/// One row of the breakdown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptSection {
    pub id: String,
    pub label: String,
    pub owner: SectionOwner,
    pub location: SectionLocation,
    pub chars: u64,
    pub est_tokens: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_hint: Option<ControlHint>,
}

/// Why a skill was left out of the session's skills list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HiddenSkillReason {
    Compatibility,
    McpServerMissing,
    McpServerDisabled,
}

/// A skill the daemon left out, with the reason and the fix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HiddenSkill {
    pub name: String,
    pub reason: HiddenSkillReason,
    pub detail: String,
    pub fix: String,
}

/// A recorded breakdown: the `available: true` arm of the wire union.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptBreakdown {
    pub harness_version: String,
    pub recorded_at: String,
    pub system_prompt_chars: u64,
    pub tool_description_chars: u64,
    pub total_chars: u64,
    pub est_tokens: u64,
    pub sections: Vec<PromptSection>,
    pub hidden_skills: Vec<HiddenSkill>,
}

impl PromptBreakdown {
    /// Build a breakdown from its sections, deriving every total. Hidden
    /// skills are sorted by name.
    #[must_use]
    pub fn new(
        harness_version: &str,
        recorded_at: String,
        sections: Vec<PromptSection>,
        mut hidden_skills: Vec<HiddenSkill>,
    ) -> Self {
        let sum = |location| {
            sections
                .iter()
                .filter(|s| s.location == location)
                .map(|s| s.chars)
                .sum::<u64>()
        };
        let system_prompt_chars = sum(SectionLocation::SystemPrompt);
        let tool_description_chars = sum(SectionLocation::ToolDescription);
        let total_chars = system_prompt_chars + tool_description_chars;
        hidden_skills.sort_by(|a, b| a.name.cmp(&b.name));
        Self {
            harness_version: harness_version.to_string(),
            recorded_at,
            system_prompt_chars,
            tool_description_chars,
            total_chars,
            est_tokens: est_tokens(total_chars),
            sections,
            hidden_skills,
        }
    }
}

/// Why no breakdown is served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnavailableReason {
    /// The session's harness does not admit the breakdown experiment.
    HarnessPredatesBreakdown,
    /// An admitted session with no stored breakdown.
    NotRecorded,
}

impl UnavailableReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HarnessPredatesBreakdown => "harness-predates-breakdown",
            Self::NotRecorded => "not-recorded",
        }
    }
}

/// The wire `promptBreakdown` value for a session stamped
/// `harness_version` with `stored` read back from the store. A session outside
/// the experiment is always unavailable, whatever is stored.
#[must_use]
pub fn prompt_breakdown_wire(harness_version: &str, stored: Option<&PromptBreakdown>) -> Value {
    if !harness_records_prompt_breakdown(harness_version) {
        return unavailable(UnavailableReason::HarnessPredatesBreakdown);
    }
    match stored.and_then(|b| serde_json::to_value(b).ok()) {
        Some(Value::Object(mut map)) => {
            map.insert("available".to_string(), Value::Bool(true));
            Value::Object(map)
        }
        _ => unavailable(UnavailableReason::NotRecorded),
    }
}

fn unavailable(reason: UnavailableReason) -> Value {
    json!({ "available": false, "reason": reason.as_str() })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn section(id: &str, location: SectionLocation, chars: u64) -> PromptSection {
        PromptSection {
            id: id.to_string(),
            label: id.to_string(),
            owner: SectionOwner::Intent,
            location,
            chars,
            est_tokens: est_tokens(chars),
            control_hint: None,
        }
    }

    /// The first recording version, spelled from the constant so a
    /// harness bump never turns these fixtures into the live version.
    fn first() -> String {
        let (major, minor) = FIRST_BREAKDOWN_HARNESS_VERSION;
        format!("{major}.{minor}")
    }

    fn before_first() -> String {
        let (major, minor) = FIRST_BREAKDOWN_HARNESS_VERSION;
        format!("{major}.{}", minor - 1)
    }

    #[test]
    fn breakdown_is_opt_in_to_the_staged_experiment() {
        assert!(harness_records_prompt_breakdown(&first()));
        for version in ["3.9", "3.12", "4.0", "garbage", "3", " 3.10", "3.10 "] {
            assert!(!harness_records_prompt_breakdown(version), "{version}");
        }
        assert!(!harness_records_prompt_breakdown(
            crate::CURRENT_HARNESS_VERSION
        ));
    }

    #[test]
    fn totals_sum_system_prompt_and_tool_sections_only() {
        let b = PromptBreakdown::new(
            &first(),
            "2026-11-09T00:00:00Z".into(),
            vec![
                section("a", SectionLocation::SystemPrompt, 10),
                section("b", SectionLocation::SystemPrompt, 5),
                section("t", SectionLocation::ToolDescription, 7),
                section("p", SectionLocation::Provider, 1000),
            ],
            vec![],
        );
        assert_eq!(b.system_prompt_chars, 15);
        assert_eq!(b.tool_description_chars, 7);
        assert_eq!(b.total_chars, 22);
        assert_eq!(b.est_tokens, 6);
    }

    #[test]
    fn wire_union_by_version_and_storage() {
        let b = PromptBreakdown::new(
            &first(),
            "2026-11-09T00:00:00Z".into(),
            vec![section("a", SectionLocation::SystemPrompt, 3)],
            vec![HiddenSkill {
                name: "z".into(),
                reason: HiddenSkillReason::McpServerMissing,
                detail: "needs mcp__memory__".into(),
                fix: "Add it".into(),
            }],
        );
        let old = prompt_breakdown_wire(&before_first(), Some(&b));
        assert_eq!(
            old,
            json!({"available": false, "reason": "harness-predates-breakdown"})
        );
        let missing = prompt_breakdown_wire(&first(), None);
        assert_eq!(
            missing,
            json!({"available": false, "reason": "not-recorded"})
        );
        let wire = prompt_breakdown_wire(&first(), Some(&b));
        assert_eq!(wire["available"], json!(true));
        assert_eq!(wire["systemPromptChars"], json!(3));
        assert_eq!(wire["sections"][0]["location"], json!("systemPrompt"));
        assert!(wire["sections"][0].get("controlHint").is_none());
        assert_eq!(
            wire["hiddenSkills"][0]["reason"],
            json!("mcp-server-missing")
        );
    }

    #[test]
    fn chars_count_utf16_units() {
        assert_eq!(utf16_len("abc"), 3);
        assert_eq!(utf16_len("é"), 1);
        assert_eq!(utf16_len("😀"), 2);
    }
}

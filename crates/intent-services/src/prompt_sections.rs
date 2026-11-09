//! Per-section sizes of an agent's spawn context, for the `promptBreakdown`
//! field of `agent.getSession` (harness 3.10+). The prompt assembler tags
//! each layer it adds; this module measures the tagged layers against the
//! exact text that is stored, so the section sizes always add up to the
//! stored `systemPrompt` and the tool description.

use intent_core::prompt_breakdown::{
    est_tokens, utf16_len, ControlHint, ControlHintKind, HiddenSkill, PromptBreakdown,
    PromptSection, SectionLocation, SectionOwner,
};

/// What a prompt layer is, without its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LayerTag {
    pub id: &'static str,
    pub label: String,
    pub owner: SectionOwner,
    pub control_hint: Option<ControlHint>,
}

impl LayerTag {
    pub(crate) fn intent(id: &'static str, label: &str) -> Self {
        Self {
            id,
            label: label.to_string(),
            owner: SectionOwner::Intent,
            control_hint: None,
        }
    }

    pub(crate) fn user(id: &'static str, label: &str) -> Self {
        Self {
            owner: SectionOwner::User,
            ..Self::intent(id, label)
        }
    }

    #[must_use]
    pub(crate) fn hint(mut self, kind: ControlHintKind, text: impl Into<String>) -> Self {
        self.control_hint = Some(ControlHint {
            kind,
            text: text.into(),
        });
        self
    }
}

/// An assembled system prompt with its measured sections.
#[derive(Debug, Clone)]
pub(crate) struct AssembledPrompt {
    pub prompt: String,
    /// `systemPrompt` sections whose `chars` add up to the UTF-16 length of
    /// [`Self::prompt`].
    pub sections: Vec<PromptSection>,
    /// Skills the skills layer left out.
    pub hidden_skills: Vec<HiddenSkill>,
    /// `provider` sections: content the provider loads itself (repo rule
    /// files Intent skipped), counted in neither sum.
    pub provider_sections: Vec<PromptSection>,
}

impl AssembledPrompt {
    /// Measure `layers` against `joined`, the text the harness produced by
    /// joining their texts. Each separator is counted with the layer before
    /// it; any difference the join adds beyond that goes to the last layer,
    /// so the sum is exact whatever the join does.
    pub(crate) fn from_layers(
        layers: &[(LayerTag, String)],
        joined: String,
        hidden_skills: Vec<HiddenSkill>,
    ) -> Self {
        let lengths: Vec<u64> = layers.iter().map(|(_, text)| utf16_len(text)).collect();
        let total = utf16_len(&joined);
        let extra = total.saturating_sub(lengths.iter().sum());
        let gaps = (layers.len() as u64).saturating_sub(1);
        let separator = extra.checked_div(gaps).unwrap_or(0);
        let mut sections: Vec<PromptSection> = Vec::new();
        for (i, ((tag, _), len)) in layers.iter().zip(&lengths).enumerate() {
            let chars = if (i as u64) < gaps {
                len + separator
            } else {
                *len
            };
            push_section(&mut sections, tag, SectionLocation::SystemPrompt, chars);
        }
        let mut out = Self {
            prompt: joined,
            sections,
            hidden_skills,
            provider_sections: Vec::new(),
        };
        out.reconcile();
        out
    }

    /// Rewrite the prompt with `f` (an appended note, say) and count the
    /// growth as a section tagged `tag`.
    pub(crate) fn append(&mut self, tag: &LayerTag, f: impl FnOnce(String) -> String) {
        let before = utf16_len(&self.prompt);
        self.prompt = f(std::mem::take(&mut self.prompt));
        let after = utf16_len(&self.prompt);
        if after > before {
            push_section(
                &mut self.sections,
                tag,
                SectionLocation::SystemPrompt,
                after - before,
            );
        }
        self.reconcile();
    }

    /// Make the sections add up to the prompt exactly: a rewrite that also
    /// trimmed text (or grew it by nothing) takes the difference from, or
    /// gives it to, the last section.
    fn reconcile(&mut self) {
        let total = utf16_len(&self.prompt);
        let sum: u64 = self.sections.iter().map(|s| s.chars).sum();
        if sum == total {
            return;
        }
        if sum < total {
            if let Some(last) = self.sections.last_mut() {
                set_chars(last, last.chars + (total - sum));
            }
            return;
        }
        let mut excess = sum - total;
        for section in self.sections.iter_mut().rev() {
            let take = excess.min(section.chars);
            set_chars(section, section.chars - take);
            excess -= take;
            if excess == 0 {
                break;
            }
        }
        self.sections.retain(|s| s.chars > 0);
    }

    /// The breakdown of this prompt, its `provider` sections and
    /// `tool_sections` (the `workspace_api` tool description).
    pub(crate) fn breakdown(
        &self,
        harness_version: &str,
        recorded_at: String,
        tool_sections: Vec<PromptSection>,
    ) -> PromptBreakdown {
        let mut sections = self.sections.clone();
        sections.extend(tool_sections);
        sections.extend(self.provider_sections.iter().cloned());
        PromptBreakdown::new(
            harness_version,
            recorded_at,
            sections,
            self.hidden_skills.clone(),
        )
    }
}

fn set_chars(section: &mut PromptSection, chars: u64) {
    section.chars = chars;
    section.est_tokens = est_tokens(chars);
}

/// Add `chars` to the section `tag` names in `location`, creating it if
/// needed, so ids stay unique per location.
fn push_section(
    sections: &mut Vec<PromptSection>,
    tag: &LayerTag,
    location: SectionLocation,
    chars: u64,
) {
    if let Some(existing) = sections
        .iter_mut()
        .find(|s| s.id == tag.id && s.location == location)
    {
        set_chars(existing, existing.chars + chars);
        return;
    }
    sections.push(PromptSection {
        id: tag.id.to_string(),
        label: tag.label.clone(),
        owner: tag.owner,
        location,
        chars,
        est_tokens: est_tokens(chars),
        control_hint: tag.control_hint.clone(),
    });
}

/// The `toolDescription` sections of a `workspace_api` description: the
/// remote-machine list (when the description carries `placement_help`) and
/// the `ws.*` reference around it. They add up to the description's length.
pub(crate) fn tool_description_sections(
    description: &str,
    placement_help: Option<&str>,
    reference_in_prompt: bool,
) -> Vec<PromptSection> {
    let total = utf16_len(description);
    let machines = placement_help
        .map(str::trim)
        .filter(|help| !help.is_empty() && description.contains(*help))
        .map_or(0, utf16_len);
    let reference = if reference_in_prompt {
        LayerTag::intent("ws-tool-header", "Workspace API tool header")
    } else {
        LayerTag::intent("ws-reference", "Workspace API reference")
    };
    let mut sections = Vec::new();
    push_section(
        &mut sections,
        &reference,
        SectionLocation::ToolDescription,
        total - machines,
    );
    if machines > 0 {
        push_section(
            &mut sections,
            &LayerTag::intent("remote-machines", "Remote machines"),
            SectionLocation::ToolDescription,
            machines,
        );
    }
    sections.retain(|s| s.chars > 0);
    sections
}

/// A `provider` section: content the provider loads itself, sized
/// `chars`, counted in neither sum.
pub(crate) fn provider_section(tag: &LayerTag, chars: u64) -> PromptSection {
    let mut sections = Vec::new();
    push_section(&mut sections, tag, SectionLocation::Provider, chars);
    sections.remove(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layers(texts: &[(&'static str, &str)]) -> Vec<(LayerTag, String)> {
        texts
            .iter()
            .map(|(id, text)| (LayerTag::intent(id, id), (*text).to_string()))
            .collect()
    }

    fn sum(sections: &[PromptSection]) -> u64 {
        sections.iter().map(|s| s.chars).sum()
    }

    #[test]
    fn separators_count_with_the_layer_before_and_sum_is_exact() {
        let l = layers(&[("a", "aaa"), ("b", "bb"), ("c", "c")]);
        let texts: Vec<&str> = l.iter().map(|(_, t)| t.as_str()).collect();
        let joined = texts.join("\n\n---\n\n");
        let a = AssembledPrompt::from_layers(&l, joined.clone(), vec![]);
        let chars: Vec<u64> = a.sections.iter().map(|s| s.chars).collect();
        assert_eq!(chars, vec![3 + 7, 2 + 7, 1]);
        assert_eq!(sum(&a.sections), utf16_len(&joined));
    }

    #[test]
    fn same_id_layers_merge() {
        let l = layers(&[("a", "x"), ("a", "y")]);
        let a = AssembledPrompt::from_layers(&l, "x\ny".into(), vec![]);
        assert_eq!(a.sections.len(), 1);
        assert_eq!(a.sections[0].chars, 3);
    }

    #[test]
    fn append_counts_growth_and_reconciles_trims() {
        let l = layers(&[("a", "body   ")]);
        let mut a = AssembledPrompt::from_layers(&l, "body   ".into(), vec![]);
        a.append(&LayerTag::intent("note", "Note"), |p| {
            format!("{}\n\nN", p.trim_end())
        });
        assert_eq!(a.prompt, "body\n\nN");
        assert_eq!(sum(&a.sections), utf16_len(&a.prompt));
        let tight = AssembledPrompt::from_layers(&l, "body   ".into(), vec![]);
        let mut shrunk = tight;
        shrunk.append(&LayerTag::intent("note", "Note"), |p| {
            p.trim_end().to_string()
        });
        assert_eq!(sum(&shrunk.sections), 4);
    }

    #[test]
    fn tool_sections_split_out_the_machine_list() {
        let help = "Machines: m1, m2";
        let desc = format!("ws reference\n{help}\nmore");
        let s = tool_description_sections(&desc, Some(help), false);
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].id, "ws-reference");
        assert_eq!(s[1].id, "remote-machines");
        assert_eq!(s[1].chars, utf16_len(help));
        assert_eq!(sum(&s), utf16_len(&desc));
        let compact = tool_description_sections("header", None, true);
        assert_eq!(compact[0].id, "ws-tool-header");
        assert_eq!(compact[0].location, SectionLocation::ToolDescription);
    }

    /// For every provider and spawn shape, the sectioned assembly yields
    /// the exact prompt the plain assembly does, and the breakdown's sizes
    /// add up to that prompt and the bridge's tool description.
    #[tokio::test]
    async fn every_provider_breakdown_sums_to_prompt_and_tool_description() {
        use std::sync::Arc;

        use intent_acp::WorkspaceMcpServer;
        use intent_core::settings_file::AgentFeaturesSettings;
        use intent_core::{AgentId, WorkspaceApi};

        let (_db, dir, services, base) = crate::session_surface_goldens::fixture().await;
        let features = AgentFeaturesSettings::default();
        let specialist = crate::rules::SpecialistPromptInjection {
            behavior_prompt: Some("Be a careful reviewer.".into()),
            specialist_name: Some("Reviewer".into()),
            role_reminder: None,
        };
        let placement = "Remote placement (experimental): fixture machine list.";
        let mut session = base.clone();
        session.harness_version = intent_core::NEWEST_HARNESS_VERSION.into();
        let mut sub = session.clone();
        sub.parent_agent_id = Some(AgentId::from("agent-parent"));
        for provider in intent_providers::config::ACP_PROVIDERS {
            for (session, specialist, placement) in [
                (&session, None, None),
                (&sub, None, None),
                (&session, Some(&specialist), Some(placement)),
            ] {
                let is_sub_agent = session.parent_agent_id.is_some();
                let api: Arc<dyn WorkspaceApi> = Arc::new(services.clone());
                let server = crate::session_surfaces::SessionSurfaces::new(session, provider)
                    .configure(
                        WorkspaceMcpServer::for_agent_type(
                            api,
                            session.workspace_id.clone(),
                            "interactive",
                        )
                        .with_agent_features(features.clone())
                        .with_sub_agent(is_sub_agent)
                        .with_remote_placement_help(placement.map(str::to_owned)),
                    );
                let docs = server.workspace_api_prompt_reference();
                let plain = crate::rules::assemble_system_prompt(
                    services.store(),
                    Some(dir.path()),
                    "interactive",
                    specialist,
                    is_sub_agent,
                    false,
                    false,
                    &features,
                    None,
                    Some(session),
                    docs.as_deref(),
                )
                .await
                .expect("prompt");
                let mut assembled = crate::rules::assemble_system_prompt_sections(
                    services.store(),
                    Some(dir.path()),
                    "interactive",
                    specialist,
                    is_sub_agent,
                    false,
                    false,
                    &features,
                    None,
                    Some(session),
                    docs.as_deref(),
                    None,
                )
                .await
                .expect("prompt");
                assert_eq!(assembled.prompt, plain, "{}", provider.id);
                assembled.append(&crate::rules::provider_note_tag(), |prompt| {
                    crate::rules::with_provider_prompt_note(prompt, provider)
                });
                assert_eq!(
                    assembled.prompt,
                    crate::rules::with_provider_prompt_note(plain, provider)
                );
                let tool = server.workspace_api_tool_description();
                let b = assembled.breakdown(
                    intent_core::NEWEST_HARNESS_VERSION,
                    "t".into(),
                    tool_description_sections(
                        &tool,
                        server.remote_placement_help(),
                        docs.is_some(),
                    ),
                );
                let label = format!("{} sub={is_sub_agent}", provider.id);
                assert_eq!(
                    b.system_prompt_chars,
                    utf16_len(&assembled.prompt),
                    "{label}"
                );
                assert_eq!(b.tool_description_chars, utf16_len(&tool), "{label}");
                assert!(b.sections.iter().all(|s| s.chars > 0), "{label}");
                let has = |id: &str| b.sections.iter().any(|s| s.id == id);
                assert!(has("doctrine") && has("repo-rules"), "{label}");
                assert_eq!(has("suggested-next-steps"), !is_sub_agent, "{label}");
                if specialist.is_some() {
                    assert!(has("specialist-prompt") && has("role-reminder"), "{label}");
                }
                if placement.is_some_and(|help| tool.contains(help)) {
                    assert!(has("remote-machines"), "{label}");
                }
            }
        }
    }

    #[test]
    fn provider_sections_stay_out_of_both_sums() {
        let mut a = AssembledPrompt::from_layers(&layers(&[("a", "abcd")]), "abcd".into(), vec![]);
        a.provider_sections = vec![provider_section(
            &LayerTag::user("repo-rules", "Repo rules"),
            500,
        )];
        let b = a.breakdown(
            intent_core::NEWEST_HARNESS_VERSION,
            "t".into(),
            tool_description_sections("tool", None, false),
        );
        assert_eq!(b.system_prompt_chars, 4);
        assert_eq!(b.tool_description_chars, 4);
        assert_eq!(b.total_chars, 8);
        assert_eq!(b.sections.len(), 3);
    }
}

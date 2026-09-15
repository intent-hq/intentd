//! Harness v2.4 golden fixtures. This version keeps the v2.2 doctrine
//! (instructions + specialist bundle, exactly as v2.3 does) and extends
//! exactly one text surface: the `## Suggested Next Steps` prompt hint,
//! which now tells the model it may write markdown inline links
//! `[label](url)` inside a prompt so PR/issue references render clickable,
//! that bare `#N` is not linkified, and that the visible text must stay
//! short. Every other [`crate::harness::Harness`] surface forwards to v1
//! unchanged.

const NEXT_STEPS_OFF: &str = "## Suggested Next Steps\n\n\
    At the end of your response, offer the user clear next actions as a \
    `<!-- suggested-prompts ... -->` HTML comment block:\n\n\
    ```\n\
    <!-- suggested-prompts\n\
    Hold off on merging [#5034](https://github.com/owner/repo/pull/5034) until I have reviewed the diff.\n\
    Skip the verifier pass and open the PR now.\n\
    -->\n\
    ```\n\n\
    Write 2–4 prompts, each a short directive sentence phrased as \
    something the user might say next. Never suggest a step you already \
    said you will take — the user does not need to ask for it. Instead \
    give the user levers on your plan: a hold or constraint (\"Do not open \
    the PR even if the verifier approves\"), an alternative path, a scope \
    change, or a decision only they can make. You may write markdown \
    inline links `[label](url)` inside a prompt so PR and issue references \
    render as clickable links (e.g. \
    `[#5034](https://github.com/owner/repo/pull/5034)`); a bare `#5034` or \
    bare URL is not linkified. Keep each prompt's visible text (link \
    labels, not URLs) short — well under 200 characters — or the prompt \
    is dropped.";

const AUTO_COMMIT_CLAUSE: &str = " Auto-commit is enabled; do not include prompts about \
    committing or reviewing changes before committing.";

#[test]
fn current_harness_version_is_v2_4() {
    assert_eq!(intent_core::model::CURRENT_HARNESS_VERSION, "2.4");
    assert_eq!(
        crate::harness::resolve_entry(intent_core::model::CURRENT_HARNESS_VERSION).version,
        "2.4"
    );
}

/// Exact bytes of the extended block, both auto-commit variants. The
/// auto-commit clause is the v1 sentence verbatim; only the example lines
/// and the trailing guidance changed.
#[test]
fn golden_v2_4_suggested_next_steps_block() {
    let h = crate::harness::resolve_entry("2.4").harness;
    assert_eq!(h.suggested_next_steps_block(false), NEXT_STEPS_OFF);
    assert_eq!(
        h.suggested_next_steps_block(true),
        format!("{NEXT_STEPS_OFF}{AUTO_COMMIT_CLAUSE}")
    );
    let v1 = crate::harness::resolve_entry("1.0").harness;
    let v1_on = v1.suggested_next_steps_block(true);
    assert!(
        v1_on.ends_with(AUTO_COMMIT_CLAUSE),
        "auto-commit clause is identical to v1"
    );
    let v2_3 = crate::harness::resolve_entry("2.3").harness;
    assert!(!v2_3
        .suggested_next_steps_block(false)
        .contains("inline links"));
}

/// v2.4 is v2.3 plus the markdown-link guidance: the block starts with
/// v2.3's guidance sentences verbatim and appends the link sentences.
#[test]
fn v2_4_next_steps_extends_v2_3_text() {
    let v2_3 = crate::harness::resolve_entry("2.3").harness;
    let v2_4 = crate::harness::resolve_entry("2.4").harness;
    let v2_3_off = v2_3.suggested_next_steps_block(false);
    let v2_4_off = v2_4.suggested_next_steps_block(false);
    let (v2_3_head, v2_3_tail) = v2_3_off.split_once("```\n\n").expect("v2.3 example block");
    let (v2_4_head, v2_4_tail) = v2_4_off.split_once("```\n\n").expect("v2.4 example block");
    assert_ne!(v2_3_head, v2_4_head, "the example shows one link");
    assert!(v2_4_head.contains("[#5034](https://github.com/owner/repo/pull/5034)"));
    assert!(
        v2_4_tail.starts_with(v2_3_tail),
        "v2.4 keeps v2.3's guidance and appends the link sentences"
    );
    for auto_commit in [false, true] {
        assert_ne!(
            v2_3.suggested_next_steps_block(auto_commit),
            v2_4.suggested_next_steps_block(auto_commit)
        );
    }
}

/// v2.4 selects the v2.2 doctrine unchanged — the same instruction set and
/// the same specialist bundle v2.3 carries — so the v2.3→v2.4 diff is one
/// text surface.
#[test]
fn v2_4_registry_keeps_v2_3_doctrine() {
    let v2_2 = crate::harness::resolve_entry("2.2");
    let v2_3 = crate::harness::resolve_entry("2.3");
    let v2_4 = crate::harness::resolve_entry("2.4");
    assert!(std::ptr::eq(
        v2_4.doctrine.instructions,
        v2_3.doctrine.instructions
    ));
    assert!(std::ptr::eq(
        v2_4.doctrine.instructions,
        v2_2.doctrine.instructions
    ));
    assert_eq!(
        v2_4.doctrine.instructions.workspace,
        crate::instructions::V2_2.workspace
    );
    assert_eq!(v2_4.doctrine.specialists, v2_3.doctrine.specialists);
    assert_eq!(
        v2_4.doctrine.specialists,
        crate::specialists::EMBEDDED_BUNDLED_V2_1
    );
    assert_eq!((v2_3.default_features)(), (v2_4.default_features)());
    assert_eq!(v2_3.feature_labels, v2_4.feature_labels);
}

/// Every other prompt-layer surface v2.4 exposes is byte-identical to v2.3
/// (and therefore to v1, the harness every earlier row shares), so an
/// existing golden for those bytes stays valid across the bump.
#[test]
fn v2_4_forwards_other_prompt_surfaces_to_v1() {
    let v1 = crate::harness::resolve_entry("1.0").harness;
    let v2_3 = crate::harness::resolve_entry("2.3").harness;
    let v2_4 = crate::harness::resolve_entry("2.4").harness;
    let parts = vec!["a".to_string(), "b".to_string()];
    assert_eq!(
        v1.join_prompt_layers(&parts),
        v2_4.join_prompt_layers(&parts)
    );
    assert_eq!(
        v1.user_rules_wrapper("body", "src"),
        v2_4.user_rules_wrapper("body", "src")
    );
    assert_eq!(
        v1.specialist_role_section("Implement."),
        v2_4.specialist_role_section("Implement.")
    );
    assert_eq!(v1.commit_policy_clause(), v2_4.commit_policy_clause());
    assert_eq!(
        v1.role_reminder_footer("Implementor", Some("Stay in scope.")),
        v2_4.role_reminder_footer("Implementor", Some("Stay in scope."))
    );
    assert_eq!(v1.ask_questions_block(), v2_4.ask_questions_block());
    assert_eq!(v2_3.ask_questions_block(), v2_4.ask_questions_block());
    assert_eq!(
        v1.first_turn_prepend_block("go"),
        v2_4.first_turn_prepend_block("go")
    );
    assert_eq!(v1.coordinator_cow_hint(), v2_4.coordinator_cow_hint());
    assert_eq!(
        v1.sandboxed_implementor_hint("/sb", "sb/x"),
        v2_4.sandboxed_implementor_hint("/sb", "sb/x")
    );
    assert_eq!(
        v1.delegation_first_message(Some("body"), "Title", "note-1"),
        v2_4.delegation_first_message(Some("body"), "Title", "note-1")
    );
    assert_eq!(
        v1.idle_timeout_warning("30 minutes"),
        v2_4.idle_timeout_warning("30 minutes")
    );
}

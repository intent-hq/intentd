//! Harness v2.3 golden fixtures. This version keeps the v2.2 doctrine
//! (instructions + specialist bundle) and rewords exactly one text surface:
//! the `## Suggested Next Steps` prompt hint, so suggested prompts become
//! levers on the agent's plan (holds, alternatives, scope changes,
//! decisions) instead of restating steps the agent already committed to.
//! Every other [`crate::harness::Harness`] surface forwards to v1 unchanged.

const NEXT_STEPS_OFF: &str = "## Suggested Next Steps\n\n\
    At the end of your response, offer the user clear next actions as a \
    `<!-- suggested-prompts ... -->` HTML comment block:\n\n\
    ```\n\
    <!-- suggested-prompts\n\
    Hold off on opening the PR until I have reviewed the diff.\n\
    Skip the verifier pass and open the PR now.\n\
    -->\n\
    ```\n\n\
    Write 2–4 prompts, each a short directive sentence phrased as \
    something the user might say next. Never suggest a step you already \
    said you will take — the user does not need to ask for it. Instead \
    give the user levers on your plan: a hold or constraint (\"Do not open \
    the PR even if the verifier approves\"), an alternative path, a scope \
    change, or a decision only they can make.";

const AUTO_COMMIT_CLAUSE: &str = " Auto-commit is enabled; do not include prompts about \
    committing or reviewing changes before committing.";

#[test]
fn current_harness_version_is_v2_3() {
    assert_eq!(intent_core::model::CURRENT_HARNESS_VERSION, "2.3");
    assert_eq!(
        crate::harness::resolve_entry(intent_core::model::CURRENT_HARNESS_VERSION).version,
        "2.3"
    );
}

/// Exact bytes of the reworded block, both auto-commit variants. The
/// auto-commit clause is the v1 sentence verbatim; only the example lines
/// and the trailing guidance changed.
#[test]
fn golden_v2_3_suggested_next_steps_block() {
    let h = crate::harness::resolve_entry("2.3").harness;
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
    assert!(!v1
        .suggested_next_steps_block(false)
        .contains("Never suggest"));
}

/// v2.3 selects the v2.2 doctrine unchanged: the same instruction set and
/// the same specialist bundle, so the v2.2→v2.3 diff is one text surface.
#[test]
fn v2_3_registry_keeps_v2_2_doctrine() {
    let v2_2 = crate::harness::resolve_entry("2.2");
    let v2_3 = crate::harness::resolve_entry("2.3");
    assert!(std::ptr::eq(
        v2_3.doctrine.instructions,
        v2_2.doctrine.instructions
    ));
    assert_eq!(
        v2_3.doctrine.instructions.workspace,
        crate::instructions::V2_2.workspace
    );
    assert_eq!(v2_3.doctrine.specialists, v2_2.doctrine.specialists);
    assert_eq!(
        v2_3.doctrine.specialists,
        crate::specialists::EMBEDDED_BUNDLED_V2_1
    );
    assert_eq!((v2_2.default_features)(), (v2_3.default_features)());
    assert_eq!(v2_2.feature_labels, v2_3.feature_labels);
}

/// Every other prompt-layer surface v2.3 exposes is byte-identical to v1
/// (the harness every earlier row shares), so an existing golden for those
/// bytes stays valid across the bump.
#[test]
fn v2_3_forwards_other_prompt_surfaces_to_v1() {
    let v1 = crate::harness::resolve_entry("1.0").harness;
    let v2_3 = crate::harness::resolve_entry("2.3").harness;
    let parts = vec!["a".to_string(), "b".to_string()];
    assert_eq!(
        v1.join_prompt_layers(&parts),
        v2_3.join_prompt_layers(&parts)
    );
    assert_eq!(
        v1.user_rules_wrapper("body", "src"),
        v2_3.user_rules_wrapper("body", "src")
    );
    assert_eq!(
        v1.specialist_role_section("Implement."),
        v2_3.specialist_role_section("Implement.")
    );
    assert_eq!(v1.commit_policy_clause(), v2_3.commit_policy_clause());
    assert_eq!(
        v1.role_reminder_footer("Implementor", Some("Stay in scope.")),
        v2_3.role_reminder_footer("Implementor", Some("Stay in scope."))
    );
    assert_eq!(v1.ask_questions_block(), v2_3.ask_questions_block());
    assert_eq!(
        v1.first_turn_prepend_block("go"),
        v2_3.first_turn_prepend_block("go")
    );
    assert_eq!(v1.coordinator_cow_hint(), v2_3.coordinator_cow_hint());
    assert_eq!(
        v1.sandboxed_implementor_hint("/sb", "sb/x"),
        v2_3.sandboxed_implementor_hint("/sb", "sb/x")
    );
    assert_eq!(
        v1.delegation_first_message(Some("body"), "Title", "note-1"),
        v2_3.delegation_first_message(Some("body"), "Title", "note-1")
    );
    assert_eq!(
        v1.idle_timeout_warning("30 minutes"),
        v2_3.idle_timeout_warning("30 minutes")
    );
}

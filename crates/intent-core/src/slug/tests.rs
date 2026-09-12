use super::*;

fn assert_slug_shape(slug: &str) {
    let parts: Vec<&str> = slug.split('-').collect();
    assert_eq!(parts.len(), 2, "slug '{slug}' must be word-word");
    for part in parts {
        assert!(
            is_valid_slug_word(part),
            "slug word '{part}' must be 2-15 lowercase letters"
        );
    }
}

#[test]
fn random_slug_is_adjective_animal_shaped() {
    for _ in 0..50 {
        assert_slug_shape(&generate_workspace_slug());
    }
}

#[test]
fn action_noun_prompt_reverses_to_noun_action() {
    assert_eq!(
        extract_local_slug("fix the auth flow").as_deref(),
        Some("auth-fix")
    );
    assert_eq!(
        extract_local_slug("add dark mode").as_deref(),
        Some("dark-add")
    );
}

#[test]
fn noun_action_prompt_keeps_order() {
    assert_eq!(
        extract_local_slug("auth refactor").as_deref(),
        Some("auth-refactor")
    );
}

#[test]
fn falls_back_to_first_two_meaningful_words() {
    assert_eq!(
        extract_local_slug("dashboard chart rendering looks wrong").as_deref(),
        Some("dashboard-chart")
    );
}

#[test]
fn single_word_gets_task_suffix() {
    assert_eq!(
        extract_local_slug("authentication").as_deref(),
        Some("authentication-task")
    );
}

#[test]
fn stop_words_and_short_prompts_are_rejected() {
    assert_eq!(extract_local_slug(""), None);
    assert_eq!(extract_local_slug("do it"), None);
    assert_eq!(extract_local_slug("the and or"), None);
}

#[test]
fn context_mentions_are_stripped() {
    assert_eq!(
        extract_local_slug("fix auth @file[src/auth.rs] @context[some stuff]").as_deref(),
        Some("auth-fix")
    );
}

#[test]
fn mixed_case_context_mentions_are_stripped() {
    assert_eq!(
        extract_local_slug("fix auth @File[src/auth.rs] @CONTEXT[some stuff]").as_deref(),
        Some("auth-fix")
    );
}

/// Regression (intent-hq/intent#4801): an `@` followed by text containing a
/// multi-byte char within the first `kind.len()` bytes must not panic on a
/// non-char-boundary slice in the case-insensitive kind comparison.
#[test]
fn at_followed_by_multibyte_char_does_not_panic() {
    // Em dash (3 bytes) at byte offset 4: the 5/6-byte kinds cut inside it.
    assert_eq!(
        extract_local_slug("@foo — bar baz").as_deref(),
        Some("foo-bar")
    );
    // 4-byte char as the very first char after `@`: the 3-byte kind cuts inside it.
    assert_eq!(
        extract_local_slug("@🎉 launch party").as_deref(),
        Some("launch-party")
    );
    // Short tails shorter than every kind are skipped without slicing.
    assert_eq!(extract_local_slug("@—"), None);
    assert_eq!(extract_local_slug("@é"), None);
    assert_eq!(
        extract_local_slug("@é fix auth").as_deref(),
        Some("auth-fix")
    );
    // A kind spelled with a non-ASCII char right after it is not a mention
    // and must not panic on the longer kinds' prefix check either.
    assert_eq!(
        extract_local_slug("@fileé[x] fix auth").as_deref(),
        Some("auth-fix")
    );
}

#[test]
fn numbers_and_long_words_are_filtered() {
    // Words with digits or >15 chars never enter the slug.
    assert_eq!(
        extract_local_slug("fix bug123 authentication flow").as_deref(),
        Some("authentication-fix")
    );
}

#[test]
fn suffix_helpers_round_trip() {
    assert_eq!(append_slug_suffix("auth-fix", 2), "auth-fix-2");
    assert_eq!(extract_base_slug("auth-fix-2"), "auth-fix");
    assert_eq!(extract_base_slug("auth-fix"), "auth-fix");
    // Non-slug shapes are returned unchanged.
    assert_eq!(extract_base_slug("feature/foo-2"), "feature/foo-2");
    assert_eq!(
        extract_base_slug("eeb596bd-85cd-4771-813c-bc38db13329b"),
        "eeb596bd-85cd-4771-813c-bc38db13329b"
    );
}

#[test]
fn is_workspace_slug_matches_dictionary_pairs() {
    assert!(is_workspace_slug("amber-fox"));
    assert!(is_workspace_slug("amber-fox-2"));
    assert!(!is_workspace_slug("amber"));
    assert!(!is_workspace_slug(""));
    // Custom human titles are not slugs.
    assert!(!is_workspace_slug("Add dark mode support"));
    assert!(!is_workspace_slug("Refactor auth"));
    // Prompt-derived word-word pairs are not dictionary slugs.
    assert!(!is_workspace_slug("auth-fix"));
    assert!(!is_workspace_slug("auth-fix-2"));
}

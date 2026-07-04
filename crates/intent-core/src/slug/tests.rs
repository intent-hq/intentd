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

//! Assertions about the node-link protocol two peers agreed on.

/// Asserts that a link which agreed on protocol `agreed` carries a feature
/// first added in protocol `feature_min` (a `*_PROTOCOL` constant such as
/// `intent_core::node_link::HOMES_PROTOCOL`).
///
/// A dial against this build agrees on the highest version both peers speak,
/// which rises with every protocol bump. A test about an older feature that
/// asserts `agreed == FEATURE_PROTOCOL` therefore breaks on the next bump
/// (intentd#3662 raised the range to 20 and broke two homes tests pinned to
/// 19). Assert the feature's floor with this helper instead.
/// `link_protocol_exact_lint` flags the exact-match form.
///
/// # Panics
///
/// When `agreed` is below `feature_min`.
#[track_caller]
pub fn assert_link_supports(agreed: u32, feature_min: u32) {
    assert!(
        agreed >= feature_min,
        "the link agreed on protocol {agreed}, below the feature's first protocol {feature_min}"
    );
}

#[cfg(test)]
mod tests {
    use super::assert_link_supports;

    #[test]
    fn a_link_at_or_above_the_feature_protocol_supports_it() {
        assert_link_supports(19, 19);
        assert_link_supports(20, 19);
    }

    #[test]
    #[should_panic(
        expected = "the link agreed on protocol 18, below the feature's first protocol 19"
    )]
    fn a_link_below_the_feature_protocol_fails() {
        assert_link_supports(18, 19);
    }
}

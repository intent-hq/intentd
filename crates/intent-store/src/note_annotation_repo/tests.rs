use super::*;

#[test]
fn interval_admission_preserves_disjoint_ranges_and_empty_sets() {
    assert!(validate_ranges(&[]).is_ok());
    assert!(validate_ranges(&[
        SourceRange { start: 0, end: 2 },
        SourceRange { start: 3, end: 4 }
    ])
    .is_ok());
    for ranges in [
        vec![SourceRange { start: -1, end: 2 }],
        vec![SourceRange { start: 2, end: 2 }],
        vec![
            SourceRange { start: 0, end: 2 },
            SourceRange { start: 2, end: 4 },
        ],
        vec![
            SourceRange { start: 3, end: 4 },
            SourceRange { start: 0, end: 2 },
        ],
        vec![SourceRange {
            start: 0,
            end: MAX_OFFSET + 1,
        }],
        vec![SourceRange { start: 0, end: 1 }; MAX_RANGES + 1],
    ] {
        assert!(validate_ranges(&ranges).is_err());
    }
    assert!(validate_limit(0).is_err());
    assert!(validate_limit(MAX_ITEMS + 1).is_err());
}

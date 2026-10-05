use super::*;

struct Vector {
    name: &'static str,
    source: &'static str,
    query: &'static str,
    ranges: Option<&'static [(u64, u64)]>,
    expected: &'static [(u64, u64)],
}

// Controlled vectors copied from the approved source-search.json specification.
// Fixture SHA-256: c819e5fa7f6f65aa0bf28a4ea23b0513b9108847d3e25240d047a1ee159d8328
// They establish pure matcher semantics, not Store/cursor/runtime support.
const VECTORS: &[Vector] = &[
    Vector {
        name: "ascii",
        source: "AbCaBC",
        query: "abc",
        ranges: None,
        expected: &[(0, 3), (3, 6)],
    },
    Vector {
        name: "overlap",
        source: "banana",
        query: "ana",
        ranges: None,
        expected: &[(1, 4), (3, 6)],
    },
    Vector {
        name: "dense-overlap",
        source: "aaaa",
        query: "aa",
        ranges: None,
        expected: &[(0, 2), (1, 3), (2, 4)],
    },
    Vector {
        name: "sharp-s",
        source: "Straße",
        query: "STRASSE",
        ranges: None,
        expected: &[(0, 6)],
    },
    Vector {
        name: "whole-expansion",
        source: "ß",
        query: "ss",
        ranges: None,
        expected: &[(0, 1)],
    },
    Vector {
        name: "partial-expansion",
        source: "ß",
        query: "s",
        ranges: None,
        expected: &[],
    },
    Vector {
        name: "mixed-expansion",
        source: "ßss",
        query: "ss",
        ranges: None,
        expected: &[(0, 1), (1, 3)],
    },
    Vector {
        name: "reverse-expansion",
        source: "SS",
        query: "ß",
        ranges: None,
        expected: &[(0, 2)],
    },
    Vector {
        name: "dotted-i-partial",
        source: "İ",
        query: "i",
        ranges: None,
        expected: &[],
    },
    Vector {
        name: "dotted-i-full",
        source: "İ",
        query: "i̇",
        ranges: None,
        expected: &[(0, 1)],
    },
    Vector {
        name: "non-turkic",
        source: "Iıi",
        query: "i",
        ranges: None,
        expected: &[(0, 1), (2, 3)],
    },
    Vector {
        name: "sigma",
        source: "Σςσ",
        query: "σ",
        ranges: None,
        expected: &[(0, 1), (1, 2), (2, 3)],
    },
    Vector {
        name: "no-normalization",
        source: "é",
        query: "é",
        ranges: None,
        expected: &[],
    },
    Vector {
        name: "combining-literal",
        source: "é",
        query: "́",
        ranges: None,
        expected: &[(1, 2)],
    },
    Vector {
        name: "astral-offset",
        source: "😀Straße",
        query: "STRASSE",
        ranges: None,
        expected: &[(2, 8)],
    },
    Vector {
        name: "astral-case",
        source: "𐐀x𐐨",
        query: "𐐨",
        ranges: None,
        expected: &[(0, 2), (3, 5)],
    },
    Vector {
        name: "ligature",
        source: "ﬃ",
        query: "ffi",
        ranges: None,
        expected: &[(0, 1)],
    },
    Vector {
        name: "ligature-partial",
        source: "ﬃ",
        query: "fi",
        ranges: None,
        expected: &[],
    },
    Vector {
        name: "literal-metacharacters",
        source: "a.*b",
        query: ".*",
        ranges: None,
        expected: &[(1, 3)],
    },
    Vector {
        name: "whitespace",
        source: " a a ",
        query: " ",
        ranges: None,
        expected: &[(0, 1), (2, 3), (4, 5)],
    },
    Vector {
        name: "empty-source",
        source: "",
        query: "a",
        ranges: None,
        expected: &[],
    },
    Vector {
        name: "range-gap",
        source: "banana",
        query: "ana",
        ranges: Some(&[(1, 3), (4, 6)]),
        expected: &[],
    },
    Vector {
        name: "range-touch",
        source: "banana",
        query: "ana",
        ranges: Some(&[(1, 3), (3, 6)]),
        expected: &[(1, 4), (3, 6)],
    },
    Vector {
        name: "range-overlap-dedup",
        source: "banana",
        query: "ana",
        ranges: Some(&[(3, 6), (1, 5), (1, 5)]),
        expected: &[(1, 4), (3, 6)],
    },
    Vector {
        name: "empty-selection",
        source: "banana",
        query: "ana",
        ranges: Some(&[]),
        expected: &[],
    },
];

// Selection normalization is a Store responsibility. This test-only driver
// exercises the approved union vectors without moving that policy into KMP.
fn union(vector: &Vector) -> Vec<(u64, u64)> {
    let length = vector.source.encode_utf16().count() as u64;
    let mut ranges = vector
        .ranges
        .map_or_else(|| vec![(0, length)], <[_]>::to_vec);
    ranges.sort_unstable();
    let mut result: Vec<(u64, u64)> = Vec::new();
    for (start, end) in ranges {
        if start == end {
            continue;
        }
        if let Some(previous) = result.last_mut().filter(|previous| start <= previous.1) {
            previous.1 = previous.1.max(end);
        } else {
            result.push((start, end));
        }
    }
    result
}

fn assert_vectors(restore_every_scalar: bool) {
    for vector in VECTORS {
        let mut search = NoteStageSearch::new(vector.query).unwrap();
        let mut hits = Vec::new();
        for (start, end) in union(vector) {
            search.reset_at_gap(start).unwrap();
            let mut offset = 0;
            for scalar in vector.source.chars() {
                if start <= offset && offset < end {
                    if let Some(hit) = search.push_scalar(scalar, offset).unwrap() {
                        hits.push((hit.start, hit.end));
                    }
                    if restore_every_scalar {
                        // A Store page can end after any scalar, including a hit
                        // or an expanding scalar. Serialize the actual carry.
                        let snapshot = search.snapshot();
                        let encoded = serde_json::to_string(&snapshot).unwrap();
                        let decoded = serde_json::from_str(&encoded).unwrap();
                        search = NoteStageSearch::restore(vector.query, &decoded).unwrap();
                        assert_eq!(search.snapshot(), snapshot, "{}", vector.name);
                    }
                }
                offset += scalar.len_utf16() as u64;
            }
        }
        assert_eq!(hits, vector.expected, "{}", vector.name);
    }
}

#[test]
fn all_approved_source_search_vectors() {
    assert_vectors(false);
}

#[test]
fn every_scalar_page_boundary_preserves_overlaps_and_expansions() {
    assert_vectors(true);
}

#[test]
fn every_chunk_cut_replays_only_future_hits() {
    for vector in VECTORS.iter().filter(|vector| vector.ranges.is_none()) {
        for cut in 0..=vector.source.chars().count() {
            let mut search = NoteStageSearch::new(vector.query).unwrap();
            let mut offset = 17;
            let mut hits = Vec::new();
            for (index, scalar) in vector.source.chars().enumerate() {
                if index == cut {
                    search = NoteStageSearch::restore(vector.query, &search.snapshot()).unwrap();
                }
                if let Some(hit) = search.push_scalar(scalar, offset).unwrap() {
                    hits.push((hit.start - 17, hit.end - 17));
                }
                offset += scalar.len_utf16() as u64;
            }
            let restored = NoteStageSearch::restore(vector.query, &search.snapshot()).unwrap();
            assert_eq!(restored.snapshot(), search.snapshot());
            assert_eq!(hits, vector.expected, "{} cut {cut}", vector.name);
        }
    }
}

#[test]
fn generated_table_is_sorted_full_non_turkic_and_bounded() {
    assert_eq!(folding::TABLE.len(), 1585);
    assert!(folding::TABLE.windows(2).all(|pair| pair[0].0 < pair[1].0));
    assert_eq!(
        folding::TABLE.iter().map(|(_, value)| value.len()).max(),
        Some(MAX_FOLD_EXPANSION)
    );
    assert!(folding::TABLE
        .iter()
        .all(|(_, value)| !value.is_empty() && !value.contains(&'\0')));
    for (scalar, expected) in [('I', "i"), ('İ', "i\u{307}"), ('ß', "ss"), ('ς', "σ")] {
        let (actual, length) = fold(scalar);
        assert_eq!(actual[..length].iter().collect::<String>(), expected);
    }
    // Pinned mappings are honored independently of compiler Unicode tables.
    let (actual, length) = fold('\u{1c89}');
    assert_eq!(&actual[..length], &['\u{1c8a}']);
}

#[test]
fn query_validation_uses_original_utf8_bytes_without_trimming() {
    for query in [
        String::new(),
        "a\0b".into(),
        "a".repeat(1025),
        "😀".repeat(257),
    ] {
        assert!(matches!(
            NoteStageSearch::new(&query),
            Err(NoteMutationError::Invalid)
        ));
    }
    assert!(NoteStageSearch::new(&"a".repeat(1024)).is_ok());
    assert!(NoteStageSearch::new(&"😀".repeat(256)).is_ok());
    assert!(NoteStageSearch::new("\t \n").is_ok());
}

#[test]
fn long_source_never_grows_matcher_or_carry_past_query_bounds() {
    let query = "ﬃ".repeat(341); // 1023 input bytes; 1023 folded scalars.
    let mut search = NoteStageSearch::new(&query).unwrap();
    let pattern_length = search.pattern.len();
    assert_eq!(pattern_length, 1023);
    let capacities = (
        search.pattern.capacity(),
        search.prefix.capacity(),
        search.boundaries.capacity(),
        search.source.capacity(),
    );
    let mut offset = 0;
    for _ in 0..100_000 {
        search.push_scalar('😀', offset).unwrap();
        offset += 2;
        assert!(search.boundaries.len() <= pattern_length);
        assert!(search.source.len() <= pattern_length);
    }
    assert_eq!(
        capacities,
        (
            search.pattern.capacity(),
            search.prefix.capacity(),
            search.boundaries.capacity(),
            search.source.capacity()
        )
    );
    let carry = search.snapshot();
    assert_eq!(carry.source.len(), 4 * pattern_length);
    assert!(carry.source.len() <= MAX_CARRY_BYTES);
    assert!(pattern_length <= MAX_PATTERN_SCALARS);
    assert_eq!(
        NoteStageSearch::restore(&query, &carry).unwrap().snapshot(),
        carry
    );
}

#[test]
fn scalar_boundaries_reject_partial_expansion_even_inside_long_matches() {
    for (source, query, expected) in [
        ("aßa", "as", vec![]),
        ("aßa", "sa", vec![]),
        ("aßa", "assa", vec![(0, 3)]),
        ("ßß", "sss", vec![]),
        ("ßß", "ssss", vec![(0, 2)]),
    ] {
        let mut search = NoteStageSearch::new(query).unwrap();
        let mut offset = 0;
        let mut actual = Vec::new();
        for scalar in source.chars() {
            if let Some(hit) = search.push_scalar(scalar, offset).unwrap() {
                actual.push((hit.start, hit.end));
            }
            offset += scalar.len_utf16() as u64;
        }
        assert_eq!(actual, expected);
    }
}

#[test]
fn gaps_do_not_match_but_touching_intervals_preserve_carry() {
    let mut search = NoteStageSearch::new("aa").unwrap();
    search.push_scalar('a', 10).unwrap();
    search.reset_at_gap(11).unwrap();
    assert_eq!(
        search.push_scalar('a', 11).unwrap(),
        Some(NoteStageSearchRange { start: 10, end: 12 })
    );
    search.reset_at_gap(14).unwrap();
    let mut search = NoteStageSearch::restore("aa", &search.snapshot()).unwrap();
    assert_eq!(search.push_scalar('a', 14).unwrap(), None);
    assert_eq!(
        search.push_scalar('a', 15).unwrap(),
        Some(NoteStageSearchRange { start: 14, end: 16 })
    );
}

#[test]
fn invalid_scalar_or_position_does_not_mutate_state() {
    let mut search = NoteStageSearch::new("a").unwrap();
    search.push_scalar('x', 100).unwrap();
    let before = search.snapshot();
    for (scalar, offset) in [('a', 99), ('a', 102), ('\0', 101), ('😀', SAFE_INTEGER)] {
        assert_eq!(
            search.push_scalar(scalar, offset),
            Err(NoteMutationError::Invalid)
        );
        assert_eq!(search.snapshot(), before);
    }
    for next in [100, SAFE_INTEGER + 1, u64::MAX] {
        assert_eq!(search.reset_at_gap(next), Err(NoteMutationError::Invalid));
        assert_eq!(search.snapshot(), before);
    }
    search.reset_at_gap(SAFE_INTEGER - 1).unwrap();
    assert_eq!(
        search.push_scalar('a', SAFE_INTEGER - 1).unwrap(),
        Some(NoteStageSearchRange {
            start: SAFE_INTEGER - 1,
            end: SAFE_INTEGER
        })
    );
    assert_eq!(
        search.push_scalar('a', SAFE_INTEGER),
        Err(NoteMutationError::Invalid)
    );
}

#[test]
fn malformed_carry_is_rejected_instead_of_trusting_prefix_state() {
    let mut search = NoteStageSearch::new("aa").unwrap();
    search.push_scalar('a', 10).unwrap();
    let valid = search.snapshot();
    for field in [
        "version",
        "fold",
        "query",
        "count",
        "bytes",
        "nul",
        "offset",
        "underflow",
        "missing-offset",
    ] {
        let mut carry = valid.clone();
        match field {
            "version" => carry.version += 1,
            "fold" => carry.folding_sha256 = "different".into(),
            "query" => carry.query = "bb".into(),
            "count" => carry.source = "aaa".into(),
            "bytes" => carry.source = "😀".repeat(3),
            "nul" => carry.source = "\0".into(),
            "offset" => carry.next_offset = Some(SAFE_INTEGER + 1),
            "underflow" => carry.next_offset = Some(0),
            "missing-offset" => carry.next_offset = None,
            _ => unreachable!(),
        }
        assert!(NoteStageSearch::restore("aa", &carry).is_err(), "{field}");
    }
    assert!(NoteStageSearch::restore("a", &valid).is_err());
    assert!(
        NoteStageSearch::restore("aa", &NoteStageSearch::new("aa").unwrap().snapshot()).is_ok()
    );
    let mut encoded = serde_json::to_value(valid).unwrap();
    encoded["matched"] = 4.into();
    assert!(serde_json::from_value::<NoteStageSearchCarry>(encoded).is_err());
}

#[test]
fn every_generated_mapping_matches_the_pinned_input_digest() {
    use sha2::{Digest, Sha256};
    use std::fmt::Write;
    let mut canonical = String::new();
    for (source, mapping) in folding::TABLE {
        write!(canonical, "{:X};", u32::from(*source)).unwrap();
        for (index, scalar) in mapping.iter().enumerate() {
            if index != 0 {
                canonical.push(' ');
            }
            write!(canonical, "{:X}", u32::from(*scalar)).unwrap();
        }
        canonical.push('\n');
    }
    // Generated independently from the hash-verified C/F source records.
    assert_eq!(
        format!("{:x}", Sha256::digest(canonical.as_bytes())),
        "b41eea2dea84d468d8c4330d4acb90d94f97c848a1448af150574ae8397913a3"
    );
}

#[test]
fn original_source_replay_range_restores_every_page_frontier() {
    for vector in VECTORS.iter().filter(|vector| vector.ranges.is_none()) {
        let mut search = NoteStageSearch::new(vector.query).unwrap();
        assert_eq!(search.replay_range(), None);
        let mut hits = Vec::new();
        let mut offset = 23;
        for scalar in vector.source.chars() {
            if let Some(hit) = search.push_scalar(scalar, offset).unwrap() {
                hits.push((hit.start - 23, hit.end - 23));
            }
            offset += scalar.len_utf16() as u64;
            let range = search.replay_range().unwrap();
            assert_eq!(range.end, offset);
            assert!(range.end - range.start <= 2 * search.pattern.len() as u64);
            let mut restored = NoteStageSearch::new(vector.query).unwrap();
            restored.reset_at_gap(range.start).unwrap();
            // Test-only source oracle. A production Store reads this bounded
            // interval through its retained immutable source index.
            let mut at = 23;
            for prior in vector.source.chars() {
                if range.start <= at && at < range.end {
                    let _past_hit = restored.push_scalar(prior, at).unwrap();
                }
                at += prior.len_utf16() as u64;
            }
            assert_eq!(restored.snapshot(), search.snapshot(), "{}", vector.name);
            search = restored;
        }
        assert_eq!(hits, vector.expected, "{}", vector.name);
        search.reset_at_gap(offset + 10).unwrap();
        assert_eq!(
            search.replay_range(),
            Some(NoteStageSearchRange {
                start: offset + 10,
                end: offset + 10
            })
        );
    }
}

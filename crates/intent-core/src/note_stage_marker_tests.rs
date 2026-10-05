use super::*;
use serde_json::json;

struct Fixture {
    record: NoteStageRecord,
    descriptor: Value,
    attrs: Value,
    literal: String,
    range: NoteStageRange,
}
impl Fixture {
    fn new(id: &str, kind: &str) -> Self {
        let literal = format!("<!--anchor:{id}:{kind}-->");
        // Prefix "😀界" occupies three UTF16 units, seven UTF8 bytes.
        let range = NoteStageRange {
            start: 3,
            end: 3 + u64::try_from(literal.encode_utf16().count()).unwrap(),
        };
        Self {
            record: serde_json::from_value(json!({"kind":"projection","ordinal":2,
                "sourceRange":{"start":range.start,"end":range.end},
                "role":"marker-occurrence","canonicalId":id,
                "detail":{"textId":"descriptor","length":1,"utf8Bytes":1,"sha256":"0".repeat(64)}}))
            .unwrap(),
            descriptor: json!({"version":1,"nodeType":"commentAnchor","parentOrdinal":0,
                "nativeRange":{"from":8,"to":9},"attributesRef":"attrs"}),
            attrs: json!({"id":format!("{id}:{kind}"),"type":kind,"commentId":id}),
            literal,
            range,
        }
    }
    fn input(&self) -> ResolvedMarkerInput<'_> {
        ResolvedMarkerInput {
            record: &self.record,
            descriptor: &self.descriptor,
            attributes: Some(&self.attrs),
            frozen_literal: &self.literal,
            frozen_range: self.range,
            view_length: self.range.end + 3,
        }
    }
}

#[test]
fn individual_kinds_keep_literal_legacy_ids_and_unicode_coordinates() {
    for (kind, expected) in [
        ("start", MarkerKind::Start),
        ("end", MarkerKind::End),
        ("point", MarkerKind::Point),
    ] {
        for id in ["legacy", "界😀", "a:b", "id:point-->literal"] {
            let fixture = Fixture::new(id, kind);
            assert_eq!(marker_literal(&fixture.input()), Ok(expected));
            // Repeated type/ID is valid shape; occurrence authority is external.
            assert_eq!(marker_literal(&fixture.input()), Ok(expected));
        }
    }
    let fixture = Fixture::new("legacy", "start");
    assert_eq!(fixture.range.start, 3);
    assert_eq!(fixture.range.end, 29);
}

#[test]
fn pair_hulls_whitespace_and_lookalikes_are_not_individual_literals() {
    let fixture = Fixture::new("legacy", "start");
    for text in [
        "<!--anchor:legacy:start-->body<!--anchor:legacy:end-->",
        " <!--anchor:legacy:start-->",
        "<!--anchor:other:start-->",
        "&lt;!--anchor:legacy:start--&gt;",
        "<!--anchor:legacy:point-->",
    ] {
        let mut input = fixture.input();
        input.frozen_literal = text;
        assert_eq!(marker_literal(&input), Err(MarkerError::Invalid));
    }
}

#[test]
fn attributes_are_explicit_exact_and_never_defaulted() {
    let fixture = Fixture::new("legacy", "point");
    let mut input = fixture.input();
    input.attributes = None;
    assert_eq!(marker_literal(&input), Err(MarkerError::Invalid));
    for attrs in [
        json!({}),
        json!({"id":"legacy:point","type":"point"}),
        json!({"id":"alias","type":"point","commentId":"legacy"}),
        json!({"id":"legacy:point","type":"point","commentId":"other"}),
        json!({"id":"legacy:point","type":"point","commentId":"legacy","extra":true}),
        json!({"id":"legacy:POINT","type":"POINT","commentId":"legacy"}),
    ] {
        let mut input = fixture.input();
        input.attributes = Some(&attrs);
        assert_eq!(marker_literal(&input), Err(MarkerError::Invalid));
    }
}

#[test]
fn descriptor_shape_parent_and_native_width_fail_closed() {
    for (key, value) in [
        ("version", json!(2)),
        ("nodeType", json!("text")),
        ("parentOrdinal", json!(2)),
        ("attributesRef", Value::Null),
        ("nativeRange", json!({"from":8,"to":8})),
        ("nativeRange", json!({"from":8,"to":10})),
        ("nativeRange", json!({"from":8,"to":9,"extra":0})),
        ("nativeRange", json!({"from":SAFE,"to":SAFE+1})),
    ] {
        let mut fixture = Fixture::new("legacy", "start");
        fixture.descriptor[key] = value;
        assert!(marker_literal(&fixture.input()).is_err(), "{key}");
    }
    for key in ["attributesRef", "parentOrdinal", "nativeRange"] {
        let mut fixture = Fixture::new("legacy", "start");
        fixture.descriptor.as_object_mut().unwrap().remove(key);
        assert!(marker_literal(&fixture.input()).is_err());
    }
    let mut fixture = Fixture::new("legacy", "start");
    fixture.descriptor["extra"] = json!(true);
    assert!(marker_literal(&fixture.input()).is_err());
}

#[test]
fn exact_frozen_extent_and_safe_source_bounds_are_required() {
    let fixture = Fixture::new("legacy", "end");
    let mut input = fixture.input();
    input.frozen_range.start += 1;
    assert_eq!(marker_literal(&input), Err(MarkerError::Invalid));
    let mut input = fixture.input();
    input.view_length = fixture.range.end - 1;
    assert_eq!(marker_literal(&input), Err(MarkerError::Invalid));
    let mut fixture = Fixture::new("legacy", "end");
    if let NoteStageRecord::Projection { source_range, .. } = &mut fixture.record {
        source_range.end += 1;
    }
    fixture.range.end += 1;
    assert_eq!(marker_literal(&fixture.input()), Err(MarkerError::Invalid));
}

#[test]
fn role_and_canonical_identity_are_not_inferred_from_text() {
    for id in [
        None,
        Some(String::new()),
        Some("x".repeat(257)),
        Some("a\0b".into()),
    ] {
        let mut fixture = Fixture::new("legacy", "point");
        if let NoteStageRecord::Projection { canonical_id, .. } = &mut fixture.record {
            *canonical_id = id;
        }
        assert_eq!(marker_literal(&fixture.input()), Err(MarkerError::Invalid));
    }
    let mut fixture = Fixture::new("legacy", "point");
    if let NoteStageRecord::Projection { role, .. } = &mut fixture.record {
        *role = NoteStageRole::InlineSpan;
    }
    assert_eq!(
        marker_literal(&fixture.input()),
        Err(MarkerError::Unsupported)
    );
}

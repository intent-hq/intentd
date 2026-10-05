use super::*;
use serde_json::json;

struct Fixture {
    header: NoteStageHeader,
    selection: Vec<NoteStageRecord>,
    records: Vec<NoteStageRecord>,
    descriptors: Vec<Value>,
    attributes: Vec<Value>,
    source: String,
    start: u64,
    view_length: u64,
}
impl Fixture {
    fn new(source: &str, start: u64, from: u64, to: u64) -> Self {
        let length = source.encode_utf16().count() as u64;
        let projection = |ordinal, role, a, b| {
            serde_json::from_value(json!({
            "kind":"projection", "ordinal":ordinal,
            "sourceRange":{"start":a,"end":b},"role":role,
            "detail":{"textId":format!("detail-{ordinal}"),"length":1,"utf8Bytes":1,"sha256":"a".repeat(64)}
        })).unwrap()
        };
        Self {
            header: serde_json::from_value(json!({
                "baseRevision":"r","editorSessionId":"editor","localEditSequence":0,
                "liveGeneration":7,"selectionGeneration":9,"action":"read",
                "output":"selectionMarkdown","selection":"ranges"
            }))
            .unwrap(),
            selection: vec![serde_json::from_value(json!({"kind":"range","ordinal":0,
                "start":start+from,"end":start+to,"direction":"backward",
                "anchorAffinity":"after","headAffinity":"before"}))
            .unwrap()],
            records: vec![
                projection(0, "selection-owner", start, start + length),
                projection(1, "inline-span", start + from, start + to),
            ],
            descriptors: vec![
                json!({"version":1,"nodeType":"paragraph","parentOrdinal":null,
                "nativeRange":{"from":0,"to":length+2},"attributesRef":"paragraph-attrs"}),
                json!({"version":1,"nodeType":"text","parentOrdinal":0,
                "nativeRange":{"from":from+1,"to":to+1},"attributesRef":"text-attrs"}),
            ],
            attributes: vec![json!({}), json!({})],
            source: source.to_owned(),
            start,
            view_length: start + length,
        }
    }
    fn check(&self) -> Result<Option<String>> {
        let live: Vec<_> = self
            .records
            .iter()
            .zip(&self.descriptors)
            .zip(&self.attributes)
            .map(
                |((record, descriptor), attributes)| ResolvedSelectionDescriptor {
                    record,
                    descriptor,
                    attributes,
                },
            )
            .collect();
        let input = NoteSelectionMarkdownInput {
            header: &self.header,
            selection: &self.selection,
            live: &live,
            frozen_paragraph: &self.source,
            frozen_range: NoteStageRange {
                start: self.start,
                end: self.start + self.source.encode_utf16().count() as u64,
            },
            view_length: self.view_length,
        };
        let result = selection_markdown(&input)?;
        assert!(matches!(result.direction, NoteStageDirection::Backward));
        assert!(matches!(result.anchor_affinity, NoteStageAffinity::After));
        assert!(matches!(result.head_affinity, NoteStageAffinity::Before));
        let NoteStageRecord::Range { start, end, .. } = self.selection[0] else {
            panic!("range")
        };
        assert_eq!(
            (result.source_range.start, result.source_range.end),
            (start, end)
        );
        Ok(result.text.map(str::to_owned))
    }
}

#[test]
fn actual_native_capture_source_and_offset_parity() {
    // Source/selection/native offsets copied exactly from actual configured FE
    // capture3a8b7382, selection-text-attrs-producer.json (abc and far abc).
    // Producer JSON SHA256: ca254b1f483f8e39d5e089743bb501beba764cdfe4cfc72b4d5f82c442e8adcf
    // Descriptor encoding below is controlled; no actual upload/Store seal claim.
    for start in [0, 65538] {
        assert_eq!(
            Fixture::new("abc", start, 0, 2).check(),
            Ok(Some("ab".to_owned()))
        );
    }
}
#[test]
fn configured_copy_space_edges_and_no_copy() {
    // Actual selected-note-markdown-copy controls in FE capture test lineage.
    for (from, to, expected) in [
        (0, 7, Some("one two")),
        (3, 7, Some("two")),
        (0, 4, Some("one")),
        (3, 4, None),
        (2, 2, None),
    ] {
        assert_eq!(
            Fixture::new("one two", 100, from, to).check(),
            Ok(expected.map(str::to_owned))
        );
    }
}
#[test]
fn rejects_unsupported_text_in_entire_paragraph_before_trim() {
    for source in [
        "a*b",
        "a  b",
        "a_b",
        "a\nb",
        "a\tb",
        "a😀b",
        "aé",
        "a\u{feff}b",
        "a\0b",
        "a  \u{85}",
        "",
    ] {
        let to = u64::from(!source.is_empty());
        assert_eq!(Fixture::new(source, 0, 0, to).check(), Err(Unsupported));
    }
}
#[test]
fn rejects_missing_nonempty_and_nonobject_attributes() {
    for i in 0..2 {
        for value in [json!({"id":null}), json!(null), json!([])] {
            let mut f = Fixture::new("abc", 0, 0, 2);
            f.attributes[i] = value;
            assert_eq!(f.check(), Err(Unsupported));
        }
        let mut f = Fixture::new("abc", 0, 0, 2);
        f.descriptors[i]
            .as_object_mut()
            .unwrap()
            .remove("attributesRef");
        assert_eq!(f.check(), Err(Unsupported));
    }
}
#[test]
fn rejects_wrong_native_context_without_aliases() {
    for (index, key, value) in [
        (0, "nodeType", json!("markdownBlock")),
        (0, "parentOrdinal", json!(2)),
        (1, "nodeType", json!("strong")),
        (1, "parentOrdinal", json!(null)),
        (1, "version", json!(2)),
    ] {
        let mut f = Fixture::new("abc", 0, 0, 2);
        f.descriptors[index][key] = value;
        assert_eq!(f.check(), Err(Unsupported));
    }
    let mut f = Fixture::new("abc", 0, 0, 2);
    f.descriptors[1]["marks"] = json!([]);
    assert_eq!(f.check(), Err(Unsupported));
}
#[test]
fn rejects_wrong_roles_ids_and_counts() {
    for i in 0..2 {
        let mut f = Fixture::new("abc", 0, 0, 2);
        if let NoteStageRecord::Projection { role, .. } = &mut f.records[i] {
            *role = NoteStageRole::MarkerOccurrence;
        }
        assert_eq!(f.check(), Err(Unsupported));
        let mut f = Fixture::new("abc", 0, 0, 2);
        if let NoteStageRecord::Projection { canonical_id, .. } = &mut f.records[i] {
            *canonical_id = Some("id".to_owned());
        }
        assert_eq!(f.check(), Err(Unsupported));
    }
    let mut f = Fixture::new("abc", 0, 0, 2);
    f.records.pop();
    assert_eq!(f.check(), Err(Unsupported));
    let mut f = Fixture::new("abc", 0, 0, 2);
    f.selection.clear();
    assert_eq!(f.check(), Err(Unsupported));
}
#[test]
fn requires_exact_read_ranges_header() {
    let mut f = Fixture::new("abc", 65538, 0, 2);
    f.header.selection = NoteStageSelection::All;
    assert_eq!(f.check(), Err(Unsupported));
    let mut f = Fixture::new("abc", 0, 0, 2);
    f.header.action = NoteStageAction::Mutate;
    assert_eq!(f.check(), Err(Unsupported));
    let mut f = Fixture::new("abc", 0, 0, 2);
    f.header.output = NoteStageOutput::Source;
    assert_eq!(f.check(), Err(Unsupported));
    let mut f = Fixture::new("abc", 0, 0, 2);
    f.header.live_generation = SAFE + 1;
    assert_eq!(f.check(), Err(Invalid));
}
#[test]
fn rejects_source_or_native_mapping_drift() {
    for i in 0..2 {
        let mut f = Fixture::new("abc", 65538, 0, 2);
        f.descriptors[i]["nativeRange"]["to"] = json!(4);
        assert_eq!(f.check(), Err(Invalid));
        let mut f = Fixture::new("abc", 65538, 0, 2);
        if let NoteStageRecord::Projection { source_range, .. } = &mut f.records[i] {
            source_range.start += 1;
        }
        assert_eq!(f.check(), Err(Invalid));
    }
    let mut f = Fixture::new("abc", 65538, 0, 2);
    f.view_length = 65539;
    assert_eq!(f.check(), Err(Invalid));
}
#[test]
fn accepts_shifted_native_positions_but_caps_subset() {
    let mut f = Fixture::new("abc", 65538, 0, 2);
    f.descriptors[0]["nativeRange"] = json!({"from":10,"to":15});
    f.descriptors[1]["nativeRange"] = json!({"from":11,"to":13});
    assert_eq!(f.check(), Ok(Some("ab".to_owned())));
    let source = "x".repeat(PARAGRAPH_UNITS);
    assert_eq!(
        Fixture::new(&source, 0, 0, 1).check(),
        Ok(Some("x".to_owned()))
    );
    assert_eq!(Fixture::new(&(source + "x"), 0, 0, 1).check(), Err(Budget));
    let mut f = Fixture::new("abc", 0, 0, 2);
    f.descriptors[0]["nativeRange"] = json!({"from":32766,"to":32771});
    assert_eq!(f.check(), Err(Budget));
}

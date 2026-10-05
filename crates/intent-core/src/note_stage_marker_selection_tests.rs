use super::*;
use crate::note_stage::NoteStageHeader;
use serde_json::json;

struct Fixture {
    header: NoteStageHeader,
    records: Vec<NoteStageRecord>,
    descriptors: Vec<Value>,
    attrs: Vec<Value>,
    source: String,
    start: u64,
    length: u64,
    marker_start: u64,
    marker_end: u64,
}
impl Fixture {
    fn new(left: &str, right: &str, id: &str) -> Self {
        let start = 65538;
        let literal = format!("<!--anchor:{id}:point-->");
        let source = format!("{left}{literal}{right}");
        let units = |s: &str| u64::try_from(s.encode_utf16().count()).unwrap();
        let l = units(left);
        let r = units(right);
        let m = units(&literal);
        let length = units(&source);
        let records = [("selection-owner", 0, length), ("inline-span", 0, l), ("marker-occurrence", l, l+m), ("inline-span", l+m, length)].into_iter().enumerate().map(|(ordinal, (role, a, b))| {
            let mut value = json!({"kind":"projection","ordinal":ordinal,"role":role,"sourceRange":{"start":start+a,"end":start+b},"detail":{"textId":format!("descriptor-{ordinal}"),"length":1,"utf8Bytes":1,"sha256":"a".repeat(64)}});
            if ordinal == 2 { value["canonicalId"] = json!(id); }
            serde_json::from_value(value).unwrap()
        }).collect();
        Self {
            header: serde_json::from_value(json!({"baseRevision":"r","editorSessionId":"editor","localEditSequence":0,"liveGeneration":1,"selectionGeneration":1,"action":"read","output":"selectionMarkdown","selection":"ranges"})).unwrap(),
            records,
            descriptors: [("paragraph", 10, 10+l+r+3), ("text", 11, 11+l), ("commentAnchor", 11+l, 12+l), ("text", 12+l, 12+l+r)].into_iter().enumerate().map(|(i,(node,from,to))| json!({"version":1,"nodeType":node,"parentOrdinal":if i==0 {Value::Null} else {json!(0)},"nativeRange":{"from":from,"to":to},"attributesRef":format!("attrs-{i}")})).collect(),
            attrs: vec![json!({}), json!({}), json!({"id":format!("{id}:point"),"type":"point","commentId":id}), json!({})],
            source,start,length,marker_start:l,marker_end:l+m,
        }
    }
    fn check(&self, from: u64, to: u64, backward: bool) -> Result<Option<String>> {
        let selection = [serde_json::from_value(json!({"kind":"range","ordinal":0,"start":self.start+from,"end":self.start+to,"direction":if backward {"backward"} else {"forward"},"anchorAffinity":"after","headAffinity":"before"})).unwrap()];
        let live: Vec<_> = self
            .records
            .iter()
            .zip(&self.descriptors)
            .zip(&self.attrs)
            .map(
                |((record, descriptor), attributes)| ResolvedSelectionDescriptor {
                    record,
                    descriptor,
                    attributes,
                },
            )
            .collect();
        let output = marker_selection_markdown(&NoteSelectionMarkdownInput {
            header: &self.header,
            selection: &selection,
            live: &live,
            frozen_paragraph: &self.source,
            frozen_range: NoteStageRange {
                start: self.start,
                end: self.start + self.length,
            },
            view_length: self.start + self.length + 5,
        })?;
        assert_eq!(
            (output.source_range.start, output.source_range.end),
            (self.start + from, self.start + to)
        );
        assert_eq!(
            matches!(output.direction, NoteStageDirection::Backward),
            backward
        );
        assert!(matches!(output.anchor_affinity, NoteStageAffinity::After));
        assert!(matches!(output.head_affinity, NoteStageAffinity::Before));
        Ok(output.text)
    }
}

#[test]
fn native_oracle_spacing_partial_and_both_directions() {
    // Independent configured-native EA7cf374 expectations. Controlled descriptors
    // here do not establish Store ownership or a native capture chain.
    let f = Fixture::new("A ", " B", "legacy:root");
    for backward in [false, true] {
        assert_eq!(f.check(0, f.length, backward), Ok(Some("A  B".into())));
        assert_eq!(f.check(0, f.marker_start, backward), Ok(Some("A".into())));
        assert_eq!(
            f.check(f.marker_end, f.length, backward),
            Ok(Some("B".into()))
        );
        assert_eq!(f.check(0, f.marker_end, backward), Ok(Some("A".into())));
        assert_eq!(
            f.check(f.marker_start, f.length, backward),
            Ok(Some("B".into()))
        );
    }
    let f = Fixture::new("ab ", " cd", "root");
    for backward in [false, true] {
        assert_eq!(f.check(1, f.length - 1, backward), Ok(Some("b  c".into())));
    }
    let f = Fixture::new("A  ", "  B", "root");
    assert_eq!(f.check(0, f.length, false), Ok(Some("A    B".into())));
}
#[test]
fn collapsed_atom_only_and_trim_empty_are_no_copy() {
    let f = Fixture::new("A ", " B", "root");
    for (a, b) in [
        (0, 0),
        (f.marker_start, f.marker_start),
        (f.marker_end, f.marker_end),
        (f.marker_start, f.marker_end),
        (1, f.marker_end + 1),
    ] {
        assert_eq!(f.check(a, b, false), Ok(None));
    }
    let f = Fixture::new(" ", " ", "root");
    assert_eq!(f.check(0, f.length, true), Ok(None));
}
#[test]
fn rejects_literal_interior_and_bad_source_coverage() {
    let f = Fixture::new("A ", " B", "root");
    assert_eq!(f.check(f.marker_start + 1, f.length, false), Err(Invalid));
    assert_eq!(f.check(0, f.marker_end - 1, false), Err(Invalid));
    assert_eq!(f.check(2, 1, false), Err(Invalid));
    assert_eq!(f.check(0, f.length + 1, false), Err(Invalid));
    for i in 0..4 {
        let mut f = Fixture::new("A ", " B", "root");
        if let NoteStageRecord::Projection { source_range, .. } = &mut f.records[i] {
            source_range.start += 1;
        }
        assert_eq!(f.check(0, f.length, false), Err(Invalid));
    }
    let f = Fixture::new("", " B", "root");
    assert_eq!(f.check(0, f.length, false), Err(Invalid));
}
#[test]
fn rejects_native_geometry_parent_and_ordinal_drift() {
    for i in 0..4 {
        let mut f = Fixture::new("A ", " B", "root");
        let n = f.descriptors[i]["nativeRange"]["to"].as_u64().unwrap();
        f.descriptors[i]["nativeRange"]["to"] = json!(n + 1);
        assert_eq!(f.check(0, f.length, false), Err(Invalid));
        let mut f = Fixture::new("A ", " B", "root");
        f.descriptors[i]["parentOrdinal"] = json!(3);
        assert_eq!(f.check(0, f.length, false), Err(Invalid));
        let mut f = Fixture::new("A ", " B", "root");
        if let NoteStageRecord::Projection { ordinal, .. } = &mut f.records[i] {
            *ordinal += 1;
        }
        assert_eq!(f.check(0, f.length, false), Err(Invalid));
    }
}
#[test]
fn rejects_unadmitted_text_roles_and_shapes() {
    for text in ["é", "😀", "a*b", "a\nb", "a\tb", "a_b"] {
        let f = Fixture::new(text, " B", "root");
        assert_eq!(f.check(0, f.length, false), Err(Unsupported));
    }
    for i in 0..4 {
        let mut f = Fixture::new("A ", " B", "root");
        f.descriptors[i]["marks"] = json!([]);
        assert_eq!(f.check(0, f.length, false), Err(Unsupported));
        let mut f = Fixture::new("A ", " B", "root");
        if let NoteStageRecord::Projection { role, .. } = &mut f.records[i] {
            *role = NoteStageRole::SelectionOwner;
        }
        if i != 0 {
            assert_eq!(f.check(0, f.length, false), Err(Unsupported));
        }
    }
    let mut f = Fixture::new("A ", " B", "root");
    f.records.pop();
    assert_eq!(f.check(0, f.length, false), Err(Unsupported));
    let mut f = Fixture::new("A ", " B", "root");
    f.header.output = NoteStageOutput::Source;
    assert_eq!(f.check(0, f.length, false), Err(Unsupported));
}
#[test]
fn requires_explicit_empty_text_attrs_and_exact_point_marker_attrs() {
    for i in [0, 1, 3] {
        let mut f = Fixture::new("A ", " B", "root");
        f.attrs[i] = json!({"bold":true});
        assert_eq!(f.check(0, f.length, false), Err(Unsupported));
    }
    for attrs in [
        json!({}),
        json!(null),
        json!({"id":"root:point","type":"point","commentId":"foreign"}),
        json!({"id":"root:point","type":"point","commentId":"root","extra":true}),
    ] {
        let mut f = Fixture::new("A ", " B", "root");
        f.attrs[2] = attrs;
        assert_eq!(f.check(0, f.length, false), Err(Invalid));
    }
    let mut f = Fixture::new("A ", " B", "root");
    f.descriptors[2]
        .as_object_mut()
        .unwrap()
        .remove("attributesRef");
    assert_eq!(f.check(0, f.length, false), Err(Unsupported));
    let mut f = Fixture::new("A ", " B", "root");
    f.source = f.source.replace("root:point", "fake:point");
    assert_eq!(f.check(0, f.length, false), Err(Invalid));
}
#[test]
fn whole_paragraph_budget_applies_before_selected_slice() {
    let literal_units = u64::try_from("<!--anchor:root:point-->".len()).unwrap();
    let left = "a".repeat(usize::try_from(4096 - literal_units - 1).unwrap());
    let f = Fixture::new(&left, "b", "root");
    assert_eq!(f.length, 4096);
    assert_eq!(f.check(0, 1, false), Ok(Some("a".into())));
    let f = Fixture::new(&(left + "a"), "b", "root");
    assert_eq!(f.check(0, 1, false), Err(Budget));
    let mut f = Fixture::new("a", "b", "root");
    f.source = "x".repeat(16385);
    assert_eq!(f.check(0, 1, false), Err(Budget));
}

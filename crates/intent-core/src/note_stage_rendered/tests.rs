use super::*;
use serde_json::json;
use std::fmt::Write as _;

struct Fixture {
    header: NoteStageHeader,
    selection: Vec<NoteStageRecord>,
    records: [NoteStageRecord; 2],
    descriptors: [Value; 2],
    attrs: [Value; 2],
    source: String,
    rendered: String,
    reference: NoteStageTextReference,
    range: NoteStageRange,
    view_length: u64,
}
fn hash(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to a String");
            output
        })
}
fn range(start: u64, end: u64) -> NoteStageRecord {
    serde_json::from_value(json!({"kind":"range","ordinal":0,"start":start,"end":end,"anchorAffinity":"after","headAffinity":"before","direction":"forward"})).unwrap()
}
impl Fixture {
    fn new(text: &str) -> Self {
        let length = u64::try_from(text.encode_utf16().count()).unwrap();
        let reference = NoteStageTextReference {
            text_id: "captured-text".into(),
            length,
            utf8_bytes: u64::try_from(text.len()).unwrap(),
            sha256: hash(text),
        };
        Self {
            header:serde_json::from_value(json!({"baseRevision":"base","editorSessionId":"editor","localEditSequence":0,"liveGeneration":1,"selectionGeneration":1,"action":"read","output":"search","selection":"ranges","query":{"text":"STRASSE","caseSensitive":false,"mode":"renderedText"}})).unwrap(),
            selection:vec![range(12,12+length)],
            records:[0,1].map(|ordinal|serde_json::from_value(json!({"kind":"projection","ordinal":ordinal,"role":if ordinal==0{"selection-owner"}else{"inline-span"},"sourceRange":{"start":12,"end":12+length},"detail":{"textId":format!("detail-{ordinal}"),"length":1,"utf8Bytes":1,"sha256":"0".repeat(64)}})).unwrap()),
            descriptors:[json!({"version":1,"nodeType":"paragraph","parentOrdinal":null,"nativeRange":{"from":100,"to":102+length},"attributesRef":"parent-attrs"}),json!({"version":2,"nodeType":"text","parentOrdinal":0,"nativeRange":{"from":101,"to":101+length},"attributesRef":"leaf-attrs","renderedText":reference})],
            attrs:[json!({}),json!({})],source:text.into(),rendered:text.into(),reference,range:NoteStageRange{start:12,end:12+length},view_length:20+length,
        }
    }
    fn resolve(&self) -> Result<RenderedIdentity<'_>> {
        let live = [
            ResolvedSelectionDescriptor {
                record: &self.records[0],
                descriptor: &self.descriptors[0],
                attributes: &self.attrs[0],
            },
            ResolvedSelectionDescriptor {
                record: &self.records[1],
                descriptor: &self.descriptors[1],
                attributes: &self.attrs[1],
            },
        ];
        rendered_identity(&NoteRenderedInput {
            header: &self.header,
            selection: &self.selection,
            live: &live,
            frozen_paragraph: &self.source,
            frozen_range: self.range,
            view_length: self.view_length,
            rendered_reference: &self.reference,
            rendered_text: &self.rendered,
        })
    }
}

#[test]
fn rendered_identity_matches_published_controlled_tuple_and_mapping() {
    // Exact identityCapture source/descriptor geometry/hash from docs16929fff,
    // rendered-search.json. Resource ownership here is a stated pure-input
    // precondition; placeholder detail refs are not Store/native authority.
    let f = Fixture::new("Straße😀");
    assert_eq!(
        f.reference.sha256,
        "7b28e3f7ff73155f7a69aa9ba70668936810a798c4c617dd0f5bd7537163a99c"
    );
    assert_eq!((f.reference.length, f.reference.utf8_bytes), (8, 11));
    let r = f.resolve().unwrap();
    assert_eq!(r.text, "Straße😀");
    assert_eq!((r.native_range.start, r.native_range.end), (101, 109));
    assert_eq!(r.leaf_ordinal, 1);
    assert!(matches!(r.direction, NoteStageDirection::Forward));
    assert!(matches!(r.anchor_affinity, NoteStageAffinity::After));
    assert!(matches!(r.head_affinity, NoteStageAffinity::Before));
    let hit = r.map_hit(0, 6).unwrap();
    assert_eq!((hit.start, hit.end), (12, 18));
    let hit = r.map_hit(6, 8).unwrap();
    assert_eq!((hit.start, hit.end), (18, 20));
    assert_eq!(r.map_hit(6, 7).unwrap_err(), Invalid);
}
#[test]
fn rendered_identity_preserves_raw_spaces_unicode_and_does_not_normalize() {
    for text in [" a  ", "Σςσ", "e\u{301}", "İ", "😀Straße😀", "\"\\\n\r\t"] {
        let f = Fixture::new(text);
        let r = f.resolve().unwrap();
        assert_eq!(r.text, text);
        let hit = r.map_hit(0, f.reference.length).unwrap();
        assert_eq!(hit.end - hit.start, f.reference.length);
    }
    let mut f = Fixture::new("é");
    f.rendered = "e\u{301}".into();
    assert_eq!(f.resolve().unwrap_err(), Invalid);
    let f = Fixture::new("\0");
    assert_eq!(f.resolve().unwrap_err(), Invalid);
}
#[test]
fn rendered_identity_requires_exact_resource_tuple_and_source_bytes() {
    for field in ["textId", "length", "utf8Bytes", "sha256"] {
        let mut f = Fixture::new("abc");
        f.descriptors[1]["renderedText"][field] = match field {
            "textId" => json!("foreign"),
            "sha256" => json!("f".repeat(64)),
            _ => json!(99),
        };
        assert_eq!(f.resolve().unwrap_err(), Invalid, "{field}");
    }
    let mut f = Fixture::new("abc");
    f.source = "abd".into();
    assert_eq!(f.resolve().unwrap_err(), Invalid);
    let mut f = Fixture::new("abc");
    f.reference.sha256 = "0".repeat(64);
    f.descriptors[1]["renderedText"]["sha256"] = json!(f.reference.sha256);
    assert_eq!(f.resolve().unwrap_err(), Invalid);
    let mut f = Fixture::new("abc");
    f.descriptors[1]["renderedText"]["extra"] = json!(true);
    assert_eq!(f.resolve().unwrap_err(), Invalid);
}
#[test]
fn rendered_identity_rejects_unknown_shapes_and_missing_attribute_authority() {
    for index in 0..2 {
        for field in ["attributesRef", "parentOrdinal", "nodeType", "version"] {
            let mut f = Fixture::new("abc");
            f.descriptors[index].as_object_mut().unwrap().remove(field);
            assert!(f.resolve().is_err());
        }
        let mut f = Fixture::new("abc");
        f.attrs[index] = json!({"bold":true});
        assert_eq!(f.resolve().unwrap_err(), Unsupported);
        let mut f = Fixture::new("abc");
        f.descriptors[index]["unknown"] = json!(1);
        assert_eq!(f.resolve().unwrap_err(), Unsupported);
    }
    let mut f = Fixture::new("abc");
    f.descriptors[1]["version"] = json!(1);
    assert_eq!(f.resolve().unwrap_err(), Unsupported);
    let mut f = Fixture::new("abc");
    f.descriptors[0]["nodeType"] = json!("heading");
    assert_eq!(f.resolve().unwrap_err(), Unsupported);
    let mut f = Fixture::new("abc");
    if let NoteStageRecord::Projection { canonical_id, .. } = &mut f.records[1] {
        *canonical_id = Some("marker".into());
    }
    assert_eq!(f.resolve().unwrap_err(), Unsupported);
}
#[test]
fn rendered_identity_checks_whole_leaf_geometry_and_scalar_domain() {
    let mut f = Fixture::new("a😀b");
    f.selection = vec![range(14, 15)];
    assert_eq!(f.resolve().unwrap_err(), Invalid);
    let mut f = Fixture::new("a😀b");
    f.selection = vec![range(13, 15)];
    let r = f.resolve().unwrap();
    assert_eq!((r.selected_range.start, r.selected_range.end), (1, 3));
    assert!(r.map_hit(0, 1).is_err());
    assert!(r.map_hit(1, 4).is_err());
    assert!(r.map_hit(1, 2).is_err());
    assert!(r.map_hit(1, 3).is_ok());
    let mut f = Fixture::new("abc");
    f.descriptors[1]["nativeRange"]["from"] = json!(102);
    assert_eq!(f.resolve().unwrap_err(), Invalid);
    let mut f = Fixture::new("abc");
    if let NoteStageRecord::Projection { source_range, .. } = &mut f.records[1] {
        source_range.start += 1;
    }
    assert_eq!(f.resolve().unwrap_err(), Invalid);
    let mut f = Fixture::new("abc");
    f.range.start += 1;
    assert_eq!(f.resolve().unwrap_err(), Invalid);
}
#[test]
fn rendered_identity_collapsed_domain_keeps_nonempty_native_leaf() {
    let mut f = Fixture::new("😀abc");
    f.selection = vec![range(14, 14)];
    let r = f.resolve().unwrap();
    assert_eq!(r.text, "😀abc");
    assert_eq!((r.selected_range.start, r.selected_range.end), (2, 2));
    assert!(r.map_hit(2, 2).is_err());
    assert!(r.map_hit(2, 3).is_err());
    let f = Fixture::new("");
    assert_eq!(f.resolve().unwrap_err(), Invalid);
}
#[test]
fn rendered_identity_rejects_wrong_header_closure_and_subset_overflow() {
    let mut f = Fixture::new("abc");
    f.header.selection = NoteStageSelection::All;
    assert_eq!(f.resolve().unwrap_err(), Unsupported);
    let mut f = Fixture::new("abc");
    f.header.query.as_mut().unwrap().mode = NoteStageSearchMode::Source;
    assert_eq!(f.resolve().unwrap_err(), Unsupported);
    let mut f = Fixture::new("abc");
    f.header.query.as_mut().unwrap().case_sensitive = true;
    assert_eq!(f.resolve().unwrap_err(), Invalid);
    let mut f = Fixture::new("abc");
    f.selection.push(range(12, 13));
    assert_eq!(f.resolve().unwrap_err(), Unsupported);
    let f = Fixture::new(&"a".repeat(4096));
    assert!(f.resolve().is_ok());
    let f = Fixture::new(&"a".repeat(4097));
    assert_eq!(f.resolve().unwrap_err(), Budget);
    let f = Fixture::new(&"a".repeat(16385));
    assert_eq!(f.resolve().unwrap_err(), Budget);
    let mut f = Fixture::new("abc");
    f.descriptors[0]["nativeRange"]["to"] = json!(32769);
    assert_eq!(f.resolve().unwrap_err(), Budget);
}

#[test]
fn rendered_v2_does_not_change_existing_v1_selection_markdown() {
    use crate::note_stage_selection_markdown::{
        selection_markdown, NoteSelectionMarkdownInput, SelectionMarkdownError,
    };
    let mut f = Fixture::new("abc");
    f.header.output = NoteStageOutput::SelectionMarkdown;
    f.header.query = None;
    for version in [2, 1] {
        if version == 1 {
            f.descriptors[1]["version"] = json!(1);
            f.descriptors[1]
                .as_object_mut()
                .unwrap()
                .remove("renderedText");
        }
        let live = [
            ResolvedSelectionDescriptor {
                record: &f.records[0],
                descriptor: &f.descriptors[0],
                attributes: &f.attrs[0],
            },
            ResolvedSelectionDescriptor {
                record: &f.records[1],
                descriptor: &f.descriptors[1],
                attributes: &f.attrs[1],
            },
        ];
        let input = NoteSelectionMarkdownInput {
            header: &f.header,
            selection: &f.selection,
            live: &live,
            frozen_paragraph: &f.source,
            frozen_range: f.range,
            view_length: f.view_length,
        };
        if version == 2 {
            assert_eq!(
                selection_markdown(&input).unwrap_err(),
                SelectionMarkdownError::Unsupported
            );
        } else {
            assert_eq!(selection_markdown(&input).unwrap().text, Some("abc"));
        }
    }
}

#[test]
fn rendered_identity_rejects_empty_query_without_trimming_whitespace() {
    let mut f = Fixture::new(" a  ");
    f.header.query.as_mut().unwrap().text = String::new();
    assert_eq!(f.resolve().unwrap_err(), Invalid);
    for query in [" ", "  ", "\t", "\n", "\t\n"] {
        f.header.query.as_mut().unwrap().text = query.into();
        assert!(f.resolve().is_ok(), "literal whitespace query {query:?}");
        assert_eq!(f.header.query.as_ref().unwrap().text, query);
    }
}

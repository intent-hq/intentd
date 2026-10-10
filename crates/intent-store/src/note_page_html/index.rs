//! Persist canonical construction results; none of this parser work runs on reads.
use super::{canonical, NativeNode};
use crate::note_page_index::Entries;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

type Descriptor = (usize, usize, String, Value);
type RawMap = (
    Range<usize>,
    Option<usize>,
    Range<usize>,
    String,
    &'static str,
);

fn wire_range(range: &Range<usize>, units: &[usize]) -> Value {
    json!({"start":units[range.start],"end":units[range.end]})
}

fn hull(ranges: impl Iterator<Item = Range<usize>>) -> Option<Range<usize>> {
    ranges.reduce(|a, b| a.start.min(b.start)..a.end.max(b.end))
}

fn literal_pieces(node: &NativeNode) -> Vec<(Range<usize>, &'static str)> {
    if node.kind == "text" {
        let mut result: Vec<(Range<usize>, &'static str)> = Vec::new();
        for map in &node.maps {
            if let Some((last, _)) = result.last_mut() {
                if last.end == map.raw.start {
                    last.end = map.raw.end;
                    continue;
                }
            }
            result.push((map.raw.clone(), "body"));
        }
        return result;
    }
    let Some(source) = &node.source else {
        return Vec::new();
    };
    let mut pieces = Vec::new();
    if let Some(opening) = &source.opening {
        pieces.push((opening.clone(), "opening"));
    }
    if let Some(closing) = &*source.closing.borrow() {
        pieces.push((closing.clone(), "closing"));
    }
    pieces
}

fn source_pieces(
    entries: &mut Entries,
    id: &str,
    pieces: &[(Range<usize>, &'static str)],
    units: &[usize],
) -> String {
    let reference = format!("d:{id}:pieces");
    for (position, (range, role)) in pieces
        .iter()
        .filter(|(range, _)| !range.is_empty())
        .enumerate()
    {
        let piece_id = entries.id();
        entries.rows.push((reference.clone(), position, json!({"kind":"sourcePiece","id":piece_id,"nodeRef":format!("d:{id}"),"sourceRange":wire_range(range,units),"role":role})));
    }
    reference
}

/// The raw-HTML entry route is distinct from Markdown conversion in the existing
/// frontend. Mixed Markdown and custom primitives require their own projection.
pub(crate) fn append(
    source: &str,
    units: &[usize],
    entries: &mut Entries,
    descriptors: &mut Vec<Descriptor>,
) {
    if !super::uses_html_entry(source) {
        return;
    }
    // The lexical Markdown scanner can find backticks in an HTML-entry tail,
    // but that entry path never interprets them as Markdown code spans.
    for (_, _, _, descriptor) in descriptors.iter_mut() {
        if descriptor["role"] == "code" {
            descriptor["role"] = json!("literal");
        }
    }
    let tree = canonical(source);
    let mut active = Vec::new();
    let mut pending = vec![0];
    while let Some(id) = pending.pop() {
        active.push(id);
        pending.extend(tree.nodes[id].children.iter().rev().copied());
    }
    let ids: BTreeMap<_, _> = active.iter().map(|id| (*id, entries.id())).collect();
    let mut envelopes = vec![None; tree.nodes.len()];
    for &id in active.iter().rev() {
        envelopes[id] = hull(
            literal_pieces(&tree.nodes[id])
                .into_iter()
                .map(|(range, _)| range)
                .chain(
                    tree.nodes[id]
                        .children
                        .iter()
                        .filter_map(|child| envelopes[*child].clone()),
                ),
        );
    }
    let mut boundaries: BTreeMap<usize, (String, Range<usize>)> = BTreeMap::new();
    for &id in &active {
        if matches!(
            tree.nodes[id].kind,
            "table" | "tableRow" | "tableCell" | "tableHeader"
        ) {
            if let Some(range) = &envelopes[id] {
                boundaries.insert(id, (entries.id(), range.clone()));
            }
        }
    }
    let mut attributes = BTreeMap::new();
    for &id in &active {
        let node = &tree.nodes[id];
        let native_id = &ids[&id];
        let attrs = if node.attributes.is_null() {
            json!({})
        } else {
            node.attributes.clone()
        };
        let attribute_start = entries.rows.len();
        let attributes_ref = entries.context_attributes(&attrs);
        let primitive = match node.kind {
            "diffBlock" => Some("diff"),
            "mermaidBlock" => Some("mermaid"),
            _ => None,
        };
        if let Some(primitive) = primitive {
            let code_ref = entries.rows[attribute_start..]
                .iter()
                .find_map(|(_, _, value)| {
                    (value["key"] == "code")
                        .then(|| value["valueRef"].as_str())
                        .flatten()
                })
                .expect("canonical primitive code field")
                .to_owned();
            entries
                .artifact_sources
                .push((format!("d:{native_id}"), code_ref, primitive));
        }
        attributes.insert(id, attributes_ref.clone());
        let pieces = literal_pieces(node);
        let void = matches!(node.kind, "image" | "hardBreak" | "horizontalRule");
        let explicit = if node.kind == "text" {
            pieces.len() == 1
        } else {
            node.source
                .as_ref()
                .is_some_and(|s| s.opening.is_some() && (s.closing.borrow().is_some() || void))
        };
        let provenance = if node.source.is_none()
            || (node.kind != "text"
                && node
                    .source
                    .as_ref()
                    .is_some_and(|source| source.opening.is_none()))
        {
            "implicit"
        } else if explicit {
            "explicit"
        } else {
            "repaired"
        };
        let envelope = envelopes[id].clone().unwrap_or(0..0);
        let range = if provenance == "implicit" {
            envelope.start..envelope.start
        } else {
            envelope
        };
        let child_index = node.parent.map_or(0, |parent| {
            tree.nodes[parent]
                .children
                .iter()
                .position(|child| *child == id)
                .expect("canonical child")
        });
        let class = if node.kind == "text" {
            "text"
        } else if void || primitive.is_some() {
            "atom"
        } else {
            "container"
        };
        let mut value = json!({"kind":"nativeNode","id":native_id,"profile":"canonicalNote","profileVersion":1,"nodeType":node.kind,"nodeClass":class,"parentRef":node.parent.map(|parent|format!("d:{}",ids[&parent])),"childIndex":child_index,"sourceRange":wire_range(&range,units),"provenance":provenance,"attributesRef":attributes_ref});
        if !node.marks.is_empty() {
            value["marksRef"] = json!(entries.context_attributes(&json!(node.marks)));
        }
        if provenance == "repaired" {
            value["sourcePiecesRef"] = json!(source_pieces(entries, native_id, &pieces, units));
        }
        if primitive.is_some() {
            value["nativeRef"] = json!(format!("d:{native_id}"));
            descriptors.push((
                units[range.start],
                units[range.end],
                native_id.clone(),
                value.clone(),
            ));
        }
        entries.rows.push((format!("d:{native_id}"), 0, value));
    }
    for (&id, (boundary_id, envelope)) in &boundaries {
        let node = &tree.nodes[id];
        let table = if node.kind == "table" {
            id
        } else {
            let mut parent = node.parent;
            loop {
                let ancestor = parent.expect("native table ancestor");
                if tree.nodes[ancestor].kind == "table" {
                    break ancestor;
                }
                parent = tree.nodes[ancestor].parent;
            }
        };
        let mut position = json!({"profile":"canonicalNote","profileVersion":1,"tableRef":format!("d:{}",boundaries[&table].0)});
        if id != table {
            let row = if node.kind == "tableRow" {
                id
            } else {
                node.parent.expect("cell row")
            };
            position["rowIndex"] = json!(tree.nodes[table]
                .children
                .iter()
                .position(|child| *child == row)
                .expect("table row"));
            if id != row {
                position["columnIndex"] = json!(tree.nodes[row]
                    .children
                    .iter()
                    .position(|child| *child == id)
                    .expect("row cell"));
                position["cellRole"] = json!(if node.kind == "tableHeader" {
                    "header"
                } else {
                    "data"
                });
            }
        }
        let handle = node.source.as_ref().expect("HTML table node");
        let opening = handle.opening.clone();
        let closing = handle.closing.borrow().clone();
        let body = opening
            .as_ref()
            .zip(closing.as_ref())
            .map(|(a, b)| a.end..b.start);
        let provenance = if opening.is_none() {
            "implicit"
        } else if closing.is_none() {
            "repaired"
        } else {
            "explicit"
        };
        let mut html_source = json!({"provenance":provenance,"openingRange":opening.as_ref().map(|r|wire_range(r,units)),"bodyRange":body.as_ref().map(|r|wire_range(r,units)),"closingRange":closing.as_ref().map(|r|wire_range(r,units))});
        if provenance == "repaired" {
            html_source["piecesRef"] = json!(source_pieces(
                entries,
                boundary_id,
                &literal_pieces(node),
                units
            ));
        }
        let detail = format!("d:{boundary_id}:details");
        for (index, (field, range)) in [("openingSource", opening), ("closingSource", closing)]
            .into_iter()
            .enumerate()
        {
            let fragment =
                entries.fragment(field, range.as_ref().map_or("", |r| &source[r.clone()]));
            entries.rows.push((detail.clone(),index,json!({"kind":"fragment","id":format!("{boundary_id}:{index}"),"field":field,"offset":0,"text":"","nextRef":fragment})));
        }
        let literal_range = if provenance == "implicit" {
            envelope.start..envelope.start
        } else {
            envelope.clone()
        };
        let mut boundary = json!({"kind":"boundary","id":boundary_id,"construct":match node.kind {"table"=>"htmlTable","tableRow"=>"htmlTableRow",_=>"htmlTableCell"},"sourceRange":wire_range(&literal_range,units),"htmlPosition":position,"htmlSource":html_source,"detailRef":detail,"attributesRef":attributes[&id],"nativeRef":format!("d:{}",ids[&id])});
        let mut parent = node.parent;
        while let Some(ancestor) = parent {
            if let Some((owner, _)) = boundaries.get(&ancestor) {
                boundary["parentRef"] = json!(format!("d:{owner}"));
                break;
            }
            parent = tree.nodes[ancestor].parent;
        }
        entries
            .rows
            .push((format!("d:{boundary_id}"), 0, boundary.clone()));
        boundary["sourceMapRef"] = json!(format!("h:{boundary_id}"));
        descriptors.push((
            units[envelope.start],
            units[envelope.end],
            boundary_id.clone(),
            boundary,
        ));
    }
    // Sort raw maps once, then sweep source owners. Work is proportional to the
    // produced segments and their actual ancestor memberships, not sibling count.
    let mut maps: Vec<RawMap> = Vec::new();
    for &id in &active {
        if tree.nodes[id].kind == "text" {
            for map in &tree.nodes[id].maps {
                maps.push((
                    map.raw.clone(),
                    Some(id),
                    map.rendered.clone(),
                    map.text.clone(),
                    map.mapping,
                ));
            }
        }
    }
    for map in &tree.omitted {
        maps.push((map.raw.clone(), None, 0..0, String::new(), "omitted"));
    }
    maps.sort_by_key(|m| (m.0.start, m.0.end));
    let mut covered = 0;
    let mut gaps = Vec::new();
    for map in &maps {
        if covered < map.0.start {
            gaps.push((covered..map.0.start, None, 0..0, String::new(), "omitted"));
        }
        covered = covered.max(map.0.end);
    }
    if covered < source.len() {
        gaps.push((covered..source.len(), None, 0..0, String::new(), "omitted"));
    }
    maps.extend(gaps);
    maps.sort_by_key(|m| (m.0.start, m.0.end));
    let mut owners: Vec<_> = boundaries.values().collect();
    owners.sort_by_key(|(_, range)| (range.start, range.end));
    let mut next_owner = 0;
    let mut admitted = BTreeSet::new();
    let mut positions = BTreeMap::<String, usize>::new();
    let document_id = entries.id();
    let mut document_ranges: Vec<Range<usize>> = Vec::new();
    let mut document_position = 0;
    for (raw, leaf, rendered, text, mapping) in maps {
        while next_owner < owners.len() && owners[next_owner].1.start < raw.end {
            admitted.insert(next_owner);
            next_owner += 1;
        }
        admitted.retain(|index| owners[*index].1.end > raw.start);
        let text_ref = if text.is_empty() {
            None
        } else {
            Some(entries.fragment("renderedText", &text))
        };
        // The document owner receives only pieces outside table ownership.
        // Subtract the sorted union; never duplicate table maps in a root map.
        let mut outside = Vec::new();
        let mut cursor = raw.start;
        for &index in &admitted {
            let range = &owners[index].1;
            if cursor < range.start.min(raw.end) {
                outside.push(cursor..range.start.min(raw.end));
            }
            cursor = cursor.max(range.end.min(raw.end));
        }
        if cursor < raw.end {
            outside.push(cursor..raw.end);
        }
        for piece in outside {
            let (piece_rendered, piece_text) = if mapping == "identity" {
                let offset = units[piece.start] - units[raw.start];
                (
                    rendered.start + offset
                        ..rendered.start + offset + units[piece.end] - units[piece.start],
                    &text[piece.start - raw.start..piece.end - raw.start],
                )
            } else {
                // Non-bijective normalization/entity seams retain their exact
                // source interval. A table already owning that seam remains its
                // owner; the document must not invent a split interpolation.
                if mapping != "omitted" && piece != raw {
                    continue;
                }
                (rendered.clone(), text.as_str())
            };
            if let Some(last) = document_ranges
                .last_mut()
                .filter(|last| last.end >= piece.start)
            {
                last.end = last.end.max(piece.end);
            } else {
                document_ranges.push(piece.clone());
            }
            let reference = if piece_text.is_empty() {
                None
            } else if piece == raw {
                text_ref.clone()
            } else {
                Some(entries.fragment("renderedText", piece_text))
            };
            let map_id = entries.id();
            let value = json!({"kind":"sourceMap","id":map_id,"profile":"canonicalNote","profileVersion":1,"ownerRef":format!("d:{document_id}"),"textNodeId":leaf.map(|id|ids[&id].clone()),"textNodeRef":leaf.map(|id|format!("d:{}",ids[&id])),"sourceRange":wire_range(&piece,units),"renderedRange":{"start":piece_rendered.start,"end":piece_rendered.end},"mapping":mapping,"textRef":reference});
            entries
                .rows
                .push((format!("h:{document_id}"), document_position, value));
            document_position += 1;
        }
        for &index in &admitted {
            let (owner, range) = owners[index];
            if raw.end <= range.start || raw.start >= range.end {
                continue;
            }
            let id = entries.id();
            let map = json!({"kind":"sourceMap","id":id,"profile":"canonicalNote","profileVersion":1,"ownerRef":format!("d:{owner}"),"textNodeId":leaf.map(|id|ids[&id].clone()),"textNodeRef":leaf.map(|id|format!("d:{}",ids[&id])),"sourceRange":wire_range(&raw,units),"renderedRange":{"start":rendered.start,"end":rendered.end},"mapping":mapping,"textRef":text_ref});
            let position = positions.entry(owner.clone()).or_default();
            entries.rows.push((format!("h:{owner}"), *position, map));
            *position += 1;
        }
    }
    if !document_ranges.is_empty() {
        let mut owner = json!({"kind":"boundary","id":document_id,"construct":"htmlDocument","profile":"canonicalNote","profileVersion":1,"entryPath":"html","sourceRange":{"start":0,"end":units[source.len()]},"nativeRef":format!("d:{}",ids[&0]),"attributesRef":attributes[&0]});
        entries
            .rows
            .push((format!("d:{document_id}"), 0, owner.clone()));
        owner["sourceMapRef"] = json!(format!("h:{document_id}"));
        for range in document_ranges {
            descriptors.push((
                units[range.start],
                units[range.end],
                document_id.clone(),
                owner.clone(),
            ));
        }
    }
}

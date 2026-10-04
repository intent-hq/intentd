//! Write-time Markdown code ownership and source normalization receipts.
use super::{canonical, TextMapping};
use crate::note_page_index::Entries;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use pulldown_cmark::{CodeBlockKind, Event, Options, Parser, Tag, TagEnd};
use serde_json::{json, Value};
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Range;

struct Output<'a> {
    html: String,
    current: &'a Cell<usize>,
    ranges: Vec<Option<Range<usize>>>,
}
impl fmt::Write for Output<'_> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let start = self.html.len();
        self.html.push_str(value);
        let range = &mut self.ranges[self.current.get()];
        if let Some(range) = range {
            range.end = self.html.len();
        } else {
            *range = Some(start..self.html.len());
        }
        Ok(())
    }
}

struct Code {
    raw: Range<usize>,
    body: Range<usize>,
    generated: Range<usize>,
    // HTML scalar/escape receipts. Long identity runs use bounded chunks.
    atoms: Vec<(Range<usize>, Range<usize>)>,
}

#[derive(Clone, Copy)]
enum Container {
    Quote,
    Indent(usize),
}

#[derive(Clone, Default)]
struct Prefix {
    at: usize,
    column: usize,
    remaining: usize,
}
impl Prefix {
    fn spaces(&mut self, bytes: &[u8], mut wanted: usize) -> bool {
        let pending = self.remaining.min(wanted);
        self.remaining -= pending;
        wanted -= pending;
        while wanted > 0 {
            let width = match bytes.get(self.at) {
                Some(b' ') => 1,
                Some(b'\t') => 4 - self.column % 4,
                _ => break,
            };
            self.at += 1;
            self.column += width;
            let consumed = wanted.min(width);
            wanted -= consumed;
            self.remaining = width - consumed;
        }
        wanted == 0
    }
}

fn skip_prefix(bytes: &[u8], containers: &[Container]) -> usize {
    let mut cursor = Prefix::default();
    for container in containers {
        let saved = cursor.clone();
        let matched = match container {
            Container::Quote => {
                cursor.spaces(bytes, 3);
                if bytes.get(cursor.at) == Some(&b'>') {
                    cursor.at += 1;
                    cursor.column += 1;
                    cursor.spaces(bytes, 1);
                    true
                } else {
                    false
                }
            }
            Container::Indent(width) => {
                cursor.spaces(bytes, *width)
                    || matches!(bytes.get(cursor.at), None | Some(b'\n' | b'\r'))
            }
        };
        if !matched {
            cursor = saved;
            break;
        }
    }
    cursor.at
}

fn marker_indent(source: &str, raw: &Range<usize>) -> usize {
    let bytes = source[raw.clone()].as_bytes();
    let mut cursor = Prefix::default();
    cursor.spaces(bytes, 3);
    if bytes.get(cursor.at).is_some_and(u8::is_ascii_digit) {
        while bytes.get(cursor.at).is_some_and(u8::is_ascii_digit) {
            cursor.at += 1;
            cursor.column += 1;
        }
    }
    cursor.at += 1;
    cursor.column += 1;
    let marker = cursor.clone();
    if cursor.spaces(bytes, 5) {
        cursor = marker;
        cursor.spaces(bytes, 1);
    }
    cursor.column - cursor.remaining
}

fn code_atoms<'a>(
    source: &'a str,
    body: Range<usize>,
    containers: &'a [Container],
    table: bool,
) -> impl Iterator<Item = (Range<usize>, char)> + 'a {
    let mut at = body.start;
    std::iter::from_fn(move || {
        if at >= body.end {
            return None;
        }
        let start = at;
        let ch = source[at..].chars().next().expect("code scalar");
        if table && ch == '\\' && source.as_bytes().get(at + 1) == Some(&b'|') {
            at += 2;
            return Some((start..at, '|'));
        }
        at += ch.len_utf8();
        let raw = start..at;
        if matches!(ch, '\r' | '\n') {
            at += skip_prefix(&source.as_bytes()[at..body.end], containers);
            Some((raw, ' '))
        } else {
            Some((raw, ch))
        }
    })
}

fn code_receipts(
    source: &str,
    raw: Range<usize>,
    generated: Range<usize>,
    containers: &[Container],
    table: bool,
) -> Code {
    let width = source[raw.clone()]
        .bytes()
        .take_while(|b| *b == b'`')
        .count();
    let body = raw.start + width..raw.end - width;
    // Two streaming passes avoid a per-scalar allocation for giant bodies.
    let mut first = None;
    let mut last = None;
    let mut nonspace = false;
    for (range, ch) in code_atoms(source, body.clone(), containers, table) {
        first.get_or_insert((range.clone(), ch));
        last = Some((range, ch));
        nonspace |= ch != ' ';
    }
    let trim = first.as_ref().is_some_and(|(_, ch)| *ch == ' ')
        && last.as_ref().is_some_and(|(_, ch)| *ch == ' ')
        && nonspace;
    let kept = if trim {
        first.expect("first").0.end..last.expect("last").0.start
    } else {
        body.clone()
    };
    let mut position = generated.start + "<code>".len();
    let mut atoms: Vec<(Range<usize>, Range<usize>)> = Vec::new();
    for (raw, ch) in code_atoms(source, body.clone(), containers, table)
        .filter(|(range, _)| range.start >= kept.start && range.end <= kept.end)
    {
        let bytes = match ch {
            '&' => 5,
            '<' | '>' => 4,
            _ => ch.len_utf8(),
        };
        let next = position + bytes;
        let identity = bytes == raw.len() && source[raw.clone()].starts_with(ch);
        if identity {
            if let Some((prior_html, prior_raw)) = atoms.last_mut() {
                if prior_html.end == position
                    && prior_raw.end == raw.start
                    && prior_html.len() == prior_raw.len()
                    && prior_html.len() + bytes <= crate::note_page_index::PIECE_BYTES
                {
                    prior_html.end = next;
                    prior_raw.end = raw.end;
                    position = next;
                    continue;
                }
            }
        }
        atoms.push((position..next, raw));
        position = next;
    }
    Code {
        raw,
        body,
        generated,
        atoms,
    }
}

fn raw_projection(code: &Code, range: &Range<usize>) -> Option<Range<usize>> {
    let first = code
        .atoms
        .partition_point(|(html, _)| html.end <= range.start);
    let mut result: Option<Range<usize>> = None;
    for (html, raw) in &code.atoms[first..] {
        if html.start >= range.end {
            break;
        }
        let selected = if html.len() == raw.len() {
            raw.start + range.start.max(html.start) - html.start
                ..raw.start + range.end.min(html.end) - html.start
        } else {
            raw.clone()
        };
        if let Some(result) = &mut result {
            result.end = selected.end;
        } else {
            result = Some(selected);
        }
    }
    result
}

pub(crate) fn append_codes(
    source: &str,
    units: &[usize],
    entries: &mut Entries,
    descriptors: &mut Vec<(usize, usize, String, Value)>,
) {
    // The existing entry path interprets raw HTML separately from Markdown.
    if source.trim().starts_with('<')
        && !source.trim().starts_with("<!--anchor:")
        && !source.contains("```ws-block")
    {
        return;
    }
    let events: Vec<_> = Parser::new_ext(source, Options::all())
        .into_offset_iter()
        .collect();
    let primitive_starts: BTreeSet<_> = events.iter().enumerate().filter_map(|(index, (event, _))| {
        matches!(event, Event::Start(Tag::CodeBlock(CodeBlockKind::Fenced(info))) if format!("language-{info}").split_ascii_whitespace().any(|class| matches!(class, "language-diff" | "language-mermaid"))).then_some(index)
    }).collect();
    if primitive_starts.is_empty()
        && !descriptors
            .iter()
            .any(|(_, _, _, value)| value["role"] == "code")
    {
        return;
    }
    let current = Cell::new(0);
    let mut output = Output {
        html: String::new(),
        current: &current,
        ranges: vec![None; events.len()],
    };
    let mut primitive_body = false;
    pulldown_cmark::html::write_html_fmt(
        &mut output,
        events.iter().enumerate().map(|(index, (event, _))| {
            current.set(index);
            if primitive_body {
                if matches!(event, Event::End(TagEnd::CodeBlock)) { primitive_body = false; }
                return Event::Html("".into());
            }
            if primitive_starts.contains(&index) {
                let Event::Start(Tag::CodeBlock(CodeBlockKind::Fenced(language))) = event else { unreachable!() };
                let mut body = String::new();
                for (part, _) in &events[index + 1..] {
                    match part {
                        Event::End(TagEnd::CodeBlock) => break,
                        Event::Text(text) => body.push_str(text),
                        _ => {}
                    }
                }
                let body = body.strip_suffix('\n').unwrap_or(&body);
                primitive_body = true;
                if matches!(language.as_ref(), "diff" | "mermaid") {
                    let encoded = STANDARD.encode(body.as_bytes());
                    return Event::Html(format!("<div data-type=\"{language}-block\" data-{language}-code=\"{encoded}\"></div>\n").into());
                }
                // The native Markdown renderer uses a pre/code fallback for
                // titled fences. Its schema recognizes the language class and
                // preserves raw code rather than the exact-language base64.
                let escape = |value: &str| value.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;").replace('\'', "&#039;");
                return Event::Html(format!("<pre><code class=\"language-{}\">{}</code></pre>\n", escape(language), escape(body)).into());
            }
            // The Markdown entry path treats source HTML as literal text. Only
            // the explicit generated primitive above creates native HTML atoms.
            match event {
                Event::Html(text) | Event::InlineHtml(text) => Event::Text(text.clone()),
                _ => event.clone(),
            }
        }),
    )
    .expect("String output cannot fail");
    let mut codes = Vec::new();
    let mut stack = Vec::new();
    let mut containers = Vec::new();
    let mut tables = 0;
    for ((event, raw), generated) in events.iter().zip(&output.ranges) {
        match event {
            Event::Start(tag) => {
                let container = match tag {
                    Tag::BlockQuote(_) => Some(Container::Quote),
                    Tag::Item | Tag::DefinitionListDefinition => {
                        Some(Container::Indent(marker_indent(source, raw)))
                    }
                    // Options::all includes the legacy footnote grammar, which
                    // does not strip the GFM-only four-space continuation.
                    _ => None,
                };
                stack.push((container.is_some(), matches!(tag, Tag::Table(_))));
                if let Some(container) = container {
                    containers.push(container);
                }
                if matches!(tag, Tag::Table(_)) {
                    tables += 1;
                }
            }
            Event::End(_) => {
                if let Some((container, table)) = stack.pop() {
                    if container {
                        containers.pop();
                    }
                    if table {
                        tables -= 1;
                    }
                }
            }
            Event::Code(_) => {
                if let Some(generated) = generated
                    .as_ref()
                    .filter(|range| output.html[(*range).clone()].starts_with("<code>"))
                {
                    codes.push(code_receipts(
                        source,
                        raw.clone(),
                        generated.clone(),
                        &containers,
                        tables > 0,
                    ));
                }
            }
            _ => {}
        }
    }
    let tree = canonical(&output.html);
    let mut active = Vec::new();
    let mut pending = vec![0];
    while let Some(id) = pending.pop() {
        active.push(id);
        pending.extend(tree.nodes[id].children.iter().rev().copied());
    }
    let mut maps: Vec<Vec<(Option<usize>, TextMapping)>> =
        (0..codes.len()).map(|_| Vec::new()).collect();
    // Receipt seeks avoid scanning every preceding code span for each text map.
    for leaf in active
        .iter()
        .copied()
        .filter(|id| tree.nodes[*id].kind == "text")
    {
        for map in &tree.nodes[leaf].maps {
            let first = codes.partition_point(|code| code.generated.end <= map.raw.start);
            for (offset_code, code) in codes[first..].iter().enumerate() {
                if code.generated.start >= map.raw.end {
                    break;
                }
                if map.mapping == "identity" {
                    let first_atom = code
                        .atoms
                        .partition_point(|(html, _)| html.end <= map.raw.start);
                    for (html, original) in &code.atoms[first_atom..] {
                        if html.start >= map.raw.end {
                            break;
                        }
                        let segment = html.start.max(map.raw.start)..html.end.min(map.raw.end);
                        let raw = if html.len() == original.len() {
                            original.start + segment.start - html.start
                                ..original.start + segment.end - html.start
                        } else {
                            original.clone()
                        };
                        let text =
                            &map.text[segment.start - map.raw.start..segment.end - map.raw.start];
                        let offset = map.text[..segment.start - map.raw.start]
                            .encode_utf16()
                            .count();
                        maps[first + offset_code].push((
                            Some(leaf),
                            TextMapping {
                                mapping: if source[raw.clone()] == *text {
                                    "identity"
                                } else {
                                    "normalized"
                                },
                                raw,
                                rendered: map.rendered.start + offset
                                    ..map.rendered.start + offset + text.encode_utf16().count(),
                                text: text.to_owned(),
                            },
                        ));
                    }
                } else if let Some(raw) = raw_projection(code, &map.raw) {
                    maps[first + offset_code].push((
                        Some(leaf),
                        TextMapping {
                            mapping: if map.text.is_empty() {
                                "omitted"
                            } else if source[raw.clone()] == map.text {
                                "identity"
                            } else {
                                "normalized"
                            },
                            raw,
                            rendered: map.rendered.clone(),
                            text: map.text.clone(),
                        },
                    ));
                }
            }
        }
    }
    let mut retained = BTreeSet::new();
    for id in active
        .iter()
        .copied()
        .filter(|id| matches!(tree.nodes[*id].kind, "diffBlock" | "mermaidBlock"))
    {
        let mut next = Some(id);
        while let Some(id) = next {
            if !retained.insert(id) {
                break;
            }
            next = tree.nodes[id].parent;
        }
    }
    for group in &maps {
        for (leaf, map) in group {
            if map.text.is_empty() {
                continue;
            }
            let mut next = *leaf;
            while let Some(id) = next {
                if !retained.insert(id) {
                    break;
                }
                next = tree.nodes[id].parent;
            }
        }
    }
    let mut pieces_by_leaf = BTreeMap::<usize, Vec<Range<usize>>>::new();
    for (leaf, map) in maps.iter().flatten() {
        if let Some(leaf) = leaf {
            pieces_by_leaf
                .entry(*leaf)
                .or_default()
                .push(map.raw.clone());
        }
    }
    let generated_ranges: Vec<_> = output
        .ranges
        .iter()
        .enumerate()
        .filter_map(|(event, range)| {
            range
                .as_ref()
                .filter(|r| !r.is_empty())
                .map(|range| (range, event))
        })
        .collect();
    let code_descriptors: BTreeMap<_, _> = descriptors
        .iter()
        .enumerate()
        .filter(|(_, (_, _, _, value))| value["role"] == "code")
        .map(|(index, (start, end, _, _))| ((*start, *end), index))
        .collect();
    let primitive_descriptors: BTreeMap<_, _> = descriptors
        .iter()
        .enumerate()
        .filter(|(_, (_, _, _, value))| value["construct"] == "codeBlock")
        .map(|(index, (start, end, _, _))| ((*start, *end), index))
        .collect();
    let direct_descriptors: BTreeMap<_, _> = entries
        .rows
        .iter()
        .enumerate()
        .filter(|(_, (_, position, value))| *position == 0 && value["construct"] == "codeBlock")
        .map(|(index, (collection, _, _))| (collection.clone(), index))
        .collect();
    let mut child_indices = vec![0; tree.nodes.len()];
    for node in &tree.nodes {
        for (index, child) in node.children.iter().enumerate() {
            child_indices[*child] = index;
        }
    }
    let ids: BTreeMap<_, _> = retained.iter().map(|id| (*id, entries.id())).collect();
    let range_value =
        |range: &Range<usize>| json!({"start":units[range.start],"end":units[range.end]});
    for &id in &retained {
        let node = &tree.nodes[id];
        let pieces = pieces_by_leaf.remove(&id).unwrap_or_default();
        let literal = pieces
            .iter()
            .cloned()
            .reduce(|a, b| a.start.min(b.start)..a.end.max(b.end));
        let generated_open = node.source.as_ref().and_then(|node| node.opening.as_ref());
        let owner_range = generated_open.and_then(|opening| {
            let index = generated_ranges.partition_point(|(range, _)| range.end <= opening.start);
            generated_ranges
                .get(index)
                .filter(|(range, _)| range.start <= opening.start)
                .map(|(_, event)| events[*event].1.clone())
        });
        let range = literal.or(owner_range).unwrap_or(0..0);
        let attributes_start = entries.rows.len();
        let attrs = entries.context_attributes(&if node.attributes.is_null() {
            json!({})
        } else {
            node.attributes.clone()
        });
        let primitive = match node.kind {
            "diffBlock" => Some("diff"),
            "mermaidBlock" => Some("mermaid"),
            _ => None,
        };
        if let Some(primitive) = primitive {
            let code_ref = entries.rows[attributes_start..]
                .iter()
                .find_map(|(_, _, value)| {
                    (value["key"] == "code")
                        .then(|| value["valueRef"].as_str())
                        .flatten()
                })
                .expect("primitive code field")
                .to_owned();
            entries
                .artifact_sources
                .push((format!("d:{}", ids[&id]), code_ref, primitive));
        }
        let child_index = child_indices[id];
        let mut native = json!({"kind":"nativeNode","id":ids[&id],"profile":"canonicalNote","profileVersion":1,
            "nodeType":node.kind,"nodeClass":if node.kind=="text" {"text"} else if primitive.is_some() {"atom"} else {"container"},
            "parentRef":node.parent.map(|parent|format!("d:{}",ids[&parent])),"childIndex":child_index,
            "sourceRange":range_value(&range),"provenance":if range.is_empty(){"implicit"}else{"explicit"},"attributesRef":attrs});
        if !node.marks.is_empty() {
            native["marksRef"] = json!(entries.context_attributes(&json!(node.marks)));
        }
        if pieces.windows(2).any(|pair| pair[0].end != pair[1].start) {
            let reference = format!("d:{}:pieces", ids[&id]);
            for (position, piece) in pieces.iter().enumerate() {
                let piece_id = entries.id();
                entries.rows.push((reference.clone(),position,json!({"kind":"sourcePiece","id":piece_id,"nodeRef":format!("d:{}",ids[&id]),"sourceRange":range_value(piece),"role":"body"})));
            }
            native["provenance"] = json!("repaired");
            native["sourcePiecesRef"] = json!(reference);
        }
        if primitive.is_some() {
            native["nativeRef"] = json!(format!("d:{}", ids[&id]));
            if let Some(index) = primitive_descriptors.get(&(units[range.start], units[range.end]))
            {
                let (_, _, owner, descriptor) = &mut descriptors[*index];
                descriptor["nativeRef"] = native["nativeRef"].clone();
                let direct = direct_descriptors
                    .get(&format!("d:{owner}"))
                    .expect("persisted lexical owner");
                entries.rows[*direct].2["nativeRef"] = native["nativeRef"].clone();
            }
            descriptors.push((
                units[range.start],
                units[range.end],
                ids[&id].clone(),
                native.clone(),
            ));
        }
        entries.rows.push((format!("d:{}", ids[&id]), 0, native));
    }
    for (code, mut group) in codes.into_iter().zip(maps) {
        let Some(index) = code_descriptors.get(&(units[code.raw.start], units[code.raw.end]))
        else {
            continue;
        };
        let (_, _, owner, descriptor) = &mut descriptors[*index];
        let native = group
            .iter()
            .find(|(_, map)| !map.text.is_empty())
            .and_then(|(leaf, _)| *leaf);
        descriptor["codeSource"] = json!({"profile":"canonicalNote","profileVersion":1,
            "openingRange":range_value(&(code.raw.start..code.body.start)),"bodyRange":range_value(&code.body),
            "closingRange":range_value(&(code.body.end..code.raw.end))});
        descriptor["nativeRef"] = json!(native.map(|id| format!("d:{}", ids[&id])));
        entries
            .rows
            .push((format!("d:{owner}"), 0, descriptor.clone()));
        descriptor["sourceMapRef"] = json!(format!("h:{owner}"));
        group.sort_by_key(|(_, map)| (map.raw.start, map.raw.end));
        let mut cursor = code.raw.start;
        let mut gaps = Vec::new();
        for (_, map) in &group {
            if cursor < map.raw.start {
                gaps.push((
                    None,
                    TextMapping {
                        raw: cursor..map.raw.start,
                        rendered: 0..0,
                        text: String::new(),
                        mapping: "omitted",
                    },
                ));
            }
            cursor = cursor.max(map.raw.end);
        }
        if cursor < code.raw.end {
            gaps.push((
                None,
                TextMapping {
                    raw: cursor..code.raw.end,
                    rendered: 0..0,
                    text: String::new(),
                    mapping: "omitted",
                },
            ));
        }
        group.extend(gaps);
        group.sort_by_key(|(_, map)| (map.raw.start, map.raw.end));
        for (position, (leaf, map)) in group.into_iter().enumerate() {
            let id = entries.id();
            let leaf = leaf.filter(|id| ids.contains_key(id));
            let text_ref =
                (!map.text.is_empty()).then(|| entries.fragment("renderedText", &map.text));
            entries.rows.push((format!("h:{owner}"),position,json!({"kind":"sourceMap","id":id,"profile":"canonicalNote","profileVersion":1,
                "ownerRef":format!("d:{owner}"),"textNodeId":leaf.map(|id|ids[&id].clone()),"textNodeRef":leaf.map(|id|format!("d:{}",ids[&id])),
                "sourceRange":range_value(&map.raw),"renderedRange":{"start":map.rendered.start,"end":map.rendered.end},"mapping":map.mapping,"textRef":text_ref})));
        }
    }
}

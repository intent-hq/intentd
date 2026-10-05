//! Write-time derived indexes. Legacy canonical TEXT and history remain intact.
//! A piece is at most 4096 UTF-8 bytes; reads seek its UTF-16 primary key.
use intent_core::{BoxFuture, Error, Note, Result};
use pulldown_cmark::{
    Alignment, BlockQuoteKind, Event, LinkType, MetadataBlockKind, Options, Parser, Tag,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{Row, SqliteConnection};
use std::collections::BTreeMap;

pub(crate) const PIECE_BYTES: usize = 4096;

/// Retire derived projections whenever the pinned parser, index implementation,
/// or recorded canonical schema oracle changes. This is distinct from raw rev.
pub(crate) fn profile_revision() -> &'static str {
    static REVISION: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
        let mut digest = Sha256::new();
        for input in [
            "canonicalNote:1:preserveAnchors:workspace:notePrimitives:mentions",
            include_str!("note_page_index.rs"),
            include_str!("note_page_html.rs"),
            include_str!("note_page_html/entry.rs"),
            include_str!("note_page_html/index.rs"),
            include_str!("note_page_html/markdown.rs"),
            include_str!("note_page_html/markdown_source.rs"),
            include_str!("note_page_html/primitive.rs"),
            include_str!("tests/fixtures/note_html_native.json"),
            include_str!("tests/fixtures/note_primitive_native.json"),
            include_str!("../../../Cargo.lock"),
        ] {
            digest.update(input.as_bytes());
            digest.update([0]);
        }
        format!("{:x}", digest.finalize())
    });
    &REVISION
}

pub(crate) async fn retire_changed_profiles(conn: &mut SqliteConnection) -> Result<()> {
    sqlx::query("UPDATE note_page_head SET indexed_rev=-1 WHERE profile_revision<>?")
        .bind(profile_revision())
        .execute(conn)
        .await
        .map_err(db_error)?;
    Ok(())
}

#[derive(Debug)]
pub(crate) struct Piece {
    pub start: usize,
    pub end: usize,
    pub byte_start: usize,
    pub scalar_start: usize,
    pub lf_start: usize,
    pub text: String,
}

pub(crate) fn pieces(text: &str) -> Vec<Piece> {
    let mut out = Vec::new();
    let (mut byte_start, mut start, mut scalar_start, mut lf_start) = (0, 0, 0, 0);
    let (mut units, mut scalars, mut lfs) = (0, 0, 0);
    for (byte, ch) in text.char_indices() {
        if byte - byte_start + ch.len_utf8() > PIECE_BYTES {
            out.push(Piece {
                start,
                end: units,
                byte_start,
                scalar_start,
                lf_start,
                text: text[byte_start..byte].into(),
            });
            (byte_start, start, scalar_start, lf_start) = (byte, units, scalars, lfs);
        }
        units += ch.len_utf16();
        scalars += 1;
        lfs += usize::from(ch == '\n');
    }
    out.push(Piece {
        start,
        end: units,
        byte_start,
        scalar_start,
        lf_start,
        text: text[byte_start..].into(),
    });
    out
}

pub(super) struct Entries {
    pub(super) rows: Vec<(String, usize, Value)>,
    pub(super) artifact_sources: Vec<(String, String, &'static str)>,
    serial: usize,
}
impl Entries {
    pub(super) fn id(&mut self) -> String {
        self.serial += 1;
        format!("{:016x}", self.serial)
    }
    pub(super) fn fragment(&mut self, field: &str, text: &str) -> String {
        let id = self.id();
        let collection = format!("f:{id}");
        let parts = pieces(text);
        for (i, part) in parts.iter().enumerate() {
            self.rows.push((collection.clone(), part.start, json!({"kind":"fragment","id":id,"field":field,"offset":part.start,"text":part.text,"nextRef": parts.get(i+1).map(|p| format!("{collection}@{}",p.start))})));
        }
        collection
    }
    pub(super) fn context_attributes(&mut self, value: &Value) -> String {
        let entry = self.metadata(value, "a", None, None, None);
        let collection = format!("a:{}:root", entry["id"].as_str().expect("metadata id"));
        self.rows.push((collection.clone(), 0, entry));
        collection
    }

    fn metadata(
        &mut self,
        value: &Value,
        namespace: &str,
        parent: Option<&str>,
        key: Option<&str>,
        index: Option<usize>,
    ) -> Value {
        let id = self.id();
        let mut entry = json!({"id":id,"parentId":parent});
        if let Some(key) = key {
            if key.len() <= 1024 && serde_json::to_string(key).expect("key encodes").len() <= 1024 {
                entry["key"] = json!(key);
            } else {
                entry["keyRef"] = json!(self.fragment("key", key));
            }
        }
        if let Some(index) = index {
            entry["index"] = json!(index);
        }
        match value {
            Value::Object(map) => {
                entry["type"] = json!("object");
                let collection = format!("{namespace}:{id}");
                entry["childrenRef"] = json!(collection);
                let sorted: BTreeMap<_, _> = map.iter().collect();
                for (position, (key, value)) in sorted.into_iter().enumerate() {
                    let child = self.metadata(value, namespace, Some(&id), Some(key), None);
                    self.rows.push((collection.clone(), position, child));
                }
            }
            Value::Array(array) => {
                entry["type"] = json!("array");
                let collection = format!("{namespace}:{id}");
                entry["childrenRef"] = json!(collection);
                for (position, value) in array.iter().enumerate() {
                    let child = self.metadata(value, namespace, Some(&id), None, Some(position));
                    self.rows.push((collection.clone(), position, child));
                }
            }
            Value::String(text) => {
                entry["type"] = json!("string");
                entry["valueRef"] = json!(self.fragment("value", text));
            }
            other => {
                entry["type"] = json!(match other {
                    Value::Null => "null",
                    Value::Bool(_) => "boolean",
                    _ => "number",
                });
                entry["value"] = other.clone();
            }
        }
        entry
    }
}

fn construct(tag: &Tag<'_>) -> &'static str {
    match tag {
        Tag::Paragraph => "paragraph",
        Tag::Heading { .. } => "heading",
        Tag::BlockQuote(_) => "blockquote",
        Tag::CodeBlock(_) => "codeBlock",
        Tag::List(_) => "list",
        Tag::Item => "listItem",
        Tag::Table(_) => "table",
        Tag::TableHead => "tableHead",
        Tag::TableRow => "tableRow",
        Tag::TableCell => "tableCell",
        Tag::Emphasis => "emphasis",
        Tag::Strong => "strong",
        Tag::Strikethrough => "strikethrough",
        Tag::Link { .. } => "link",
        Tag::Image { .. } => "image",
        Tag::HtmlBlock => "htmlBlock",
        Tag::FootnoteDefinition(_) => "footnoteDefinition",
        Tag::DefinitionList => "definitionList",
        Tag::DefinitionListTitle => "definitionListTitle",
        Tag::DefinitionListDefinition => "definitionListDefinition",
        Tag::Superscript => "superscript",
        Tag::Subscript => "subscript",
        Tag::MetadataBlock(_) => "metadataBlock",
    }
}

fn attributes(tag: &Tag<'_>) -> Vec<(String, String)> {
    match tag {
        Tag::CodeBlock(pulldown_cmark::CodeBlockKind::Fenced(info)) => vec![
            ("codeStyle".into(), "fenced".into()),
            ("info".into(), info.to_string()),
        ],
        Tag::CodeBlock(pulldown_cmark::CodeBlockKind::Indented) => {
            vec![("codeStyle".into(), "indented".into())]
        }
        Tag::List(start) => vec![(
            "listStart".into(),
            start.map_or_else(|| "unordered".into(), |n| n.to_string()),
        )],
        Tag::Table(alignments) => alignments
            .iter()
            .enumerate()
            .map(|(i, a)| {
                (
                    format!("alignment:{i}"),
                    match a {
                        Alignment::None => "none",
                        Alignment::Left => "left",
                        Alignment::Center => "center",
                        Alignment::Right => "right",
                    }
                    .into(),
                )
            })
            .collect(),
        Tag::Link {
            link_type,
            dest_url,
            title,
            id,
        }
        | Tag::Image {
            link_type,
            dest_url,
            title,
            id,
        } => {
            let wire_type = match link_type {
                LinkType::Inline => "Inline",
                LinkType::Reference => "Reference",
                LinkType::ReferenceUnknown => "ReferenceUnknown",
                LinkType::Collapsed => "Collapsed",
                LinkType::CollapsedUnknown => "CollapsedUnknown",
                LinkType::Shortcut => "Shortcut",
                LinkType::ShortcutUnknown => "ShortcutUnknown",
                LinkType::Autolink => "Autolink",
                LinkType::Email => "Email",
                LinkType::WikiLink { .. } => "WikiLink",
            };
            let mut fields = vec![
                ("linkType".into(), wire_type.into()),
                ("destination".into(), dest_url.to_string()),
                ("title".into(), title.to_string()),
                ("referenceId".into(), id.to_string()),
            ];
            if let LinkType::WikiLink { has_pothole } = link_type {
                fields.push(("hasPothole".into(), has_pothole.to_string()));
            }
            fields
        }
        Tag::Heading {
            level,
            id,
            classes,
            attrs,
        } => {
            let mut fields = vec![("level".into(), level.to_string())];
            if let Some(id) = id {
                fields.push(("headingId".into(), id.to_string()));
            }
            for (i, class) in classes.iter().enumerate() {
                fields.push((format!("class:{i}"), class.to_string()));
            }
            for (i, (key, value)) in attrs.iter().enumerate() {
                fields.push((format!("attributeKey:{i}"), key.to_string()));
                if let Some(value) = value {
                    fields.push((format!("attributeValue:{i}"), value.to_string()));
                }
            }
            fields
        }
        Tag::FootnoteDefinition(label) => vec![("label".into(), label.to_string())],
        Tag::BlockQuote(Some(kind)) => vec![(
            "quoteKind".into(),
            match kind {
                BlockQuoteKind::Note => "Note",
                BlockQuoteKind::Tip => "Tip",
                BlockQuoteKind::Important => "Important",
                BlockQuoteKind::Warning => "Warning",
                BlockQuoteKind::Caution => "Caution",
            }
            .into(),
        )],
        Tag::MetadataBlock(kind) => vec![(
            "metadataStyle".into(),
            match kind {
                MetadataBlockKind::YamlStyle => "YamlStyle",
                MetadataBlockKind::PlusesStyle => "PlusesStyle",
            }
            .into(),
        )],
        _ => Vec::new(),
    }
}

/// All byte-to-unit conversion and grammar parsing happens on writes. Membership
/// lists duplicate bounded descriptors, never giant construct bodies, per piece.
fn context_entries(text: &str, parts: &[Piece], entries: &mut Entries) {
    struct Parent {
        index: usize,
        full: std::ops::Range<usize>,
        first: Option<usize>,
        last: usize,
        fields: Vec<(String, String)>,
    }

    struct TablePosition {
        reference: String,
        row: usize,
        column: usize,
        alignments: Vec<Alignment>,
    }

    let entry_path = if crate::note_page_html::uses_html_entry(text) {
        "html"
    } else {
        "markdown"
    };
    let mut units = vec![0; text.len() + 1];
    let mut count = 0;
    for (byte, ch) in text.char_indices() {
        units[byte] = count;
        count += ch.len_utf16();
    }
    units[text.len()] = count;
    let mut tables: Vec<TablePosition> = Vec::new();
    let mut descriptors: Vec<(usize, usize, String, Value)> = Vec::new();
    let mut parents: Vec<Parent> = Vec::new();
    for (event, range) in Parser::new_ext(text, Options::all()).into_offset_iter() {
        if matches!(event, Event::End(_)) {
            if let Some(Parent {
                index,
                full,
                first,
                last,
                mut fields,
            }) = parents.pop()
            {
                let descriptor = &mut descriptors[index].3;
                let id = descriptor["id"].as_str().expect("boundary id").to_owned();
                let details = format!("d:{id}:details");
                let opening = &text[full.start..first.unwrap_or(full.end)];
                let closing = &text[last.max(first.unwrap_or(full.end))..full.end];
                fields.insert(0, ("closingSource".into(), closing.into()));
                fields.insert(0, ("openingSource".into(), opening.into()));
                for (position, (field, value)) in fields.into_iter().enumerate() {
                    let reference = entries.fragment(&field, &value);
                    entries.rows.push((details.clone(),position,json!({"kind":"fragment","id":format!("{id}:{position}"),"field":field,"offset":0,"text":"","nextRef":reference})));
                }
                descriptor["detailRef"] = json!(details);
                entries
                    .rows
                    .push((format!("d:{id}"), 0, descriptor.clone()));
            }
            if matches!(event, Event::End(pulldown_cmark::TagEnd::Table)) {
                tables.pop();
            }
            continue;
        }
        if let Some(Parent { first, last, .. }) = parents.last_mut() {
            first.get_or_insert(range.start);
            *last = range.end;
        }
        let id = entries.id();
        let start = units[range.start];
        let end = units[range.end];
        let mut descriptor = match &event {
            Event::Start(tag) => {
                json!({"kind":"boundary","id":id,"sourceRange":{"start":start,"end":end},"construct":construct(tag),"continuationBefore":false,"continuationAfter":false})
            }
            _ => {
                json!({"kind":"span","id":id,"sourceRange":{"start":start,"end":end},"role":match &event {
                    Event::Code(_)=>"code", Event::Html(s)|Event::InlineHtml(s) if s.starts_with("<!--anchor:")=>"commentMarker",
                    Event::Html(_)|Event::InlineHtml(_)=>"literal", Event::SoftBreak|Event::HardBreak=>"lineBreak",
                    Event::Rule=>"rule", Event::TaskListMarker(_)=>"taskMarker", _=>"text"
                }})
            }
        };
        if matches!(&event, Event::Start(Tag::Paragraph)) {
            descriptor["entryPath"] = json!(entry_path);
        }
        match &event {
            Event::Start(Tag::Table(alignments)) => tables.push(TablePosition {
                reference: format!("d:{id}"),
                row: 0,
                column: 0,
                alignments: alignments.clone(),
            }),
            Event::Start(Tag::TableHead | Tag::TableRow | Tag::TableCell) => {
                let table = tables.last_mut().expect("parser table ownership");
                if matches!(event, Event::Start(Tag::TableRow)) {
                    table.row += 1;
                    table.column = 0;
                }
                let mut position = json!({"tableRef":table.reference,"rowIndex":table.row});
                if matches!(event, Event::Start(Tag::TableCell)) {
                    position["columnIndex"] = json!(table.column);
                    position["alignment"] = json!(match table.alignments.get(table.column) {
                        Some(Alignment::Left) => "left",
                        Some(Alignment::Center) => "center",
                        Some(Alignment::Right) => "right",
                        _ => "none",
                    });
                    table.column += 1;
                }
                descriptor["tablePosition"] = position;
            }
            _ => {}
        }
        if let Some(Parent { index: parent, .. }) = parents.last() {
            descriptor["parentRef"] = json!(format!("d:{}", descriptors[*parent].2));
        }
        if let Event::Start(tag) = &event {
            parents.push(Parent {
                index: descriptors.len(),
                full: range.clone(),
                first: None,
                last: range.start,
                fields: attributes(tag),
            });
        }
        descriptors.push((start, end, id, descriptor));
    }
    crate::note_page_html::append(text, &units, entries, &mut descriptors);
    crate::note_page_html::append_codes(text, &units, entries, &mut descriptors);
    descriptors.sort_by(|a, b| (&a.0, &a.1, &a.2).cmp(&(&b.0, &b.1, &b.2)));
    let mut positions = vec![0; parts.len()];
    let mut document_occurrences = BTreeMap::<usize, usize>::new();
    for (start, end, _, descriptor) in descriptors {
        let first = parts.partition_point(|p| p.end <= start && p.end != p.start);
        for i in first..parts.len() {
            let part = &parts[i];
            if part.start >= end && end != start {
                break;
            }
            if descriptor["construct"] == "htmlDocument" {
                if let Some(index) = document_occurrences.get(&i) {
                    let admission = &mut entries.rows[*index].2["_admissionRange"];
                    let old_start = admission["start"].as_u64().expect("admission start");
                    let old_end = admission["end"].as_u64().expect("admission end");
                    *admission =
                        json!({"start":old_start.min(start as u64),"end":old_end.max(end as u64)});
                    continue;
                }
                document_occurrences.insert(i, entries.rows.len());
            }
            let mut occurrence = descriptor.clone();
            // Admission can include an implicit/repaired canonical ancestor whose
            // literal anchor is elsewhere. Keep this separate from wire provenance.
            occurrence["_admissionRange"] = json!({"start":start,"end":end});
            entries
                .rows
                .push((format!("c:{}", part.start), positions[i], occurrence));
            positions[i] += 1;
            if start == end {
                break;
            }
        }
    }
}

/// The legacy regex is `\[([^\]]+)\]\(intent://local/task/([^)]+)\)`.
/// All opening brackets before the same closing bracket share its delimiter:
/// after a failed delimiter we can skip that entire label in linear time.
/// This avoids compiling a regex on a deeply nested async writer's stack.
fn task_link_ranges(text: &str) -> impl Iterator<Item = std::ops::Range<usize>> + '_ {
    let mut position = 0;
    std::iter::from_fn(move || {
        while position < text.len() {
            let open = position + text[position..].find('[')?;
            let close = open + 1 + text[open + 1..].find(']')?;
            position = close + 1;
            if close == open + 1 {
                continue;
            }
            let Some(tail) = text[position..].strip_prefix("(intent://local/task/") else {
                continue;
            };
            let start = text.len() - tail.len();
            let end = start + tail.find(')')?;
            if start == end {
                continue;
            }
            position = end + 1;
            return Some(start..end);
        }
        None
    })
}

/// Preserve raw captures, including prose and code, in the write-time index.
fn task_entries(text: &str, entries: &mut Entries) -> usize {
    let mut seen = std::collections::BTreeSet::new();
    let mut previous_byte = 0;
    let mut previous_units = 0;
    for capture in task_link_ranges(text) {
        let id = &text[capture.clone()];
        if !seen.insert(id) {
            continue;
        }
        let index = seen.len() - 1;
        let start = previous_units + text[previous_byte..capture.start].encode_utf16().count();
        let end = start + id.encode_utf16().count();
        previous_byte = capture.end;
        previous_units = end;
        let mut item = json!({"index":index,"taskNoteIdLength":id.encode_utf16().count(),"sourceRange":{"start":start,"end":end}});
        if id.len() <= 256 {
            item["taskNoteId"] = json!(id);
        } else {
            item["taskNoteIdRef"] = json!(entries.fragment("taskNoteId", id));
        }
        entries.rows.push(("t:root".into(), index, item));
    }
    seen.len()
}

#[expect(clippy::needless_pass_by_value)] // Result::map_err transfers ownership.
fn db_error(e: sqlx::Error) -> Error {
    Error::Internal(format!("note page index: {e}"))
}

fn sql_offset(value: usize) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| Error::Internal("note index exceeds SQLite offset range".into()))
}

// Keep write-time parser and SQL state from inflating every enclosing legacy
// writer future (including workspace creation before its forge lookup).
pub(crate) fn rebuild<'a>(
    conn: &'a mut SqliteConnection,
    note: &'a Note,
    rev: i64,
    content_changed: bool,
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        let ws = &note.workspace_id.0;
        let id = &note.id.0;
        let mut entries = Entries {
            rows: Vec::new(),
            artifact_sources: Vec::new(),
            serial: 0,
        };
        // Separate namespaces keep metadata-only rewrites from colliding with context.
        entries.serial = 1_usize << 40;
        let metadata = json!({"title":note.title,"tags":note.tags,"parentId":note.parent_id,"metadata":note.metadata,"contentType":note.content_type,"isPinned":note.is_pinned,"isArchived":note.is_archived,"isDefault":note.is_default,"visibility":note.visibility,"createdAt":note.created_at,"updatedAt":note.updated_at});
        let root = entries.metadata(&metadata, "m", None, None, None);
        entries.rows.push(("m:root".into(), 0, root));
        // Metadata fragments occupy the high serial namespace. The classification
        // column avoids inspecting or hydrating any old JSON during invalidation.
        sqlx::query("DELETE FROM note_page_entry WHERE workspace_id=? AND note_id=? AND (collection LIKE 'm:%' OR collection >= 'f:0000010000000000' AND collection < 'g:')")
        .bind(ws).bind(id).execute(&mut *conn).await.map_err(db_error)?;
        if content_changed {
            sqlx::query("DELETE FROM note_page_piece WHERE workspace_id=? AND note_id=?")
                .bind(ws)
                .bind(id)
                .execute(&mut *conn)
                .await
                .map_err(db_error)?;
            sqlx::query("DELETE FROM note_page_entry WHERE workspace_id=? AND note_id=? AND (collection LIKE 'c:%' OR collection LIKE 'd:%' OR collection LIKE 't:%' OR collection LIKE 'h:%' OR collection < 'f:0000010000000000')").bind(ws).bind(id).execute(&mut *conn).await.map_err(db_error)?;
            let parts = pieces(&note.content);
            for part in &parts {
                sqlx::query("INSERT INTO note_page_piece VALUES (?,?,?,?,?,?,?,?)")
                    .bind(ws)
                    .bind(id)
                    .bind(sql_offset(part.start)?)
                    .bind(sql_offset(part.end)?)
                    .bind(&part.text)
                    .bind(sql_offset(part.byte_start)?)
                    .bind(sql_offset(part.scalar_start)?)
                    .bind(sql_offset(part.lf_start)?)
                    .execute(&mut *conn)
                    .await
                    .map_err(db_error)?;
            }
            entries.serial = 0;
            context_entries(&note.content, &parts, &mut entries);
            let task_count = task_entries(&note.content, &mut entries);
            sqlx::query("UPDATE note_page_head SET task_count=?,source_length=?,source_bytes=?,scalar_count=?,lf_count=? WHERE workspace_id=? AND note_id=?")
            .bind(sql_offset(task_count)?).bind(sql_offset(note.content.encode_utf16().count())?).bind(sql_offset(note.content.len())?).bind(sql_offset(note.content.chars().count())?).bind(sql_offset(note.content.bytes().filter(|b|*b==b'\n').count())?).bind(ws).bind(id).execute(&mut *conn).await.map_err(db_error)?;
        }
        for (collection, position, mut value) in entries.rows {
            let admission = value
                .as_object_mut()
                .expect("index object")
                .remove("_admissionRange")
                .unwrap_or_else(|| value["sourceRange"].clone());
            sqlx::query("INSERT INTO note_page_entry VALUES (?,?,?,?,?,?,?)")
                .bind(ws)
                .bind(id)
                .bind(collection)
                .bind(sql_offset(position)?)
                .bind(value.to_string())
                .bind(admission["start"].as_i64())
                .bind(admission["end"].as_i64())
                .execute(&mut *conn)
                .await
                .map_err(db_error)?;
        }
        for (native, source, primitive) in entries.artifact_sources {
            sqlx::query("INSERT INTO note_artifact_source(workspace_id,note_id,native_collection,source_collection,primitive) VALUES (?,?,?,?,?)")
                .bind(ws).bind(id).bind(native).bind(source).bind(primitive)
                .execute(&mut *conn).await.map_err(db_error)?;
        }
        sqlx::query("UPDATE note_page_head SET indexed_rev=?,profile_revision=? WHERE workspace_id=? AND note_id=?")
            .bind(rev)
            .bind(profile_revision())
            .bind(ws)
            .bind(id)
            .execute(&mut *conn)
            .await
            .map_err(db_error)?;
        Ok(())
    })
}

/// Used only during open/import/adoption, never from a read RPC. One legacy row
/// at a time bounds migration memory independently of workspace note count.
pub(crate) async fn rebuild_pending(conn: &mut SqliteConnection) -> Result<()> {
    loop {
        let row = sqlx::query("SELECT n.* FROM note n JOIN note_page_head h ON h.workspace_id=n.workspace_id AND h.note_id=n.id WHERE h.indexed_rev = -1 LIMIT 1").fetch_optional(&mut *conn).await.map_err(db_error)?;
        let Some(row) = row else {
            break;
        };
        let note = crate::note_repo::map_note_row(&row)?;
        rebuild(conn, &note, row.try_get("rev").map_err(db_error)?, true).await?;
    }
    Ok(())
}

#[cfg(test)]
mod task_link_tests {
    use super::task_link_ranges;

    #[test]
    fn task_scanner_matches_legacy_regex_captures() {
        let regex = regex::Regex::new(r"\[([^\]]+)\]\(intent://local/task/([^)]+)\)").unwrap();
        let tokens = [
            "[",
            "]",
            "(",
            ")",
            "x",
            "😀\r\n",
            "(intent://local/task/",
            "[a](intent://local/task/b)",
        ];
        // Exhaustive delimiter combinations include nested labels, empty captures,
        // malformed prefixes, raw Markdown in IDs, and adjacent valid links.
        for code in 0..tokens.len().pow(5) {
            let mut value = code;
            let mut source = String::new();
            for _ in 0..5 {
                source.push_str(tokens[value % tokens.len()]);
                value /= tokens.len();
            }
            let expected: Vec<_> = regex
                .captures_iter(&source)
                .map(|capture| capture.get(2).unwrap().range())
                .collect();
            assert_eq!(
                task_link_ranges(&source).collect::<Vec<_>>(),
                expected,
                "{source:?}"
            );
        }
    }
}

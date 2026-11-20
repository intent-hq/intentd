//! Display-only cleanup of a message's own leading capture envelopes.
//! Raw messages and search ranking remain untouched. Ambiguous batches fail closed.
use serde_json::{Map, Value};
use std::collections::HashSet;

pub(crate) const SCREEN_HEADER: &str = "[Screen evidence: screenshots lead the attached images in this order; [[mark:n]] in the message refers to mark n; [[clip:n]] and [[clip:n:end]] mark where recording n started and stopped while the user was speaking]";
const EDITED_HEADER: &str = "[Edited capture outputs]";
// Frontend String.length counts UTF-16 units. Valid UTF-8 uses at most three
// bytes per unit; reject grossly oversized input before JSON parsing.
const MAX_ENVELOPE_UTF16_UNITS: usize = 262_144;
const MAX_ENVELOPE_UTF8_BYTES: usize = 3 * MAX_ENVELOPE_UTF16_UNITS;

fn envelope_line(text: &str) -> Option<(&str, &str)> {
    let end = text
        .as_bytes()
        .iter()
        .take(MAX_ENVELOPE_UTF8_BYTES + 1)
        .position(|b| *b == b'\n')
        .unwrap_or(text.len());
    if end > MAX_ENVELOPE_UTF8_BYTES {
        return None;
    }
    let line = &text[..end];
    if line
        .encode_utf16()
        .take(MAX_ENVELOPE_UTF16_UNITS + 1)
        .count()
        > MAX_ENVELOPE_UTF16_UNITS
    {
        return None;
    }
    Some((line, &text[end..]))
}

fn owner(metadata: &Value) -> Option<&Value> {
    let metadata = if metadata.get("type").and_then(Value::as_str) == Some("dock_screen_evidence") {
        metadata
    } else {
        let parts = metadata.get("mergedMessageMetadata")?.as_array()?;
        if parts.len() != 1 {
            return None;
        }
        &parts[0]
    };
    (metadata.get("type").and_then(Value::as_str) == Some("dock_screen_evidence")
        && metadata.get("mergedMessageMetadata").is_none())
    .then_some(metadata)
}

fn screen(text: &str) -> Option<&str> {
    let body = text
        .strip_prefix(SCREEN_HEADER)?
        .strip_prefix("\n```json\n")?;
    let (line, after) = envelope_line(body)?;
    let json: Value = serde_json::from_str(line).ok()?;
    if json.get("type").and_then(Value::as_str) != Some("dock_screen_evidence") {
        return None;
    }
    let remaining = after.strip_prefix("\n```")?;
    if remaining.is_empty() {
        Some(remaining)
    } else {
        remaining.strip_prefix("\n\n")
    }
}

fn fields<'a>(
    value: &'a Value,
    required: &[&str],
    optional: &[&str],
) -> Option<&'a Map<String, Value>> {
    let obj = value.as_object()?;
    (required.iter().all(|key| obj.contains_key(*key))
        && obj
            .keys()
            .all(|key| required.contains(&key.as_str()) || optional.contains(&key.as_str())))
    .then_some(obj)
}
fn text(value: &Value, max: usize) -> bool {
    value
        .as_str()
        .is_some_and(|s| s.encode_utf16().count() <= max)
}
fn id(value: &Value) -> bool {
    value
        .as_str()
        .is_some_and(|s| !s.is_empty() && s.encode_utf16().count() <= 512)
}
fn nonnegative(value: &Value) -> bool {
    value.as_f64().is_some_and(|n| n.is_finite() && n >= 0.0)
}
fn positive(value: &Value) -> bool {
    value.as_f64().is_some_and(|n| n.is_finite() && n > 0.0)
}
fn finite(value: &Value) -> bool {
    value.as_f64().is_some_and(f64::is_finite)
}
fn point(v: &Value) -> bool {
    fields(v, &["x", "y"], &[]).is_some() && finite(&v["x"]) && finite(&v["y"])
}
fn geometry(v: &Value) -> bool {
    match v.get("kind").and_then(Value::as_str) {
        Some("point") => {
            fields(v, &["kind", "x", "y"], &[]).is_some() && finite(&v["x"]) && finite(&v["y"])
        }
        Some("rect") => {
            fields(v, &["kind", "x", "y", "w", "h"], &[]).is_some()
                && finite(&v["x"])
                && finite(&v["y"])
                && positive(&v["w"])
                && positive(&v["h"])
        }
        Some("arrow") => {
            fields(v, &["kind", "from", "to"], &[]).is_some()
                && point(&v["from"])
                && point(&v["to"])
        }
        Some("path") => {
            fields(v, &["kind", "points"], &[]).is_some()
                && v["points"]
                    .as_array()
                    .is_some_and(|p| !p.is_empty() && p.len() <= 4096 && p.iter().all(point))
        }
        Some("text") => {
            fields(
                v,
                &["kind", "x", "y", "text", "fontSize"],
                &["style", "width", "padding"],
            )
            .is_some()
                && finite(&v["x"])
                && finite(&v["y"])
                && text(&v["text"], 10000)
                && positive(&v["fontSize"])
                && v.get("style").is_none_or(|s| s == "sticky")
                && v.get("width")
                    .is_none_or(|w| positive(w) && w.as_f64().is_some_and(|w| w <= 4096.0))
                && v.get("padding")
                    .is_none_or(|p| nonnegative(p) && p.as_f64().is_some_and(|p| p <= 1024.0))
        }
        _ => false,
    }
}
fn mark(v: &Value) -> bool {
    fields(
        v,
        &[
            "id", "kind", "label", "note", "context", "geometry", "n", "at_ms",
        ],
        &[],
    )
    .is_some()
        && id(&v["id"])
        && v["n"].as_u64().is_some_and(|n| n > 0)
        && nonnegative(&v["at_ms"])
        && text(&v["label"], 2048)
        && text(&v["note"], 20000)
        && matches!(
            v["kind"].as_str(),
            Some("click" | "box" | "draw" | "arrow" | "element" | "text")
        )
        && geometry(&v["geometry"])
        && fields(&v["context"], &["selector", "text", "url"], &[]).is_some()
        && [("selector", 8192), ("text", 20000), ("url", 8192)]
            .iter()
            .all(|(key, max)| v["context"][key].is_null() || text(&v["context"][key], *max))
}
fn captures(v: &Value) -> bool {
    let Some(rows) = v.as_array().filter(|r| !r.is_empty() && r.len() <= 12) else {
        return false;
    };
    let mut ids = HashSet::new();
    rows.iter().all(|row| {
        if fields(
            row,
            &["id", "kind", "video", "voice", "transcript", "marks"],
            &["batches", "video_attachment_id", "frames", "kept_ms"],
        )
        .is_none()
            || !id(&row["id"])
            || !ids.insert(row["id"].clone())
            || !matches!(row["video"].as_str(), Some("full" | "highlights"))
            || !matches!(row["voice"].as_str(), Some("audio" | "transcript" | "off"))
            || !(row["transcript"].is_null() || row["transcript"].is_string())
            || row.get("batches").is_some_and(|b| !b.is_array())
        {
            return false;
        }
        let Some(marks) = row["marks"].as_array().filter(|m| m.len() <= 1000) else {
            return false;
        };
        let mut mark_ids = HashSet::new();
        let mut numbers = HashSet::new();
        if !marks
            .iter()
            .all(|m| mark(m) && mark_ids.insert(m["id"].clone()) && numbers.insert(m["n"].clone()))
        {
            return false;
        }
        match row["kind"].as_str() {
            Some("screenshot") => ["video_attachment_id", "frames", "kept_ms"]
                .iter()
                .all(|k| row.get(k).is_none()),
            Some("recording") => {
                if !(row
                    .get("video_attachment_id")
                    .is_some_and(|v| v.is_null() || id(v)))
                {
                    return false;
                }
                let Some(frames) = row["frames"].as_array().filter(|f| f.len() <= 1000) else {
                    return false;
                };
                let Some(ranges) = row["kept_ms"].as_array().filter(|r| r.len() <= 1000) else {
                    return false;
                };
                if row["video_attachment_id"].is_null()
                    && (!frames.is_empty() || !ranges.is_empty())
                {
                    return false;
                }
                let mut frame_ids = HashSet::new();
                if !frames.iter().all(|f| {
                    fields(f, &["mark_id", "attachment_id"], &[]).is_some()
                        && id(&f["mark_id"])
                        && id(&f["attachment_id"])
                        && mark_ids.contains(&f["mark_id"])
                        && frame_ids.insert(f["attachment_id"].clone())
                }) {
                    return false;
                }
                let mut end = 0.0;
                ranges.iter().all(|r| {
                    if fields(r, &["startMs", "endMs"], &[]).is_none()
                        || !nonnegative(&r["startMs"])
                        || !nonnegative(&r["endMs"])
                    {
                        return false;
                    }
                    let start = r["startMs"].as_f64().unwrap_or(-1.0);
                    let next = r["endMs"].as_f64().unwrap_or(-1.0);
                    let valid = start >= end && next >= start;
                    end = next;
                    valid
                })
            }
            _ => false,
        }
    })
}

pub(super) fn visible_capture_text<'a>(input: &'a str, metadata: Option<&Value>) -> &'a str {
    let Some(metadata) = metadata.and_then(owner) else {
        return input;
    };
    let Some(after_screen) = screen(input) else {
        return input;
    };
    let edited = metadata.get("edited_captures");
    if edited.is_none_or(|v| v.as_array().is_some_and(Vec::is_empty))
        && !after_screen.starts_with(EDITED_HEADER)
    {
        return after_screen;
    }
    let Some(edited) = edited else {
        return input;
    };
    let Some(body) = after_screen
        .strip_prefix(EDITED_HEADER)
        .and_then(|s| s.strip_prefix('\n'))
    else {
        return input;
    };
    let Some((line, after)) = envelope_line(body) else {
        return input;
    };
    if !captures(edited) {
        return input;
    }
    let Ok(parsed) = serde_json::from_str::<Value>(line) else {
        return input;
    };
    if &parsed != edited {
        return input;
    }
    if after.is_empty() {
        after
    } else {
        after.strip_prefix("\n\n").unwrap_or(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn screen() -> String {
        format!(
            "{SCREEN_HEADER}\n```json\n{}\n```",
            json!({
                "type":"dock_screen_evidence","version":1,"images":[{
                    "image":1,"size":[32,32],"scale":1,"marks":[]
                }]
            })
        )
    }

    #[test]
    fn only_owned_leading_screen_is_hidden() {
        let meta = json!({"type":"dock_screen_evidence"});
        let input = format!("{}\n\nPerson asks about deploy", screen());
        assert_eq!(
            visible_capture_text(&input, Some(&meta)),
            "Person asks about deploy"
        );
        assert_eq!(visible_capture_text(&input, None), input);
        let copied = format!("Person quotes this:\n\n{input}");
        assert_eq!(visible_capture_text(&copied, Some(&meta)), copied);
        let repeated = format!("{}\n\n{}", screen(), screen());
        assert_eq!(visible_capture_text(&repeated, Some(&meta)), screen());
    }

    #[test]
    fn singleton_wrapper_only_and_malformed_pair_fails_closed() {
        let meta = json!({"type":"dock_screen_evidence"});
        let input = format!("{}\n\nDeploy", screen());
        let single = json!({"mergedMessageMetadata":[meta]});
        assert_eq!(visible_capture_text(&input, Some(&single)), "Deploy");
        let multi = json!({"mergedMessageMetadata":[meta, null]});
        assert_eq!(visible_capture_text(&input, Some(&multi)), input);
        let nested = json!({"mergedMessageMetadata":[single]});
        assert_eq!(visible_capture_text(&input, Some(&nested)), input);
        let bad = format!(
            "{}\n\n[Edited capture outputs]\n{{bad}}\n\nDeploy",
            screen()
        );
        assert_eq!(visible_capture_text(&bad, Some(&meta)), bad);
        let declared = json!({"type":"dock_screen_evidence","edited_captures":[]});
        assert_eq!(visible_capture_text(&input, Some(&declared)), "Deploy");
    }

    #[test]
    fn edited_pair_requires_exact_valid_metadata_and_preserves_later_copies() {
        let edited = json!([{"id":"capture-1","kind":"screenshot","video":"full",
            "voice":"off","transcript":null,"marks":[]}]);
        let meta = json!({"type":"dock_screen_evidence","edited_captures":edited});
        let prefix = format!("{}\n\n[Edited capture outputs]\n{edited}", screen());
        let input = format!("{prefix}\n\nPerson asks\n\n{prefix}");
        assert_eq!(
            visible_capture_text(&input, Some(&meta)),
            format!("Person asks\n\n{prefix}")
        );
        let mismatch = json!({"type":"dock_screen_evidence","edited_captures":[{"id":"other"}]});
        assert_eq!(visible_capture_text(&input, Some(&mismatch)), input);
        let malformed = json!({"type":"dock_screen_evidence","edited_captures":[{}]});
        let text = format!("{}\n\n[Edited capture outputs]\n[{{}}]\n\nPerson", screen());
        assert_eq!(visible_capture_text(&text, Some(&malformed)), text);
    }

    #[test]
    fn edited_mark_id_budget_counts_frontend_utf16_units() {
        let mut edited = json!([{"id":"capture-1","kind":"screenshot","video":"full",
        "voice":"off","transcript":null,"marks":[{
            "id":"😀".repeat(256),"n":1,"at_ms":0,"kind":"box","label":"","note":"",
            "context":{"selector":null,"text":null,"url":null},
            "geometry":{"kind":"rect","x":0,"y":0,"w":1,"h":1}
        }]}]);
        let metadata = json!({"type":"dock_screen_evidence","edited_captures":edited});
        let input = format!(
            "{}\n\n[Edited capture outputs]\n{edited}\n\nPerson",
            screen()
        );
        assert_eq!(visible_capture_text(&input, Some(&metadata)), "Person");

        edited[0]["marks"][0]["id"] = json!("😀".repeat(300));
        let metadata = json!({"type":"dock_screen_evidence","edited_captures":edited});
        let input = format!(
            "{}\n\n[Edited capture outputs]\n{edited}\n\nPerson",
            screen()
        );
        assert_eq!(visible_capture_text(&input, Some(&metadata)), input);
    }

    #[test]
    fn envelope_budget_counts_frontend_utf16_units_not_utf8_bytes() {
        let metadata = json!({"type":"dock_screen_evidence"});
        let prefix = format!(
            "{SCREEN_HEADER}\n```json\n{}\n```",
            json!({
                "type":"dock_screen_evidence","future":"界".repeat(100_000)
            })
        );
        assert!(prefix.len() > 262_144);
        assert!(prefix.encode_utf16().count() < 262_144);
        let input = format!("{prefix}\n\nVisible words");
        assert_eq!(
            visible_capture_text(&input, Some(&metadata)),
            "Visible words"
        );
        let oversized = format!(
            "{SCREEN_HEADER}\n```json\n{}\n```\n\nVisible words",
            json!({
                "type":"dock_screen_evidence","future":"界".repeat(262_144)
            })
        );
        assert_eq!(visible_capture_text(&oversized, Some(&metadata)), oversized);
    }

    #[test]
    fn real_frontend_producer_contracts() {
        let fixtures: serde_json::Value =
            serde_json::from_str(include_str!("capture-preview-fixtures.json")).unwrap();
        for fixture in fixtures.as_array().unwrap() {
            let text = fixture["content"].as_str().unwrap();
            assert_eq!(
                visible_capture_text(text, fixture.get("metadata")),
                fixture["expected"].as_str().unwrap(),
                "{}",
                fixture["name"]
            );
        }
    }
}

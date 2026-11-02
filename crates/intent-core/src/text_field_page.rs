//! Paged reads of one large text field of a single definition
//! (`script.get`'s `command`, the hook read's `code`), so a definition
//! bigger than one reply is served in parts without dropping a byte.

use serde_json::{json, Value};

/// Bytes `c` takes inside a JSON string as `serde_json` writes it.
fn json_escaped_len(c: char) -> usize {
    match c {
        '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
        c if u32::from(c) < 0x20 => 6,
        c => c.len_utf8(),
    }
}

fn json_len(value: &Value) -> usize {
    serde_json::to_vec(value).map_or(0, |v| v.len())
}

/// The longest prefix of `text` whose JSON-escaped size fits `budget`.
fn prefix_within_json_bytes(text: &str, budget: usize) -> &str {
    let mut used = 0usize;
    for (i, c) in text.char_indices() {
        let len = json_escaped_len(c);
        if used + len > budget {
            return &text[..i];
        }
        used += len;
    }
    text
}

/// Page the text `field` of `row` so the row stays within `budget`
/// serialized bytes. The field is served from byte `offset`; a row that
/// already fits whole (offset 0) is left untouched. Any other row carries
/// the next part of the field plus `<field>Truncated: true`,
/// `<field>Bytes` (the full UTF-8 size), `<field>Offset` (where this part
/// starts) and `next<Field>Offset` (where the next part starts, on a UTF-8
/// boundary; `null` at the end). Concatenating the parts in order rebuilds
/// the field byte for byte.
///
/// # Errors
/// A message for `-32602` when a non-zero `offset` is not a character
/// boundary strictly inside the field.
pub fn page_text_field(
    row: &mut Value,
    field: &str,
    offset: usize,
    budget: usize,
) -> Result<(), String> {
    let full_text = row
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let full = full_text.len();
    if offset > 0 && (offset >= full || !full_text.is_char_boundary(offset)) {
        return Err(format!(
            "{field}Offset ({offset}) is not a character boundary inside the {field} ({full} bytes)"
        ));
    }
    if offset == 0 && json_len(row) <= budget {
        return Ok(());
    }
    let Some(obj) = row.as_object_mut() else {
        return Ok(());
    };
    let mut capital = field.to_owned();
    capital[..1].make_ascii_uppercase();
    let markers = [
        format!("{field}Truncated"),
        format!("{field}Bytes"),
        format!("{field}Offset"),
        format!("next{capital}Offset"),
    ];
    obj.insert(field.to_owned(), Value::String(String::new()));
    obj.insert(markers[0].clone(), Value::Bool(true));
    obj.insert(markers[1].clone(), json!(full));
    obj.insert(markers[2].clone(), json!(offset));
    obj.insert(markers[3].clone(), json!(full));
    let room = budget.saturating_sub(json_len(&Value::Object(obj.clone())));
    let rest = &full_text[offset..];
    let mut part = prefix_within_json_bytes(rest, room);
    if part.is_empty() {
        part = rest.chars().next().map_or("", |c| &rest[..c.len_utf8()]);
    }
    let end = offset + part.len();
    obj.insert(field.to_owned(), Value::String(part.to_owned()));
    if offset == 0 && end == full {
        for key in &markers {
            obj.remove(key);
        }
    } else {
        obj.insert(markers[3].clone(), json!((end < full).then_some(end)));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const BUDGET: usize = 480_000;

    fn rebuild(row: &Value) -> String {
        let mut out = String::new();
        let mut offset = 0;
        loop {
            let mut part = row.clone();
            page_text_field(&mut part, "command", offset, BUDGET).expect("page");
            assert!(
                json_len(&part) <= BUDGET,
                "part is {} bytes",
                json_len(&part)
            );
            assert_eq!(part["commandTruncated"], true);
            assert_eq!(part["commandBytes"], row["command"].as_str().unwrap().len());
            assert_eq!(part["commandOffset"], offset);
            assert_eq!(part["name"], "big", "other fields ride every part");
            out.push_str(part["command"].as_str().unwrap());
            match part["nextCommandOffset"].as_u64() {
                Some(next) => offset = usize::try_from(next).unwrap(),
                None => return out,
            }
        }
    }

    #[test]
    fn rebuilds_an_escape_heavy_multibyte_field_exactly() {
        let command = format!("{}é😀{}", "\"".repeat(250_001), "\\é".repeat(200_000));
        let row = json!({ "id": "s1", "name": "big", "command": command });
        assert_eq!(rebuild(&row), command);
    }

    #[test]
    fn a_row_within_budget_is_unchanged() {
        let mut row = json!({ "id": "s1", "command": "ls -la" });
        let before = row.clone();
        page_text_field(&mut row, "command", 0, BUDGET).unwrap();
        assert_eq!(row, before);
    }

    #[test]
    fn bad_offsets_are_rejected() {
        let row = json!({ "id": "s1", "command": "aé" });
        for bad in [2, 3, 4, 99] {
            let mut part = row.clone();
            assert!(
                page_text_field(&mut part, "command", bad, BUDGET).is_err(),
                "offset {bad}"
            );
        }
        let mut part = row.clone();
        page_text_field(&mut part, "command", 1, BUDGET).unwrap();
        assert_eq!(part["command"], "é");
        assert_eq!(part["nextCommandOffset"], Value::Null);
    }
}

//! Ignored, explicitly coordinated test peer. Never part of a production route.
//! Preparation gates execute only the spool/contract tests; no source lease is issued.
use super::Services;
use intent_core::{
    note_receipt_detail::NoteOperationReceiptRead,
    note_stage::{
        NoteStageAppend, NoteStageBegin, NoteStageCancel, NoteStageCommit, NoteStageSeal,
    },
    NoteId, WorkspaceApi, WorkspaceId,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{path::Path, time::Duration};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
mod spool {
    //! Bounded test-only framing and append-only evidence. Never production transport.
    use serde_json::Value;
    use std::{fs::OpenOptions, io::Write, path::Path};
    use tokio::io::{AsyncBufRead, AsyncBufReadExt};

    pub(super) const REQUEST_BYTES: usize = 65_536;
    pub(super) const RESPONSE_BYTES: usize = 8192;
    pub(super) const CONTROL_BYTES: usize = 4096;
    pub(super) const TOTAL_BYTES: usize = 48 * 1024 * 1024;
    const TERMINAL_RESERVE: usize = 65_536;
    pub(super) const SUMMARY_BYTES: usize = 131_072;

    pub(super) async fn line<R: AsyncBufRead + Unpin>(
        reader: &mut R,
        cap: usize,
    ) -> Result<Vec<u8>, String> {
        let mut output = Vec::new();
        loop {
            let available = reader.fill_buf().await.map_err(|e| e.to_string())?;
            if available.is_empty() {
                return Err("peer EOF before newline".into());
            }
            let newline = available.iter().position(|b| *b == b'\n');
            let take = newline.unwrap_or(available.len());
            if output.len().checked_add(take).is_none_or(|n| n > cap) {
                return Err("frame byte ceiling".into());
            }
            output.extend_from_slice(&available[..take]);
            reader.consume(take + usize::from(newline.is_some()));
            if newline.is_some() {
                return Ok(output);
            }
        }
    }

    struct Limited {
        bytes: Vec<u8>,
        cap: usize,
    }
    impl Write for Limited {
        fn write(&mut self, input: &[u8]) -> std::io::Result<usize> {
            if input.len() > self.cap.saturating_sub(self.bytes.len()) {
                return Err(std::io::Error::other("serialization byte ceiling"));
            }
            self.bytes.extend_from_slice(input);
            Ok(input.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    fn bounded_json(value: &Value, cap: usize) -> Result<Vec<u8>, String> {
        let mut writer = Limited {
            bytes: Vec::new(),
            cap,
        };
        serde_json::to_writer(&mut writer, value).map_err(|e| e.to_string())?;
        Ok(writer.bytes)
    }
    pub(super) struct Spool {
        file: std::fs::File,
        terminal: std::fs::File,
        poisoned: bool,
        terminal_poisoned: bool,
        terminal_chain: String,
        fail_after: Option<usize>,
        pub bytes: usize,
        chain: String,
        pub frames: usize,
        pub emergency: bool,
    }
    impl Spool {
        pub fn new(path: &Path) -> Result<Self, String> {
            Ok(Self {
                file: OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)
                    .map_err(|e| e.to_string())?,
                terminal: OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path.with_file_name("backend-terminal.jsonl"))
                    .map_err(|e| e.to_string())?,
                poisoned: false,
                terminal_poisoned: false,
                terminal_chain: "0".repeat(64),
                fail_after: None,
                bytes: 0,
                chain: "0".repeat(64),
                frames: 0,
                emergency: false,
            })
        }
        pub fn reserve(&self, bytes: usize) -> Result<(), String> {
            if self
                .bytes
                .checked_add(bytes)
                .is_none_or(|n| n > TOTAL_BYTES - if self.emergency { 0 } else { TERMINAL_RESERVE })
            {
                Err("artifact byte ceiling".into())
            } else {
                Ok(())
            }
        }
        pub fn record(&mut self, direction: &str, bytes: &[u8]) -> Result<(), String> {
            if self.terminal_poisoned || (self.poisoned && !self.emergency) {
                return Err("incomplete transcript is poisoned".into());
            }
            let prior = if self.emergency {
                &self.terminal_chain
            } else {
                &self.chain
            };
            let hash = crate::attachment_upload::sha256_hex(bytes);
            let chain = crate::attachment_upload::sha256_hex(
                format!("{prior}:{direction}:{hash}").as_bytes(),
            );
            let metadata = bounded_json(
                &serde_json::json!({"frame":self.frames,"direction":direction,"bytes":bytes.len(),"sha256":hash,"chain":chain,
                "channel":if self.emergency{"terminal"}else{"data"},"dataChainIncomplete":self.poisoned}),
                CONTROL_BYTES,
            )?;
            let charge = metadata.len() + bytes.len() + 2;
            self.reserve(charge)?;
            // Keep the FULL admitted debit even after partial write or sync failure.
            self.bytes += charge;
            self.frames += 1;
            let file = if self.emergency {
                &mut self.terminal
            } else {
                &mut self.file
            };
            let started = std::time::Instant::now();
            let result = (|| -> std::io::Result<()> {
                if let Some(n) = self.fail_after.take() {
                    file.write_all(&metadata[..n.min(metadata.len())])?;
                    return Err(std::io::Error::other("injected partial test write"));
                }
                file.write_all(&metadata)?;
                file.write_all(b"\n")?;
                file.write_all(bytes)?;
                file.write_all(b"\n")?;
                file.flush()?;
                file.sync_data()
            })();
            if result.is_err() || started.elapsed() > std::time::Duration::from_secs(30) {
                if self.emergency {
                    self.terminal_poisoned = true;
                } else {
                    self.poisoned = true;
                }
                return Err(result.err().map_or_else(
                    || "disk deadline after settlement".into(),
                    |e| e.to_string(),
                ));
            }
            if self.emergency {
                self.terminal_chain = chain;
            } else {
                self.chain = chain;
            }
            Ok(())
        }
        pub fn summary(&mut self, value: &Value) -> Result<(), String> {
            let bytes = bounded_json(value, SUMMARY_BYTES)?;
            if bytes.len() > SUMMARY_BYTES {
                return Err("summary ceiling".into());
            }
            self.record("summary", &bytes)
        }
    }
    #[derive(Default)]
    pub(super) struct Counts {
        total: usize,
        source: usize,
        roots: usize,
        detail: usize,
        text: usize,
        stage: usize,
    }
    impl Counts {
        pub fn admit(&mut self, method: &str, params: &Value) -> Result<(), String> {
            if self.total >= 768 {
                return Err("total exchange ceiling".into());
            }
            let (counter, cap) = match method {
                "note.get" => (&mut self.source, 64),
                "note.operation.read" => match params["kind"].as_str() {
                    Some("mapping" | "effects" | "inverse") => (&mut self.roots, 64),
                    Some("detail") => (&mut self.detail, 512),
                    Some("inverseText") => (&mut self.text, 16),
                    _ => return Err("read kind outside driver contract".into()),
                },
                "note.operation.begin"
                | "note.operation.append"
                | "note.operation.seal"
                | "note.operation.commit"
                | "note.operation.cancel"
                | "note.operationStatus" => (&mut self.stage, 16),
                _ => return Err("method outside driver contract".into()),
            };
            if *counter >= cap {
                return Err("category exchange ceiling".into());
            }
            *counter += 1;
            self.total += 1;
            Ok(())
        }
    }
    #[tokio::test]
    async fn linked_framing_accepts_exact_cap_and_rejects_overrun_before_parse() {
        use tokio::io::BufReader;
        let exact = b"abcd\nnext\n";
        let mut reader = BufReader::with_capacity(2, &exact[..]);
        assert_eq!(line(&mut reader, 4).await.unwrap(), b"abcd");
        assert_eq!(line(&mut reader, 4).await.unwrap(), b"next");
        let mut reader = BufReader::with_capacity(2, &b"abcde\n"[..]);
        assert!(line(&mut reader, 4).await.is_err());
        let mut reader = BufReader::new(&b"abc"[..]);
        assert!(line(&mut reader, 4).await.is_err());
    }
    #[test]
    fn linked_spool_and_category_limits_refuse_without_replacing_evidence() {
        let dir = crate::test_support::test_tempdir("linked-spool-");
        let path = dir.path().join("transcript.jsonl");
        let mut spool = Spool::new(&path).unwrap();
        spool.record("request", b"{}").unwrap();
        let original = std::fs::read(&path).unwrap();
        assert!(Spool::new(&path).is_err());
        spool.bytes = TOTAL_BYTES;
        assert!(spool.record("response", b"{}").is_err());
        assert_eq!(std::fs::read(&path).unwrap(), original);
        let mut counts = Counts::default();
        for _ in 0..16 {
            counts.admit("note.operationStatus", &Value::Null).unwrap();
        }
        assert!(counts.admit("note.operationStatus", &Value::Null).is_err());
        assert!(counts.admit("note.update", &Value::Null).is_err());
    }

    pub(super) async fn mixed_line<R: AsyncBufRead + Unpin>(
        reader: &mut R,
    ) -> Result<Vec<u8>, String> {
        use tokio::io::AsyncReadExt;
        const DATA: &[u8] = b"{\"jsonrpc\":\"2.0\",";
        const CONTROL: &[u8] = b"{\"control\":";
        let mut prefix = Vec::with_capacity(DATA.len());
        let cap = loop {
            prefix.push(reader.read_u8().await.map_err(|e| e.to_string())?);
            if prefix == CONTROL {
                break CONTROL_BYTES;
            }
            if prefix == DATA {
                break REQUEST_BYTES;
            }
            if !DATA.starts_with(&prefix) && !CONTROL.starts_with(&prefix) {
                return Err("invalid frame prefix".into());
            }
        };
        let remaining = line(reader, cap - prefix.len()).await?;
        prefix.extend_from_slice(&remaining);
        Ok(prefix)
    }

    pub(super) fn encode_frame(value: &Value, control: bool) -> Result<Vec<u8>, String> {
        use serde::ser::SerializeMap;
        use serde::Serializer as _;
        let object = value.as_object().ok_or("frame object")?;
        let key = if control { "control" } else { "jsonrpc" };
        let first = object.get(key).ok_or("frame prefix field")?;
        let mut writer = Limited {
            bytes: Vec::new(),
            cap: if control {
                CONTROL_BYTES
            } else {
                RESPONSE_BYTES
            },
        };
        let mut serializer = serde_json::Serializer::new(&mut writer);
        let mut map = serializer
            .serialize_map(Some(object.len()))
            .map_err(|e| e.to_string())?;
        map.serialize_entry(key, first).map_err(|e| e.to_string())?;
        for (k, v) in object {
            if k != key {
                map.serialize_entry(k, v).map_err(|e| e.to_string())?;
            }
        }
        map.end().map_err(|e| e.to_string())?;
        let bytes = writer.bytes;
        if bytes.len()
            > if control {
                CONTROL_BYTES
            } else {
                RESPONSE_BYTES
            }
        {
            return Err("outgoing frame cap".into());
        }
        Ok(bytes)
    }
    #[tokio::test]
    async fn linked_mixed_prefix_selects_control_cap_before_json_parse() {
        let mut oversized = b"{\"control\":".to_vec();
        oversized.resize(CONTROL_BYTES + 1, b' ');
        oversized.push(b'\n');
        let mut reader = tokio::io::BufReader::with_capacity(7, &oversized[..]);
        assert!(mixed_line(&mut reader).await.is_err());
        let mut reader = tokio::io::BufReader::new(&b" {\"control\":\"abort\"}\n"[..]);
        assert!(mixed_line(&mut reader).await.is_err());
        let encoded = encode_frame(
            &serde_json::json!({"control":"ready","contractHash":"abc"}),
            true,
        )
        .unwrap();
        assert!(encoded.starts_with(b"{\"control\":"));
        let encoded = encode_frame(
            &serde_json::json!({"jsonrpc":"2.0","id":1,"result":{}}),
            false,
        )
        .unwrap();
        assert!(encoded.starts_with(b"{\"jsonrpc\":\"2.0\","));
    }
    #[test]
    fn linked_partial_write_keeps_full_debit_and_separates_terminal_chain() {
        let dir = crate::test_support::test_tempdir("linked-partial-");
        let path = dir.path().join("transcript.jsonl");
        let mut spool = Spool::new(&path).unwrap();
        spool.fail_after = Some(7);
        assert!(spool.record("request", b"{}").is_err());
        let debit = spool.bytes;
        assert!(debit > 7);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 7);
        assert!(spool.record("response", b"{}").is_err());
        assert_eq!(spool.bytes, debit);
        spool.emergency = true;
        spool
            .summary(&serde_json::json!({"failure":"partial write; original chain incomplete"}))
            .unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 7);
        assert!(
            std::fs::read_to_string(dir.path().join("backend-terminal.jsonl"))
                .unwrap()
                .contains("dataChainIncomplete\":true")
        );
    }
}

use spool::{Counts, Spool, CONTROL_BYTES, REQUEST_BYTES, RESPONSE_BYTES, SUMMARY_BYTES};
mod oracle {
    //! Independent retained-state and complete peer receipt traversal assertions.
    use super::{require, DriverResult, Operation, Services};
    use serde_json::{json, Value};
    use std::collections::{BTreeMap, BTreeSet};

    #[derive(Default)]
    pub(super) struct Evidence {
        rows: BTreeMap<String, Vec<Value>>,
        cursors: BTreeMap<String, Value>,
        roots: BTreeMap<String, String>,
        string_units: usize,
        fields: usize,
        serialized: usize,
    }
    impl Evidence {
        pub fn observe(
            &mut self,
            request: &Value,
            page: &Value,
            receipt: &Value,
        ) -> DriverResult<()> {
            let kind = request["kind"].as_str().ok_or("receipt kind")?;
            for field in [
                "scope",
                "operationId",
                "headerDigest",
                "payloadDigest",
                "viewId",
            ] {
                require(page[field] == receipt[field], "receipt identity mismatch")?;
            }
            require(
                page["expiresAt"] == receipt["receiptExpiresAt"] && page["outputKind"] == kind,
                "receipt expiry/kind mismatch",
            )?;
            require(
                page["sourceLength"]
                    == if matches!(kind, "inverse" | "inverseText") {
                        3
                    } else {
                        2
                    },
                "receipt length domain mismatch",
            )?;
            let reference = request["ref"].as_str().ok_or("receipt ref")?;
            let key = format!("{kind}:{reference}");
            require(self.cursors.len() < 64, "oracle ref ceiling")?;
            if let Some(next) = self.cursors.get(&key) {
                require(
                    !next.is_null() && request.get("cursor") == Some(next),
                    "duplicate or invalid receipt cursor",
                )?;
            } else {
                require(request.get("cursor").is_none(), "unexpected initial cursor")?;
            }
            let next = page.get("nextCursor").ok_or("missing terminal cursor")?;
            let items = page["items"].as_array().ok_or("missing items")?;
            require(
                next.is_null() || (!items.is_empty() && request.get("cursor") != Some(next)),
                "nonadvancing cursor",
            )?;
            let cap = match kind {
                "effects" => 2,
                "inverse" | "inverseText" => 1,
                "detail" => 32,
                "mapping" => 64,
                _ => return Err("unexpected receipt kind".into()),
            };
            let current = self.rows.get(kind).map_or(0, Vec::len);
            require(current + items.len() <= cap, "exact fixture row ceiling")?;
            // Admit aggregate scalar/field and serialized budgets before retained copies.
            let mut fields = 0;
            let mut strings = 0;
            measure(&page["items"], &mut fields, &mut strings)?;
            measure(next, &mut fields, &mut strings)?;
            strings += key.encode_utf16().count();
            let bytes = serde_json::to_vec(&page["items"])
                .map_err(|e| e.to_string())?
                .len()
                + key.len()
                + next.to_string().len();
            require(
                self.fields + fields <= 8192
                    && self.string_units + strings <= 131_072
                    && self.serialized + bytes <= 131_072,
                "oracle summary admission",
            )?;
            self.fields += fields;
            self.string_units += strings;
            self.serialized += bytes;
            if kind == "detail" {
                for node in items
                    .iter()
                    .filter(|n| n.get("parentId") == Some(&Value::Null))
                {
                    require(
                        self.roots
                            .insert(
                                reference.into(),
                                node["id"].as_str().ok_or("root id")?.into(),
                            )
                            .is_none(),
                        "duplicate root response",
                    )?;
                }
            }
            self.cursors.insert(key, next.clone());
            self.rows
                .entry(kind.into())
                .or_default()
                .extend(items.iter().cloned());
            Ok(())
        }
        fn rows(&self, kind: &str) -> DriverResult<&[Value]> {
            require(
                self.cursors
                    .iter()
                    .filter(|(k, _)| k.starts_with(&format!("{kind}:")))
                    .all(|(_, v)| v.is_null()),
                "unterminated receipt pages",
            )?;
            self.rows
                .get(kind)
                .map(Vec::as_slice)
                .ok_or_else(|| "missing required receipt traversal".into())
        }
    }
    fn measure(value: &Value, fields: &mut usize, strings: &mut usize) -> DriverResult<()> {
        *fields += 1;
        require(
            *fields <= 8192 && *strings <= 131_072,
            "summary traversal ceiling",
        )?;
        match value {
            Value::String(s) => *strings += s.encode_utf16().count(),
            Value::Array(a) => {
                for v in a {
                    measure(v, fields, strings)?;
                }
            }
            Value::Object(o) => {
                for (k, v) in o {
                    *strings += k.encode_utf16().count();
                    measure(v, fields, strings)?;
                }
            }
            _ => {}
        }
        require(*strings <= 131_072, "summary string ceiling")
    }
    fn tree(
        id: &str,
        nodes: &BTreeMap<String, &Value>,
        seen: &mut BTreeSet<String>,
    ) -> DriverResult<Value> {
        require(
            seen.len() < 32 && seen.insert(id.into()),
            "detail cycle or size ceiling",
        )?;
        let node = nodes.get(id).ok_or("missing detail node")?;
        require(
            node.get("valueRef").is_none(),
            "tiny fixture scalar unexpectedly referenced",
        )?;
        if node["type"] != "object" {
            return node
                .get("value")
                .cloned()
                .ok_or("missing scalar")
                .map_err(Into::into);
        }
        let mut object = serde_json::Map::new();
        for (child_id, child) in nodes.iter().filter(|(_, n)| n["parentId"] == id) {
            let key = child["key"].as_str().ok_or("missing child key")?;
            require(
                object
                    .insert(key.into(), tree(child_id, nodes, seen)?)
                    .is_none(),
                "duplicate detail key",
            )?;
        }
        Ok(Value::Object(object))
    }
    fn exact_mapping(mapping: &[Value]) -> DriverResult<()> {
        require(
            mapping == [json!({"start":1,"end":1,"insertedLength":1})],
            "exact base-to-final mapping differs",
        )
    }
    #[test]
    fn linked_mapping_rejects_wrong_nonempty_base_to_final_correspondence() {
        exact_mapping(&[json!({"start":1,"end":1,"insertedLength":1})]).unwrap();
        assert!(exact_mapping(&[json!({"start":1,"end":1,"insertedLength":57})]).is_err());
        assert!(exact_mapping(&[json!({"start":2,"end":58,"insertedLength":0})]).is_err());
        assert!(exact_mapping(&[]).is_err());
    }
    pub(super) async fn verify(services: &Services, operation: &Operation) -> DriverResult<Value> {
        let receipt = operation.receipt.as_ref().ok_or("no committed receipt")?;
        let begin = operation.begin.as_ref().ok_or("no begin")?;
        let text = operation.text.as_ref().ok_or("no text")?["text"]
            .as_str()
            .ok_or("text missing")?;
        let group = operation.dirty.as_ref().ok_or("no dirty")?["localSequence"]
            .as_u64()
            .ok_or("no group")?;
        let phantom = &text[1..];
        let digest = |s: &str| crate::attachment_upload::sha256_hex(s.as_bytes());
        exact_mapping(operation.evidence.rows("mapping")?)?;
        let effects = operation.evidence.rows("effects")?;
        require(
            effects.len() == 2
                && effects[1]["kind"] == "annotationInvalidation"
                && effects[1]["sourceRevision"] == receipt["afterRevision"],
            "unexpected effects",
        )?;
        let effect = &effects[0];
        require(
            effect["kind"] == "sourceEffect"
                && effect["reason"] == "phantom-scrub"
                && effect["range"] == json!({"start":2,"end":58})
                && effect["insertedLength"] == 0
                && effect["beforeDigest"] == digest(phantom)
                && effect["afterDigest"] == digest("")
                && effect["inputState"] != effect["outputState"],
            "unexpected canonical phase",
        )?;
        let details = operation.evidence.rows("detail")?;
        let mut nodes = BTreeMap::new();
        for node in details {
            require(
                nodes
                    .insert(node["id"].as_str().ok_or("node id")?.to_owned(), node)
                    .is_none(),
                "duplicate node",
            )?;
        }
        for reference in std::iter::once(effect["detailRef"].as_str().ok_or("detailRef")?)
            .chain(details.iter().filter_map(|n| n["childrenRef"].as_str()))
        {
            require(
                operation
                    .evidence
                    .cursors
                    .get(&format!("detail:{reference}"))
                    == Some(&Value::Null),
                "unread reachable detail",
            )?;
        }
        let effect_root = operation
            .evidence
            .roots
            .get(effect["detailRef"].as_str().ok_or("effect detail ref")?)
            .ok_or("missing effect root")?;
        let mut reached = BTreeSet::new();
        let detail = tree(effect_root, &nodes, &mut reached)?;
        require(
            detail
                == json!({"inputState":effect["inputState"],"outputState":effect["outputState"],"range":{"start":2,"end":58},"removed":phantom,"inserted":""}),
            "actual detail mismatch",
        )?;
        let inverse = operation.evidence.rows("inverse")?;
        require(inverse.len() == 1, "one inverse required")?;
        let inverse = &inverse[0];
        let group_string = group.to_string();
        require(
            inverse["historyGroup"].as_str() == Some(group_string.as_str())
                && inverse["inputState"] == receipt["afterRevision"]
                && inverse["outputState"] == receipt["beforeRevision"]
                && inverse["start"] == 1
                && inverse["end"] == 2
                && inverse["replacement"]["length"] == 0
                && inverse["replacement"]["utf8Bytes"] == 0
                && inverse["replacement"]["sha256"] == digest(""),
            "native group inverse mismatch",
        )?;
        require(
            operation.evidence.rows("inverseText")?
                == vec![json!({"textId":inverse["replacement"]["textId"],"offset":0,"text":""})],
            "empty inverse fragment mismatch",
        )?;
        let provenance_ref = inverse["provenanceRef"]
            .as_str()
            .ok_or("missing inverse provenance")?;
        require(inverse["ordinal"] == 0, "inverse ordinal differs")?;
        require(
            operation
                .evidence
                .cursors
                .get(&format!("detail:{provenance_ref}"))
                == Some(&Value::Null),
            "inverse provenance not drained",
        )?;
        let provenance_root = operation
            .evidence
            .roots
            .get(provenance_ref)
            .ok_or("inverse provenance root")?;
        let provenance = tree(provenance_root, &nodes, &mut reached)?;
        require(
            provenance
                == json!({"kind":"sourceProvenance","inputState":inverse["inputState"],"outputState":inverse["outputState"],
            "baseRange":{"start":1,"end":1},"finalRange":{"start":1,"end":2},"replacement":inverse["replacement"]}),
            "inverse provenance differs",
        )?;
        require(
            reached.len() == nodes.len() && operation.evidence.roots.len() == 2,
            "extra or unreachable detail nodes",
        )?;
        let operation_key:String=sqlx::query_scalar("SELECT o.operation_key FROM note_operation o JOIN note_stage s ON s.operation_key=o.operation_key WHERE o.principal=? AND s.header_digest=? AND o.backend_id=? AND o.workspace_id=? AND o.note_id=? AND o.instance_id=? AND o.operation_id=? AND o.payload_digest=?")
        .bind("daemon").bind(begin["headerDigest"].as_str()).bind(begin["backendId"].as_str()).bind(begin["workspaceId"].as_str())
        .bind(begin["noteId"].as_str()).bind(begin["noteInstanceId"].as_str()).bind(begin["operationId"].as_str()).bind(receipt["payloadDigest"].as_str())
        .fetch_one(services.store.read_pool()).await.map_err(|e|e.to_string())?;
        let caller = format!("a{text}b");
        for (phase, expected) in [
            ("callerResult", caller.as_str()),
            (
                effect["inputState"].as_str().ok_or("input phase")?,
                caller.as_str(),
            ),
            (effect["outputState"].as_str().ok_or("output phase")?, "aXb"),
        ] {
            let chunks:Vec<String>=sqlx::query_scalar("SELECT text FROM note_operation_source WHERE operation_key=? AND phase=? ORDER BY start")
            .bind(&operation_key).bind(phase).fetch_all(services.store.read_pool()).await.map_err(|e|e.to_string())?;
            require(chunks.concat() == expected, "retained phase source differs")?;
        }
        let workspace =
            intent_core::WorkspaceId(begin["workspaceId"].as_str().ok_or("workspace")?.into());
        let note = intent_core::NoteId(begin["noteId"].as_str().ok_or("note")?.into());
        let final_source = services
            .store
            .get_note(&workspace, &note)
            .await
            .map_err(|e| e.to_string())?
            .content;
        require(final_source == "aXb", "final stored source differs")?;
        let comments: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM comment")
            .fetch_one(services.store.read_pool())
            .await
            .map_err(|e| e.to_string())?;
        require(
            comments == 0
                && services
                    .store
                    .list_notes(&workspace)
                    .await
                    .map_err(|e| e.to_string())?
                    .len()
                    == 1,
            "unexpected comments/tasks",
        )?;
        let restored = intent_core::note_mutation::apply_note_splices(
            &final_source,
            &[intent_core::note_mutation::NoteSplice {
                start: 1,
                end: 2,
                text: String::new(),
            }],
        )
        .map_err(|e| format!("{e:?}"))?;
        require(restored.source == "ab", "inverse does not restore base")?;
        Ok(
            json!({"baseLength":2,"callerLength":59,"finalLength":3,"nativeGroup":group,"phase":effect,"detail":detail,"inverse":inverse,
        "restoredSource":restored.source,"operationKey":operation_key,"principal":"daemon","claim":"internal scoped Store oracle plus actual peer receipt reads; no native adoption"}),
        )
    }
}

type DriverResult<T> = Result<T, String>;
fn require(condition: bool, message: &str) -> DriverResult<()> {
    condition.then_some(()).ok_or_else(|| message.into())
}
fn decode<T: serde::de::DeserializeOwned>(value: Value) -> DriverResult<T> {
    serde_json::from_value(value).map_err(|e| e.to_string())
}
fn deadline(value: &Value) -> DriverResult<u64> {
    let parsed = intent_core::parse_iso(value.as_str().ok_or("missing deadline")?)
        .ok_or("invalid deadline")?;
    u64::try_from(parsed.unix_timestamp_nanos() / 1_000_000).map_err(|e| e.to_string())
}
fn bounded_response(id: &Value, response: &Value) -> DriverResult<()> {
    spool::encode_frame(&json!({"jsonrpc":"2.0","id":id,"result":response}), false).map(|_| ())
}
#[test]
fn linked_response_encoding_enforces_wire_limit_including_escapes() {
    assert!(bounded_response(&json!(1), &json!("a".repeat(8100))).is_ok());
    assert!(bounded_response(&json!(1), &json!("\n".repeat(4100))).is_err());
    assert!(bounded_response(&json!(1), &json!("a".repeat(8192))).is_err());
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    jsonrpc: String,
    id: Value,
    method: String,
    params: Value,
}
#[derive(Default)]
struct Operation {
    begin: Option<Value>,
    first: Option<Value>,
    captured_at_ms: Option<u64>,
    commit_issued: bool,
    text: Option<Value>,
    dirty: Option<Value>,
    sealed: bool,
    receipt: Option<Value>,
    evidence: oracle::Evidence,
    service_error: Option<Value>,
}
impl Operation {
    fn call_deadline(&self, overall: tokio::time::Instant) -> DriverResult<tokio::time::Instant> {
        let now = tokio::time::Instant::now();
        require(now < overall, "absolute capture deadline before dispatch")?;
        let mut stop = overall;
        let expiry = if let Some(receipt) = &self.receipt {
            Some(&receipt["receiptExpiresAt"])
        } else if let Some(begin) = &self.begin {
            Some(&begin["expiresAt"])
        } else {
            self.first.as_ref().map(|v| &v["expiresAt"])
        };
        if let Some(expiry) = expiry {
            let left = deadline(expiry)?
                .checked_sub(intent_core::now_epoch_ms())
                .ok_or("phase deadline before dispatch")?;
            require(left > 0, "phase deadline before dispatch")?;
            stop = stop.min(now + Duration::from_millis(left));
        }
        Ok(stop)
    }

    fn identity(&self, params: &Value) -> DriverResult<()> {
        let begin = self.begin.as_ref().ok_or("begin required")?;
        for field in [
            "backendId",
            "workspaceId",
            "noteId",
            "noteInstanceId",
            "operationId",
            "headerDigest",
        ] {
            require(params[field] == begin[field], "operation identity changed")?;
        }
        Ok(())
    }
    async fn dispatch(&mut self, services: &Services, request: &Request) -> DriverResult<Value> {
        self.service_error = None;
        let p = &request.params;
        let method = request.method.as_str();
        if !matches!(method, "note.operation.begin" | "note.get") {
            self.identity(p)?;
        }
        let value = match method {
            "note.get" => {
                require(self.begin.is_none(), "lexical reads after stage begin")?;
                let page = &p["page"];
                require(
                    page["maxWireBytes"] == 8192 && page["maxItems"] == 64,
                    "lexical budgets",
                )?;
                let source = page["kind"] == "source";
                if source {
                    require(
                        self.first.is_none() && page["at"] == 0 && page["maxSourceBytes"] == 4096,
                        "one original source at0",
                    )?;
                    self.captured_at_ms = Some(intent_core::now_epoch_ms());
                } else {
                    require(
                        self.first.is_some()
                            && matches!(page["kind"].as_str(), Some("context" | "metadata"))
                            && page.get("maxSourceBytes").is_none(),
                        "context kind/budget",
                    )?;
                }
                let response = services
                    .get_note_page(
                        WorkspaceId(p["workspaceId"].as_str().ok_or("workspace")?.into()),
                        NoteId(p["noteId"].as_str().ok_or("note")?.into()),
                        decode(page.clone())?,
                        request.id.clone(),
                    )
                    .await
                    .map_err(|e| {
                        self.service_error = Some(json!({"code":e.code(),"message":e.to_string()}));
                        e.to_string()
                    })?;
                if source {
                    require(
                        response["text"] == "ab"
                            && response["sourceLength"] == 2
                            && response["range"] == json!({"start":0,"end":2})
                            && response.get("nextCursor") == Some(&Value::Null),
                        "base source differs",
                    )?;
                    self.first = Some(response.clone());
                } else {
                    let first = self.first.as_ref().unwrap();
                    for field in ["scope", "sourceRevision", "snapshotId", "expiresAt"] {
                        require(
                            response[field] == first[field],
                            "original lexical identity differs",
                        )?;
                    }
                }
                response
            }
            "note.operation.begin" => {
                require(self.begin.is_none(), "duplicate begin")?;
                let first = self.first.as_ref().ok_or("original source required")?;
                for field in ["backendId", "workspaceId", "noteId", "noteInstanceId"] {
                    require(p[field] == first["scope"][field], "foreign lexical scope")?;
                }
                require(
                    p["header"]["baseRevision"] == first["sourceRevision"]
                        && p["header"]["action"] == "mutate"
                        && p["header"]["output"] == "source"
                        && p["header"]["selection"] == "all",
                    "unexpected mutation header",
                )?;
                require(
                    deadline(&p["expiresAt"])? <= deadline(&first["expiresAt"])?
                        && deadline(&p["expiresAt"])? > intent_core::now_epoch_ms(),
                    "operation deadline must remain within original lexical lease",
                )?;
                let typed: NoteStageBegin = decode(p.clone())?;
                let response = services.note_operation_begin(typed).await.map_err(|e| {
                    self.service_error = Some(json!({"code":e.code(),"message":e.to_string()}));
                    e.to_string()
                })?;
                self.begin = Some(p.clone());
                response
            }
            "note.operation.append" => {
                require(!self.sealed && self.receipt.is_none(), "append after seal")?;
                let typed: NoteStageAppend = decode(p.clone())?;
                require(
                    typed.sequence == 0
                        && typed.previous_digest.is_none()
                        && typed.records.len() == 1,
                    "fixture requires one record per stream",
                )?;
                let record = typed.records[0].clone();
                match p["stream"].as_str() {
                    Some("text") => {
                        require(
                            self.text.is_none()
                                && record["kind"] == "text"
                                && record["offset"] == 0,
                            "unexpected text stream",
                        )?;
                        let text = record["text"].as_str().ok_or("text missing")?;
                        require(
                            text.is_ascii()
                                && text.len() == 57
                                && text.starts_with("X<!--anchor:")
                                && text.ends_with(":point-->"),
                            "unexpected phantom text",
                        )?;
                        let id = &text[12..48];
                        require(uuid::Uuid::parse_str(id).is_ok(), "invalid point UUID")?;
                    }
                    Some("dirty") => {
                        require(
                            self.dirty.is_none()
                                && record["kind"] == "splice"
                                && record["ordinal"] == 0
                                && record["start"] == 1
                                && record["end"] == 1,
                            "unexpected dirty stream",
                        )?;
                        require(
                            record["localSequence"].as_u64().is_some_and(|v| v > 0)
                                && record["localSequence"]
                                    == self.begin.as_ref().unwrap()["header"]["localEditSequence"],
                            "native group fence mismatch",
                        )?;
                    }
                    _ => return Err("live/selection/mutation streams must be empty".into()),
                }
                let response = services.note_operation_append(typed).await.map_err(|e| {
                    self.service_error = Some(json!({"code":e.code(),"message":e.to_string()}));
                    e.to_string()
                })?;
                if p["stream"] == "text" {
                    self.text = Some(record.clone());
                } else {
                    self.dirty = Some(record.clone());
                }
                response
            }
            "note.operation.seal" => {
                require(
                    !self.sealed && self.text.is_some() && self.dirty.is_some(),
                    "seal requires exact two streams",
                )?;
                let text = self.text.as_ref().unwrap();
                let replacement = &self.dirty.as_ref().unwrap()["replacement"];
                require(
                    replacement["textId"] == text["id"]
                        && replacement["length"] == 57
                        && replacement["utf8Bytes"] == 57
                        && replacement["sha256"]
                            == crate::attachment_upload::sha256_hex(
                                text["text"].as_str().unwrap().as_bytes(),
                            ),
                    "replacement identity mismatch",
                )?;
                let response = services
                    .note_operation_seal(decode::<NoteStageSeal>(p.clone())?)
                    .await
                    .map_err(|e| {
                        self.service_error = Some(json!({"code":e.code(),"message":e.to_string()}));
                        e.to_string()
                    })?;
                self.sealed = true;
                response
            }
            "note.operation.commit" => {
                require(
                    self.sealed && self.receipt.is_none(),
                    "commit requires one sealed operation",
                )?;
                self.commit_issued = true;
                let response = services
                    .note_operation_commit(decode::<NoteStageCommit>(p.clone())?)
                    .await
                    .map_err(|e| {
                        self.service_error = Some(json!({"code":e.code(),"message":e.to_string()}));
                        e.to_string()
                    })?;
                require(
                    response["kind"] == "noteCommitReceipt"
                        && response["outcome"] == "committed"
                        && response["sourceLength"] == 3,
                    "actual Services canonical result differed",
                )?;
                self.receipt = Some(response.clone());
                response
            }
            "note.operation.read" => {
                require(
                    self.receipt.is_some() && p.get("payloadDigest").is_none(),
                    "receipt read requires committed header selector",
                )?;
                let kind = p["kind"].as_str().ok_or("read kind")?;
                let items = match kind {
                    "mapping" | "effects" | "inverse" => 64,
                    "detail" => 16,
                    "inverseText" => 1,
                    _ => return Err("unsupported receipt kind".into()),
                };
                require(
                    p["maxItems"] == items && p["maxWireBytes"] == 8192,
                    "receipt budget mismatch",
                )?;
                if kind == "inverseText" {
                    require(p["maxSourceBytes"] == 4096, "inverse text budget mismatch")?;
                }
                let typed: NoteOperationReceiptRead = decode(p.clone())?;
                services
                    .get_note_receipt_detail(
                        typed.query().map_err(|e| format!("{e:?}"))?,
                        request.id.clone(),
                    )
                    .await
                    .map_err(|e| {
                        self.service_error = Some(json!({"code":e.code(),"message":e.to_string()}));
                        e.to_string()
                    })?
            }
            "note.operationStatus" => services
                .note_operation_status(decode(p.clone())?)
                .await
                .map_err(|e| {
                    self.service_error = Some(json!({"code":e.code(),"message":e.to_string()}));
                    e.to_string()
                })?,
            "note.operation.cancel" => {
                require(!self.commit_issued, "cannot cancel issued commit")?;
                services
                    .note_operation_cancel(decode::<NoteStageCancel>(p.clone())?)
                    .await
                    .map_err(|e| {
                        self.service_error = Some(json!({"code":e.code(),"message":e.to_string()}));
                        e.to_string()
                    })?
            }
            _ => return Err("method outside test contract".into()),
        };
        bounded_response(&request.id, &value)?;
        Ok(value)
    }
}

async fn send_frame(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    spool: &mut Spool,
    value: &Value,
    control: bool,
    until: Option<tokio::time::Instant>,
) -> DriverResult<()> {
    let bytes = spool::encode_frame(value, control)?;
    require(
        bytes.len()
            <= if control {
                CONTROL_BYTES
            } else {
                RESPONSE_BYTES
            },
        "outgoing frame cap",
    )?;
    spool.record(if control { "control-out" } else { "response" }, &bytes)?;
    let send = async {
        writer.write_all(&bytes).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await
    };
    let send = std::pin::pin!(send);
    settled(send, spool, "socket-write", until)
        .await?
        .map_err(|e: std::io::Error| e.to_string())
}
// A timeout records failure immediately but retains the sole future until it really
// settles. If the supervisor kills the process, the transcript has unknown debt.
async fn settled<F: std::future::Future>(
    mut future: std::pin::Pin<&mut F>,
    spool: &mut Spool,
    label: &str,
    until: Option<tokio::time::Instant>,
) -> DriverResult<F::Output> {
    let stop = until.map_or(tokio::time::Instant::now() + Duration::from_secs(30), |d| {
        d.min(tokio::time::Instant::now() + Duration::from_secs(30))
    });
    require(
        tokio::time::Instant::now() < stop,
        "deadline refused before first poll",
    )?;
    if let Ok(value) = tokio::time::timeout_at(stop, future.as_mut()).await {
        if tokio::time::Instant::now() >= stop {
            spool.emergency = true;
            spool.summary(&json!({"failure":"IO completion exceeded deadline","outstanding":label,"settlement":"completion observed; completed future is not polled again"}))?;
            return Err(format!("{label} completed after IO deadline"));
        }
        Ok(value)
    } else {
        let diagnostic = json!({"failure":"IO timeout","outstanding":label,"settlement":"unknown; borrower retained"});
        spool.emergency = true;
        let saved = spool.summary(&diagnostic);
        let _ = future.await;
        saved?;
        spool.summary(&json!({"failure":"IO timeout","outstanding":label,"settlement":"late completion observed; run failed"}))?;
        Err(format!("{label} exceeded IO deadline"))
    }
}
async fn conversation(
    services: &Services,
    workspace: &WorkspaceId,
    note: &NoteId,
    socket: tokio::net::UnixStream,
    spool: &mut Spool,
    operation: &mut Operation,
    contract: &str,
) -> DriverResult<Value> {
    let (read, mut write) = socket.into_split();
    let mut reader = BufReader::with_capacity(4096, read);
    let initial_state = {
        let future =
            std::pin::pin!(services.get_note_page_state(workspace.clone(), note.clone(), None));
        settled(future, spool, "initial state", None)
            .await?
            .map_err(|e| e.to_string())?
    };
    let ready = json!({"control":"ready","contractHash":contract,"backendHead":std::env::var("NOTE_LINKED_CAPTURE_HEAD").map_err(|e|e.to_string())?,"principal":"daemon","workspaceId":workspace.0,"noteId":note.0,"initialState":initial_state});
    let started = tokio::time::Instant::now();
    let overall = started + Duration::from_secs(120);
    send_frame(&mut write, spool, &ready, true, Some(overall)).await?;
    let mut counts = Counts::default();
    let mut last_id = 0;
    let outcome = async {

    loop {
        require(
            started.elapsed() < Duration::from_secs(120),
            "post-ready wall deadline",
        )?;
        if let Some(first) = &operation.first {
            if !operation.commit_issued {
                require(
                    intent_core::now_epoch_ms() < deadline(&first["expiresAt"])?,
                    "original lexical deadline",
                )?;
            }
        }
        // Reserve worst-case current exchange plus bounded summary before admitting it.
        spool.reserve(REQUEST_BYTES + RESPONSE_BYTES + SUMMARY_BYTES + 4096)?;
        let bytes = tokio::time::timeout(
            Duration::from_secs(30).min(Duration::from_secs(120).saturating_sub(started.elapsed())),
            spool::mixed_line(&mut reader),
        )
        .await
        .map_err(|_| "request IO deadline")??;
        let call_until = operation.call_deadline(overall)?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
        if value.get("control").is_some() {
            require(
                bytes.len() <= CONTROL_BYTES
                    && value.as_object().is_some_and(|o| o.len() == 2)
                    && value["contractHash"] == contract,
                "control identity/size",
            )?;
            spool.record("control-in", &bytes)?;
            match value["control"].as_str() {
                Some("abort") => return Err("peer aborted; delivery revoked".into()),
                Some("finish") => {
                    let summary = { let future=std::pin::pin!(oracle::verify(services,operation)); settled(future,spool,"receipt oracle",Some(call_until)).await?? };
                    let call_until = operation.call_deadline(overall)?;
                    let final_state = {
                        let future = std::pin::pin!(services.get_note_page_state(
                            workspace.clone(),
                            note.clone(),
                            None
                        ));
                        settled(future, spool, "final state", Some(call_until))
                            .await?
                            .map_err(|e| e.to_string())?
                    };
                    require(final_state["scope"] == operation.receipt.as_ref().unwrap()["scope"] && final_state["sourceRevision"] == operation.receipt.as_ref().unwrap()["afterRevision"], "final state does not bind receipt")?;
                    spool.summary(&summary)?;
                    let call_until = operation.call_deadline(overall)?;
                    send_frame(&mut write,spool,&json!({"control":"finish","contractHash":contract,"ok":true,"baseLength":2,"callerLength":59,"finalLength":3,"nativeGroup":summary["nativeGroup"],"summary":{"finalState":final_state,"backendBytesBeforeFinish":spool.bytes}}),true,Some(call_until)).await?;
                    { let future=std::pin::pin!(write.shutdown()); settled(future,spool,"socket shutdown",Some(call_until)).await?.map_err(|e|e.to_string())?; }
                    return Ok(summary);
                }
                _ => return Err("unexpected control".into()),
            }
        }
        let request: Request = decode(value)?;
        let id = request
            .id
            .as_u64()
            .ok_or("positive integer RPC id required")?;
        require(
            request.jsonrpc == "2.0"
                && id <= 9_007_199_254_740_991
                && ((last_id == 0 && id == 1) || (last_id > 0 && id > last_id)),
            "RPC identity/order",
        )?;
        require(
            request.params["workspaceId"] == workspace.0 && request.params["noteId"] == note.0,
            "foreign driver note",
        )?;
        counts.admit(&request.method, &request.params)?;
        spool.record("request", &bytes)?;
        let call_until = operation.call_deadline(overall)?;
        let response_result = {
            let future = std::pin::pin!(operation.dispatch(services, &request));
            settled(future, spool, "Services call", Some(call_until)).await?
        };
        let response = match response_result {
            Ok(value) => value,
            Err(error) => {
                if let Some(service_error) = &operation.service_error {
                    send_frame(
                        &mut write,
                        spool,
                        &json!({"jsonrpc":"2.0","id":request.id,"error":service_error}),
                        false,
                        Some(call_until),
                    )
                    .await?;
                }
                return Err(error);
            }
        };
        require(
            started.elapsed() < Duration::from_secs(120),
            "post-ready wall deadline after Services settlement",
        )?;
        if request.method == "note.operation.read" {
            operation.evidence.observe(
                &request.params,
                &response,
                operation.receipt.as_ref().ok_or("no receipt")?,
            )?;
        }
        if request.method == "note.get" && operation.captured_at_ms.is_some() && last_id == 0 {
            spool.summary(&json!({"capturedAtMs":operation.captured_at_ms,"lexicalExpiresAt":response["expiresAt"],"principal":"daemon"}))?;
        }
        send_frame(
            &mut write,
            spool,
            &json!({"jsonrpc":"2.0","id":request.id,"result":response}),
            false,
            Some(call_until),
        )
        .await?;
        last_id = id;
    }
    }.await;
    if outcome.is_err() {
        spool.emergency = true;
        let control = json!({"control":"failure","contractHash":contract,"kind":"harness","diagnostic":"capture failed; inspect bounded backend evidence","requestId":last_id,"commitIssued":operation.commit_issued,"commitReceiptObserved":operation.receipt.is_some()});
        let _ = send_frame(&mut write, spool, &control, true, None).await;
    }
    outcome
}

#[intent_test_macros::daemon_test]
#[ignore = "requires separately authorized coordinated native producer; preparation must never run this test"]
async fn linked_native_phantom_services_capture() {
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        std::env::var("NOTE_LINKED_CAPTURE_ENABLE").as_deref(),
        Ok("coordinated-live-capture")
    );
    let contract = std::env::var("NOTE_LINKED_CONTRACT_SHA256").expect("reviewed shared contract");
    assert_eq!(
        contract,
        "943ac9cd2086865fb4df7446eb079c3bf833d3374f6576f176072dda0e68eeb4"
    );
    let contract_path = std::env::var("NOTE_LINKED_CONTRACT_PATH").unwrap();
    assert_eq!(
        crate::attachment_upload::sha256_hex(&std::fs::read(contract_path).unwrap()),
        contract
    );
    let directory =
        std::env::var("NOTE_LINKED_CAPTURE_DIR").expect("private empty output directory");
    let directory = Path::new(&directory);
    assert!(directory.is_dir() && std::fs::read_dir(directory).unwrap().next().is_none());
    std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket_path = directory.join("driver.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let mut spool = Spool::new(&directory.join("backend-transcript.jsonl")).unwrap();
    let (temporary, services, workspace, note) = crate::tests::setup("ab").await;
    let mut operation = Operation::default();
    let accepted = tokio::time::timeout(Duration::from_secs(30), listener.accept()).await;
    let result = match accepted {
        Ok(Ok((socket, _))) => {
            conversation(
                &services,
                &workspace,
                &note,
                socket,
                &mut spool,
                &mut operation,
                &contract,
            )
            .await
        }
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err("peer connection timeout; no lexical source issued".into()),
    };
    let cleanup = if operation.commit_issued {
        "commit was issued and call settled; no rollback or stage cancellation".into()
    } else if let Some(begin) = &operation.begin {
        let mut value = begin.clone();
        value.as_object_mut().unwrap().remove("header");
        value.as_object_mut().unwrap().remove("expiresAt");
        let request = decode::<NoteStageCancel>(value).unwrap();
        let future = std::pin::pin!(services.note_operation_cancel(request));
        format!(
            "{:?}",
            settled(future, &mut spool, "stage cancellation", None).await
        )
    } else {
        "no admitted stage".into()
    };
    let closed = {
        let close = std::pin::pin!(services.store.close());
        settled(close, &mut spool, "Store close", None).await
    };
    drop(services);
    drop(temporary);
    drop(listener);
    let removed = std::fs::remove_file(&socket_path);
    spool.emergency = true;
    let terminal = json!({"contractHash":contract,"ok":result.is_ok() && closed.is_ok() && removed.is_ok(),"error":result.as_ref().err(),"cleanup":cleanup,"backendBytesBeforeTerminal":spool.bytes,"storeClose":format!("{closed:?}"),"socketRemoval":format!("{removed:?}"),"claim":"internal Services test adapter; no WSS, production save or native adoption"});
    spool.summary(&terminal).unwrap();
    assert!(
        result.is_ok() && closed.is_ok() && removed.is_ok(),
        "{terminal}"
    );
}

#[tokio::test(start_paused = true)]
async fn linked_timeout_keeps_future_until_observed_settlement() {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    let dir = crate::test_support::test_tempdir("linked-debt-");
    let mut spool = Spool::new(&dir.path().join("transcript.jsonl")).unwrap();
    let completed = Arc::new(AtomicBool::new(false));
    let observed = completed.clone();
    let future = std::pin::pin!(async move {
        tokio::time::sleep(Duration::from_secs(31)).await;
        observed.store(true, Ordering::SeqCst);
    });
    assert!(settled(future, &mut spool, "synthetic held call", None)
        .await
        .is_err());
    assert!(
        completed.load(Ordering::SeqCst),
        "deadline cannot drop the owned future"
    );
    let log = std::fs::read_to_string(dir.path().join("backend-terminal.jsonl")).unwrap();
    assert!(
        log.contains("unknown; borrower retained")
            && log.contains("late completion observed; run failed")
    );
}

#[tokio::test(start_paused = true)]
async fn linked_deadline_rechecks_after_read_before_dispatch() {
    let operation = Operation::default();
    let until = tokio::time::Instant::now() + Duration::from_secs(1);
    operation.call_deadline(until).unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(operation.call_deadline(until).is_err());
    let operation = Operation {
        first: Some(json!({"expiresAt":"2000-01-01T00:00:00Z"})),
        ..Operation::default()
    };
    assert!(operation
        .call_deadline(tokio::time::Instant::now() + Duration::from_secs(120))
        .is_err());
}

#[tokio::test(start_paused = true)]
async fn linked_expired_dispatch_never_polls_ready_future() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let dir = crate::test_support::test_tempdir("linked-expired-");
    let mut spool = Spool::new(&dir.path().join("transcript.jsonl")).unwrap();
    let polls = AtomicUsize::new(0);
    let until = tokio::time::Instant::now() + Duration::from_secs(1);
    tokio::time::advance(Duration::from_secs(2)).await;
    let future = std::pin::pin!(std::future::poll_fn(|_| {
        polls.fetch_add(1, Ordering::SeqCst);
        std::task::Poll::Ready(())
    }));
    assert!(
        settled(future, &mut spool, "expired ready future", Some(until))
            .await
            .is_err()
    );
    assert_eq!(polls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn linked_late_ready_completion_fails_without_repolling() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let dir = crate::test_support::test_tempdir("linked-late-");
    let mut spool = Spool::new(&dir.path().join("transcript.jsonl")).unwrap();
    let polls = AtomicUsize::new(0);
    let until = tokio::time::Instant::now() + Duration::from_millis(100);
    let future = std::pin::pin!(std::future::poll_fn(|_| {
        polls.fetch_add(1, Ordering::SeqCst);
        // Deliberately cross the deadline inside one poll returning Ready.
        std::thread::sleep(Duration::from_millis(200));
        std::task::Poll::Ready(())
    }));
    assert!(settled(future, &mut spool, "late ready", Some(until))
        .await
        .is_err());
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    let terminal = std::fs::read_to_string(dir.path().join("backend-terminal.jsonl")).unwrap();
    assert!(terminal.contains("completion observed; completed future is not polled again"));
}

//! Executed compatibility observations, not a new timestamp policy or a source
//! admission test. Original request strings and exact parsed instants stay distinct.
use super::ArtifactBegin;
use serde_json::{json, Value};

fn specimens() -> Vec<(String, String)> {
    let mut values = vec![
        ("utc-z", "2026-10-04T12:34:56Z"),
        ("utc-plus-zero", "2026-10-04T12:34:56+00:00"),
        ("utc-minus-zero", "2026-10-04T12:34:56-00:00"),
        ("lower-t", "2026-10-04t12:34:56Z"),
        ("lower-z", "2026-10-04T12:34:56z"),
        ("lower-both", "2026-10-04t12:34:56z"),
        ("space-separator", "2026-10-04 12:34:56Z"),
        ("slash-separator", "2026-10-04/12:34:56Z"),
        ("underscore-separator", "2026-10-04_12:34:56Z"),
        ("newline-separator", "2026-10-04\n12:34:56Z"),
        ("multibyte-separator", "2026-10-04×12:34:56Z"),
        ("offset-plus-one", "2026-10-04T13:34:56+01:00"),
        ("offset-minus-one", "2026-10-04T11:34:56-01:00"),
        ("invalid-offset", "2026-10-04T12:34:56+24:00"),
        ("offset-with-seconds", "2026-10-04T12:34:56+00:00:00"),
        ("valid-leap-date", "2024-02-29T12:34:56Z"),
        ("invalid-leap-date", "2026-02-29T12:34:56Z"),
        ("invalid-month-day", "2026-04-31T12:34:56Z"),
        ("invalid-month", "2026-13-01T12:34:56Z"),
        ("invalid-day-zero", "2026-10-00T12:34:56Z"),
        ("invalid-hour", "2026-10-04T24:00:00Z"),
        ("invalid-minute", "2026-10-04T12:60:00Z"),
        ("invalid-second-61", "2026-10-04T12:34:61Z"),
        ("month-end-leap-second", "2016-12-31T23:59:60Z"),
        ("nonmonth-end-leap-second", "2016-12-30T23:59:60Z"),
        ("midday-leap-second", "2016-12-31T12:00:60Z"),
        ("fractional-leap-second", "2016-12-31T23:59:60.123Z"),
        ("fraction-one", "2026-10-04T12:34:56.1Z"),
        ("fraction-three", "2026-10-04T12:34:56.123Z"),
        ("fraction-nine", "2026-10-04T12:34:56.123456789Z"),
        ("fraction-ten-nonzero", "2026-10-04T12:34:56.1234567891Z"),
        ("fraction-ten-nine", "2026-10-04T12:34:56.1234567899Z"),
        ("fraction-ten-overflow", "2026-10-04T12:34:56.9999999999Z"),
        ("fraction-none", "2026-10-04T12:34:56.Z"),
        ("fraction-comma", "2026-10-04T12:34:56,123Z"),
        ("trailing-space", "2026-10-04T12:34:56Z "),
        ("leading-space", " 2026-10-04T12:34:56Z"),
    ]
    .into_iter()
    .map(|(id, text)| (id.to_owned(), text.to_owned()))
    .collect::<Vec<_>>();
    for length in [63, 64, 65] {
        let text = format!("2026-10-04T12:34:56.{}Z", "7".repeat(length - 21));
        assert_eq!(text.len(), length);
        values.push((format!("byte-limit-{length}"), text));
    }
    values
}

#[test]
fn artifact_timestamp_capture_observes_pinned_parser_and_request_separately() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/native_artifact_canonicalization.json"
    ))
    .unwrap();
    let base: Value = serde_json::from_str(
        fixture["canonicalization"]["header"]["rawJson"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    let mut vectors = Vec::new();
    for (id, original) in specimens() {
        let parsed = crate::parse_iso(&original);
        let envelope = json!({"domain":"note.artifact.begin.v1","jobId":base["jobId"],"expiresAt":original,"header":base["header"]});
        let digest = crate::note_artifact::canonical::digest(&envelope.to_string()).unwrap();
        let raw = json!({"jobId":base["jobId"],"expiresAt":original,"header":base["header"],"headerDigest":digest});
        let request: ArtifactBegin = serde_json::from_value(raw.clone()).unwrap();
        let validation = request.validate(base["header"]["scope"]["workspaceId"].as_str().unwrap());
        assert_eq!(
            serde_json::to_value(&request).unwrap()["expiresAt"],
            original
        );
        if id == "utc-z" || id == "utc-plus-zero" {
            assert!(parsed.is_some() && validation.is_ok());
        }
        if id == "byte-limit-65" {
            assert!(validation.is_err());
        }
        vectors.push(json!({"id":id,"originalTimestamp":original,"utf8Bytes":original.len(),
            "parseAccepted":parsed.is_some(),"parsedEpochNanoseconds":parsed.map(|t|t.unix_timestamp_nanos().to_string()),
            "parsedOffsetSeconds":parsed.map(|t|t.offset().whole_seconds()),
            "parsedNormalizedRfc3339":parsed.map(|t|t.format(&time::format_description::well_known::Rfc3339).unwrap()),
            "artifactBeginValidateAccepted":validation.is_ok(),"requestError":validation.err().map(|e|format!("{e:?}")),
            "retainedRequestExpiresAt":request.expires_at,"headerDigest":digest,"beginRequest":raw}));
    }
    assert_eq!(vectors.len(), 40);
    let output = json!({"schema":"artifact-timestamp-observations/1","timeVersion":"0.3.55",
        "boundary":"Actual parse_iso and ArtifactBegin.validate execution, not Store begin/source lifetime/storage admission. Retained string is request serialization, not proof of persisted or accepted server grant. No parser/schema policy change. Nanoseconds are exact decimal strings.",
        "vectors":vectors});
    if let Ok(path) = std::env::var("ARTIFACT_TIMESTAMP_CAPTURE") {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap();
        serde_json::to_writer_pretty(file, &output).unwrap();
    }
}

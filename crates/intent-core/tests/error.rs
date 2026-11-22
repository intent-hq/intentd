//! Unit-style integration coverage for `intent-core::error`.

use intent_core::error::{Error, Result};
use serde_json::json;

#[test]
fn invalid_params_code_and_display() {
    let e = Error::InvalidParams("missing `id`".to_string());
    assert_eq!(e.code(), -32602);
    assert_eq!(e.to_string(), "invalid params: missing `id`");
}

#[test]
fn not_found_code_and_display() {
    let e = Error::NotFound("workspace ws-1".to_string());
    assert_eq!(e.code(), -32602);
    assert_eq!(e.to_string(), "not found: workspace ws-1");
}

#[test]
fn internal_code_and_display() {
    let e = Error::Internal("boom".to_string());
    assert_eq!(e.code(), -32603);
    assert_eq!(e.to_string(), "internal error: boom");
}

#[test]
fn store_busy_code_and_display() {
    let e = Error::StoreBusy("tenant writer lock".to_string());
    assert_eq!(e.code(), -32603);
    assert_eq!(e.to_string(), "store busy: tenant writer lock");
}

#[test]
fn not_owner_code_and_display() {
    let e = Error::NotOwner("tenant t is owned by instance b (epoch 2)".into());
    assert_eq!(e.code(), -32603);
    assert_eq!(
        e.to_string(),
        "not the tenant owner: tenant t is owned by instance b (epoch 2)"
    );
}

#[test]
fn schema_mismatch_code_and_display() {
    use intent_core::error::SchemaMismatch;
    let mismatch = |applied: Vec<i64>, changed: Vec<i64>, part_way: Vec<i64>| {
        Error::SchemaMismatch(Box::new(SchemaMismatch {
            applied,
            expected: vec![1, 2],
            changed,
            part_way,
            legacy_sqlx: false,
            folded_history: false,
        }))
    };
    let missing = mismatch(vec![1], vec![], vec![]);
    assert_eq!(missing.code(), -32603);
    assert_eq!(
        missing.to_string(),
        "postgres schema does not match this build: applied migrations [1], this build \
         expects [1, 2]; run the migrate job with this build, then start the daemon"
    );
    let known_part_way = mismatch(vec![1], vec![], vec![2]).to_string();
    assert!(
        known_part_way.contains("; part-way: [2]"),
        "{known_part_way}"
    );
    assert!(
        known_part_way.ends_with("run the migrate job with this build, then start the daemon"),
        "{known_part_way}"
    );
    let changed = mismatch(vec![1, 2], vec![2], vec![]).to_string();
    assert!(
        changed.contains("changed since they were applied: [2]"),
        "{changed}"
    );
    assert!(
        changed.contains("restore it, or recreate the schema"),
        "{changed}"
    );
    // Not "run the migrate job" alone: migrate refuses a changed file.
    assert!(
        !changed.contains("run the migrate job with this build"),
        "{changed}"
    );
    assert!(changed.contains("recreate the schema and run"), "{changed}");
    let newer = mismatch(vec![1, 2, 3], vec![2], vec![]).to_string();
    assert!(
        newer.contains("newer than this build (unknown [3])"),
        "{newer}"
    );
    let legacy = Error::SchemaMismatch(Box::new(SchemaMismatch {
        applied: vec![],
        expected: vec![1, 2],
        changed: vec![],
        part_way: vec![],
        legacy_sqlx: true,
        folded_history: false,
    }))
    .to_string();
    assert!(legacy.contains("earlier sqlx migrator"), "{legacy}");
    assert!(legacy.contains("recreate the schema"), "{legacy}");
    // A schema left at the numbered migrations before they were folded into
    // one file: its versions 2-15 are unknown, but the repair is recreating
    // it, not a newer build.
    let folded = Error::SchemaMismatch(Box::new(SchemaMismatch {
        applied: (1..=15).collect(),
        expected: vec![1],
        changed: vec![1],
        part_way: vec![],
        legacy_sqlx: false,
        folded_history: true,
    }))
    .to_string();
    assert!(folded.contains("numbered Postgres migrations"), "{folded}");
    assert!(folded.contains("recreate the schema"), "{folded}");
    assert!(!folded.contains("newer than this build"), "{folded}");
    // An unknown version a newer build left part-way is a newer schema too.
    let newer_part_way = mismatch(vec![1, 2], vec![], vec![3]).to_string();
    assert!(
        newer_part_way.contains("newer than this build (unknown [3])"),
        "{newer_part_way}"
    );
}

#[test]
fn conflict_code_and_display_and_payload() {
    let current = json!({ "rev": 7 });
    let e = Error::Conflict {
        current: current.clone(),
    };
    assert_eq!(e.code(), -32005);
    assert_eq!(e.to_string(), "conflict: version mismatch");
    if let Error::Conflict { current: c } = e {
        assert_eq!(c, current);
    } else {
        panic!("variant should be Conflict");
    }
}

#[test]
fn base_ref_unresolvable_code_and_display_and_payload() {
    let e = Error::BaseRefUnresolvable {
        base_ref: "no-such-ref".to_string(),
    };
    assert_eq!(e.code(), -32602);
    assert_eq!(
        e.to_string(),
        "invalid params: cannot resolve base ref 'no-such-ref'"
    );
    if let Error::BaseRefUnresolvable { base_ref } = e {
        assert_eq!(base_ref, "no-such-ref");
    } else {
        panic!("variant should be BaseRefUnresolvable");
    }
}

#[test]
fn debug_includes_variant_name() {
    let e = Error::InvalidParams("x".to_string());
    let dbg = format!("{e:?}");
    assert!(
        dbg.contains("InvalidParams"),
        "debug missing variant: {dbg}"
    );
}

#[test]
fn result_alias_is_usable() {
    #[expect(clippy::unnecessary_wraps)] // exercising the Result alias is the point of the test
    fn ok() -> Result<u32> {
        Ok(42)
    }
    fn err() -> Result<u32> {
        Err(Error::NotFound("nope".to_string()))
    }
    assert_eq!(ok().unwrap(), 42);
    assert!(matches!(err(), Err(Error::NotFound(_))));
}

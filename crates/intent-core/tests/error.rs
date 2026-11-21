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
fn schema_mismatch_code_and_display() {
    use intent_core::error::SchemaMismatch;
    let missing = Error::SchemaMismatch(Box::new(SchemaMismatch {
        applied: vec![1],
        expected: vec![1, 2],
        changed: vec![],
    }));
    assert_eq!(missing.code(), -32603);
    assert_eq!(
        missing.to_string(),
        "postgres schema does not match this build: applied migrations [1], this build \
         expects [1, 2]; run the migrate job with this build, then start the daemon"
    );
    let newer = Error::SchemaMismatch(Box::new(SchemaMismatch {
        applied: vec![1, 2, 3],
        expected: vec![1, 2],
        changed: vec![2],
    }));
    let text = newer.to_string();
    assert!(
        text.contains("changed since they were applied: [2]"),
        "{text}"
    );
    assert!(
        text.contains("the schema is newer than this build"),
        "{text}"
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

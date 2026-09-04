//! Harness v2.1 golden fixtures. This version reuses v2 instruction and
//! system-text bytes and extends the frozen v1.1 specialist bundle with the
//! bundled Vulnerability Scanner definition.

use std::fmt::Write as _;

use sha2::{Digest, Sha256};

fn sha256_hex(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    h.finalize().iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

#[test]
fn v2_1_registry_selects_the_extended_bundle() {
    let v2 = crate::harness::resolve_entry("2.0");
    let v2_1 = crate::harness::resolve_entry("2.1");
    assert_eq!(
        v2.doctrine.specialists,
        crate::specialists::EMBEDDED_BUNDLED_V1_1
    );
    assert_eq!(
        v2_1.doctrine.specialists,
        crate::specialists::EMBEDDED_BUNDLED_V2_1
    );
    assert!(std::ptr::eq(
        v2.doctrine.instructions,
        v2_1.doctrine.instructions
    ));
}

#[test]
fn v2_1_bundle_only_adds_vulnerability_scanner() {
    let old = crate::specialists::EMBEDDED_BUNDLED_V1_1;
    let new = crate::specialists::EMBEDDED_BUNDLED_V2_1;
    assert_eq!(&new[..old.len()], old);
    assert_eq!(new.len(), old.len() + 1);
    assert_eq!(new[old.len()].0, "vulnerability-scanner");
}

#[test]
fn golden_vulnerability_scanner_definition_hash() {
    let content = crate::specialists::EMBEDDED_BUNDLED_V2_1
        .iter()
        .find_map(|(id, content)| (*id == "vulnerability-scanner").then_some(*content))
        .expect("vulnerability scanner is bundled");
    assert_eq!(
        sha256_hex(content),
        "d682949a05d728d1a484667d8285c1cb21d63281e25f0bcb0dc8d7aa6dc85c1c"
    );
}

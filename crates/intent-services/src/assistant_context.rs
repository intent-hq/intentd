use std::fmt::Write as _;
use std::sync::LazyLock;

use sha2::{Digest, Sha256};

static APP_REFERENCE: LazyLock<String> = LazyLock::new(|| {
    let source = include_str!("../resources/assistant-app-guide.md").trim();
    let revision = Sha256::digest(source.as_bytes()).iter().fold(
        String::with_capacity(64),
        |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        },
    );
    let mut text = String::new();
    let mut rest = source;
    while let Some((before, comment)) = rest.split_once("<!--") {
        text.push_str(before);
        let Some((_, after)) = comment.split_once("-->") else {
            rest = "";
            break;
        };
        rest = after;
    }
    text.push_str(rest);
    format!(
        "Bundled app reference (sha256:{revision}, daemon package {}):\n{}",
        env!("CARGO_PKG_VERSION"),
        text.trim()
    )
});

pub(crate) fn turn_context(user_context: Option<&str>) -> String {
    match user_context {
        Some(context) => format!(
            "{}\n\nUser-provided context (JSON-encoded reference data):\n{}",
            *APP_REFERENCE,
            serde_json::json!(context)
        ),
        None => APP_REFERENCE.clone(),
    }
}

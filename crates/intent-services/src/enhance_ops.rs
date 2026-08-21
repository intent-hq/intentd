//! `agent.enhancePrompt` — one-shot prompt-enhance / AI-layout generation via
//! the auggie CLI (PROTOCOL §5.31).
//!
//! Ports the FE's last local-CLI bypass (`agent:enhance-prompt` /
//! `agent:generate-layout` in `agent-missing.ipc.ts`, which spawn
//! `auggie --print` on the client): the enhancement template
//! (`getInputWithEnhancePrompt`), the `<augment-enhanced-prompt>` tag extractor
//! (`extractEnhancedPrompt`), and the output cleaner (`cleanAgentMessage` —
//! ANSI strip, 🤖-delimited response extraction, tool-artifact line filtering).
//! Follows the one-shot CLI discipline of `workspace.generateSetupScript`
//! (§5.25) and `models.list` (§5.30): auggie discovery, piped stdin, hard
//! timeout, `kill_on_drop` + unix process-group reap. No session is created;
//! no events are emitted.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use intent_core::{Error, Result, WorkspaceId};
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;

use crate::file_ops;
use crate::Services;

/// Default request timeout — the FE's 30-second enhancement timeout (§5.31).
pub(crate) const DEFAULT_TIMEOUT_MS: u64 = 30_000;
/// Upper bound on a caller-supplied `timeoutMs` (§5.31).
pub(crate) const MAX_TIMEOUT_MS: u64 = 120_000;

/// System prompt for `mode: "enhance"` (FE `agent:enhance-prompt`).
const ENHANCE_SYSTEM_PROMPT: &str =
    "You are a helpful assistant. Respond directly and concisely. Do not use any tools.";

/// System prompt for `mode: "layout"` (FE `agent:generate-layout`).
const LAYOUT_SYSTEM_PROMPT: &str = "You are a layout configuration assistant. \
     Follow the instructions exactly and respond only with the requested JSON format.";

/// The template placeholder the model must not echo back (FE
/// `extractEnhancedPrompt` guard).
const PLACEHOLDER: &str = "[Your enhanced version of the instruction goes here]";

/// Wrap the raw user input in the enhancement template (port of the FE
/// `getInputWithEnhancePrompt`).
pub(crate) fn build_enhancement_prompt(input: &str) -> String {
    format!(
        "⚠️ NO TOOLS ALLOWED ⚠️\n\n\
         Here is an instruction that I'd like to give you, but it needs to be improved. \
         Rewrite and enhance this instruction to make it clearer, more specific, \
         less ambiguous, and correct any mistakes. \
         Do not use any tools: reply immediately with your answer, even if you're not sure. \
         Consider the context of our conversation history when enhancing the prompt. \
         If there is code in triple backticks (```) consider whether it is a code sample and should remain unchanged.\
         Reply with the following format:\n\n\
         ### BEGIN RESPONSE ###\n\
         Here is an enhanced version of the original instruction that is more specific and clear:\n\
         <augment-enhanced-prompt>enhanced prompt goes here</augment-enhanced-prompt>\n\n\
         ### END RESPONSE ###\n\n\
         Here is my original instruction:\n\n{input}"
    )
}

/// Remove ANSI SGR escape sequences (`ESC [ … m`), the FE `removeAnsiCodes`.
fn strip_ansi(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == 0x1b && i + 1 < bytes.len() && bytes[i + 1] == b'[' {
            let mut j = i + 2;
            while j < bytes.len() && (bytes[j].is_ascii_digit() || bytes[j] == b';') {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b'm' {
                i = j + 1;
                continue;
            }
        }
        let ch = text[i..].chars().next().expect("in-bounds char");
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// ASCII-case-insensitive substring search starting at `from` (byte offset).
fn find_ci(haystack: &str, needle: &str, from: usize) -> Option<usize> {
    let h = haystack.as_bytes();
    let n = needle.as_bytes();
    if n.is_empty() || from + n.len() > h.len() {
        return None;
    }
    (from..=h.len() - n.len()).find(|&i| h[i..i + n.len()].eq_ignore_ascii_case(n))
}

/// Extract the enhanced prompt from the model reply (port of the FE
/// `extractEnhancedPrompt`): the trimmed payload between
/// `<augment-enhanced-prompt>` tags (case-insensitive), rejecting the empty
/// string and the un-filled template placeholder.
pub(crate) fn extract_enhanced_prompt(response: &str) -> Option<String> {
    const OPEN: &str = "<augment-enhanced-prompt>";
    const CLOSE: &str = "</augment-enhanced-prompt>";
    let start = find_ci(response, OPEN, 0)? + OPEN.len();
    let end = find_ci(response, CLOSE, start)?;
    let extracted = response[start..end].trim();
    if extracted.is_empty() || extracted == PLACEHOLDER {
        return None;
    }
    Some(extracted.to_string())
}

/// True for the tool-output artifact lines the FE `cleanAgentMessage` strips.
fn is_artifact_line(line: &str) -> bool {
    const PREFIXES: &[&str] = &[
        "Here's the result of running",
        "Here's the file and directories",
        "Here's the files and directories",
        "Here's the content",
        "Created note:",
        "Created file:",
        "Created workspace:",
        "Updated note:",
        "Updated file:",
        "Updated workspace:",
        "Successfully edited",
        "Successfully created",
        "Successfully deleted",
        "Successfully modified",
        "Result for ",
        "Total lines in file:",
    ];
    PREFIXES.iter().any(|p| line.starts_with(p))
}

/// Clean a raw `auggie --print` transcript into the model's response (port of
/// the FE `cleanAgentMessage`): ANSI-strip, then take the part after the last
/// 🤖 marker (auggie prints one before the actual response); when no marker is
/// present, drop tool call/result sections line-by-line. Finally filter the
/// common tool-output artifact lines.
pub(crate) fn clean_agent_message(content: &str) -> String {
    if content.is_empty() {
        return String::new();
    }
    let cleaned = strip_ansi(content);
    let cleaned = if let Some(pos) = cleaned.rfind('🤖') {
        cleaned[pos + '🤖'.len_utf8()..].trim().to_string()
    } else {
        // No robot marker: strip tool call/result sections manually (the
        // FE fallback branch).
        let mut kept: Vec<&str> = Vec::new();
        let mut in_tool_section = false;
        for line in cleaned.split('\n') {
            let trimmed = line.trim();
            if trimmed.starts_with("🔧 Tool call:")
                || trimmed.starts_with("Tool call:")
                || trimmed.starts_with("📋 Tool result:")
                || trimmed.starts_with("Tool result:")
            {
                in_tool_section = true;
                continue;
            }
            if in_tool_section
                && (line.starts_with("   ")
                    || line.starts_with('\t')
                    || (trimmed.contains(':') && !trimmed.starts_with('#')))
            {
                continue;
            }
            if in_tool_section && trimmed.is_empty() {
                in_tool_section = false;
                continue;
            }
            if !in_tool_section {
                kept.push(line);
            }
        }
        kept.join("\n")
    };
    let filtered: Vec<&str> = cleaned
        .split('\n')
        .filter(|line| !is_artifact_line(line))
        .collect();
    filtered.join("\n").trim().to_string()
}

/// Spawn `auggie --print` with MCP disabled, pipe `prompt` over stdin, and
/// collect stdout under a hard timeout. `kill_on_drop` (plus a unix
/// process-group SIGKILL on timeout) keeps a hung CLI from outliving the
/// request. Shared with `agent.completeOnce` (§5.32) so both one-shot RPCs
/// use the same reap-on-failure discipline.
pub(crate) async fn run_auggie_print(
    bin: &Path,
    model: Option<&str>,
    cwd: Option<&Path>,
    prompt: &str,
    timeout_ms: u64,
) -> Result<String> {
    let mut cmd = tokio::process::Command::new(bin);
    cmd.arg("--print")
        .arg("--mcp-config")
        .arg(r#"{"mcpServers":{}}"#);
    if let Some(m) = model {
        cmd.arg("--model").arg(m);
    }
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }
    // Prepend the binary's own directory so its co-located `node` resolves
    // (§8.2), mirroring the FE exec path.
    cmd.env("PATH", intent_context::discovery::exec_path(bin));
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    cmd.process_group(0);

    let mut child = cmd
        .spawn()
        .map_err(|e| Error::Internal(format!("failed to spawn auggie CLI: {e}")))?;
    let pid = child.id();
    if let Some(mut stdin) = child.stdin.take() {
        // A failed write is non-fatal — the child may have already exited.
        let _ = stdin.write_all(prompt.as_bytes()).await;
        // Dropping stdin closes it so the read-to-EOF `--print` exits cleanly.
    }

    match tokio::time::timeout(Duration::from_millis(timeout_ms), child.wait_with_output()).await {
        Ok(Ok(output)) => {
            if !output.status.success() {
                let code = output.status.code().unwrap_or(-1);
                return Err(Error::Internal(format!(
                    "auggie process exited with code {code}"
                )));
            }
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        }
        Ok(Err(e)) => Err(Error::Internal(format!("auggie wait failed: {e}"))),
        Err(_) => {
            // Reap the whole process group (pgid == pid via `process_group`);
            // the dropped `wait_with_output` future's `kill_on_drop` covers the
            // direct child on non-unix.
            #[cfg(unix)]
            if let Some(pid) = pid {
                use nix::sys::signal::{killpg, Signal};
                use nix::unistd::Pid;
                let _ = killpg(Pid::from_raw(pid.cast_signed()), Signal::SIGKILL);
            }
            #[cfg(not(unix))]
            let _ = pid;
            Err(Error::Internal(format!(
                "Prompt enhancement timed out after {timeout_ms}ms"
            )))
        }
    }
}

impl Services {
    /// `agent.enhancePrompt` (PROTOCOL §5.31): compose the mode-specific
    /// prompt, run the one-shot auggie CLI, clean the transcript, and parse
    /// the mode-specific result. `mode` is pre-validated by the router
    /// (`"enhance"` or `"layout"`). Gated on auggie being the effective
    /// provider per spec Decision 5 — the settings-derived default (provider
    /// of `model.default`, else `providers.active`) must be auggie.
    /// Unset/undecidable settings resolve the gate CLOSED (unavailable):
    /// falling through to the first registered provider would always be
    /// auggie and functionally reinstate the removed hardcoded default
    /// (matches FE #759, where unset resolves disabled).
    pub(crate) async fn agent_enhance_prompt_op(
        &self,
        prompt: String,
        mode: String,
        model: Option<String>,
        workspace_id: Option<WorkspaceId>,
        timeout_ms: Option<u64>,
    ) -> Result<Value> {
        // Provider neutrality gate: enhance-prompt is an auggie-specific capability.
        // When the effective provider is not auggie — including unset/undecidable
        // settings, which resolve the gate closed rather than falling through to
        // the first registered provider — return a typed unavailable response so
        // the FE can hide the affordance gracefully without an error crash.
        let effective_provider =
            crate::agent_session::derived_default_provider(&self.effective_settings());
        if effective_provider.as_deref() != Some("auggie") {
            return Ok(json!({
                "available": false,
                "reason": "enhance-prompt requires auggie as the effective default provider"
            }));
        }

        // Optional cwd pin: unknown workspace surfaces as -32602 (NotFound);
        // a workspace without a filesystem root just runs without a cwd
        // (mirrors the FE dropping `cwd` when no workspace is bound).
        let cwd: Option<PathBuf> = match &workspace_id {
            Some(id) => {
                let ws = self.store.get_workspace(id).await?;
                let root = file_ops::workspace_root(&ws);
                (!root.is_empty()).then(|| PathBuf::from(root))
            }
            None => None,
        };
        let auggie = match &self.auggie_bin {
            Some(bin) => bin.clone(),
            None => intent_context::discovery::find_auggie()
                .ok_or_else(|| Error::Internal("auggie CLI not found".to_string()))?,
        };
        let system = if mode == "layout" {
            LAYOUT_SYSTEM_PROMPT
        } else {
            ENHANCE_SYSTEM_PROMPT
        };
        let message = if mode == "enhance" {
            build_enhancement_prompt(&prompt)
        } else {
            prompt.clone()
        };
        // `System: <system>\n\n<message>` mirrors the FE streamChat composition.
        let full_prompt = format!("System: {system}\n\n{message}");
        let timeout = timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS).min(MAX_TIMEOUT_MS);
        let stdout = run_auggie_print(
            &auggie,
            model.as_deref(),
            cwd.as_deref(),
            &full_prompt,
            timeout,
        )
        .await?;
        let cleaned = clean_agent_message(&stdout);
        let enhanced = if mode == "enhance" {
            extract_enhanced_prompt(&cleaned).ok_or_else(|| {
                Error::Internal("Failed to parse enhanced prompt from response".to_string())
            })?
        } else {
            cleaned
        };
        Ok(json!({ "enhanced": enhanced, "original": prompt, "mode": mode }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_store::Store;

    #[test]
    fn build_enhancement_prompt_wraps_input_with_template() {
        let p = build_enhancement_prompt("fix the login bug");
        assert!(p.starts_with("⚠️ NO TOOLS ALLOWED ⚠️"));
        assert!(p.contains(
            "<augment-enhanced-prompt>enhanced prompt goes here</augment-enhanced-prompt>"
        ));
        assert!(p.ends_with("Here is my original instruction:\n\nfix the login bug"));
    }

    #[test]
    fn extract_enhanced_prompt_finds_tagged_payload() {
        let r =
            "noise\n<augment-enhanced-prompt>\n  Do X carefully.\n</augment-enhanced-prompt>\ntail";
        assert_eq!(
            extract_enhanced_prompt(r).as_deref(),
            Some("Do X carefully.")
        );
    }

    #[test]
    fn extract_enhanced_prompt_is_case_insensitive() {
        let r = "<AUGMENT-ENHANCED-PROMPT>payload</AUGMENT-ENHANCED-PROMPT>";
        assert_eq!(extract_enhanced_prompt(r).as_deref(), Some("payload"));
    }

    #[test]
    fn extract_enhanced_prompt_rejects_placeholder_and_empty() {
        let placeholder =
            format!("<augment-enhanced-prompt>{PLACEHOLDER}</augment-enhanced-prompt>");
        assert_eq!(extract_enhanced_prompt(&placeholder), None);
        assert_eq!(
            extract_enhanced_prompt("<augment-enhanced-prompt>  </augment-enhanced-prompt>"),
            None
        );
        assert_eq!(extract_enhanced_prompt("no tags at all"), None);
    }

    #[test]
    fn clean_agent_message_takes_response_after_robot_marker() {
        let raw = "\u{1b}[32m🔧 Tool call: something\u{1b}[0m\nparams: x\n🤖\nThe actual answer.";
        assert_eq!(clean_agent_message(raw), "The actual answer.");
    }

    #[test]
    fn clean_agent_message_without_marker_strips_tool_sections_and_artifacts() {
        let raw = "Tool call: read_file\n   path: a.txt\n\nAnswer line one.\nTotal lines in file: 12\nAnswer line two.";
        assert_eq!(
            clean_agent_message(raw),
            "Answer line one.\nAnswer line two."
        );
    }

    #[test]
    fn clean_agent_message_strips_ansi_codes() {
        assert_eq!(clean_agent_message("\u{1b}[1;31mhello\u{1b}[0m"), "hello");
    }

    // ---- op-level tests over a temp store + fake auggie script ----

    /// RAII temp `SQLite` store: the db (and its `-wal`/`-shm` sidecars) live in
    /// a guarded temp dir removed on drop — including on panic — unless
    /// `INTENTD_TEST_KEEP_TMP` (non-empty) is set.
    struct TempDb {
        dir: tempfile::TempDir,
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let dir = crate::tests::test_tempdir("intentd-enhanceops-");
            let path = dir.path().join("store.db");
            Self { dir, path }
        }
    }

    /// Services with a fake CLI and `providers.active = "auggie"` so the
    /// provider gate is open: unset settings resolve the gate CLOSED
    /// (see `enhance_op_unavailable_when_settings_unset`), so op-level
    /// tests must opt in to an auggie-active registry to reach the CLI.
    async fn services_with_bin(bin: PathBuf) -> (TempDb, Services) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let registry = std::sync::Arc::new(
            crate::SettingsRegistry::load(tmp.dir.path().join("config.toml"))
                .expect("load registry"),
        );
        registry
            .apply(&[("providers.active".to_string(), serde_json::json!("auggie"))])
            .expect("set providers.active");
        let services = Services::new(store)
            .with_auggie_bin(bin)
            .with_settings_registry(registry);
        (tmp, services)
    }

    /// Fake auggie CLI inside an RAII temp dir; keep the returned guard alive
    /// for the duration of the test (dropping it removes the dir).
    #[cfg(unix)]
    fn fake_auggie(tag: &str, body: &str) -> (tempfile::TempDir, PathBuf) {
        use std::os::unix::fs::PermissionsExt;
        let dir = crate::tests::test_tempdir(&format!("intentd-enhance-{tag}-"));
        let bin = dir.path().join("auggie");
        std::fs::write(&bin, format!("#!/bin/sh\ncat > /dev/null\n{body}\n")).unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        (dir, bin)
    }

    #[tokio::test]
    async fn enhance_op_errors_when_cli_missing() {
        let (_tmp, services) =
            services_with_bin(PathBuf::from("/nonexistent/intentd-test/auggie")).await;
        let err = services
            .agent_enhance_prompt_op("improve me".into(), "enhance".into(), None, None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Internal(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn enhance_op_unavailable_when_settings_unset() {
        // Unset/undecidable provider settings resolve the gate CLOSED: falling
        // through to the first registered provider would always be auggie and
        // functionally reinstate the removed hardcoded default (coordinator
        // ruling; matches FE #759 where unset resolves disabled). No registry
        // wired → schema defaults → both `model.default` and
        // `providers.active` unset.
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let services =
            Services::new(store).with_auggie_bin(PathBuf::from("/nonexistent/intentd-test/auggie"));
        let v = services
            .agent_enhance_prompt_op("improve me".into(), "enhance".into(), None, None, None)
            .await
            .unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "available": false,
                "reason": "enhance-prompt requires auggie as the effective default provider"
            }),
            "unset provider settings must close the gate, not fall back to the first registered provider"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn enhance_op_extracts_tagged_prompt() {
        let (_bin_dir, bin) = fake_auggie(
            "ok",
            "printf '🤖\\n<augment-enhanced-prompt>Enhanced: do the thing</augment-enhanced-prompt>\\n'",
        );
        let (_tmp, services) = services_with_bin(bin).await;
        let v = services
            .agent_enhance_prompt_op("do the thing".into(), "enhance".into(), None, None, None)
            .await
            .unwrap();
        assert_eq!(v["enhanced"], "Enhanced: do the thing");
        assert_eq!(v["original"], "do the thing");
        assert_eq!(v["mode"], "enhance");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn layout_op_returns_full_cleaned_reply() {
        let (_bin_dir, bin) = fake_auggie("layout", "printf '🤖\\n{\"layout\":\"two-column\"}\\n'");
        let (_tmp, services) = services_with_bin(bin).await;
        let v = services
            .agent_enhance_prompt_op("make a layout".into(), "layout".into(), None, None, None)
            .await
            .unwrap();
        assert_eq!(v["enhanced"], "{\"layout\":\"two-column\"}");
        assert_eq!(v["mode"], "layout");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn enhance_op_errors_when_tags_missing() {
        let (_bin_dir, bin) = fake_auggie("notags", "printf '🤖\\nno tags here\\n'");
        let (_tmp, services) = services_with_bin(bin).await;
        let err = services
            .agent_enhance_prompt_op("improve me".into(), "enhance".into(), None, None, None)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("Failed to parse enhanced prompt"),
            "got {err:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn enhance_op_times_out() {
        let (_bin_dir, bin) = fake_auggie("slow", "sleep 30");
        let (_tmp, services) = services_with_bin(bin).await;
        let err = services
            .agent_enhance_prompt_op("improve me".into(), "enhance".into(), None, None, Some(200))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("timed out after 200ms"),
            "got {err:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn enhance_op_errors_on_nonzero_exit() {
        let (_bin_dir, bin) = fake_auggie("fail", "exit 3");
        let (_tmp, services) = services_with_bin(bin).await;
        let err = services
            .agent_enhance_prompt_op("improve me".into(), "enhance".into(), None, None, None)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("exited with code 3"),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn enhance_op_rejects_unknown_workspace() {
        let (_tmp, services) =
            services_with_bin(PathBuf::from("/nonexistent/intentd-test/auggie")).await;
        let err = services
            .agent_enhance_prompt_op(
                "improve me".into(),
                "enhance".into(),
                None,
                Some(WorkspaceId::from("ws-missing")),
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), -32602);
    }
}

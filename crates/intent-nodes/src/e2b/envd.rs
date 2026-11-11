//! envd `process.Process/Start` over the Connect protocol with JSON frames.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use base64::Engine as _;
use reqwest::{Response, StatusCode};
use serde::{Deserialize, Serialize};

use super::client::{scrub, validate_sandbox_id, E2bClient, Op};
use super::{E2bError, Sandbox, SecretString};
use crate::cloud::{ProcessExit, ProcessOutput, ProcessSpec};

/// Port envd listens on inside every sandbox.
pub const ENVD_PORT: u16 = 49983;

const FLAG_COMPRESSED: u8 = 0x01;
const FLAG_END_STREAM: u8 = 0x02;
const FRAME_HEADER: usize = 5;
const MAX_FRAME: usize = 16 * 1024 * 1024;
/// Per-stream cap for [`ProcessStream::collect`].
const MAX_COLLECTED: usize = 16 * 1024 * 1024;
/// Extra time on top of the process timeout before the HTTP call gives up.
const STREAM_GRACE: Duration = Duration::from_secs(10);

/// Process output bytes. `Debug` shows only the length: output can carry
/// tokens (for example `executor.mint`).
#[derive(Clone, PartialEq, Eq)]
pub struct OutputChunk(Vec<u8>);

impl OutputChunk {
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl fmt::Debug for OutputChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OutputChunk({} bytes)", self.0.len())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessEvent {
    Started { pid: u32 },
    Stdout(OutputChunk),
    Stderr(OutputChunk),
    Exited(ProcessExit),
}

/// Server stream of one started process.
pub struct ProcessStream {
    response: Response,
    buf: Vec<u8>,
    finished: bool,
    /// The API key and access token, redacted from envd error messages.
    secrets: Vec<SecretString>,
}

impl fmt::Debug for ProcessStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessStream")
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

#[derive(Serialize)]
struct StartRequest<'a> {
    process: ProcessConfig<'a>,
    stdin: bool,
}

#[derive(Serialize)]
struct ProcessConfig<'a> {
    cmd: &'a str,
    args: &'a [String],
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    envs: BTreeMap<&'a str, &'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<&'a str>,
}

#[derive(Deserialize)]
struct StartResponse {
    #[serde(default)]
    event: Option<EventJson>,
}

#[derive(Deserialize)]
struct EventJson {
    #[serde(default)]
    start: Option<StartJson>,
    #[serde(default)]
    data: Option<DataJson>,
    #[serde(default)]
    end: Option<EndJson>,
}

#[derive(Deserialize)]
struct StartJson {
    #[serde(default)]
    pid: u32,
}

#[derive(Deserialize)]
struct DataJson {
    #[serde(default)]
    stdout: Option<String>,
    #[serde(default)]
    stderr: Option<String>,
    #[serde(default)]
    pty: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EndJson {
    #[serde(default)]
    exit_code: i32,
    #[serde(default)]
    exited: bool,
    #[serde(default)]
    status: String,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
struct EndStream {
    #[serde(default)]
    error: Option<ConnectError>,
}

#[derive(Deserialize)]
struct ConnectError {
    #[serde(default)]
    code: String,
    #[serde(default)]
    message: String,
}

impl E2bClient {
    /// Start a process in the sandbox and return its output stream.
    ///
    /// # Errors
    ///
    /// [`E2bError::Unauthorized`] for a bad access token,
    /// [`E2bError::NotFound`] when the sandbox is gone, or an API error.
    pub async fn start_process(
        &self,
        sandbox: &Sandbox,
        spec: &ProcessSpec,
    ) -> Result<ProcessStream, E2bError> {
        validate_sandbox_id(&sandbox.sandbox_id)?;
        if spec.cmd.is_empty() {
            return Err(E2bError::InvalidArgument("cmd is empty".into()));
        }
        let payload = serde_json::to_vec(&StartRequest {
            process: ProcessConfig {
                cmd: &spec.cmd,
                args: &spec.args,
                envs: spec
                    .envs
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.expose()))
                    .collect(),
                cwd: spec.cwd.as_deref(),
            },
            stdin: false,
        })
        .map_err(|e| E2bError::InvalidArgument(e.to_string()))?;
        let body = frame(0, &payload)?;
        let url = format!(
            "{}/process.Process/Start",
            self.config.envd_url.trim_end_matches('/')
        );
        let basic = spec.user.as_ref().map(|user| {
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!("{user}:"))
            )
        });
        let timeout_ms = spec
            .timeout
            .map(|t| u64::try_from(t.as_millis()).unwrap_or(u64::MAX));
        let token: Vec<&str> = sandbox
            .envd_access_token
            .iter()
            .map(SecretString::expose)
            .collect();
        let response = self
            .execute(Op::Process, &token, || {
                let mut req = self
                    .http
                    .post(&url)
                    .header("Content-Type", "application/connect+json")
                    .header("Connect-Protocol-Version", "1")
                    .header("E2b-Sandbox-Id", &sandbox.sandbox_id)
                    .header("E2b-Sandbox-Port", ENVD_PORT.to_string())
                    .body(body.clone());
                if let Some(token) = &sandbox.envd_access_token {
                    req = req.header("X-Access-Token", token.expose());
                }
                if let Some(basic) = &basic {
                    req = req.header("Authorization", basic);
                }
                if let (Some(ms), Some(timeout)) = (timeout_ms, spec.timeout) {
                    req = req
                        .header("Connect-Timeout-Ms", ms.to_string())
                        .timeout(timeout + STREAM_GRACE);
                }
                req
            })
            .await?;
        if response.status() != StatusCode::OK {
            return Err(E2bError::Protocol(format!(
                "envd answered HTTP {} to a stream request",
                response.status().as_u16()
            )));
        }
        let mut secrets = vec![self.key.clone()];
        secrets.extend(sandbox.envd_access_token.clone());
        Ok(ProcessStream {
            response,
            buf: Vec::new(),
            finished: false,
            secrets,
        })
    }

    /// Start a process and wait for it to finish.
    ///
    /// # Errors
    ///
    /// As [`Self::start_process`] and [`ProcessStream::collect`].
    pub async fn run_process(
        &self,
        sandbox: &Sandbox,
        spec: &ProcessSpec,
    ) -> Result<ProcessOutput, E2bError> {
        self.start_process(sandbox, spec).await?.collect().await
    }
}

impl ProcessStream {
    /// The next event, or `None` once the stream ended cleanly. Keepalives
    /// are skipped.
    ///
    /// # Errors
    ///
    /// [`E2bError::Process`] when envd ends the stream with an error,
    /// [`E2bError::Protocol`] for a malformed or cut-short stream.
    pub async fn next_event(&mut self) -> Result<Option<ProcessEvent>, E2bError> {
        while !self.finished {
            if let Some((flags, payload)) = self.take_frame()? {
                if flags & FLAG_COMPRESSED != 0 {
                    self.finished = true;
                    return Err(E2bError::Protocol(
                        "compressed frames are not supported".into(),
                    ));
                }
                if flags & FLAG_END_STREAM != 0 {
                    self.finished = true;
                    let end: EndStream = decode(&payload, "end-of-stream")?;
                    return match end.error {
                        Some(e) => Err(E2bError::Process {
                            code: self.scrub(&e.code),
                            message: self.scrub(&e.message),
                        }),
                        None => Ok(None),
                    };
                }
                let message: StartResponse = decode(&payload, "process event")?;
                if let Some(event) = message.event.map(convert).transpose()?.flatten() {
                    return Ok(Some(event));
                }
                continue;
            }
            match self.response.chunk().await {
                Ok(Some(bytes)) => self.buf.extend_from_slice(&bytes),
                Ok(None) => {
                    self.finished = true;
                    return Err(E2bError::Protocol(
                        "the process stream closed before it ended".into(),
                    ));
                }
                Err(e) => {
                    self.finished = true;
                    let message = self.scrub(&e.without_url().to_string());
                    return Err(E2bError::Transport(message));
                }
            }
        }
        Ok(None)
    }

    /// Read the stream to its end and gather the output.
    ///
    /// # Errors
    ///
    /// As [`Self::next_event`], plus [`E2bError::Protocol`] when the stream
    /// ends without a process end event.
    pub async fn collect(mut self) -> Result<ProcessOutput, E2bError> {
        let mut pid = None;
        let mut exit = None;
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut truncated = false;
        while let Some(event) = self.next_event().await? {
            match event {
                ProcessEvent::Started { pid: p } => pid = Some(p),
                ProcessEvent::Stdout(chunk) => truncated |= append(&mut stdout, chunk),
                ProcessEvent::Stderr(chunk) => truncated |= append(&mut stderr, chunk),
                ProcessEvent::Exited(e) => exit = Some(e),
            }
        }
        let exit = exit.ok_or_else(|| {
            E2bError::Protocol("the process stream ended without an end event".into())
        })?;
        Ok(ProcessOutput {
            pid,
            exit,
            stdout,
            stderr,
            truncated,
        })
    }

    fn scrub(&self, message: &str) -> String {
        let secrets: Vec<&str> = self.secrets.iter().map(SecretString::expose).collect();
        scrub(message, &secrets)
    }

    fn take_frame(&mut self) -> Result<Option<(u8, Vec<u8>)>, E2bError> {
        if self.buf.len() < FRAME_HEADER {
            return Ok(None);
        }
        let len = u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]);
        let len = usize::try_from(len).unwrap_or(usize::MAX);
        if len > MAX_FRAME {
            self.finished = true;
            return Err(E2bError::Protocol(format!(
                "frame of {len} bytes is too large"
            )));
        }
        if self.buf.len() < FRAME_HEADER + len {
            return Ok(None);
        }
        let flags = self.buf[0];
        let payload = self.buf[FRAME_HEADER..FRAME_HEADER + len].to_vec();
        self.buf.drain(..FRAME_HEADER + len);
        Ok(Some((flags, payload)))
    }
}

fn convert(event: EventJson) -> Result<Option<ProcessEvent>, E2bError> {
    if let Some(start) = event.start {
        return Ok(Some(ProcessEvent::Started { pid: start.pid }));
    }
    if let Some(data) = event.data {
        if let Some(out) = data.stdout.or(data.pty) {
            return Ok(Some(ProcessEvent::Stdout(OutputChunk(decode_b64(&out)?))));
        }
        if let Some(err) = data.stderr {
            return Ok(Some(ProcessEvent::Stderr(OutputChunk(decode_b64(&err)?))));
        }
        return Ok(None);
    }
    if let Some(end) = event.end {
        return Ok(Some(ProcessEvent::Exited(ProcessExit {
            exit_code: end.exit_code,
            exited: end.exited,
            status: end.status,
            error: end.error,
        })));
    }
    Ok(None)
}

fn decode_b64(text: &str) -> Result<Vec<u8>, E2bError> {
    base64::engine::general_purpose::STANDARD
        .decode(text)
        .map_err(|_| E2bError::Protocol("process output is not base64".into()))
}

fn decode<T: serde::de::DeserializeOwned>(payload: &[u8], what: &str) -> Result<T, E2bError> {
    serde_json::from_slice(payload).map_err(|e| {
        E2bError::Protocol(format!(
            "malformed {what} at line {} column {}",
            e.line(),
            e.column()
        ))
    })
}

fn frame(flags: u8, payload: &[u8]) -> Result<Vec<u8>, E2bError> {
    let len = u32::try_from(payload.len())
        .map_err(|_| E2bError::InvalidArgument("request is too large".into()))?;
    let mut out = Vec::with_capacity(FRAME_HEADER + payload.len());
    out.push(flags);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

/// Append up to the cap; returns `true` when bytes were dropped.
fn append(into: &mut Vec<u8>, chunk: OutputChunk) -> bool {
    let room = MAX_COLLECTED.saturating_sub(into.len());
    let bytes = chunk.into_bytes();
    if bytes.len() <= room {
        into.extend_from_slice(&bytes);
        false
    } else {
        into.extend_from_slice(&bytes[..room]);
        true
    }
}

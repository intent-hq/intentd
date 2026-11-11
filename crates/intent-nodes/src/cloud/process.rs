use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use super::SecretString;

/// A command to run in a cloud machine.
#[derive(Clone)]
pub struct ProcessSpec {
    pub cmd: String,
    pub args: Vec<String>,
    pub envs: BTreeMap<String, SecretString>,
    pub cwd: Option<String>,
    /// System user to run as. `None` lets the provider choose.
    pub user: Option<String>,
    /// Ends the process after this time.
    pub timeout: Option<Duration>,
}

impl ProcessSpec {
    /// A spec for `cmd` that runs as the machine's default user.
    #[must_use]
    pub fn new(cmd: impl Into<String>) -> Self {
        Self {
            cmd: cmd.into(),
            args: Vec::new(),
            envs: BTreeMap::new(),
            cwd: None,
            user: None,
            timeout: None,
        }
    }
}

impl fmt::Debug for ProcessSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessSpec")
            .field("cmd", &self.cmd)
            .field("args", &self.args)
            .field("envs", &self.envs.keys().collect::<Vec<_>>())
            .field("cwd", &self.cwd)
            .field("user", &self.user)
            .field("timeout", &self.timeout)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessExit {
    pub exit_code: i32,
    pub exited: bool,
    /// The provider's text, for example `exit status 1` or `signal: killed`.
    pub status: String,
    pub error: Option<String>,
}

/// Everything a finished process wrote.
#[derive(Clone)]
pub struct ProcessOutput {
    pub pid: Option<u32>,
    pub exit: ProcessExit,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    /// Output past the collection cap was dropped.
    pub truncated: bool,
}

impl ProcessOutput {
    #[must_use]
    pub fn success(&self) -> bool {
        self.exit.exited && self.exit.exit_code == 0 && self.exit.error.is_none()
    }
}

impl fmt::Debug for ProcessOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessOutput")
            .field("pid", &self.pid)
            .field("exit", &self.exit)
            .field("stdout_len", &self.stdout.len())
            .field("stderr_len", &self.stderr.len())
            .field("truncated", &self.truncated)
            .finish()
    }
}

//! Native Codex remains the credential authority. Isolated app-servers receive
//! access tokens over private stdio and never own its refresh token or logout.
//! The lock coordinates Intent readers only; ordinary native CLI processes
//! retain their upstream refresh/concurrent-login limitations.
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use base64::Engine;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

pub(crate) const NATIVE_ENV: &[(&str, &str)] = &[
    ("XDG_CONFIG_HOME", "INTENT_CODEX_NATIVE_XDG_CONFIG_HOME"),
    ("XDG_DATA_HOME", "INTENT_CODEX_NATIVE_XDG_DATA_HOME"),
    ("XDG_CACHE_HOME", "INTENT_CODEX_NATIVE_XDG_CACHE_HOME"),
    ("XDG_RUNTIME_DIR", "INTENT_CODEX_NATIVE_XDG_RUNTIME_DIR"),
    (
        "DBUS_SESSION_BUS_ADDRESS",
        "INTENT_CODEX_NATIVE_DBUS_SESSION_BUS_ADDRESS",
    ),
];

const AUTH_FRAME_LIMIT: usize = 8 * 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(7);
const AUTH_ERROR: &str =
    "Native Codex authentication is unavailable. Run codex login on this daemon host, then Retry.";
const BUSY_ERROR: &str =
    "Native Codex authentication is busy. Retry shortly; your native login is unchanged.";
const CONTRACT_ERROR: &str = "The installed Codex does not support Intent's native authentication bridge. Update Codex on this daemon host.";
const POLICY_ERROR: &str = "Managed Codex credential storage cannot safely be used by an isolated Intent worker. Ask your administrator for a supported configuration.";
const ACCOUNT_ERROR: &str = "Native Codex is signed in to a different account. Restore this agent's original account or start a new agent.";
type Result<T> = std::result::Result<T, &'static str>;

fn response_error(frame: &Value) -> &'static str {
    match frame["error"]["code"].as_i64() {
        Some(-32602..=-32600) => CONTRACT_ERROR,
        _ => AUTH_ERROR,
    }
}

// Deliberately no Debug or Serialize: tokens must never become diagnostics.
struct Credentials {
    token: String,
    mode: String,
    account: Option<String>,
    identity: String,
}
impl Credentials {
    fn from_status(status: &Value) -> Result<Option<Self>> {
        if status["requiresOpenaiAuth"] == false {
            return Ok(None);
        }
        let token = status["authToken"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or(AUTH_ERROR)?
            .to_owned();
        let mode = status["authMethod"].as_str().ok_or(AUTH_ERROR)?.to_owned();
        let (account, identity) = match mode.as_str() {
            "chatgpt" => {
                let claims = token_claims(&token)?;
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|_| AUTH_ERROR)?
                    .as_secs();
                if claims["exp"].as_u64().is_none_or(|exp| exp <= now) {
                    return Err(AUTH_ERROR);
                }
                let (account, user) = token_identity(&token)?;
                let identity = fingerprint(&format!("chatgpt\0{account}\0{user}"));
                (Some(account), identity)
            }
            "apikey" => (None, fingerprint(&format!("apikey\0{token}"))),
            _ => return Err(CONTRACT_ERROR),
        };
        Ok(Some(Self {
            token,
            mode,
            account,
            identity,
        }))
    }
    fn login(&self) -> Value {
        if let Some(account) = &self.account {
            json!({"type":"chatgptAuthTokens", "accessToken":self.token, "chatgptAccountId":account})
        } else {
            json!({"type":"apiKey", "apiKey":self.token})
        }
    }
    fn refresh(&self) -> Result<Value> {
        let account = self.account.as_ref().ok_or(AUTH_ERROR)?;
        Ok(json!({"accessToken":self.token,"chatgptAccountId":account}))
    }
}
fn fingerprint(value: &str) -> String {
    use std::fmt::Write;
    Sha256::digest(value.as_bytes())
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            write!(out, "{byte:02x}").expect("writing into a String cannot fail");
            out
        })
}
fn token_claims(token: &str) -> Result<Value> {
    let payload = token.split('.').nth(1).ok_or(AUTH_ERROR)?;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| AUTH_ERROR)?;
    serde_json::from_slice(&decoded).map_err(|_| AUTH_ERROR)
}
fn token_identity(token: &str) -> Result<(String, String)> {
    let claims = token_claims(token)?;
    let auth = &claims["https://api.openai.com/auth"];
    let account = auth["chatgpt_account_id"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or(AUTH_ERROR)?;
    let user = auth["chatgpt_user_id"]
        .as_str()
        .or_else(|| claims["sub"].as_str())
        .filter(|s| !s.is_empty())
        .ok_or(AUTH_ERROR)?;
    Ok((account.to_owned(), user.to_owned()))
}
struct Frames<R> {
    reader: R,
    bytes: Vec<u8>,
    limit: Option<usize>,
}
impl<R: AsyncBufRead + Unpin> Frames<R> {
    fn new(reader: R) -> Self {
        Self {
            reader,
            bytes: Vec::new(),
            limit: None,
        }
    }
    // Bytes live on the reader, so cancelling a select branch cannot discard
    // a partial frame while the other direction makes progress.
    async fn next(&mut self) -> Result<Option<Value>> {
        loop {
            let buffer = self.reader.fill_buf().await.map_err(|_| CONTRACT_ERROR)?;
            if buffer.is_empty() {
                return if self.bytes.is_empty() {
                    Ok(None)
                } else {
                    Err(CONTRACT_ERROR)
                };
            }
            let take = buffer
                .iter()
                .position(|&b| b == b'\n')
                .map_or(buffer.len(), |i| i + 1);
            if self
                .limit
                .is_some_and(|limit| self.bytes.len().saturating_add(take) > limit)
            {
                return Err(CONTRACT_ERROR);
            }
            self.bytes.extend_from_slice(&buffer[..take]);
            self.reader.consume(take);
            if self.bytes.last() == Some(&b'\n') {
                let result = serde_json::from_slice(&self.bytes)
                    .map(Some)
                    .map_err(|_| CONTRACT_ERROR);
                self.bytes.clear();
                return result;
            }
        }
    }
}
async fn write_frame(writer: &mut (impl AsyncWrite + Unpin), value: &Value) -> Result<()> {
    let mut bytes = serde_json::to_vec(value).map_err(|_| CONTRACT_ERROR)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await.map_err(|_| CONTRACT_ERROR)?;
    writer.flush().await.map_err(|_| CONTRACT_ERROR)
}
struct Server {
    child: Child,
    input: ChildStdin,
    output: Frames<BufReader<ChildStdout>>,
}
impl Server {
    fn spawn(command: &mut Command) -> Result<Self> {
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| CONTRACT_ERROR)?;
        let input = child.stdin.take().ok_or(CONTRACT_ERROR)?;
        let output = Frames::new(BufReader::new(child.stdout.take().ok_or(CONTRACT_ERROR)?));
        Ok(Self {
            child,
            input,
            output,
        })
    }
    async fn stop(&mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
    async fn native_call(&mut self, id: u64, method: &str, params: Value) -> Result<Value> {
        // Only authority responses have an auth-sized cap. Worker traffic must
        // preserve ACP's inline images, tool output and session history sizes.
        self.output.limit = Some(AUTH_FRAME_LIMIT);
        write_frame(
            &mut self.input,
            &json!({"id":id,"method":method,"params":params}),
        )
        .await?;
        loop {
            let frame = self.output.next().await?.ok_or(CONTRACT_ERROR)?;
            if frame["id"] == id {
                if frame.get("error").is_some() {
                    return Err(response_error(&frame));
                }
                return frame.get("result").cloned().ok_or(CONTRACT_ERROR);
            }
            // The authority only services auth reads. No tools, threads, model
            // calls or external-token callbacks are accepted from this child.
            if frame.get("id").is_some() {
                return Err(CONTRACT_ERROR);
            }
        }
    }
}

struct Authority {
    runtime: PathBuf,
    home: PathBuf,
    user_home: PathBuf,
}
impl Authority {
    async fn read(&self, previous: Option<&str>) -> Result<Option<Credentials>> {
        tokio::time::timeout(TIMEOUT, self.read_inner(previous))
            .await
            .map_err(|_| AUTH_ERROR)?
    }
    async fn read_inner(&self, previous: Option<&str>) -> Result<Option<Credentials>> {
        std::fs::create_dir_all(&self.home).map_err(|_| AUTH_ERROR)?;
        let home = self.home.canonicalize().map_err(|_| AUTH_ERROR)?;
        let lock = auth_lock(&home).await?;
        let mut command = Command::new(&self.runtime);
        command
            .args(["app-server", "--strict-config"])
            .env("CODEX_HOME", &home)
            .env("HOME", &self.user_home)
            .env("USERPROFILE", &self.user_home)
            .env_remove("CODEX_CONFIG")
            .current_dir(&home)
            .stderr(Stdio::null());
        for (key, saved) in NATIVE_ENV {
            if let Some(value) = std::env::var_os(saved) {
                if value.is_empty() {
                    command.env_remove(key);
                } else {
                    command.env(key, value);
                }
            }
        }
        // Keep the OS lock in the authority child too. If the proxy is killed,
        // a surviving helper cannot rotate outside the lock while it exits.
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            let fd = lock.as_raw_fd();
            unsafe {
                command.pre_exec(move || {
                    if libc::fcntl(fd, libc::F_SETFD, 0) == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }
        let mut server = Server::spawn(&mut command)?;
        let result = tokio::time::timeout(TIMEOUT, async {
            server.native_call(1, "initialize", json!({"clientInfo":{"name":"intent-native-auth","version":env!("CARGO_PKG_VERSION")},"capabilities":{"experimentalApi":true}})).await?;
            write_frame(&mut server.input, &json!({"method":"initialized"})).await?;
            let requirements = server.native_call(4, "configRequirements/read", json!({})).await?;
            let layers = server.native_call(5, "config/read", json!({"includeLayers":true})).await?;
            worker_policy(&requirements, &layers)?;
            let mut status = server.native_call(2, "getAuthStatus", json!({"includeToken":true,"refreshToken":false})).await?;
            // Native status can export an expired access token without trying
            // its still-valid refresh lineage. Renew before worker injection.
            let unusable_chatgpt = status["authMethod"] == "chatgpt"
                && Credentials::from_status(&status).is_err();
            if unusable_chatgpt || previous.is_some_and(|old| status["authToken"].as_str() == Some(old)) {
                status = server.native_call(3, "getAuthStatus", json!({"includeToken":true,"refreshToken":true})).await?;
            }
            if previous.is_some_and(|old| status["authToken"].as_str() == Some(old)) { return Err(AUTH_ERROR); }
            Credentials::from_status(&status)
        }).await.unwrap_or(Err(AUTH_ERROR));
        server.stop().await;
        drop(lock);
        result
    }
}
fn worker_policy(requirements: &Value, config: &Value) -> Result<()> {
    let required = requirements.get("requirements").ok_or(CONTRACT_ERROR)?;
    if !required.is_null() {
        match required
            .get("cliAuthCredentialsStore")
            .ok_or(CONTRACT_ERROR)?
        {
            Value::Null => {}
            Value::String(mode) if mode == "ephemeral" => {}
            _ => return Err(POLICY_ERROR),
        }
    }
    for layer in config["layers"].as_array().ok_or(CONTRACT_ERROR)? {
        let kind = layer["name"]["type"].as_str().ok_or(CONTRACT_ERROR)?;
        match kind {
            "packagedDefaults" | "system" | "user" | "project" | "sessionFlags" => {}
            "mdm"
            | "enterpriseManaged"
            | "legacyManagedConfigTomlFromFile"
            | "legacyManagedConfigTomlFromMdm" => {
                if layer["config"]
                    .get("cli_auth_credentials_store")
                    .is_some_and(|mode| mode != "ephemeral")
                {
                    return Err(POLICY_ERROR);
                }
            }
            _ => return Err(CONTRACT_ERROR),
        }
    }
    Ok(())
}

async fn auth_lock(home: &Path) -> Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(home.join(".intent-auth.lock"))
        .map_err(|_| AUTH_ERROR)?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        match fs2::FileExt::try_lock_exclusive(&file) {
            Ok(()) => return Ok(file),
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Err(BUSY_ERROR);
            }
            Err(_) => return Err(AUTH_ERROR),
        }
    }
}

fn read_record(path: &Path) -> Result<Option<Vec<u8>>> {
    use std::io::Read;
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(AUTH_ERROR),
    };
    let meta = file.metadata().map_err(|_| AUTH_ERROR)?;
    if !meta.is_file() || meta.len() > 65536 {
        return Err(AUTH_ERROR);
    }
    let mut bytes = Vec::new();
    file.take(65537)
        .read_to_end(&mut bytes)
        .map_err(|_| AUTH_ERROR)?;
    if bytes.len() > 65536 {
        return Err(AUTH_ERROR);
    }
    Ok(Some(bytes))
}

fn persist_identity(profile: &Path, identity: &str) -> Result<()> {
    use std::io::Write;
    let marker = profile.join(".intent-native-account");
    match read_record(&marker)? {
        Some(saved) if saved != identity.as_bytes() => return Err(ACCOUNT_ERROR),
        Some(_) => {}
        None => {
            let mut file = tempfile::NamedTempFile::new_in(profile).map_err(|_| AUTH_ERROR)?;
            file.write_all(identity.as_bytes())
                .map_err(|_| AUTH_ERROR)?;
            file.as_file().sync_all().map_err(|_| AUTH_ERROR)?;
            match file.persist_noclobber(&marker) {
                Ok(_) => {}
                Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if read_record(&marker)?.as_deref() != Some(identity.as_bytes()) {
                        return Err(ACCOUNT_ERROR);
                    }
                }
                Err(_) => return Err(AUTH_ERROR),
            }
        }
    }
    // The binding must survive a crash before any legacy credential is removed.
    std::fs::File::open(profile)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| AUTH_ERROR)
}

fn migrate_credentials(profile: &Path, native: &Path) -> Result<()> {
    let profile_path = profile.canonicalize().map_err(|_| AUTH_ERROR)?;
    if native.canonicalize().ok().as_ref() == Some(&profile_path) {
        return Err(AUTH_ERROR);
    }
    let mut sources = Vec::new();
    let mut identity = None;
    for name in ["auth.json", "auth.json.intent-legacy"] {
        let path = profile.join(name);
        if let Some(bytes) = read_record(&path)? {
            let auth: Value = serde_json::from_slice(&bytes).map_err(|_| AUTH_ERROR)?;
            let binding =
                if let Some(token) = auth.pointer("/tokens/access_token").and_then(Value::as_str) {
                    // Historical JWTs may have expired; only identity is retained.
                    let (account, user) = token_identity(token)?;
                    fingerprint(&format!("chatgpt\0{account}\0{user}"))
                } else if let Some(key) = auth["OPENAI_API_KEY"]
                    .as_str()
                    .filter(|key| !key.is_empty())
                {
                    fingerprint(&format!("apikey\0{key}"))
                } else {
                    return Err(AUTH_ERROR);
                };
            if identity.as_ref().is_some_and(|old| old != &binding) {
                return Err(ACCOUNT_ERROR);
            }
            identity = Some(binding);
            sources.push((path, bytes));
        }
    }
    if let Some(identity) = identity {
        // All sources agree before anything changes. Failed persistence retains
        // them; a retry after binding reuses the marker and finishes removal.
        persist_identity(profile, &identity)?;
        for (path, bytes) in sources {
            match read_record(&path)? {
                Some(current) if current == bytes => {
                    std::fs::remove_file(path).map_err(|_| AUTH_ERROR)?;
                }
                None => {}
                Some(_) => return Err(ACCOUNT_ERROR),
            }
        }
    }
    Ok(())
}

struct Bridge {
    authority: Authority,
    profile: PathBuf,
    credentials: Option<Credentials>,
}
impl Bridge {
    fn accept_identity(&self, credentials: &Credentials) -> Result<()> {
        persist_identity(&self.profile, &credentials.identity)
    }

    async fn refresh(&mut self, frame: &Value, server: &mut Server) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(8), self.refresh_inner(frame, server))
            .await
            .map_err(|_| AUTH_ERROR)?
    }
    async fn refresh_inner(&mut self, frame: &Value, server: &mut Server) -> Result<()> {
        let result = async {
            let old = self.credentials.as_ref().ok_or(AUTH_ERROR)?;
            if frame["params"]["previousAccountId"]
                .as_str()
                .is_some_and(|id| Some(id) != old.account.as_deref())
            {
                return Err(ACCOUNT_ERROR);
            }
            let credentials = self
                .authority
                .read(Some(&old.token))
                .await?
                .ok_or(AUTH_ERROR)?;
            self.accept_identity(&credentials)?;
            let result = credentials.refresh()?;
            self.credentials = Some(credentials);
            Ok(result)
        }
        .await;
        let response = match result {
            Ok(result) => json!({"id":frame["id"],"result":result}),
            Err(error) => json!({"id":frame["id"],"error":{"code":-32000,"message":error}}),
        };
        write_frame(&mut server.input, &response).await
    }
    async fn worker_call(
        &mut self,
        server: &mut Server,
        output: &mut (impl AsyncWrite + Unpin),
        params: Value,
    ) -> Result<()> {
        let id = format!("intent-auth-{}", uuid::Uuid::now_v7());
        write_frame(
            &mut server.input,
            &json!({"id":id,"method":"account/login/start","params":params}),
        )
        .await?;
        loop {
            let frame = server.output.next().await?.ok_or(CONTRACT_ERROR)?;
            if frame["id"] == id {
                return if frame.get("error").is_some() {
                    Err(response_error(&frame))
                } else {
                    Ok(())
                };
            }
            if frame["method"] == "account/chatgptAuthTokens/refresh" {
                self.refresh(&frame, server).await?;
            } else {
                write_frame(output, &frame).await?;
            }
        }
    }
    async fn synchronize(
        &mut self,
        server: &mut Server,
        output: &mut (impl AsyncWrite + Unpin),
    ) -> Result<()> {
        let credentials = self.authority.read(None).await?;
        if let Some(credentials) = credentials {
            self.accept_identity(&credentials)?;
            if self
                .credentials
                .as_ref()
                .is_none_or(|old| old.token != credentials.token || old.mode != credentials.mode)
            {
                let login = credentials.login();
                self.credentials = Some(credentials);
                tokio::time::timeout(TIMEOUT, self.worker_call(server, output, login))
                    .await
                    .map_err(|_| AUTH_ERROR)??;
            }
        } else if self.credentials.is_some() || self.profile.join(".intent-native-account").exists()
        {
            return Err(AUTH_ERROR);
        }
        Ok(())
    }
    async fn proxy(
        &mut self,
        server: &mut Server,
        input: &mut Frames<impl AsyncBufRead + Unpin>,
        output: &mut (impl AsyncWrite + Unpin),
    ) -> Result<()> {
        loop {
            tokio::select! {
                frame = input.next() => {
                    let Some(mut frame) = frame? else { return Ok(()); };
                    let method = frame["method"].as_str().unwrap_or("");
                    if method == "account/logout" {
                        // Worker logout is local detach, never global revocation.
                        server.stop().await;
                        write_frame(output, &json!({"id":frame["id"],"result":{}})).await?;
                        return Ok(());
                    }
                    if method.starts_with("account/login/") {
                        write_frame(output, &json!({"id":frame["id"],"error":{"code":-32000,"message":"Use native codex login on this daemon host, then Retry."}})).await?;
                        continue;
                    }
                    // Synchronize requests by default, including future methods:
                    // new upstream model operations must not silently bypass identity.
                    // Initialize precedes external login; cancellation/cleanup must
                    // remain usable after native logout. Responses and notifications
                    // finish existing work and retain their original protocol flow.
                    if frame.get("id").is_some() && !method.is_empty() && !matches!(method,
                        "initialize" | "turn/interrupt" | "thread/unsubscribe"
                        | "thread/backgroundTerminals/terminate" | "thread/goal/clear") {
                        if let Err(error) = self.synchronize(server, output).await {
                            write_frame(output, &json!({"id":frame["id"],"error":{"code":-32000,"message":error}})).await?;
                            // Contention rejects only this request. Keep existing
                            // work and cancellation alive so a later retry can sync.
                            if error == BUSY_ERROR { continue; }
                            return Ok(());
                        }
                    }
                    if method == "initialize" { frame["params"]["capabilities"]["experimentalApi"] = json!(true); }
                    write_frame(&mut server.input, &frame).await?;
                }
                frame = server.output.next() => {
                    let Some(frame) = frame? else { return Ok(()); };
                    if frame["method"] == "account/chatgptAuthTokens/refresh" { self.refresh(&frame, server).await?; }
                    else { write_frame(output, &frame).await?; }
                }
            }
        }
    }
}

/// Private CLI entrypoint. Only app-server and version discovery are supported;
/// credentials travel through stdio, never arguments, environment or logging.
///
/// # Errors
/// Returns a fixed, credential-free diagnostic on failure.
pub async fn run(
    runtime: PathBuf,
    native_home: PathBuf,
    user_home: PathBuf,
    profile: PathBuf,
    args: Vec<String>,
) -> std::result::Result<(), String> {
    if args.len() == 1 && matches!(args[0].as_str(), "--version" | "-V") {
        return Command::new(runtime)
            .arg("--version")
            .status()
            .await
            .map_err(|_| CONTRACT_ERROR.to_string())
            .and_then(|s| {
                if s.success() {
                    Ok(())
                } else {
                    Err(CONTRACT_ERROR.to_string())
                }
            });
    }
    if args.first().map(String::as_str) != Some("app-server") {
        return Err(CONTRACT_ERROR.into());
    }
    migrate_credentials(&profile, &native_home).map_err(str::to_owned)?;
    let authority = Authority {
        runtime: runtime.clone(),
        home: native_home,
        user_home,
    };
    // Native policy can override session flags. Inspect its resolved requirements
    // before a worker can touch any persisted profile/keyring credential.
    if let Err(error) = authority.read(None).await {
        // Deliver an actionable protocol error as well as the redacted stderr
        // hint. Otherwise adapters reduce a pre-bootstrap failure to EOF.
        let mut input = Frames::new(BufReader::new(tokio::io::stdin()));
        if let Ok(Ok(Some(frame))) =
            tokio::time::timeout(Duration::from_secs(1), input.next()).await
        {
            if let Some(id) = frame.get("id") {
                let code = if error == AUTH_ERROR { 401 } else { -32000 };
                let _ = write_frame(
                    &mut tokio::io::stdout(),
                    &json!({"id":id,"error":{"code":code,"message":error}}),
                )
                .await;
            }
        }
        return Err(error.into());
    }
    let mut command = Command::new(&runtime);
    command
        .args(args)
        .args([
            "--strict-config",
            "-c",
            "cli_auth_credentials_store=\"ephemeral\"",
        ])
        .env("CODEX_HOME", &profile)
        .stderr(Stdio::inherit());
    let mut server = Server::spawn(&mut command).map_err(str::to_owned)?;
    let mut bridge = Bridge {
        authority,
        profile,
        credentials: None,
    };
    let result = bridge
        .proxy(
            &mut server,
            &mut Frames::new(BufReader::new(tokio::io::stdin())),
            &mut tokio::io::stdout(),
        )
        .await;
    server.stop().await;
    result.map_err(str::to_owned)
}

/// Install an owned launcher, without rewriting the adapter or installed CLI.
/// All arguments here are filesystem paths, never credentials.
///
/// # Errors
/// Returns a fixed diagnostic if the owned launcher cannot be installed.
#[cfg(unix)]
pub fn install_wrapper(
    profile: &Path,
    runtime: &Path,
    native: &Path,
    user_home: &Path,
    helper: &Path,
) -> std::result::Result<PathBuf, String> {
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    let quote = |p: &Path| format!("'{}'", p.to_string_lossy().replace('\'', "'\\''"));
    migrate_credentials(profile, native).map_err(str::to_owned)?;
    let body = format!("#!/bin/sh\nexec {} provider codex-auth-bridge --runtime {} --native-home {} --user-home {} --profile {} -- \"$@\"\n", quote(helper), quote(runtime), quote(native), quote(user_home), quote(profile));
    let path = profile.join("codex-native-auth.sh");
    let result = (|| -> std::io::Result<()> {
        let mut file = tempfile::NamedTempFile::new_in(profile)?;
        file.write_all(body.as_bytes())?;
        file.as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o700))?;
        file.persist(&path).map_err(std::io::Error::other)?;
        Ok(())
    })();
    result.map_err(|_| CONTRACT_ERROR.to_string())?;
    Ok(path)
}
#[cfg(not(unix))]
pub fn install_wrapper(
    _: &Path,
    _: &Path,
    _: &Path,
    _: &Path,
    _: &Path,
) -> std::result::Result<PathBuf, String> {
    Err("Native Codex authentication bridge requires a Unix daemon host.".into())
}

#[cfg(all(test, unix))]
mod tests;

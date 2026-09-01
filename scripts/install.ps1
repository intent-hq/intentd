# One-line installer for the intentd sitter (Windows x86_64).
#
#   powershell -c "irm https://github.com/intent-hq/intentd-releases/releases/download/sitter-latest/install.ps1 | iex"
#
# Downloads the intentd-x86_64-pc-windows-msvc.zip archive from the fixed
# sitter-latest release on the public intent-hq/intentd-releases mirror,
# verifies its .sha256 sidecar, installs intentd.exe (the self-updating
# sitter) to $env:INTENTD_INSTALL_DIR (default: $env:LOCALAPPDATA\intentd\bin),
# and adds the install dir to the user PATH. Idempotent: re-running replaces
# the installed binary.
#
# After a successful install it offers to register a per-user Scheduled Task
# ("intentd") that runs `intentd serve` at logon, and starts it now. The
# prompt only appears on an interactive console and never hangs in
# non-interactive runs (default: skip with a hint). Force either way:
#
#   $env:INTENTD_INSTALL_SERVICE = '1'  (or -Service, on direct runs)     set up
#   $env:INTENTD_INSTALL_SERVICE = '0'  (or -NoService, on direct runs)   skip
#
# When the task is set up, the installer also asks whether the service should
# auto-resume interrupted agents at startup (the daemon setting
# agents.resumeInterruptedOnStart). The default 'auto' resumes only on
# headless hosts, so answering auto — or a non-interactive run — writes
# nothing; on/off are applied via `intentd settings` once the daemon is up.
# $env:INTENTD_AUTO_RESUME = 'auto'|'on'|'off' (or -AutoResume <value>, on
# direct runs) forces an answer.
#
# Task setup is refused up front when a daemon is already running and owns the
# data dir the task would serve: a daemon locks its data dir for its whole
# lifetime, so a second one on the same dir can only crash-loop. One exception:
# a daemon running under the "intentd" task itself does not block a re-run -
# that is an upgrade, and the task is re-registered and restarted onto the new
# binary. Any other owner refuses the setup and nothing is registered.
#
# $env:INTENTD_SERVICE_NAME overrides the task name (testing).
param(
    [switch]$Service,
    [switch]$NoService,
    [string]$AutoResume = ''
)
$ErrorActionPreference = 'Stop'
# Windows PowerShell 5.1: silence the progress bar (it slows Invoke-WebRequest
# dramatically) and force TLS 1.2, which older .NET defaults omit.
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

$BaseUrl = 'https://github.com/intent-hq/intentd-releases/releases/download/sitter-latest'
$Archive = 'intentd-x86_64-pc-windows-msvc.zip'

$arch = $env:PROCESSOR_ARCHITECTURE
if ($arch -ne 'AMD64') {
    throw "install.ps1: unsupported architecture '$arch' (only x86_64/AMD64 Windows builds are published)"
}

# Validate -AutoResume and INTENTD_AUTO_RESUME before any download so garbage
# fails fast (matching install.sh) instead of aborting mid-install after the
# binary is already on disk. Both consumed in the service branch below, where
# the parameter beats the env var.
$autoResumeArg = ''
if ($AutoResume) {
    $autoResumeArg = $AutoResume.Trim().ToLowerInvariant()
    if (@('auto', 'on', 'off') -notcontains $autoResumeArg) {
        throw "install.ps1: invalid -AutoResume value '$AutoResume' (expected auto, on, or off)"
    }
}
$autoResumeEnv = ''
if ($env:INTENTD_AUTO_RESUME) {
    $autoResumeEnv = $env:INTENTD_AUTO_RESUME.Trim().ToLowerInvariant()
    if (@('auto', 'on', 'off') -notcontains $autoResumeEnv) {
        throw "install.ps1: invalid INTENTD_AUTO_RESUME value '$env:INTENTD_AUTO_RESUME' (expected auto, on, or off)"
    }
}

# >>> BEGIN resolve-data-dir (extracted verbatim and executed by
# crates/intentd-sitter/tests/install_ps1_owner.rs — keep these markers).
#
# Anchor a relative INTENTD_DATA_DIR to the directory the installer runs in,
# once, before anything reads it — the same normalization install.sh's
# resolve_data_dir does. The override is carried verbatim into the Scheduled
# Task action, and a task has no working directory of its own, so a bare
# `.\data` would otherwise name one dir here — where the ownership check below
# looks, and where this run's `intentd` calls read — and a different one once
# the task starts. GetUnresolvedProviderPathFromPSPath resolves against the
# current location without requiring the dir to exist yet, and leaves an
# already-absolute path unchanged.
if ($env:INTENTD_DATA_DIR) {
    $env:INTENTD_DATA_DIR =
        $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($env:INTENTD_DATA_DIR)
}
# <<< END resolve-data-dir

$installDir = if ($env:INTENTD_INSTALL_DIR) {
    $env:INTENTD_INSTALL_DIR
} else {
    Join-Path $env:LOCALAPPDATA 'intentd\bin'
}
New-Item -ItemType Directory -Force -Path $installDir | Out-Null

$tmp = Join-Path ([System.IO.Path]::GetTempPath()) ('intentd-install-' + [System.IO.Path]::GetRandomFileName())
New-Item -ItemType Directory -Force -Path $tmp | Out-Null
try {
    Write-Host "install.ps1: downloading $Archive from the sitter-latest release..."
    $zipPath = Join-Path $tmp $Archive
    Invoke-WebRequest -Uri "$BaseUrl/$Archive" -OutFile $zipPath -UseBasicParsing
    Invoke-WebRequest -Uri "$BaseUrl/$Archive.sha256" -OutFile "$zipPath.sha256" -UseBasicParsing

    # Sidecar format is "HASH *NAME" (sha256sum --binary); the hash is the
    # first whitespace-delimited token.
    $expected = ((Get-Content "$zipPath.sha256" -Raw).Trim() -split '\s+')[0].ToLowerInvariant()
    $actual = (Get-FileHash -Algorithm SHA256 -Path $zipPath).Hash.ToLowerInvariant()
    if ($actual -ne $expected) {
        throw "install.ps1: sha256 verification failed for $Archive (expected $expected, got $actual)"
    }
    Write-Host 'install.ps1: sha256 verified'

    Expand-Archive -Path $zipPath -DestinationPath $tmp -Force
    $binary = Join-Path $tmp 'intentd.exe'
    if (-not (Test-Path $binary)) {
        throw 'install.ps1: archive did not contain intentd.exe'
    }
    # Windows locks a running executable's file against writes and deletes but
    # allows renaming it, so move any existing binary aside before copying the
    # new one — keeps re-runs working while `intentd serve` is running. Stale
    # .old files from earlier updates are swept opportunistically (a locked one
    # is skipped and picked up next time).
    $dest = Join-Path $installDir 'intentd.exe'
    Get-ChildItem -Path $installDir -Filter 'intentd.exe.*.old' -ErrorAction SilentlyContinue |
        Remove-Item -Force -ErrorAction SilentlyContinue
    if (Test-Path $dest) {
        Move-Item $dest "$dest.$PID.$(Get-Random).old" -Force
    }
    Copy-Item $binary $dest -Force
} finally {
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
}

# Add the install dir to the user PATH (persisted) and the current session.
# Read/write the registry value raw so REG_EXPAND_SZ entries like
# %USERPROFILE%\bin are preserved verbatim ([Environment]:: expands them,
# which would flatten the stored value on rewrite).
$envKey = [Microsoft.Win32.Registry]::CurrentUser.OpenSubKey('Environment', $true)
try {
    $userPath = ''
    $valueKind = [Microsoft.Win32.RegistryValueKind]::ExpandString
    if ($envKey.GetValueNames() -contains 'Path') {
        $userPath = [string]$envKey.GetValue('Path', '', [Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames)
        $valueKind = $envKey.GetValueKind('Path')
    }
    $parts = @(($userPath -split ';') | Where-Object { $_ -ne '' })
    if ($parts -notcontains $installDir) {
        $envKey.SetValue('Path', (($parts + $installDir) -join ';'), $valueKind)
        Write-Host "install.ps1: added $installDir to your user PATH (new terminals pick it up automatically)"
    }
} finally {
    $envKey.Close()
}
if (@($env:Path -split ';') -notcontains $installDir) {
    $env:Path = "$env:Path;$installDir"
}

Write-Host "install.ps1: installed intentd to $installDir\intentd.exe"

# Service setup decision: switches beat the env var beat the prompt. Never
# hang in non-interactive runs — skip with a hint instead.
$serviceMode = ''
if ($Service) { $serviceMode = 'yes' }
elseif ($NoService) { $serviceMode = 'no' }
elseif ($env:INTENTD_INSTALL_SERVICE) {
    $serviceMode = if (@('0', 'false', 'no') -contains $env:INTENTD_INSTALL_SERVICE.ToLowerInvariant()) { 'no' } else { 'yes' }
}
if (-not $serviceMode) {
    if ([Environment]::UserInteractive -and -not [Console]::IsInputRedirected) {
        $reply = Read-Host 'Set up intentd to start at logon and start it now? [Y/n]'
        $serviceMode = if ($reply -match '^[nN]') { 'no' } else { 'yes' }
    } else {
        $serviceMode = 'skip'
    }
}

if ($serviceMode -eq 'yes') {
    # >>> BEGIN data-dir-owner-check (extracted verbatim and executed by
    # crates/intentd-sitter/tests/install_ps1_owner.rs — keep these markers,
    # and keep the block self-contained: its only inputs are the environment).
    #
    # Before anything is asked or registered: refuse a data dir a live daemon
    # already owns (install.sh's check_data_dir_not_owned). The daemon holds an
    # exclusive lock on its data dir for as long as it runs, so a second task on
    # the same dir never serves anything - it crash-loops until the sitter gives
    # up, while the daemon it is fighting is usually the one the user wants to
    # keep. `<data_dir>\intentd.pid` names the owner and only a live owner
    # counts: a missing, unreadable or malformed pidfile - and the stale one a
    # crash or a hard reboot leaves behind - all mean "not owned". The default
    # data dir mirrors
    # intent_core::Config::resolve (the `directories` crate's roaming data dir).
    #
    # "Malformed" is read exactly as the daemon's read_pid
    # (crates/intentd/src/main.rs) reads it: the *whole* file, trimmed, parsed
    # as a u32 - so `123\nnot-a-pid` and `1 23` are malformed, not pid 123.
    # Taking only the first line would find an owner where the daemon finds
    # none, and an unrelated live pid would then abort a legitimate install
    # with nothing the user could do about it. [uint32]::TryParse accepts the
    # same leading `+`, leading zeros and surrounding whitespace u32::from_str
    # does, and rejects the same negatives and overflows.
    $dataDir = if ($env:INTENTD_DATA_DIR) { $env:INTENTD_DATA_DIR } else { Join-Path $env:APPDATA 'intentd\data' }
    $ownerPid = 0
    $pidFile = Join-Path $dataDir 'intentd.pid'
    try {
        if (Test-Path $pidFile) {
            $pidText = [string](Get-Content $pidFile -Raw -ErrorAction Stop)
            $parsed = [uint32]0
            if ([uint32]::TryParse($pidText.Trim(), [ref]$parsed) -and
                $parsed -gt 0 -and $parsed -le [int]::MaxValue) {
                if (Get-Process -Id ([int]$parsed) -ErrorAction SilentlyContinue) { $ownerPid = [int]$parsed }
            }
        }
    } catch {
        # An unreadable pidfile proves nothing; treat it as unowned.
        $ownerPid = 0
    }
    if ($ownerPid -gt 0) {
        # One owner is not foreign: the daemon of the very task this installer
        # manages ('intentd', or $env:INTENTD_SERVICE_NAME). Re-running the
        # installer then is an upgrade - the registration below stops that
        # exact task and restarts it onto the new binary - so it proceeds
        # instead of refusing (the same allowance install.sh makes for its
        # systemd unit / launchd label).
        # The owner counts as ours only when ALL of these hold; any query
        # error or uncertainty along the way falls through to the refusal
        # below, never to the allowance:
        #   1. A running task's full Path is exactly "\<task name>".
        #      Register-ScheduledTask -TaskName always lands the task in the
        #      root folder, so that path identifies the task this installer
        #      manages - never IRunningTask.Name, which is only the leaf name
        #      and which a foreign \other\<name> task shares.
        #   2. That task's engine process (IRunningTask.EnginePID) is on the
        #      owner's parent chain: Win32_Process ParentProcessId, walked a
        #      bounded number of hops, stopping when a parent started after
        #      its child (that parent pid was reused, so the chain is broken
        #      there).
        #   3. The walked chain also holds the pid recorded in
        #      <data_dir>\sitter\sitter.pid - the serve-mode sitter's own
        #      pidfile (PidFile in crates/intentd-sitter/src/supervisor.rs),
        #      i.e. the supervisor of the daemon serving *this* data dir.
        #      EnginePID alone is not enough: it names the Task Scheduler
        #      engine hosting the task, one engine can host several tasks,
        #      so "descends from the engine" would also admit a daemon some
        #      *other* task on a shared engine launched - and an image-path
        #      witness would too, since a foreign task can run this same
        #      installed intentd.exe. The sitter pidfile is written per data
        #      dir, so it ties the chain to the one supervisor of the data
        #      dir being checked. A missing, malformed or stale file is no
        #      witness, hence no allowance.
        # A daemon the task scheduler does not control - a manual `intentd
        # serve`, another task's tree - keeps being refused below, and so
        # does everything when the scheduler cannot be asked at all.
        $taskName = if ($env:INTENTD_SERVICE_NAME) { $env:INTENTD_SERVICE_NAME } else { 'intentd' }
        $taskEnginePid = 0
        try {
            $scheduler = New-Object -ComObject 'Schedule.Service'
            $scheduler.Connect()
            # 1 = TASK_ENUM_HIDDEN: include hidden tasks, harmless for ours.
            foreach ($runningTask in @($scheduler.GetRunningTasks(1))) {
                if ($runningTask.Path -eq "\$taskName") {
                    $taskEnginePid = [int]$runningTask.EnginePID
                    break
                }
            }
        } catch {
            # No task scheduler to ask (or access denied): no allowance.
            $taskEnginePid = 0
        }
        # Requirement 3's witness: the pid in <data_dir>\sitter\sitter.pid,
        # read exactly as the sitter's own read_live_pid reads it (whole
        # file, trimmed, parsed as u32) and only counted while that pid is
        # live. Missing, malformed, stale or dead all mean no witness,
        # hence no allowance.
        $sitterPid = 0
        try {
            $sitterPidFile = Join-Path (Join-Path $dataDir 'sitter') 'sitter.pid'
            if (Test-Path $sitterPidFile) {
                $sitterText = [string](Get-Content $sitterPidFile -Raw -ErrorAction Stop)
                $sitterParsed = [uint32]0
                if ([uint32]::TryParse($sitterText.Trim(), [ref]$sitterParsed) -and
                    $sitterParsed -gt 0 -and $sitterParsed -le [int]::MaxValue) {
                    if (Get-Process -Id ([int]$sitterParsed) -ErrorAction SilentlyContinue) { $sitterPid = [int]$sitterParsed }
                }
            }
        } catch {
            $sitterPid = 0
        }
        $ownedByOurTask = $false
        if ($taskEnginePid -gt 0 -and $sitterPid -gt 0) {
            $chainHasSitter = $false
            $chainPid = $ownerPid
            $chainRow = $null
            try { $chainRow = @(Get-CimInstance -ClassName Win32_Process -Filter "ProcessId = $chainPid" -ErrorAction Stop)[0] } catch { $chainRow = $null }
            for ($hop = 0; $hop -lt 12; $hop++) {
                if ($chainPid -eq $sitterPid) { $chainHasSitter = $true }
                if ($chainPid -eq $taskEnginePid) { $ownedByOurTask = $chainHasSitter; break }
                if (-not $chainRow) { break }
                $parentPid = [int]$chainRow.ParentProcessId
                if ($parentPid -le 0) { break }
                $parentRow = $null
                try { $parentRow = @(Get-CimInstance -ClassName Win32_Process -Filter "ProcessId = $parentPid" -ErrorAction Stop)[0] } catch { $parentRow = $null }
                if (-not $parentRow) { break }
                if ($chainRow.CreationDate -and $parentRow.CreationDate -and ($parentRow.CreationDate -gt $chainRow.CreationDate)) { break }
                $chainPid = $parentPid
                $chainRow = $parentRow
            }
        }
        if ($ownedByOurTask) {
            Write-Host "install.ps1: the running daemon (pid $ownerPid) belongs to the '$taskName' scheduled task - it will be restarted onto the new binary"
        } else {
            # Best-effort image path, so the refusal names the program instead
            # of a bare number (Get-Process cannot read Path for some
            # processes).
            $ownerPath = ''
            try { $ownerPath = (Get-Process -Id $ownerPid -ErrorAction Stop).Path } catch { $ownerPath = '' }
            $ownerDesc = if ($ownerPath) { "pid $ownerPid ($ownerPath)" } else { "pid $ownerPid" }
            throw ("install.ps1: an intentd daemon is already running and owns the data dir this task would use:`n" +
                "  data dir: $dataDir`n" +
                "  owner:    $ownerDesc`n" +
                "A daemon locks its data dir for as long as it runs, so a second task on the same dir cannot start - it would only crash-loop. Nothing has been registered.`n" +
                "Pick one:`n" +
                "  * keep the daemon that is already running - it serves this data dir now: intentd status`n" +
                "  * stop it first (quit the app that started it, or stop its task), then re-run this installer`n" +
                "  * give this task its own data dir: `$env:INTENTD_DATA_DIR = `"`$env:LOCALAPPDATA\intentd\service-data`"`n" +
                "  * install just the binary, with no task: re-run with `$env:INTENTD_INSTALL_SERVICE = '0'")
        }
    }
    # <<< END data-dir-owner-check
    # Auto-resume choice: parameter beats the env var beats the prompt (both
    # validated up front); 'auto' — or a non-interactive run — is the daemon
    # default and writes nothing. Applied via `intentd settings` after the
    # wait-for-daemon loop below, so on a re-install over an existing data dir
    # the first service start still runs under the prior effective setting.
    $autoResume = ''
    if ($autoResumeArg) {
        $autoResume = $autoResumeArg
    } elseif ($autoResumeEnv) {
        $autoResume = $autoResumeEnv
    } elseif ([Environment]::UserInteractive -and -not [Console]::IsInputRedirected) {
        $reply = (Read-Host 'Auto-resume interrupted agents when the service starts? [auto/on/off] (default auto)').Trim().ToLowerInvariant()
        if (@('on', 'off') -contains $reply) {
            $autoResume = $reply
        } else {
            if ($reply -and $reply -ne 'auto') {
                Write-Warning "install.ps1: unrecognized answer '$reply' - keeping the default (auto)"
            }
            $autoResume = 'auto'
        }
    } else {
        $autoResume = 'auto'
    }
    # Per-user Scheduled Task at logon: no admin rights needed, and -Force
    # makes re-runs update the existing task instead of duplicating it.
    $taskName = if ($env:INTENTD_SERVICE_NAME) { $env:INTENTD_SERVICE_NAME } else { 'intentd' }
    # Windows service log source: a Scheduled Task discards its action's
    # stderr, so every action wraps through cmd and appends stderr to a log
    # file - the analog of the LaunchAgent's StandardErrorPath install.sh
    # uses on macOS. The post-timeout diagnosis below reads this run's slice
    # of it to tell a permanent failure from a slow first download.
    $logDir = Join-Path $env:LOCALAPPDATA 'intentd'
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    $logFile = Join-Path $logDir 'intentd.err.log'
    # Carry a custom data dir into the task so it serves the same data dir the
    # install-time CLI used. A task action cannot set environment variables, so
    # wrap through cmd (the quotes survive & and spaces in the path). cmd binds
    # a redirection to a single command, so it trails the serve command in the
    # set && form; in the plain form it leads instead, so the /c payload does
    # not start with a quote (cmd strips a leading quote pair).
    $action = if ($env:INTENTD_DATA_DIR) {
        New-ScheduledTaskAction -Execute $env:ComSpec `
            -Argument ('/d /c set "INTENTD_DATA_DIR=' + $env:INTENTD_DATA_DIR + '" && "' + $dest + '" serve 2>>"' + $logFile + '"')
    } else {
        New-ScheduledTaskAction -Execute $env:ComSpec `
            -Argument ('/d /c 2>>"' + $logFile + '" "' + $dest + '" serve')
    }
    $trigger = New-ScheduledTaskTrigger -AtLogOn -User "$env:USERDOMAIN\$env:USERNAME"
    # S4U logon: the task runs as this user with the profile loaded but outside
    # the interactive session, so no console window pops up at every logon. The
    # daemon needs no interactive-session resources (the trade-off S4U makes).
    $principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType S4U
    # The daemon is long-running: disable the 72h execution limit and the
    # battery cutoffs, and restart it if it crashes.
    $settings = New-ScheduledTaskSettingsSet `
        -ExecutionTimeLimit ([TimeSpan]::Zero) `
        -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
        -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) `
        -StartWhenAvailable
    # A re-run replaces the on-disk binary; stop any running instance so the
    # restart below picks the new one up (Start-ScheduledTask is a no-op while
    # an instance is running under the default multiple-instances policy).
    if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
        Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    }
    Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
        -Principal $principal -Settings $settings `
        -Description 'Intent backend daemon (intentd)' -Force | Out-Null
    # Record where the log ends before this start, so the failure diagnosis
    # below never quotes a previous run's crash (mirrors install.sh's
    # note_log_start). A byte offset: the tail below slices raw bytes.
    $logOffset = 0
    if (Test-Path $logFile) { $logOffset = (Get-Item $logFile).Length }
    Start-ScheduledTask -TaskName $taskName
    Write-Host "install.ps1: scheduled task '$taskName' registered (runs at logon) and started"

    # This run's slice of the service log, bounded to 40 lines. Empty when
    # there is nothing to read; a file shorter than the noted offset was
    # rotated or replaced, so read it whole. The offset is a byte count taken
    # before decoding, so the first line can start mid-word; the lines that
    # matter come after it. The log is append-only and never rotated, so seek
    # with [long] offsets (it can exceed 2 GiB) and read at most a 256 KiB tail
    # instead of loading the whole file — only the last 40 lines are quoted
    # anyway. A failed read is reported as its own warning below, never passed
    # off as an empty log.
    function Get-ServiceLogSlice {
        param([string]$Path, [long]$Offset)
        $text = ''
        try {
            if (Test-Path $Path) {
                $stream = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open,
                    [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
                try {
                    $length = [long]$stream.Length
                    $sliceFrom = if ($length -lt $Offset) { [long]0 } else { $Offset }
                    $maxTailBytes = [long]262144
                    if (($length - $sliceFrom) -gt $maxTailBytes) { $sliceFrom = $length - $maxTailBytes }
                    $null = $stream.Seek($sliceFrom, [System.IO.SeekOrigin]::Begin)
                    $buffer = New-Object byte[] ([int]($length - $sliceFrom))
                    $read = 0
                    while ($read -lt $buffer.Length) {
                        $n = $stream.Read($buffer, $read, $buffer.Length - $read)
                        if ($n -le 0) { break }
                        $read += $n
                    }
                    $slice = [System.Text.Encoding]::UTF8.GetString($buffer, 0, $read).TrimEnd()
                    if ($slice) {
                        $lines = @($slice -split "`r?`n")
                        if ($lines.Count -gt 40) { $lines = $lines[($lines.Count - 40)..($lines.Count - 1)] }
                        $text = $lines -join "`n"
                    }
                } finally {
                    $stream.Dispose()
                }
            }
        } catch {
            return [pscustomobject]@{ Text = ''; Failed = $true }
        }
        return [pscustomobject]@{ Text = $text; Failed = $false }
    }

    # Wait for the first start to resolve into an outcome - up, failed, or (only
    # once the deadline runs out with nothing decided) unknown - rather than
    # classifying the log once at a fixed 60s mark. A sitter still resolving a
    # channel or downloading at that mark has not crashed yet, so the old window
    # reported "may still be downloading" seconds before the daemon proved it
    # could never start. Mirrors install.sh's verify_daemon, including its
    # knobs: $deadline bounds the whole wait, $settle is the grace after crash
    # evidence appears (the sitter respawns, so a daemon that died once and then
    # came up is a working install). Success still returns the moment the daemon
    # answers.
    $deadline = 300
    $poll = 2
    $settle = 10
    $progressAt = 60
    # The substrings matched below are the sitter's own log lines - a detection
    # contract with crates/intentd-sitter/src/supervisor.rs and install.sh,
    # pinned by the install_log_contract_* tests in
    # crates/intentd-sitter/tests/supervisor_e2e.rs. Change only in lockstep.
    Write-Host 'install.ps1: waiting for the daemon to respond (first start downloads the daemon binary)...'
    $verdict = ''
    $crashText = ''
    $crashAt = -1
    $logReadFailed = $false
    $progressShown = $false
    $waited = 0
    while ($true) {
        & $dest status *> $null
        if ($LASTEXITCODE -eq 0) { $verdict = 'up'; break }
        $slice = Get-ServiceLogSlice -Path $logFile -Offset ([long]$logOffset)
        $logReadFailed = $slice.Failed
        $logOut = $slice.Text
        if ($logOut.Contains('times in a row without ever staying up')) {
            # Terminal: the sitter has stopped trying, so nothing can change.
            $crashText = $logOut
            $verdict = 'gaveup'
            break
        } elseif ($logOut.Contains('exited unexpectedly') -or $logOut.Contains('failed to spawn') -or $logOut.Contains('failed waiting on intentd')) {
            $crashText = $logOut
            if ($crashAt -lt 0) { $crashAt = $waited }
            if (($waited - $crashAt) -ge $settle) { $verdict = 'crashing'; break }
        }
        if ($waited -ge $deadline) {
            $verdict = if ($crashText) { 'crashing' } else { 'undecided' }
            break
        }
        if (-not $progressShown -and $waited -ge $progressAt) {
            Write-Host "install.ps1: still waiting (${waited}s) - nothing has failed yet; a first download over a slow link can take a few minutes"
            $progressShown = $true
        }
        Start-Sleep -Seconds $poll
        $waited += $poll
    }
    if ($verdict -eq 'up') {
        Write-Host "install.ps1: daemon is up - 'intentd status' responds"
    } else {
        # An explicit on/off auto-resume answer is applied only once the daemon
        # is reachable, so any other outcome drops it - say so instead of
        # dropping it silently.
        $autoResumeNote = ''
        if (@('on', 'off') -contains $autoResume) {
            $autoResumeNote = "`nYour auto-resume choice ('$autoResume') was not applied; once the daemon is up, apply it with:`n  intentd settings agents.resumeInterruptedOnStart $autoResume"
        }
        $restartHint = "Start-ScheduledTask -TaskName $taskName"
        if ($verdict -eq 'gaveup') {
            Write-Host "install.ps1: the service's output for this start (from ${logFile}):"
            foreach ($line in ($crashText -split "`n")) { Write-Host "  | $line" }
            throw ("install.ps1: the daemon could not start and the sitter has given up - the service is stopped.`nFix the cause reported above, then start it again with:`n  $restartHint$autoResumeNote")
        } elseif ($verdict -eq 'crashing') {
            Write-Host "install.ps1: the service's output for this start (from ${logFile}):"
            foreach ($line in ($crashText -split "`n")) { Write-Host "  | $line" }
            throw ("install.ps1: the daemon is failing to start and the sitter is still respawning it; it gives up in a few minutes and leaves the service stopped.`nFix the cause reported above, then start it again with:`n  $restartHint$autoResumeNote")
        } elseif ($logReadFailed) {
            # A read failure is not an empty log: without the slice a crash and
            # a slow download are indistinguishable, so say so instead of
            # guessing "still downloading".
            Write-Warning "install.ps1: the daemon has not responded in ${waited}s and the service log could not be read ($logFile) - cannot tell whether it is still downloading or crashing; check later with: intentd status$autoResumeNote"
        } else {
            Write-Warning "install.ps1: the daemon has not responded in ${waited}s and nothing in this run's service log reports a failure - this install could not tell whether the daemon binary is still downloading or the task is stuck.`nThe task is registered and started; its output is in $logFile.`nCheck on it with: intentd status`nIf it is still not responding in a few minutes, restart it and re-read that log:`n  $restartHint$autoResumeNote"
        }
    }
    # 'auto' is the daemon default — nothing to write. A failure is a warning,
    # not a fatal install error: the setting can be changed later with the
    # same command. Only attempted once the daemon actually answered: an
    # undecided wait already told the user how to apply the setting themselves.
    if ($verdict -eq 'up' -and @('on', 'off') -contains $autoResume) {
        & $dest settings agents.resumeInterruptedOnStart $autoResume *> $null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "install.ps1: auto-resume on service start set to '$autoResume' (agents.resumeInterruptedOnStart)"
        } else {
            Write-Warning "install.ps1: could not set agents.resumeInterruptedOnStart=$autoResume - set it later with: intentd settings agents.resumeInterruptedOnStart $autoResume"
        }
    }
    Write-Host "install.ps1: manage the service with: Get-ScheduledTask/Start-ScheduledTask/Stop-ScheduledTask/Unregister-ScheduledTask -TaskName $taskName"
    Write-Host 'install.ps1: connecting from another machine (desktop/mobile app)? Run: intentd pair'
} else {
    if ($serviceMode -eq 'skip') {
        Write-Host 'install.ps1: skipping service setup (non-interactive session). To set it up, re-run with $env:INTENTD_INSTALL_SERVICE = ''1'''
    }
    Write-Host ''
    Write-Host 'Next steps:'
    Write-Host '  intentd serve   # start the daemon (downloads the real daemon on first run)'
    Write-Host '  intentd pair    # once the daemon is running (another terminal): pairing info for remote clients (desktop/mobile app)'
}

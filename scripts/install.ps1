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

    # First service start can be slow: the sitter downloads the real daemon
    # before serving.
    Write-Host 'install.ps1: waiting for the daemon to respond (first start downloads the daemon binary)...'
    $up = $false
    for ($waited = 0; $waited -lt 60; $waited += 2) {
        & $dest status *> $null
        if ($LASTEXITCODE -eq 0) { $up = $true; break }
        Start-Sleep -Seconds 2
    }
    if ($up) {
        Write-Host "install.ps1: daemon is up - 'intentd status' responds"
    } else {
        # Timing out is three different things. The sitter reports a daemon
        # that starts and dies on the service log, so this run's slice says
        # whether it has already given up (service stopped for good), is still
        # respawning a daemon that cannot start (it only gives up minutes
        # after this 60s wait, so this is the common case), or nothing crashed
        # at all and the first download is merely slow. Only the last of the
        # three is a warning. The substrings matched below are the sitter's
        # own log lines - a detection contract with
        # crates/intentd-sitter/src/supervisor.rs and install.sh, pinned by
        # the install_log_contract_* tests in
        # crates/intentd-sitter/tests/supervisor_e2e.rs. Change only in
        # lockstep.
        # An explicit on/off auto-resume answer is applied only after this
        # check passes, so a failure here drops it - say so instead of
        # dropping it silently.
        $autoResumeNote = ''
        if (@('on', 'off') -contains $autoResume) {
            $autoResumeNote = "`nYour auto-resume choice ('$autoResume') was not applied; once the daemon is up, apply it with:`n  intentd settings agents.resumeInterruptedOnStart $autoResume"
        }
        # This run's slice of the service log, bounded to 40 lines. Empty when
        # there is nothing to read; a file shorter than the noted offset was
        # rotated or replaced, so read it whole. The offset is a byte count
        # taken before decoding, so the first line can start mid-word; the
        # lines that matter come after it. The log is append-only and never
        # rotated, so seek with [long] offsets (it can exceed 2 GiB) and read
        # at most a 256 KiB tail instead of loading the whole file — only the
        # last 40 lines are quoted anyway. A failed read is reported as its
        # own warning below, never passed off as an empty log.
        $logOut = ''
        $logReadFailed = $false
        try {
            if (Test-Path $logFile) {
                $stream = [System.IO.File]::Open($logFile, [System.IO.FileMode]::Open,
                    [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
                try {
                    $length = [long]$stream.Length
                    $sliceFrom = if ($length -lt [long]$logOffset) { [long]0 } else { [long]$logOffset }
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
                        $logOut = $lines -join "`n"
                    }
                } finally {
                    $stream.Dispose()
                }
            }
        } catch {
            $logOut = ''
            $logReadFailed = $true
        }
        $restartHint = "Start-ScheduledTask -TaskName $taskName"
        if ($logOut.Contains('times in a row without ever staying up')) {
            Write-Host "install.ps1: the service's output for this start (from ${logFile}):"
            foreach ($line in ($logOut -split "`n")) { Write-Host "  | $line" }
            throw ("install.ps1: the daemon could not start and the sitter has given up - the service is stopped.`nFix the cause reported above, then start it again with:`n  $restartHint$autoResumeNote")
        } elseif ($logOut.Contains('exited unexpectedly') -or $logOut.Contains('failed to spawn') -or $logOut.Contains('failed waiting on intentd')) {
            Write-Host "install.ps1: the service's output for this start (from ${logFile}):"
            foreach ($line in ($logOut -split "`n")) { Write-Host "  | $line" }
            throw ("install.ps1: the daemon is failing to start and the sitter is still respawning it; it gives up in a few minutes and leaves the service stopped.`nFix the cause reported above, then start it again with:`n  $restartHint$autoResumeNote")
        } elseif ($logReadFailed) {
            # A read failure is not an empty log: without the slice the three
            # cases above are indistinguishable, so say so instead of guessing
            # "still downloading".
            Write-Warning "install.ps1: daemon did not respond within 60s and the service log could not be read ($logFile) - cannot tell whether it is still downloading or crashing; check later with: intentd status"
        } else {
            Write-Warning "install.ps1: daemon did not respond within 60s - it may still be downloading; check later with: intentd status"
        }
    }
    # 'auto' is the daemon default — nothing to write. A failure is a warning,
    # not a fatal install error: the setting can be changed later with the
    # same command.
    if (@('on', 'off') -contains $autoResume) {
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

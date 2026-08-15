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
# $env:INTENTD_AUTO_RESUME = 'auto'|'on'|'off' forces an answer.
#
# $env:INTENTD_SERVICE_NAME overrides the task name (testing).
param(
    [switch]$Service,
    [switch]$NoService
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
    # Auto-resume choice: env var beats the prompt; 'auto' — or a
    # non-interactive run — is the daemon default and writes nothing. Applied
    # via `intentd settings` after the wait-for-daemon loop below.
    $autoResume = ''
    if ($env:INTENTD_AUTO_RESUME) {
        $autoResume = $env:INTENTD_AUTO_RESUME.ToLowerInvariant()
        if (@('auto', 'on', 'off') -notcontains $autoResume) {
            throw "install.ps1: invalid INTENTD_AUTO_RESUME value '$env:INTENTD_AUTO_RESUME' (expected auto, on, or off)"
        }
    } elseif ([Environment]::UserInteractive -and -not [Console]::IsInputRedirected) {
        $reply = (Read-Host 'Auto-resume interrupted agents when the service starts? [auto/on/off] (default auto)').Trim().ToLowerInvariant()
        $autoResume = if (@('on', 'off') -contains $reply) { $reply } else { 'auto' }
    } else {
        $autoResume = 'auto'
    }
    # Per-user Scheduled Task at logon: no admin rights needed, and -Force
    # makes re-runs update the existing task instead of duplicating it.
    $taskName = if ($env:INTENTD_SERVICE_NAME) { $env:INTENTD_SERVICE_NAME } else { 'intentd' }
    # Carry a custom data dir into the task so it serves the same data dir the
    # install-time CLI used. A task action cannot set environment variables, so
    # wrap through cmd (the quotes survive & and spaces in the path).
    $action = if ($env:INTENTD_DATA_DIR) {
        New-ScheduledTaskAction -Execute $env:ComSpec `
            -Argument ('/d /c set "INTENTD_DATA_DIR=' + $env:INTENTD_DATA_DIR + '" && "' + $dest + '" serve')
    } else {
        New-ScheduledTaskAction -Execute $dest -Argument 'serve'
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
        Write-Warning "install.ps1: daemon did not respond within 60s - it may still be downloading; check later with: intentd status"
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
} else {
    if ($serviceMode -eq 'skip') {
        Write-Host 'install.ps1: skipping service setup (non-interactive session). To set it up, re-run with $env:INTENTD_INSTALL_SERVICE = ''1'''
    }
    Write-Host ''
    Write-Host 'Next steps:'
    Write-Host '  intentd serve   # start the daemon (downloads the real daemon on first run)'
}

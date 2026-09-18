# Pester tests for the Windows-only halves of scripts/install.ps1 that
# crates/intentd-sitter/tests/install_ps1_owner.rs cannot reach from a Unix
# host: the Scheduled Task registration and the startup wait that polls the
# task's log. Like that suite, these run the SHIPPED code: the blocks between
# install.ps1's `>>> BEGIN <name>` / `<<< END <name>` markers are extracted
# verbatim and dot-sourced into the test scope, so the assertions bind to the
# real variables the installer sets ($verdict, $waited, $logOffset, ...).
#
# The startup-wait block is plain .NET plus `& $dest status`, so it runs under
# pwsh on any platform (the fake daemon is a .cmd on Windows, a sh script
# elsewhere); Start-Sleep is mocked so a 300s deadline resolves instantly and
# the poll count stands in for elapsed time. The registration block needs the
# ScheduledTasks module and is skipped off Windows; the four cmdlets that
# touch the Task Scheduler service are mocked, the New-ScheduledTask* builders
# run for real (client-side CIM objects), so an invalid parameter combination
# fails here rather than on a user's machine.
#
# Run: Invoke-Pester -Path scripts/install.ps1.Tests.ps1 -Output Detailed
# CI runs this on a Windows runner (see .github/workflows/ci.yml, install-ps1).

BeforeAll {
    $script:installPs1 = Join-Path $PSScriptRoot 'install.ps1'
    $script:onWindows = $env:OS -eq 'Windows_NT'

    # One marked region of install.ps1 as a scriptblock, mirroring ps_region in
    # install_ps1_owner.rs: the lines between the markers, minus the `#` that
    # opens the end marker's comment line.
    function Get-InstallRegion {
        param([string]$Name)
        $script = Get-Content -Raw -LiteralPath $script:installPs1
        $begin = ">>> BEGIN $Name"
        $end = "<<< END $Name"
        $start = $script.IndexOf($begin)
        if ($start -lt 0) { throw "install.ps1 must mark the $Name region with '$begin'" }
        $afterMarker = $script.IndexOf("`n", $start) + 1
        $stop = $script.IndexOf($end, $afterMarker)
        if ($stop -lt 0) { throw "install.ps1 must close the $Name region with '$end'" }
        $region = $script.Substring($afterMarker, $stop - $afterMarker).TrimEnd()
        if ($region.EndsWith('#')) { $region = $region.Substring(0, $region.Length - 1).TrimEnd() }
        return [scriptblock]::Create($region)
    }

    # A stand-in for the installed intentd.exe: `status` exits 0 only once
    # <dir>/up.marker exists, so a test scripts the moment the daemon "answers".
    function New-FakeDaemon {
        param([string]$Dir)
        New-Item -ItemType Directory -Force -Path $Dir | Out-Null
        if ($script:onWindows) {
            $path = Join-Path $Dir 'intentd.cmd'
            Set-Content -LiteralPath $path -Encoding Ascii -Value @(
                '@echo off',
                'if exist "%~dp0up.marker" exit /b 0',
                'exit /b 1'
            )
        } else {
            $path = Join-Path $Dir 'intentd'
            Set-Content -LiteralPath $path -Encoding Ascii -Value @(
                '#!/bin/sh',
                '[ -e "$(dirname "$0")/up.marker" ] && exit 0',
                'exit 1'
            )
            & chmod +x $path
        }
        return $path
    }

    function Set-DaemonUp {
        param([string]$Dest)
        New-Item -ItemType File -Force -Path (Join-Path (Split-Path -Parent $Dest) 'up.marker') | Out-Null
    }
}

Describe 'install.ps1 service-startup-wait' {
    BeforeAll {
        $script:waitRegion = Get-InstallRegion 'service-startup-wait'
    }

    BeforeEach {
        # Mocked Start-Sleep: no wall-clock wait, and the poll count is the
        # clock. $script:onSleep lets a test act "after N polls" (bring the
        # daemon up, append crash lines) exactly as the loop would observe it.
        $script:sleeps = 0
        $script:onSleep = $null
        Mock Start-Sleep {
            $script:sleeps++
            if ($script:onSleep) { & $script:onSleep $script:sleeps }
        }
        Mock Write-Host {}
        Mock Write-Warning {}
        # A fresh dir per test: the up.marker must never leak into the next one.
        $testDir = Join-Path $TestDrive ([guid]::NewGuid().ToString('n'))
        $dest = New-FakeDaemon -Dir $testDir
        $logFile = Join-Path $testDir 'intentd.err.log'
        $logOffset = 0
        $taskName = 'intentd-test'
        $autoResume = 'auto'
        $err = $null
    }

    It 'returns up the moment the daemon answers, without sleeping' {
        Set-DaemonUp $dest
        . $script:waitRegion
        $verdict | Should -Be 'up'
        $waited | Should -Be 0
        Should -Invoke Start-Sleep -Times 0 -Exactly
        Should -Invoke Write-Host -Times 1 -Exactly -ParameterFilter { $Object -like '*daemon is up*' }
        Should -Invoke Write-Warning -Times 0 -Exactly
    }

    It 'keeps polling every 2s until the daemon answers' {
        $script:onSleep = { param($n) if ($n -eq 3) { Set-DaemonUp $dest } }
        . $script:waitRegion
        $verdict | Should -Be 'up'
        $waited | Should -Be 6
        Should -Invoke Start-Sleep -Times 3 -Exactly -ParameterFilter { $Seconds -eq 2 }
    }

    It 'treats crash evidence that resolves within the settle window as a working install' {
        Set-Content -LiteralPath $logFile -Value 'intentd exited unexpectedly (exit 1), respawning'
        $script:onSleep = { param($n) if ($n -eq 4) { Set-DaemonUp $dest } }
        . $script:waitRegion
        $verdict | Should -Be 'up'
        $waited | Should -Be 8
        Should -Invoke Write-Warning -Times 0 -Exactly
    }

    It 'reports crashing once crash evidence outlives the 10s settle window, quoting the log' {
        Set-Content -LiteralPath $logFile -Value 'intentd exited unexpectedly (exit 1), respawning'
        try { . $script:waitRegion } catch { $err = $_ }
        $err | Should -Not -BeNullOrEmpty
        $err.Exception.Message | Should -BeLike '*failing to start and the sitter is still respawning*'
        $err.Exception.Message | Should -BeLike '*Start-ScheduledTask -TaskName intentd-test*'
        $verdict | Should -Be 'crashing'
        $waited | Should -Be 10
        Should -Invoke Start-Sleep -Times 5 -Exactly
        Should -Invoke Write-Host -Times 1 -Exactly -ParameterFilter { $Object -like '  | intentd exited unexpectedly*' }
    }

    It 'stops at once when the sitter reports it has given up' {
        Set-Content -LiteralPath $logFile -Value 'intentd exited 5 times in a row without ever staying up; giving up'
        try { . $script:waitRegion } catch { $err = $_ }
        $err.Exception.Message | Should -BeLike '*could not start and the sitter has given up*'
        $verdict | Should -Be 'gaveup'
        $waited | Should -Be 0
        Should -Invoke Start-Sleep -Times 0 -Exactly
    }

    It 'crash evidence appearing mid-wait starts the settle window from that poll' {
        $script:onSleep = { param($n) if ($n -eq 20) { Set-Content -LiteralPath $logFile -Value 'sitter: failed to spawn intentd' } }
        try { . $script:waitRegion } catch { $err = $_ }
        $verdict | Should -Be 'crashing'
        # Seen at 40s (poll 20), settled 10s later.
        $waited | Should -Be 50
    }

    It 'gives up undecided at the 300s deadline when nothing failed, after one progress line' {
        try { . $script:waitRegion } catch { $err = $_ }
        $err | Should -BeNullOrEmpty
        $verdict | Should -Be 'undecided'
        $waited | Should -Be 300
        Should -Invoke Start-Sleep -Times 150 -Exactly
        Should -Invoke Write-Host -Times 1 -Exactly -ParameterFilter { $Object -like '*still waiting (60s)*' }
        Should -Invoke Write-Warning -Times 1 -Exactly -ParameterFilter {
            $Message -like "*has not responded in 300s and nothing in this run's service log reports a failure*" -and
            $Message -like '*Start-ScheduledTask -TaskName intentd-test*' -and
            $Message -notlike '*was not applied*'
        }
    }

    It "ignores a previous run's crash lines that lie before the log offset" {
        Set-Content -LiteralPath $logFile -Value 'intentd exited unexpectedly (exit 1), respawning'
        $logOffset = (Get-Item -LiteralPath $logFile).Length
        try { . $script:waitRegion } catch { $err = $_ }
        $err | Should -BeNullOrEmpty
        $verdict | Should -Be 'undecided'
        $crashText | Should -Be ''
    }

    It 'quotes at most the last 40 lines of this run''s log slice' {
        $lines = 1..60 | ForEach-Object { "line $_" }
        $lines += 'intentd exited unexpectedly (exit 1), respawning'
        Set-Content -LiteralPath $logFile -Value $lines
        try { . $script:waitRegion } catch { $err = $_ }
        $verdict | Should -Be 'crashing'
        @($crashText -split "`n").Count | Should -Be 40
        ($crashText -split "`n")[0] | Should -Be 'line 22'
    }

    It 'says so when the log cannot be read instead of guessing "still downloading"' {
        # A directory where the log file should be: Test-Path is true, the open fails.
        New-Item -ItemType Directory -Force -Path $logFile | Out-Null
        try { . $script:waitRegion } catch { $err = $_ }
        $err | Should -BeNullOrEmpty
        $verdict | Should -Be 'undecided'
        $logReadFailed | Should -BeTrue
        Should -Invoke Write-Warning -Times 1 -Exactly -ParameterFilter { $Message -like '*service log could not be read*' }
    }

    It 'names an unapplied explicit auto-resume choice in the undecided warning' {
        $autoResume = 'on'
        try { . $script:waitRegion } catch { $err = $_ }
        Should -Invoke Write-Warning -Times 1 -Exactly -ParameterFilter {
            $Message -like "*auto-resume choice ('on') was not applied*" -and
            $Message -like '*intentd settings agents.resumeInterruptedOnStart on*'
        }
    }
}

Describe 'install.ps1 service-task-register' -Skip:($env:OS -ne 'Windows_NT') {
    BeforeAll {
        Import-Module ScheduledTasks
        $script:registerRegion = Get-InstallRegion 'service-task-register'
        $script:envNames = @('INTENTD_SERVICE_NAME', 'INTENTD_DATA_DIR', 'LOCALAPPDATA')
    }

    BeforeEach {
        $script:savedEnv = @{}
        foreach ($n in $script:envNames) { $script:savedEnv[$n] = [Environment]::GetEnvironmentVariable($n) }
        $env:LOCALAPPDATA = Join-Path $TestDrive ([guid]::NewGuid().ToString('n'))
        Remove-Item Env:INTENTD_SERVICE_NAME, Env:INTENTD_DATA_DIR -ErrorAction SilentlyContinue
        $dest = 'C:\Users\u\AppData\Local\intentd\bin\intentd.exe'
        $expectedLog = Join-Path $env:LOCALAPPDATA 'intentd\intentd.err.log'
        # Only the cmdlets that reach the Task Scheduler service are mocked;
        # Register's arguments are captured so each one can be asserted on.
        $script:registered = [System.Collections.Generic.List[hashtable]]::new()
        Mock Get-ScheduledTask { $null }
        Mock Stop-ScheduledTask {}
        Mock Register-ScheduledTask {
            $script:registered.Add(@{
                TaskName = $TaskName; Action = $Action; Trigger = $Trigger; Principal = $Principal
                Settings = $Settings; Description = $Description; Force = $Force
            })
        }
        Mock Start-ScheduledTask {}
    }

    AfterEach {
        foreach ($n in $script:envNames) { [Environment]::SetEnvironmentVariable($n, $script:savedEnv[$n]) }
    }

    It 'registers a logon-triggered S4U task that runs the binary through cmd with stderr appended to the log' {
        . $script:registerRegion
        $taskName | Should -Be 'intentd'
        $logFile | Should -Be $expectedLog
        Split-Path -Parent $logFile | Should -Exist
        $logOffset | Should -Be 0
        $script:registered.Count | Should -Be 1
        $r = $script:registered[0]
        $r.TaskName | Should -Be 'intentd'
        $r.Force.IsPresent | Should -BeTrue
        $r.Description | Should -Be 'Intent backend daemon (intentd)'
        $r.Action[0].Execute | Should -Be $env:ComSpec
        $r.Action[0].Arguments | Should -Be ('/d /c 2>>"' + $expectedLog + '" "' + $dest + '" serve')
        $r.Trigger[0].CimClass.CimClassName | Should -Be 'MSFT_TaskLogonTrigger'
        $r.Trigger[0].UserId | Should -Be "$env:USERDOMAIN\$env:USERNAME"
        $r.Principal.UserId | Should -Be "$env:USERDOMAIN\$env:USERNAME"
        "$($r.Principal.LogonType)" | Should -Be 'S4U'
        $r.Settings.ExecutionTimeLimit | Should -Be 'PT0S'
        $r.Settings.DisallowStartIfOnBatteries | Should -BeFalse
        $r.Settings.StopIfGoingOnBatteries | Should -BeFalse
        $r.Settings.RestartCount | Should -Be 3
        $r.Settings.RestartInterval | Should -Be 'PT1M'
        $r.Settings.StartWhenAvailable | Should -BeTrue
        Should -Invoke Stop-ScheduledTask -Times 0 -Exactly
        Should -Invoke Start-ScheduledTask -Times 1 -Exactly -ParameterFilter { $TaskName -eq 'intentd' }
    }

    It 'carries a custom data dir and service name into the task action' {
        $env:INTENTD_DATA_DIR = 'D:\intent data\dir'
        $env:INTENTD_SERVICE_NAME = 'intentd-test'
        . $script:registerRegion
        $taskName | Should -Be 'intentd-test'
        $script:registered[0].TaskName | Should -Be 'intentd-test'
        $script:registered[0].Action[0].Arguments | Should -Be ('/d /c set "INTENTD_DATA_DIR=D:\intent data\dir" && "' + $dest + '" serve 2>>"' + $expectedLog + '"')
        Should -Invoke Start-ScheduledTask -Times 1 -Exactly -ParameterFilter { $TaskName -eq 'intentd-test' }
    }

    It 'is idempotent: a re-run stops the running task, re-registers with -Force and notes the log offset' {
        . $script:registerRegion
        Should -Invoke Stop-ScheduledTask -Times 0 -Exactly
        # Second run: the task now exists and the log carries a previous run.
        Mock Get-ScheduledTask { [pscustomobject]@{ TaskName = $TaskName } }
        Set-Content -LiteralPath $logFile -Value 'previous run' -NoNewline
        . $script:registerRegion
        Should -Invoke Get-ScheduledTask -Times 2 -Exactly -ParameterFilter { $TaskName -eq 'intentd' }
        Should -Invoke Stop-ScheduledTask -Times 1 -Exactly -ParameterFilter { $TaskName -eq 'intentd' }
        Should -Invoke Start-ScheduledTask -Times 2 -Exactly
        $script:registered.Count | Should -Be 2
        $script:registered[1].Force.IsPresent | Should -BeTrue
        $logOffset | Should -Be (Get-Item -LiteralPath $logFile).Length
        $logOffset | Should -BeGreaterThan 0
    }
}

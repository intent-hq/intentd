# Artifact-only; future native setup selection required. No behavioral fixture command.
param([Parameter(Mandatory)][string]$Node,[Parameter(Mandatory)][string]$OwnedRoot)
$ErrorActionPreference='Stop'
if (-not $IsWindows) { throw 'native_windows_required' }
if (Test-Path -LiteralPath $OwnedRoot) { throw 'exclusive_root_exists' }
$null=New-Item -ItemType Directory -Path $OwnedRoot
if(-not ('OwnedSetup' -as [type])){throw 'owned_type_not_bound'}
$rows=[System.Collections.Generic.List[object]]::new()
function Check([bool]$Condition,[string]$Code) { if(-not $Condition){throw $Code} }
function Run-Synthetic([string]$Name,[string]$Mode,[int]$Deadline,[int]$Cap,[bool]$Reject,[string[]]$Extra=@()) {
 $marker=Join-Path $OwnedRoot ($Name+'.entry')
 $arguments=@((Join-Path $PSScriptRoot 'synthetic-child.mjs'),$Mode,$marker)+$Extra
 $log=Join-Path $OwnedRoot ($Name+'.log')
 $r=[OwnedSetup]::Run($Node,[string[]]$arguments,$OwnedRoot,$log,$Deadline,$Cap,$Reject)
 $rows.Add([pscustomobject]@{name=$Name;receipt=$r})
 $rows | ConvertTo-Json -Depth 12 | Set-Content -Encoding utf8NoBOM (Join-Path $OwnedRoot 'native-receipts.json')
 Check $r.rootSignalled 'root_disposal_unconfirmed'
 if($Reject){Check (-not $r.assigned -and -not $r.resumed -and $r.reason -eq 'assignment_failed' -and -not(Test-Path -LiteralPath $marker)) 'assignment_failure_control'}
 else {Check ($r.assigned -and $r.resumed -and $r.activeZero -and $r.readerDone -and -not $r.readerError) 'ownership_or_reader_incomplete'}
 if(Test-Path -LiteralPath $log){Check ((Get-Item -LiteralPath $log).Length -le $Cap) 'disk_cap_exceeded'}
 return $r
}
$r=Run-Synthetic 'assigned-before-entry' 'entry' 5000 8192 $false
Check ($r.reason -eq 'completed' -and $r.originalExit -eq 0 -and (Test-Path (Join-Path $OwnedRoot 'assigned-before-entry.entry'))) 'entry_control'
$r=Run-Synthetic 'assignment-refusal-unresumed' 'entry' 5000 8192 $true
$r=Run-Synthetic 'original-nonzero' 'nonzero' 5000 8192 $false
Check ($r.reason -eq 'completed' -and $r.originalExit -eq 7) 'original_exit_changed'
$r=Run-Synthetic 'argument-quoting' 'arguments' 5000 8192 $false @('space value','quote"value','tail\','&()%^!')
Check ($r.reason -eq 'completed' -and $r.originalExit -eq 0) 'argument_control'
$r=Run-Synthetic 'deadline-disposal' 'wait' 500 8192 $false
Check ($r.reason -eq 'deadline' -and $r.interventions.Contains('terminate_owned_job')) 'deadline_control'
$r=Run-Synthetic 'grandchild-disposal' 'chain' 2000 8192 $false
Check ($r.reason -eq 'deadline' -and $r.maximumActive -ge 3 -and $r.interventions.Contains('terminate_owned_job')) 'grandchild_control'
$r=Run-Synthetic 'retired-parent-disposal' 'retired-parent' 1500 8192 $false
Check ($r.reason -eq 'deadline' -and $r.originalExit -eq 0 -and $r.maximumActive -ge 2) 'retired_parent_control'
$r=Run-Synthetic 'stream-cap' 'flood' 5000 8192 $false
Check ($r.reason -eq 'output_cap' -and $r.overflow -and $r.written -le 8192 -and $r.read -le 12288) 'stream_cap_control'
# The accepted 46-control payload includes actual fd3 inheritance, closed descriptor,
# short/throwing diagnostic writer and strict missing/truncated/secret schema controls.
$log=Join-Path $OwnedRoot 'controls.tap'
$r=[OwnedSetup]::Run($Node,[string[]]@('--test','--test-concurrency=1','--test-reporter=tap',(Join-Path $PSScriptRoot 'payload/diagnostic/controls.mjs')),$OwnedRoot,$log,120000,8388608,$false)
$rows.Add([pscustomobject]@{name='accepted46-native';receipt=$r})
$rows | ConvertTo-Json -Depth 12 | Set-Content -Encoding utf8NoBOM (Join-Path $OwnedRoot 'native-receipts.json')
Check ($r.reason -eq 'completed' -and $r.originalExit -eq 0 -and $r.activeZero -and $r.rootSignalled -and $r.readerDone -and -not $r.readerError) 'native46_outcome'
$expected=Get-Content -Raw (Join-Path $PSScriptRoot 'payload/diagnostic/expected-tests.json') | ConvertFrom-Json
$lines=Get-Content -LiteralPath $log
$actual=@($lines | Where-Object {$_ -match '^ok [0-9]+ - '} | ForEach-Object {$_ -replace '^ok [0-9]+ - ',''})
Check ($actual.Count -eq 46 -and ($actual | ConvertTo-Json -Compress) -eq ($expected | ConvertTo-Json -Compress)) 'native46_identity'
Check (-not($lines | Where-Object {$_ -match '^not ok|^ok .*# (SKIP|TODO)'})) 'native46_failure_or_skip'
foreach($line in @('1..46','# tests 46','# pass 46','# fail 0','# cancelled 0','# skipped 0','# todo 0')){Check ($lines -contains $line) 'native46_summary'}
[pscustomobject]@{stage='native-setup';outcome='PASS';syntheticCases=8;exactControls=46;behavioralInvocations=0;qualification='Native setup only; no real adapter/Pi/model or historical Windows failure observation.'} | ConvertTo-Json | Set-Content -Encoding utf8NoBOM (Join-Path $OwnedRoot 'native-result.json')

# AUTHORED ONLY. No process, import of adapter, native type compilation, install, or workflow run.
$ErrorActionPreference='Stop'
$source=Get-Content -Raw (Join-Path $PSScriptRoot 'setup-only.ps1')
$first=$source.IndexOf('# BEGIN FIXED SETUP FAILURE CAPTURE')
$last=$source.IndexOf('# END FIXED SETUP FAILURE CAPTURE')
if($first -lt 0 -or $last -le $first){throw 'helper_bounds'}
$helper=$source.Substring($first,$last-$first)+'# END FIXED SETUP FAILURE CAPTURE'+"`n"
if($helper -cne (Get-Content -Raw (Join-Path $PSScriptRoot 'capture-functions.ps1'))){throw 'helper_identity'}
Invoke-Expression $helper
if('OwnedSetup' -as [type]){throw 'requires_fresh_type_free_process'}
$results=@()
function Check([string]$Name,[scriptblock]$Body){& $Body;$script:results+=@{name=$Name;outcome='PASS'}}
function Assert($Value){if(-not $Value){throw 'control_assertion'}}
function Record($Phase='original',$Stage='native-compile',$Category='InvalidOperation',$Hr=-2146233087,$Ownership='not-initialized') {New-SetupFailureRecord $Phase $Stage $Category $Hr $Ownership}
Check 'original-fixed-fields' { $r=Record;Assert ($r.phase -eq 'original' -and $r.stage -eq 'native-compile' -and $r.hresult -eq -2146233087 -and $r.exitCode -eq 1 -and $r.outcome -eq 'FAILED') }
Check 'provenance-distinct-from-compile' {Assert ((Record -Stage 'tool-provenance').stage -ne (Record).stage)}
Check 'unknown-stage-redacted' {Assert ((Record -Stage 'secret-value').stage -eq 'unknown')}
Check 'unknown-category-redacted' {Assert ((Record -Category 'secret-value').category -eq 'Other')}
Check 'unknown-ownership-fails-closed' {Assert ((Record -Ownership 'invented').ownership -eq 'unresolved')}
Check 'phase-cannot-inject' {Assert ((Record -Phase 'secret-value').phase -eq 'finalizer')}
Check 'hresult-minimum' {Assert ((Record -Hr ([int]::MinValue)).hresult -eq [int]::MinValue)}
Check 'hresult-maximum' {Assert ((Record -Hr ([int]::MaxValue)).hresult -eq [int]::MaxValue)}
Check 'type-absent-no-invocation' {$script:ownedInvocationStarted=$false;Assert ((Get-SetupOwnershipState) -eq 'not-initialized')}
Check 'type-absent-after-invocation-unresolved' {$script:ownedInvocationStarted=$true;Assert ((Get-SetupOwnershipState) -eq 'unresolved')}
Check 'writer-strips-extra-properties' {
 $old=[Console]::Error;$writer=[IO.StringWriter]::new()
 try {[Console]::SetError($writer);$r=Record;$r.extra='SECRET_SENTINEL';Write-SetupFailureRecord $r} finally {[Console]::SetError($old)}
 $line=$writer.ToString();Assert (-not $line.Contains('SECRET_SENTINEL'));Assert ($line.Length -le 1026)
 $parsed=($line -replace '^SETUP_FAILURE_V1 ','')|ConvertFrom-Json
 Assert (@($parsed.PSObject.Properties).Count -eq 9)
}
Check 'writer-exception-does-not-escape' {
 $old=[Console]::Error;$writer=[IO.StringWriter]::new();$writer.Dispose()
 $outcome='FAILED';$code=-2146233087;$r=Record -Hr $code
 try {
  [Console]::SetError($writer)
  $didThrow=$false
  try {[Console]::Error.WriteLine('fault-probe')} catch {$didThrow=$true}
  Assert $didThrow
  Write-SetupFailureRecord $r
  Assert ($outcome -eq 'FAILED' -and $code -eq -2146233087 -and $r.exitCode -eq 1 -and $r.hresult -eq $code)
 } finally {[Console]::SetError($old)}
}
Check 'original-record-precedes-finalizer-record' {
 $old=[Console]::Error;$writer=[IO.StringWriter]::new()
 try {
  [Console]::SetError($writer);$stage='provenance';$diagnosticStage='native-compile';$script:ownedInvocationStarted=$false
  # Exact original catch body extracted from the proposed script; no setup or child code.
  $start=$source.IndexOf("} catch {`n `$code=`$_.Exception.HResult; `$outcome='FAILED'")
  $end=$source.IndexOf("`n}`nfinally {",$start)
  if($start -lt 0 -or $end -le $start){throw 'catch_bounds'}
  $body=$source.Substring($start+9,$end-$start-9)
  try {throw 'SECRET_SENTINEL'} catch {Invoke-Expression $body}
  Write-SetupFailureRecord (New-SetupFailureRecord 'finalizer' $diagnosticStage 'ResourceUnavailable' $code (Get-SetupOwnershipState))
 } finally {[Console]::SetError($old)}
 $lines=@($writer.ToString().TrimEnd() -split '\r?\n');Assert ($lines.Count -eq 2)
 $a=($lines[0] -replace '^SETUP_FAILURE_V1 ','')|ConvertFrom-Json;$b=($lines[1] -replace '^SETUP_FAILURE_V1 ','')|ConvertFrom-Json
 Assert ($a.phase -eq 'original' -and $b.phase -eq 'finalizer' -and $a.hresult -eq $b.hresult -and $a.ownership -eq 'not-initialized');Assert (-not $writer.ToString().Contains('SECRET_SENTINEL'))
}
Check 'summary-stage-compatible-with-original-policy' {
 $original=Get-Content -Raw (Join-Path $PSScriptRoot 'original-setup-only.ps1')
 $coarsePattern='(?m)\$stage=''([^'']+)'''
 $before=@([regex]::Matches($original,$coarsePattern)|ForEach-Object {$_.Groups[1].Value})
 $after=@([regex]::Matches($source,$coarsePattern)|ForEach-Object {$_.Groups[1].Value})
 Assert (($before -join '|') -ceq ($after -join '|'))
 Assert (($after -join '|') -ceq 'provenance|native-controls|dependency-install|dependency-acceptance|final-packet-guards|setup-complete')
 Assert ($source.Contains('$summary=@{stage=$stage;outcome=$outcome;'))
 $early=@('payload-verify','packet-inventory','tool-provenance','native-compile','compiler-provenance','latch-controls','node-version','npm-version')
 $stage=$null;$diagnosticStage=$null
 # Inspect only exact string-literal stage assignments, in original order; no setup code.
 foreach($match in [regex]::Matches($source,'\$(stage|diagnosticStage)=''([a-z-]+)''')){
  if($match.Groups[1].Value -eq 'stage'){$stage=$match.Groups[2].Value}
  else {
   $diagnosticStage=$match.Groups[2].Value
   if($diagnosticStage -in $early){Assert ($stage -eq 'provenance')}
   else {Assert ($stage -ceq $diagnosticStage)}
  }
 }
}
$results|ConvertTo-Json -Depth 4

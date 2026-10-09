param([hashtable]$DiagnosticState=@{})
function Set-ControlDiagnostic([string]$Stage,[string]$Case='', [string]$Assertion='', [string]$Completed='', [string]$Outcome='') {
 # Diagnostic bookkeeping cannot replace an original assertion/throw.
 try {
  if($Stage){$DiagnosticState.stage=$Stage;$DiagnosticState.assertion=$null}
  if($Case){$DiagnosticState.case=$Case}
  if($Assertion){$DiagnosticState.assertion=$Assertion}
  if($Completed){$DiagnosticState.completed=@($DiagnosticState.completed)+@($Completed)}
  if($Outcome){$DiagnosticState.outcome=$Outcome}
 } catch {}
}
try {
Set-ControlDiagnostic -Stage 'suite-initialization' -Outcome 'entered'
# Artifact-only authored controls. Not executed. Exact guard functions; synthetic read-only providers.
$ErrorActionPreference='Stop'
$original=Get-Content -Raw (Join-Path $PSScriptRoot 'original-guard.ps1')
$marked=Get-Content -Raw (Join-Path $PSScriptRoot 'guard-functions.ps1')
Invoke-Expression ($original.Replace('function Guard {','function OriginalGuard {'))
Invoke-Expression $marked
function Assert($x,[string]$DiagnosticId){Set-ControlDiagnostic -Stage 'assertion' -Assertion $DiagnosticId;if(-not $x){throw 'control_assertion'}}
function Get-Content {param([switch]$Raw,[Parameter(Position=0)][string]$Path);if($script:fixture.manifestError){throw 'PRIVATE_SENTINEL'};return $script:fixture.json}
function Get-ChildItem {param([string]$LiteralPath,[switch]$File,[switch]$Directory);if($script:fixture.inventoryError){throw 'PRIVATE_SENTINEL'};if($Directory){return $script:fixture.directories};return @($script:fixture.files|ForEach-Object {[pscustomobject]@{Name=$_}})}
function Hash([string]$Path){$script:hashCalls++;if($script:fixture.hashError){throw 'PRIVATE_SENTINEL'};return $script:fixture.observed}
$cases=@('equal-pass','missing-known','extra-unknown','case-mismatch','directory-present','digest-mismatch','file-read-error','manifest-read-error','inventory-read-error','invalid-name','extra-truncated')
$results=@()
foreach($name in $cases){
 Set-ControlDiagnostic -Stage 'case-setup' -Case $name
 $manifest=[ordered]@{'controls.ps1'=('a'*64)}
 $script:fixture=@{json='';files=@('controls.ps1','payload-manifest.json');directories=@();observed=('a'*64);hashError=$false;manifestError=$false;inventoryError=$false}
 switch($name){
  'missing-known' {$script:fixture.files=@('payload-manifest.json')}
  'extra-unknown' {$script:fixture.files+=@('PRIVATE_SENTINEL')}
  'case-mismatch' {$script:fixture.files=@('Controls.ps1','payload-manifest.json')}
  'directory-present' {$script:fixture.directories=@([pscustomobject]@{Name='PRIVATE_SENTINEL'})}
  'digest-mismatch' {$script:fixture.observed=('b'*64)}
  'file-read-error' {$script:fixture.hashError=$true}
  'manifest-read-error' {$script:fixture.manifestError=$true}
  'inventory-read-error' {$script:fixture.inventoryError=$true}
  'invalid-name' {$manifest=[ordered]@{'PRIVATE/SENTINEL'=('a'*64)};$script:fixture.files=@('PRIVATE/SENTINEL','payload-manifest.json')}
  'extra-truncated' {$script:fixture.files+=@(1..17|ForEach-Object {'PRIVATE_SENTINEL_'+$_})}
 }
 Set-ControlDiagnostic -Stage 'fixture-json'
 $script:fixture.json=$manifest|ConvertTo-Json -Compress
 $script:hashCalls=0;$oldOk=$true;$oldError=$null
 Set-ControlDiagnostic -Stage 'original-call'
 try {OriginalGuard} catch {$oldOk=$false;$oldError=$_.Exception.Message}
 $oldCalls=$script:hashCalls
 $script:hashCalls=0;$newOk=$true;$newError=$null
 Set-ControlDiagnostic -Stage 'marked-call'
 try {Guard} catch {$newOk=$false;$newError=$_.Exception.Message}
 Assert -DiagnosticId 'A01' ($oldOk -eq $newOk -and $oldCalls -eq $script:hashCalls -and $oldError -ceq $newError)
 Assert -DiagnosticId 'A02' ($newOk -eq ($name -eq 'equal-pass'))
 $g=$script:guardEvidence
 Assert -DiagnosticId 'A03' ($g.schema -eq 'payload-guard-v1')
 Set-ControlDiagnostic -Stage 'evidence-json'
 $json=$g|ConvertTo-Json -Depth 5 -Compress
 Assert -DiagnosticId 'A04' ($json.Length -lt 4096 -and -not $json.Contains('PRIVATE_SENTINEL') -and -not $json.Contains('PRIVATE/SENTINEL'))
 switch($name){
  'equal-pass' {Assert -DiagnosticId 'A05' ($g.id -eq 'complete')}
  'missing-known' {Assert -DiagnosticId 'A06' ($g.id -eq 'payload-inventory' -and $g.missing.Count -eq 1 -and $g.missing[0] -ceq 'controls.ps1')}
  'extra-unknown' {Assert -DiagnosticId 'A07' ($g.extraCount -eq 1 -and $g.extraNameDigests[0] -cmatch '^[a-f0-9]{64}$')}
  'case-mismatch' {Assert -DiagnosticId 'A08' ($g.missing -ccontains 'controls.ps1');Assert -DiagnosticId 'A09' ($g.extraCount -eq 1)}
  'directory-present' {Assert -DiagnosticId 'A10' ($g.directoryCount -eq 1 -and $g.id -eq 'payload-inventory')}
  'digest-mismatch' {Assert -DiagnosticId 'A11' ($g.id -eq 'payload-hash' -and $g.file -ceq 'controls.ps1' -and $g.expectedSha256 -ceq ('a'*64) -and $g.observedSha256 -ceq ('b'*64))}
  'file-read-error' {Assert -DiagnosticId 'A12' ($g.id -eq 'payload-read' -and $null -eq $g.observedSha256);$roundtrip=$json|ConvertFrom-Json;Assert -DiagnosticId 'A13' ($null -eq $roundtrip.observedSha256)}
  'manifest-read-error' {Assert -DiagnosticId 'A14' ($g.id -eq 'manifest-read')}
  'inventory-read-error' {Assert -DiagnosticId 'A15' ($g.id -eq 'inventory-read')}
  'invalid-name' {Assert -DiagnosticId 'A16' ($g.id -eq 'payload-name' -and $null -eq $g.file);$roundtrip=$json|ConvertFrom-Json;Assert -DiagnosticId 'A17' ($null -eq $roundtrip.file)}
  'extra-truncated' {Assert -DiagnosticId 'A18' ($g.extraCount -eq 17 -and $g.extraNameDigests.Count -eq 16 -and $g.inventoryTruncated)}
 }
 $results+=@{name=$name;outcome='PASS'}
 Set-ControlDiagnostic -Stage 'case-complete' -Completed $name
}
Set-ControlDiagnostic -Stage 'case-setup' -Case 'unknown-fields-redacted'
$redacted=New-GuardEvidence 'PRIVATE_SENTINEL' 'PRIVATE_SENTINEL' 'PRIVATE_SENTINEL' 'PRIVATE_SENTINEL'
Assert -DiagnosticId 'A19' ($redacted.id -eq 'unknown' -and $null -eq $redacted.file -and $null -eq $redacted.expectedSha256 -and $null -eq $redacted.observedSha256)
$results+=@{name='unknown-fields-redacted';outcome='PASS'}
Set-ControlDiagnostic -Stage 'case-complete' -Completed 'unknown-fields-redacted'
Set-ControlDiagnostic -Stage 'case-setup' -Case 'redacted-null-json-roundtrip'
$roundtrip=($redacted|ConvertTo-Json -Depth 5 -Compress)|ConvertFrom-Json
Assert -DiagnosticId 'A20' ($null -eq $roundtrip.file -and $null -eq $roundtrip.expectedSha256 -and $null -eq $roundtrip.observedSha256)
$results+=@{name='redacted-null-json-roundtrip';outcome='PASS'}
Set-ControlDiagnostic -Stage 'case-complete' -Completed 'redacted-null-json-roundtrip'
Set-ControlDiagnostic -Stage 'case-setup' -Case 'valid-digest-json-roundtrip'
$valid=New-GuardEvidence 'payload-hash' 'controls.ps1' ('a'*64) ('b'*64)
$roundtrip=($valid|ConvertTo-Json -Depth 5 -Compress)|ConvertFrom-Json
Assert -DiagnosticId 'A21' ($roundtrip.file -ceq 'controls.ps1' -and $roundtrip.expectedSha256 -ceq ('a'*64) -and $roundtrip.observedSha256 -ceq ('b'*64))
$results+=@{name='valid-digest-json-roundtrip';outcome='PASS'}
Set-ControlDiagnostic -Stage 'case-complete' -Completed 'valid-digest-json-roundtrip'
Set-ControlDiagnostic -Stage 'result-json'
$results|ConvertTo-Json -Depth 5
Set-ControlDiagnostic -Stage 'complete' -Outcome 'returned'

} catch {
 try {$DiagnosticState.outcome='threw'} catch {}
 throw
}

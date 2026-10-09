# Artifact-only authored controls. Not executed. Exact guard functions; synthetic read-only providers.
$ErrorActionPreference='Stop'
. (Join-Path $PSScriptRoot 'original-guard-named.ps1')
. (Join-Path $PSScriptRoot 'guard-functions.ps1')
function Assert($x){if(-not $x){throw 'control_assertion'}}
function Get-Content {param([switch]$Raw,[Parameter(Position=0)][string]$Path);if($script:fixture.manifestError){throw 'PRIVATE_SENTINEL'};return $script:fixture.json}
function Get-ChildItem {param([string]$LiteralPath,[switch]$File,[switch]$Directory);if($script:fixture.inventoryError){throw 'PRIVATE_SENTINEL'};if($Directory){return $script:fixture.directories};return @($script:fixture.files|ForEach-Object {[pscustomobject]@{Name=$_}})}
function Hash([string]$Path){$script:hashCalls++;if($script:fixture.hashError){throw 'PRIVATE_SENTINEL'};return $script:fixture.observed}
$cases=@('equal-pass','missing-known','extra-unknown','case-mismatch','directory-present','digest-mismatch','file-read-error','manifest-read-error','inventory-read-error','invalid-name','extra-truncated')
$results=@()
foreach($name in $cases){
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
 $script:fixture.json=$manifest|ConvertTo-Json -Compress
 $script:hashCalls=0;$oldOk=$true;$oldError=$null
 try {OriginalGuard} catch {$oldOk=$false;$oldError=$_.Exception.Message}
 $oldCalls=$script:hashCalls
 $script:hashCalls=0;$newOk=$true;$newError=$null
 try {Guard} catch {$newOk=$false;$newError=$_.Exception.Message}
 Assert ($oldOk -eq $newOk -and $oldCalls -eq $script:hashCalls -and $oldError -ceq $newError)
 Assert ($newOk -eq ($name -eq 'equal-pass'))
 $g=$script:guardEvidence
 Assert ($g.schema -eq 'payload-guard-v1')
 $json=$g|ConvertTo-Json -Depth 5 -Compress
 Assert ($json.Length -lt 4096 -and -not $json.Contains('PRIVATE_SENTINEL') -and -not $json.Contains('PRIVATE/SENTINEL'))
 switch($name){
  'equal-pass' {Assert ($g.id -eq 'complete')}
  'missing-known' {Assert ($g.id -eq 'payload-inventory' -and $g.missing.Count -eq 1 -and $g.missing[0] -ceq 'controls.ps1')}
  'extra-unknown' {Assert ($g.extraCount -eq 1 -and $g.extraNameDigests[0] -cmatch '^[a-f0-9]{64}$')}
  'case-mismatch' {Assert ($g.missing -ccontains 'controls.ps1');Assert ($g.extraCount -eq 1)}
  'directory-present' {Assert ($g.directoryCount -eq 1 -and $g.id -eq 'payload-inventory')}
  'digest-mismatch' {Assert ($g.id -eq 'payload-hash' -and $g.file -ceq 'controls.ps1' -and $g.expectedSha256 -ceq ('a'*64) -and $g.observedSha256 -ceq ('b'*64))}
  'file-read-error' {Assert ($g.id -eq 'payload-read' -and $null -eq $g.observedSha256);$roundtrip=$json|ConvertFrom-Json;Assert ($null -eq $roundtrip.observedSha256)}
  'manifest-read-error' {Assert ($g.id -eq 'manifest-read')}
  'inventory-read-error' {Assert ($g.id -eq 'inventory-read')}
  'invalid-name' {Assert ($g.id -eq 'payload-name' -and $null -eq $g.file);$roundtrip=$json|ConvertFrom-Json;Assert ($null -eq $roundtrip.file)}
  'extra-truncated' {Assert ($g.extraCount -eq 17 -and $g.extraNameDigests.Count -eq 16 -and $g.inventoryTruncated)}
 }
 $results+=@{name=$name;outcome='PASS'}
}
$redacted=New-GuardEvidence 'PRIVATE_SENTINEL' 'PRIVATE_SENTINEL' 'PRIVATE_SENTINEL' 'PRIVATE_SENTINEL'
Assert ($redacted.id -eq 'unknown' -and $null -eq $redacted.file -and $null -eq $redacted.expectedSha256 -and $null -eq $redacted.observedSha256)
$results+=@{name='unknown-fields-redacted';outcome='PASS'}
$roundtrip=($redacted|ConvertTo-Json -Depth 5 -Compress)|ConvertFrom-Json
Assert ($null -eq $roundtrip.file -and $null -eq $roundtrip.expectedSha256 -and $null -eq $roundtrip.observedSha256)
$results+=@{name='redacted-null-json-roundtrip';outcome='PASS'}
$valid=New-GuardEvidence 'payload-hash' 'controls.ps1' ('a'*64) ('b'*64)
$roundtrip=($valid|ConvertTo-Json -Depth 5 -Compress)|ConvertFrom-Json
Assert ($roundtrip.file -ceq 'controls.ps1' -and $roundtrip.expectedSha256 -ceq ('a'*64) -and $roundtrip.observedSha256 -ceq ('b'*64))
$results+=@{name='valid-digest-json-roundtrip';outcome='PASS'}
$results|ConvertTo-Json -Depth 5

# Authored only. Exact PowerShell runtime required; no execution selected.
param([hashtable]$Progress)
$ErrorActionPreference='Stop'
$Progress.stage='case';$Progress.current='named-file-context'
function Require($Value){if(-not $Value){throw 'fixture_context_control'}}
$root=$PSScriptRoot
. (Join-Path $root 'lower-evidence-functions.ps1')
. (Join-Path $root 'completion-functions.ps1')
. (Join-Path $root 'original-guard-named.ps1')
. (Join-Path $root 'guard-functions.ps1')
Require ((Get-Command OriginalGuard).ScriptBlock.File -ceq (Join-Path $root 'original-guard-named.ps1'))
Require ((Get-Command Guard).ScriptBlock.File -ceq (Join-Path $root 'guard-functions.ps1'))
$results=@(@{name='named-file-context';outcome='PASS'})
$Progress.completed=@('named-file-context')
# Providers inspect the actual consumed paths privately; only fixed control names escape.
function Get-Content {param([switch]$Raw,[Parameter(Position=0)][string]$Path)
 Require ($Raw -and $Path -ceq (Join-Path $script:root 'payload-manifest.json'))
 $script:ledger+=@('manifest');return $script:fixture.json
}
function Get-ChildItem {param([string]$LiteralPath,[switch]$File,[switch]$Directory)
 Require ($LiteralPath -ceq $script:root -and ($File -xor $Directory))
 if($Directory){$script:ledger+=@('directories');return @()}
 $script:ledger+=@('files');return @($script:fixture.files|ForEach-Object {[pscustomobject]@{Name=$_}})
}
function Hash([string]$Path){
 Require ($Path -ceq (Join-Path $script:root 'controls.ps1'))
 $script:ledger+=@('hash');return $script:fixture.observed
}
foreach($case in @('nominal-pass','missing-refusal','digest-refusal')){
 $Progress.current=$case
 $Progress.originalLower=$null;$Progress.markedLower=$null
 $script:fixture=@{json=(@{'controls.ps1'=('a'*64)}|ConvertTo-Json -Compress);files=@('controls.ps1','payload-manifest.json');observed=('a'*64)}
 if($case -ceq 'missing-refusal'){$script:fixture.files=@('payload-manifest.json')}
 if($case -ceq 'digest-refusal'){$script:fixture.observed=('b'*64)}
 $pair=@()
 foreach($side in @('OriginalGuard','Guard')){
  $script:ledger=@();$ok=$true;$message=$null
  try {& $side} catch {
   $ok=$false;$message=$_.Exception.Message;$caughtFailure=$_
   try {
    $counts=@{manifestReads=[int]@($script:ledger|Where-Object {$_ -ceq 'manifest'}).Count;fileLists=[int]@($script:ledger|Where-Object {$_ -ceq 'files'}).Count;directoryLists=[int]@($script:ledger|Where-Object {$_ -ceq 'directories'}).Count;hashCalls=[int]@($script:ledger|Where-Object {$_ -ceq 'hash'}).Count}
    $lower=Get-LowerGuardEvidence $caughtFailure $counts $script:fixture
    if($side -ceq 'OriginalGuard'){$Progress.originalLower=$lower}else{$Progress.markedLower=$lower}
   } catch {}
  }
  Require ($ok -eq ($case -ceq 'nominal-pass'))
  $expectedLedger=if($case -ceq 'missing-refusal'){'manifest|files|directories'}else{'manifest|files|directories|hash'}
  Require (($script:ledger -join '|') -ceq $expectedLedger)
  if($case -ceq 'missing-refusal'){Require ($message -ceq 'payload_inventory')}
  if($case -ceq 'digest-refusal'){Require ($message -ceq 'payload_hash')}
  $pair+=@{ok=$ok;message=$message;ledger=($script:ledger -join '|')}
 }
 Require ($pair[0].ok -eq $pair[1].ok -and $pair[0].message -ceq $pair[1].message -and $pair[0].ledger -ceq $pair[1].ledger)
 $results+=@{name=$case;outcome='PASS'}
 $Progress.completed+=@($case)
}
$expected=@('equal-pass','missing-known','extra-unknown','case-mismatch','directory-present','digest-mismatch','file-read-error','manifest-read-error','inventory-read-error','invalid-name','extra-truncated','unknown-fields-redacted','redacted-null-json-roundtrip','valid-digest-json-roundtrip')
foreach($suite in @('original','marked')){
 $Progress.current=$suite+'-fourteen-oracles'
 $Progress.originalLower=$null;$Progress.markedLower=$null
 $state=@{completed=@()}
 $Progress.validation='invocation'
 try {
  if($suite -ceq 'marked'){$raw=@(& (Join-Path $root 'marked-suite.ps1') -DiagnosticState $state)}
  else {$raw=@(& (Join-Path $root 'original-suite.ps1'))}
  if($suite -ceq 'marked'){$Progress.suiteMarked='returned'}else{$Progress.suiteOriginal='returned'}
 } catch {
  if($suite -ceq 'marked'){$Progress.suiteMarked='threw'}else{$Progress.suiteOriginal='threw'}
  throw
 } finally {
  if($suite -ceq 'marked'){$Progress.suiteDiagnostic=$state;$Progress.originalLower=$state.originalLower;$Progress.markedLower=$state.markedLower}
 }
 $Progress.validation='output-shape'
 Require ($raw.Count -gt 0 -and ($raw|Where-Object {$_ -isnot [string]}).Count -eq 0)
 $Progress.validation='output-cap'
 $encoded=$raw -join "`n";Require ($encoded.Length -le 16384)
 $Progress.validation='output-json'
 $rows=@($encoded|ConvertFrom-Json)
 $Progress.validation='count'
 Require ($rows.Count -eq 14)
 $Progress.validation='identity'
 for($i=0;$i -lt 14;$i++){Require ($rows[$i].name -ceq $expected[$i] -and $rows[$i].outcome -ceq 'PASS')}
 if($suite -ceq 'marked'){
  $Progress.validation='completion-list'
  $completion=Get-CompletionEvidence $state.completed
  Require ($completion.valid)
 }
 $Progress.validation='state-oracle'
 if($suite -ceq 'marked'){Require ($state.stage -ceq 'complete' -and $state.outcome -ceq 'returned' -and (($state.completed -join '|') -ceq ($expected -join '|')))}
 $Progress.validation='validated'
 $results+=@{name=($suite+'-fourteen-oracles');outcome='PASS'}
 $Progress.completed+=@($suite+'-fourteen-oracles')
}
$Progress.stage='result-json'
$results|ConvertTo-Json -Depth 4 -Compress
$Progress.stage='complete'

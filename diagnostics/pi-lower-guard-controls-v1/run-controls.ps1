param([Parameter(Mandatory)][string]$EvidenceRoot)
$ErrorActionPreference='Stop';$watch=[Diagnostics.Stopwatch]::StartNew()
$timing=[ordered]@{sourceStart=$null;sourceEnd=$null;runtimeBeforeStart=$null;runtimeBeforeEnd=$null;controlsStart=$null;controlsEnd=$null;runtimeAfterStart=$null;runtimeAfterEnd=$null;finalGuardStart=$null;finalGuardEnd=$null;outputStart=$null}
$phase='initial';$outcome='FAILED';$category='Other';$hresult=0;$before=$null;$after=$null;$sourceAfter=$false;$script:guardEvidence=$null
$progress=@{current=$null;stage='not-invoked';completed=@();failed=$false;suiteOriginal=$null;suiteMarked=$null;suiteDiagnostic=$null}
$rows=@();$invoked=$false
function Project-LowerGuardEvidence($InputRecord) {
 $r=[ordered]@{schema='lower-guard-v1';valid=$false;category=$null;hresult=$null;exception=$null;command=$null;parameter=$null;invocationScript=$null;manifestReads=$null;fileLists=$null;directoryLists=$null;hashCalls=$null;fixtureShape=$false}
 try {
  if($InputRecord -isnot [System.Collections.IDictionary]){return $r}
  $enums=@{
   category=@('NotSpecified','InvalidOperation','InvalidData','ObjectNotFound','ResourceUnavailable','PermissionDenied','WriteError','ReadError','OperationStopped','ParserError','SyntaxError','InvalidArgument');
   exception=@('RuntimeException','ParameterBindingException','ParameterBindingValidationException','CommandNotFoundException','ItemNotFoundException','PSArgumentException','ArgumentException','InvalidOperationException','Other');
   command=@('Join-Path','Get-Content','Get-ChildItem','ConvertFrom-Json','Sort-Object','ForEach-Object','OriginalGuard','Guard','Hash');
   parameter=@('Path','LiteralPath','ChildPath','InputObject','Raw','File','Directory');invocationScript=@('empty','present')
  }
  foreach($key in $enums.Keys){if($InputRecord[$key] -is [string] -and $InputRecord[$key] -cin $enums[$key]){$r[$key]=$InputRecord[$key]}}
  if($InputRecord.hresult -isnot [int]){return $r};$r.hresult=$InputRecord.hresult
  foreach($key in @('manifestReads','fileLists','directoryLists','hashCalls')){
   $n=$InputRecord[$key];if($n -isnot [int] -or $n -lt 0 -or $n -gt 8){return $r};$r[$key]=$n
  }
  if($InputRecord.fixtureShape -isnot [bool]){return $r};$r.fixtureShape=$InputRecord.fixtureShape
  $r.valid=$InputRecord.valid -is [bool] -and $InputRecord.valid -and $null -ne $r.category -and $null -ne $r.exception -and $null -ne $r.invocationScript
 } catch {$r.valid=$false}
 return $r
}

function Get-ControlDiagnostic($State,[string]$Boundary) {
 $names=@('equal-pass','missing-known','extra-unknown','case-mismatch','directory-present','digest-mismatch','file-read-error','manifest-read-error','inventory-read-error','invalid-name','extra-truncated','unknown-fields-redacted','redacted-null-json-roundtrip','valid-digest-json-roundtrip')
 $stages=@('not-invoked','suite-initialization','case-setup','fixture-json','original-call','marked-call','assertion','evidence-json','case-complete','result-json','complete')
 $assertions=@('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15','A16','A17','A18','A19','A20','A21')
 $record=[ordered]@{schema='guard-control-progress-v1';valid=$false;boundary=$null;stage=$null;case=$null;assertion=$null;outcome=$null;completed=@()}
 try {
  if($Boundary -cin @('not-invoked','invocation','output-shape','output-json','expected-json','count','identity','validated')){$record.boundary=$Boundary}
  if($State -isnot [hashtable]){return $record}
  if($State.stage -is [string] -and $State.stage -cin $stages){$record.stage=$State.stage}
  if($State.case -is [string] -and $State.case -cin $names){$record.case=$State.case}
  if($State.assertion -is [string] -and $State.assertion -cin $assertions){$record.assertion=$State.assertion}
  if($State.outcome -is [string] -and $State.outcome -cin @('not-entered','entered','threw','returned')){$record.outcome=$State.outcome}
  $items=$State.completed
  if($items -isnot [array] -or $items.Count -gt 14){return $record}
  for($i=0;$i -lt $items.Count;$i++){
   if($items[$i] -isnot [string] -or $items[$i] -cne $names[$i]){return $record}
   $record.completed+=@($names[$i])
  }
  $record.valid=$null -ne $record.boundary -and $null -ne $record.stage -and $null -ne $record.outcome
  if($null -ne $State.case -and $null -eq $record.case){$record.valid=$false}
  if($null -ne $State.assertion -and $null -eq $record.assertion){$record.valid=$false}
 } catch {$record.valid=$false}
 return $record
}

function Hash([string]$Path){(Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()}
function Runtime {
 $process=[Diagnostics.Process]::GetCurrentProcess()
 $exe=[Environment]::ProcessPath
 if(-not $exe.StartsWith($PSHOME+[IO.Path]::DirectorySeparatorChar,[StringComparison]::OrdinalIgnoreCase)){throw 'unbound_host'}
 $assemblies=@()
 foreach($a in [AppDomain]::CurrentDomain.GetAssemblies()){
  if($a.IsDynamic -or -not $a.Location){continue}
  if(-not $a.Location.StartsWith($PSHOME+[IO.Path]::DirectorySeparatorChar,[StringComparison]::OrdinalIgnoreCase)){throw 'assembly_outside_runtime'}
  if($assemblies.Count -ge 256){throw 'assembly_count'}
  $name=$a.GetName().Name;$version=$a.GetName().Version.ToString()
  if($name -notmatch '^[A-Za-z0-9._-]{1,128}$' -or $version -notmatch '^[0-9.]{1,64}$'){throw 'assembly_metadata'}
  $f=Get-Item -LiteralPath $a.Location
  if($f.Length -gt 268435456){throw 'assembly_cap'}
  $assemblies+=@{name=$name;version=$version;sha256=(Hash $a.Location);bytes=$f.Length}
 }
 $psVersion=$PSVersionTable.PSVersion.ToString()
 if($psVersion -notmatch '^[0-9.]{1,64}$' -or $PSVersionTable.PSEdition -ne 'Core' -or $PSVersionTable.PSVersion.Major -ne 7){throw 'runtime_version'}
 return @{hostSha256=(Hash $exe);version=$psVersion;edition='Core';platform='Windows';pid=$PID;birthUtcTicks=$process.StartTime.ToUniversalTime().Ticks.ToString();assemblies=@($assemblies|Sort-Object name)}
}
function New-GuardEvidence([string]$Id,[string]$Name='',[string]$Expected='',[string]$Observed='') {
 $allowed=@('diagnostic-functions.ps1','expected-controls.json','guard-functions.ps1','lower-controls.ps1','lower-evidence-functions.ps1','marked-suite.ps1','original-guard.ps1','original-suite.ps1','payload-manifest.json','projection-functions.ps1','run-controls.ps1')
 if($Id -notin @('manifest-read','inventory-read','payload-inventory','payload-name','payload-read','payload-hash','complete')){$Id='unknown'}
 $safeName=$null;$safeExpected=$null;$safeObserved=$null
 if($Name -cin $allowed){$safeName=$Name}
 if($Expected -cmatch '^[0-9a-f]{64}$'){$safeExpected=$Expected}
 if($Observed -cmatch '^[0-9a-f]{64}$'){$safeObserved=$Observed}
 return [ordered]@{schema='payload-guard-v1';id=$Id;file=$safeName;expectedSha256=$safeExpected;observedSha256=$safeObserved;missing=@();extraNameDigests=@();extraCount=0;directoryCount=0;inventoryTruncated=$false}
}
function Set-GuardInventory($Record,$Expected,$Actual,[int]$DirectoryCount) {
 $allowed=@('diagnostic-functions.ps1','expected-controls.json','guard-functions.ps1','lower-controls.ps1','lower-evidence-functions.ps1','marked-suite.ps1','original-guard.ps1','original-suite.ps1','payload-manifest.json','projection-functions.ps1','run-controls.ps1')
 $Record.missing=@($allowed|Where-Object {$_ -cin $Expected -and $_ -cnotin $Actual})
 $extras=@($Actual|Where-Object {$_ -cnotin $Expected})
 $Record.extraCount=$extras.Count;$Record.directoryCount=$DirectoryCount
 $Record.inventoryTruncated=$extras.Count -gt 16
 $Record.extraNameDigests=@($extras|Select-Object -First 16|ForEach-Object {
  # Unknown filenames are never emitted; only an opaque bounded digest is retained.
  [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes([string]$_))).ToLowerInvariant()
 })
}
function Guard {
 $script:guardEvidence=New-GuardEvidence 'manifest-read' 'payload-manifest.json'
 $manifest=Get-Content -Raw (Join-Path $PSScriptRoot 'payload-manifest.json')|ConvertFrom-Json -AsHashtable
 $expected=@($manifest.Keys)+@('payload-manifest.json')
 $script:guardEvidence=New-GuardEvidence 'inventory-read'
 $actual=@(Get-ChildItem -LiteralPath $PSScriptRoot -File|ForEach-Object {$_.Name})
 $directoryCount=@(Get-ChildItem -LiteralPath $PSScriptRoot -Directory).Count
 if($directoryCount -ne 0 -or (($expected|Sort-Object)-join '|') -cne (($actual|Sort-Object)-join '|')){
  $script:guardEvidence=New-GuardEvidence 'payload-inventory'
  Set-GuardInventory $script:guardEvidence $expected $actual $directoryCount
  throw 'payload_inventory'
 }
 foreach($n in $manifest.Keys){
  if($n -notmatch '^[a-z0-9.-]+$'){$script:guardEvidence=New-GuardEvidence 'payload-name' $n;throw 'payload_hash'}
  $script:guardEvidence=New-GuardEvidence 'payload-read' $n ([string]$manifest[$n])
  $observed=Hash (Join-Path $PSScriptRoot $n)
  if($observed -cne $manifest[$n]){$script:guardEvidence=New-GuardEvidence 'payload-hash' $n ([string]$manifest[$n]) $observed;throw 'payload_hash'}
 }
 $script:guardEvidence=New-GuardEvidence 'complete'
}

try {
 if(-not $IsWindows -or (Test-Path -LiteralPath $EvidenceRoot)){throw 'exclusive_windows_required'}
 $null=New-Item -ItemType Directory -Path $EvidenceRoot
 $phase='source-guard';$timing.sourceStart=$watch.ElapsedMilliseconds;Guard;$timing.sourceEnd=$watch.ElapsedMilliseconds
 if('OwnedSetup' -as [type]){throw 'native_type_forbidden'}
 $phase='runtime-before';$timing.runtimeBeforeStart=$watch.ElapsedMilliseconds;$before=Runtime;$timing.runtimeBeforeEnd=$watch.ElapsedMilliseconds
 $phase='lower-controls';$timing.controlsStart=$watch.ElapsedMilliseconds;$invoked=$true
 $captured=@(& (Join-Path $PSScriptRoot 'lower-controls.ps1') -Progress $progress)
 if($captured.Count -ne 1 -or $captured[0] -isnot [string] -or [Text.Encoding]::UTF8.GetByteCount($captured[0]) -gt 65536){throw 'control_output_shape'}
 $parsed=@($captured[0]|ConvertFrom-Json)
 $expected=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-controls.json')|ConvertFrom-Json)
 if($parsed.Count -ne 16 -or $expected.Count -ne 16){throw 'control_count'}
 for($i=0;$i -lt 16;$i++){
  $item=$parsed[$i];$keys=@($item.PSObject.Properties.Name|Sort-Object)
  if(($keys-join '|') -cne 'name|outcome' -or $item.name -cne $expected[$i] -or $item.outcome -cne 'PASS'){throw 'control_identity_or_outcome'}
  $rows+=@{name=$expected[$i];outcome='PASS'}
 }
 $timing.controlsEnd=$watch.ElapsedMilliseconds
 $phase='runtime-after';$timing.runtimeAfterStart=$watch.ElapsedMilliseconds;$after=Runtime;$timing.runtimeAfterEnd=$watch.ElapsedMilliseconds
 if($before.hostSha256 -cne $after.hostSha256 -or $before.pid -ne $after.pid -or $before.birthUtcTicks -cne $after.birthUtcTicks){throw 'runtime_changed'}
 foreach($a in $before.assemblies){$match=@($after.assemblies|Where-Object {$_.name -ceq $a.name -and $_.version -ceq $a.version -and $_.sha256 -ceq $a.sha256});if($match.Count -ne 1){throw 'assembly_changed'}}
 $phase='final-guard';$timing.finalGuardStart=$watch.ElapsedMilliseconds;Guard;$sourceAfter=$true;$timing.finalGuardEnd=$watch.ElapsedMilliseconds
 if('OwnedSetup' -as [type]){throw 'native_type_loaded'}
 if($watch.ElapsedMilliseconds -gt 45000){throw 'controls_elapsed_bound'}
 $phase='complete';$outcome='PASS';$category='None'
} catch {
 $hresult=[int]$_.Exception.HResult;$c=[string]$_.CategoryInfo.Category
 if($c -cin @('NotSpecified','InvalidOperation','InvalidData','ObjectNotFound','ResourceUnavailable','PermissionDenied','WriteError','ReadError','OperationStopped','ParserError','SyntaxError','InvalidArgument')){$category=$c}
} finally {
 try {
  $allowed=@('known-binding-category-command-parameter','unknown-projection-fields-redacted','null-error-invalid','non-error-record-invalid','int32-hresult-boundaries','binding-failure-zero-provider-counts','counter-null-type-overflow-invalid','bookkeeping-fault-preserves-caught-behavior','caught-original-state-and-output-preserved','lower-null-fields-json-roundtrip','extra-fields-never-exported','original-twentyone-oracles-and-throws-preserved','reset-before-unknown-fields-redacted','reset-before-redacted-null-json-roundtrip','reset-before-valid-digest-json-roundtrip','affected-original-marked-pair-transparency')
  $safeCurrent=$null;if($progress.current -is [string] -and $progress.current -cin $allowed){$safeCurrent=$progress.current}
  $safeStage=$null;if($progress.stage -is [string] -and $progress.stage -cin @('not-invoked','case','result-json','complete')){$safeStage=$progress.stage}
  $partial=@();$partialValid=$progress.completed -is [array] -and $progress.completed.Count -le 16
  if($partialValid){for($i=0;$i -lt $progress.completed.Count;$i++){if($progress.completed[$i] -isnot [string] -or $progress.completed[$i] -cne $allowed[$i]){$partialValid=$false;break};$partial+=@($allowed[$i])}}
  $old=$null;$new=$null
  if($progress.suiteOriginal -cin @('returned','threw')){$old=$progress.suiteOriginal}
  if($progress.suiteMarked -cin @('returned','threw')){$new=$progress.suiteMarked}
  # Re-project even already-sanitized metadata through the exact finite helper; never serialize arbitrary dictionary fields.
  $suite=$null
  if($progress.suiteDiagnostic -is [System.Collections.IDictionary]){
   $inputState=@{stage=$progress.suiteDiagnostic.stage;case=$progress.suiteDiagnostic.case;assertion=$progress.suiteDiagnostic.assertion;outcome=$progress.suiteDiagnostic.outcome;completed=$progress.suiteDiagnostic.completed}
   $suite=Get-ControlDiagnostic $inputState 'invocation'
  }
  foreach($key in @($timing.Keys)){if($null -ne $timing[$key] -and ($timing[$key] -lt 0 -or $timing[$key] -gt 180000)){$timing[$key]=$null}}
  $outputStart=$watch.ElapsedMilliseconds
  $timing.outputStart=$(if($outputStart -ge 0 -and $outputStart -le 180000){$outputStart}else{$null})
  $record=[ordered]@{schema='lower-guard-controls-v1';outcome=$outcome;phase=$phase;category=$category;hresult=$hresult;guard=$script:guardEvidence;invoked=$invoked;acceptedCount=$(if($outcome -eq 'PASS'){16}else{0});results=$(if($outcome -eq 'PASS'){$rows}else{@()});current=$safeCurrent;stage=$safeStage;completed=$partial;partialValid=$partialValid;suiteOriginal=$old;suiteMarked=$new;suiteDiagnostic=$suite;originalLower=(Project-LowerGuardEvidence $progress.originalLower);markedLower=(Project-LowerGuardEvidence $progress.markedLower);timing=$timing;elapsedMs=$watch.ElapsedMilliseconds;runtimeBefore=$before;runtimeAfter=$after;sourceAfter=$sourceAfter;guardSuiteAccepted=0;captureSuiteInvoked=$false;historicalCleanupProven=$false;setupInvocations=0;behavioralInvocations=0}
  $json=$record|ConvertTo-Json -Depth 12
  if([Text.Encoding]::UTF8.GetByteCount($json) -gt 262144){throw 'evidence_cap'}
  [IO.File]::WriteAllText((Join-Path $EvidenceRoot 'result.json'),$json,[Text.UTF8Encoding]::new($false))
  $outputEnd=$watch.ElapsedMilliseconds
  if($outputStart -ge 0 -and $outputEnd -ge $outputStart -and $outputEnd -le 180000){[Console]::Out.WriteLine('LOWER_GUARD_OUTPUT_MS '+$outputStart+' '+$outputEnd)}else{[Console]::Out.WriteLine('LOWER_GUARD_OUTPUT_UNAVAILABLE')}
  [Console]::Out.WriteLine('LOWER_GUARD_CONTROLS '+$outcome)
 } catch {$outcome='FAILED';[Console]::Out.WriteLine('LOWER_GUARD_CONTROLS EVIDENCE_UNAVAILABLE')}
}
if($outcome -ne 'PASS'){exit 1}

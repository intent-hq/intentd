param([Parameter(Mandatory)][string]$EvidenceRoot)
$ErrorActionPreference='Stop';$watch=[Diagnostics.Stopwatch]::StartNew()
$phase='initial';$outcome='FAILED';$category='Other';$hresult=0;$before=$null;$after=$null;$sourceAfter=$false;$script:guardEvidence=$null
$progress=@{current=$null;stage='not-invoked';completed=@();failed=$false;suiteOriginal=$null;suiteMarked=$null;suiteDiagnostic=$null}
$rows=@();$invoked=$false
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
 $allowed=@('diagnostic-functions.ps1','expected-metadata-controls.json','guard-case-names.json','guard-functions.ps1','marked-suite.ps1','metadata-controls.ps1','original-guard.ps1','original-suite.ps1','payload-manifest.json','run-metadata-controls.ps1')
 if($Id -notin @('manifest-read','inventory-read','payload-inventory','payload-name','payload-read','payload-hash','complete')){$Id='unknown'}
 $safeName=$null;$safeExpected=$null;$safeObserved=$null
 if($Name -cin $allowed){$safeName=$Name}
 if($Expected -cmatch '^[0-9a-f]{64}$'){$safeExpected=$Expected}
 if($Observed -cmatch '^[0-9a-f]{64}$'){$safeObserved=$Observed}
 return [ordered]@{schema='payload-guard-v1';id=$Id;file=$safeName;expectedSha256=$safeExpected;observedSha256=$safeObserved;missing=@();extraNameDigests=@();extraCount=0;directoryCount=0;inventoryTruncated=$false}
}
function Set-GuardInventory($Record,$Expected,$Actual,[int]$DirectoryCount) {
 $allowed=@('diagnostic-functions.ps1','expected-metadata-controls.json','guard-case-names.json','guard-functions.ps1','marked-suite.ps1','metadata-controls.ps1','original-guard.ps1','original-suite.ps1','payload-manifest.json','run-metadata-controls.ps1')
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
 $phase='source-guard';Guard
 if('OwnedSetup' -as [type]){throw 'native_type_forbidden'}
 $phase='runtime-before';$before=Runtime
 $phase='metadata-controls';$invoked=$true
 $captured=@(& (Join-Path $PSScriptRoot 'metadata-controls.ps1') -Progress $progress)
 if($captured.Count -ne 1 -or $captured[0] -isnot [string] -or [Text.Encoding]::UTF8.GetByteCount($captured[0]) -gt 65536){throw 'control_output_shape'}
 $parsed=@($captured[0]|ConvertFrom-Json)
 $expected=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-metadata-controls.json')|ConvertFrom-Json)
 if($parsed.Count -ne 25 -or $expected.Count -ne 25){throw 'control_count'}
 for($i=0;$i -lt 25;$i++){
  $item=$parsed[$i];$keys=@($item.PSObject.Properties.Name|Sort-Object)
  if(($keys-join '|') -cne 'name|outcome' -or $item.name -cne $expected[$i] -or $item.outcome -cne 'PASS'){throw 'control_identity_or_outcome'}
  $rows+=@{name=$expected[$i];outcome='PASS'}
 }
 $phase='runtime-after';$after=Runtime
 if($before.hostSha256 -cne $after.hostSha256 -or $before.pid -ne $after.pid -or $before.birthUtcTicks -cne $after.birthUtcTicks){throw 'runtime_changed'}
 foreach($a in $before.assemblies){$match=@($after.assemblies|Where-Object {$_.name -ceq $a.name -and $_.version -ceq $a.version -and $_.sha256 -ceq $a.sha256});if($match.Count -ne 1){throw 'assembly_changed'}}
 $phase='final-guard';Guard;$sourceAfter=$true
 if('OwnedSetup' -as [type]){throw 'native_type_loaded'}
 if($watch.ElapsedMilliseconds -gt 10000){throw 'controls_elapsed_bound'}
 $phase='complete';$outcome='PASS';$category='None'
} catch {
 $hresult=[int]$_.Exception.HResult;$c=[string]$_.CategoryInfo.Category
 if($c -cin @('NotSpecified','InvalidOperation','InvalidData','ObjectNotFound','ResourceUnavailable','PermissionDenied','WriteError','ReadError','OperationStopped','ParserError','SyntaxError','InvalidArgument')){$category=$c}
} finally {
 try {
  $allowed=@('null-state-redacted','wrong-state-type-redacted','strict-null-roundtrip','valid-empty-prefix','valid-fourteen-prefix','unknown-case-redacted','unknown-assertion-redacted','unknown-stage-redacted','unknown-outcome-redacted','unknown-boundary-redacted','null-completed-invalid','scalar-completed-invalid','nonprefix-invalid-bounded-prefix','duplicate-invalid','overflow-invalid','nonstring-member-invalid','reference-update-visible','metadata-no-success-output','assert-true-no-output','assert-false-original-throw','expression-throw-precedes-marker','bookkeeping-fault-proven','bookkeeping-fault-keeps-assert-throw','boundary-values-distinct','original-versus-marked-suite-transparency')
  $safeCurrent=$null;if($progress.current -is [string] -and $progress.current -cin $allowed){$safeCurrent=$progress.current}
  $safeStage=$null;if($progress.stage -is [string] -and $progress.stage -cin @('not-invoked','case','result-json','complete')){$safeStage=$progress.stage}
  $partial=@();$partialValid=$progress.completed -is [array] -and $progress.completed.Count -le 25
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
  $record=[ordered]@{schema='guard-metadata-controls-v1';outcome=$outcome;phase=$phase;category=$category;hresult=$hresult;guard=$script:guardEvidence;invoked=$invoked;acceptedCount=$(if($outcome -eq 'PASS'){25}else{0});results=$(if($outcome -eq 'PASS'){$rows}else{@()});current=$safeCurrent;stage=$safeStage;completed=$partial;partialValid=$partialValid;suiteOriginal=$old;suiteMarked=$new;suiteDiagnostic=$suite;elapsedMs=$watch.ElapsedMilliseconds;runtimeBefore=$before;runtimeAfter=$after;sourceAfter=$sourceAfter;guardSuiteAccepted=0;captureSuiteInvoked=$false;historicalCleanupProven=$false;setupInvocations=0;behavioralInvocations=0}
  $json=$record|ConvertTo-Json -Depth 12
  if([Text.Encoding]::UTF8.GetByteCount($json) -gt 262144){throw 'evidence_cap'}
  [IO.File]::WriteAllText((Join-Path $EvidenceRoot 'result.json'),$json,[Text.UTF8Encoding]::new($false))
  [Console]::Out.WriteLine('GUARD_METADATA_CONTROLS '+$outcome)
 } catch {$outcome='FAILED';[Console]::Out.WriteLine('GUARD_METADATA_CONTROLS EVIDENCE_UNAVAILABLE')}
}
if($outcome -ne 'PASS'){exit 1}

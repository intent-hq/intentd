param([Parameter(Mandatory)][string]$EvidenceRoot)
$ErrorActionPreference='Stop';$watch=[Diagnostics.Stopwatch]::StartNew()
$timing=[ordered]@{sourceStart=$null;sourceEnd=$null;runtimeBeforeStart=$null;runtimeBeforeEnd=$null;controlsStart=$null;controlsEnd=$null;runtimeAfterStart=$null;runtimeAfterEnd=$null;finalGuardStart=$null;finalGuardEnd=$null;outputStart=$null}
$phase='initial';$outcome='FAILED';$category='Other';$hresult=0;$before=$null;$after=$null;$sourceAfter=$false;$script:guardEvidence=$null
$progress=@{current=$null;stage='not-invoked';completed=@();failed=$false;suiteOriginal=$null;suiteMarked=$null;suiteDiagnostic=$null}
$rows=@();$invoked=$false
$completionRows=@();$completionInvoked=$false;$completionProgress=@{current=$null;completed=@()}
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

function Get-CompletionEvidence($Items) {
 $expected=@('equal-pass','missing-known','extra-unknown','case-mismatch','directory-present','digest-mismatch','file-read-error','manifest-read-error','inventory-read-error','invalid-name','extra-truncated','unknown-fields-redacted','redacted-null-json-roundtrip','valid-digest-json-roundtrip')
 $r=[ordered]@{schema='guard-completion-v1';valid=$false;reason='not-array';count=$null;nullIndices=@();firstMismatch=$null}
 if($Items -isnot [array] -or $Items.Rank -ne 1){return $r}
 if($Items.Count -gt 64){$r.reason='count-over-cap';return $r}
 $r.count=[int]$Items.Count
 for($i=0;$i -lt $Items.Count;$i++){if($null -eq $Items[$i]){$r.nullIndices+=@($i)}}
 if($Items.Count -ne 14){$r.reason='count';return $r}
 $seen=@()
 for($i=0;$i -lt 14;$i++){
  if($null -eq $Items[$i]){$r.reason='null-element';$r.firstMismatch=$i;return $r}
  if($Items[$i] -isnot [string]){$r.reason='element-type';$r.firstMismatch=$i;return $r}
  if($seen -ccontains $Items[$i]){$r.reason='duplicate';$r.firstMismatch=$i;return $r}
  if($Items[$i] -cne $expected[$i]){$r.reason='identity';$r.firstMismatch=$i;return $r}
  $seen+=@($Items[$i])
 }
 $r.valid=$true;$r.reason='complete';return $r
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
 $allowed=@('completion-controls.ps1','completion-functions.ps1','completion-producer.ps1','context-controls.ps1','expected-completion-controls.json','expected-controls.json','guard-functions.ps1','lower-evidence-functions.ps1','marked-suite.ps1','original-guard-named.ps1','original-guard.ps1','original-suite.ps1','payload-manifest.json','run-controls.ps1')
 if($Id -notin @('manifest-read','inventory-read','payload-inventory','payload-name','payload-read','payload-hash','complete')){$Id='unknown'}
 $safeName=$null;$safeExpected=$null;$safeObserved=$null
 if($Name -cin $allowed){$safeName=$Name}
 if($Expected -cmatch '^[0-9a-f]{64}$'){$safeExpected=$Expected}
 if($Observed -cmatch '^[0-9a-f]{64}$'){$safeObserved=$Observed}
 return [ordered]@{schema='payload-guard-v1';id=$Id;file=$safeName;expectedSha256=$safeExpected;observedSha256=$safeObserved;missing=@();extraNameDigests=@();extraCount=0;directoryCount=0;inventoryTruncated=$false}
}
function Set-GuardInventory($Record,$Expected,$Actual,[int]$DirectoryCount) {
 $allowed=@('completion-controls.ps1','completion-functions.ps1','completion-producer.ps1','context-controls.ps1','expected-completion-controls.json','expected-controls.json','guard-functions.ps1','lower-evidence-functions.ps1','marked-suite.ps1','original-guard-named.ps1','original-guard.ps1','original-suite.ps1','payload-manifest.json','run-controls.ps1')
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
 $phase='completion-controls';$timing.controlsStart=$watch.ElapsedMilliseconds;$completionInvoked=$true
 $completionCaptured=@(& (Join-Path $PSScriptRoot 'completion-controls.ps1') -CompletionProgress $completionProgress)
 if($completionCaptured.Count -ne 1 -or $completionCaptured[0] -isnot [string] -or [Text.Encoding]::UTF8.GetByteCount($completionCaptured[0]) -gt 65536){throw 'completion_output_shape'}
 $completionParsed=@($completionCaptured[0]|ConvertFrom-Json)
 $completionExpected=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-completion-controls.json')|ConvertFrom-Json)
 if($completionParsed.Count -ne 12 -or $completionExpected.Count -ne 12){throw 'completion_count'}
 for($i=0;$i -lt 12;$i++){
  $item=$completionParsed[$i];$keys=@($item.PSObject.Properties.Name|Sort-Object)
  if(($keys-join '|') -cne 'name|outcome' -or $item.name -cne $completionExpected[$i] -or $item.outcome -cne 'PASS'){throw 'completion_identity_or_outcome'}
  $completionRows+=@{name=$completionExpected[$i];outcome='PASS'}
 }
 $phase='context-controls';$invoked=$true
 $captured=@(& (Join-Path $PSScriptRoot 'context-controls.ps1') -Progress $progress)
 if($captured.Count -ne 1 -or $captured[0] -isnot [string] -or [Text.Encoding]::UTF8.GetByteCount($captured[0]) -gt 65536){throw 'control_output_shape'}
 $parsed=@($captured[0]|ConvertFrom-Json)
 $expected=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-controls.json')|ConvertFrom-Json)
 if($parsed.Count -ne 6 -or $expected.Count -ne 6){throw 'control_count'}
 for($i=0;$i -lt 6;$i++){
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
  $allowed=@('named-file-context','nominal-pass','missing-refusal','digest-refusal','original-fourteen-oracles','marked-fourteen-oracles')
  $safeCurrent=$null;if($progress.current -is [string] -and $progress.current -cin $allowed){$safeCurrent=$progress.current}
  $safeStage=$null;if($progress.stage -is [string] -and $progress.stage -cin @('not-invoked','case','result-json','complete')){$safeStage=$progress.stage}
  $partial=@();$partialValid=$progress.completed -is [array] -and $progress.completed.Count -le 6
  if($partialValid){for($i=0;$i -lt $progress.completed.Count;$i++){if($progress.completed[$i] -isnot [string] -or $progress.completed[$i] -cne $allowed[$i]){$partialValid=$false;break};$partial+=@($allowed[$i])}}
  $validation=$null
  if($progress.validation -is [string] -and $progress.validation -cin @('invocation','output-shape','output-cap','output-json','count','identity','completion-list','state-oracle','validated')){$validation=$progress.validation}
  $completionEvidence=Get-CompletionEvidence $null
  if($progress.suiteDiagnostic -is [System.Collections.IDictionary]){$completionEvidence=Get-CompletionEvidence $progress.suiteDiagnostic.completed}
  $completionAllowed=@('legacy-missing-key-null-seed','initialized-exact-producer-fourteen','null-completion-refused','scalar-joined-list-refused','short-prefix-refused','reordered-identities-refused','duplicate-identity-refused','case-changed-identity-refused','null-slot-refused','nonstring-element-refused','oversized-array-bounded-null-roundtrip','foreign-content-never-exported')
  $completionCurrent=$null
  if($completionProgress.current -is [string] -and $completionProgress.current -cin $completionAllowed){$completionCurrent=$completionProgress.current}
  $completionPartial=@();$completionPartialValid=$completionProgress.completed -is [array] -and $completionProgress.completed.Count -le 12
  if($completionPartialValid){for($i=0;$i -lt $completionProgress.completed.Count;$i++){if($completionProgress.completed[$i] -isnot [string] -or $completionProgress.completed[$i] -cne $completionAllowed[$i]){$completionPartialValid=$false;break};$completionPartial+=@($completionAllowed[$i])}}
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
  $record=[ordered]@{schema='completion-fixture-controls-v1';outcome=$outcome;phase=$phase;category=$category;hresult=$hresult;guard=$script:guardEvidence;completionInvoked=$completionInvoked;completionAcceptedCount=$(if($outcome -eq 'PASS'){12}else{0});completionResults=$(if($outcome -eq 'PASS'){$completionRows}else{@()});completionCurrent=$completionCurrent;completionCompleted=$completionPartial;completionPartialValid=$completionPartialValid;invoked=$invoked;acceptedCount=$(if($outcome -eq 'PASS'){6}else{0});results=$(if($outcome -eq 'PASS'){$rows}else{@()});current=$safeCurrent;stage=$safeStage;completed=$partial;partialValid=$partialValid;validation=$validation;completionEvidence=$completionEvidence;suiteOriginal=$old;suiteMarked=$new;suiteDiagnostic=$suite;originalLower=(Project-LowerGuardEvidence $progress.originalLower);markedLower=(Project-LowerGuardEvidence $progress.markedLower);timing=$timing;elapsedMs=$watch.ElapsedMilliseconds;runtimeBefore=$before;runtimeAfter=$after;sourceAfter=$sourceAfter;guardSuiteAccepted=$(if($outcome -eq 'PASS'){14}else{0});originalGuardSuiteAccepted=$(if($outcome -eq 'PASS'){14}else{0});namedFileAndConsumedProviderPathsValidated=($outcome -eq 'PASS');captureSuiteInvoked=$false;historicalCleanupProven=$false;setupInvocations=0;behavioralInvocations=0}
  $json=$record|ConvertTo-Json -Depth 12
  if([Text.Encoding]::UTF8.GetByteCount($json) -gt 262144){throw 'evidence_cap'}
  [IO.File]::WriteAllText((Join-Path $EvidenceRoot 'result.json'),$json,[Text.UTF8Encoding]::new($false))
  $outputEnd=$watch.ElapsedMilliseconds
  if($outputStart -ge 0 -and $outputEnd -ge $outputStart -and $outputEnd -le 180000){[Console]::Out.WriteLine('COMPLETION_FIXTURE_OUTPUT_MS '+$outputStart+' '+$outputEnd)}else{[Console]::Out.WriteLine('COMPLETION_FIXTURE_OUTPUT_UNAVAILABLE')}
  [Console]::Out.WriteLine('COMPLETION_FIXTURE_CONTROLS '+$outcome)
 } catch {$outcome='FAILED';[Console]::Out.WriteLine('COMPLETION_FIXTURE_CONTROLS EVIDENCE_UNAVAILABLE')}
}
if($outcome -ne 'PASS'){exit 1}

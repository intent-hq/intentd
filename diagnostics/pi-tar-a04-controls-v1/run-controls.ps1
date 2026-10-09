param([Parameter(Mandatory)][string]$EvidenceRoot)
$ErrorActionPreference='Stop'
$watch=[Diagnostics.Stopwatch]::StartNew()
$timing=[ordered]@{sourceStart=$null;sourceEnd=$null;runtimeBeforeStart=$null;runtimeBeforeEnd=$null;controlsStart=$null;controlsEnd=$null;runtimeAfterStart=$null;runtimeAfterEnd=$null;finalGuardStart=$null;finalGuardEnd=$null;outputStart=$null}
$script:guardEvidence=$null
$controlProgress=@{case=$null;assertion=0;completed=@()}
$fixtureState=@{created=$false;pathRestored=$false;ofsUnchanged=$false;writerRestored=$false;cleaned=$false;complete=$false;childStarted=$false;childExited=$false;childKilled=$false;childDisposed=$false;jsComplete=$false;jsCase=0;jsAssertion=0;jsOutcome='unavailable';duplicateBehavior='unavailable'}
$node=$null;$nodeBefore=$null;$nodeAfter=$null
$phase='initial';$outcome='FAILED';$category='Other';$hresult=0;$invoked=$false;$rows=@();$before=$null;$after=$null;$sourceAfter=$false;$savedError=[Console]::Error
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
 $allowed=@('accept-dependencies.mjs','acceptance-delta.json','controls.ps1','expected-controls.json','node-controls.mjs','original-accept-dependencies.mjs','original-setup-only.ps1','payload-manifest.json','predicate-map.json','relay-function.ps1','run-controls.ps1','schema.json','setup-delta.json','setup-only.ps1')
 if($Id -notin @('manifest-read','inventory-read','payload-inventory','payload-name','payload-read','payload-hash','complete')){$Id='unknown'}
 $safeName=$null;$safeExpected=$null;$safeObserved=$null
 if($Name -cin $allowed){$safeName=$Name}
 if($Expected -cmatch '^[0-9a-f]{64}$'){$safeExpected=$Expected}
 if($Observed -cmatch '^[0-9a-f]{64}$'){$safeObserved=$Observed}
 return [ordered]@{schema='payload-guard-v1';id=$Id;file=$safeName;expectedSha256=$safeExpected;observedSha256=$safeObserved;missing=@();extraNameDigests=@();extraCount=0;directoryCount=0;inventoryTruncated=$false}
}
function Set-GuardInventory($Record,$Expected,$Actual,[int]$DirectoryCount) {
 $allowed=@('accept-dependencies.mjs','acceptance-delta.json','controls.ps1','expected-controls.json','node-controls.mjs','original-accept-dependencies.mjs','original-setup-only.ps1','payload-manifest.json','predicate-map.json','relay-function.ps1','run-controls.ps1','schema.json','setup-delta.json','setup-only.ps1')
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
 $node=(Get-Command node.exe -CommandType Application -TotalCount 1).Source
 if(-not [IO.Path]::IsPathFullyQualified($node) -or -not(Test-Path -LiteralPath $node -PathType Leaf)){throw 'node_unbound'}
 $nodeBefore=Hash $node
 $phase='runtime-before';$timing.runtimeBeforeStart=$watch.ElapsedMilliseconds;$before=Runtime;$timing.runtimeBeforeEnd=$watch.ElapsedMilliseconds
 $phase='controls';$timing.controlsStart=$watch.ElapsedMilliseconds;$invoked=$true
 $captured=@(& (Join-Path $PSScriptRoot 'controls.ps1') -Progress $controlProgress -FixtureRoot (Join-Path $EvidenceRoot 'inert-fixture') -FixtureState $fixtureState -Node $node)
 if($fixtureState.complete -isnot [bool] -or -not $fixtureState.complete -or -not $fixtureState.created -or -not $fixtureState.pathRestored -or -not $fixtureState.ofsUnchanged -or -not $fixtureState.writerRestored -or -not $fixtureState.cleaned -or -not $fixtureState.childExited -or -not $fixtureState.childDisposed -or $fixtureState.childKilled -or -not $fixtureState.jsComplete){throw 'fixture_incomplete'}
 if($captured.Count -ne 1 -or $captured[0] -isnot [string] -or [Text.Encoding]::UTF8.GetByteCount($captured[0]) -gt 65536){throw 'control_output_shape'}
 $parsed=@($captured[0]|ConvertFrom-Json)
 $expected=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-controls.json')|ConvertFrom-Json)
 if($expected.Count -ne 6 -or $parsed.Count -ne 6){throw 'control_count'}
 for($i=0;$i -lt 6;$i++){
  $item=$parsed[$i];$keys=@($item.PSObject.Properties.Name|Sort-Object)
  if(($keys-join '|') -cne 'name|outcome' -or $item.name -cne $expected[$i] -or $item.outcome -cne 'PASS'){throw 'control_identity_or_outcome'}
  $rows+=@{name=$expected[$i];outcome='PASS'}
 }
 $timing.controlsEnd=$watch.ElapsedMilliseconds
 $phase='runtime-after';$timing.runtimeAfterStart=$watch.ElapsedMilliseconds;$after=Runtime;$timing.runtimeAfterEnd=$watch.ElapsedMilliseconds
 if($before.hostSha256 -cne $after.hostSha256 -or $before.version -cne $after.version -or $before.pid -ne $after.pid -or $before.birthUtcTicks -cne $after.birthUtcTicks){throw 'runtime_changed'}
 foreach($a in $before.assemblies){$match=@($after.assemblies|Where-Object {$_.name -ceq $a.name -and $_.version -ceq $a.version -and $_.sha256 -ceq $a.sha256});if($match.Count -ne 1){throw 'assembly_changed'}}
 $phase='final-guard';$timing.finalGuardStart=$watch.ElapsedMilliseconds;Guard;$sourceAfter=$true;$timing.finalGuardEnd=$watch.ElapsedMilliseconds
 if('OwnedSetup' -as [type]){throw 'native_type_loaded'}
 $nodeAfter=Hash $node;if($nodeBefore -cne $nodeAfter){throw 'node_changed'}
 # Post-return latency acceptance only. Host step timeout is the external interruption bound.
 if($watch.ElapsedMilliseconds -gt 60000){throw 'controls_elapsed_bound'}
 $phase='complete';$outcome='PASS';$category='None'
} catch {
 $hresult=[int]$_.Exception.HResult
 $candidate=[string]$_.CategoryInfo.Category
 if($candidate -in @('NotSpecified','InvalidOperation','InvalidData','ObjectNotFound','ResourceUnavailable','PermissionDenied','WriteError','ReadError','OperationStopped','ParserError','SyntaxError','InvalidArgument')){$category=$candidate}
} finally {
 [Console]::SetError($savedError)
 try {
  foreach($key in @($timing.Keys)){if($null -ne $timing[$key] -and ($timing[$key] -lt 0 -or $timing[$key] -gt 180000)){$timing[$key]=$null}}
  $outputStart=$watch.ElapsedMilliseconds;$timing.outputStart=$(if($outputStart -ge 0 -and $outputStart -le 180000){$outputStart}else{$null})
  $progressSafe=[ordered]@{valid=$false;case=$null;assertion=$null;completed=@()}
  $allowedCases=@('exact-reversal-and-success-single-evaluation','four-predicate-first-false-and-sensitive-negatives','evaluation-fault-and-return-type-boundaries','fresh-block-marker-and-observer-faults','fixed-sixfield-relay-schema-and-redaction','outercatch-file-writer-ownership-composition')
  try {
   $cp=$controlProgress
   if($cp -isnot [hashtable] -or (($cp.Keys|Sort-Object)-join '|') -cne 'assertion|case|completed'){throw 'progress_shape'}
   if($null -ne $cp.case -and ($cp.case -isnot [string] -or $cp.case -cnotin $allowedCases)){throw 'progress_case'}
   if($cp.assertion -isnot [int] -or $cp.assertion -lt 0 -or $cp.assertion -gt 96){throw 'progress_assertion'}
   if($cp.completed -isnot [object[]] -or $cp.completed.Rank -ne 1 -or $cp.completed.GetLowerBound(0) -ne 0 -or $cp.completed.Count -gt 6){throw 'progress_prefix'}
   for($j=0;$j -lt $cp.completed.Count;$j++){if($cp.completed[$j] -isnot [string] -or $cp.completed[$j] -cne $allowedCases[$j]){throw 'progress_identity'}}
   $progressSafe=[ordered]@{valid=$true;case=$cp.case;assertion=$cp.assertion;completed=@($cp.completed)}
  } catch { }
  $fixtureSafe=[ordered]@{valid=$false}
  try {
   $bools=@('created','pathRestored','ofsUnchanged','writerRestored','cleaned','complete','childStarted','childExited','childKilled','childDisposed','jsComplete')
   $keys=@($bools)+@('jsCase','jsAssertion','jsOutcome','duplicateBehavior')
   if($fixtureState -isnot [hashtable] -or (($fixtureState.Keys|Sort-Object)-join '|') -cne (($keys|Sort-Object)-join '|')){throw 'fixture_schema'}
   foreach($key in $bools){if($fixtureState[$key] -isnot [bool]){throw 'fixture_type'}}
   if($fixtureState.jsCase -isnot [int] -or $fixtureState.jsCase -lt 0 -or $fixtureState.jsCase -gt 6 -or $fixtureState.jsAssertion -isnot [int] -or $fixtureState.jsAssertion -lt 0 -or $fixtureState.jsAssertion -gt 96){throw 'fixture_number'}
   if($fixtureState.jsOutcome -cnotin @('unavailable','PASS','FAILED') -or $fixtureState.duplicateBehavior -cnotin @('unavailable','last-wins','first-wins','rejected')){throw 'fixture_enum'}
   foreach($key in $keys){$fixtureSafe[$key]=$fixtureState[$key]};$fixtureSafe.valid=$true
  }catch{ }
  $record=[ordered]@{schema='tar-a04-controls-v1';outcome=$outcome;phase=$phase;category=$category;hresult=$hresult;guard=$script:guardEvidence;timing=$timing;controlsInvoked=$invoked;progress=$progressSafe;fixture=$fixtureSafe;acceptedCount=$(if($outcome -eq 'PASS'){6}else{0});results=$(if($outcome -eq 'PASS'){$rows}else{@()});elapsedMs=$watch.ElapsedMilliseconds;sourceAfter=$sourceAfter;runtimeBefore=$before;runtimeAfter=$after;nodeBefore=$nodeBefore;nodeAfter=$nodeAfter;childCreationRoute='one-owned-node-control-process';privateAcceptanceLogRead=$false;historicalCleanupProven=$false;setupInvocations=0;behavioralInvocations=0}
  $json=$record|ConvertTo-Json -Depth 10
  if([Text.Encoding]::UTF8.GetByteCount($json) -gt 262144){throw 'evidence_cap'}
  if($fixtureState.childStarted -and -not $fixtureState.childExited){throw 'unsafe_child_evidence_block'}
  [IO.File]::WriteAllText((Join-Path $EvidenceRoot 'result.json'),$json,[Text.UTF8Encoding]::new($false))
  $outputEnd=$watch.ElapsedMilliseconds
  if($outputStart -ge 0 -and $outputEnd -ge $outputStart -and $outputEnd -le 180000){[Console]::Out.WriteLine('TAR_A04_OUTPUT_MS '+$outputStart+' '+$outputEnd)}else{[Console]::Out.WriteLine('TAR_A04_OUTPUT_UNAVAILABLE')}
  [Console]::Out.WriteLine('TAR_A04_CONTROLS '+$outcome)
 } catch {$outcome='FAILED';[Console]::Out.WriteLine('TAR_A04_CONTROLS EVIDENCE_UNAVAILABLE')}
}
if($outcome -ne 'PASS'){exit 1}

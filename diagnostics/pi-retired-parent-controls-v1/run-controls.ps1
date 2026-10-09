param([Parameter(Mandatory)][string]$EvidenceRoot)
$ErrorActionPreference='Stop'
$watch=[Diagnostics.Stopwatch]::StartNew()
$timing=[ordered]@{sourceStart=$null;sourceEnd=$null;runtimeBeforeStart=$null;runtimeBeforeEnd=$null;controlsStart=$null;controlsEnd=$null;runtimeAfterStart=$null;runtimeAfterEnd=$null;finalGuardStart=$null;finalGuardEnd=$null;outputStart=$null}
$script:guardEvidence=$null
$controlProgress=@{case=$null;assertion=0;completed=@()}
$fixtureState=@{native=@();pathUnchanged=$false;ofsUnchanged=$false;writerRestored=$false;cleaned=$false;complete=$false};$node=$null;$nodeBefore=$null;$nodeAfter=$null;$ownershipSafe=$true
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
 $allowed=@('controls.ps1','expected-controls.json','lifetime-observer.mjs','native-controls.ps1','original-native-controls.ps1','original-owned.cs','original-synthetic-child.mjs','owned-lifetime.cs','payload-manifest.json','run-controls.ps1','setup-only.ps1','synthetic-child.mjs')
 if($Id -notin @('manifest-read','inventory-read','payload-inventory','payload-name','payload-read','payload-hash','complete')){$Id='unknown'}
 $safeName=$null;$safeExpected=$null;$safeObserved=$null
 if($Name -cin $allowed){$safeName=$Name}
 if($Expected -cmatch '^[0-9a-f]{64}$'){$safeExpected=$Expected}
 if($Observed -cmatch '^[0-9a-f]{64}$'){$safeObserved=$Observed}
 return [ordered]@{schema='payload-guard-v1';id=$Id;file=$safeName;expectedSha256=$safeExpected;observedSha256=$safeObserved;missing=@();extraNameDigests=@();extraCount=0;directoryCount=0;inventoryTruncated=$false}
}
function Set-GuardInventory($Record,$Expected,$Actual,[int]$DirectoryCount) {
 $allowed=@('controls.ps1','expected-controls.json','lifetime-observer.mjs','native-controls.ps1','original-native-controls.ps1','original-owned.cs','original-synthetic-child.mjs','owned-lifetime.cs','payload-manifest.json','run-controls.ps1','setup-only.ps1','synthetic-child.mjs')
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
 if(('OwnedSetup' -as [type]) -or ('OwnedLifetime' -as [type])){throw 'native_type_preexisting'}
 $node=(Get-Command node.exe -CommandType Application -TotalCount 1).Source
 if($node -isnot [string] -or -not [IO.Path]::IsPathFullyQualified($node)){throw 'node_selection'}
 $nodeBefore=Hash $node
 if($nodeBefore -cne 'ba4e6d110e8c1592a1ecd390f6b05f3da124b13871a5be62b341a07a853c6c32'){throw 'node_binary_mismatch'}
 $phase='runtime-before';$timing.runtimeBeforeStart=$watch.ElapsedMilliseconds;$before=Runtime;$timing.runtimeBeforeEnd=$watch.ElapsedMilliseconds
 $phase='controls';$timing.controlsStart=$watch.ElapsedMilliseconds;$invoked=$true
 $captured=@(& (Join-Path $PSScriptRoot 'controls.ps1') -Progress $controlProgress -Node $node -FixtureRoot (Join-Path $EvidenceRoot 'lifetime-fixture') -Evidence $fixtureState)
 if($fixtureState.complete -isnot [bool] -or -not $fixtureState.complete -or -not $fixtureState.pathUnchanged -or -not $fixtureState.ofsUnchanged -or -not $fixtureState.writerRestored -or -not $fixtureState.cleaned){throw 'fixture_incomplete'}
 if($captured.Count -ne 1 -or $captured[0] -isnot [string] -or [Text.Encoding]::UTF8.GetByteCount($captured[0]) -gt 65536){throw 'control_output_shape'}
 $parsed=@($captured[0]|ConvertFrom-Json)
 $expected=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-controls.json')|ConvertFrom-Json)
 if($expected.Count -ne 8 -or $parsed.Count -ne 8){throw 'control_count'}
 for($i=0;$i -lt 8;$i++){
  $item=$parsed[$i];$keys=@($item.PSObject.Properties.Name|Sort-Object)
  if(($keys-join '|') -cne 'name|outcome' -or $item.name -cne $expected[$i] -or $item.outcome -cne 'PASS'){throw 'control_identity_or_outcome'}
  $rows+=@{name=$expected[$i];outcome='PASS'}
 }
 $timing.controlsEnd=$watch.ElapsedMilliseconds
 $phase='runtime-after';$timing.runtimeAfterStart=$watch.ElapsedMilliseconds;$after=Runtime;$timing.runtimeAfterEnd=$watch.ElapsedMilliseconds
 if($before.hostSha256 -cne $after.hostSha256 -or $before.version -cne $after.version -or $before.pid -ne $after.pid -or $before.birthUtcTicks -cne $after.birthUtcTicks){throw 'runtime_changed'}
 foreach($a in $before.assemblies){$match=@($after.assemblies|Where-Object {$_.name -ceq $a.name -and $_.version -ceq $a.version -and $_.sha256 -ceq $a.sha256});if($match.Count -ne 1){throw 'assembly_changed'}}
 $phase='final-guard';$timing.finalGuardStart=$watch.ElapsedMilliseconds;Guard;$sourceAfter=$true;$timing.finalGuardEnd=$watch.ElapsedMilliseconds
 if('OwnedSetup' -as [type]){throw 'production_native_type_loaded'}
 if(-not ('OwnedLifetime' -as [type]) -or -not [OwnedLifetime]::OwnershipSafe){throw 'native_ownership_unresolved'}
 $nodeAfter=Hash $node;if($nodeAfter -cne $nodeBefore){throw 'node_changed'}
 # Post-return latency acceptance only. Host step timeout is the external interruption bound.
 if($watch.ElapsedMilliseconds -gt 120000){throw 'controls_elapsed_bound'}
 $phase='complete';$outcome='PASS';$category='None'
} catch {
 $hresult=[int]$_.Exception.HResult
 $candidate=[string]$_.CategoryInfo.Category
 if($candidate -in @('NotSpecified','InvalidOperation','InvalidData','ObjectNotFound','ResourceUnavailable','PermissionDenied','WriteError','ReadError','OperationStopped','ParserError','SyntaxError','InvalidArgument')){$category=$candidate}
} finally {
 [Console]::SetError($savedError)
 try {
  foreach($key in @($timing.Keys)){if($null -ne $timing[$key] -and ($timing[$key] -lt 0 -or $timing[$key] -gt 300000)){$timing[$key]=$null}}
  $outputStart=$watch.ElapsedMilliseconds;$timing.outputStart=$(if($outputStart -ge 0 -and $outputStart -le 300000){$outputStart}else{$null})
  $progressSafe=[ordered]@{valid=$false;case=$null;assertion=$null;completed=@()}
  $allowedCases=@('exact-check-pass-and-refusal','assertion-redaction-and-no-normalization','installed-writer-fault-preserves-refusal','check-expression-and-catch-composition','exact-one-option-source-reversal','native-parent-retirement-and-owned-descendant','default-vs-detached-discriminator','ownership-refusal-and-bounds-sensitivity')
  try {
   $cp=$controlProgress
   if($cp -isnot [hashtable] -or (($cp.Keys|Sort-Object)-join '|') -cne 'assertion|case|completed'){throw 'progress_shape'}
   if($null -ne $cp.case -and ($cp.case -isnot [string] -or $cp.case -cnotin $allowedCases)){throw 'progress_case'}
   if($cp.assertion -isnot [int] -or $cp.assertion -lt 0 -or $cp.assertion -gt 32){throw 'progress_assertion'}
   if($cp.completed -isnot [object[]] -or $cp.completed.Rank -ne 1 -or $cp.completed.GetLowerBound(0) -ne 0 -or $cp.completed.Count -gt 8){throw 'progress_prefix'}
   for($j=0;$j -lt $cp.completed.Count;$j++){if($cp.completed[$j] -isnot [string] -or $cp.completed[$j] -cne $allowedCases[$j]){throw 'progress_identity'}}
   $progressSafe=[ordered]@{valid=$true;case=$cp.case;assertion=$cp.assertion;completed=@($cp.completed)}
  } catch { }
  $fixtureSafe=[ordered]@{valid=$false;pathUnchanged=$null;ofsUnchanged=$null;writerRestored=$null;cleaned=$null;complete=$null;native=@()}
  try {
   $keys=@('cleaned','complete','native','ofsUnchanged','pathUnchanged','writerRestored')
   if($fixtureState -isnot [hashtable] -or (($fixtureState.Keys|Sort-Object)-join '|') -cne ($keys-join '|')){throw 'fixture_schema'}
   foreach($key in @('cleaned','complete','ofsUnchanged','pathUnchanged','writerRestored')){if($fixtureState[$key] -isnot [bool]){throw 'fixture_type'};$fixtureSafe[$key]=$fixtureState[$key]}
   if($fixtureState.native -isnot [object[]] -or $fixtureState.native.Count -gt 4){throw 'native_prefix'}
   $nativeNames=@('candidate','original','refused','alias');$projected=@()
   for($i=0;$i -lt $fixtureState.native.Count;$i++){
    $entry=$fixtureState.native[$i];if($entry.name -cne $nativeNames[$i]){throw 'native_identity'}
    $r=$entry.receipt;$safe=[ordered]@{name=$nativeNames[$i]}
    if($null -eq $r -or $r.GetType().FullName -cne 'OwnedLifetime+Receipt'){throw 'receipt_type'}
    foreach($k in @('assigned','resumed','overflow','readerError','activeZero','rootSignalled','readerDone','childHandleBound','childInOwner','rootIdentityStable','childIdentityStable','retiredWithLiveChild','childSignalledAfter','childHandleClosed','pipeEof','observerLineReady')){if($r.$k -isnot [bool]){throw 'receipt_bool'};$safe[$k]=$r.$k}
    foreach($k in @('rootPid','childPid','observerRootPid','originalExit')){if($r.$k -isnot [uint32]){throw 'receipt_uint'};$safe[$k]=$r.$k}
    foreach($k in @('rootCreation','childCreation')){if($r.$k -isnot [string] -or $r.$k -cnotmatch '^[0-9]{1,20}$'){throw 'receipt_birth'};$safe[$k]=$r.$k}
    foreach($k in @('maximumActive','snapshots','observerLines')){if($r.$k -isnot [int] -or $r.$k -lt 0 -or $r.$k -gt 65536){throw 'receipt_count'};$safe[$k]=$r.$k}
    foreach($k in @('read','written','elapsedMs','retiredObservedMs')){if($r.$k -isnot [long] -or $r.$k -lt -1 -or $r.$k -gt 300000){throw 'receipt_number'};$safe[$k]=$r.$k}
    if($r.reason -cnotin @('none','completed','assignment_failed','deadline','output_cap','reader_error','setup_or_observation_error')){throw 'receipt_reason'};$safe.reason=$r.reason
    if($r.observerFailure -cnotin @('none','extra-output','line-shape','root-mismatch','child-root-alias','open-child','child-identity','handle-wait','root-exit')){throw 'observer_reason'};$safe.observerFailure=$r.observerFailure
    if($r.interventions.Count -gt 2 -or @($r.interventions|Where-Object {$_ -cnotin @('terminate_unresumed_process_handle','terminate_owned_job')}).Count -ne 0){throw 'receipt_interventions'};$safe.interventions=@($r.interventions)
    $projected+=@($safe)
   }
   $fixtureSafe.native=$projected;$fixtureSafe.valid=$true
  }catch{ }
  $ownershipSafe=$false
  try {if('OwnedLifetime' -as [type]){$ownershipSafe=[OwnedLifetime]::OwnershipSafe}else{$ownershipSafe=-not $invoked -or $fixtureState.native.Count -eq 0}}catch{}
  if($outcome -eq 'PASS' -and (-not $progressSafe.valid -or $progressSafe.completed.Count -ne 8 -or -not $fixtureSafe.valid -or $fixtureSafe.native.Count -ne 4 -or -not $ownershipSafe)){throw 'evidence_acceptance_incomplete'}
  $record=[ordered]@{schema='retired-parent-controls-v1';outcome=$outcome;phase=$phase;category=$category;hresult=$hresult;guard=$script:guardEvidence;timing=$timing;controlsInvoked=$invoked;progress=$progressSafe;fixture=$fixtureSafe;acceptedCount=$(if($outcome -eq 'PASS'){8}else{0});results=$(if($outcome -eq 'PASS'){$rows}else{@()});elapsedMs=$watch.ElapsedMilliseconds;sourceAfter=$sourceAfter;runtimeBefore=$before;runtimeAfter=$after;childCreationRoute='four-exclusive-owned-synthetic-invocations';markerAccepted=$(if($outcome -eq 'PASS'){4}else{0});lifetimeAccepted=$(if($outcome -eq 'PASS'){4}else{0});nodeBefore=$nodeBefore;nodeAfter=$nodeAfter;ownershipSafe=$ownershipSafe;wholeNativeSetupAccepted=0;historicalCleanupProven=$false;setupInvocations=0;behavioralInvocations=0}
  $json=$record|ConvertTo-Json -Depth 10
  if([Text.Encoding]::UTF8.GetByteCount($json) -gt 262144){throw 'evidence_cap'}
  [IO.File]::WriteAllText((Join-Path $EvidenceRoot 'result.json'),$json,[Text.UTF8Encoding]::new($false))
  $outputEnd=$watch.ElapsedMilliseconds
  if($outputStart -ge 0 -and $outputEnd -ge $outputStart -and $outputEnd -le 300000){[Console]::Out.WriteLine('RETIRED_PARENT_OUTPUT_MS '+$outputStart+' '+$outputEnd)}else{[Console]::Out.WriteLine('RETIRED_PARENT_OUTPUT_UNAVAILABLE')}
  # The next workflow action is gated on this finite ownership result; no child is launched here.
  if($env:GITHUB_OUTPUT){[IO.File]::AppendAllText($env:GITHUB_OUTPUT,('ownership_safe='+$ownershipSafe.ToString().ToLowerInvariant()+[Environment]::NewLine))}
  [Console]::Out.WriteLine('RETIRED_PARENT_CONTROLS '+$outcome)
 } catch {$outcome='FAILED';[Console]::Out.WriteLine('RETIRED_PARENT_CONTROLS EVIDENCE_UNAVAILABLE')}
}
if($outcome -ne 'PASS'){exit 1}

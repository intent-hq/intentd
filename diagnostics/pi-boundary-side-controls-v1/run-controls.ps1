param([Parameter(Mandatory)][string]$EvidenceRoot)
$ErrorActionPreference='Stop'
$watch=[Diagnostics.Stopwatch]::StartNew()
$timing=[ordered]@{sourceStart=$null;sourceEnd=$null;runtimeBeforeStart=$null;runtimeBeforeEnd=$null;sideStart=$null;sideEnd=$null;controlsStart=$null;controlsEnd=$null;runtimeAfterStart=$null;runtimeAfterEnd=$null;finalGuardStart=$null;finalGuardEnd=$null;outputStart=$null}
$script:guardEvidence=$null
$controlProgress=@{case=$null;assertion=0;completed=@();underlying=$null;binding=$null};$projectorLoaded=$false
$sideProgress=@{case=$null;assertion=0;completed=@()};$sideInvoked=$false;$sideValidated=$false;$sideRows=@()
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
 $allowed=@('capture-functions.ps1','controls.metadata-candidate.ps1','controls.ps1','expected-controls.json','expected-side-controls.json','identity-detail-helper.ps1','identity-detail-original.ps1','identity-projector.ps1','original-tool-provenance.ps1','payload-manifest.json','run-controls.ps1','selection-helper.ps1','setup-only.ps1','side-controls.ps1','tool-provenance.ps1')
 if($Id -notin @('manifest-read','inventory-read','payload-inventory','payload-name','payload-read','payload-hash','complete')){$Id='unknown'}
 $safeName=$null;$safeExpected=$null;$safeObserved=$null
 if($Name -cin $allowed){$safeName=$Name}
 if($Expected -cmatch '^[0-9a-f]{64}$'){$safeExpected=$Expected}
 if($Observed -cmatch '^[0-9a-f]{64}$'){$safeObserved=$Observed}
 return [ordered]@{schema='payload-guard-v1';id=$Id;file=$safeName;expectedSha256=$safeExpected;observedSha256=$safeObserved;missing=@();extraNameDigests=@();extraCount=0;directoryCount=0;inventoryTruncated=$false}
}
function Set-GuardInventory($Record,$Expected,$Actual,[int]$DirectoryCount) {
 $allowed=@('capture-functions.ps1','controls.metadata-candidate.ps1','controls.ps1','expected-controls.json','expected-side-controls.json','identity-detail-helper.ps1','identity-detail-original.ps1','identity-projector.ps1','original-tool-provenance.ps1','payload-manifest.json','run-controls.ps1','selection-helper.ps1','setup-only.ps1','side-controls.ps1','tool-provenance.ps1')
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
 . (Join-Path $PSScriptRoot 'identity-projector.ps1');$projectorLoaded=$true
 $phase='runtime-before';$timing.runtimeBeforeStart=$watch.ElapsedMilliseconds;$before=Runtime;$timing.runtimeBeforeEnd=$watch.ElapsedMilliseconds
 $phase='side-controls';$timing.sideStart=$watch.ElapsedMilliseconds;$sideInvoked=$true
 $sideCaptured=@(& (Join-Path $PSScriptRoot 'side-controls.ps1') -Progress $sideProgress)
 if($sideCaptured.Count -ne 1 -or $sideCaptured[0] -isnot [string] -or [Text.Encoding]::UTF8.GetByteCount($sideCaptured[0]) -gt 65536){throw 'side_output_shape'}
 $sideParsed=@($sideCaptured[0]|ConvertFrom-Json)
 $sideExpected=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-side-controls.json')|ConvertFrom-Json)
 if($sideExpected.Count -ne 6 -or $sideParsed.Count -ne 6){throw 'side_count'}
 for($i=0;$i -lt 6;$i++){
  $item=$sideParsed[$i];$keys=@($item.PSObject.Properties.Name|Sort-Object)
  if(($keys-join '|') -cne 'name|outcome' -or $item.name -cne $sideExpected[$i] -or $item.outcome -cne 'PASS'){throw 'side_identity_or_outcome'}
  $sideRows+=@{name=$sideExpected[$i];outcome='PASS'}
 }
 $sideValidated=$true;$timing.sideEnd=$watch.ElapsedMilliseconds
 $phase='controls';$timing.controlsStart=$watch.ElapsedMilliseconds;$invoked=$true
 $captured=@(& (Join-Path $PSScriptRoot 'controls.ps1') -Progress $controlProgress)
 if($captured.Count -ne 1 -or $captured[0] -isnot [string] -or [Text.Encoding]::UTF8.GetByteCount($captured[0]) -gt 65536){throw 'control_output_shape'}
 $parsed=@($captured[0]|ConvertFrom-Json)
 $expected=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-controls.json')|ConvertFrom-Json)
 if($expected.Count -ne 7 -or $parsed.Count -ne 7){throw 'control_count'}
 for($i=0;$i -lt 7;$i++){
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
 # Post-return latency acceptance only. Host step timeout is the external interruption bound.
 if($watch.ElapsedMilliseconds -gt 45000){throw 'controls_elapsed_bound'}
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
  $progressSafe=@{valid=$false;case=$null;assertion=$null;completed=@()}
  $allowedCases=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-controls.json')|ConvertFrom-Json)
  $done=@($controlProgress.completed);$prefix=$done.Count -le 7
  for($i=0;$i -lt $done.Count -and $prefix;$i++){if($done[$i] -isnot [string] -or $done[$i] -cne $allowedCases[$i]){$prefix=$false}}
  if($prefix -and ($null -eq $controlProgress.case -or $controlProgress.case -cin $allowedCases) -and $controlProgress.assertion -is [int] -and $controlProgress.assertion -ge 0 -and $controlProgress.assertion -le 33){$progressSafe=@{valid=$true;case=$controlProgress.case;assertion=$controlProgress.assertion;completed=$done}}
  $sideProgressSafe=@{valid=$false;case=$null;assertion=$null;completed=@()}
  $sideAllowed=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-side-controls.json')|ConvertFrom-Json)
  $sideDone=@($sideProgress.completed);$sidePrefix=$sideDone.Count -le 6
  for($i=0;$i -lt $sideDone.Count -and $sidePrefix;$i++){if($sideDone[$i] -isnot [string] -or $sideDone[$i] -cne $sideAllowed[$i]){$sidePrefix=$false}}
  if($sidePrefix -and ($null -eq $sideProgress.case -or $sideProgress.case -cin $sideAllowed) -and $sideProgress.assertion -is [int] -and $sideProgress.assertion -ge 0 -and $sideProgress.assertion -le 24){$sideProgressSafe=@{valid=$true;case=$sideProgress.case;assertion=$sideProgress.assertion;completed=$sideDone}}
  $identitySafe=@{classification='incomplete';detail=$null}
  if($projectorLoaded){$identitySafe=Project-IdentityDetail $controlProgress.underlying $controlProgress.binding}
  $record=[ordered]@{schema='boundary-side-controls-v1';outcome=$outcome;phase=$phase;category=$category;hresult=$hresult;guard=$script:guardEvidence;controlProgress=$progressSafe;sideProgress=$sideProgressSafe;sideControlsInvoked=$sideInvoked;sideControlsValidated=$sideValidated;sideAcceptedCount=$(if($outcome -eq 'PASS'){6}else{0});sideResults=$(if($outcome -eq 'PASS'){$sideRows}else{@()});underlyingEightAccepted=0;identityComparison=$identitySafe;timing=$timing;controlsInvoked=$invoked;acceptedCount=$(if($outcome -eq 'PASS'){7}else{0});results=$(if($outcome -eq 'PASS'){$rows}else{@()});elapsedMs=$watch.ElapsedMilliseconds;sourceAfter=$sourceAfter;runtimeBefore=$before;runtimeAfter=$after;childCreationRoute='none-in-reviewed-controls';historicalCleanupProven=$false;setupInvocations=0;behavioralInvocations=0}
  $json=$record|ConvertTo-Json -Depth 10
  if([Text.Encoding]::UTF8.GetByteCount($json) -gt 262144){throw 'evidence_cap'}
  [IO.File]::WriteAllText((Join-Path $EvidenceRoot 'result.json'),$json,[Text.UTF8Encoding]::new($false))
  $outputEnd=$watch.ElapsedMilliseconds
  if($outputStart -ge 0 -and $outputEnd -ge $outputStart -and $outputEnd -le 180000){[Console]::Out.WriteLine('BOUNDARY_SIDE_OUTPUT_MS '+$outputStart+' '+$outputEnd)}else{[Console]::Out.WriteLine('BOUNDARY_SIDE_OUTPUT_UNAVAILABLE')}
  [Console]::Out.WriteLine('BOUNDARY_SIDE_CONTROLS '+$outcome)
 } catch {$outcome='FAILED';[Console]::Out.WriteLine('BOUNDARY_SIDE_CONTROLS EVIDENCE_UNAVAILABLE')}
}
if($outcome -ne 'PASS'){exit 1}

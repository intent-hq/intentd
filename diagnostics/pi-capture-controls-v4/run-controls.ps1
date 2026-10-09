param([Parameter(Mandatory)][string]$EvidenceRoot)
$ErrorActionPreference='Stop'
$watch=[Diagnostics.Stopwatch]::StartNew()
$script:guardEvidence=$null;$guardInvoked=$false;$guardValidated=$false;$guardRows=@()
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
 $allowed=@('capture-functions.ps1','controls.ps1','expected-controls.json','original-setup-only.ps1','original-upload-policy.mjs','payload-manifest.json','run-controls.ps1','setup-only.ps1','guard-controls.ps1','guard-functions.ps1','original-guard.ps1','expected-guard-controls.json')
 if($Id -notin @('manifest-read','inventory-read','payload-inventory','payload-name','payload-read','payload-hash','complete')){$Id='unknown'}
 $safeName=$null;$safeExpected=$null;$safeObserved=$null
 if($Name -cin $allowed){$safeName=$Name}
 if($Expected -cmatch '^[0-9a-f]{64}$'){$safeExpected=$Expected}
 if($Observed -cmatch '^[0-9a-f]{64}$'){$safeObserved=$Observed}
 return [ordered]@{schema='payload-guard-v1';id=$Id;file=$safeName;expectedSha256=$safeExpected;observedSha256=$safeObserved;missing=@();extraNameDigests=@();extraCount=0;directoryCount=0;inventoryTruncated=$false}
}
function Set-GuardInventory($Record,$Expected,$Actual,[int]$DirectoryCount) {
 $allowed=@('capture-functions.ps1','controls.ps1','expected-controls.json','original-setup-only.ps1','original-upload-policy.mjs','payload-manifest.json','run-controls.ps1','setup-only.ps1','guard-controls.ps1','guard-functions.ps1','original-guard.ps1','expected-guard-controls.json')
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
 $phase='guard-controls';$guardInvoked=$true
 $guardCaptured=@(& (Join-Path $PSScriptRoot 'guard-controls.ps1'))
 if($guardCaptured.Count -ne 1 -or $guardCaptured[0] -isnot [string] -or [Text.Encoding]::UTF8.GetByteCount($guardCaptured[0]) -gt 65536){throw 'guard_control_output_shape'}
 $guardParsed=@($guardCaptured[0]|ConvertFrom-Json)
 $guardExpected=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-guard-controls.json')|ConvertFrom-Json)
 if($guardExpected.Count -ne 14 -or $guardParsed.Count -ne 14){throw 'guard_control_count'}
 for($i=0;$i -lt 14;$i++){
  $item=$guardParsed[$i];$keys=@($item.PSObject.Properties.Name|Sort-Object)
  if(($keys-join '|') -cne 'name|outcome' -or $item.name -cne $guardExpected[$i] -or $item.outcome -cne 'PASS'){throw 'guard_control_identity_or_outcome'}
  $guardRows+=@{name=$guardExpected[$i];outcome='PASS'}
 }
 $guardValidated=$true
 $phase='controls';$invoked=$true
 $captured=@(& (Join-Path $PSScriptRoot 'controls.ps1'))
 if($captured.Count -ne 1 -or $captured[0] -isnot [string] -or [Text.Encoding]::UTF8.GetByteCount($captured[0]) -gt 65536){throw 'control_output_shape'}
 $parsed=@($captured[0]|ConvertFrom-Json)
 $expected=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-controls.json')|ConvertFrom-Json)
 if($expected.Count -ne 14 -or $parsed.Count -ne 14){throw 'control_count'}
 for($i=0;$i -lt 14;$i++){
  $item=$parsed[$i];$keys=@($item.PSObject.Properties.Name|Sort-Object)
  if(($keys-join '|') -cne 'name|outcome' -or $item.name -cne $expected[$i] -or $item.outcome -cne 'PASS'){throw 'control_identity_or_outcome'}
  $rows+=@{name=$expected[$i];outcome='PASS'}
 }
 $phase='runtime-after';$after=Runtime
 if($before.hostSha256 -cne $after.hostSha256 -or $before.version -cne $after.version -or $before.pid -ne $after.pid -or $before.birthUtcTicks -cne $after.birthUtcTicks){throw 'runtime_changed'}
 foreach($a in $before.assemblies){$match=@($after.assemblies|Where-Object {$_.name -ceq $a.name -and $_.version -ceq $a.version -and $_.sha256 -ceq $a.sha256});if($match.Count -ne 1){throw 'assembly_changed'}}
 $phase='final-guard';Guard;$sourceAfter=$true
 if('OwnedSetup' -as [type]){throw 'native_type_loaded'}
 # Post-return latency acceptance only. Host step timeout is the external interruption bound.
 if($watch.ElapsedMilliseconds -gt 10000){throw 'controls_elapsed_bound'}
 $phase='complete';$outcome='PASS';$category='None'
} catch {
 $hresult=[int]$_.Exception.HResult
 $candidate=[string]$_.CategoryInfo.Category
 if($candidate -in @('NotSpecified','InvalidOperation','InvalidData','ObjectNotFound','ResourceUnavailable','PermissionDenied','WriteError','ReadError','OperationStopped','ParserError','SyntaxError','InvalidArgument')){$category=$candidate}
} finally {
 [Console]::SetError($savedError)
 try {
  $record=[ordered]@{schema='capture-controls-v2';guardControlsInvoked=$guardInvoked;guardControlsValidated=$guardValidated;acceptedGuardCount=$(if($outcome -eq 'PASS'){14}else{0});guardResults=$guardRows;guard=$script:guardEvidence;outcome=$outcome;phase=$phase;category=$category;hresult=$hresult;controlsInvoked=$invoked;acceptedCount=$(if($outcome -eq 'PASS'){14}else{0});results=$(if($outcome -eq 'PASS'){$rows}else{@()});elapsedMs=$watch.ElapsedMilliseconds;sourceAfter=$sourceAfter;runtimeBefore=$before;runtimeAfter=$after;childCreationRoute='none-in-reviewed-controls';historicalCleanupProven=$false;setupInvocations=0;behavioralInvocations=0}
  $json=$record|ConvertTo-Json -Depth 10
  if([Text.Encoding]::UTF8.GetByteCount($json) -gt 262144){throw 'evidence_cap'}
  [IO.File]::WriteAllText((Join-Path $EvidenceRoot 'result.json'),$json,[Text.UTF8Encoding]::new($false))
  [Console]::Out.WriteLine('CAPTURE_CONTROLS_V2 '+$outcome)
 } catch {$outcome='FAILED';[Console]::Out.WriteLine('CAPTURE_CONTROLS_V2 EVIDENCE_UNAVAILABLE')}
}
if($outcome -ne 'PASS'){exit 1}

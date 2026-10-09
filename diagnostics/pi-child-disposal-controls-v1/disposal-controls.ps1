param([Parameter(Mandatory)][hashtable]$Progress,[Parameter(Mandatory)][hashtable]$Evidence,[Parameter(Mandatory)][string]$Node)
$ErrorActionPreference='Stop';$rows=[Collections.Generic.List[object]]::new();$savedWriter=[Console]::Error;$savedPath=$env:PATH;$savedOFS=$OFS
$Evidence.probes=@();$Evidence.writerRestored=$false;$Evidence.pathUnchanged=$false;$Evidence.ofsUnchanged=$false;$Evidence.complete=$false
function Need-D([bool]$Ok,[int]$Id){$Progress.assertion=$Id;if(-not $Ok){throw ('disposal_control_'+$Id)}}
function Start-D([string]$Name){$Progress.case=$Name;$Progress.assertion=0}
function Finish-D {$rows.Add(@{name=$Progress.case;outcome='PASS'});$Progress.completed=@($Progress.completed)+@($Progress.case)}
function Extract-D([string]$File,[string]$Name){$t=$null;$e=$null;$a=[Management.Automation.Language.Parser]::ParseFile((Join-Path $PSScriptRoot $File),[ref]$t,[ref]$e);Need-D ($e.Count -eq 0) 1;$f=@($a.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $Name},$true));Need-D ($f.Count -eq 1) 2;return $f[0].Extent.Text}
Invoke-Expression (Extract-D 'run-controls.ps1' 'Project-ChildFinalWait')
Invoke-Expression (Extract-D 'run-controls.ps1' 'Project-DisposalProbe')
function Project-D($Value){$o=@(Project-ChildFinalWait $Value);Need-D ($o.Count -eq 1 -and (($o[0].Keys|Sort-Object)-join '|') -ceq 'attempted|error|handlePresent|kind|result|returned|stage|valid') 3;Need-D ([Text.Encoding]::UTF8.GetByteCount(($o[0]|ConvertTo-Json -Compress)) -le 512) 4;return $o[0]}
function Budget-Oracle([uint32]$Actual,[uint32]$Expected){Need-D ($Actual -eq $Expected) 30}
function Accept-D($R){return ($R.state.childSignalledAfter -and $R.state.childIdentityStable -and $R.state.childHandleClosed -and $null -eq $R.error)}
try {
 Start-D 'exact-reversal-and-held-owner-contract'
 $old=Get-Content -Raw (Join-Path $PSScriptRoot 'before-disposal-owned.cs');$new=Get-Content -Raw (Join-Path $PSScriptRoot 'owned-lifetime.cs');$delta=Get-Content -Raw (Join-Path $PSScriptRoot 'disposal-source-delta.json')|ConvertFrom-Json;$back=$new
 for($i=$delta.changes.Count-1;$i -ge 0;$i--){$c=$delta.changes[$i];Need-D (([regex]::Matches($back,[regex]::Escape($c.new))).Count -eq 1) 5;$back=$back.Replace($c.new,$c.old)}
 Need-D ($delta.changes.Count -eq 3 -and $back -ceq $old) 6
 foreach($token in @('WaitForSingleObject(','TerminateJobObject(','TerminateProcess(','OpenProcess(','CloseHandle(','GetProcessTimes(','GetProcessId(','IsProcessInJob(','Members(','Thread.Sleep(')){Need-D (([regex]::Matches($old,[regex]::Escape($token))).Count -eq ([regex]::Matches($new,[regex]::Escape($token))).Count) 7}
 $start=$old.IndexOf(' public sealed class OwnershipLatch');$end=$old.IndexOf(' [StructLayout');Need-D ($new.Contains($old.Substring($start,$end-$start)) -and $new.Contains('r.childSignalledAfter=childWaitResult==0;')) 8
 Need-D (-not ('DisposalModel' -as [type]) -and -not ('DisposalProbe' -as [type]) -and -not ('OwnedLifetime' -as [type]) -and -not ('OwnedSetup' -as [type])) 9
 Add-Type -Path (Join-Path $PSScriptRoot 'disposal-model.cs');$injected=[InvalidOperationException]::new('injected-disposal-error');Finish-D
 Start-D 'remaining-budget-and-no-restart'
 foreach($edge in @(@([long]0,[uint32]10000),@([long]1234,[uint32]8766),@([long]9999,[uint32]1),@([long]10000,[uint32]0),@([long]10001,[uint32]0),@([long]::MaxValue,[uint32]0))){
  $r=[DisposalModel]::Run($true,[long[]]@($edge[0],0,0,0),0,'delayed','none',$injected)
  Need-D ($null -eq $r.error -and $r.waits -eq 1 -and $r.requested -eq $edge[1] -and $r.charged -eq $edge[0] -and $r.contextRestored -and $r.cacheRestored) 10
 }
 $r=[DisposalModel]::Run($true,[long[]]@(6000,1000,1000,1234),900,'delayed','none',$injected)
 Need-D ($r.requested -eq 766 -and $r.returned -eq 258 -and -not (Accept-D $r) -and ($r.ledger -join '|') -ceq 'clock-start|charge:termination-members:6000|charge:root:1000|charge:reader:1000|charge:dispose:1234|wait:766|identity|pid|close') 11
 # Constant allowance/restarted clock/unsigned underflow disagree with this exact argument oracle.
 Budget-Oracle $r.requested 766
 foreach($variant in @(@('constant',[long]9234,[uint32]766),@('restart-after-members',[long]9234,[uint32]766),@('unsigned-underflow',[long]10001,[uint32]0))){$bad=[DisposalModel]::MutantBudget($variant[1],$variant[0]);$caught=$null;try{Budget-Oracle $bad $variant[2]}catch{$caught=$_};Need-D ($null -ne $caught -and $caught.Exception.Message -ceq 'disposal_control_30') 12}
 $expired=[DisposalModel]::Run($true,[long[]]@(10000,1000,1000,0),1,'delayed','none',$injected);Need-D ($expired.waits -eq 1 -and $expired.requested -eq 0 -and $expired.returned -eq 258 -and -not (Accept-D $expired)) 13
 $already=[DisposalModel]::Run($true,[long[]]@(10000,1000,1000,0),0,'delayed','none',$injected);Need-D ($already.waits -eq 1 -and $already.requested -eq 0 -and (Accept-D $already)) 14;Finish-D
 Start-D 'delayed-signal-versus-never-and-failed'
 $a=[DisposalModel]::Run($false,[long[]]@(9000,100,100,0),50,'delayed','none',$injected);$b=[DisposalModel]::Run($true,[long[]]@(9000,100,100,0),50,'delayed','none',$injected)
 Need-D ($a.requested -eq 0 -and $a.returned -eq 258 -and -not (Accept-D $a) -and $b.requested -eq 800 -and $b.returned -eq 0 -and (Accept-D $b)) 15
 foreach($mode in @('never','failed','other')){
  $r=[DisposalModel]::Run($true,[long[]]@(9000,100,100,0),50,$mode,'none',$injected);$q=Project-D $r.state
  $code=if($mode -ceq 'never'){[uint32]258}elseif($mode -ceq 'failed'){[uint32]::MaxValue}else{[uint32]128};$kind=if($mode -ceq 'never'){'timeout'}elseif($mode -ceq 'failed'){'failed'}else{'other'}
  Need-D ($r.waits -eq 1 -and $r.requested -eq 800 -and $r.returned -eq $code -and -not (Accept-D $r) -and $q.valid -and $q.result -eq $code -and $q.kind -ceq $kind -and $q.error -eq $(if($mode -ceq 'failed'){6}else{0})) 16
  $tail=if($mode -ceq 'failed'){'wait:800|cached-error|identity|pid|close'}else{'wait:800|identity|pid|close'};$tailCount=if($mode -ceq 'failed'){5}else{4};$tailItems=@($r.ledger|Select-Object -Last $tailCount);Need-D (($tailItems -join '|') -ceq $tail) 17
  $r.state.childSignalledAfter=$true;$bad=Project-D $r.state;Need-D (-not $bad.valid) 18
 };Finish-D
 Start-D 'identity-cleanup-and-fault-boundaries'
 foreach($fault in @('wrong-birth','wrong-pid','close-false')){$r=[DisposalModel]::Run($true,[long[]]@(0,0,0,0),0,'delayed',$fault,$injected);Need-D ($r.state.childSignalledAfter -and -not (Accept-D $r) -and $r.waits -eq 1) 19}
 foreach($site in @('wait','identity','close')){
  $a=[DisposalModel]::Run($false,[long[]]@(0,0,0,0),0,'delayed',$site,$injected);$b=[DisposalModel]::Run($true,[long[]]@(0,0,0,0),0,'delayed',$site,$injected)
  Need-D ([object]::ReferenceEquals($a.error,$injected) -and [object]::ReferenceEquals($b.error,$injected) -and $a.waits -eq 1 -and $b.waits -eq 1 -and $a.requested -eq 0 -and $b.requested -eq 10000 -and $a.cacheRestored -and $b.cacheRestored -and $a.contextRestored -and $b.contextRestored) 20
 }
 foreach($fault in @('pre','post')){$r=[DisposalModel]::Run($true,[long[]]@(0,0,0,0),0,'delayed',$fault,$injected);$q=Project-D $r.state;Need-D ((Accept-D $r) -and -not $q.valid -and $r.cacheRestored -and $r.contextRestored) 21}
 $fresh=Project-D ([DisposalModel+State]::new());Need-D ($fresh.valid -and $fresh.kind -ceq 'unavailable' -and $null -eq $fresh.result -and $null -eq $fresh.error) 22;Finish-D
 Start-D 'owned-native-delayed-completion-discriminator'
 Add-Type -Path (Join-Path $PSScriptRoot 'disposal-probe.cs')
 foreach($delayed in @($true,$false)){
  Need-D ([DisposalProbe]::OwnershipSafe) 23;$n=[DisposalProbe]::Run($Node,$delayed);$Evidence.probes+=@($n)
  foreach($key in @('created','assigned','inJob','neverResumed','initialReturned','workerReady','workerDone','workerJoined','testedReturned','terminationAttempted','terminationSucceeded','processSignalled','identityStable','processClosed','threadClosed','jobClosed','eventsClosed','cacheRestored','complete')){Need-D ($n.$key -is [bool] -and $n.$key) 24}
  Need-D (-not $n.workerFailed -and $n.terminationCount -eq 1 -and $n.initialWait -eq 258 -and $n.initialError -eq 0 -and $n.testedWait -eq $(if($delayed){0}else{258}) -and $n.testedError -eq 0 -and $n.cleanupWait -eq 0 -and $n.cleanupError -eq 0 -and [DisposalProbe]::OwnershipSafe) 25
  Need-D ($n.mode -ceq $(if($delayed){'delayed'}else{'withheld'}) -and $n.phase -ceq 'complete' -and $n.testedElapsedMs -ge $(if($delayed){20}else{0})) 26
  $projected=@(Project-DisposalProbe $n);Need-D ($projected.Count -eq 1 -and $projected[0].valid -and $projected[0].Keys.Count -eq 31 -and [Text.Encoding]::UTF8.GetByteCount(($projected[0]|ConvertTo-Json -Compress)) -le 1024) 26
  foreach($mutation in @(@('mode','private-value'),@('phase','private-value'),@('terminationCount',[int]2),@('testedElapsedMs',[long]-1),@('testedElapsedMs',[long]300001))){$key=$mutation[0];$saved=$n.$key;try{$n.$key=$mutation[1];$bad=Project-DisposalProbe $n;Need-D (-not $bad.valid -and $bad.Keys.Count -eq 1 -and -not ($bad|ConvertTo-Json -Compress).Contains('private-value')) 26}finally{$n.$key=$saved}}
  $bad=Project-DisposalProbe ([pscustomobject]@{phase='complete';mode='delayed'});Need-D (-not $bad.valid -and $bad.Keys.Count -eq 1) 26

 };Finish-D
 Start-D 'affected-whole-eight-and-fixed-evidence'
 $current=Get-Content -Raw (Join-Path $PSScriptRoot 'controls.ps1');$bound=Get-Content -Raw (Join-Path $PSScriptRoot 'bound-whole-controls.ps1');Need-D ($current -ceq $bound -and $current.Contains('$candidateReceipt.childSignalledAfter')) 27
 Need-D (-not ('OwnedLifetime' -as [type]) -and -not ('OwnedSetup' -as [type]) -and [DisposalProbe]::OwnershipSafe -and $Evidence.probes.Count -eq 2) 28;Finish-D
}finally{[Console]::SetError($savedWriter);$Evidence.writerRestored=[object]::ReferenceEquals([Console]::Error,$savedWriter);$Evidence.pathUnchanged=$env:PATH -ceq $savedPath;$Evidence.ofsUnchanged=$OFS -ceq $savedOFS}
Need-D ($rows.Count -eq 6 -and $Evidence.writerRestored -and $Evidence.pathUnchanged -and $Evidence.ofsUnchanged -and [DisposalProbe]::OwnershipSafe) 29
$Evidence.complete=$true;ConvertTo-Json -InputObject @($rows.ToArray()) -Compress -Depth 4

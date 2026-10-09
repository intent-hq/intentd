param([Parameter(Mandatory)][hashtable]$Progress,[Parameter(Mandatory)][hashtable]$Evidence,[Parameter(Mandatory)][string]$Node)
$ErrorActionPreference='Stop';$rows=[Collections.Generic.List[object]]::new();$savedWriter=[Console]::Error;$savedPath=$env:PATH;$savedOFS=$OFS
$Evidence.probe=$null;$Evidence.writerRestored=$false;$Evidence.pathUnchanged=$false;$Evidence.ofsUnchanged=$false;$Evidence.complete=$false
function Need-W([bool]$Condition,[int]$Id){$Progress.assertion=$Id;if(-not $Condition){throw ('wait_control_'+$Id)}}
function Start-W([string]$Name){$Progress.case=$Name;$Progress.assertion=0}
function Finish-W {$rows.Add(@{name=$Progress.case;outcome='PASS'});$Progress.completed=@($Progress.completed)+@($Progress.case)}
function Extract-W([string]$File,[string]$Name){
 $tokens=$null;$errors=$null;$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $PSScriptRoot $File),[ref]$tokens,[ref]$errors)
 Need-W ($errors.Count -eq 0) 1;$found=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $Name},$true));Need-W ($found.Count -eq 1) 2;return $found[0].Extent.Text
}
Invoke-Expression (Extract-W 'run-controls.ps1' 'Project-ChildFinalWait')
function State-W([uint32]$Code,[int]$Error){
 $r=[WaitDiagnosticModel+State]::new();$r.childWaitStage='after-reader-before-child-close';$r.childWaitAttempted=$true;$r.childWaitHandlePresent=$true;$r.childWaitReturned=$true;$r.childWaitResult=$Code;$r.childWaitError=$Error;$r.childSignalledAfter=$Code -eq 0;return $r
}
function Project-W($Value){
 $captured=@(Project-ChildFinalWait $Value);Need-W ($captured.Count -eq 1) 3;$r=$captured[0]
 Need-W ((($r.Keys|Sort-Object)-join '|') -ceq 'attempted|error|handlePresent|kind|result|returned|stage|valid') 4
 Need-W ([Text.Encoding]::UTF8.GetByteCount(($r|ConvertTo-Json -Compress)) -le 512) 5;return $r
}
function Need-Invalid($Value){$r=Project-W $Value;Need-W (-not $r.valid -and $r.stage -ceq 'unknown' -and $r.kind -ceq 'unavailable' -and $null -eq $r.result -and $null -eq $r.error) 6}
function Copy-W($Value){$h=@{};foreach($key in @('childWaitAttempted','childWaitReturned','childWaitHandlePresent','childSignalledAfter','childWaitResult','childWaitError','childWaitStage')){$h[$key]=$Value.$key};return $h}
try {
 Start-W 'same-wait-expression-and-original-owner-preservation'
 $old=Get-Content -Raw (Join-Path $PSScriptRoot 'before-wait-owned.cs');$new=Get-Content -Raw (Join-Path $PSScriptRoot 'owned-lifetime.cs');$delta=Get-Content -Raw (Join-Path $PSScriptRoot 'wait-source-delta.json')|ConvertFrom-Json
 $reversed=$new
 for($i=$delta.ownerChanges.Count-1;$i -ge 0;$i--){$edit=$delta.ownerChanges[$i];Need-W (([regex]::Matches($reversed,[regex]::Escape($edit.new))).Count -eq 1) 7;$reversed=$reversed.Replace($edit.new,$edit.old)}
 Need-W ($delta.ownerChanges.Count -eq 2 -and $reversed -ceq $old) 8
 foreach($token in @('WaitForSingleObject(','TerminateJobObject(','TerminateProcess(','OpenProcess(','CloseHandle(','GetProcessTimes(','GetProcessId(','IsProcessInJob(')){Need-W (([regex]::Matches($old,[regex]::Escape($token))).Count -eq ([regex]::Matches($new,[regex]::Escape($token))).Count) 9}
 Need-W (-not ('WaitDiagnosticModel' -as [type]) -and -not ('WaitDiagnosticProbe' -as [type]) -and -not ('OwnedLifetime' -as [type]) -and -not ('OwnedSetup' -as [type])) 10
 Add-Type -Path (Join-Path $PSScriptRoot 'wait-model.cs')
 Finish-W
 Start-W 'fixed-result-mapping-and-strict-negative-sensitivity'
 foreach($spec in @(@([uint32]0,[int]0,'signaled'),@([uint32]258,[int]0,'timeout'),@([uint32]::MaxValue,[int]6,'failed'),@([uint32]::MaxValue,[int]0,'failed'),@([uint32]128,[int]0,'other'),@([uint32]42,[int]0,'other'))){
  $state=State-W $spec[0] $spec[1];$q=Project-W $state
  Need-W ($q.valid -and $q.kind -ceq $spec[2] -and $q.result -eq $spec[0] -and $q.error -eq $spec[1] -and $q.returned -and $q.attempted -and $q.handlePresent) 11
 }
 $good=State-W 258 0
 foreach($mutation in @(@('childSignalledAfter',$true),@('childWaitError',[int]6),@('childWaitResult',[long]258),@('childWaitError',[long]0),@('childWaitStage','private-stage'),@('childWaitAttempted','true'),@('childWaitHandlePresent',$false),@('childWaitReturned',$false))){$bad=Copy-W $good;$bad[$mutation[0]]=$mutation[1];Need-Invalid $bad}
 foreach($key in @('childWaitAttempted','childWaitReturned','childWaitHandlePresent','childSignalledAfter','childWaitResult','childWaitError','childWaitStage')){$bad=Copy-W $good;$bad.Remove($key);Need-Invalid $bad}
 $extra=Copy-W $good;$extra['private']='private-extra-content';$q=Project-W $extra;$clean=Project-W $good;Need-W (($q|ConvertTo-Json -Compress) -ceq ($clean|ConvertTo-Json -Compress) -and -not ($q|ConvertTo-Json -Compress).Contains('private-extra-content')) 12
 $fault=[pscustomobject]@{};$fault|Add-Member -MemberType ScriptProperty -Name childWaitAttempted -Value {throw 'observer-getter-fault'};Need-Invalid $fault
 # Execute the exact wrapper cap expression, extracted as a single reviewed source line.
 $wrapper=Get-Content -Raw (Join-Path $PSScriptRoot 'run-controls.ps1');$capLine=@($wrapper -split '\r?\n'|Where-Object {$_ -like "*throw 'child_wait_evidence_cap'*"});Need-W ($capLine.Count -eq 1) 13;$capBlock=[ScriptBlock]::Create($capLine[0])
 $safe=@{finalChildWait=$clean};& $capBlock
 $safe=@{finalChildWait=@{stage=('x'*600)}};$caught=$null;try{& $capBlock}catch{$caught=$_};Need-W ($null -ne $caught -and $caught.Exception.Message -ceq 'child_wait_evidence_cap') 14
 Finish-W
 Start-W 'not-run-partial-and-freshness'
 $fresh=[WaitDiagnosticModel+State]::new();$q=Project-W $fresh;Need-W ($q.valid -and $q.stage -ceq 'not-run' -and $q.kind -ceq 'unavailable' -and $null -eq $q.result -and $null -eq $q.error -and -not $q.returned) 15
 $partial=[WaitDiagnosticModel+State]::new();$partial.childWaitStage='after-reader-before-child-close';$partial.childWaitAttempted=$true;$partial.childWaitHandlePresent=$true
 $q=Project-W $partial;Need-W ($q.valid -and $q.kind -ceq 'unavailable' -and $q.attempted -and -not $q.returned -and $null -eq $q.result) 16
 foreach($mutation in @(@('childWaitResult',[uint32]258),@('childWaitError',[int]6),@('childSignalledAfter',$true))){$bad=Copy-W $partial;$bad[$mutation[0]]=$mutation[1];Need-Invalid $bad}
 $q=Project-W (State-W ([uint32]::MaxValue) 6);Need-W ($q.error -eq 6 -and $q.kind -ceq 'failed') 17
 $q=Project-W ([WaitDiagnosticModel+State]::new());Need-W ($q.valid -and $null -eq $q.error -and $null -eq $q.result -and $q.kind -ceq 'unavailable') 18
 $round=$q|ConvertTo-Json -Compress|ConvertFrom-Json;Need-W ($null -eq $round.result -and $null -eq $round.error) 19
 Finish-W
 Start-W 'original-finalizer-transparency-and-bookkeeping-fault'
 $injected=[InvalidOperationException]::new('exact-injected-identity')
 foreach($code in @([uint32]0,[uint32]258,[uint32]::MaxValue,[uint32]128)){
  foreach($fault in @('none','pre','post')){
   $a=[WaitDiagnosticModel]::Run($false,$code,6,$fault,'none',$injected);$b=[WaitDiagnosticModel]::Run($true,$code,6,$fault,'none',$injected)
   Need-W ($null -eq $a.error -and $null -eq $b.error -and $a.state.childSignalledAfter -eq ($code -eq 0) -and $b.state.childSignalledAfter -eq $a.state.childSignalledAfter -and $a.state.childIdentityStable -and $b.state.childIdentityStable -and $a.state.childHandleClosed -and $b.state.childHandleClosed) 20
   Need-W (($a.ledger -join '|') -ceq 'wait:0|identity|pid|close') 21
   $expected=if($code -eq [uint32]::MaxValue){'wait:0|cached-error|identity|pid|close'}else{'wait:0|identity|pid|close'};Need-W (($b.ledger -join '|') -ceq $expected -and $a.cachedErrorRestored -and $b.cachedErrorRestored -and $a.contextRestored -and $b.contextRestored) 22
   if($fault -eq 'none'){$q=Project-W $b.state;Need-W ($q.valid -and $q.result -eq $code -and $q.error -eq $(if($code -eq [uint32]::MaxValue){6}else{0})) 23}
   elseif($fault -eq 'pre' -or $code -eq 0){Need-Invalid $b.state}else{$q=Project-W $b.state;Need-W ($q.valid -and $q.kind -ceq 'unavailable' -and -not $q.returned -and $null -eq $q.result -and $null -eq $q.error) 32}
  }
 }
 foreach($site in @('wait','identity','close')){
  $a=[WaitDiagnosticModel]::Run($false,258,6,'none',$site,$injected);$b=[WaitDiagnosticModel]::Run($true,258,6,'none',$site,$injected)
  Need-W ([object]::ReferenceEquals($a.error,$injected) -and [object]::ReferenceEquals($b.error,$injected) -and ($a.ledger -join '|') -ceq ($b.ledger -join '|') -and $a.state.childSignalledAfter -eq $b.state.childSignalledAfter -and $a.cachedErrorRestored -and $b.cachedErrorRestored) 24
 }
 Finish-W
 Start-W 'owned-native-wait-and-error-discrimination'
 Add-Type -Path (Join-Path $PSScriptRoot 'wait-native-probe.cs');Need-W ([WaitDiagnosticProbe]::OwnershipSafe) 25
 $probe=[WaitDiagnosticProbe]::Run($Node);$Evidence.probe=$probe
 Need-W ($probe.complete -and $probe.created -and $probe.assigned -and $probe.inJob -and $probe.neverResumed -and $probe.initialReturned -and $probe.finalReturned -and $probe.invalidReturned -and $probe.identityStable -and $probe.terminationAttempted -and $probe.terminationSucceeded -and $probe.processSignalled -and $probe.processClosed -and $probe.threadClosed -and $probe.jobClosed -and $probe.cachedErrorRestored -and [WaitDiagnosticProbe]::OwnershipSafe) 26
 Need-W ($probe.initialWait -eq 258 -and $probe.initialError -eq 0 -and $probe.finalWait -eq 0 -and $probe.finalError -eq 0 -and $probe.invalidWait -eq [uint32]::MaxValue -and $probe.invalidError -eq 6) 27
 foreach($pair in @(@($probe.initialWait,$probe.initialError,'timeout'),@($probe.finalWait,$probe.finalError,'signaled'),@($probe.invalidWait,$probe.invalidError,'failed'))){$q=Project-W (State-W $pair[0] $pair[1]);Need-W ($q.valid -and $q.kind -ceq $pair[2] -and $q.result -eq $pair[0] -and $q.error -eq $pair[1]) 28}
 Finish-W
 Start-W 'affected-whole-lifetime-evidence'
 $current=Get-Content -Raw (Join-Path $PSScriptRoot 'controls.ps1');$bound=Get-Content -Raw (Join-Path $PSScriptRoot 'bound-whole-controls.ps1');Need-W ($current -ceq $bound) 29
 Need-W ($current.Contains('$candidateReceipt.childSignalledAfter') -and -not ('OwnedLifetime' -as [type]) -and -not ('OwnedSetup' -as [type]) -and [WaitDiagnosticProbe]::OwnershipSafe) 30
 Finish-W
} finally {
 [Console]::SetError($savedWriter);$Evidence.writerRestored=[object]::ReferenceEquals([Console]::Error,$savedWriter);$Evidence.pathUnchanged=$env:PATH -ceq $savedPath;$Evidence.ofsUnchanged=$OFS -ceq $savedOFS
}
Need-W ($rows.Count -eq 6 -and $Evidence.writerRestored -and $Evidence.pathUnchanged -and $Evidence.ofsUnchanged -and [WaitDiagnosticProbe]::OwnershipSafe) 31
$Evidence.complete=$true
ConvertTo-Json -InputObject @($rows.ToArray()) -Compress -Depth 4

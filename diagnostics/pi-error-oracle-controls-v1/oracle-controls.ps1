param([hashtable]$Progress=@{case=$null;assertion=0;completed=@();scenario=$null})
$ErrorActionPreference='Stop'
$script:oracleProgress=$Progress
function Check([bool]$Value,[int]$Id){$script:oracleProgress.assertion=$Id;if(-not $Value){throw 'oracle_control_assertion'}}
function Extract([string]$Source,[string]$Name){
 $t=$null;$e=$null;$ast=[Management.Automation.Language.Parser]::ParseInput($Source,[ref]$t,[ref]$e);Check ($e.Count -eq 0) 1
 $f=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $Name},$true));Check ($f.Count -eq 1) 2;return $f[0].Extent.Text
}
$source=Get-Content -Raw (Join-Path $PSScriptRoot 'controls.ps1');$prior=Get-Content -Raw (Join-Path $PSScriptRoot 'controls.prior.ps1')
$fn=@('Need','Function-Text','Canonical','Json','Run-Tools','Set-IdentityDetail','Compare-Failure','Lines')
. ([scriptblock]::Create((@($fn|ForEach-Object {Extract $source $_})-join "`n")))
$newCompare=${function:Compare-Failure}
. ([scriptblock]::Create((Extract $prior 'Compare-Failure')));$oldCompare=${function:Compare-Failure}
. ([scriptblock]::Create((Extract $source 'Compare-Failure')))
$old=Get-Content -Raw (Join-Path $PSScriptRoot 'original-tool-provenance.ps1');$new=Get-Content -Raw (Join-Path $PSScriptRoot 'tool-provenance.ps1')
$toolNames=@('Set-ToolProbe','Write-ToolProbeFailure','Get-BoundedTree','Get-SetupTools')
$oldBlock=[scriptblock]::Create((@($toolNames|ForEach-Object {Extract $old $_})-join "`n"));$newBlock=[scriptblock]::Create((Extract $new 'Write-NpmSelectionFailure')+"`n"+(@($toolNames|ForEach-Object {Extract $new $_})-join "`n"))
$expected=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-oracle-controls.json')|ConvertFrom-Json)
function Observe([scriptblock]$Oracle,$A,$B,[Exception]$Injected=$null){
 $script:controlProgress=@{case='original-other-refusal-no-probe';assertion=0;completed=@();identityDetail=$null};$caught=$null;$output=@()
 try{$output=@(& $Oracle $A $B $Injected)}catch{$caught=$_}
 return @{error=$caught;assertion=$script:controlProgress.assertion;output=$output}
}
function Reject($A,$B,[int]$ExpectedId,[Exception]$Injected=$null){
 $seen=Observe $newCompare $A $B $Injected
 Check ($null -ne $seen.error -and $seen.error.Exception.Message -ceq 'control_assertion' -and $seen.assertion -eq $ExpectedId -and $seen.output.Count -eq 0) 3
}
function Accept($A,$B,[Exception]$Injected=$null){$seen=Observe $newCompare $A $B $Injected;Check ($null -eq $seen.error -and $seen.assertion -eq 19 -and $seen.output.Count -eq 0) 4}
function Record([Exception]$Exception,[string]$Id='fixed-oracle-id',[Management.Automation.ErrorCategory]$Category=[Management.Automation.ErrorCategory]::ReadError){return [Management.Automation.ErrorRecord]::new($Exception,$Id,$Category,$null)}
function Fixture {
 $ex=[IO.IOException]::new('fixed-oracle-message',-1234567)
 $pairs=@{}
 foreach($side in @('original','marked')){
  $er=Record $ex;$boundary=[Management.Automation.ErrorRecord]::new($er,$null)
  $pairs[$side]=@{error=$er;boundary=$boundary;observerCalled=$true;observerContext=@{mode='original-provider-error';side=$side};mode='original-provider-error';side=$side;stdout=@();ledger=@('fixed-provider-call')}
 }
 return @{a=$pairs.original;b=$pairs.marked;injected=$ex}
}
function Preconditions($A,$B){
 Check ($null -ne $A.error -and $null -ne $B.error -and $A.stdout.Count -eq 0 -and $B.stdout.Count -eq 0) 5
 Check ($A.error.Exception.GetType() -eq $B.error.Exception.GetType() -and $A.error.Exception.HResult -eq $B.error.Exception.HResult -and $A.error.CategoryInfo.Category -eq $B.error.CategoryInfo.Category -and $A.error.FullyQualifiedErrorId -ceq $B.error.FullyQualifiedErrorId -and $A.error.Exception.Message -ceq $B.error.Exception.Message) 6
}
$rows=[Collections.Generic.List[object]]::new()
function Case([string]$Name,[scriptblock]$Body){$script:oracleProgress.case=$Name;$script:oracleProgress.assertion=0;$script:oracleProgress.scenario=$null;$out=@(& $Body);Check ($out.Count -eq 0) 7;$rows.Add(@{name=$Name;outcome='PASS'});$script:oracleProgress.completed=@($script:oracleProgress.completed)+$Name}
Case 'cross-catch-wrapper-positive' {
 foreach($explicit in @($false,$true)){
  $script:oracleProgress.scenario=if($explicit){'explicit-record'}else{'exception'}
  $f=Fixture
  foreach($side in @('original','marked')){
   $inner=$null;$outer=$null;$inputValue=if($explicit){Record $f.injected}else{$f.injected}
   try{try{throw $inputValue}catch{$inner=$_;throw}}catch{$outer=$_}
   $x=if($side -ceq 'original'){$f.a}else{$f.b};$x.error=$outer;$x.boundary=$inner
   Check (-not [Object]::ReferenceEquals($inner,$outer) -and [Object]::ReferenceEquals($inner.Exception,$f.injected) -and [Object]::ReferenceEquals($outer.Exception,$f.injected)) 8
  }
  Preconditions $f.a $f.b;$oldResult=Observe $oldCompare $f.a $f.b $f.injected
  Check ($null -ne $oldResult.error -and $oldResult.error.Exception.Message -ceq 'control_assertion' -and $oldResult.assertion -eq 16) 9
  Accept $f.a $f.b $f.injected
  $f.a.boundary=$f.a.error;$f.b.boundary=$f.b.error;Accept $f.a $f.b $f.injected
 }
}
Case 'exception-replacement-negative' {
 foreach($side in @('original','marked')){
  $script:oracleProgress.scenario=$side;$f=Fixture;$x=if($side -ceq 'original'){$f.a}else{$f.b}
  $replacement=[IO.IOException]::new($f.injected.Message,$f.injected.HResult);$x.error=Record $replacement
  Preconditions $f.a $f.b;Reject $f.a $f.b 16 $f.injected
  $x.boundary=[Management.Automation.ErrorRecord]::new($x.error,$null)
  Preconditions $f.a $f.b;Reject $f.a $f.b 17 $f.injected
 }
}
Case 'record-semantic-negative' {
 foreach($side in @('original','marked')){foreach($field in @('type','hresult','category','fqid','message')){
  $script:oracleProgress.scenario=$field;$f=Fixture;$x=if($side -ceq 'original'){$f.a}else{$f.b}
  switch($field){
   'type'{$x.boundary=Record ([InvalidOperationException]::new('fixed-oracle-message'))}
   'hresult'{$x.boundary=Record ([IO.IOException]::new('fixed-oracle-message',-7654321))}
   'category'{$x.boundary=Record $f.injected 'fixed-oracle-id' ([Management.Automation.ErrorCategory]::PermissionDenied)}
   'fqid'{$x.boundary=Record $f.injected 'changed-oracle-id'}
   'message'{$x.boundary=Record ([IO.IOException]::new('changed-oracle-message',-1234567))}
  }
  # Outward records stay intact so A14/A15 cannot hide the intended A16 rejection.
  Preconditions $f.a $f.b;Reject $f.a $f.b 16 $f.injected
 }}
 foreach($field in @('type','hresult','category','fqid','message')){
  $script:oracleProgress.scenario='cross-side-'+$field;$f=Fixture
  switch($field){
   'type'{$f.b.error=Record ([InvalidOperationException]::new('fixed-oracle-message'))}
   'hresult'{$f.b.error=Record ([IO.IOException]::new('fixed-oracle-message',-7654321))}
   'category'{$f.b.error=Record $f.injected 'fixed-oracle-id' ([Management.Automation.ErrorCategory]::PermissionDenied)}
   'fqid'{$f.b.error=Record $f.injected 'changed-oracle-id'}
   'message'{$f.b.error=Record ([IO.IOException]::new('changed-oracle-message',-1234567))}
  }
  $f.b.boundary=$f.b.error;Reject $f.a $f.b 15 $f.injected
 }
}
Case 'observer-and-context-negative' {
 foreach($side in @('original','marked')){foreach($kind in @('observer-false','observer-nonbool','observer-missing','boundary-missing','boundary-wrongtype','context-missing','context-wrongtype','context-extra','context-mode','context-side','side-wrong','side-missing','mode-invalid','mode-mismatch')){
  $script:oracleProgress.scenario=$kind;$f=Fixture;$x=if($side -ceq 'original'){$f.a}else{$f.b}
  switch($kind){
   'observer-false'{$x.observerCalled=$false};'observer-nonbool'{$x.observerCalled='true'};'observer-missing'{$x.Remove('observerCalled')}
   'boundary-missing'{$x.Remove('boundary')};'boundary-wrongtype'{$x.boundary='never-export'}
   'context-missing'{$x.Remove('observerContext')};'context-wrongtype'{$x.observerContext=[pscustomobject]@{mode=$x.mode;side=$side}}
   'context-extra'{$x.observerContext.secret='never-export'};'context-mode'{$x.observerContext.mode='package-error'};'context-side'{$x.observerContext.side=if($side -ceq 'original'){'marked'}else{'original'}}
   'side-wrong'{$x.side=if($side -ceq 'original'){'marked'}else{'original'}};'side-missing'{$x.Remove('side')}
   'mode-invalid'{$x.mode='never-export';$x.observerContext.mode='never-export'};'mode-mismatch'{$x.mode='package-error';$x.observerContext.mode='package-error'}
  }
  Preconditions $f.a $f.b;Reject $f.a $f.b 16 $f.injected
 }}
 $script:oracleProgress.scenario='swapped';$f=Fixture;Preconditions $f.a $f.b;Reject $f.b $f.a 16 $f.injected
}
Case 'output-provider-ledger-negative' {
 foreach($side in @('original','marked')){
  $script:oracleProgress.scenario=$side;$f=Fixture;$x=if($side -ceq 'original'){$f.a}else{$f.b};$x.stdout=@('never-export');Reject $f.a $f.b 14 $f.injected
 }
 $script:oracleProgress.scenario='short-ledger';$f=Fixture;$f.b.ledger=@();Preconditions $f.a $f.b;Reject $f.a $f.b 18 $f.injected
 $script:oracleProgress.scenario='changed-ledger';$f=Fixture;$f.b.ledger=@('different-provider-call');Preconditions $f.a $f.b;Reject $f.a $f.b 19 $f.injected
 $script:oracleProgress.scenario='allowed-extra';$f=Fixture;$f.b.ledger+=@('diagnostic-extra');Accept $f.a $f.b $f.injected
}
Case 'exact-original-marked-error-paths' {
 foreach($mode in @('original-provider-error','package-error','package-refusal')){
  $script:oracleProgress.scenario=$mode;$ex=[IO.IOException]::new('fixed-provider-error',-2345678)
  $a=Run-Tools $false $mode -Injected $ex;$b=Run-Tools $true $mode -Injected $ex
  if($mode -ceq 'package-refusal'){Accept $a $b}else{Accept $a $b $ex}
  Check ((Json $a.ledger) -ceq (Json $b.ledger) -and $a.stderr -ceq $b.stderr -and @(Lines $a.stderr).Count -eq 1) 10
 }
 $script:oracleProgress.scenario='writer-fault';$ex=[IO.IOException]::new('fixed-writer-error',-3456789)
 $a=Run-Tools $false 'original-provider-error' -Injected $ex;$b=Run-Tools $true 'original-provider-error' -Injected $ex -BrokenWriter
 Accept $a $b $ex;Check ($b.writerFault -and $b.stderr -ceq '' -and (Json $a.ledger) -ceq (Json $b.ledger)) 11
 $script:oracleProgress.scenario='reset-sequence'
 $a=@(Run-Tools $false 'missing' -FollowingModes @('success','no-command'));$b=@(Run-Tools $true 'missing' -FollowingModes @('success','no-command'))
 Check ($a.Count -eq 3 -and $b.Count -eq 3) 12
 Accept $a[0] $b[0];Accept $a[2] $b[2]
 foreach($set in @(@{values=$a;side='original'},@{values=$b;side='marked'})){
  Check ($null -eq $set.values[1].observerContext -and -not $set.values[1].observerCalled -and $null -eq $set.values[1].boundary -and $null -eq $set.values[1].error) 13
  Check ($set.values[0].observerContext.mode -ceq 'missing' -and $set.values[2].observerContext.mode -ceq 'no-command' -and $set.values[0].observerContext.side -ceq $set.side -and $set.values[2].observerContext.side -ceq $set.side -and -not [Object]::ReferenceEquals($set.values[0].observerContext,$set.values[2].observerContext)) 14
 }
 Check ((Json $a[1].stdout) -ceq (Json $b[1].stdout) -and (Json $a[1].ledger) -ceq (Json $b[1].ledger) -and $b[1].stderr -ceq '') 15
}
Check ($rows.Count -eq 6 -and $expected.Count -eq 6) 16
for($i=0;$i -lt 6;$i++){Check ($rows[$i].name -ceq $expected[$i]) 17}
ConvertTo-Json -InputObject @($rows.ToArray()) -Depth 4 -Compress

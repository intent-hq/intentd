param([Parameter(Mandatory)][hashtable]$Progress,[Parameter(Mandatory)][hashtable]$Evidence,[Parameter(Mandatory)][string]$Node,[Parameter(Mandatory)][string]$FixtureRoot)
$ErrorActionPreference='Stop'
$rows=[Collections.Generic.List[object]]::new();$savedWriter=[Console]::Error;$savedPath=$env:PATH;$savedOFS=$OFS
$Evidence.native=@();$Evidence.writerRestored=$false;$Evidence.pathUnchanged=$false;$Evidence.ofsUnchanged=$false;$Evidence.cleaned=$false;$Evidence.complete=$false
function Need([bool]$Condition,[int]$Id){$Progress.assertion=$Id;if(-not $Condition){throw ('control_'+$Id)}}
function Start-Case([string]$Name){$Progress.case=$Name;$Progress.assertion=0}
function Finish-Case { $rows.Add(@{name=$Progress.case;outcome='PASS'});$Progress.completed=@($Progress.completed)+@($Progress.case) }
function Functions([string]$File,[string[]]$Names){
 $tokens=$null;$errors=$null;$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $PSScriptRoot $File),[ref]$tokens,[ref]$errors)
 Need ($errors.Count -eq 0) 1
 $out=@();foreach($n in $Names){$found=@($ast.FindAll({param($x)$x -is [Management.Automation.Language.FunctionDefinitionAst] -and $x.Name -ceq $n},$true));Need ($found.Count -eq 1) 2;$out+=@($found[0].Extent.Text)}
 return ($out -join "`n")
}
$oldCheck=Functions 'original-native-controls.ps1' @('Check');$newCheck=Functions 'native-controls.ps1' @('Check')
$setupFunctions=Functions 'setup-only.ps1' @('New-SetupFailureRecord','Write-SetupFailureRecord','Get-SetupOwnershipState')
$tokens=$null;$errors=$null;$setupAst=[Management.Automation.Language.Parser]::ParseFile((Join-Path $PSScriptRoot 'setup-only.ps1'),[ref]$tokens,[ref]$errors)
$topTry=@($setupAst.EndBlock.Statements|Where-Object {$_ -is [Management.Automation.Language.TryStatementAst]});Need ($errors.Count -eq 0 -and $topTry.Count -eq 1 -and $topTry[0].CatchClauses.Count -eq 1) 3
$catchText=$topTry[0].CatchClauses[0].Body.Extent.Text;$catchBody=[ScriptBlock]::Create($catchText.Substring(1,$catchText.Length-2))
$codes=@('root_disposal_unconfirmed','assignment_failure_control','ownership_or_reader_incomplete','disk_cap_exceeded','entry_control','original_exit_changed','argument_control','deadline_control','grandchild_control','retired_parent_control','stream_cap_control','native46_outcome','native46_identity','native46_failure_or_skip','native46_summary')
function Exercise([string]$Function,[bool]$Condition,[AllowNull()][string]$Code,[bool]$Fault=$false,[bool]$Composition=$false,[bool]$ExpressionFault=$false){
 & {
  param($Function,$Condition,$Code,$Fault,$Composition,$ExpressionFault)
  Invoke-Expression $Function
  Invoke-Expression $setupFunctions
  $script:ownedInvocationStarted=$false;$diagnosticStage='native-controls';$outcome='PASS';$codeValue=0
  function Fail-Condition { throw $injectedCondition };$injectedCondition=[InvalidOperationException]::new('fixture-expression')
  $writer=[IO.StringWriter]::new();$previous=[Console]::Error;$errorObject=$null;$outputs=@();$directFault=$false;$originalAtCatch=$null
  try {
   if($Fault){$writer.Dispose()};[Console]::SetError($writer)
   if($Fault){try{[Console]::Error.WriteLine('installed-fault-proof')}catch{$directFault=$true}}
   try {
    $outputs=@(if($ExpressionFault){Check (Fail-Condition) $Code}else{Check $Condition $Code})
   } catch {
    $errorObject=$_;$originalAtCatch=$_.Exception
    if($Composition){. $catchBody;$codeValue=$code}
   }
  } finally {[Console]::SetError($previous)}
  $text=if($Fault){''}else{$writer.ToString()};$writer.Dispose()
  @{error=$errorObject;originalException=$originalAtCatch;outputs=$outputs;stderr=$text;directFault=$directFault;outcome=$outcome;hresult=$codeValue;injected=$injectedCondition}
 } $Function $Condition $Code $Fault $Composition $ExpressionFault
}
function Same-Error($A,$B){
 Need ($null -ne $A.error -and $null -ne $B.error) 4
 Need ($A.error.Exception.GetType() -eq $B.error.Exception.GetType() -and $A.error.Exception.HResult -eq $B.error.Exception.HResult -and [string]$A.error.CategoryInfo.Category -ceq [string]$B.error.CategoryInfo.Category -and $A.error.FullyQualifiedErrorId -ceq $B.error.FullyQualifiedErrorId -and $A.error.Exception.Message -ceq $B.error.Exception.Message) 5
 Need ($A.outputs.Count -eq 0 -and $B.outputs.Count -eq 0 -and [object]::ReferenceEquals($A.error.Exception,$A.originalException) -and [object]::ReferenceEquals($B.error.Exception,$B.originalException)) 6
}
function Marker($Text,[string]$Expected){
 $lines=@($Text -split '\r?\n'|Where-Object {$_ -ne ''});Need ($lines.Count -eq 1 -and $lines[0].Length -le 163 -and $lines[0].StartsWith('NATIVE_ASSERTION_V1 ')) 7
 $j=$lines[0].Substring(20)|ConvertFrom-Json -AsHashtable
 Need ((($j.Keys|Sort-Object)-join '|') -ceq 'behavioralInvocations|code|outcome|schema|stage' -and $j.schema -ceq 'native-assertion-v1' -and $j.stage -ceq 'native-controls' -and $j.code -ceq $Expected -and $j.outcome -ceq 'FAILED' -and $j.behavioralInvocations -is [long] -and $j.behavioralInvocations -eq 0) 8
}
function Safety($R){Need ($R.assigned -and $R.resumed -and $R.activeZero -and $R.rootSignalled -and $R.readerDone -and -not $R.readerError -and -not $R.overflow -and $R.written -le 8192 -and $R.read -le 12288 -and $R.pipeEof -and [OwnedLifetime]::OwnershipSafe) 20}
function Native([string]$Name,[string]$Script,[bool]$Reject=$false,[bool]$Alias=$false){
 Need ([OwnedLifetime]::OwnershipSafe) 21
 $uri=([Uri](Join-Path $PSScriptRoot 'lifetime-observer.mjs')).AbsoluteUri;if($Alias){$uri+='?fault=root-alias'}
 $argsForChild=[string[]]@('--import',$uri,(Join-Path $PSScriptRoot $Script),'retired-parent',(Join-Path $FixtureRoot ($Name+'.entry')))
 $r=[OwnedLifetime]::Run($Node,$argsForChild,$FixtureRoot,(Join-Path $FixtureRoot ($Name+'.log')),1500,8192,$Reject)
 $Evidence.native+=@(@{name=$Name;receipt=$r})
 return $r
}
try {
 Start-Case 'exact-check-pass-and-refusal'
 foreach($fn in @($oldCheck,$newCheck)){$r=Exercise $fn $true 'retired_parent_control';Need ($null -eq $r.error -and $r.outputs.Count -eq 0 -and $r.stderr -ceq '') 9}
 foreach($name in $codes){$a=Exercise $oldCheck $false $name;$b=Exercise $newCheck $false $name;Same-Error $a $b;Need ($a.stderr -ceq '') 10;Marker $b.stderr $name};Finish-Case
 Start-Case 'assertion-redaction-and-no-normalization'
 foreach($name in @('RETIRED_PARENT_CONTROL','private-path-secret-value',"bad`nrecord",'', $null)){$a=Exercise $oldCheck $false $name;$b=Exercise $newCheck $false $name;Same-Error $a $b;Marker $b.stderr 'unknown';Need (-not $b.stderr.Contains('private-path-secret-value')) 11};$valid=(Exercise $newCheck $false 'entry_control').stderr
 foreach($bad in @($valid.Replace('native-assertion-v1','wrong-schema'),$valid.Replace('"outcome":"FAILED"','"outcome":"PASS"'),$valid.Replace('"behavioralInvocations":0','"behavioralInvocations":1'),$valid.Replace('"code":','"extra":true,"code":'))){
  $rejected=$false;try{Marker $bad 'entry_control'}catch{if($_.Exception.Message -ceq 'control_8'){$rejected=$true}else{throw}}
  Need $rejected 31
 };Finish-Case
 Start-Case 'installed-writer-fault-preserves-refusal'
 $a=Exercise $oldCheck $false 'retired_parent_control' $true;$b=Exercise $newCheck $false 'retired_parent_control' $true
 Need ($a.directFault -and $b.directFault) 12;Same-Error $a $b;Need ($b.stderr -ceq '') 13
 $pass=Exercise $newCheck $true 'retired_parent_control' $true;Need ($pass.directFault -and $null -eq $pass.error -and $pass.outputs.Count -eq 0) 14;Finish-Case
 Start-Case 'check-expression-and-catch-composition'
 $a=Exercise $oldCheck $false 'retired_parent_control' $false $true;$b=Exercise $newCheck $false 'retired_parent_control' $false $true;Same-Error $a $b
 Need ($a.outcome -ceq 'FAILED' -and $b.outcome -ceq 'FAILED' -and $a.hresult -eq $a.error.Exception.HResult -and $b.hresult -eq $b.error.Exception.HResult) 15
 $lines=@($b.stderr -split '\r?\n'|Where-Object {$_ -ne ''});Need ($lines.Count -eq 2) 16;Marker $lines[0] 'retired_parent_control'
 $failure=$lines[1].Substring('SETUP_FAILURE_V1 '.Length)|ConvertFrom-Json
 Need ($failure.phase -ceq 'original' -and $failure.stage -ceq 'native-controls' -and $failure.outcome -ceq 'FAILED' -and $failure.exitCode -eq 1 -and $failure.ownership -ceq 'not-initialized') 17
 $a=Exercise $oldCheck $false 'retired_parent_control' $false $true $true;$b=Exercise $newCheck $false 'retired_parent_control' $false $true $true;Same-Error $a $b;Need (-not $b.stderr.Contains('NATIVE_ASSERTION_V1') -and $b.outcome -ceq 'FAILED' -and [object]::ReferenceEquals($a.error.Exception,$a.injected) -and [object]::ReferenceEquals($b.error.Exception,$b.injected)) 18;Finish-Case
 Start-Case 'exact-one-option-source-reversal'
 $original=Get-Content -Raw (Join-Path $PSScriptRoot 'original-synthetic-child.mjs');$candidate=Get-Content -Raw (Join-Path $PSScriptRoot 'synthetic-child.mjs')
 Need ($candidate.Replace("{stdio:'inherit',detached:true}","{stdio:'inherit'}") -ceq $original -and ([regex]::Matches($candidate,'detached:true')).Count -eq 1) 19
 Need (-not ('OwnedSetup' -as [type]) -and -not ('OwnedLifetime' -as [type]) -and -not (Test-Path -LiteralPath $FixtureRoot)) 22
 $null=New-Item -ItemType Directory -Path $FixtureRoot
 Add-Type -Path (Join-Path $PSScriptRoot 'owned-lifetime.cs')
 Need ([OwnedLifetime]::OwnershipSafe) 23;Finish-Case
 Start-Case 'native-parent-retirement-and-owned-descendant'
 $candidateReceipt=Native 'candidate' 'synthetic-child.mjs';Safety $candidateReceipt
 # Exact original per-case predicate remains necessary, plus direct observer obligations.
 Need ($candidateReceipt.reason -ceq 'deadline' -and $candidateReceipt.originalExit -eq 0 -and $candidateReceipt.maximumActive -ge 2) 24
 Need ($candidateReceipt.observerFailure -ceq 'none' -and $candidateReceipt.observerLines -eq 1 -and $candidateReceipt.childHandleBound -and $candidateReceipt.childInOwner -and $candidateReceipt.rootIdentityStable -and $candidateReceipt.childIdentityStable -and $candidateReceipt.retiredWithLiveChild -and $candidateReceipt.retiredObservedMs -ge 0 -and $candidateReceipt.retiredObservedMs -lt 1500 -and $candidateReceipt.childSignalledAfter -and $candidateReceipt.childHandleClosed -and $candidateReceipt.interventions.Contains('terminate_owned_job')) 25;Finish-Case
 Start-Case 'default-vs-detached-discriminator'
 $oldReceipt=Native 'original' 'original-synthetic-child.mjs';Safety $oldReceipt
 Need ($oldReceipt.observerFailure -ceq 'none' -and $oldReceipt.observerLines -eq 1 -and $oldReceipt.childHandleBound -and $oldReceipt.childIdentityStable -and $oldReceipt.rootIdentityStable -and $oldReceipt.childSignalledAfter -and $oldReceipt.childHandleClosed) 26
 # If the tolerated libuv assignment failure prevents discrimination, stop; do not waive it.
 Need ($oldReceipt.reason -ceq 'completed' -and $oldReceipt.originalExit -eq 0 -and $oldReceipt.interventions.Count -eq 0 -and $candidateReceipt.reason -ceq 'deadline') 27;Finish-Case
 Start-Case 'ownership-refusal-and-bounds-sensitivity'
 $refused=Native 'refused' 'synthetic-child.mjs' $true
 Need ($refused.reason -ceq 'assignment_failed' -and -not $refused.assigned -and -not $refused.resumed -and $refused.rootSignalled -and $refused.rootIdentityStable -and $refused.observerLines -eq 0 -and -not $refused.childHandleBound -and $refused.read -eq 0 -and $refused.interventions.Contains('terminate_unresumed_process_handle') -and [OwnedLifetime]::OwnershipSafe) 28
 $negative=Native 'alias' 'synthetic-child.mjs' $false $true;Safety $negative
 Need ($negative.observerFailure -ceq 'child-root-alias' -and -not $negative.childHandleBound -and -not $negative.retiredWithLiveChild -and $negative.reason -ceq 'deadline' -and $negative.interventions.Contains('terminate_owned_job')) 29;Finish-Case
} finally {
 [Console]::SetError($savedWriter);$Evidence.writerRestored=[object]::ReferenceEquals([Console]::Error,$savedWriter)
 $Evidence.pathUnchanged=$env:PATH -ceq $savedPath;$Evidence.ofsUnchanged=$OFS -ceq $savedOFS
 # No new process and no recursive or unknown-file removal on any path.
 try {
  if(Test-Path -LiteralPath $FixtureRoot){
   if(-not ('OwnedLifetime' -as [type]) -or -not [OwnedLifetime]::OwnershipSafe){throw 'cleanup_ownership_unresolved'}
   $rootItem=Get-Item -LiteralPath $FixtureRoot;if(($rootItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0){throw 'cleanup_root_reparse'}
   $items=@(Get-ChildItem -LiteralPath $FixtureRoot -Force);if($items.Count -gt 4){throw 'cleanup_inventory'}
   foreach($f in $items){if($f.PSIsContainer -or $f.Name -cnotin @('candidate.log','original.log','refused.log','alias.log') -or $f.Length -gt 8192 -or ($f.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0){throw 'cleanup_entry'}}
   foreach($f in $items){Remove-Item -LiteralPath $f.FullName};Remove-Item -LiteralPath $FixtureRoot
  }
  $Evidence.cleaned=-not (Test-Path -LiteralPath $FixtureRoot)
 }catch{$Evidence.cleaned=$false}
}
Need ($Evidence.writerRestored -and $Evidence.pathUnchanged -and $Evidence.ofsUnchanged -and $Evidence.cleaned -and $rows.Count -eq 8) 30
$Evidence.complete=$true
$rows|ConvertTo-Json -Depth 3 -Compress

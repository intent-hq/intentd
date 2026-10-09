param([hashtable]$Progress=@{case=$null;assertion=0;completed=@();underlying=$null;binding=$null})
$ErrorActionPreference='Stop'
$script:boundaryProgress=$Progress
function Require([bool]$Value,[int]$Id){$script:boundaryProgress.assertion=$Id;if(-not $Value){throw 'boundary_control_assertion'}}
function Extract([string]$Source,[string]$Name){
 $tokens=$null;$errors=$null;$ast=[Management.Automation.Language.Parser]::ParseInput($Source,[ref]$tokens,[ref]$errors);Require ($errors.Count -eq 0) 1
 $matches=@($ast.FindAll({param($a)$a -is [Management.Automation.Language.FunctionDefinitionAst] -and $a.Name -ceq $Name},$true));Require ($matches.Count -eq 1) 2;return $matches[0].Extent.Text
}
$metadata=Get-Content -Raw (Join-Path $PSScriptRoot 'controls.metadata-candidate.ps1')
$expected=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-controls.json')|ConvertFrom-Json)
# Extract only exact function definitions: no eight-case top-level invocation occurs.
$names=@('Need','Function-Text','Canonical','Json','Run-Tools','Set-IdentityDetail','Compare-Failure','Lines')
. ([scriptblock]::Create((@($names|ForEach-Object {Extract $metadata $_})-join "`n")))
. (Join-Path $PSScriptRoot 'identity-projector.ps1')
$script:controlProgress=@{case='original-other-refusal-no-probe';assertion=0;completed=@();identityDetail=$null}
$old=Get-Content -Raw (Join-Path $PSScriptRoot 'original-tool-provenance.ps1');$new=Get-Content -Raw (Join-Path $PSScriptRoot 'tool-provenance.ps1')
$fn=@('Set-ToolProbe','Write-ToolProbeFailure','Get-BoundedTree','Get-SetupTools')
$oldBlock=[scriptblock]::Create((@($fn|ForEach-Object {Extract $old $_})-join "`n"))
$newBlock=[scriptblock]::Create((Extract $new 'Write-NpmSelectionFailure')+"`n"+(@($fn|ForEach-Object {Extract $new $_})-join "`n"))
$savedIdentityHelper=${function:Set-IdentityDetail}
# Independent call binding, reset before each comparison; delegate exact accepted metadata.
$script:identityBinding=$null
function Set-IdentityDetail($Progress,[string]$Side,$Result){
 $script:identityBinding=@{active=$true;case=$Progress.case;mode=$Result.mode;side=$Side}
 & $savedIdentityHelper $Progress $Side $Result
}
function Observe-Comparison($A,$B){
 $script:controlProgress=@{case='original-other-refusal-no-probe';assertion=0;completed=@();identityDetail=$null}
 $script:identityBinding=$null;$failure=$null
 try{Compare-Failure $A $B}catch{$failure=$_}
 $script:boundaryProgress.underlying=$script:controlProgress;$script:boundaryProgress.binding=$script:identityBinding
 $projection=Project-IdentityDetail $script:controlProgress $script:identityBinding
 return @{failure=$failure;projection=$projection;state=$script:controlProgress}
}
function Semantic-Values($A,$B){
 return ($null -ne $A.error -and $null -ne $B.error -and $A.error.Exception.GetType() -eq $B.error.Exception.GetType() -and $A.error.Exception.HResult -eq $B.error.Exception.HResult -and $A.error.CategoryInfo.Category -eq $B.error.CategoryInfo.Category -and $A.error.FullyQualifiedErrorId -ceq $B.error.FullyQualifiedErrorId -and $A.error.Exception.Message -ceq $B.error.Exception.Message -and $A.stdout.Count -eq 0 -and $B.stdout.Count -eq 0 -and (Json $A.ledger) -ceq (Json $B.ledger))
}
function Semantic-Preservation($A,$B,[Exception]$Injected){return ((Semantic-Values $A $B) -and [Object]::ReferenceEquals($A.error.Exception,$Injected) -and [Object]::ReferenceEquals($B.error.Exception,$Injected))}
$rows=[Collections.Generic.List[object]]::new()
function Case([string]$Name,[scriptblock]$Body){
 $script:boundaryProgress.case=$Name;$script:boundaryProgress.assertion=0;$script:boundaryProgress.underlying=$null;$script:boundaryProgress.binding=$null
 $out=@(& $Body);Require ($out.Count -eq 0) 3;$rows.Add(@{name=$Name;outcome='PASS'});$script:boundaryProgress.completed=@($script:boundaryProgress.completed)+$Name
}
Case 'same-catch-alias' {
 foreach($explicit in @($false,$true)){
  $ex=[IO.IOException]::new('fixed-boundary-error',-1234567)
  $inputError=if($explicit){[Management.Automation.ErrorRecord]::new($ex,'fixed-boundary-id',[Management.Automation.ErrorCategory]::ReadError,$null)}else{$ex}
  try{throw $inputError}catch{$a=$_;$b=$_;Require ($a -is [Management.Automation.ErrorRecord] -and [Object]::ReferenceEquals($a,$b) -and [Object]::ReferenceEquals($a.Exception,$ex) -and [Object]::ReferenceEquals($a.Exception,$b.Exception)) 4}
 }
}
Case 'nested-bare-rethrow' {
 foreach($explicit in @($false,$true)){
  $ex=[IO.IOException]::new('fixed-boundary-error',-2345678);$inner=$null;$outer=$null
  $inputError=if($explicit){[Management.Automation.ErrorRecord]::new($ex,'fixed-boundary-id',[Management.Automation.ErrorCategory]::PermissionDenied,$null)}else{$ex}
  try{try{throw $inputError}catch{$inner=$_;throw}}catch{$outer=$_}
  Require ($inner -is [Management.Automation.ErrorRecord] -and $outer -is [Management.Automation.ErrorRecord] -and -not [Object]::ReferenceEquals($inner,$outer)) 5
  Require ([Object]::ReferenceEquals($inner.Exception,$ex) -and [Object]::ReferenceEquals($outer.Exception,$ex) -and $inner.Exception.HResult -eq $outer.Exception.HResult -and $inner.CategoryInfo.Category -eq $outer.CategoryInfo.Category -and $inner.FullyQualifiedErrorId -ceq $outer.FullyQualifiedErrorId -and $inner.Exception.Message -ceq $outer.Exception.Message) 6
 }
}
Case 'observer-scope-discriminator' {
 $ex=[IO.IOException]::new('fixed-observer-error',-3456789)
 $pair=@(& {
  $fx=@{boundary=$null;observerCalled=$false}
  # Recreate the exact writer as a named function before taking its ScriptBlock.
  . ([scriptblock]::Create((Extract $old 'Write-ToolProbeFailure')));$savedToolWriter=${function:Write-ToolProbeFailure}
  . ([scriptblock]::Create((Extract (Extract $metadata 'Run-Tools') 'Write-ToolProbeFailure')))
  $saved=[Console]::Error;$writer=[IO.StringWriter]::new();$inner=$null;$outer=$null
  try{
   [Console]::SetError($writer)
   try{try{throw $ex}catch{$inner=$_;Write-ToolProbeFailure @{value=@{stage='npm-entry';predicate='none';tree='none'}} ([string]$_.CategoryInfo.Category) $_.Exception.HResult;throw}}catch{$outer=$_}
  }finally{[Console]::SetError($saved);$writer.Dispose()}
  $result=[pscustomobject]@{error=$outer;boundary=$fx.boundary;observerCalled=$fx.observerCalled;mode='original-provider-error';stdout=@();ledger=@()}
  [pscustomobject]@{result=$result;inner=$inner}
 })
 Require ($pair.Count -eq 1) 7
 $o=Observe-Comparison $pair[0].result $pair[0].result
 Require ($null -ne $o.failure -and $o.state.assertion -eq 16) 8
 Require ($o.projection.classification -ceq 'complete' -and [Object]::ReferenceEquals($pair[0].result.boundary,$pair[0].inner)) 9
 Require (-not $o.projection.detail.sameRecord -and $o.projection.detail.sameException) 10
}
Case 'exact-original-marked-provider-boundary' {
 $ex=[IO.IOException]::new('fixed-provider-error',-4567890)
 $a=Run-Tools $false 'original-provider-error' -Injected $ex;$b=Run-Tools $true 'original-provider-error' -Injected $ex
 $o=Observe-Comparison $a $b
 Require (Semantic-Preservation $a $b $ex) 11
 Require ((@($a.ledger).Count -eq 1) -and $a.ledger[0].StartsWith('exists|') -and $a.stderr -ceq $b.stderr) 12
 Require ($null -ne $o.failure -and $o.state.assertion -eq 16 -and $o.projection.classification -ceq 'complete') 13
 Require ($a.observerCalled -and $b.observerCalled -and $a.boundary -is [Management.Automation.ErrorRecord] -and $b.boundary -is [Management.Automation.ErrorRecord] -and [Object]::ReferenceEquals($a.boundary.Exception,$ex) -and [Object]::ReferenceEquals($b.boundary.Exception,$ex)) 14
 Require (-not $o.projection.detail.sameRecord -and $o.projection.detail.sameException) 15
}
Case 'replacement-negative-controls' {
 $ex=[IO.IOException]::new('fixed-negative',-5678901);$base=[Management.Automation.ErrorRecord]::new($ex,'fixed-id',[Management.Automation.ErrorCategory]::ReadError,$null)
 $a=@{error=$base;stdout=@();ledger=@('fixed-call')};$b=@{error=[Management.Automation.ErrorRecord]::new($base,$null);stdout=@();ledger=@('fixed-call')}
 Require (Semantic-Preservation $a $b $ex) 16
 $variants=@(
  [Management.Automation.ErrorRecord]::new([IO.IOException]::new('fixed-negative',-5678901),'fixed-id',[Management.Automation.ErrorCategory]::ReadError,$null),
  [Management.Automation.ErrorRecord]::new([IO.IOException]::new('fixed-negative',-6789012),'fixed-id',[Management.Automation.ErrorCategory]::ReadError,$null),
  [Management.Automation.ErrorRecord]::new($ex,'fixed-id',[Management.Automation.ErrorCategory]::PermissionDenied,$null),
  [Management.Automation.ErrorRecord]::new($ex,'different-id',[Management.Automation.ErrorCategory]::ReadError,$null),
  [Management.Automation.ErrorRecord]::new([IO.IOException]::new('different-message',-5678901),'fixed-id',[Management.Automation.ErrorCategory]::ReadError,$null))
 for($i=0;$i -lt $variants.Count;$i++){$b.error=$variants[$i];Require (-not (Semantic-Preservation $a $b $ex)) 17;Require ((Semantic-Values $a $b) -eq ($i -eq 0)) 17}
 $b.error=$base;$b.stdout=@('unexpected');Require (-not (Semantic-Preservation $a $b $ex)) 18
 $b.stdout=@();$b.ledger=@('different-call');Require (-not (Semantic-Preservation $a $b $ex)) 19
}
Case 'fixed-schema-and-bookkeeping' {
 $ex=[IO.IOException]::new('fixed-schema',-7890123);$er=[Management.Automation.ErrorRecord]::new($ex,'fixed-id',[Management.Automation.ErrorCategory]::ReadError,$null)
 $state=@{case='original-other-refusal-no-probe';assertion=16;identityDetail=$null}
 $binding=@{active=$true;case=$state.case;mode='original-provider-error';side='original'}
 $result=@{error=$er;boundary=[Management.Automation.ErrorRecord]::new($er,$null);observerCalled=$true;mode=$binding.mode}
 & $savedIdentityHelper $state 'original' $result
 Require ((Project-IdentityDetail $state $binding).classification -ceq 'complete') 20
 $valid=$state.identityDetail|ConvertTo-Json -Compress
 foreach($kind in @('extra','missing','wrong-bool','null-ref','case','mode','side','assertion','inactive','unavailable')){
  $state.identityDetail=$valid|ConvertFrom-Json -AsHashtable;$state.assertion=16;$binding.active=$true
  switch($kind){
   'extra' {$state.identityDetail.secret='never-output'}
   'missing' {$state.identityDetail.Remove('mode')}
   'wrong-bool' {$state.identityDetail.observerCalled='true'}
   'null-ref' {$state.identityDetail.sameException=$null}
   'case' {$state.identityDetail.case='missing-entry-same-parent'}
   'mode' {$state.identityDetail.mode='package-error'}
   'side' {$state.identityDetail.side='marked'}
   'assertion' {$state.assertion=15}
   'inactive' {$binding.active=$false}
   'unavailable' {$state.identityDetail.observedRecord=$false;$state.identityDetail.observedException=$false;$state.identityDetail.sameRecord=$null;$state.identityDetail.sameException=$null}
  }
  $projection=Project-IdentityDetail $state $binding;Require ($projection.classification -ceq 'incomplete') 21
  $roundtrip=($projection|ConvertTo-Json -Compress)|ConvertFrom-Json
  if($kind -eq 'unavailable'){Require ($null -ne $roundtrip.detail -and -not $roundtrip.detail.observedRecord -and $null -eq $roundtrip.detail.sameRecord -and $null -eq $roundtrip.detail.sameException) 22}else{Require ($null -eq $roundtrip.detail) 22}
 }
 $state.assertion=16;$binding.active=$true;$state.identityDetail=$valid|ConvertFrom-Json -AsHashtable;$state.case='capture-composition-and-scope';Require ((Project-IdentityDetail $state $binding).classification -ceq 'incomplete') 23
 $dict=[Collections.Generic.Dictionary[string,object]]::new();$dict.Add('case','original-other-refusal-no-probe');$dict.Add('identityDetail',$null);$ro=[Collections.ObjectModel.ReadOnlyDictionary[string,object]]::new($dict)
 $fault=$false;try{$ro.identityDetail=@{}}catch{$fault=$true};Require $fault 24
 $outer=$null;try{try{throw $ex}catch{$alias=$_;$out=@(& $savedIdentityHelper $ro 'original' $result);Require ($out.Count -eq 0 -and [Object]::ReferenceEquals($_,$alias)) 25;throw}}catch{$outer=$_}
 Require ([Object]::ReferenceEquals($outer.Exception,$ex) -and $null -eq $ro.identityDetail) 26
}
Case 'writer-and-capture-composition' {
 $ex=[IO.IOException]::new('fixed-writer-error',-8901234);$a=Run-Tools $false 'original-provider-error' -Injected $ex;$b=Run-Tools $true 'original-provider-error' -Injected $ex -BrokenWriter
 Require ($b.writerFault -and $b.stderr -ceq '' -and (Semantic-Preservation $a $b $ex)) 27
 $o=Observe-Comparison $a $b;Require ($null -ne $o.failure -and $o.state.assertion -eq 16 -and $o.projection.classification -ceq 'complete' -and -not $o.projection.detail.sameRecord -and $o.projection.detail.sameException) 28
 . (Join-Path $PSScriptRoot 'capture-functions.ps1')
 $setup=Get-Content -Raw (Join-Path $PSScriptRoot 'setup-only.ps1');$start=$setup.IndexOf('} catch {'+"`n"+' $code=$_.Exception.HResult; $outcome=');$end=$setup.IndexOf("`n}"+"`nfinally {",$start);Require ($start -ge 0 -and $end -gt $start) 29
 $body=$setup.Substring($start+'} catch {'.Length,$end-$start-'} catch {'.Length);$saved=[Console]::Error;$writer=[IO.StringWriter]::new();$diagnosticStage='tool-provenance';$script:ownedInvocationStarted=$false;$outcome='PASS';$code=0
 try{[Console]::SetError($writer);try{throw $b.error}catch{Invoke-Expression $body}}finally{[Console]::SetError($saved)}
 $lines=@(Lines $writer.ToString());$writer.Dispose();Require ($lines.Count -eq 1 -and $lines[0].StartsWith('SETUP_FAILURE_V1 ')) 30
 $record=$lines[0].Substring('SETUP_FAILURE_V1 '.Length)|ConvertFrom-Json;Require ($outcome -ceq 'FAILED' -and $record.exitCode -eq 1 -and $record.phase -ceq 'original' -and $record.ownership -ceq 'not-initialized') 31
 # Exact A16 remains failed in the current comparison context, even if all seven diagnostics pass.
}
Require ($rows.Count -eq 7 -and $expected.Count -eq 7) 32
for($i=0;$i -lt 7;$i++){Require ($rows[$i].name -ceq $expected[$i]) 33}
ConvertTo-Json -InputObject @($rows.ToArray()) -Depth 4 -Compress

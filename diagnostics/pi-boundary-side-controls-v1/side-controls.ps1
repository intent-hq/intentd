param([hashtable]$Progress=@{case=$null;assertion=0;completed=@()})
$ErrorActionPreference='Stop'
$script:sideProgress=$Progress
function Check([bool]$Value,[int]$Id){$script:sideProgress.assertion=$Id;if(-not $Value){throw 'side_control_assertion'}}
$oldText=Get-Content -Raw (Join-Path $PSScriptRoot 'identity-detail-original.ps1')
$newText=Get-Content -Raw (Join-Path $PSScriptRoot 'identity-detail-helper.ps1')
Check ($oldText.Replace('$side','$safeSide') -ceq $newText) 1
. ([scriptblock]::Create($oldText));$oldHelper=${function:Set-IdentityDetail}
. ([scriptblock]::Create($newText));$newHelper=${function:Set-IdentityDetail}
. (Join-Path $PSScriptRoot 'identity-projector.ps1')
$expected=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-side-controls.json')|ConvertFrom-Json)
$rows=[Collections.Generic.List[object]]::new()
function Case([string]$Name,[scriptblock]$Body){$script:sideProgress.case=$Name;$script:sideProgress.assertion=0;$out=@(& $Body);Check ($out.Count -eq 0) 2;$rows.Add(@{name=$Name;outcome='PASS'});$script:sideProgress.completed=@($script:sideProgress.completed)+$Name}
function Fixture {
 $ex=[IO.IOException]::new('fixed-side-fixture',-1234567)
 $er=[Management.Automation.ErrorRecord]::new($ex,'fixed-side-id',[Management.Automation.ErrorCategory]::ReadError,$null)
 return @{state=@{case='original-other-refusal-no-probe';assertion=16;identityDetail=$null};binding=@{active=$true;case='original-other-refusal-no-probe';mode='original-provider-error';side='original'};result=@{mode='original-provider-error';observerCalled=$true;error=$er;boundary=$er};exception=$ex}
}
function Invoke-Helper([scriptblock]$Helper,$State,[string]$Label,$Result){$out=@(& $Helper $State $Label $Result);Check ($out.Count -eq 0) 3}
function Schema($Detail){
 $keys=@('schema','valid','case','mode','side','observerCalled','outwardRecord','observedRecord','outwardException','observedException','sameRecord','sameException')
 Check ((@($Detail.Keys|Sort-Object)-join '|') -ceq (@($keys|Sort-Object)-join '|')) 4
 $json=$Detail|ConvertTo-Json -Compress;Check ([Text.Encoding]::UTF8.GetByteCount($json) -le 2048 -and -not $json.Contains('never-export') -and -not $json.Contains('fixed-side-fixture')) 5
}
Case 'original-side-collision' {
 foreach($label in @('original','marked')){
  $f=Fixture;$f.binding.side=$label;Invoke-Helper $oldHelper $f.state $label $f.result
  Schema $f.state.identityDetail
  Check ($f.state.identityDetail.valid -eq $true -and $f.state.identityDetail.side -ceq 'unknown') 6
  $p=Project-IdentityDetail $f.state $f.binding;Check ($p.classification -ceq 'incomplete' -and $null -eq $p.detail) 7
 }
}
Case 'corrected-original-and-marked' {
 foreach($label in @('original','marked')){
  $a=Fixture;$b=Fixture;$b.result=$a.result;$b.binding.side=$label
  Invoke-Helper $oldHelper $a.state $label $a.result;Invoke-Helper $newHelper $b.state $label $b.result
  Schema $b.state.identityDetail;Check ($b.state.identityDetail.valid -eq $true -and $b.state.identityDetail.side -ceq $label) 8
  foreach($key in $a.state.identityDetail.Keys){if($key -cne 'side'){Check ($a.state.identityDetail[$key] -ceq $b.state.identityDetail[$key]) 9}}
  $p=Project-IdentityDetail $b.state $b.binding;Check ($p.classification -ceq 'complete' -and $p.detail.sameRecord -and $p.detail.sameException) 10
  $copy=($p|ConvertTo-Json -Depth 5 -Compress)|ConvertFrom-Json;Check ($copy.detail.side -ceq $label -and $copy.detail.valid) 11
 }
}
Case 'missing-observer-stays-incomplete' {
 foreach($label in @('original','marked')){foreach($kind in @('not-called','no-boundary','no-outward','missing-boundary-key')){
  $f=Fixture;$f.binding.side=$label
  switch($kind){'not-called'{$f.result.observerCalled=$false};'no-boundary'{$f.result.boundary=$null};'no-outward'{$f.result.error=$null};'missing-boundary-key'{$f.result.Remove('boundary')}}
  Invoke-Helper $newHelper $f.state $label $f.result;Schema $f.state.identityDetail
  $p=Project-IdentityDetail $f.state $f.binding;Check ($p.classification -ceq 'incomplete' -and $null -ne $p.detail -and $p.detail.side -ceq $label) 12
  if($kind -ne 'not-called'){Check ($null -eq $p.detail.sameRecord -and $null -eq $p.detail.sameException) 13}
 }}
}
Case 'invalid-context-and-stale-binding' {
 foreach($kind in @('bad-case','missing-case','bad-mode','missing-mode','bad-side','null-side','bad-observer','missing-observer')){
  $f=Fixture;$label='original'
  switch($kind){'bad-case'{$f.state.case='never-export'};'missing-case'{$f.state.Remove('case')};'bad-mode'{$f.result.mode='never-export'};'missing-mode'{$f.result.Remove('mode')};'bad-side'{$label='never-export'};'null-side'{$label=$null};'bad-observer'{$f.result.observerCalled='never-export'};'missing-observer'{$f.result.Remove('observerCalled')}}
  Invoke-Helper $newHelper $f.state $label $f.result;Schema $f.state.identityDetail
  Check (-not $f.state.identityDetail.valid -and $f.state.identityDetail.case -ceq 'unknown' -and $f.state.identityDetail.mode -ceq 'unknown' -and $f.state.identityDetail.side -ceq 'unknown') 14
  $p=Project-IdentityDetail $f.state $f.binding;Check ($p.classification -ceq 'incomplete' -and $null -eq $p.detail) 15
 }
 foreach($kind in @('assertion','case','mode','side','inactive','extra','null-reference','wrong-bool')){
  $f=Fixture;Invoke-Helper $newHelper $f.state 'original' $f.result
  switch($kind){'assertion'{$f.state.assertion=15};'case'{$f.binding.case='missing-entry-same-parent'};'mode'{$f.binding.mode='package-error'};'side'{$f.binding.side='marked'};'inactive'{$f.binding.active=$false};'extra'{$f.state.identityDetail.secret='never-export'};'null-reference'{$f.state.identityDetail.sameRecord=$null};'wrong-bool'{$f.state.identityDetail.observerCalled='true'}}
  $p=Project-IdentityDetail $f.state $f.binding;Check ($p.classification -ceq 'incomplete' -and $null -eq $p.detail) 16
 }
}
Case 'same-scope-side-reset' {
 # One loaded function and one mutable fixture for all calls; no state reset between invocations.
 & {
  . ([scriptblock]::Create($newText));$f=Fixture
  foreach($label in @('original','marked','never-export','original')){
   $out=@(Set-IdentityDetail $f.state $label $f.result);Check ($out.Count -eq 0) 17
   $wanted=if($label -ceq 'never-export'){'unknown'}else{$label}
   Check ($f.state.identityDetail.side -ceq $wanted -and $f.state.identityDetail.valid -eq ($label -cne 'never-export')) 18
  }
 }
}
Case 'readonly-and-exception-preservation' {
 $f=Fixture;$dict=[Collections.Generic.Dictionary[string,object]]::new();$dict.Add('case',$f.state.case);$dict.Add('identityDetail',$null);$ro=[Collections.ObjectModel.ReadOnlyDictionary[string,object]]::new($dict)
 $fault=$false;try{$ro.identityDetail=@{}}catch{$fault=$true};Check $fault 19
 $errors=[Collections.Generic.List[object]]::new()
 foreach($helper in @($oldHelper,$newHelper)){
  $caught=$null
  try{try{throw $f.exception}catch{$alias=$_;$out=@(& $helper $ro 'original' $f.result);Check ($out.Count -eq 0 -and [Object]::ReferenceEquals($_,$alias)) 20;throw}}catch{$caught=$_}
  Check ([Object]::ReferenceEquals($caught.Exception,$f.exception) -and $null -eq $ro.identityDetail) 21;$errors.Add($caught)
 }
 Check ($errors[0].Exception.HResult -eq $errors[1].Exception.HResult -and $errors[0].CategoryInfo.Category -eq $errors[1].CategoryInfo.Category -and $errors[0].FullyQualifiedErrorId -ceq $errors[1].FullyQualifiedErrorId -and $errors[0].Exception.Message -ceq $errors[1].Exception.Message) 22
}
Check ($rows.Count -eq 6 -and $expected.Count -eq 6) 23
for($i=0;$i -lt 6;$i++){Check ($rows[$i].name -ceq $expected[$i]) 24}
ConvertTo-Json -InputObject @($rows.ToArray()) -Depth 4 -Compress

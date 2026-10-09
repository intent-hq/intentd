param([Parameter(Mandatory)][hashtable]$Progress,[Parameter(Mandatory)][hashtable]$Evidence)
$ErrorActionPreference='Stop'
$rows=[Collections.Generic.List[object]]::new();$savedWriter=[Console]::Error;$savedPath=$env:PATH;$savedOFS=$OFS
$Evidence.writerRestored=$false;$Evidence.pathUnchanged=$false;$Evidence.ofsUnchanged=$false;$Evidence.complete=$false
function Assert-S([bool]$Condition,[int]$Id){$Progress.assertion=$Id;if(-not $Condition){throw ('sensitivity_'+$Id)}}
function Start-S([string]$Name){$Progress.case=$Name;$Progress.assertion=0}
function Finish-S {$rows.Add(@{name=$Progress.case;outcome='PASS'});$Progress.completed=@($Progress.completed)+@($Progress.case)}
function Extract([string]$File,[string[]]$Names){
 $tokens=$null;$errors=$null;$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $PSScriptRoot $File),[ref]$tokens,[ref]$errors)
 Assert-S ($errors.Count -eq 0) 40
 $out=@();foreach($n in $Names){$found=@($ast.FindAll({param($x)$x -is [Management.Automation.Language.FunctionDefinitionAst] -and $x.Name -ceq $n},$true));Assert-S ($found.Count -eq 1) 41;$out+=@($found[0].Extent.Text)}
 return ($out -join "`n")
}
# Load only these exact source functions, never the eight-case script body or native owner.
Invoke-Expression (Extract 'controls.ps1' @('Need','Exercise','Same-Error'))
$markerText=Extract 'controls.ps1' @('Marker')
$oldCheck=Extract 'original-native-controls.ps1' @('Check');$newCheck=Extract 'native-controls.ps1' @('Check')
$setupFunctions=Extract 'setup-only.ps1' @('New-SetupFailureRecord','Write-SetupFailureRecord','Get-SetupOwnershipState')
# Exercise composition is false in this population; the affected whole eight retains the exact catch test.
$catchBody=$null
function Try-Marker([string]$Definition,[string]$Text,[string]$Expected){
 & {
  param($Definition,$Text,$Expected)
  Invoke-Expression $Definition
  $caught=$null;$output=@()
  try{$output=@(Marker $Text $Expected)}catch{$caught=$_}
  @{error=$caught;output=$output}
 } $Definition $Text $Expected
}
function Is-ExactFailure($Result,[string]$Id){
 return ($null -ne $Result.error -and $Result.error.Exception.Message -ceq $Id -and $Result.output.Count -eq 0)
}
function Line([string]$Text,[int]$Length){
 $lines=@($Text -split '\r?\n'|Where-Object {$_ -ne ''})
 Assert-S ($lines.Count -eq 1 -and $lines[0].Length -eq $Length -and $lines[0].StartsWith('NATIVE_ASSERTION_V1 ')) 42
 return $lines[0]
}
try {
 Start-S 'fresh-valid-baseline'
 $a=Exercise $oldCheck $false 'entry_control';$b=Exercise $newCheck $false 'entry_control';Same-Error $a $b
 Assert-S ($a.stderr -ceq '') 43;$null=Line $b.stderr 146
 $r=Try-Marker $markerText $b.stderr 'entry_control';Assert-S ($null -eq $r.error -and $r.output.Count -eq 0) 44
 Finish-S
 Start-S 'four-in-bound-schema-refusals'
 $b=Exercise $newCheck $false 'entry_control';$null=Line $b.stderr 146
 $r=Try-Marker $markerText $b.stderr 'entry_control';Assert-S ($null -eq $r.error -and $r.output.Count -eq 0) 44
 $valid=$b.stderr
 $mutations=@($valid.Replace('native-assertion-v1','wrong-schema'),$valid.Replace('"outcome":"FAILED"','"outcome":"PASS"'),$valid.Replace('"behavioralInvocations":0','"behavioralInvocations":1'),$valid.Replace('"code":','"extra":true,"code":'))
 $lengths=@(139,144,146,159)
 # Mutants live only in isolated function scopes. Production Marker remains exact.
 $schemaLines=@($markerText -split '\r?\n'|Where-Object {$_ -like ' Need *' -and $_.EndsWith(' 8')})
 Assert-S ($schemaLines.Count -eq 1) 45
 $skip=$markerText.Replace($schemaLines[0],'');$wrong=$markerText.Replace($schemaLines[0],' Need $false 7')
 Assert-S ($skip -cne $markerText -and $wrong -cne $markerText) 46
 for($i=0;$i -lt 4;$i++){
  $null=Line $mutations[$i] $lengths[$i]
  $r=Try-Marker $markerText $mutations[$i] 'entry_control';Assert-S (Is-ExactFailure $r 'control_8') 47
  $skipped=Try-Marker $skip $mutations[$i] 'entry_control';Assert-S ($null -eq $skipped.error -and $skipped.output.Count -eq 0 -and -not (Is-ExactFailure $skipped 'control_8')) 48
  $wrongId=Try-Marker $wrong $mutations[$i] 'entry_control';Assert-S ((Is-ExactFailure $wrongId 'control_7') -and -not (Is-ExactFailure $wrongId 'control_8')) 49
 }
 Finish-S
 Start-S 'separate-oversize-refusal'
 $b=Exercise $newCheck $false 'retired_parent_control';$null=Line $b.stderr 155
 $r=Try-Marker $markerText $b.stderr 'retired_parent_control';Assert-S ($null -eq $r.error -and $r.output.Count -eq 0) 44
 $over=$b.stderr.Replace('"code":','"extra":true,"code":');$null=Line $over 168
 $r=Try-Marker $markerText $over 'retired_parent_control';Assert-S (Is-ExactFailure $r 'control_7') 50
 $b=Exercise $newCheck $false 'ownership_or_reader_incomplete';$long=Line $b.stderr 163
 $r=Try-Marker $markerText $b.stderr 'ownership_or_reader_incomplete';Assert-S ($null -eq $r.error -and $r.output.Count -eq 0) 44
 # Trailing JSON whitespace leaves the schema intact while crossing the exact A7 size boundary.
 $over=$long+' ';$null=Line $over 164
 $r=Try-Marker $markerText $over 'ownership_or_reader_incomplete';Assert-S (Is-ExactFailure $r 'control_7') 51
 Finish-S
 Start-S 'line-and-prefix-sensitivity'
 $b=Exercise $newCheck $false 'entry_control';$line=Line $b.stderr 146
 foreach($ending in @("`n","`r`n")){
  $r=Try-Marker $markerText ($line+$ending) 'entry_control';Assert-S ($null -eq $r.error -and $r.output.Count -eq 0) 44
  $r=Try-Marker $markerText ($ending+$line+$ending+$ending) 'entry_control';Assert-S ($null -eq $r.error -and $r.output.Count -eq 0) 44
  $r=Try-Marker $markerText ($line+$ending+$line+$ending) 'entry_control';Assert-S (Is-ExactFailure $r 'control_7') 52
 }
 $bad='X'+$line.Substring(1);Assert-S ($bad.Length -eq 146 -and @($bad -split '\r?\n').Count -eq 1) 53
 $r=Try-Marker $markerText $bad 'entry_control';Assert-S (Is-ExactFailure $r 'control_7') 54
 Finish-S
 Start-S 'redaction-and-no-normalization'
 foreach($name in @('RETIRED_PARENT_CONTROL','private-path-secret-value',"bad`nrecord",'', $null)){
  $a=Exercise $oldCheck $false $name;$b=Exercise $newCheck $false $name;Same-Error $a $b
  Assert-S ($a.stderr -ceq '' -and -not $b.stderr.Contains('private-path-secret-value')) 55
  $null=Line $b.stderr 140;$r=Try-Marker $markerText $b.stderr 'unknown';Assert-S ($null -eq $r.error -and $r.output.Count -eq 0) 44
 }
 Finish-S
 Start-S 'exact-reversal-and-population-boundary'
 $original=Get-Content -Raw (Join-Path $PSScriptRoot 'failed-controls.ps1');$candidate=Get-Content -Raw (Join-Path $PSScriptRoot 'controls.ps1')
 $old1="`$valid=(Exercise `$newCheck `$false 'retired_parent_control').stderr";$new1="`$valid=(Exercise `$newCheck `$false 'entry_control').stderr"
 $old2="try{Marker `$bad 'retired_parent_control'}";$new2="try{Marker `$bad 'entry_control'}"
 Assert-S (([regex]::Matches($candidate,[regex]::Escape($new1))).Count -eq 1 -and ([regex]::Matches($candidate,[regex]::Escape($new2))).Count -eq 1) 56
 Assert-S ($candidate.Replace($new1,$old1).Replace($new2,$old2) -ceq $original) 57
 $known=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-controls.json')|ConvertFrom-Json)
 $expected=@('exact-check-pass-and-refusal','assertion-redaction-and-no-normalization','installed-writer-fault-preserves-refusal','check-expression-and-catch-composition','exact-one-option-source-reversal','native-parent-retirement-and-owned-descendant','default-vs-detached-discriminator','ownership-refusal-and-bounds-sensitivity')
 Assert-S ($known.Count -eq 8 -and ($known -join '|') -ceq ($expected -join '|') -and -not ('OwnedLifetime' -as [type]) -and -not ('OwnedSetup' -as [type])) 58
 Finish-S
} finally {
 [Console]::SetError($savedWriter);$Evidence.writerRestored=[object]::ReferenceEquals([Console]::Error,$savedWriter)
 $Evidence.pathUnchanged=$env:PATH -ceq $savedPath;$Evidence.ofsUnchanged=$OFS -ceq $savedOFS
}
Assert-S ($Evidence.writerRestored -and $Evidence.pathUnchanged -and $Evidence.ofsUnchanged -and $rows.Count -eq 6) 59
$Evidence.complete=$true
ConvertTo-Json -InputObject @($rows.ToArray()) -Compress -Depth 4

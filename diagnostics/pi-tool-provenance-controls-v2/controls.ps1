param([hashtable]$Progress=@{case=$null;assertion=0;completed=@()})
$ErrorActionPreference='Stop'
$expected=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-controls.json')|ConvertFrom-Json)
$original=Get-Content -Raw (Join-Path $PSScriptRoot 'original-tool-provenance.ps1')
$candidate=Get-Content -Raw (Join-Path $PSScriptRoot 'tool-provenance.ps1')
$script:controlProgress=$Progress
function Need([bool]$Value,[int]$Id){$script:controlProgress.assertion=$Id;if(-not $Value){throw 'control_assertion'}}
function Function-Text([string]$Source,[string]$Name){
 $tokens=$null;$errors=$null;$ast=[Management.Automation.Language.Parser]::ParseInput($Source,[ref]$tokens,[ref]$errors)
 Need ($errors.Count -eq 0) 1
 $matches=@($ast.FindAll({param($a) $a -is [Management.Automation.Language.FunctionDefinitionAst] -and $a.Name -ceq $Name},$true))
 Need ($matches.Count -eq 1) 2
 return $matches[0].Extent.Text
}
$oldTree=Function-Text $original 'Get-BoundedTree';$oldTools=Function-Text $original 'Get-SetupTools'
$newTree=Function-Text $candidate 'Get-BoundedTree';$newTools=Function-Text $candidate 'Get-SetupTools'
$setProbe=Function-Text $candidate 'Set-ToolProbe';$writeProbe=Function-Text $candidate 'Write-ToolProbeFailure'
$oldBlock=[scriptblock]::Create($oldTree.Replace('Get-BoundedTree','Original-Tree')+"`n"+$oldTools.Replace('Get-SetupTools','Original-Tools').Replace('Get-BoundedTree','Original-Tree'))
$newBlock=[scriptblock]::Create($setProbe+"`n"+$writeProbe+"`n"+$newTree+"`n"+$newTools)
. ([scriptblock]::Create($setProbe+"`n"+$writeProbe))
function Canonical($Value){
 if($Value -is [Collections.IDictionary]){$map=[ordered]@{};foreach($k in @($Value.Keys|Sort-Object)){$map[$k]=Canonical $Value[$k]};return $map}
 if($Value -is [array]){return ,@($Value|ForEach-Object {Canonical $_})}
 return $Value
}
function Json($Value){return ((Canonical $Value)|ConvertTo-Json -Depth 20 -Compress)}
function Run-Tools([bool]$Marked,[string]$Mode,[Exception]$Injected=$null,[switch]$BrokenWriter,[string[]]$FollowingModes=@()){
 & {
  $fx=@{mode=$Mode;ledger=[Collections.Generic.List[string]]::new();injected=$Injected}
  $fakeRoot=Join-Path ([IO.Path]::GetTempPath()) 'fixed-provenance-control'
  $fakeNpm=Join-Path $fakeRoot 'npm/bin/npm-cli.js';$fakeNode=Join-Path $fakeRoot 'node.exe';$npmRoot=Join-Path $fakeRoot 'npm'
  function Test-Path {param([string]$LiteralPath) $fx.ledger.Add('exists|'+$LiteralPath);return ($fx.mode -ne 'entry-missing')}
  function Get-Content {param([switch]$Raw,[string]$LiteralPath)
   $fx.ledger.Add('read|'+$LiteralPath)
   if($fx.mode -eq 'package-read'){throw $fx.injected}
   if($fx.mode -eq 'package-parse'){return '{secret malformed package input'}
   if($fx.mode -eq 'package-name'){return '{"name":"other","version":"1.2.3"}'}
   if($fx.mode -eq 'package-version'){return '{"name":"npm","version":"bad secret package value"}'}
   return '{"name":"npm","version":"1.2.3"}'
  }
  function Get-ChildItem {param([string]$LiteralPath,[switch]$Recurse,[switch]$Force)
   $fx.ledger.Add('enumerate|'+$LiteralPath)
   if($fx.mode -eq 'tree-enumerate'){throw $fx.injected}
   $role=if($LiteralPath -eq $npmRoot){'npm'}else{'powershell'}
   $attrs=[IO.FileAttributes]::Normal
   if($fx.mode -eq ('reparse-'+$role)){$attrs=[IO.FileAttributes]::ReparsePoint}
   $length=if($fx.mode -eq 'file-cap'){268435457L}else{1L}
   return [pscustomobject]@{Attributes=$attrs;PSIsContainer=$false;Length=$length;FullName=(Join-Path $LiteralPath 'fixture.dat')}
  }
  function Get-FileHash {param([string]$Algorithm,[string]$LiteralPath)
   $fx.ledger.Add('hash|'+$LiteralPath)
   if(($fx.mode -eq 'tree-hash' -and $LiteralPath.EndsWith('fixture.dat')) -or ($fx.mode -eq 'node-hash' -and $LiteralPath -eq $fakeNode)){throw $fx.injected}
   return [pscustomobject]@{Hash=('A'*64)}
  }
  if($Marked){. $newBlock}else{. $oldBlock}
  $savedOS=[Environment]::GetEnvironmentVariable('ImageOS');$savedVersion=[Environment]::GetEnvironmentVariable('ImageVersion');$savedError=[Console]::Error
  $writer=[IO.StringWriter]::new();$allResults=[Collections.Generic.List[object]]::new()
  try {
   if($BrokenWriter){$writer.Dispose()}
   [Console]::SetError($writer)
   $installedFault=$false
   if($BrokenWriter){try{[Console]::Error.WriteLine('probe')}catch{$installedFault=$true};Need $installedFault 3}
   # Function definitions and their containing scope remain loaded across this entire sequence.
   # Only fixture input/output bookkeeping changes; candidate diagnostic context is never reset here.
   foreach($currentMode in (@($Mode)+$FollowingModes)){
    $fx.mode=$currentMode;$fx.ledger.Clear();$result=$null;$failure=$null
    $outputs=[Collections.Generic.List[object]]::new()
    if(-not $BrokenWriter){$null=$writer.GetStringBuilder().Clear()}
    [Environment]::SetEnvironmentVariable('ImageOS','windows-fixture');[Environment]::SetEnvironmentVariable('ImageVersion','1.2.3')
    if($currentMode -eq 'image-os'){[Environment]::SetEnvironmentVariable('ImageOS','secret invalid value /x')}
    if($currentMode -eq 'image-version'){[Environment]::SetEnvironmentVariable('ImageVersion',$null)}
    try {
     if($Marked){Get-SetupTools $fakeNode $fakeNpm|ForEach-Object {$outputs.Add($_)}}
     else{Original-Tools $fakeNode $fakeNpm|ForEach-Object {$outputs.Add($_)}}
    }catch{$failure=$_}
    if($outputs.Count -eq 1){$result=$outputs[0]}elseif($outputs.Count -gt 1){$result=@($outputs.ToArray())}
    $text=if($BrokenWriter){''}else{$writer.ToString()}
    $allResults.Add([pscustomobject]@{value=$result;error=$failure;stderr=$text;ledger=@($fx.ledger.ToArray());injected=$Injected;successOutputCount=$outputs.Count;installedWriterFault=$installedFault})
   }
  } finally {
   [Console]::SetError($savedError);[Environment]::SetEnvironmentVariable('ImageOS',$savedOS);[Environment]::SetEnvironmentVariable('ImageVersion',$savedVersion);$writer.Dispose()
  }
  return $allResults.ToArray()
 }
}
function Decode([string]$Text){
 $lines=@($Text -split '\r?\n'|Where-Object {$_ -ne ''});Need ($lines.Count -eq 1) 4
 Need ($lines[0].StartsWith('TOOL_PROBE_FAILURE_V1 ') -and $lines[0].Length -le 1024) 5
 $record=$lines[0].Substring('TOOL_PROBE_FAILURE_V1 '.Length)|ConvertFrom-Json
 Need ((@($record.PSObject.Properties.Name|Sort-Object)-join '|') -ceq 'category|hresult|markerValid|predicate|schema|stage|tree') 6
 return $record
}
function Pair([string]$Mode,[string]$Predicate,[string]$Stage='',[string]$Tree='',[Exception]$Injected=$null){
 $a=Run-Tools $false $Mode $Injected;$b=Run-Tools $true $Mode $Injected
 Need ((Json $a.ledger) -ceq (Json $b.ledger)) 7
 Need ($null -ne $a.error -and $null -ne $b.error -and $a.stderr -ceq '') 8
 Need ($a.error.Exception.GetType() -eq $b.error.Exception.GetType() -and $a.error.Exception.HResult -eq $b.error.Exception.HResult -and $a.error.CategoryInfo.Category -eq $b.error.CategoryInfo.Category) 9
 Need ($a.error.Exception.Message -ceq $b.error.Exception.Message -and $a.error.FullyQualifiedErrorId -ceq $b.error.FullyQualifiedErrorId -and $a.successOutputCount -eq 0 -and $b.successOutputCount -eq 0) 10
 if($null -ne $Injected){Need ([Object]::ReferenceEquals($a.error.Exception,$Injected) -and [Object]::ReferenceEquals($b.error.Exception,$Injected)) 11}
 $rec=Decode $b.stderr;Need ($rec.markerValid -and $rec.predicate -ceq $Predicate) 12
 if($Stage -ne ''){Need ($rec.stage -ceq $Stage) 13};if($Tree -ne ''){Need ($rec.tree -ceq $Tree) 14}
 Need (-not $b.stderr.Contains('secret invalid') -and -not $b.stderr.Contains('secret package') -and -not $b.stderr.Contains('secret malformed')) 15
 return $rec
}
$rows=[Collections.Generic.List[object]]::new()
function Case([string]$Name,[scriptblock]$Body){
 $script:controlProgress.case=$Name;$script:controlProgress.assertion=0
 $output=@(& $Body);Need ($output.Count -eq 0) 16
 $rows.Add(@{name=$Name;outcome='PASS'});$script:controlProgress.completed=@($script:controlProgress.completed)+$Name
}
Case 'nominal-return-and-ledger' {
 $a=Run-Tools $false 'nominal';$b=Run-Tools $true 'nominal'
 Need ($null -eq $a.error -and $null -eq $b.error) 17
 Need ((Json $a.value) -ceq (Json $b.value) -and (Json $a.ledger) -ceq (Json $b.ledger)) 18
 Need ($a.stderr -ceq '' -and $b.stderr -ceq '') 19
}
Case 'npm-entry-refusal' {$null=Pair 'entry-missing' 'npm_entry_missing' 'npm-entry'}
Case 'npm-package-identity-refusal' {$null=Pair 'package-name' 'npm_package_identity' 'npm-identity';$null=Pair 'package-version' 'npm_package_identity' 'npm-identity'}
Case 'host-identity-refusal' {
 # Exact original/candidate if-statements isolated: no attempt to mutate Environment.ProcessPath.
 $oldLine=@($oldTools -split "`n"|Where-Object {$_ -like '*if((Split-Path $hostProcess*'});$newLine=@($newTools -split "`n"|Where-Object {$_ -like '*if((Split-Path $hostProcess*'})
 Need ($oldLine.Count -eq 1 -and $newLine.Count -eq 1) 20
 $hostProcess='C:\fixture\other.exe';$toolProbe=@{value=$null};$caught=@()
 foreach($line in @($oldLine[0],$newLine[0])){try{& ([scriptblock]::Create($line));$caught+=@('returned')}catch{$caught+=@($_.Exception.Message)}}
 Need ($caught.Count -eq 2 -and $caught[0] -ceq 'powershell_host_identity' -and $caught[1] -ceq $caught[0]) 21
 Need ($toolProbe.value.predicate -ceq 'powershell_host_identity') 22
}
Case 'image-shape-refusal' {$null=Pair 'image-os' 'hosted_image_metadata' 'image-shape';$null=Pair 'image-version' 'hosted_image_metadata' 'image-shape'}
Case 'tree-reparse-refusal' {$null=Pair 'reparse-npm' 'tool_reparse_point' 'tree-entry' 'npm';$null=Pair 'reparse-powershell' 'tool_reparse_point' 'tree-entry' 'powershell'}
Case 'tree-caps-refusal' {
 $null=Pair 'file-cap' 'tool_inventory_cap' 'tree-file'
 # Evaluate original exact finite predicate at all three boundaries; no 20001 hashes/large files allocated.
 $tokens=$null;$errors=$null;$ast=[Management.Automation.Language.Parser]::ParseInput($oldTree,[ref]$tokens,[ref]$errors)
 $conditions=@($ast.FindAll({param($a) $a -is [Management.Automation.Language.IfStatementAst] -and $a.Extent.Text.StartsWith('if($map.Count -ge 20000')},$true));Need ($conditions.Count -eq 1) 23
 $predicate=$conditions[0].Clauses[0].Item1.Extent.Text;Need ($newTree.Contains($predicate)) 24
 foreach($v in @(@(19999,2147483648L,268435456L,$false),@(20000,0L,1L,$true),@(0,2147483649L,1L,$true),@(0,1L,268435457L,$true))){$map=[pscustomobject]@{Count=$v[0]};$total=$v[1];$f=[pscustomobject]@{Length=$v[2]};$value=& ([scriptblock]::Create($predicate));Need ($value -eq $v[3]) 25}
}
Case 'provider-and-hash-errors' {
 foreach($pair in @(@('package-read','npm-package'),@('tree-enumerate','tree-enumerate'),@('tree-hash','tree-hash'))){$exception=[IO.IOException]::new('secret injected error');$null=Pair $pair[0] 'none' $pair[1] '' $exception}
 $null=Pair 'package-parse' 'none' 'npm-package'
}
Case 'record-construction-error' {$null=Pair 'node-hash' 'none' 'record-construction' '' ([IO.IOException]::new('secret node error'))}
Case 'fresh-invocation-reset' {
 $exception=[IO.IOException]::new('secret next error')
 $a=@(Run-Tools $false 'entry-missing' $exception -FollowingModes @('package-read'))
 $b=@(Run-Tools $true 'entry-missing' $exception -FollowingModes @('package-read'))
 Need ($a.Count -eq 2 -and $b.Count -eq 2) 26
 for($i=0;$i -lt 2;$i++){
  Need ((Json $a[$i].ledger) -ceq (Json $b[$i].ledger) -and $a[$i].stderr -ceq '' -and $a[$i].successOutputCount -eq 0 -and $b[$i].successOutputCount -eq 0) 26
  Need ($null -ne $a[$i].error -and $null -ne $b[$i].error -and $a[$i].error.Exception.GetType() -eq $b[$i].error.Exception.GetType() -and $a[$i].error.Exception.HResult -eq $b[$i].error.Exception.HResult -and $a[$i].error.CategoryInfo.Category -eq $b[$i].error.CategoryInfo.Category -and $a[$i].error.Exception.Message -ceq $b[$i].error.Exception.Message) 26
 }
 Need ([Object]::ReferenceEquals($a[1].error.Exception,$exception) -and [Object]::ReferenceEquals($b[1].error.Exception,$exception)) 26
 $first=Decode $b[0].stderr;$second=Decode $b[1].stderr
 Need ($first.predicate -ceq 'npm_entry_missing' -and $second.markerValid -and $second.predicate -ceq 'none' -and $second.stage -ceq 'npm-package' -and $second.tree -ceq 'none') 26
}
function Capture-Helper($Context,[string]$Category='OperationStopped',[int]$HResult=-2146233087){
 $saved=[Console]::Error;$writer=[IO.StringWriter]::new();try{[Console]::SetError($writer);$out=@(Write-ToolProbeFailure $Context $Category $HResult);Need ($out.Count -eq 0) 27;return $writer.ToString()}finally{[Console]::SetError($saved);$writer.Dispose()}
}
Case 'missing-malformed-marker-redaction' {
 $clean=@{value=@{stage='npm-entry';predicate='npm_entry_missing';tree='none'}}
 $extra=@{value=@{stage='npm-entry';predicate='npm_entry_missing';tree='none';secret='secret hidden path'};secret='secret environment'}
 $a=Capture-Helper $clean;$b=Capture-Helper $extra;Need ($a -ceq $b -and -not $b.Contains('secret')) 28
 foreach($bad in @($null,@{},@{value=@{stage='npm-entry';tree='none'}},@{value=@{stage='secret';predicate='none';tree='none'}})){$x=Decode (Capture-Helper $bad);Need (-not $x.markerValid -and $x.stage -ceq 'unknown' -and $x.predicate -ceq 'unknown' -and $x.tree -ceq 'unknown') 29}
}
Case 'category-hresult-schema' {
 foreach($number in @([int]::MinValue,[int]::MaxValue)){$x=Decode (Capture-Helper @{value=@{stage='npm-entry';predicate='none';tree='none'}} 'secret category' $number);Need ($x.category -ceq 'Other' -and $x.hresult -eq $number) 30}
}
Case 'disposed-writer-original-throw' {
 $exception=[IO.IOException]::new('secret writer failure');$a=Run-Tools $false 'package-read' $exception;$b=Run-Tools $true 'package-read' $exception -BrokenWriter
 Need ($b.installedWriterFault -and [Object]::ReferenceEquals($a.error.Exception,$exception) -and [Object]::ReferenceEquals($b.error.Exception,$exception) -and $b.stderr -ceq '') 31
 Need ((Json $a.ledger) -ceq (Json $b.ledger) -and $a.error.Exception.HResult -eq $b.error.Exception.HResult -and $a.error.CategoryInfo.Category -eq $b.error.CategoryInfo.Category -and $a.error.FullyQualifiedErrorId -ceq $b.error.FullyQualifiedErrorId -and $a.successOutputCount -eq 0 -and $b.successOutputCount -eq 0) 31
}
Case 'bookkeeping-fault-transparency' {
 $map=[Collections.Generic.Dictionary[string,object]]::new();$map.Add('value',@{stage='npm-entry';predicate='none';tree='none'})
 $readOnly=[Collections.ObjectModel.ReadOnlyDictionary[string,object]]::new($map)
 $direct=$false;try{$readOnly.value=@{}}catch{$direct=$true};Need $direct 32
 $out=@(Set-ToolProbe $readOnly 'image-shape' 'hosted_image_metadata');Need ($out.Count -eq 0 -and $readOnly.value.stage -ceq 'npm-entry') 33
 $x=Decode (Capture-Helper $readOnly);Need ($x.markerValid -and $x.predicate -ceq 'none') 34
 # Execute exact original/marked refusal blocks with the faulting context, not just the helper.
 $oldRefusal=@($oldTools -split "`n"|Where-Object {$_ -like '*if(-not(Test-Path -LiteralPath $Npm))*'})
 $newRefusal=@($newTools -split "`n"|Where-Object {$_ -like '*if(-not(Test-Path -LiteralPath $Npm))*'})
 Need ($oldRefusal.Count -eq 1 -and $newRefusal.Count -eq 1) 34
 $refusalResults=@(& {
  function Test-Path {param([string]$LiteralPath) return $false}
  $Npm='fixed-fixture-name';$toolProbe=$readOnly
  foreach($line in @($oldRefusal[0],$newRefusal[0])){
   $seen=[Collections.Generic.List[object]]::new();$failure=$null
   try{& ([scriptblock]::Create($line))|ForEach-Object {$seen.Add($_)}}catch{$failure=$_}
   [pscustomobject]@{error=$failure;outputCount=$seen.Count}
  }
 })
 Need ($refusalResults.Count -eq 2) 34
 $a=$refusalResults[0];$b=$refusalResults[1]
 Need ($null -ne $a.error -and $null -ne $b.error -and $a.error.Exception.Message -ceq 'npm_entry_missing' -and $b.error.Exception.Message -ceq $a.error.Exception.Message -and $a.error.Exception.GetType() -eq $b.error.Exception.GetType() -and $a.error.Exception.HResult -eq $b.error.Exception.HResult -and $a.error.CategoryInfo.Category -eq $b.error.CategoryInfo.Category -and $a.outputCount -eq 0 -and $b.outputCount -eq 0) 34
 # Admissible stale marker is deliberately not treated as causal evidence.
}
Case 'existing-capture-composition' {
 . (Join-Path $PSScriptRoot 'capture-functions.ps1')
 $setup=Get-Content -Raw (Join-Path $PSScriptRoot 'setup-only.ps1')
 $start=$setup.IndexOf('} catch {'+"`n"+' $code=$_.Exception.HResult; $outcome=');Need ($start -ge 0) 35
 $end=$setup.IndexOf("`n}"+"`nfinally {",$start);Need ($end -gt $start) 36
 $catchBody=$setup.Substring($start+'} catch {'.Length,$end-$start-'} catch {'.Length)
 $saved=[Console]::Error;$writer=[IO.StringWriter]::new();$diagnosticStage='tool-provenance';$script:ownedInvocationStarted=$false;$outcome='PASS';$code=0
 try {
  [Console]::SetError($writer)
  $b=Run-Tools $true 'entry-missing';[Console]::Error.Write($b.stderr)
  try{throw $b.error}catch{Invoke-Expression $catchBody}
 }finally{[Console]::SetError($saved)}
 $lines=@($writer.ToString() -split '\r?\n'|Where-Object {$_ -ne ''});$writer.Dispose()
 Need ($lines.Count -eq 2 -and $lines[0].StartsWith('TOOL_PROBE_FAILURE_V1 ') -and $lines[1].StartsWith('SETUP_FAILURE_V1 ')) 37
 $rec=$lines[1].Substring('SETUP_FAILURE_V1 '.Length)|ConvertFrom-Json
 Need ($outcome -ceq 'FAILED' -and $rec.phase -ceq 'original' -and $rec.exitCode -eq 1 -and $rec.ownership -ceq 'not-initialized') 38
}
Need ($rows.Count -eq 15 -and $expected.Count -eq 15) 39
for($i=0;$i -lt 15;$i++){Need ($rows[$i].name -ceq $expected[$i]) 40}
ConvertTo-Json -InputObject @($rows.ToArray()) -Depth 4 -Compress

param([hashtable]$Progress=@{case=$null;assertion=0;completed=@()})
$ErrorActionPreference='Stop'
$script:controlProgress=$Progress
function Need([bool]$Value,[int]$Id){$script:controlProgress.assertion=$Id;if(-not $Value){throw 'control_assertion'}}
function Function-Text([string]$Source,[string]$Name){
 $tokens=$null;$errors=$null;$ast=[Management.Automation.Language.Parser]::ParseInput($Source,[ref]$tokens,[ref]$errors)
 Need ($errors.Count -eq 0) 1
 $found=@($ast.FindAll({param($a) $a -is [Management.Automation.Language.FunctionDefinitionAst] -and $a.Name -ceq $Name},$true));Need ($found.Count -eq 1) 2
 return $found[0].Extent.Text
}
function Canonical($Value){
 if($Value -is [Collections.IDictionary]){$map=[ordered]@{};foreach($k in @($Value.Keys|Sort-Object)){$map[$k]=Canonical $Value[$k]};return $map}
 if($Value -is [array]){return ,@($Value|ForEach-Object {Canonical $_})}
 return $Value
}
function Json($Value){return ((Canonical $Value)|ConvertTo-Json -Depth 20 -Compress)}
$expected=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-controls.json')|ConvertFrom-Json)
$old=Get-Content -Raw (Join-Path $PSScriptRoot 'original-tool-provenance.ps1')
$new=Get-Content -Raw (Join-Path $PSScriptRoot 'tool-provenance.ps1')
$functionNames=@('Set-ToolProbe','Write-ToolProbeFailure','Get-BoundedTree','Get-SetupTools')
$oldBlock=[scriptblock]::Create((@($functionNames|ForEach-Object {Function-Text $old $_}) -join "`n"))
$newBlock=[scriptblock]::Create((Function-Text $new 'Write-NpmSelectionFailure')+"`n"+(@($functionNames|ForEach-Object {Function-Text $new $_}) -join "`n"))
function Run-Tools([bool]$Marked,[string]$Mode='missing',[string]$Node='C:\secret-fixture\selected\node.exe',[string]$Npm='C:\secret-fixture\selected\node_modules\npm\bin\npm-cli.js',[switch]$BrokenWriter,[Exception]$Injected=$null,[string[]]$FollowingModes=@(),[Management.Automation.ErrorRecord]$ProbeError=$null){
 & {
  $fx=@{mode=$Mode;ledger=[Collections.Generic.List[string]]::new();injected=$Injected;probeError=$ProbeError;boundary=$null;observerCalled=$false}
  function Test-Path {param([string]$LiteralPath,[string]$PathType,[string]$ErrorAction)
   $fx.ledger.Add('exists|'+$LiteralPath+'|'+$PathType+'|'+$ErrorAction)
   if($PathType -eq 'Leaf'){
    if($fx.mode -eq 'sibling-error'){if($null -ne $fx.probeError){throw $fx.probeError};throw $fx.injected}
    return ($fx.mode -notin @('sibling-absent','other-absent'))
   }
   if($fx.mode -eq 'original-provider-error'){throw $fx.injected}
   return ($fx.mode -in @('success','package-error','package-refusal'))
  }
  function Get-Command {param([string]$Name,[string]$CommandType,[string]$ErrorAction)
   $fx.ledger.Add('command|'+$Name+'|'+$CommandType+'|'+$ErrorAction)
   if($fx.mode -eq 'command-error'){if($null -ne $fx.probeError){throw $fx.probeError};throw $fx.injected}
   if($fx.mode -eq 'no-command'){return}
   if($fx.mode -eq 'multiple-command'){return @([pscustomobject]@{Source='C:\secret-one\npm.cmd'},[pscustomobject]@{Source='C:\secret-two\npm.cmd'})}
   $source=switch($fx.mode){
    'source-not-string' {42;break}
    'source-relative' {'relative\npm.cmd';break}
    'source-drive-relative' {'C:npm.cmd';break}
    'source-root-relative' {'\secret-fixture\selected\npm.cmd';break}
    'source-wrong-leaf' {'C:\secret-fixture\selected\not-npm.cmd';break}
    'source-oversize' {'C:\'+('s'*32769)+'\npm.cmd';break}
    'other-present' {'C:\secret-other\npm.cmd';break}
    'other-absent' {'C:\secret-other\npm.cmd';break}
    default {'C:\secret-fixture\selected\npm.cmd'}
   }
   return [pscustomobject]@{Source=$source;secret='never-export'}
  }
  function Get-Content {param([switch]$Raw,[string]$LiteralPath)
   $fx.ledger.Add('read|'+$LiteralPath)
   if($fx.mode -eq 'package-error'){throw $fx.injected}
   if($fx.mode -eq 'package-refusal'){return '{"name":"not-npm","version":"1.2.3"}'}
   return '{"name":"npm","version":"1.2.3"}'
  }
  function Get-ChildItem {param([string]$LiteralPath,[switch]$Recurse,[switch]$Force)
   $fx.ledger.Add('tree|'+$LiteralPath)
   return [pscustomobject]@{Attributes=[IO.FileAttributes]::Normal;PSIsContainer=$false;Length=1L;FullName=(Join-Path $LiteralPath 'fixture.dat')}
  }
  function Get-FileHash {param([string]$Algorithm,[string]$LiteralPath)
   $fx.ledger.Add('hash|'+$LiteralPath);return [pscustomobject]@{Hash=('A'*64)}
  }
  if($Marked){. $newBlock}else{. $oldBlock}
  # Observe the existing catch's ErrorRecord at its logging boundary, then delegate
  # the exact saved writer. The decision function and its catch are unmodified.
  $savedToolWriter=${function:Write-ToolProbeFailure}
  function Write-ToolProbeFailure($Context,[string]$Category,[int]$HResult){$fx.observerCalled=$true;$fx.boundary=$_;& $savedToolWriter $Context $Category $HResult}
  $savedError=[Console]::Error;$savedOS=[Environment]::GetEnvironmentVariable('ImageOS');$savedVersion=[Environment]::GetEnvironmentVariable('ImageVersion')
  $writer=[IO.StringWriter]::new();$all=[Collections.Generic.List[object]]::new()
  try {
   [Environment]::SetEnvironmentVariable('ImageOS','windows-fixture');[Environment]::SetEnvironmentVariable('ImageVersion','1.2.3')
   if($BrokenWriter){$writer.Dispose()};[Console]::SetError($writer)
   $directFault=$false;if($BrokenWriter){try{[Console]::Error.WriteLine('direct-fault-proof')}catch{$directFault=$true};Need $directFault 3}
   foreach($m in (@($Mode)+$FollowingModes)){
    $fx.mode=$m;$fx.ledger.Clear();$fx.boundary=$null;$fx.observerCalled=$false;$outputs=[Collections.Generic.List[object]]::new();$failure=$null
    if(-not $BrokenWriter){$null=$writer.GetStringBuilder().Clear()}
    try{Get-SetupTools $Node $Npm|ForEach-Object {$outputs.Add($_)}}catch{$failure=$_}
    $all.Add([pscustomobject]@{error=$failure;boundary=$fx.boundary;observerCalled=$fx.observerCalled;mode=$m;ledger=@($fx.ledger.ToArray());stdout=@($outputs.ToArray());stderr=$(if($BrokenWriter){''}else{$writer.ToString()});writerFault=$directFault})
   }
  }finally{[Console]::SetError($savedError);[Environment]::SetEnvironmentVariable('ImageOS',$savedOS);[Environment]::SetEnvironmentVariable('ImageVersion',$savedVersion);$writer.Dispose()}
  return $all.ToArray()
 }
}
function Lines([string]$Text){return @($Text -split '\r?\n'|Where-Object {$_ -ne ''})}
function Decode-Selection([string]$Text){
 $lines=@(Lines $Text);Need ($lines.Count -eq 2 -and $lines[0].StartsWith('TOOL_PROBE_FAILURE_V1 ') -and $lines[1].StartsWith('NPM_SELECTION_FAILURE_V1 ')) 4
 Need ($lines[0].Length -le 1024 -and $lines[1].Length -le 1024) 5
 $r=$lines[1].Substring('NPM_SELECTION_FAILURE_V1 '.Length)|ConvertFrom-Json
 Need ((@($r.PSObject.Properties.Name|Sort-Object)-join '|') -ceq 'category|command|commandShape|derivedRelation|hresult|nodeShape|originalPredicate|parentRelation|probeStage|schema|siblingEntry') 6
 Need ($r.schema -ceq 'npm-selection-failure-v1' -and $r.originalPredicate -ceq 'npm_entry_missing') 7
 Need ($r.nodeShape -cin @('unknown','rooted-node-exe') -and $r.derivedRelation -cin @('unknown','matches-node-parent','different')) 8
 Need ($r.command -cin @('unread','missing','multiple','one','error') -and $r.commandShape -cin @('unknown','rooted-npm-cmd')) 9
 Need ($r.parentRelation -cin @('unknown','same','different') -and $r.siblingEntry -cin @('unread','present','absent')) 10
 Need ($r.probeStage -cin @('input-shape','npm-command','path-relation','sibling-existence','complete') -and $r.category -cin @('none','ObjectNotFound','PermissionDenied','InvalidArgument','InvalidData','ReadError','SecurityError','Other')) 11
 Need (($r.hresult -is [int] -or $r.hresult -is [long]) -and $r.hresult -ge [int]::MinValue -and $r.hresult -le [int]::MaxValue) 12
 Need (-not $Text.Contains('secret') -and -not $Text.Contains('never-export') -and -not $Text.Contains('C:\')) 13
 return $r
}
# Metadata proposal only. Values are fixed enums/booleans/null; no error objects leave memory.
function Set-IdentityDetail($Progress,[string]$Side,$Result) {
 try {
  $cases=@('original-success-no-probe','original-other-refusal-no-probe','missing-entry-same-parent','missing-entry-other-parent','selection-shape-and-command-errors','path-and-existence-errors','installed-writer-fault-and-redaction','capture-composition-and-scope')
  $modes=@('missing','success','original-provider-error','package-error','package-refusal','sibling-absent','other-present','other-absent','no-command','multiple-command','source-not-string','source-relative','source-drive-relative','source-root-relative','source-wrong-leaf','source-oversize','command-error','sibling-error')
  $valid=$Progress.case -cin $cases -and $Side -cin @('original','marked') -and $Result.mode -cin $modes -and $Result.observerCalled -is [bool]
  $case='unknown';$mode='unknown';$side='unknown'
  if($valid){$case=$Progress.case;$mode=$Result.mode;$side=$Side}
  $outward=$Result.error -is [Management.Automation.ErrorRecord]
  $observed=$Result.boundary -is [Management.Automation.ErrorRecord]
  $outwardException=$outward -and $Result.error.Exception -is [Exception]
  $observedException=$observed -and $Result.boundary.Exception -is [Exception]
  $sameRecord=$null;$sameException=$null
  if($outward -and $observed){$sameRecord=[Object]::ReferenceEquals($Result.error,$Result.boundary)}
  if($outwardException -and $observedException){$sameException=[Object]::ReferenceEquals($Result.error.Exception,$Result.boundary.Exception)}
  $Progress.identityDetail=[ordered]@{schema='identity-comparison-v1';valid=[bool]$valid;case=$case;mode=$mode;side=$side;observerCalled=[bool]$Result.observerCalled;outwardRecord=$outward;observedRecord=$observed;outwardException=$outwardException;observedException=$observedException;sameRecord=$sameRecord;sameException=$sameException}
 } catch { }
}
function Compare-Failure($A,$B,[Exception]$Injected=$null){
 Need ($null -ne $A.error -and $null -ne $B.error -and $A.stdout.Count -eq 0 -and $B.stdout.Count -eq 0) 14
 Need ($A.error.Exception.GetType() -eq $B.error.Exception.GetType() -and $A.error.Exception.HResult -eq $B.error.Exception.HResult -and $A.error.CategoryInfo.Category -eq $B.error.CategoryInfo.Category -and $A.error.FullyQualifiedErrorId -ceq $B.error.FullyQualifiedErrorId -and $A.error.Exception.Message -ceq $B.error.Exception.Message) 15
 foreach($x in @($A,$B)){try{$script:controlProgress.identityDetail=$null}catch{};Set-IdentityDetail $script:controlProgress $(if([Object]::ReferenceEquals($x,$A)){'original'}else{'marked'}) $x;Need ([Object]::ReferenceEquals($x.error,$x.boundary) -and [Object]::ReferenceEquals($x.error.Exception,$x.boundary.Exception)) 16}
 if($null -ne $Injected){Need ([Object]::ReferenceEquals($A.error.Exception,$Injected) -and [Object]::ReferenceEquals($B.error.Exception,$Injected)) 17}
 Need ($B.ledger.Count -ge $A.ledger.Count) 18
 for($i=0;$i -lt $A.ledger.Count;$i++){Need ($A.ledger[$i] -ceq $B.ledger[$i]) 19}
}
function Pair([string]$Mode,[int]$Extra=2,[string]$Node='C:\secret-fixture\selected\node.exe',[string]$Npm='C:\secret-fixture\selected\node_modules\npm\bin\npm-cli.js',[Exception]$Injected=$null,[Management.Automation.ErrorRecord]$ProbeError=$null){
 $a=Run-Tools $false $Mode $Node $Npm -Injected $Injected -ProbeError $ProbeError;$b=Run-Tools $true $Mode $Node $Npm -Injected $Injected -ProbeError $ProbeError
 Compare-Failure $a $b
 Need ($b.ledger.Count -eq $a.ledger.Count+$Extra) 20
 Need ($b.ledger[$a.ledger.Count] -ceq 'command|npm.cmd|Application|Stop') 21
 if($Extra -eq 2){
  $parent=if($Mode -in @('other-present','other-absent')){'C:\secret-other'}elseif($Mode -eq 'source-root-relative'){'\secret-fixture\selected'}elseif($Mode -eq 'source-drive-relative'){'C:'}else{'C:\secret-fixture\selected'}
  $expectedPath=[IO.Path]::Combine($parent,'node_modules/npm/bin/npm-cli.js')
  Need ($b.ledger[-1] -ceq ('exists|'+$expectedPath+'|Leaf|Stop')) 22
 }
 $before=@(Lines $a.stderr);$after=@(Lines $b.stderr);Need ($before.Count -eq 1 -and $before[0] -ceq $after[0]) 23
 return Decode-Selection $b.stderr
}
$rows=[Collections.Generic.List[object]]::new()
function Case([string]$Name,[scriptblock]$Body){$script:controlProgress.case=$Name;$script:controlProgress.assertion=0;try{$script:controlProgress.identityDetail=$null}catch{};$out=@(& $Body);Need ($out.Count -eq 0) 24;$rows.Add(@{name=$Name;outcome='PASS'});$script:controlProgress.completed=@($script:controlProgress.completed)+$Name}
Case 'original-success-no-probe' {
 $a=Run-Tools $false 'success';$b=Run-Tools $true 'success'
 Need ($null -eq $a.error -and $null -eq $b.error -and $a.stdout.Count -eq 1 -and $b.stdout.Count -eq 1) 25
 Need ((Json $a.stdout) -ceq (Json $b.stdout) -and (Json $a.ledger) -ceq (Json $b.ledger) -and $a.stderr -ceq '' -and $b.stderr -ceq '') 26
}
Case 'original-other-refusal-no-probe' {
 foreach($m in @('original-provider-error','package-error','package-refusal')){
  $e=[IO.IOException]::new('secret-original-error');$a=Run-Tools $false $m -Injected $e;$b=Run-Tools $true $m -Injected $e
  if($m -eq 'package-refusal'){Compare-Failure $a $b}else{Compare-Failure $a $b $e}
  Need ((Json $a.ledger) -ceq (Json $b.ledger) -and $a.stderr -ceq $b.stderr -and @(Lines $b.stderr).Count -eq 1) 27
 }
}
Case 'missing-entry-same-parent' {
 $r=Pair 'missing';Need ($r.command -ceq 'one' -and $r.derivedRelation -ceq 'matches-node-parent' -and $r.parentRelation -ceq 'same' -and $r.siblingEntry -ceq 'present' -and $r.category -ceq 'none' -and $r.hresult -eq 0) 28
 $r=Pair 'sibling-absent';Need ($r.parentRelation -ceq 'same' -and $r.siblingEntry -ceq 'absent') 29
}
Case 'missing-entry-other-parent' {
 foreach($m in @('other-present','other-absent')){$r=Pair $m;Need ($r.parentRelation -ceq 'different' -and $r.siblingEntry -ceq $(if($m -eq 'other-present'){'present'}else{'absent'})) 30}
}
Case 'selection-shape-and-command-errors' {
 foreach($m in @('no-command','multiple-command','source-not-string','source-relative','source-wrong-leaf','source-oversize','command-error')){
  $injected=[IO.IOException]::new('secret-command-error',-1234567);$r=Pair $m 1 -Injected $injected;Need ($r.siblingEntry -ceq 'unread' -and $r.parentRelation -ceq 'unknown') 31
  if($m -eq 'no-command'){Need ($r.command -ceq 'missing') 32}
  elseif($m -eq 'multiple-command'){Need ($r.command -ceq 'multiple') 32}
  elseif($m -eq 'command-error'){Need ($r.command -ceq 'error' -and $r.probeStage -ceq 'npm-command' -and $r.category -ceq 'Other' -and $r.hresult -eq $injected.HResult) 32}
  else{Need ($r.command -ceq 'one' -and $r.commandShape -ceq 'unknown') 32}
 }
 # Both drive-relative and root-relative paths are rooted but not fully qualified.
 # GetFullPath resolves against current drive/directory; no canonical identity is inferred.
 foreach($path in @('C:npm.cmd','C:node.exe','\secret-fixture\selected\npm.cmd','\secret-fixture\selected\node.exe')){Need ([IO.Path]::IsPathRooted($path) -and -not [IO.Path]::IsPathFullyQualified($path)) 33}
 Need ([IO.Path]::GetDirectoryName('C:npm.cmd') -ceq 'C:' -and [IO.Path]::GetDirectoryName('C:node.exe') -ceq 'C:') 33
 $relativeRelation=if([string]::Equals([IO.Path]::GetFullPath('C:'),[IO.Path]::GetFullPath('C:\secret-fixture\selected'),[StringComparison]::OrdinalIgnoreCase)){'same'}else{'different'}
 $r=Pair 'source-drive-relative';Need ($r.commandShape -ceq 'rooted-npm-cmd' -and $r.parentRelation -ceq $relativeRelation -and $r.siblingEntry -ceq 'present') 33
 $r=Pair 'missing' 2 'C:node.exe';Need ($r.nodeShape -ceq 'rooted-node-exe' -and $r.parentRelation -ceq $relativeRelation -and $r.commandShape -ceq 'rooted-npm-cmd') 33
 $r=Pair 'source-root-relative';Need ($r.commandShape -ceq 'rooted-npm-cmd') 33
 $r=Pair 'missing' 2 '\secret-fixture\selected\node.exe';Need ($r.nodeShape -ceq 'rooted-node-exe') 33
 foreach($mapping in @(@('PermissionDenied','PermissionDenied',-2345678),@('OperationStopped','Other',-3456789))){
  $exception=[IO.IOException]::new('secret-command-record',[int]$mapping[2])
  $record=[Management.Automation.ErrorRecord]::new($exception,'fixed-command-provider',[Management.Automation.ErrorCategory]$mapping[0],$null)
  $r=Pair 'command-error' 1 -ProbeError $record
  Need ($r.probeStage -ceq 'npm-command' -and $r.command -ceq 'error' -and $r.siblingEntry -ceq 'unread' -and $r.category -ceq $mapping[1] -and $r.hresult -eq $exception.HResult) 32
 }
}
Case 'path-and-existence-errors' {
 $exception=[IO.IOException]::new('secret-existence-error',-4567890)
 $r=Pair 'sibling-error' 2 -Injected $exception;Need ($r.probeStage -ceq 'sibling-existence' -and $r.command -ceq 'one' -and $r.siblingEntry -ceq 'unread' -and $r.category -ceq 'Other' -and $r.hresult -eq $exception.HResult) 34
 foreach($mapping in @(@('ReadError','ReadError',-5678901),@('OperationStopped','Other',-6789012))){
  $exception=[IO.IOException]::new('secret-existence-record',[int]$mapping[2])
  $record=[Management.Automation.ErrorRecord]::new($exception,'fixed-existence-provider',[Management.Automation.ErrorCategory]$mapping[0],$null)
  $r=Pair 'sibling-error' 2 -ProbeError $record
  Need ($r.probeStage -ceq 'sibling-existence' -and $r.command -ceq 'one' -and $r.parentRelation -ceq 'same' -and $r.siblingEntry -ceq 'unread' -and $r.category -ceq $mapping[1] -and $r.hresult -eq $exception.HResult) 34
 }
 $a=Run-Tools $false 'missing' 'C:\secret-fixture\node.exe' ("C:\secret"+[char]0+'\npm-cli.js')
 $b=Run-Tools $true 'missing' 'C:\secret-fixture\node.exe' ("C:\secret"+[char]0+'\npm-cli.js')
 Compare-Failure $a $b;Need ((Json $a.ledger) -ceq (Json $b.ledger)) 35
 $r=Decode-Selection $b.stderr;Need ($r.probeStage -ceq 'input-shape' -and $r.command -ceq 'unread' -and $r.category -ne 'none') 36
}
Case 'installed-writer-fault-and-redaction' {
 $a=Run-Tools $false 'missing';$b=Run-Tools $true 'missing' -BrokenWriter
 Compare-Failure $a $b;Need ($b.writerFault -and $b.stderr -ceq '' -and $b.ledger.Count -eq $a.ledger.Count+2) 37
 $null=Pair 'other-present';$null=Pair 'command-error' 1 -Injected ([IO.IOException]::new('secret-raw-error C:\never-export'))
}
Case 'capture-composition-and-scope' {
 $a=@(Run-Tools $false 'missing' -FollowingModes @('success','no-command'))
 $b=@(Run-Tools $true 'missing' -FollowingModes @('success','no-command'))
 Need ($a.Count -eq 3 -and $b.Count -eq 3) 38
 Compare-Failure $a[0] $b[0];Compare-Failure $a[2] $b[2]
 Need ($b[1].stderr -ceq '' -and (Json $a[1].stdout) -ceq (Json $b[1].stdout) -and (Json $a[1].ledger) -ceq (Json $b[1].ledger)) 39
 $first=Decode-Selection $b[0].stderr;$last=Decode-Selection $b[2].stderr;Need ($first.parentRelation -ceq 'same' -and $last.command -ceq 'missing' -and $last.parentRelation -ceq 'unknown' -and $last.siblingEntry -ceq 'unread') 40
 . (Join-Path $PSScriptRoot 'capture-functions.ps1')
 $setup=Get-Content -Raw (Join-Path $PSScriptRoot 'setup-only.ps1')
 $start=$setup.IndexOf('} catch {'+"`n"+' $code=$_.Exception.HResult; $outcome=');$end=$setup.IndexOf("`n}"+"`nfinally {",$start);Need ($start -ge 0 -and $end -gt $start) 41
 $catchBody=$setup.Substring($start+'} catch {'.Length,$end-$start-'} catch {'.Length)
 $saved=[Console]::Error;$writer=[IO.StringWriter]::new();$diagnosticStage='tool-provenance';$script:ownedInvocationStarted=$false;$outcome='PASS';$code=0
 try{[Console]::SetError($writer);[Console]::Error.Write($b[0].stderr);try{throw $b[0].error}catch{Invoke-Expression $catchBody}}finally{[Console]::SetError($saved)}
 $lines=@(Lines $writer.ToString());$writer.Dispose();Need ($lines.Count -eq 3 -and $lines[0].StartsWith('TOOL_PROBE_FAILURE_V1 ') -and $lines[1].StartsWith('NPM_SELECTION_FAILURE_V1 ') -and $lines[2].StartsWith('SETUP_FAILURE_V1 ')) 42
 $r=$lines[2].Substring('SETUP_FAILURE_V1 '.Length)|ConvertFrom-Json;Need ($outcome -ceq 'FAILED' -and $r.phase -ceq 'original' -and $r.exitCode -eq 1 -and $r.ownership -ceq 'not-initialized') 43
}
Need ($rows.Count -eq 8 -and $expected.Count -eq 8) 44
for($i=0;$i -lt 8;$i++){Need ($rows[$i].name -ceq $expected[$i]) 45}
ConvertTo-Json -InputObject @($rows.ToArray()) -Depth 4 -Compress

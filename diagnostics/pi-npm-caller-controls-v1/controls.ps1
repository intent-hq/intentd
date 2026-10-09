param([hashtable]$Progress=@{case=$null;assertion=0;completed=@()})
$ErrorActionPreference='Stop'
$script:callerProgress=$Progress
function Need([bool]$Value,[int]$Id){$script:callerProgress.assertion=$Id;if(-not $Value){throw 'caller_control_assertion'}}
function Function-Text([string]$Source,[string]$Name){
 $t=$null;$e=$null;$a=[Management.Automation.Language.Parser]::ParseInput($Source,[ref]$t,[ref]$e)
 Need ($e.Count -eq 0) 1
 $f=@($a.FindAll({param($x)$x -is [Management.Automation.Language.FunctionDefinitionAst] -and $x.Name -ceq $Name},$true));Need ($f.Count -eq 1) 2
 return $f[0].Extent.Text
}
function Canonical($Value){if($Value -is [Collections.IDictionary]){$m=[ordered]@{};foreach($k in @($Value.Keys|Sort-Object)){$m[$k]=Canonical $Value[$k]};return $m};if($Value -is [Array]){return ,@($Value|ForEach-Object{Canonical $_})};return $Value}
function Json($Value){return ((Canonical $Value)|ConvertTo-Json -Depth 20 -Compress)}
function Lines([string]$Text){return @($Text -split '\r?\n'|Where-Object{$_ -ne ''})}
$old=Get-Content -Raw (Join-Path $PSScriptRoot 'original-tool-provenance.ps1')
$new=Get-Content -Raw (Join-Path $PSScriptRoot 'tool-provenance.ps1')
$oldSetup=Get-Content -Raw (Join-Path $PSScriptRoot 'original-setup-only.ps1')
$newSetup=Get-Content -Raw (Join-Path $PSScriptRoot 'setup-only.ps1')
$expected=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-controls.json')|ConvertFrom-Json)
$common=@('Write-NpmSelectionFailure','Set-ToolProbe','Write-ToolProbeFailure','Get-BoundedTree','Get-SetupTools')
$oldBlock=[scriptblock]::Create((@($common|ForEach-Object{Function-Text $old $_})-join "`n"))
$newBlock=[scriptblock]::Create((@(@('Get-NpmArgumentShape','Write-NpmCallerFailure','Write-NpmLookupCount')+$common|ForEach-Object{Function-Text $new $_})-join "`n"))
$helperBlock=[scriptblock]::Create((@(@('Get-NpmArgumentShape','Write-NpmCallerFailure','Write-NpmLookupCount')|ForEach-Object{Function-Text $new $_})-join "`n"))
. $helperBlock
$nodeLine='$node=(Get-Command node.exe -CommandType Application).Source'
$npmLine='$npm=Join-Path (Split-Path $node -Parent) ''node_modules/npm/bin/npm-cli.js'''
Need (($oldSetup.Split("`n")|Where-Object{$_ -ceq $nodeLine}).Count -eq 1 -and $newSetup.Contains($nodeLine)) 3
Need (($oldSetup.Split("`n")|Where-Object{$_.Trim() -ceq $npmLine}).Count -eq 2 -and ($newSetup.Split("`n")|Where-Object{$_.Trim() -ceq $npmLine}).Count -eq 2) 4
$nodeBlock=[scriptblock]::Create($nodeLine);$npmBlock=[scriptblock]::Create($npmLine)
$sites=@('initial','final','post-validator');$lhs=@('$toolsBefore=','$toolsAfter=','$afterValidatorTools=');$callsOld=@{};$callsNew=@{}
for($i=0;$i -lt 3;$i++){
 $oldCall='Get-SetupTools $node $npm';$newCall=$oldCall+" -CallerEvidence @{schema='npm-caller-context-v1';site='"+$sites[$i]+"';rawNode=`$node;rawNpm=`$npm}"
 Need ($oldSetup.Contains($lhs[$i]+$oldCall) -and $newSetup.Contains($lhs[$i]+$newCall)) 5
 $callsOld[$sites[$i]]=[scriptblock]::Create($oldCall);$callsNew[$sites[$i]]=[scriptblock]::Create($newCall)
}
function Capture([scriptblock]$Body,[switch]$Broken){
 $previous=[Console]::Error;$writer=[IO.StringWriter]::new();$failed=$null;$outputs=@();$direct=$false
 try{if($Broken){$writer.Dispose()};[Console]::SetError($writer);if($Broken){try{[Console]::Error.WriteLine('installed-fault-proof')}catch{$direct=$true};Need $direct 6};try{$outputs=@(& $Body)}catch{$failed=$_};return @{text=$(if($Broken){''}else{$writer.ToString()});error=$failed;output=$outputs;directFault=$direct}}finally{[Console]::SetError($previous);$writer.Dispose()}
}
function Decode([string]$Text,[string]$Label,[int]$Cap){
 $lines=@(Lines $Text|Where-Object{$_.StartsWith($Label+' ')})
 Need ($lines.Count -eq 1 -and $lines[0].Length -le $Cap) 7
 Need (-not $lines[0].Contains('secret') -and -not $lines[0].Contains('C:\') -and -not $lines[0].Contains('never-export')) 8
 return ($lines[0].Substring($Label.Length+1)|ConvertFrom-Json)
}
function Check-Caller($Record){
 Need ((@($Record.PSObject.Properties.Name|Sort-Object)-join '|') -ceq 'nodeBoundEqualsSole|nodeCount|nodeKind|nodeOverflow|nodeStrings|npmBoundEqualsSole|npmCount|npmKind|npmOverflow|npmStrings|pairwiseLexical|schema|site|valid') 9
 Need ($Record.schema -ceq 'npm-caller-failure-v1' -and $Record.valid -is [bool] -and $Record.site -cin @('unknown','initial','final','post-validator')) 10
 foreach($prefix in @('node','npm')){
  $kind=$Record.($prefix+'Kind');$count=$Record.($prefix+'Count');$overflow=$Record.($prefix+'Overflow');$strings=$Record.($prefix+'Strings');$equal=$Record.($prefix+'BoundEqualsSole')
  Need ($kind -cin @('other','null','string','string-array','object-array','other-array') -and $overflow -is [bool] -and $strings -is [bool]) 11
  Need ($null -eq $count -or (($count -is [int] -or $count -is [long]) -and $count -ge 0 -and $count -le 16)) 12
  Need ($null -eq $equal -or $equal -is [bool]) 13
  if($overflow){Need ($null -eq $count -and -not $strings -and $null -eq $equal) 14}
 }
 Need ($Record.pairwiseLexical -cin @('unknown','all-match','different')) 15
}
function Check-Count($Record){
 Need ((@($Record.PSObject.Properties.Name|Sort-Object)-join '|') -ceq 'count|overflow|schema|valid' -and $Record.schema -ceq 'npm-lookup-count-v1' -and $Record.valid -is [bool] -and $Record.overflow -is [bool]) 16
 Need ($null -eq $Record.count -or (($Record.count -is [int] -or $Record.count -is [long]) -and $Record.count -ge 0 -and $Record.count -le 16)) 17
 if($Record.overflow -or -not $Record.valid){Need ($null -eq $Record.count) 18}
}
function Run-Caller([bool]$Marked,[object[]]$Sources,[int]$LookupCount=1,[string]$Mode='missing',[Exception]$Injected=$null,[switch]$BrokenWriter,[switch]$BookkeepingFault,[string[]]$CallSites=@('initial'),[string[]]$Modes=@(),[switch]$ComposeSetup){
 & {
  $OFS=' ';$fx=@{mode=$Mode;sources=$Sources;lookupCount=$LookupCount;injected=$Injected;ledger=[Collections.Generic.List[string]]::new();boundary=$null;observed=$false;callerContext=$null;boundNode=$null;boundNpm=$null}
  function Get-Command {param([string]$Name,[string]$CommandType,[string]$ErrorAction)
   $fx.ledger.Add('command|'+$Name+'|'+$CommandType+'|'+$ErrorAction)
   if($Name -ceq 'node.exe'){foreach($v in $fx.sources){[pscustomobject]@{Source=$v}};return}
   if($Name -cne 'npm.cmd'){throw 'unexpected_command'}
   if($fx.mode -eq 'command-error'){throw $fx.injected}
   for($j=0;$j -lt $fx.lookupCount;$j++){[pscustomobject]@{Source='C:\secret-fixture\selected\npm.cmd'}}
  }
  function Test-Path {param([string]$LiteralPath,[string]$PathType,[string]$ErrorAction)
   $fx.ledger.Add('exists|'+$LiteralPath+'|'+$PathType+'|'+$ErrorAction)
   if($PathType -eq 'Leaf'){return $true}
   if($fx.mode -eq 'original-provider-error'){throw $fx.injected}
   return ($fx.mode -in @('success','package-error','package-refusal'))
  }
  function Get-Content {param([switch]$Raw,[string]$LiteralPath)
   $fx.ledger.Add('read|'+$LiteralPath)
   if($fx.mode -eq 'package-error'){throw $fx.injected};if($fx.mode -eq 'package-refusal'){return '{"name":"not-npm","version":"1.2.3"}'};return '{"name":"npm","version":"1.2.3"}'
  }
  function Get-ChildItem {param([string]$LiteralPath,[switch]$Recurse,[switch]$Force)
   $fx.ledger.Add('tree|'+$LiteralPath);return [pscustomobject]@{Attributes=[IO.FileAttributes]::Normal;PSIsContainer=$false;Length=1L;FullName=(Join-Path $LiteralPath 'fixture.dat')}
  }
  function Get-FileHash {param([string]$LiteralPath,[string]$Algorithm)
   $fx.ledger.Add('hash|'+$LiteralPath);return [pscustomobject]@{Hash=('A'*64)}
  }
  if($Marked){. $newBlock}else{. $oldBlock}
  $savedToolWriter=${function:Write-ToolProbeFailure}
  function Write-ToolProbeFailure($Context,[string]$Category,[int]$HResult){$fx.boundary=$_;$fx.observed=$true;& $savedToolWriter $Context $Category $HResult}
  if($Marked){
   $savedCallerWriter=${function:Write-NpmCallerFailure}
   function Write-NpmCallerFailure($CallerEvidence,[string]$BoundNode,[string]$BoundNpm){$fx.callerContext=$CallerEvidence;$fx.boundNode=$BoundNode;$fx.boundNpm=$BoundNpm;& $savedCallerWriter $CallerEvidence $BoundNode $BoundNpm}
   if($BookkeepingFault){function Write-NpmCallerFailure {throw 'injected_metadata_fault'};function Write-NpmLookupCount {throw 'injected_metadata_fault'}}
  }
  $oldOS=[Environment]::GetEnvironmentVariable('ImageOS');$oldVersion=[Environment]::GetEnvironmentVariable('ImageVersion');$previous=[Console]::Error;$writer=[IO.StringWriter]::new();$rows=[Collections.Generic.List[object]]::new()
  try{
   [Environment]::SetEnvironmentVariable('ImageOS','windows-fixture');[Environment]::SetEnvironmentVariable('ImageVersion','1.2.3')
   if($BrokenWriter){$writer.Dispose()};[Console]::SetError($writer);$direct=$false;if($BrokenWriter){try{[Console]::Error.WriteLine('installed-fault-proof')}catch{$direct=$true};Need $direct 19}
   . $nodeBlock
   . $npmBlock
   $selectionLedger=@($fx.ledger.ToArray())
   for($k=0;$k -lt $CallSites.Count;$k++){
    $site=$CallSites[$k];Need ($site -cin $sites) 20
    $fx.mode=if($Modes.Count -gt $k){$Modes[$k]}else{$Mode};$fx.ledger.Clear();$fx.boundary=$null;$fx.observed=$false;$fx.callerContext=$null;$fx.boundNode=$null;$fx.boundNpm=$null
    if(-not $BrokenWriter){$null=$writer.GetStringBuilder().Clear()}
    $failure=$null;$outputs=@();$invoke=if($Marked){$callsNew[$site]}else{$callsOld[$site]}
    try{if($ComposeSetup){$diagnosticStage='tool-provenance';$outcome='FAILED';$code=0;$outputs=@(. $composeBlock)}else{$outputs=@(. $invoke)}}catch{$failure=$_}
    $rows.Add([pscustomobject]@{error=$failure;boundary=$fx.boundary;observed=$fx.observed;mode=$fx.mode;site=$site;rawNode=$node;rawNpm=$npm;selectionLedger=$selectionLedger;ledger=@($fx.ledger.ToArray());stdout=$outputs;stderr=$(if($BrokenWriter){''}else{$writer.ToString()});writerFault=$direct;context=$fx.callerContext;boundNode=$fx.boundNode;boundNpm=$fx.boundNpm;expectedNode=[string]$node;expectedNpm=[string]$npm;setupOutcome=$(if($ComposeSetup){$outcome}else{$null});setupCode=$(if($ComposeSetup){$code}else{$null})})
   }
  }finally{[Console]::SetError($previous);[Environment]::SetEnvironmentVariable('ImageOS',$oldOS);[Environment]::SetEnvironmentVariable('ImageVersion',$oldVersion);$writer.Dispose()}
  return $rows.ToArray()
 }
}
function Compare-Result($A,$B,[Exception]$Injected=$null){
 Need ((Json $A.selectionLedger) -ceq (Json $B.selectionLedger) -and (Json $A.ledger) -ceq (Json $B.ledger)) 21
 Need ((Json $A.rawNode) -ceq (Json $B.rawNode) -and (Json $A.rawNpm) -ceq (Json $B.rawNpm)) 22
 if($null -eq $A.error){Need ($null -eq $B.error -and (Json $A.stdout) -ceq (Json $B.stdout) -and $A.stderr -ceq $B.stderr) 23;return}
 Need ($null -ne $B.error -and $A.stdout.Count -eq 0 -and $B.stdout.Count -eq 0) 24
 Need ($A.error.Exception.GetType() -eq $B.error.Exception.GetType() -and $A.error.Exception.HResult -eq $B.error.Exception.HResult -and $A.error.CategoryInfo.Category -eq $B.error.CategoryInfo.Category -and $A.error.FullyQualifiedErrorId -ceq $B.error.FullyQualifiedErrorId -and $A.error.Exception.Message -ceq $B.error.Exception.Message) 25
 foreach($x in @($A,$B)){
  Need ($x.observed -is [bool] -and $x.observed -and $x.boundary -is [Management.Automation.ErrorRecord] -and [Object]::ReferenceEquals($x.error.Exception,$x.boundary.Exception) -and $x.error.CategoryInfo.Category -eq $x.boundary.CategoryInfo.Category -and $x.error.FullyQualifiedErrorId -ceq $x.boundary.FullyQualifiedErrorId) 26
  if($null -ne $Injected){Need ([Object]::ReferenceEquals($x.error.Exception,$Injected)) 27}
 }
 $prior=@(Lines $A.stderr);$retained=@(Lines $B.stderr|Where-Object{-not $_.StartsWith('NPM_CALLER_FAILURE_V1 ') -and -not $_.StartsWith('NPM_LOOKUP_COUNT_V1 ')})
 Need ((Json $prior) -ceq (Json $retained)) 28
}
function Reject([scriptblock]$Body,[int]$Id){$failure=$null;try{& $Body}catch{$failure=$_};Need ($null -ne $failure -and $failure.Exception.Message -ceq 'caller_control_assertion' -and $script:callerProgress.assertion -eq $Id) 29}
$rows=[Collections.Generic.List[object]]::new()
function Case([string]$Name,[scriptblock]$Body){$script:callerProgress.case=$Name;$script:callerProgress.assertion=0;$captured=@(& $Body);Need ($captured.Count -eq 0) 30;$rows.Add(@{name=$Name;outcome='PASS'});$script:callerProgress.completed=@($script:callerProgress.completed)+$Name}
$one='C:\secret-fixture\selected\node.exe';$two='C:\secret-other\node.exe'
Case 'caller-enumeration-and-typed-boundary' {
 foreach($sources in @(@($one),@($one,$two),@($one,$one,$two))){
  $a=Run-Caller $false $sources;$b=Run-Caller $true $sources;Compare-Result $a $b
  Need ($a.selectionLedger.Count -eq 1 -and $a.selectionLedger[0] -ceq 'command|node.exe|Application|') 31
  Need ($b.boundNode -is [string] -and $b.boundNpm -is [string] -and $b.boundNode -ceq $b.expectedNode -and $b.boundNpm -ceq $b.expectedNpm -and $b.ledger[0] -ceq ('exists|'+$b.boundNpm+'||')) 64
  $n=Decode $b.stderr 'NPM_CALLER_FAILURE_V1' 1024;Check-Caller $n
  Need ($n.valid -and $n.nodeCount -eq $sources.Count -and $n.npmCount -eq $sources.Count -and $n.nodeStrings -and $n.npmStrings -and $n.pairwiseLexical -ceq 'all-match') 32
  if($sources.Count -eq 1){Need ($n.nodeKind -ceq 'string' -and $n.npmKind -ceq 'string' -and $n.nodeBoundEqualsSole -eq $true -and $n.npmBoundEqualsSole -eq $true) 33}else{Need ($n.nodeKind -ceq 'object-array' -and $n.npmKind -ceq 'object-array' -and $null -eq $n.nodeBoundEqualsSole -and $null -eq $n.npmBoundEqualsSole) 34}
 }
}
Case 'single-and-multiple-relationship' {
 foreach($kind in @('scalar','array-one','array-two')){
  $rn=$one;$rm=[IO.Path]::Combine([IO.Path]::GetDirectoryName($one),'node_modules/npm/bin/npm-cli.js')
  if($kind -eq 'array-one'){$rn=,($one);$rm=,($rm)};if($kind -eq 'array-two'){$rn=@($one,$two);$rm=@($rm,[IO.Path]::Combine([IO.Path]::GetDirectoryName($two),'node_modules/npm/bin/npm-cli.js'))}
  $ctx=@{schema='npm-caller-context-v1';site='initial';rawNode=$rn;rawNpm=$rm}
  $c=Capture {Write-NpmCallerFailure $ctx ([string]$rn) ([string]$rm)};Need ($null -eq $c.error -and $c.output.Count -eq 0) 35
  $v=Decode $c.text 'NPM_CALLER_FAILURE_V1' 1024;Check-Caller $v;Need ($v.pairwiseLexical -ceq 'all-match') 36
 }
 foreach($rn in @('C:node.exe','\secret-fixture\selected\node.exe','relative\node.exe')){
  $ctx=@{schema='npm-caller-context-v1';site='final';rawNode=$rn;rawNpm='C:\wrong\npm-cli.js'};$c=Capture {Write-NpmCallerFailure $ctx $rn 'C:\wrong\npm-cli.js'};$v=Decode $c.text 'NPM_CALLER_FAILURE_V1' 1024;Check-Caller $v
  if([IO.Path]::IsPathRooted($rn)){Need ($v.pairwiseLexical -ceq 'different') 37}else{Need ($v.pairwiseLexical -ceq 'unknown') 38}
 }
}
Case 'bounded-shape-negative' {
 $samples=[Collections.Generic.List[object]]::new();$samples.Add(@{v=$null;kind='null';count=0;overflow=$false;strings=$false});$samples.Add(@{v=@();kind='object-array';count=0;overflow=$false;strings=$true});$samples.Add(@{v=@($one,$null);kind='object-array';count=2;overflow=$false;strings=$false});$samples.Add(@{v=@($one,42);kind='object-array';count=2;overflow=$false;strings=$false});$samples.Add(@{v=[int[]]@(1);kind='other-array';count=$null;overflow=$false;strings=$false});$samples.Add(@{v=[object[,]]::new(1,1);kind='other-array';count=$null;overflow=$false;strings=$false});$samples.Add(@{v=[Array]::CreateInstance([object],[int[]]@(1),[int[]]@(1));kind='other-array';count=$null;overflow=$false;strings=$false});$samples.Add(@{v=@(1..17|ForEach-Object{$one});kind='object-array';count=$null;overflow=$true;strings=$false});$samples.Add(@{v=('secret'*6000);kind='string';count=1;overflow=$false;strings=$false})
 $hostile=[pscustomobject]@{};$hostile|Add-Member ScriptMethod ToString {throw 'do_not_convert'} -Force;$samples.Add(@{v=$hostile;kind='other';count=$null;overflow=$false;strings=$false})
 foreach($sample in $samples){$shape=Get-NpmArgumentShape $sample.v;Need ($shape.kind -ceq $sample.kind -and $shape.count -eq $sample.count -and $shape.overflow -eq $sample.overflow -and $shape.strings -eq $sample.strings) 39}
 $s=Get-NpmArgumentShape ([string[]]@($one));Need ($s.kind -ceq 'string-array' -and $s.count -eq 1 -and $s.strings) 40
}
Case 'lookup-cardinality-no-new-query' {
 foreach($count in @(0,1,2,16,17)){
  $a=Run-Caller $false @($one) $count;$b=Run-Caller $true @($one) $count;Compare-Result $a $b;$v=Decode $b.stderr 'NPM_LOOKUP_COUNT_V1' 256;Check-Count $v
  Need ($v.valid -and $v.overflow -eq ($count -gt 16)) 41;if($count -le 16){Need ($v.count -eq $count) 42}else{Need ($null -eq $v.count) 43}
  Need (@($b.ledger|Where-Object{$_.StartsWith('command|npm.cmd|')}).Count -eq 1 -and $b.ledger.Count -eq $(if($count -eq 1){3}else{2})) 44
 }
 $err=[IO.IOException]::new('secret-query');$a=Run-Caller $false @($one) 1 'command-error' $err;$b=Run-Caller $true @($one) 1 'command-error' $err;Compare-Result $a $b
 Need (-not $b.stderr.Contains('NPM_LOOKUP_COUNT_V1') -and $b.ledger.Count -eq 2) 45
}
Case 'schema-redaction-and-context' {
 foreach($site in $sites){$ctx=@{schema='npm-caller-context-v1';site=$site;rawNode=$one;rawNpm='C:\secret-wrong\npm-cli.js'};$c=Capture {Write-NpmCallerFailure $ctx $one 'C:\secret-wrong\npm-cli.js'};$v=Decode $c.text 'NPM_CALLER_FAILURE_V1' 1024;Check-Caller $v;Need ($v.valid -and $v.site -ceq $site -and $v.pairwiseLexical -ceq 'different') 46}
 $bad=[Collections.Generic.List[object]]::new();$bad.Add($null);$bad.Add(@{});$bad.Add(@{schema='wrong';site='initial';rawNode=$one;rawNpm=$one});$bad.Add(@{schema='npm-caller-context-v1';site=1;rawNode=$one;rawNpm=$one});$bad.Add(@{schema='npm-caller-context-v1';site='initial';rawNode=$one;rawNpm=$one;secret='never-export'})
 foreach($ctx in $bad){$c=Capture {Write-NpmCallerFailure $ctx $one $one};$v=Decode $c.text 'NPM_CALLER_FAILURE_V1' 1024;Check-Caller $v;Need (-not $v.valid -and $v.site -ceq 'unknown' -and $null -eq $v.nodeCount -and $null -eq $v.nodeBoundEqualsSole -and $v.pairwiseLexical -ceq 'unknown') 47}
 $wrong=$v.PSObject.Copy();$wrong.nodeCount='1';Reject {Check-Caller $wrong} 12
 $wrong=$v.PSObject.Copy();$wrong.nodeBoundEqualsSole='false';Reject {Check-Caller $wrong} 13
 $wrong=$v.PSObject.Copy();$wrong|Add-Member NoteProperty secret 'never-export';Reject {Check-Caller $wrong} 9
 $c=Capture {Write-NpmLookupCount @()};$v=Decode $c.text 'NPM_LOOKUP_COUNT_V1' 256;Check-Count $v;$v.count='0';Reject {Check-Count $v} 17
}
Case 'original-error-and-provider-transparency' {
 foreach($mode in @('success','missing','original-provider-error','package-error','package-refusal')){
  $err=[IO.IOException]::new('secret-provider');$a=Run-Caller $false @($one) 1 $mode $err;$b=Run-Caller $true @($one) 1 $mode $err
  Compare-Result $a $b $(if($mode -in @('original-provider-error','package-error')){$err}else{$null})
  if($mode -ne 'missing'){Need (-not $b.stderr.Contains('NPM_CALLER_FAILURE_V1') -and -not $b.stderr.Contains('NPM_LOOKUP_COUNT_V1')) 48}
 }
 $a=Run-Caller $false @($one);$b=Run-Caller $true @($one);$bad=$b.PSObject.Copy();$bad.ledger=@('wrong');Reject {Compare-Result $a $bad} 21
 $bad=$b.PSObject.Copy();$bad.observed=$false;Reject {Compare-Result $a $bad} 26
 $bad=$b.PSObject.Copy();$bad.error=[Management.Automation.ErrorRecord]::new([IO.IOException]::new('replacement'),'different',[Management.Automation.ErrorCategory]::ReadError,$null);Reject {Compare-Result $a $bad} 25
}
Case 'installed-writer-and-bookkeeping-faults' {
 $a=Run-Caller $false @($one) -BrokenWriter;$b=Run-Caller $true @($one) -BrokenWriter;Compare-Result $a $b;Need ($a.writerFault -and $b.writerFault) 49
 $a=Run-Caller $false @($one);$b=Run-Caller $true @($one) -BookkeepingFault;Compare-Result $a $b;Need (-not $b.stderr.Contains('NPM_CALLER_FAILURE_V1') -and -not $b.stderr.Contains('NPM_LOOKUP_COUNT_V1')) 50
 $ctx=@{schema='npm-caller-context-v1';site='initial';rawNode=$one;rawNpm=$one};$readOnly=[Collections.ObjectModel.ReadOnlyDictionary[string,object]]::new([Collections.Generic.Dictionary[string,object]]::new())
 $c=Capture {Write-NpmCallerFailure $readOnly $one $one};Need ($null -eq $c.error -and $c.output.Count -eq 0) 51;$v=Decode $c.text 'NPM_CALLER_FAILURE_V1' 1024;Need (-not $v.valid) 52
}
Case 'three-site-composition' {
 $a=@(Run-Caller $false @($one) -CallSites $sites);$b=@(Run-Caller $true @($one) -CallSites $sites);Need ($a.Count -eq 3 -and $b.Count -eq 3) 53
 for($i=0;$i -lt 3;$i++){
  Compare-Result $a[$i] $b[$i];$v=Decode $b[$i].stderr 'NPM_CALLER_FAILURE_V1' 1024;Need ($v.valid -and $v.site -ceq $sites[$i] -and [Object]::ReferenceEquals($b[$i].context.rawNode,$b[$i].rawNode)) 54
  if($i -gt 0){Need (-not [Object]::ReferenceEquals($b[$i-1].context,$b[$i].context)) 55}
  $labels=@(Lines $b[$i].stderr|ForEach-Object{($_ -split ' ',2)[0]});Need (($labels -join '|') -ceq 'TOOL_PROBE_FAILURE_V1|NPM_CALLER_FAILURE_V1|NPM_LOOKUP_COUNT_V1|NPM_SELECTION_FAILURE_V1') 56
 }
 $b=@(Run-Caller $true @($one) -CallSites $sites -Modes @('missing','success','missing'));Need ($null -eq $b[1].error -and $null -eq $b[1].context -and $b[1].stderr -ceq '') 57
 # Capture functions are extracted as data. No setup, OwnedSetup type, finalizer or child is invoked.
 $captureText=(Function-Text $newSetup 'New-SetupFailureRecord')+"`n"+(Function-Text $newSetup 'Write-SetupFailureRecord')+"`n"+(Function-Text $newSetup 'Get-SetupOwnershipState');. ([scriptblock]::Create($captureText))
 $pattern='(?s)(catch \{\r?\n \$code=\$_\.Exception\.HResult; \$outcome=''FAILED''.*?\r?\n\})\r?\nfinally \{'
 $cm=[regex]::Matches($newSetup,$pattern);$om=[regex]::Matches($oldSetup,$pattern);Need ($cm.Count -eq 1 -and $om.Count -eq 1 -and $cm[0].Groups[1].Value -ceq $om[0].Groups[1].Value) 58
 $composeBlock=[scriptblock]::Create('try { . $invoke } '+$cm[0].Groups[1].Value)
 $script:ownedInvocationStarted=$false
 $composed=Run-Caller $true @($one) -ComposeSetup
 Need ($null -eq $composed.error -and $composed.stdout.Count -eq 0 -and $composed.setupOutcome -ceq 'FAILED' -and $composed.setupCode -eq $composed.boundary.Exception.HResult) 61
 $record=Decode $composed.stderr 'SETUP_FAILURE_V1' 1024
 Need ($record.outcome -ceq 'FAILED' -and $record.exitCode -eq 1 -and $record.ownership -ceq 'not-initialized' -and $record.behavioralInvocations -eq 0 -and $record.hresult -eq $composed.boundary.Exception.HResult) 62
 $labels=@(Lines $composed.stderr|ForEach-Object{($_ -split ' ',2)[0]});Need (($labels -join '|') -ceq 'TOOL_PROBE_FAILURE_V1|NPM_CALLER_FAILURE_V1|NPM_LOOKUP_COUNT_V1|NPM_SELECTION_FAILURE_V1|SETUP_FAILURE_V1') 63
}
Need ($rows.Count -eq 8 -and $expected.Count -eq 8) 59
for($i=0;$i -lt 8;$i++){Need ($rows[$i].name -ceq $expected[$i]) 60}
$rows.ToArray()|ConvertTo-Json -Depth 5 -Compress

param([Parameter(Mandatory)][string]$FixtureRoot,[Parameter(Mandatory)][hashtable]$FixtureState,[hashtable]$Progress=@{case=$null;assertion=0;completed=@()})
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
$old=Get-Content -Raw (Join-Path $PSScriptRoot 'tool-provenance.ps1')
$new=Get-Content -Raw (Join-Path $PSScriptRoot 'tool-provenance.ps1')
$oldSetup=Get-Content -Raw (Join-Path $PSScriptRoot 'original-setup-only.ps1')
$newSetup=Get-Content -Raw (Join-Path $PSScriptRoot 'setup-only.ps1')
$expected=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-controls.json')|ConvertFrom-Json)
$common=@('Write-NpmSelectionFailure','Set-ToolProbe','Write-ToolProbeFailure','Get-BoundedTree','Get-SetupTools')
$oldBlock=[scriptblock]::Create((@(@('Get-NpmArgumentShape','Write-NpmCallerFailure','Write-NpmLookupCount')+$common|ForEach-Object{Function-Text $old $_})-join "`n"))
$newBlock=[scriptblock]::Create((@(@('Get-NpmArgumentShape','Write-NpmCallerFailure','Write-NpmLookupCount')+$common|ForEach-Object{Function-Text $new $_})-join "`n"))
$helperBlock=[scriptblock]::Create((@(@('Get-NpmArgumentShape','Write-NpmCallerFailure','Write-NpmLookupCount')|ForEach-Object{Function-Text $new $_})-join "`n"))
. $helperBlock
$nodeLine='$node=(Get-Command node.exe -CommandType Application).Source'
$npmLine='$npm=Join-Path (Split-Path $node -Parent) ''node_modules/npm/bin/npm-cli.js'''
Need (($oldSetup.Split("`n")|Where-Object{$_ -ceq $nodeLine}).Count -eq 1 -and $newSetup.Contains($nodeLine.Replace(' Application)',' Application -TotalCount 1)'))) 3
Need (($oldSetup.Split("`n")|Where-Object{$_.Trim() -ceq $npmLine}).Count -eq 2 -and ($newSetup.Split("`n")|Where-Object{$_.Trim() -ceq $npmLine}).Count -eq 2) 4
$nodeBlockOld=[scriptblock]::Create($nodeLine);$nodeBlockNew=[scriptblock]::Create($nodeLine.Replace(' Application)',' Application -TotalCount 1)'));$npmBlock=[scriptblock]::Create($npmLine)
$sites=@('initial','final','post-validator');$lhs=@('$toolsBefore=','$toolsAfter=','$afterValidatorTools=');$callsOld=@{};$callsNew=@{}
for($i=0;$i -lt 3;$i++){
 $baseCall='Get-SetupTools $node $npm';$newCall=$baseCall+" -CallerEvidence @{schema='npm-caller-context-v1';site='"+$sites[$i]+"';rawNode=`$node;rawNpm=`$npm}"
 $oldCall=$newCall
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
function Run-Caller([bool]$Marked,[object[]]$Sources,[int]$LookupCount=1,[string]$Mode='missing',[Exception]$Injected=$null,[switch]$BrokenWriter,[switch]$BookkeepingFault,[string[]]$CallSites=@('initial'),[string[]]$Modes=@(),[switch]$ComposeSetup,[switch]$Native,[string]$FixtureOFS=' '){
 & {
  $OFS=$FixtureOFS;$fx=@{mode=$Mode;sources=$Sources;lookupCount=$LookupCount;injected=$Injected;ledger=[Collections.Generic.List[string]]::new();boundary=$null;observed=$false;callerContext=$null;boundNode=$null;boundNpm=$null}
  function Get-Command {param([string]$Name,[string]$CommandType,[string]$ErrorAction,[int]$TotalCount)
   $limit=if($PSBoundParameters.ContainsKey('TotalCount')){[string]$TotalCount}else{'unlimited'}
   $fx.ledger.Add('command|'+$Name+'|'+$CommandType+'|'+$ErrorAction+'|'+$limit)
   if($Name -ceq 'node.exe'){if($Native){Microsoft.PowerShell.Core\Get-Command @PSBoundParameters;return};foreach($v in $fx.sources){[pscustomobject]@{Source=$v}};return}
   if($Name -cne 'npm.cmd'){throw 'unexpected_command'}
   if($fx.mode -eq 'command-error'){throw $fx.injected}
   for($j=0;$j -lt $fx.lookupCount;$j++){[pscustomobject]@{Source='C:\secret-fixture\selected\npm.cmd'}}
  }
  function Test-Path {param([string]$LiteralPath,[string]$PathType,[string]$ErrorAction)
   $fx.ledger.Add('exists|'+$LiteralPath+'|'+$PathType+'|'+$ErrorAction)
   if($fx.mode -eq 'real-missing'){return (Microsoft.PowerShell.Management\Test-Path @PSBoundParameters)}
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
  . {
   $savedCallerWriter=${function:Write-NpmCallerFailure}
   function Write-NpmCallerFailure($CallerEvidence,[string]$BoundNode,[string]$BoundNpm){$fx.callerContext=$CallerEvidence;$fx.boundNode=$BoundNode;$fx.boundNpm=$BoundNpm;& $savedCallerWriter $CallerEvidence $BoundNode $BoundNpm}
   if($BookkeepingFault){function Write-NpmCallerFailure {throw 'injected_metadata_fault'};function Write-NpmLookupCount {throw 'injected_metadata_fault'}}
  }
  $oldOS=[Environment]::GetEnvironmentVariable('ImageOS');$oldVersion=[Environment]::GetEnvironmentVariable('ImageVersion');$previous=[Console]::Error;$writer=[IO.StringWriter]::new();$rows=[Collections.Generic.List[object]]::new()
  try{
   [Environment]::SetEnvironmentVariable('ImageOS','windows-fixture');[Environment]::SetEnvironmentVariable('ImageVersion','1.2.3')
   if($BrokenWriter){$writer.Dispose()};[Console]::SetError($writer);$direct=$false;if($BrokenWriter){try{[Console]::Error.WriteLine('installed-fault-proof')}catch{$direct=$true};Need $direct 19}
   if($Marked){. $nodeBlockNew}else{. $nodeBlockOld}
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
 Need ($A.selectionLedger.Count -eq 1 -and $B.selectionLedger.Count -eq 1 -and $A.selectionLedger[0] -ceq 'command|node.exe|Application||unlimited' -and $B.selectionLedger[0] -ceq 'command|node.exe|Application||1' -and (Json $A.ledger) -ceq (Json $B.ledger)) 21
 Need ((Json $A.rawNode) -ceq (Json $B.rawNode) -and (Json $A.rawNpm) -ceq (Json $B.rawNpm)) 22
 if($null -eq $A.error){Need ($null -eq $B.error -and (Json $A.stdout) -ceq (Json $B.stdout) -and $A.stderr -ceq $B.stderr) 23;return}
 Need ($null -ne $B.error -and $A.stdout.Count -eq 0 -and $B.stdout.Count -eq 0) 24
 Need ($A.error.Exception.GetType() -eq $B.error.Exception.GetType() -and $A.error.Exception.HResult -eq $B.error.Exception.HResult -and $A.error.CategoryInfo.Category -eq $B.error.CategoryInfo.Category -and $A.error.FullyQualifiedErrorId -ceq $B.error.FullyQualifiedErrorId -and $A.error.Exception.Message -ceq $B.error.Exception.Message) 25
 foreach($x in @($A,$B)){
  Need ($x.observed -is [bool] -and $x.observed -and $x.boundary -is [Management.Automation.ErrorRecord] -and [Object]::ReferenceEquals($x.error.Exception,$x.boundary.Exception) -and $x.error.CategoryInfo.Category -eq $x.boundary.CategoryInfo.Category -and $x.error.FullyQualifiedErrorId -ceq $x.boundary.FullyQualifiedErrorId) 26
  if($null -ne $Injected){Need ([Object]::ReferenceEquals($x.error.Exception,$Injected)) 27}
 }
 $prior=@(Lines $A.stderr);$retained=@(Lines $B.stderr)
 Need ((Json $prior) -ceq (Json $retained)) 28
}
function Reject([scriptblock]$Body,[int]$Id){$failure=$null;try{& $Body}catch{$failure=$_};Need ($null -ne $failure -and $failure.Exception.Message -ceq 'caller_control_assertion' -and $script:callerProgress.assertion -eq $Id) 29}
# This helper delegates to the native cmdlet. It never slices, deduplicates or synthesizes its output.
function Native-Selection([bool]$Limited,[string]$Separator=' ',[switch]$CompetingNames){
 & {
  $OFS=$Separator;$selection=@{ledger=[Collections.Generic.List[string]]::new();commands=@()}
  function NeverInvoke {throw 'executable_invocation_forbidden'}
  if($CompetingNames){function node.exe {throw 'executable_invocation_forbidden'};Set-Alias -Name node.exe -Value NeverInvoke -Scope Local}
  function Get-Command {
   param([string]$Name,[string]$CommandType,[int]$TotalCount)
   $limit=if($PSBoundParameters.ContainsKey('TotalCount')){[string]$TotalCount}else{'unlimited'}
   $selection.ledger.Add($Name+'|'+$CommandType+'|'+$limit)
   $found=@(Microsoft.PowerShell.Core\Get-Command @PSBoundParameters)
   $selection.commands=$found
   $found
  }
  function Basic-Boundary([string]$SelectedNode,[string]$DerivedNpm){return @{node=$SelectedNode;npm=$DerivedNpm}}
  $node=$null;$npm=$null;$first=$null;$bound=$null;$errorRecord=$null
  try {
   if($Limited){. $nodeBlockNew}else{. $nodeBlockOld}
   . $npmBlock;$first=$npm
   . $npmBlock
   $bound=Basic-Boundary $node $npm
  }catch{$errorRecord=$_}
  return @{node=$node;npm=$npm;firstNpm=$first;bound=$bound;error=$errorRecord;commands=$selection.commands;ledger=@($selection.ledger.ToArray())}
 }
}
function Check-Native($A,$B,[string]$Winner,[int]$OriginalCount){
 Need ($null -eq $A.error -and $null -eq $B.error -and @($A.node).Count -eq $OriginalCount -and $B.node -is [string] -and $B.node -ceq $Winner) 65
 Need ($A.ledger.Count -eq 1 -and $A.ledger[0] -ceq 'node.exe|Application|unlimited' -and $B.ledger.Count -eq 1 -and $B.ledger[0] -ceq 'node.exe|Application|1') 66
 Need ($A.commands.Count -eq $OriginalCount -and $B.commands.Count -eq 1 -and $B.commands[0].Source -ceq $A.commands[0].Source) 67
 foreach($c in @($A.commands)+@($B.commands)){Need ($c -is [Management.Automation.ApplicationInfo] -and $c.CommandType -eq [Management.Automation.CommandTypes]::Application -and $c.Source -is [string]) 68}
}
function Same-Error($A,$B,[Exception]$Injected=$null){
 Need ($A -is [Management.Automation.ErrorRecord] -and $B -is [Management.Automation.ErrorRecord]) 69
 Need ($A.Exception.GetType() -eq $B.Exception.GetType() -and $A.Exception.HResult -eq $B.Exception.HResult -and $A.CategoryInfo.Category -eq $B.CategoryInfo.Category -and $A.FullyQualifiedErrorId -ceq $B.FullyQualifiedErrorId -and $A.Exception.Message -ceq $B.Exception.Message) 70
 if($null -ne $Injected){Need ([Object]::ReferenceEquals($A.Exception,$Injected) -and [Object]::ReferenceEquals($B.Exception,$Injected)) 71}
}
# Adversarial provider returns are deliberately separate from the native discovery proof.
function Malformed-Selection([bool]$Limited,$SourceValue,[string]$Shape,[Exception]$Injected=$null){
 & {
  $OFS=' ';$localLedger=[Collections.Generic.List[string]]::new();$failure=$null;$node=$null;$npm=$null
  function Get-Command {param([string]$Name,[string]$CommandType,[int]$TotalCount)
   $localLedger.Add($Name+'|'+$CommandType+'|'+$(if($PSBoundParameters.ContainsKey('TotalCount')){[string]$TotalCount}else{'unlimited'}))
   if($Shape -ceq 'error'){throw $Injected}
   if($Shape -ceq 'missing'){return [pscustomobject]@{}}
   return [pscustomobject]@{Source=$SourceValue}
  }
  try{if($Limited){. $nodeBlockNew}else{. $nodeBlockOld};. $npmBlock}catch{$failure=$_}
  return @{error=$failure;node=$node;npm=$npm;ledger=@($localLedger.ToArray())}
 }
}
$rows=[Collections.Generic.List[object]]::new()
function Case([string]$Name,[scriptblock]$Body){$script:callerProgress.case=$Name;$script:callerProgress.assertion=0;$captured=@(& $Body);Need ($captured.Count -eq 0) 30;$rows.Add(@{name=$Name;outcome='PASS'});$script:callerProgress.completed=@($script:callerProgress.completed)+$Name}
$fixturePaths=@('z space','a space','empty','a space/node_modules','a space/node_modules/npm','a space/node_modules/npm/bin')
$fixtureFiles=@('z space/node.exe','a space/node.exe','a space/node_modules/npm/bin/npm-cli.js')
$savedPath=[Environment]::GetEnvironmentVariable('PATH','Process');$savedOFS=Get-Variable -Name OFS -ErrorAction SilentlyContinue
$savedOFSValue=if($null -ne $savedOFS){$savedOFS.Value}else{$null};$savedErrorWriter=[Console]::Error
$created=$false;$originalFailure=$null
try {
 Need ($IsWindows -and [IO.Path]::IsPathFullyQualified($FixtureRoot) -and $FixtureRoot.Length -le 4096 -and -not (Test-Path -LiteralPath $FixtureRoot)) 72
 $null=New-Item -ItemType Directory -Path $FixtureRoot -ErrorAction Stop;$created=$true;$FixtureState.created=$true
 foreach($rel in $fixturePaths){$null=New-Item -ItemType Directory -Path (Join-Path $FixtureRoot $rel) -ErrorAction Stop}
 foreach($rel in $fixtureFiles){$stream=[IO.File]::Open((Join-Path $FixtureRoot $rel),[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,[IO.FileShare]::None);$stream.Dispose()}
 $z=Join-Path $FixtureRoot 'z space';$a=Join-Path $FixtureRoot 'a space';$empty=Join-Path $FixtureRoot 'empty';$zNode=Join-Path $z 'node.exe';$aNode=Join-Path $a 'node.exe'
 Case 'native-precedence-multiple-distinct' {
  foreach($order in @(@($z,$a),@($a,$z))){
   [Environment]::SetEnvironmentVariable('PATH',($order -join ';'),'Process')
   $oldSelection=Native-Selection $false;$newSelection=Native-Selection $true
   Check-Native $oldSelection $newSelection (Join-Path $order[0] 'node.exe') 2
   Need ($oldSelection.node[1] -ceq (Join-Path $order[1] 'node.exe') -and $oldSelection.node[0] -cne $oldSelection.node[1]) 73
   $bad=$newSelection.Clone();$bad.node=$oldSelection.node[1];Reject {Check-Native $oldSelection $bad (Join-Path $order[0] 'node.exe') 2} 65
  }
 }
 Case 'single-and-duplicate-paths' {
  foreach($pathValue in @($z,($z+';'+$z),($z+';'+$z+';'+$a+';'+$a))){
   [Environment]::SetEnvironmentVariable('PATH',$pathValue,'Process')
   $x=Native-Selection $false;$y=Native-Selection $true
   Need ($x.commands.Count -ge 1 -and $x.commands.Count -le 4) 74
   Check-Native $x $y $zNode $x.commands.Count
   if($pathValue -ceq $z){Need ($x.node -is [string] -and $x.node -ceq $y.node -and $x.npm -ceq $y.npm) 75}
  }
 }
 Case 'scalar-derivation-and-basic-boundary' {
  [Environment]::SetEnvironmentVariable('PATH',($z+';'+$a),'Process')
  foreach($separator in @(' ','|fixture-separator|')){
   $x=Native-Selection $false $separator;$y=Native-Selection $true $separator;Check-Native $x $y $zNode 2
   $wanted=Join-Path $z 'node_modules/npm/bin/npm-cli.js'
   Need ($x.node -is [object[]] -and $x.npm -is [object[]] -and $x.npm.Count -eq 2 -and $x.bound.node -ceq ($x.node -join $separator) -and $x.bound.npm -ceq ($x.npm -join $separator)) 76
   Need ($y.npm -is [string] -and $y.npm -ceq $wanted -and $y.bound.node -ceq $zNode -and $y.bound.npm -ceq $wanted -and (Json $x.firstNpm) -ceq (Json $x.npm) -and $y.firstNpm -ceq $y.npm) 77
   $r=Run-Caller $true @() 2 -Native -FixtureOFS $separator
   Need ($r.rawNode -is [string] -and $r.rawNpm -is [string] -and $r.boundNode -ceq $zNode -and $r.boundNpm -ceq $wanted -and $r.ledger[0] -ceq ('exists|'+$wanted+'||')) 78
   $v=Decode $r.stderr 'NPM_CALLER_FAILURE_V1' 1024;Check-Caller $v
   Need ($v.valid -and $v.nodeCount -eq 1 -and $v.npmCount -eq 1 -and $v.nodeBoundEqualsSole -eq $true -and $v.npmBoundEqualsSole -eq $true -and $v.pairwiseLexical -ceq 'all-match') 79
  }
 }
 Case 'application-filter-and-no-fallback' {
  [Environment]::SetEnvironmentVariable('PATH',($z+';'+$a),'Process')
  $x=Native-Selection $false -CompetingNames;$y=Native-Selection $true -CompetingNames;Check-Native $x $y $zNode 2
  Need (-not (Test-Path -LiteralPath (Join-Path $z 'node_modules/npm/bin/npm-cli.js')) -and (Test-Path -LiteralPath (Join-Path $a 'node_modules/npm/bin/npm-cli.js') -PathType Leaf)) 80
  $r=Run-Caller $true @() 2 'real-missing' -Native
  Need ($r.error.Exception.Message -ceq 'npm_entry_missing' -and $r.stdout.Count -eq 0 -and $r.observed -and [Object]::ReferenceEquals($r.error.Exception,$r.boundary.Exception)) 81
  Need ($r.selectionLedger.Count -eq 1 -and $r.selectionLedger[0] -ceq 'command|node.exe|Application||1' -and $r.ledger.Count -eq 2 -and $r.ledger[0] -ceq ('exists|'+(Join-Path $z 'node_modules/npm/bin/npm-cli.js')+'||') -and $r.ledger[1] -ceq 'command|npm.cmd|Application|Stop|unlimited') 82
  $v=Decode $r.stderr 'NPM_LOOKUP_COUNT_V1' 256;Check-Count $v;Need ($v.count -eq 2) 83
 }
 Case 'missing-malformed-and-query-errors' {
  [Environment]::SetEnvironmentVariable('PATH',$empty,'Process')
  $x=Native-Selection $false;$y=Native-Selection $true;Same-Error $x.error $y.error
  Need ($null -eq $x.node -and $null -eq $y.node -and $x.commands.Count -eq 0 -and $y.commands.Count -eq 0 -and $x.error.CategoryInfo.Category -eq [Management.Automation.ErrorCategory]::ObjectNotFound) 84
  foreach($shape in @('missing','null')){$x=Malformed-Selection $false $null $shape;$y=Malformed-Selection $true $null $shape;Same-Error $x.error $y.error;Need ($null -eq $x.npm -and $null -eq $y.npm) 85}
  $hostile=[pscustomobject]@{};$hostile|Add-Member ScriptMethod ToString {throw 'injected_conversion_refusal'} -Force
  $x=Malformed-Selection $false $hostile 'value';$y=Malformed-Selection $true $hostile 'value';Same-Error $x.error $y.error
  Need ($null -eq $x.npm -and $null -eq $y.npm) 86
  $e=[IO.IOException]::new('secret-query');$x=Malformed-Selection $false $null 'error' $e;$y=Malformed-Selection $true $null 'error' $e;Same-Error $x.error $y.error $e
  Need ($x.ledger.Count -eq 1 -and $x.ledger[0] -ceq 'node.exe|Application|unlimited' -and $y.ledger.Count -eq 1 -and $y.ledger[0] -ceq 'node.exe|Application|1') 87
 }
 Case 'refusal-provenance-error-transparency' {
  [Environment]::SetEnvironmentVariable('PATH',$z,'Process')
  foreach($mode in @('success','missing','original-provider-error','package-error','package-refusal')){
   $e=[IO.IOException]::new('secret-provider');$x=Run-Caller $false @() 2 $mode $e -Native;$y=Run-Caller $true @() 2 $mode $e -Native
   Compare-Result $x $y $(if($mode -in @('original-provider-error','package-error')){$e}else{$null})
  }
  $x=Run-Caller $false @() 2 -Native;$y=Run-Caller $true @() 2 -Native
  $bad=$y.PSObject.Copy();$bad.ledger=@('wrong');Reject {Compare-Result $x $bad} 21
  $bad=$y.PSObject.Copy();$bad.observed=$false;Reject {Compare-Result $x $bad} 26
  $bad=$y.PSObject.Copy();$bad.error=[Management.Automation.ErrorRecord]::new([IO.IOException]::new('replacement'),'changed',[Management.Automation.ErrorCategory]::ReadError,$null);Reject {Compare-Result $x $bad} 25
 }
 Case 'unchanged-npm-observer-and-faults' {
  [Environment]::SetEnvironmentVariable('PATH',$z,'Process')
  foreach($count in @(2,17)){
   $x=Run-Caller $false @() $count -Native;$y=Run-Caller $true @() $count -Native;Compare-Result $x $y
   $v=Decode $y.stderr 'NPM_LOOKUP_COUNT_V1' 256;Check-Count $v
   Need ($v.valid -and $v.overflow -eq ($count -gt 16) -and $v.count -eq $(if($count -le 16){$count}else{$null}) -and $y.ledger.Count -eq 2 -and $y.ledger[1] -ceq 'command|npm.cmd|Application|Stop|unlimited') 88
  }
  $x=Run-Caller $false @() 2 -Native -BrokenWriter;$y=Run-Caller $true @() 2 -Native -BrokenWriter;Compare-Result $x $y;Need ($x.writerFault -and $y.writerFault) 89
  # Both sides use the unchanged helper; compare faulted outcomes against each other, then the unfaulted semantics/ledger.
  $x=Run-Caller $false @() 2 -Native -BookkeepingFault;$y=Run-Caller $true @() 2 -Native -BookkeepingFault;Compare-Result $x $y
  $normal=Run-Caller $true @() 2 -Native;Same-Error $normal.error $y.error;Need ((Json $normal.ledger) -ceq (Json $y.ledger) -and $y.stdout.Count -eq 0) 90
 }
 Case 'source-reversal-and-current-context' {
  Need ($newSetup.Replace($nodeLine.Replace(' Application)',' Application -TotalCount 1)'),$nodeLine) -ceq $oldSetup -and $new -ceq $old) 91
  [Environment]::SetEnvironmentVariable('PATH',$z,'Process')
  $seq=@(Run-Caller $true @() 2 -Native -CallSites $sites -Modes @('missing','success','missing'))
  Need ($seq.Count -eq 3 -and $null -eq $seq[1].error -and $null -eq $seq[1].context -and $seq[1].stderr -ceq '') 92
  foreach($i in @(0,2)){$v=Decode $seq[$i].stderr 'NPM_CALLER_FAILURE_V1' 1024;Check-Caller $v;Need ($v.valid -and $v.site -ceq $sites[$i] -and $v.nodeKind -ceq 'string' -and $v.npmKind -ceq 'string' -and $v.nodeCount -eq 1 -and $v.npmCount -eq 1 -and $v.nodeBoundEqualsSole -eq $true -and $v.npmBoundEqualsSole -eq $true) 93}
  Need (-not [Object]::ReferenceEquals($seq[0].context,$seq[2].context)) 94
  $c=Capture {Write-NpmCallerFailure $null '' ''};$v=Decode $c.text 'NPM_CALLER_FAILURE_V1' 1024;Check-Caller $v;Need (-not $v.valid -and $v.site -ceq 'unknown' -and $null -eq $v.nodeCount -and $null -eq $v.nodeBoundEqualsSole) 95
  $bad=$v.PSObject.Copy();$bad.nodeBoundEqualsSole='false';Reject {Check-Caller $bad} 13
  $captureText=(Function-Text $newSetup 'New-SetupFailureRecord')+"`n"+(Function-Text $newSetup 'Write-SetupFailureRecord')+"`n"+(Function-Text $newSetup 'Get-SetupOwnershipState');. ([scriptblock]::Create($captureText))
  $pattern='(?s)(catch \{\r?\n \$code=\$_\.Exception\.HResult; \$outcome=''FAILED''.*?\r?\n\})\r?\nfinally \{'
  $cm=[regex]::Matches($newSetup,$pattern);$om=[regex]::Matches($oldSetup,$pattern);Need ($cm.Count -eq 1 -and $om.Count -eq 1 -and $cm[0].Groups[1].Value -ceq $om[0].Groups[1].Value) 58
  $composeBlock=[scriptblock]::Create('try { . $invoke } '+$cm[0].Groups[1].Value);$script:ownedInvocationStarted=$false
  $composed=Run-Caller $true @() 2 -Native -ComposeSetup
  $v=Decode $composed.stderr 'SETUP_FAILURE_V1' 1024
  Need ($null -eq $composed.error -and $composed.stdout.Count -eq 0 -and $composed.setupOutcome -ceq 'FAILED' -and $composed.setupCode -eq $composed.boundary.Exception.HResult -and $v.outcome -ceq 'FAILED' -and $v.exitCode -eq 1 -and $v.ownership -ceq 'not-initialized' -and $v.behavioralInvocations -eq 0) 96
 }
 Need ($rows.Count -eq 8 -and $expected.Count -eq 8) 59
 for($i=0;$i -lt 8;$i++){Need ($rows[$i].name -ceq $expected[$i]) 60}
}catch{$originalFailure=$_;throw}
finally {
 [Environment]::SetEnvironmentVariable('PATH',$savedPath,'Process');[Console]::SetError($savedErrorWriter)
 $FixtureState.pathRestored=[string]::Equals([Environment]::GetEnvironmentVariable('PATH','Process'),$savedPath,[StringComparison]::Ordinal)
 $nowOFS=Get-Variable -Name OFS -ErrorAction SilentlyContinue
 $FixtureState.ofsUnchanged=($null -eq $savedOFS -and $null -eq $nowOFS) -or ($null -ne $savedOFS -and $null -ne $nowOFS -and [Object]::Equals($savedOFSValue,$nowOFS.Value))
 $FixtureState.writerRestored=[Object]::ReferenceEquals([Console]::Error,$savedErrorWriter)
 try {
  if($created){
   # Bounded direct inventories; never recursively delete an unknown or reparse entry.
   $known=@('')+$fixturePaths
   foreach($rel in $known){
    $dir=if($rel -ceq ''){$FixtureRoot}else{Join-Path $FixtureRoot $rel}
    if(-not [IO.Directory]::Exists($dir)){continue}
    if(([IO.File]::GetAttributes($dir) -band [IO.FileAttributes]::ReparsePoint) -ne 0){throw 'fixture_reparse'}
    $count=0
    foreach($entry in [IO.Directory]::EnumerateFileSystemEntries($dir)){
     $count++;if($count -gt 8){throw 'fixture_inventory_cap'}
     $relative=$entry.Substring($FixtureRoot.Length+1).Replace('\','/')
     if($relative -cnotin $fixturePaths -and $relative -cnotin $fixtureFiles){throw 'fixture_unknown_entry'}
     if(([IO.File]::GetAttributes($entry) -band [IO.FileAttributes]::ReparsePoint) -ne 0){throw 'fixture_reparse'}
    }
   }
   foreach($rel in $fixtureFiles){$file=Join-Path $FixtureRoot $rel;if([IO.File]::Exists($file)){if(([IO.FileInfo]::new($file)).Length -ne 0){throw 'fixture_bytes_changed'};[IO.File]::Delete($file)}}
   for($j=$fixturePaths.Count-1;$j -ge 0;$j--){$dir=Join-Path $FixtureRoot $fixturePaths[$j];if([IO.Directory]::Exists($dir)){[IO.Directory]::Delete($dir,$false)}}
   [IO.Directory]::Delete($FixtureRoot,$false)
  }
  $FixtureState.cleaned= -not [IO.Directory]::Exists($FixtureRoot)
 }catch{$FixtureState.cleaned=$false}
 if(-not ($FixtureState.pathRestored -and $FixtureState.ofsUnchanged -and $FixtureState.writerRestored -and $FixtureState.cleaned)){$FixtureState.complete=$false;if($null -eq $originalFailure){throw 'fixture_restoration_failed'}}else{$FixtureState.complete=$true}
}
$rows.ToArray()|ConvertTo-Json -Depth 5 -Compress

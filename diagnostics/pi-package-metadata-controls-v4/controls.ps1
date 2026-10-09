param([hashtable]$Progress=@{case=$null;assertion=0;completed=@()},[Parameter(Mandatory)][string]$FixtureRoot,[hashtable]$FixtureState,[Parameter(Mandatory)][string]$Node)
$ErrorActionPreference='Stop'
$script:depProgress=$Progress
$expected=@(Get-Content -Raw (Join-Path $PSScriptRoot 'expected-controls.json')|ConvertFrom-Json)
$schema=Get-Content -Raw (Join-Path $PSScriptRoot 'schema.json')|ConvertFrom-Json -AsHashtable
$old=Get-Content -Raw (Join-Path $PSScriptRoot 'original-setup-only.ps1')
$new=Get-Content -Raw (Join-Path $PSScriptRoot 'setup-only.ps1')
$rows=[Collections.Generic.List[object]]::new();$savedWriter=[Console]::Error;$savedPath=$env:PATH;$savedOfs=$OFS
$child=$null;$created=$false;$knownFiles=[Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
function Need([bool]$Value,[int]$Id){$script:depProgress.assertion=$Id;if(-not $Value){throw ('dependency_ps_'+$Id)}}
function Function-Text([string]$Source,[string]$Name){$t=$null;$e=$null;$ast=[Management.Automation.Language.Parser]::ParseInput($Source,[ref]$t,[ref]$e);Need ($e.Count -eq 0) 1;$f=@($ast.FindAll({param($x)$x -is [Management.Automation.Language.FunctionDefinitionAst] -and $x.Name -ceq $Name},$true));Need ($f.Count -eq 1) 2;return $f[0].Extent.Text}
$relayText=Function-Text $new 'Write-DependencyFailureMetadata'
$oldRelayText=Function-Text $old 'Write-DependencyFailureMetadata'
$oldRelayBlock=[scriptblock]::Create($oldRelayText)
$relayBlock=[scriptblock]::Create($relayText)
. $relayBlock
function Json($x){return ($x|ConvertTo-Json -Depth 10 -Compress)}
function Capture([scriptblock]$Body,[switch]$Broken){$previous=[Console]::Error;$w=[IO.StringWriter]::new();$failed=$null;$out=@();$direct=$false;try{if($Broken){$w.Dispose()};[Console]::SetError($w);if($Broken){try{[Console]::Error.WriteLine('direct-installed-writer-proof')}catch{$direct=$true};Need $direct 3};try{$out=@(& $Body)}catch{$failed=$_};return @{text=$(if($Broken){''}else{$w.ToString()});error=$failed;output=$out;directFault=$direct}}finally{[Console]::SetError($previous);$w.Dispose()}}
function Base-Record {return [ordered]@{schema='dependency-failure-v1';stage='package-keys';group='adapter';code='ERR_ASSERTION';outcome='FAILED';behavioralInvocations=0}}
function Set-Bytes([byte[]]$Bytes){$file=Join-Path $FixtureRoot 'dependency-failure.json';[IO.File]::WriteAllBytes($file,$Bytes);$null=$knownFiles.Add($file)}
function Set-Text([string]$Text){Set-Bytes ([Text.UTF8Encoding]::new($false).GetBytes($Text))}
function Relay([switch]$Broken){return Capture {Write-DependencyFailureMetadata $FixtureRoot} -Broken:$Broken}
function Check-Line($Capture,$Expected){Need ($null -eq $Capture.error -and $Capture.output.Count -eq 0) 4;$lines=@($Capture.text -split '\r?\n'|Where-Object {$_ -ne ''});Need ($lines.Count -eq 1 -and $lines[0].Length -le 768 -and $lines[0].StartsWith('DEPENDENCY_FAILURE_V1 ')) 5;Need (-not $lines[0].Contains('secret')) 6;$v=$lines[0].Substring(22)|ConvertFrom-Json -AsHashtable;Need ((($v.Keys|Sort-Object)-join '|') -ceq 'behavioralInvocations|code|group|outcome|schema|stage') 7;foreach($k in $Expected.Keys){Need ($v[$k] -ceq $Expected[$k]) 8};return $v}
function No-Line($Capture){Need ($null -eq $Capture.error -and $Capture.output.Count -eq 0 -and $Capture.text -ceq '') 9}
function Reject([scriptblock]$Body,[int]$Id){$e=$null;try{& $Body}catch{$e=$_};Need ($null -ne $e -and $e.Exception.Message -ceq ('dependency_ps_'+$Id)) 10}
function Check-JsEnvelope($js){Need ($js.schema -ceq 'dependency-js-controls-v1' -and $js.outcome -cin @('PASS','FAILED') -and ($js.caseIndex -is [int] -or $js.caseIndex -is [long]) -and $js.caseIndex -ge 0 -and $js.caseIndex -le 6 -and ($js.assertion -is [int] -or $js.assertion -is [long]) -and $js.assertion -ge 0 -and $js.assertion -le 96 -and $js.created -is [bool] -and $js.cleaned -is [bool] -and $js.completed -is [Array] -and $js.completed.Rank -eq 1 -and $js.completed.Count -le 6 -and $js.results -is [Array] -and $js.results.Rank -eq 1 -and ($js.setupInvocations -is [int] -or $js.setupInvocations -is [long]) -and ($js.packageEntryExecutions -is [int] -or $js.packageEntryExecutions -is [long]) -and ($js.privateLogReads -is [int] -or $js.privateLogReads -is [long]) -and $js.setupInvocations -eq 0 -and $js.packageEntryExecutions -eq 0 -and $js.privateLogReads -eq 0) 20}
function Check-JsSuccess($Value,[int]$ExitCode,[string]$ErrorText){Need ($ExitCode -eq 0 -and $ErrorText -ceq '' -and $Value.outcome -ceq 'PASS' -and $Value.created -and $Value.cleaned -and $Value.results.Count -eq 6 -and $Value.completed.Count -eq 6 -and $Value.caseIndex -eq 6) 18}
function Case([int]$Index,[scriptblock]$Body){$script:depProgress.case=$expected[$Index-1];$script:depProgress.assertion=0;Need ($js.results[$Index-1].name -ceq $expected[$Index-1] -and $js.results[$Index-1].outcome -ceq 'PASS') 11;$out=@(& $Body);Need ($out.Count -eq 0) 12;$rows.Add(@{name=$expected[$Index-1];outcome='PASS'});$script:depProgress.completed=@($script:depProgress.completed)+$expected[$Index-1]}
try {
 Need ($IsWindows -and -not(Test-Path -LiteralPath $FixtureRoot) -and [IO.Path]::IsPathFullyQualified($Node)) 13
 $null=New-Item -ItemType Directory -Path $FixtureRoot;$created=$true;$FixtureState.created=$true
 # One pinned control process only. Only exact extracted comparison/helper/catch bodies use synthetic providers; no setup or installed-package body is invoked.
 $si=[Diagnostics.ProcessStartInfo]::new();$si.FileName=$Node;$si.UseShellExecute=$false;$si.CreateNoWindow=$true;$si.RedirectStandardOutput=$true;$si.RedirectStandardError=$true
 $si.ArgumentList.Add((Join-Path $PSScriptRoot 'node-controls.mjs'));$si.ArgumentList.Add((Join-Path $FixtureRoot 'node-fixture'))
 $child=[Diagnostics.Process]::new();$child.StartInfo=$si;$FixtureState.childStarted=$true;Need ($child.Start()) 14
 $stdoutTask=$child.StandardOutput.ReadToEndAsync();$stderrTask=$child.StandardError.ReadToEndAsync()
 if(-not $child.WaitForExit(30000)){$FixtureState.childKilled=$true;$child.Kill();Need ($child.WaitForExit(1000)) 15;throw 'dependency_node_timeout'}
 $FixtureState.childExited=$true
 Need ($stdoutTask.Wait(1000) -and $stderrTask.Wait(1000)) 16
 $stdout=$stdoutTask.Result;$stderr=$stderrTask.Result
 Need ([Text.Encoding]::UTF8.GetByteCount($stdout) -le 65536 -and [Text.Encoding]::UTF8.GetByteCount($stderr) -le 65536) 17
 $js=$stdout|ConvertFrom-Json
 Need ((($js.PSObject.Properties.Name|Sort-Object)-join '|') -ceq 'assertion|caseIndex|cleaned|completed|created|outcome|packageEntryExecutions|privateLogReads|results|schema|setupInvocations') 19
 Check-JsEnvelope $js
 for($j=0;$j -lt $js.completed.Count;$j++){Need ($js.completed[$j] -is [string] -and $js.completed[$j] -ceq $expected[$j]) 41}
 $FixtureState.jsCase=[int]$js.caseIndex;$FixtureState.jsAssertion=[int]$js.assertion;$FixtureState.jsOutcome=[string]$js.outcome
 Check-JsSuccess $js $child.ExitCode $stderr
 for($i=0;$i -lt 6;$i++){Need ((($js.results[$i].PSObject.Properties.Name|Sort-Object)-join '|') -ceq 'name|outcome') 21;Need ($js.results[$i].name -ceq $expected[$i] -and $js.results[$i].outcome -ceq 'PASS' -and $js.completed[$i] -ceq $expected[$i]) 22}
 $FixtureState.jsComplete=$true;$FixtureState.jsAssertion=[int]$js.assertion
 Case 1 {
  $delta=Get-Content -Raw (Join-Path $PSScriptRoot 'setup-delta.json')|ConvertFrom-Json
  $reverse=$new;for($i=$delta.changes.Count-1;$i -ge 0;$i--){$c=$delta.changes[$i];Need ([regex]::Matches($reverse,[regex]::Escape($c.new)).Count -eq 1) 23;$reverse=$reverse.Replace($c.new,$c.old)};Need ($reverse -ceq $old) 24
  Need ($relayText -ceq (Get-Content -Raw (Join-Path $PSScriptRoot 'relay-function.ps1')).TrimEnd([char[]]"`r`n")) 25
 }
 Case 2 {
  $metadataStages=@($schema.stages|Where-Object {$_ -clike 'pm-*'})
  Need ($metadataStages.Count -eq 13) 45
  foreach($stage in $metadataStages){
   $record=Base-Record;$record.stage=$stage;Set-Text (Json $record);$null=Check-Line (Relay) $record
   & {. $oldRelayBlock;No-Line (Capture {Write-DependencyFailureMetadata $FixtureRoot})}
  }
  foreach($stage in @('package-metadata','unknown')){$record=Base-Record;$record.stage=$stage;Set-Text (Json $record);$null=Check-Line (Relay) $record}
  $record=Base-Record;Set-Text (Json $record);$good=Relay
  $changed=Base-Record;$changed.code='OTHER';Reject {$null=Check-Line $good $changed} 8
  Reject {No-Line $good} 9
 }
 Case 3 {
  foreach($pair in @(@('pm-identity','adapter'),@('pm-unclassified','pi'),@('pm-equal-call','adapter'))){$record=Base-Record;$record.stage=$pair[0];$record.group=$pair[1];Set-Text (Json $record);$null=Check-Line (Relay) $record}
  Remove-Item -LiteralPath (Join-Path $FixtureRoot 'dependency-failure.json');No-Line (Relay)
  $record=Base-Record;$record.stage='unknown';$record.group='unknown';Set-Text (Json $record);$null=Check-Line (Relay) $record
 }
 Case 4 {
  # Exact wrapper projection and PASS-admission controls; no wrapper/runtime invocation.
  $contract=Get-Content -Raw (Join-Path $PSScriptRoot 'progress-contract.json')|ConvertFrom-Json -AsHashtable
  $wrapper=Get-Content -Raw (Join-Path $PSScriptRoot 'run-controls.ps1')
  $back=$wrapper
  for($n=$contract.changes.Count-1;$n -ge 0;$n--){$change=$contract.changes[$n];Need ([regex]::Matches($back,[regex]::Escape($change.new)).Count -eq 1) 46;$back=$back.Replace($change.new,$change.old)}
  $backHash=[Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($back))).ToLowerInvariant()
  Need ($backHash -ceq $contract.originalWrapperSha256) 47
  function Unique-Span([string]$Text,[string]$Start,[string]$End){Need ([regex]::Matches($Text,[regex]::Escape($Start)).Count -eq 1 -and [regex]::Matches($Text,[regex]::Escape($End)).Count -eq 1) 48;$a=$Text.IndexOf($Start,[StringComparison]::Ordinal);$b=$Text.IndexOf($End,$a,[StringComparison]::Ordinal);Need ($b -gt $a) 48;return $Text.Substring($a,$b-$a)}
  $projection=Unique-Span $wrapper ("`n"+'  $progressSafe=[ordered]@{') ("`n"+'  $fixtureSafe=[ordered]@{')
  $oldProjection=Unique-Span $back ("`n"+'  $progressSafe=[ordered]@{') ("`n"+'  $fixtureSafe=[ordered]@{')
  $gateText=Function-Text $wrapper 'Test-ControlProgressAgreement';. ([scriptblock]::Create($gateText))
  $admission=Unique-Span $wrapper '  # BEGIN COMPLETE CONTROL PROGRESS ADMISSION' '  # END COMPLETE CONTROL PROGRESS ADMISSION'
  function Project-Progress($Value,[string]$Text=$projection){$body=[scriptblock]::Create('param($controlProgress)' + "`n" + $Text + "`n" + 'return ,$progressSafe');return (& $body $Value)}
  function Progress-Sample([int]$Count=6){$prefix=[object[]]@($expected|Select-Object -First $Count);return @{case=$(if($Count -eq 0){$null}else{$expected[$Count-1]});assertion=[int]40;completed=$prefix}}
  function Rows-Sample {return ,([object[]]@($expected|ForEach-Object {@{name=$_;outcome='PASS'}}))}
  function Check-Projected($Value,[bool]$Valid){Need ($Value -is [Collections.IDictionary] -and (($Value.Keys|Sort-Object)-join '|') -ceq 'assertion|case|completed|valid' -and $Value.valid -is [bool] -and $Value.valid -eq $Valid) 49;if(-not $Valid){Need ($null -eq $Value.case -and $null -eq $Value.assertion -and $Value.completed -is [object[]] -and $Value.completed.Count -eq 0) 50}}
  function Admit-Progress($Value,$Items,[string]$Initial='PASS',[string]$Text=$admission){& {
   $progressSafe=$Value;$rows=$Items;$outcome=$Initial;$phase='sentinel-phase';$category='sentinel-category';$hresult=17
   . ([scriptblock]::Create($Text))
   return @{outcome=$outcome;phase=$phase;category=$category;hresult=$hresult;acceptedCount=$(if($outcome -eq 'PASS'){6}else{0});progress=$progressSafe;rows=$rows}
  }}
  $complete=Project-Progress (Progress-Sample);$completeRows=Rows-Sample;Check-Projected $complete $true
  Need ($complete.case -ceq $expected[5] -and $complete.assertion -ceq 40 -and ($complete.completed -join '|') -ceq ($expected -join '|') -and (Test-ControlProgressAgreement $complete $completeRows)) 51
  $admitted=Admit-Progress $complete $completeRows;Need ($admitted.outcome -ceq 'PASS' -and $admitted.acceptedCount -eq 6 -and $admitted.phase -ceq 'sentinel-phase') 52
  for($length=0;$length -lt 6;$length++){$partial=Project-Progress (Progress-Sample $length);Check-Projected $partial $true;Need (-not (Test-ControlProgressAgreement $partial $completeRows)) 53;$refused=Admit-Progress $partial $completeRows;Need ($refused.outcome -ceq 'FAILED' -and $refused.acceptedCount -eq 0 -and $refused.phase -ceq 'progress-admission' -and [Object]::ReferenceEquals($refused.progress,$partial)) 54}
  foreach($case in $expected){$sample=Progress-Sample;$sample.case=$case;$newProjection=Project-Progress $sample;Check-Projected $newProjection $true;Check-Projected (Project-Progress $sample $oldProjection) $false}
  foreach($case in $contract.oldCases){$sample=Progress-Sample;$sample.case=$case;Check-Projected (Project-Progress $sample) $false}
  foreach($fault in @('missing','extra','case-type','old-prefix','order','duplicate','skip','extra-prefix','prefix-type','assertion-null','assertion-bool','assertion-string','assertion-long','assertion-fraction','assertion-negative','assertion-high','prefix-rank','prefix-lowerbound')){
   $sample=Progress-Sample
   switch($fault){
    'missing' {$sample.Remove('assertion')}
    'extra' {$sample.extra='fixed'}
    'case-type' {$sample.case=1}
    'old-prefix' {$sample.completed=[object[]]@($contract.oldCases)}
    'order' {$a=$sample.completed[0];$sample.completed[0]=$sample.completed[1];$sample.completed[1]=$a}
    'duplicate' {$sample.completed[1]=$sample.completed[0]}
    'skip' {$sample.completed=[object[]]@($expected[1..5])}
    'extra-prefix' {$sample.completed=[object[]]@($expected)+@($expected[5])}
    'prefix-type' {$sample.completed=[string]'fixed'}
    'assertion-null' {$sample.assertion=$null}
    'assertion-bool' {$sample.assertion=$true}
    'assertion-string' {$sample.assertion='40'}
    'assertion-long' {$sample.assertion=[long]40}
    'assertion-fraction' {$sample.assertion=40.5}
    'assertion-negative' {$sample.assertion=-1}
    'assertion-high' {$sample.assertion=97}
    'prefix-rank' {$sample.completed=[object[,]]::new(1,1)}
    'prefix-lowerbound' {$sample.completed=[Array]::CreateInstance([object],[int[]]@(1),[int[]]@(1))}
   }
   $invalid=Project-Progress $sample;Check-Projected $invalid $false;$refused=Admit-Progress $invalid $completeRows;Need ($refused.outcome -ceq 'FAILED' -and $refused.acceptedCount -eq 0) 55
  }
  foreach($counter in @(0,96)){$sample=Progress-Sample;$sample.assertion=[int]$counter;$bounded=Project-Progress $sample;Check-Projected $bounded $true;Need (-not (Test-ControlProgressAgreement $bounded $completeRows)) 56}
  foreach($fault in @('missing','incomplete','name','order','type','outcome')){
   $items=Rows-Sample
   switch($fault){'missing' {$items=$null};'incomplete' {$items=[object[]]@($items[0..4])};'name' {$items[0].name='fixed-unknown'};'order' {$a=$items[0];$items[0]=$items[1];$items[1]=$a};'type' {$items[0]=[pscustomobject]$items[0]};'outcome' {$items[0].outcome='FAILED'}}
   $refused=Admit-Progress $complete $items;Need ($refused.outcome -ceq 'FAILED' -and $refused.acceptedCount -eq 0) 57
  }
  $wrongLast=Project-Progress (Progress-Sample);$wrongLast.case=$expected[0];Need ((Admit-Progress $wrongLast $completeRows).outcome -ceq 'FAILED') 58
  Need (-not (Test-ControlProgressAgreement $null $completeRows) -and (Admit-Progress $null $completeRows).outcome -ceq 'FAILED') 58
  $historical=$contract.withheldProgress
  Check-Projected $historical $false
  Need ($contract.withheldOutcome -ceq 'PASS' -and $contract.withheldAcceptedCount -eq 6 -and ($contract.withheldRows.name -join '|') -ceq ($expected -join '|')) 59
  Need ((Admit-Progress $historical $completeRows).outcome -ceq 'FAILED') 60
  $preserved=Admit-Progress $historical $completeRows 'FAILED';Need ($preserved.outcome -ceq 'FAILED' -and $preserved.phase -ceq 'sentinel-phase' -and $preserved.category -ceq 'sentinel-category' -and $preserved.hresult -eq 17 -and [Object]::ReferenceEquals($preserved.progress,$historical)) 61
  # Exact intended refusal oracles: an unrelated exception cannot count as a passing mutant.
  Reject {Check-Projected (Project-Progress (Progress-Sample) $oldProjection) $true} 49
  $badAdmission=$admission.Replace('$outcome=''FAILED'';','$outcome=''PASS'';');Need ($badAdmission -cne $admission) 62
  Reject {Need ((Admit-Progress $historical $completeRows 'PASS' $badAdmission).outcome -ceq 'FAILED') 63} 63
  $badOrder=$projection.Replace('$cp.completed[$j] -cne $allowedCases[$j]','$false');Need ($badOrder -cne $projection) 64
  $wrongOrder=Progress-Sample;$x=$wrongOrder.completed[0];$wrongOrder.completed[0]=$wrongOrder.completed[1];$wrongOrder.completed[1]=$x
  Reject {Check-Projected (Project-Progress $wrongOrder $badOrder) $false} 49
  # Exercise the admission guard itself with native JSON before any internal casts.
  $envelopeJson=$js|ConvertTo-Json -Depth 10 -Compress
  # The same success admission used for actual child output must refuse incomplete/failed records.
  Check-JsSuccess $js 0 ''
  foreach($field in @('outcome','created','cleaned','results','completed','caseIndex')){
   $sample=$envelopeJson|ConvertFrom-Json
   switch($field){
    'outcome' {$sample.outcome='FAILED'}
    'created' {$sample.created=$false}
    'cleaned' {$sample.cleaned=$false}
    'results' {$sample.results=@($sample.results[0])}
    'completed' {$sample.completed=@($sample.completed[0])}
    'caseIndex' {$sample.caseIndex=[long]5}
   }
   Reject {Check-JsSuccess $sample 0 ''} 18
  }
  Reject {Check-JsSuccess $js 1 ''} 18
  Reject {Check-JsSuccess $js 0 'fixed-nonempty'} 18

  $nativeEnvelope=$envelopeJson|ConvertFrom-Json
  Need ($nativeEnvelope.caseIndex -is [long] -and $nativeEnvelope.assertion -is [long]) 42
  Check-JsEnvelope $nativeEnvelope
  foreach($field in @('caseIndex','assertion')){
   $maximum=if($field -ceq 'caseIndex'){6}else{96}
   $pattern='"'+$field+'":-?[0-9]+'
   Need ([regex]::Matches($envelopeJson,$pattern).Count -eq 1) 43
   foreach($value in @(0,$maximum)){
    $text=[regex]::Replace($envelopeJson,$pattern,('"'+$field+'":'+$value))
    $sample=$text|ConvertFrom-Json
    Need ($sample.$field -is [long] -and $sample.$field -eq $value) 44
    Check-JsEnvelope $sample
    $sample.$field=[int]$value;Check-JsEnvelope $sample
    $sample.$field=[long]$value;Check-JsEnvelope $sample
   }
   foreach($token in @('true','false','"0"','0.5','0.0','null','-1',[string]($maximum+1),'9223372036854775807')){
    $text=[regex]::Replace($envelopeJson,$pattern,('"'+$field+'":'+$token))
    $sample=$text|ConvertFrom-Json
    Reject {Check-JsEnvelope $sample} 20
   }
  }
  $bad=[Collections.Generic.List[string]]::new();$valid=Json (Base-Record)
  $bad.Add('null');$bad.Add('[]');$bad.Add('{}');$bad.Add($valid.Replace('"code":"ERR_ASSERTION"','"code":null'));$bad.Add($valid.Replace('"behavioralInvocations":0','"behavioralInvocations":"0"'));$bad.Add($valid.Replace('"stage":"package-keys"','"stage":"PACKAGE-KEYS"'));$bad.Add($valid.Replace('"code":"ERR_ASSERTION"','"code":"secret"'));$bad.Add($valid.TrimEnd('}')+',"secret":"never-export"}')
  foreach($field in @('packageName','header','offset','path','message','stack','url')){$bad.Add($valid.TrimEnd('}')+',"'+$field+'":"secret-never-export"}')}
  foreach($stage in @('pm-unknown','PM-IDENTITY','pm-Identity','pm-identity-secret')){$bad.Add($valid.Replace('"stage":"package-keys"','"stage":"'+$stage+'"'))}
  foreach($v in $bad){Set-Text $v;No-Line (Relay)}
  # Characterize the actual native parser, then compare relay to that parsed-object contract.
  $duplicates=@($valid.Replace('"code":"ERR_ASSERTION"','"code":"ERR_ASSERTION","code":"OTHER"'),$valid.Replace('"code":"ERR_ASSERTION"','"code":"OTHER","code":"ERR_ASSERTION"'))
  $observed=[Collections.Generic.List[string]]::new()
  foreach($text in $duplicates){$parsed=$null;$parseFailed=$false;try{$parsed=ConvertFrom-Json -InputObject $text -AsHashtable}catch{$parseFailed=$true};Set-Text $text;if($parseFailed){No-Line (Relay);$observed.Add('rejected')}else{Need ($parsed.Count -eq 6 -and $parsed.code -cin @('ERR_ASSERTION','OTHER')) 26;$null=Check-Line (Relay) $parsed;$observed.Add([string]$parsed.code)}}
  if(($observed -join '|') -ceq 'OTHER|ERR_ASSERTION'){$FixtureState.duplicateBehavior='last-wins'}elseif(($observed -join '|') -ceq 'ERR_ASSERTION|OTHER'){$FixtureState.duplicateBehavior='first-wins'}elseif(($observed -join '|') -ceq 'rejected|rejected'){$FixtureState.duplicateBehavior='rejected'}else{throw 'duplicate_parser_unclassified'}
  $text=$valid.Replace('"code":"ERR_ASSERTION"','"code":"secret","code":"ERR_ASSERTION"');$parsed=$null;try{$parsed=ConvertFrom-Json -InputObject $text -AsHashtable}catch{};Set-Text $text;if($null -ne $parsed -and $parsed.code -ceq 'ERR_ASSERTION'){$null=Check-Line (Relay) $parsed}else{No-Line (Relay)}
  $text=$valid.Replace('"schema"','"\u0073chema"');Set-Text $text;$null=Check-Line (Relay) (Base-Record)
  foreach($tokens in @('"stage":"tar-a04-form-root-dir","stage":"tar-a04-form-pax-x"','"stage":"tar-a04-form-pax-x","stage":"tar-a04-form-root-dir"','"stage":"secret","stage":"tar-a04-form-root-dir"','"stage":"tar-a04-form-root-dir","stage":"secret"')){
   $text=$valid.Replace('"stage":"package-keys"',$tokens);$parsed=$null;try{$parsed=ConvertFrom-Json -InputObject $text -AsHashtable}catch{}
   Set-Text $text
   if($null -ne $parsed -and $parsed.stage -cin @('tar-a04-form-root-dir','tar-a04-form-pax-x')){$null=Check-Line (Relay) $parsed}else{No-Line (Relay)}
  }
 }
 Case 5 {
  $valid=Json (Base-Record);$n=[Text.Encoding]::UTF8.GetByteCount($valid);Need ($n -lt 512) 27
  Set-Text ($valid+(' '*(512-$n)));$null=Check-Line (Relay) (Base-Record)
  Set-Text ($valid+(' '*(513-$n)));No-Line (Relay)
  Set-Bytes ([byte[]]@(255,254,255));No-Line (Relay)
  Set-Text ($valid.Substring(0,$valid.Length-1));No-Line (Relay)
  Set-Text $valid
  $locked=[IO.File]::Open((Join-Path $FixtureRoot 'dependency-failure.json'),[IO.FileMode]::Open,[IO.FileAccess]::ReadWrite,[IO.FileShare]::None)
  try{No-Line (Relay)}finally{$locked.Dispose()};$null=Check-Line (Relay) (Base-Record)
  & {function Get-Item {param([string]$LiteralPath) return [pscustomobject]@{Attributes=[IO.FileAttributes]::ReparsePoint}};. $relayBlock;No-Line (Capture {Write-DependencyFailureMetadata $FixtureRoot})}
  # Explicit model-only stream provider substitution; target relay bytes stay unchanged.
  $open='[IO.File]::Open($path,[IO.FileMode]::Open,[IO.FileAccess]::Read,[IO.FileShare]::Read)'
  Need ([regex]::Matches($relayText,[regex]::Escape($open)).Count -eq 1) 28
  $model=$relayText.Replace($open,'(Open-RelayFixtureStream)');Need ($model.Replace('(Open-RelayFixtureStream)',$open) -ceq $relayText) 29
  foreach($fault in @('read','dispose')){& {
   $fx=@{fault=$fault;reads=0;disposes=0;bytes=[Text.Encoding]::UTF8.GetBytes($valid)}
   function Open-RelayFixtureStream { $v=[pscustomobject]@{Length=$fx.bytes.Length};$v|Add-Member ScriptMethod Read {param($buffer,$offset,$count)$fx.reads++;if($fx.fault -eq 'read'){throw 'stream_read_fault'};if($fx.reads -gt 1){return 0};[Array]::Copy($fx.bytes,0,$buffer,$offset,$fx.bytes.Length);return $fx.bytes.Length};$v|Add-Member ScriptMethod Dispose {$fx.disposes++;if($fx.fault -eq 'dispose'){throw 'stream_dispose_fault'}};return $v }
   . ([scriptblock]::Create($model));No-Line (Capture {Write-DependencyFailureMetadata $FixtureRoot});Need ($fx.reads -ge 1 -and $fx.disposes -eq 1) 30
  }}
 }
 Case 6 {
  $record=Base-Record;$record.stage='tar-a04-form-pax-x';Set-Text (Json $record);$broken=Relay -Broken;No-Line $broken;Need $broken.directFault 31
  $common=(Function-Text $new 'New-SetupFailureRecord')+"`n"+(Function-Text $new 'Write-SetupFailureRecord');. ([scriptblock]::Create($common))
  $pattern='(?s)(catch \{\r?\n \$code=\$_\.Exception\.HResult; \$outcome=''FAILED''.*?\r?\n\})\r?\nfinally \{'
  $oc=[regex]::Matches($old,$pattern);$nc=[regex]::Matches($new,$pattern);Need ($oc.Count -eq 1 -and $nc.Count -eq 1) 32
  function Compose([bool]$Marked,[string]$Stage,[string]$Ownership,[switch]$WriterFault,[switch]$FileFault){& {
   function Get-SetupOwnershipState {return $Ownership}
   if($FileFault){function Get-Item {param([string]$LiteralPath)throw 'file_metadata_fault'}}
   if($Marked){. $relayBlock}else{. $oldRelayBlock}
   $body=if($Marked){$nc[0].Groups[1].Value}else{$oc[0].Groups[1].Value}
   $injected=[IO.IOException]::new('secret-original');$diagnosticStage=$Stage;$outcome='FAILED';$code=0;$WorkRoot=$FixtureRoot
   $observed=$null;$savedNew=${function:New-SetupFailureRecord}
   function New-SetupFailureRecord([string]$Phase,[string]$Stage,[string]$Category,[int]$HResult,[string]$Ownership){$observedHolder.value=$_;& $savedNew $Phase $Stage $Category $HResult $Ownership}
   $observedHolder=@{value=$null}
   $invoke=[scriptblock]::Create('try {throw $injected} '+$body+'; [pscustomobject]@{outcome=$outcome;code=$code;record=$originalFailure}')
   $c=Capture {. $invoke} -Broken:$WriterFault
   Need ($null -eq $c.error -and $c.output.Count -eq 1 -and $c.output[0].outcome -ceq 'FAILED' -and $c.output[0].code -eq $injected.HResult) 33
   Need ($observedHolder.value -is [Management.Automation.ErrorRecord] -and [Object]::ReferenceEquals($observedHolder.value.Exception,$injected)) 34
   return @{capture=$c;record=$c.output[0].record}
  }}
  foreach($stage in @('dependency-acceptance','native-controls')){foreach($own in @('safe','unresolved','not-initialized')){
   $a=Compose $false $stage $own;$b=Compose $true $stage $own
   Need ((Json $a.record) -ceq (Json $b.record)) 35
   $lines=@($b.capture.text -split '\r?\n'|Where-Object{$_ -ne ''});$baseline=@($a.capture.text -split '\r?\n'|Where-Object{$_ -ne ''})
   $want=($stage -ceq 'dependency-acceptance' -and $own -ceq 'safe');Need ($baseline.Count -eq $(if($want){2}else{1}) -and $lines.Count -eq $baseline.Count -and ($lines -join "`n") -ceq ($baseline -join "`n")) 36
   if($want){Need ($lines[1].StartsWith('DEPENDENCY_FAILURE_V1 ')) 37}
  }}
  foreach($kind in @('writer','file')){$x=Compose $true 'dependency-acceptance' 'safe' -WriterFault:($kind -eq 'writer') -FileFault:($kind -eq 'file');if($kind -eq 'writer'){Need ($x.capture.directFault -and $x.capture.text -ceq '') 38}else{Need (@($x.capture.text -split '\r?\n'|Where-Object{$_ -ne ''}).Count -eq 1) 39}}
 }
 Need ($rows.Count -eq 6) 40
} finally {
 [Console]::SetError($savedWriter)
 $FixtureState.writerRestored=[Object]::ReferenceEquals([Console]::Error,$savedWriter);$FixtureState.pathRestored=($env:PATH -ceq $savedPath);$FixtureState.ofsUnchanged=($OFS -ceq $savedOfs)
 if($null -ne $child){try{if(-not $child.HasExited -and -not $FixtureState.childKilled){$FixtureState.childKilled=$true;$child.Kill()};$FixtureState.childExited=$child.WaitForExit(1000)}catch{$FixtureState.childExited=$false};if($FixtureState.childExited){$child.Dispose();$FixtureState.childDisposed=$true}}
 if($created -and $FixtureState.childExited){try{foreach($f in Get-ChildItem -LiteralPath $FixtureRoot -Force){if($f.PSIsContainer -or ($f.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0 -or -not $knownFiles.Contains($f.FullName)){throw 'fixture_unexpected'}};foreach($file in $knownFiles){if(Test-Path -LiteralPath $file){Remove-Item -LiteralPath $file}};Remove-Item -LiteralPath $FixtureRoot;$FixtureState.cleaned=-not(Test-Path -LiteralPath $FixtureRoot)}catch{$FixtureState.cleaned=$false}}
 $FixtureState.complete=($FixtureState.childExited -and $FixtureState.childDisposed -and $FixtureState.cleaned -and $FixtureState.writerRestored -and $FixtureState.pathRestored -and $FixtureState.ofsUnchanged -and $FixtureState.jsComplete)
}
$rows.ToArray()|ConvertTo-Json -Depth 5 -Compress

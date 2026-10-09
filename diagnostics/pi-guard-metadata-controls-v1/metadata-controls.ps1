param([hashtable]$Progress=@{})
$ErrorActionPreference='Stop'
$results=@()
$expected=@(Get-Content -Raw (Join-Path $PSScriptRoot 'guard-case-names.json')|ConvertFrom-Json)
. (Join-Path $PSScriptRoot 'diagnostic-functions.ps1')
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $PSScriptRoot 'marked-suite.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count -ne 0){throw 'candidate_parse'}
foreach($name in @('Set-ControlDiagnostic','Assert')){
 $nodes=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst]},$true)|Where-Object {$_.Name -ceq $name})
 if($nodes.Count -ne 1){throw 'helper_identity'}
 Invoke-Expression $nodes[0].Extent.Text
}
function Ensure($Value){if(-not $Value){throw 'metadata_control_assertion'}}
function New-State {return @{stage='not-invoked';case=$null;assertion=$null;outcome='not-entered';completed=@()}}
function Check([string]$Name,[scriptblock]$Body){
 try {$Progress.current=$Name;$Progress.stage='case'} catch {}
 & $Body
 $script:results+=@{name=$Name;outcome='PASS'}
 try {$Progress.completed=@($Progress.completed)+@($Name)} catch {}
}
function Shape($Record){
 Ensure ((@($Record.Keys|Sort-Object)-join '|') -ceq 'assertion|boundary|case|completed|outcome|schema|stage|valid')
 $j=$Record|ConvertTo-Json -Depth 6 -Compress
 Ensure ([Text.Encoding]::UTF8.GetByteCount($j) -le 4096 -and -not $j.Contains('PRIVATE_SENTINEL'))
}
try {
 Check 'null-state-redacted' {$g=Get-ControlDiagnostic $null 'invocation';Shape $g;Ensure (-not $g.valid -and $null -eq $g.stage)}
 Check 'wrong-state-type-redacted' {$g=Get-ControlDiagnostic 'PRIVATE_SENTINEL' 'invocation';Shape $g;Ensure (-not $g.valid)}
 Check 'strict-null-roundtrip' {$g=Get-ControlDiagnostic (New-State) 'not-invoked';$q=($g|ConvertTo-Json -Depth 6)|ConvertFrom-Json;Ensure ($q.valid -and $null -eq $q.case -and $null -eq $q.assertion)}
 Check 'valid-empty-prefix' {$g=Get-ControlDiagnostic (New-State) 'invocation';Shape $g;Ensure ($g.valid -and $g.completed.Count -eq 0)}
 Check 'valid-fourteen-prefix' {$s=New-State;$s.completed=@($expected);$g=Get-ControlDiagnostic $s 'validated';Shape $g;Ensure ($g.valid -and $g.completed.Count -eq 14)}
 Check 'unknown-case-redacted' {$s=New-State;$s.case='PRIVATE_SENTINEL';$g=Get-ControlDiagnostic $s 'invocation';Shape $g;Ensure (-not $g.valid -and $null -eq $g.case)}
 Check 'unknown-assertion-redacted' {$s=New-State;$s.assertion='PRIVATE_SENTINEL';$g=Get-ControlDiagnostic $s 'invocation';Shape $g;Ensure (-not $g.valid -and $null -eq $g.assertion)}
 Check 'unknown-stage-redacted' {$s=New-State;$s.stage='PRIVATE_SENTINEL';$g=Get-ControlDiagnostic $s 'invocation';Shape $g;Ensure (-not $g.valid -and $null -eq $g.stage)}
 Check 'unknown-outcome-redacted' {$s=New-State;$s.outcome='PRIVATE_SENTINEL';$g=Get-ControlDiagnostic $s 'invocation';Shape $g;Ensure (-not $g.valid -and $null -eq $g.outcome)}
 Check 'unknown-boundary-redacted' {$g=Get-ControlDiagnostic (New-State) 'PRIVATE_SENTINEL';Shape $g;Ensure (-not $g.valid -and $null -eq $g.boundary)}
 Check 'null-completed-invalid' {$s=New-State;$s.completed=$null;$g=Get-ControlDiagnostic $s 'invocation';Shape $g;Ensure (-not $g.valid)}
 Check 'scalar-completed-invalid' {$s=New-State;$s.completed=$expected[0];$g=Get-ControlDiagnostic $s 'invocation';Shape $g;Ensure (-not $g.valid)}
 Check 'nonprefix-invalid-bounded-prefix' {$s=New-State;$s.completed=@($expected[0],$expected[2]);$g=Get-ControlDiagnostic $s 'invocation';Shape $g;Ensure (-not $g.valid -and $g.completed.Count -eq 1)}
 Check 'duplicate-invalid' {$s=New-State;$s.completed=@($expected[0],$expected[0]);$g=Get-ControlDiagnostic $s 'invocation';Shape $g;Ensure (-not $g.valid -and $g.completed.Count -eq 1)}
 Check 'overflow-invalid' {$s=New-State;$s.completed=@($expected)+@($expected[0]);$g=Get-ControlDiagnostic $s 'invocation';Shape $g;Ensure (-not $g.valid -and $g.completed.Count -eq 0)}
 Check 'nonstring-member-invalid' {$s=New-State;$s.completed=@(17);$g=Get-ControlDiagnostic $s 'invocation';Shape $g;Ensure (-not $g.valid)}
 Check 'reference-update-visible' {$DiagnosticState=New-State;$alias=$DiagnosticState;Set-ControlDiagnostic -Stage 'assertion' -Case $expected[0] -Assertion 'A01';Ensure ([object]::ReferenceEquals($alias,$DiagnosticState) -and $alias.case -ceq $expected[0] -and $alias.assertion -ceq 'A01')}
 Check 'metadata-no-success-output' {$DiagnosticState=New-State;$output=@(Set-ControlDiagnostic -Stage 'case-complete' -Completed $expected[0]);Ensure ($output.Count -eq 0 -and $DiagnosticState.completed.Count -eq 1)}
 Check 'assert-true-no-output' {$DiagnosticState=New-State;$output=@(Assert -DiagnosticId 'A01' $true);Ensure ($output.Count -eq 0 -and $DiagnosticState.assertion -ceq 'A01')}
 Check 'assert-false-original-throw' {$DiagnosticState=New-State;$caught=$null;try {Assert -DiagnosticId 'A01' $false}catch{$caught=$_};Ensure ($null -ne $caught -and $caught.Exception.Message -ceq 'control_assertion' -and $DiagnosticState.assertion -ceq 'A01')}
 Check 'expression-throw-precedes-marker' {$DiagnosticState=New-State;$DiagnosticState.assertion='A02';$caught=$null;try {Assert -DiagnosticId 'A01' $(throw 'expression_sentinel')}catch{$caught=$_};Ensure ($null -ne $caught -and $caught.Exception.Message -ceq 'expression_sentinel' -and $DiagnosticState.assertion -ceq 'A02')}
 Check 'bookkeeping-fault-proven' {$DiagnosticState=[object]::new();$fault=$false;try {$DiagnosticState.stage='assertion'}catch{$fault=$true};Ensure $fault;$output=@(Set-ControlDiagnostic -Stage 'assertion');Ensure ($output.Count -eq 0)}
 Check 'bookkeeping-fault-keeps-assert-throw' {$DiagnosticState=[object]::new();$caught=$null;try {Assert -DiagnosticId 'A01' $false}catch{$caught=$_};Ensure ($null -ne $caught -and $caught.Exception.Message -ceq 'control_assertion')}
 Check 'boundary-values-distinct' {foreach($b in @('invocation','output-shape','output-json','expected-json','count','identity','validated')){$g=Get-ControlDiagnostic (New-State) $b;Shape $g;Ensure ($g.valid -and $g.boundary -ceq $b)}}
 Check 'original-versus-marked-suite-transparency' {
  $originalOutput=[Collections.Generic.List[object]]::new();$markedOutput=[Collections.Generic.List[object]]::new();$oldFailure=$null;$newFailure=$null;$state=New-State
  try {& (Join-Path $PSScriptRoot 'original-suite.ps1') | ForEach-Object {$originalOutput.Add($_)}}catch{$oldFailure=$_}
  try {& (Join-Path $PSScriptRoot 'marked-suite.ps1') -DiagnosticState $state | ForEach-Object {$markedOutput.Add($_)}}catch{$newFailure=$_}
  Ensure (($null -eq $oldFailure) -eq ($null -eq $newFailure))
  if($null -ne $oldFailure){Ensure ($oldFailure.Exception.GetType().FullName -ceq $newFailure.Exception.GetType().FullName -and $oldFailure.Exception.Message -ceq $newFailure.Exception.Message -and $oldFailure.Exception.HResult -eq $newFailure.Exception.HResult -and [string]$oldFailure.CategoryInfo.Category -ceq [string]$newFailure.CategoryInfo.Category)}
  Ensure (($originalOutput|ConvertTo-Json -Depth 6 -Compress) -ceq ($markedOutput|ConvertTo-Json -Depth 6 -Compress))
  Ensure ($state.outcome -cin @('threw','returned'))
  $g=Get-ControlDiagnostic $state 'invocation';Shape $g
  # Matching failures prove transparency only; they do not pass the underlying suite.
  try {$Progress.suiteOriginal=$(if($null -eq $oldFailure){'returned'}else{'threw'});$Progress.suiteMarked=$(if($null -eq $newFailure){'returned'}else{'threw'});$Progress.suiteDiagnostic=$g}catch{}
 }
 try {$Progress.stage='result-json'}catch{}
 $results|ConvertTo-Json -Depth 6
 try {$Progress.stage='complete'}catch{}
} catch {try {$Progress.failed=$true}catch{};throw}

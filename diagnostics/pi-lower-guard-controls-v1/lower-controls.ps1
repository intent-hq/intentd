param([hashtable]$Progress=@{})
$ErrorActionPreference='Stop'
$results=@()
. (Join-Path $PSScriptRoot 'lower-evidence-functions.ps1')
. (Join-Path $PSScriptRoot 'projection-functions.ps1')
. (Join-Path $PSScriptRoot 'diagnostic-functions.ps1')
function Ensure($v){if(-not $v){throw 'lower_metadata_assertion'}}
function Counts {return @{manifestReads=0;fileLists=0;directoryLists=0;hashCalls=0}}
function Fixture {return @{json='{}';files=@();observed='fixed'}}
function SyntheticError([int]$Code=-2146233087){return [Management.Automation.ErrorRecord]::new([Runtime.InteropServices.COMException]::new('PRIVATE_SENTINEL',$Code),'PRIVATE_SENTINEL',[Management.Automation.ErrorCategory]::InvalidData,$null)}
function Shape($g){
 Ensure ((@($g.Keys|Sort-Object)-join '|') -ceq 'category|command|directoryLists|exception|fileLists|fixtureShape|hashCalls|hresult|invocationScript|manifestReads|parameter|schema|valid')
 $j=$g|ConvertTo-Json -Depth 5 -Compress
 Ensure ([Text.Encoding]::UTF8.GetByteCount($j) -le 2048 -and -not $j.Contains('PRIVATE_SENTINEL'))
}
function BindingError {try {$null=Join-Path -Path '' -ChildPath 'fixed';throw 'expected_binding_error_absent'}catch{return $_}}
function Check([string]$Name,[scriptblock]$Body){
 try {$Progress.current=$Name;$Progress.stage='case'}catch{}
 & $Body
 $script:results+=@{name=$Name;outcome='PASS'}
 try {$Progress.completed=@($Progress.completed)+@($Name)}catch{}
}
try {
 Check 'known-binding-category-command-parameter' {
  $e=BindingError;Ensure ($e.Exception -is [Management.Automation.ParameterBindingException])
  $g=Get-LowerGuardEvidence $e (Counts) (Fixture);Shape $g
  Ensure ($g.valid -and $g.command -ceq 'Join-Path' -and $g.parameter -ceq 'Path' -and $null -ne $g.category -and $g.hresult -is [int])
 }
 Check 'unknown-projection-fields-redacted' {
  $g=Get-LowerGuardEvidence (SyntheticError) (Counts) (Fixture);$g.category='PRIVATE_SENTINEL';$g.exception='PRIVATE_SENTINEL';$g.command='PRIVATE_SENTINEL';$g.parameter='PRIVATE_SENTINEL';$g.invocationScript='PRIVATE_SENTINEL'
  $q=Project-LowerGuardEvidence $g;Shape $q;Ensure (-not $q.valid -and $null -eq $q.category -and $null -eq $q.command -and $null -eq $q.parameter -and $null -eq $q.exception -and $null -eq $q.invocationScript)
 }
 Check 'null-error-invalid' {$g=Get-LowerGuardEvidence $null (Counts) (Fixture);Shape $g;Ensure (-not $g.valid -and $null -eq $g.hresult)}
 Check 'non-error-record-invalid' {$g=Get-LowerGuardEvidence 'PRIVATE_SENTINEL' (Counts) (Fixture);Shape $g;Ensure (-not $g.valid)}
 Check 'int32-hresult-boundaries' {foreach($code in @([int]::MinValue,[int]::MaxValue)){$g=Get-LowerGuardEvidence (SyntheticError $code) (Counts) (Fixture);$q=Project-LowerGuardEvidence $g;Shape $q;Ensure ($q.valid -and $q.hresult -eq $code)}}
 Check 'binding-failure-zero-provider-counts' {$g=Get-LowerGuardEvidence (BindingError) (Counts) (Fixture);Shape $g;Ensure ($g.valid -and $g.manifestReads -eq 0 -and $g.fileLists -eq 0 -and $g.directoryLists -eq 0 -and $g.hashCalls -eq 0)}
 Check 'counter-null-type-overflow-invalid' {
  foreach($bad in @($null,'PRIVATE_SENTINEL',-1,9)){$c=Counts;$c.hashCalls=$bad;$g=Get-LowerGuardEvidence (SyntheticError) $c (Fixture);Shape $g;Ensure (-not $g.valid);$q=Project-LowerGuardEvidence $g;Shape $q;Ensure (-not $q.valid)}
 }
 Check 'bookkeeping-fault-preserves-caught-behavior' {
  $DiagnosticState=[object]::new();$script:metaProviderCounters=Counts;$script:fixture=Fixture;$caught=SyntheticError;$beforeMessage=$caught.Exception.Message;$beforeCode=$caught.Exception.HResult
  $fault=$false;try {$DiagnosticState.originalLower=@{}}catch{$fault=$true};Ensure $fault
  $output=@(Save-LowerGuardFailure 'original' $caught);Ensure ($output.Count -eq 0 -and $caught.Exception.Message -ceq $beforeMessage -and $caught.Exception.HResult -eq $beforeCode)
 }
 Check 'caught-original-state-and-output-preserved' {
  $DiagnosticState=@{};$script:metaProviderCounters=Counts;$script:fixture=Fixture;$e=SyntheticError
  $oldOk=$true;$oldError=$null;try {throw $e}catch{$oldOk=$false;$oldError=$_.Exception.Message;$output=@(Save-LowerGuardFailure 'original' $_)}
  Ensure (-not $oldOk -and $oldError -ceq 'PRIVATE_SENTINEL' -and $output.Count -eq 0)
  Shape $DiagnosticState.originalLower;Ensure ($DiagnosticState.originalLower.valid -and $DiagnosticState.originalLower.hresult -eq $e.Exception.HResult)
 }
 Check 'lower-null-fields-json-roundtrip' {$g=Get-LowerGuardEvidence (SyntheticError) (Counts) (Fixture);$q=($g|ConvertTo-Json -Depth 5)|ConvertFrom-Json;Ensure ($null -eq $q.command -and $null -eq $q.parameter -and $q.hresult -eq $g.hresult)}
 Check 'extra-fields-never-exported' {$g=Get-LowerGuardEvidence (SyntheticError) (Counts) (Fixture);$g.secret='PRIVATE_SENTINEL';$q=Project-LowerGuardEvidence $g;Shape $q;Ensure (-not $q.Contains('secret'))}
 Check 'original-twentyone-oracles-and-throws-preserved' {
  $original=Get-Content -Raw (Join-Path $PSScriptRoot 'original-suite.ps1');$marked=Get-Content -Raw (Join-Path $PSScriptRoot 'marked-suite.ps1')
  $restored=[regex]::Replace($marked,"Assert -DiagnosticId 'A[0-9]{2}' \(",'Assert (')
  $a=@($original -split "`n"|Where-Object {$_ -match 'Assert \('}|ForEach-Object {$_.Trim()});$b=@($restored -split "`n"|Where-Object {$_ -match 'Assert \('}|ForEach-Object {$_.Trim()})
  Ensure ($a.Count -gt 0 -and ($a-join "`n") -ceq ($b-join "`n"))
  $a=@([regex]::Matches($original,"throw '[^']+'")|ForEach-Object {$_.Value});$b=@([regex]::Matches($marked,"throw '[^']+'")|ForEach-Object {$_.Value});Ensure (($a-join '|') -ceq ($b-join '|'))
 }
 # Three transition controls reproduce the exact sourceclosed two-line boundaries and helper.
 $source=Get-Content -Raw (Join-Path $PSScriptRoot 'marked-suite.ps1');$tokens=$null;$errors=$null
 $ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $PSScriptRoot 'marked-suite.ps1'),[ref]$tokens,[ref]$errors);Ensure ($errors.Count -eq 0)
 $nodes=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst]},$true)|Where-Object {$_.Name -ceq 'Set-ControlDiagnostic'});Ensure ($nodes.Count -eq 1);Invoke-Expression $nodes[0].Extent.Text
 foreach($case in @('unknown-fields-redacted','redacted-null-json-roundtrip','valid-digest-json-roundtrip')){
  Check ('reset-before-'+$case) {
   $reset='try {$DiagnosticState.originalLower=$null;$DiagnosticState.markedLower=$null} catch {}';$anchor="Set-ControlDiagnostic -Stage 'case-setup' -Case '$case'";$snippet=$reset+"`n"+$anchor
   Ensure (([regex]::Matches($source,[regex]::Escape($snippet))).Count -eq 1)
   $DiagnosticState=@{stage='case-complete';case='extra-truncated';assertion='A18';completed=@();outcome='entered';originalLower=@{hresult=1};markedLower=@{hresult=2}};$alias=$DiagnosticState
   Ensure ($null -ne $alias.originalLower -and $null -ne $alias.markedLower)
   $output=@(. ([scriptblock]::Create($snippet)))
   Ensure ($output.Count -eq 0 -and [object]::ReferenceEquals($alias,$DiagnosticState) -and $DiagnosticState.case -ceq $case -and $DiagnosticState.stage -ceq 'case-setup' -and $null -eq $DiagnosticState.assertion)
   Ensure ($null -eq $DiagnosticState.originalLower -and $null -eq $DiagnosticState.markedLower)
   $q=($DiagnosticState|ConvertTo-Json -Depth 6)|ConvertFrom-Json;Ensure ($null -eq $q.originalLower -and $null -eq $q.markedLower)
  }
 }
 Check 'affected-original-marked-pair-transparency' {
  $oldOutput=[Collections.Generic.List[object]]::new();$newOutput=[Collections.Generic.List[object]]::new();$oldFailure=$null;$newFailure=$null;$state=@{stage='not-invoked';case=$null;assertion=$null;outcome='not-entered';completed=@()}
  try {& (Join-Path $PSScriptRoot 'original-suite.ps1')|ForEach-Object {$oldOutput.Add($_)}}catch{$oldFailure=$_}
  try {& (Join-Path $PSScriptRoot 'marked-suite.ps1') -DiagnosticState $state|ForEach-Object {$newOutput.Add($_)}}catch{$newFailure=$_}
  # Preserve bounded lower evidence even if a later transparency oracle fails.
  try {$Progress.suiteOriginal=$(if($null -eq $oldFailure){'returned'}else{'threw'});$Progress.suiteMarked=$(if($null -eq $newFailure){'returned'}else{'threw'});$Progress.suiteDiagnostic=Get-ControlDiagnostic $state 'invocation';$Progress.originalLower=Project-LowerGuardEvidence $state.originalLower;$Progress.markedLower=Project-LowerGuardEvidence $state.markedLower}catch{}
  Ensure (($null -eq $oldFailure) -eq ($null -eq $newFailure))
  if($null -ne $oldFailure){Ensure ($oldFailure.Exception.GetType().FullName -ceq $newFailure.Exception.GetType().FullName -and $oldFailure.Exception.Message -ceq $newFailure.Exception.Message -and $oldFailure.Exception.HResult -eq $newFailure.Exception.HResult -and [string]$oldFailure.CategoryInfo.Category -ceq [string]$newFailure.CategoryInfo.Category)}
  Ensure (($oldOutput|ConvertTo-Json -Depth 6 -Compress) -ceq ($newOutput|ConvertTo-Json -Depth 6 -Compress))
 }
 try {$Progress.stage='result-json'}catch{}
 $results|ConvertTo-Json -Depth 6
 try {$Progress.stage='complete'}catch{}
} catch {try {$Progress.failed=$true}catch{};throw}

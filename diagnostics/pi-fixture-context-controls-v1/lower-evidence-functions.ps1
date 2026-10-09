function Get-LowerGuardEvidence($Failure,$Counters,$Fixture) {
 $record=[ordered]@{schema='lower-guard-v1';valid=$false;category=$null;hresult=$null;exception=$null;command=$null;parameter=$null;invocationScript=$null;manifestReads=$null;fileLists=$null;directoryLists=$null;hashCalls=$null;fixtureShape=$false}
 try {
  if($Failure -isnot [System.Management.Automation.ErrorRecord]){return $record}
  $cat=[string]$Failure.CategoryInfo.Category
  if($cat -cin @('NotSpecified','InvalidOperation','InvalidData','ObjectNotFound','ResourceUnavailable','PermissionDenied','WriteError','ReadError','OperationStopped','ParserError','SyntaxError','InvalidArgument')){$record.category=$cat}
  $record.hresult=[int]$Failure.Exception.HResult
  $type=$Failure.Exception.GetType().Name
  if($type -cin @('RuntimeException','ParameterBindingException','ParameterBindingValidationException','CommandNotFoundException','ItemNotFoundException','PSArgumentException','ArgumentException','InvalidOperationException')){$record.exception=$type}else{$record.exception='Other'}
  $cmd=$Failure.InvocationInfo.MyCommand.Name
  if($cmd -is [string] -and $cmd -cin @('Join-Path','Get-Content','Get-ChildItem','ConvertFrom-Json','Sort-Object','ForEach-Object','OriginalGuard','Guard','Hash')){$record.command=$cmd}
  if($Failure.Exception -is [System.Management.Automation.ParameterBindingException]){
   $param=$Failure.Exception.ParameterName
   if($param -cin @('Path','LiteralPath','ChildPath','InputObject','Raw','File','Directory')){$record.parameter=$param}
  }
  $record.invocationScript=$(if([string]::IsNullOrEmpty($Failure.InvocationInfo.ScriptName)){'empty'}else{'present'})
  if($Counters -isnot [hashtable]){return $record}
  foreach($key in @('manifestReads','fileLists','directoryLists','hashCalls')){
   $n=$Counters[$key]
   if($n -isnot [int] -or $n -lt 0 -or $n -gt 8){return $record}
   $record[$key]=$n
  }
  $record.fixtureShape=$Fixture -is [hashtable] -and $Fixture.json -is [string] -and $Fixture.files -is [array] -and $Fixture.observed -is [string]
  $record.valid=$null -ne $record.category
 } catch {$record.valid=$false}
 return $record
}
function Save-LowerGuardFailure([string]$Side,$Failure) {
 try {
  if($Side -cnotin @('original','marked')){return}
  $DiagnosticState[$Side+'Lower']=Get-LowerGuardEvidence $Failure $script:metaProviderCounters $script:fixture
 } catch {}
}
function Note-GuardProvider([string]$Name) {
 try {
  if($Name -cin @('manifestReads','fileLists','directoryLists','hashCalls')){$script:metaProviderCounters[$Name]++}
 } catch {}
}

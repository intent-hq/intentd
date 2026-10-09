function Project-LowerGuardEvidence($InputRecord) {
 $r=[ordered]@{schema='lower-guard-v1';valid=$false;category=$null;hresult=$null;exception=$null;command=$null;parameter=$null;invocationScript=$null;manifestReads=$null;fileLists=$null;directoryLists=$null;hashCalls=$null;fixtureShape=$false}
 try {
  if($InputRecord -isnot [System.Collections.IDictionary]){return $r}
  $enums=@{
   category=@('NotSpecified','InvalidOperation','InvalidData','ObjectNotFound','ResourceUnavailable','PermissionDenied','WriteError','ReadError','OperationStopped','ParserError','SyntaxError','InvalidArgument');
   exception=@('RuntimeException','ParameterBindingException','ParameterBindingValidationException','CommandNotFoundException','ItemNotFoundException','PSArgumentException','ArgumentException','InvalidOperationException','Other');
   command=@('Join-Path','Get-Content','Get-ChildItem','ConvertFrom-Json','Sort-Object','ForEach-Object','OriginalGuard','Guard','Hash');
   parameter=@('Path','LiteralPath','ChildPath','InputObject','Raw','File','Directory');invocationScript=@('empty','present')
  }
  foreach($key in $enums.Keys){if($InputRecord[$key] -is [string] -and $InputRecord[$key] -cin $enums[$key]){$r[$key]=$InputRecord[$key]}}
  if($InputRecord.hresult -isnot [int]){return $r};$r.hresult=$InputRecord.hresult
  foreach($key in @('manifestReads','fileLists','directoryLists','hashCalls')){
   $n=$InputRecord[$key];if($n -isnot [int] -or $n -lt 0 -or $n -gt 8){return $r};$r[$key]=$n
  }
  if($InputRecord.fixtureShape -isnot [bool]){return $r};$r.fixtureShape=$InputRecord.fixtureShape
  $r.valid=$InputRecord.valid -is [bool] -and $InputRecord.valid -and $null -ne $r.category -and $null -ne $r.exception -and $null -ne $r.invocationScript
 } catch {$r.valid=$false}
 return $r
}

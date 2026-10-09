# Bounded evidence only; no provider query, process, file output or raw exception serialization.
function Set-ToolProbe($Context,[string]$Stage,[string]$Predicate='none',[string]$Tree='none') {
 try { if($null -ne $Context){$Context.value=@{stage=$Stage;predicate=$Predicate;tree=$Tree}} } catch { }
}
function Write-ToolProbeFailure($Context,[string]$Category,[int]$HResult) {
 try {
  $stages=@('npm-entry','npm-root','npm-package','npm-identity','host-path','host-identity','image-read','image-shape','record-construction','tree-enumerate','tree-entry','tree-file','tree-hash')
  $predicates=@('none','npm_entry_missing','npm_package_identity','powershell_host_identity','hosted_image_metadata','tool_reparse_point','tool_inventory_cap')
  $categories=@('NotSpecified','OpenError','CloseError','DeviceError','DeadlockDetected','InvalidArgument','InvalidData','InvalidOperation','InvalidResult','InvalidType','MetadataError','NotImplemented','NotInstalled','ObjectNotFound','OperationStopped','OperationTimeout','SyntaxError','ParserError','PermissionDenied','ResourceBusy','ResourceExists','ResourceUnavailable','ReadError','WriteError','FromStdErr','SecurityError','ProtocolError','ConnectionError','AuthenticationError','LimitsExceeded','QuotaExceeded','NotEnabled')
  $stage='unknown';$predicate='unknown';$tree='unknown';$valid=$false
  if($Context.value.stage -in $stages -and $Context.value.predicate -in $predicates -and $Context.value.tree -in @('none','npm','powershell','unknown')){
   $stage=$Context.value.stage;$predicate=$Context.value.predicate;$tree=$Context.value.tree;$valid=$true
  }
  if($Category -notin $categories){$Category='Other'}
  $record=[ordered]@{schema='tool-probe-failure-v1';stage=$stage;predicate=$predicate;tree=$tree;markerValid=$valid;category=$Category;hresult=$HResult}
  $line='TOOL_PROBE_FAILURE_V1 '+($record | ConvertTo-Json -Compress)
  if($line.Length -le 1024){[Console]::Error.WriteLine($line)}
 } catch { }
}

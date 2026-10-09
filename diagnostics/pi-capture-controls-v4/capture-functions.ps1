# BEGIN FIXED SETUP FAILURE CAPTURE
function New-SetupFailureRecord([string]$Phase,[string]$Stage,[string]$Category,[int]$HResult,[string]$Ownership) {
 $stages=@('provenance','payload-verify','packet-inventory','tool-provenance','native-compile','compiler-provenance','latch-controls','node-version','npm-version','native-controls','dependency-install','dependency-acceptance','final-packet-guards','setup-complete')
 $categories=@('NotSpecified','OpenError','CloseError','DeviceError','DeadlockDetected','InvalidArgument','InvalidData','InvalidOperation','InvalidResult','InvalidType','MetadataError','NotImplemented','NotInstalled','ObjectNotFound','OperationStopped','OperationTimeout','SyntaxError','ParserError','PermissionDenied','ResourceBusy','ResourceExists','ResourceUnavailable','ReadError','WriteError','FromStdErr','SecurityError','ProtocolError','ConnectionError','AuthenticationError','LimitsExceeded','QuotaExceeded','NotEnabled')
 if($Phase -notin @('original','finalizer')){$Phase='finalizer'}
 if($Stage -notin $stages){$Stage='unknown'}
 if($Category -notin $categories){$Category='Other'}
 if($Ownership -notin @('not-initialized','safe','unresolved')){$Ownership='unresolved'}
 return [ordered]@{schema='setup-failure-v1';phase=$Phase;stage=$Stage;category=$Category;hresult=$HResult;ownership=$Ownership;outcome='FAILED';exitCode=1;behavioralInvocations=0}
}
function Write-SetupFailureRecord($Record) {
 # Only a newly constructed fixed schema can be written. Never serialize an ErrorRecord.
 try {
  $safe=New-SetupFailureRecord $Record.phase $Record.stage $Record.category ([int]$Record.hresult) $Record.ownership
  $line='SETUP_FAILURE_V1 '+($safe | ConvertTo-Json -Compress)
  if($line.Length -le 1024){[Console]::Error.WriteLine($line)}
 } catch { } # Diagnostic write failure cannot replace the original failure or launch a child.
}
function Get-SetupOwnershipState {
 try {
  if(-not ('OwnedSetup' -as [type])){if(-not $script:ownedInvocationStarted){return 'not-initialized'};return 'unresolved'}
  if([OwnedSetup]::OwnershipSafe){return 'safe'}
 } catch { }
 return 'unresolved'
}
# END FIXED SETUP FAILURE CAPTURE

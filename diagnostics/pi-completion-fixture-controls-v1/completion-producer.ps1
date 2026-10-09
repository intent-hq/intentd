function Set-ControlDiagnostic([string]$Stage,[string]$Case='', [string]$Assertion='', [string]$Completed='', [string]$Outcome='') {
 # Diagnostic bookkeeping cannot replace an original assertion/throw.
 try {
  if($Stage){$DiagnosticState.stage=$Stage;$DiagnosticState.assertion=$null}
  if($Case){$DiagnosticState.case=$Case}
  if($Assertion){$DiagnosticState.assertion=$Assertion}
  if($Completed){$DiagnosticState.completed=@($DiagnosticState.completed)+@($Completed)}
  if($Outcome){$DiagnosticState.outcome=$Outcome}
 } catch {}
}

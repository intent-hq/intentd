param([Parameter(Mandatory)][string]$Output)
$ErrorActionPreference='Stop'
# Pure injected state controls exercise the exact latch class used by every Run,
# with zero child creation and no changes to the global OwnedSetup gate.
$rows=@()
foreach($name in @('safe-no-child','safe-unassigned-stopped','safe-disposed','unresolved-active','unresolved-root','unresolved-reader','reader-error','exception-before-receipt')){
 $latch=[OwnedSetup+OwnershipLatch]::new();$latch.Begin();$r=[OwnedSetup+Receipt]::new()
 $created=$true;$reader=$true;$expected=$false
 $r.assigned=$true;$r.resumed=$true;$r.rootSignalled=$true;$r.activeZero=$true;$r.readerDone=$true
 switch($name){
  'safe-no-child' {$created=$false;$reader=$false;$r.assigned=$false;$r.resumed=$false;$r.rootSignalled=$false;$r.readerDone=$false;$expected=$true}
  'safe-unassigned-stopped' {$r.assigned=$false;$r.resumed=$false;$reader=$false;$expected=$true}
  'safe-disposed' {$expected=$true}
  'unresolved-active' {$r.activeZero=$false}
  'unresolved-root' {$r.rootSignalled=$false}
  'unresolved-reader' {$r.readerDone=$false}
  'reader-error' {$r.readerError=$true}
  'exception-before-receipt' {$latch.Fault()}
 }
 if($name -ne 'exception-before-receipt'){$latch.Complete($r,$created,$reader)}
 $entered=$false;try{$latch.Begin();$entered=$true}catch{}
 if($entered -ne $expected){throw 'latch_control_failed'}
 $rows+=@{name=$name;nextLaunchPermitted=$entered;expected=$expected;outcome='PASS';actualChildLaunches=0}
}
$rows | ConvertTo-Json -Depth 6 | Set-Content -Encoding utf8NoBOM -LiteralPath $Output

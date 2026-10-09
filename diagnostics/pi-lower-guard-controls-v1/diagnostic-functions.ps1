function Get-ControlDiagnostic($State,[string]$Boundary) {
 $names=@('equal-pass','missing-known','extra-unknown','case-mismatch','directory-present','digest-mismatch','file-read-error','manifest-read-error','inventory-read-error','invalid-name','extra-truncated','unknown-fields-redacted','redacted-null-json-roundtrip','valid-digest-json-roundtrip')
 $stages=@('not-invoked','suite-initialization','case-setup','fixture-json','original-call','marked-call','assertion','evidence-json','case-complete','result-json','complete')
 $assertions=@('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15','A16','A17','A18','A19','A20','A21')
 $record=[ordered]@{schema='guard-control-progress-v1';valid=$false;boundary=$null;stage=$null;case=$null;assertion=$null;outcome=$null;completed=@()}
 try {
  if($Boundary -cin @('not-invoked','invocation','output-shape','output-json','expected-json','count','identity','validated')){$record.boundary=$Boundary}
  if($State -isnot [hashtable]){return $record}
  if($State.stage -is [string] -and $State.stage -cin $stages){$record.stage=$State.stage}
  if($State.case -is [string] -and $State.case -cin $names){$record.case=$State.case}
  if($State.assertion -is [string] -and $State.assertion -cin $assertions){$record.assertion=$State.assertion}
  if($State.outcome -is [string] -and $State.outcome -cin @('not-entered','entered','threw','returned')){$record.outcome=$State.outcome}
  $items=$State.completed
  if($items -isnot [array] -or $items.Count -gt 14){return $record}
  for($i=0;$i -lt $items.Count;$i++){
   if($items[$i] -isnot [string] -or $items[$i] -cne $names[$i]){return $record}
   $record.completed+=@($names[$i])
  }
  $record.valid=$null -ne $record.boundary -and $null -ne $record.stage -and $null -ne $record.outcome
  if($null -ne $State.case -and $null -eq $record.case){$record.valid=$false}
  if($null -ne $State.assertion -and $null -eq $record.assertion){$record.valid=$false}
 } catch {$record.valid=$false}
 return $record
}

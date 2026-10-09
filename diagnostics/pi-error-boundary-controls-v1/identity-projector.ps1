function Project-IdentityDetail($State,$Binding) {
 $incomplete=[ordered]@{classification='incomplete';detail=$null}
 try {
  $cases=@('original-success-no-probe','original-other-refusal-no-probe','missing-entry-same-parent','missing-entry-other-parent','selection-shape-and-command-errors','path-and-existence-errors','installed-writer-fault-and-redaction','capture-composition-and-scope')
  $modes=@('missing','success','original-provider-error','package-error','package-refusal','sibling-absent','other-present','other-absent','no-command','multiple-command','source-not-string','source-relative','source-drive-relative','source-root-relative','source-wrong-leaf','source-oversize','command-error','sibling-error')
  if($null -eq $State -or $null -eq $Binding -or $Binding.active -isnot [bool] -or -not $Binding.active -or $State.assertion -isnot [int] -or $State.assertion -ne 16){return $incomplete}
  if($Binding.case -cnotin $cases -or $Binding.mode -cnotin $modes -or $Binding.side -cnotin @('original','marked') -or $State.case -cne $Binding.case){return $incomplete}
  $d=$State.identityDetail
  if($d -isnot [Collections.IDictionary]){return $incomplete}
  $keys=@('schema','valid','case','mode','side','observerCalled','outwardRecord','observedRecord','outwardException','observedException','sameRecord','sameException')
  if((@($d.Keys|Sort-Object)-join '|') -cne (@($keys|Sort-Object)-join '|')){return $incomplete}
  if($d.schema -cne 'identity-comparison-v1' -or $d.valid -isnot [bool] -or -not $d.valid -or $d.case -cne $Binding.case -or $d.mode -cne $Binding.mode -or $d.side -cne $Binding.side){return $incomplete}
  foreach($k in @('observerCalled','outwardRecord','observedRecord','outwardException','observedException')){if($d[$k] -isnot [bool]){return $incomplete}}
  foreach($k in @('sameRecord','sameException')){if($null -ne $d[$k] -and $d[$k] -isnot [bool]){return $incomplete}}
  if(($d.outwardException -and -not $d.outwardRecord) -or ($d.observedException -and -not $d.observedRecord)){return $incomplete}
  if($d.outwardRecord -and $d.observedRecord){if($d.sameRecord -isnot [bool]){return $incomplete}}elseif($null -ne $d.sameRecord){return $incomplete}
  if($d.outwardException -and $d.observedException){if($d.sameException -isnot [bool]){return $incomplete}}elseif($null -ne $d.sameException){return $incomplete}
  # Availability is not inferred from admissible null/false fields.
  $available=$d.observerCalled -and $d.outwardRecord -and $d.observedRecord -and $d.outwardException -and $d.observedException
  $safe=[ordered]@{};foreach($k in $keys){$safe[$k]=$d[$k]}
  if([Text.Encoding]::UTF8.GetByteCount(($safe|ConvertTo-Json -Compress)) -gt 2048){return $incomplete}
  return [ordered]@{classification=$(if($available){'complete'}else{'incomplete'});detail=$safe}
 } catch {return $incomplete}
}

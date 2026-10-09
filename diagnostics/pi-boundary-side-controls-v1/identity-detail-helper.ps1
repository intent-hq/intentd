# Metadata proposal only. Values are fixed enums/booleans/null; no error objects leave memory.
function Set-IdentityDetail($Progress,[string]$Side,$Result) {
 try {
  $cases=@('original-success-no-probe','original-other-refusal-no-probe','missing-entry-same-parent','missing-entry-other-parent','selection-shape-and-command-errors','path-and-existence-errors','installed-writer-fault-and-redaction','capture-composition-and-scope')
  $modes=@('missing','success','original-provider-error','package-error','package-refusal','sibling-absent','other-present','other-absent','no-command','multiple-command','source-not-string','source-relative','source-drive-relative','source-root-relative','source-wrong-leaf','source-oversize','command-error','sibling-error')
  $valid=$Progress.case -cin $cases -and $Side -cin @('original','marked') -and $Result.mode -cin $modes -and $Result.observerCalled -is [bool]
  $case='unknown';$mode='unknown';$safeSide='unknown'
  if($valid){$case=$Progress.case;$mode=$Result.mode;$safeSide=$Side}
  $outward=$Result.error -is [Management.Automation.ErrorRecord]
  $observed=$Result.boundary -is [Management.Automation.ErrorRecord]
  $outwardException=$outward -and $Result.error.Exception -is [Exception]
  $observedException=$observed -and $Result.boundary.Exception -is [Exception]
  $sameRecord=$null;$sameException=$null
  if($outward -and $observed){$sameRecord=[Object]::ReferenceEquals($Result.error,$Result.boundary)}
  if($outwardException -and $observedException){$sameException=[Object]::ReferenceEquals($Result.error.Exception,$Result.boundary.Exception)}
  $Progress.identityDetail=[ordered]@{schema='identity-comparison-v1';valid=[bool]$valid;case=$case;mode=$mode;side=$safeSide;observerCalled=[bool]$Result.observerCalled;outwardRecord=$outward;observedRecord=$observed;outwardException=$outwardException;observedException=$observedException;sameRecord=$sameRecord;sameException=$sameException}
 } catch { }
}

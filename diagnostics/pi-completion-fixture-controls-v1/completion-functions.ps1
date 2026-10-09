function Get-CompletionEvidence($Items) {
 $expected=@('equal-pass','missing-known','extra-unknown','case-mismatch','directory-present','digest-mismatch','file-read-error','manifest-read-error','inventory-read-error','invalid-name','extra-truncated','unknown-fields-redacted','redacted-null-json-roundtrip','valid-digest-json-roundtrip')
 $r=[ordered]@{schema='guard-completion-v1';valid=$false;reason='not-array';count=$null;nullIndices=@();firstMismatch=$null}
 if($Items -isnot [array] -or $Items.Rank -ne 1){return $r}
 if($Items.Count -gt 64){$r.reason='count-over-cap';return $r}
 $r.count=[int]$Items.Count
 for($i=0;$i -lt $Items.Count;$i++){if($null -eq $Items[$i]){$r.nullIndices+=@($i)}}
 if($Items.Count -ne 14){$r.reason='count';return $r}
 $seen=@()
 for($i=0;$i -lt 14;$i++){
  if($null -eq $Items[$i]){$r.reason='null-element';$r.firstMismatch=$i;return $r}
  if($Items[$i] -isnot [string]){$r.reason='element-type';$r.firstMismatch=$i;return $r}
  if($seen -ccontains $Items[$i]){$r.reason='duplicate';$r.firstMismatch=$i;return $r}
  if($Items[$i] -cne $expected[$i]){$r.reason='identity';$r.firstMismatch=$i;return $r}
  $seen+=@($Items[$i])
 }
 $r.valid=$true;$r.reason='complete';return $r
}

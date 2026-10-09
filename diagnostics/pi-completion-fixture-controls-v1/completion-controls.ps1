# Authored only; supported native PowerShell required, no execution selected.
param([hashtable]$CompletionProgress)
$ErrorActionPreference='Stop'
$CompletionProgress.current='legacy-missing-key-null-seed'
. (Join-Path $PSScriptRoot 'completion-functions.ps1')
. (Join-Path $PSScriptRoot 'completion-producer.ps1')
function Assert($Value){if(-not $Value){throw 'completion_control'}}
$expected=@('equal-pass','missing-known','extra-unknown','case-mismatch','directory-present','digest-mismatch','file-read-error','manifest-read-error','inventory-read-error','invalid-name','extra-truncated','unknown-fields-redacted','redacted-null-json-roundtrip','valid-digest-json-roundtrip')
$results=@()
$DiagnosticState=@{}
foreach($n in $expected){Set-ControlDiagnostic -Completed $n}
$e=Get-CompletionEvidence $DiagnosticState.completed
Assert (-not $e.valid -and $e.count -eq 15 -and $e.reason -ceq 'count' -and $e.nullIndices.Count -eq 1 -and $e.nullIndices[0] -eq 0)
$results+=@{name='legacy-missing-key-null-seed';outcome='PASS'}
$CompletionProgress.completed+=@('legacy-missing-key-null-seed')
$CompletionProgress.current='initialized-exact-producer-fourteen'
$DiagnosticState=@{completed=@()}
foreach($n in $expected){Set-ControlDiagnostic -Completed $n}
$e=Get-CompletionEvidence $DiagnosticState.completed
Assert ($e.valid -and $e.count -eq 14 -and $e.reason -ceq 'complete' -and $e.nullIndices.Count -eq 0 -and $null -eq $e.firstMismatch)
$results+=@{name='initialized-exact-producer-fourteen';outcome='PASS'}
$CompletionProgress.completed+=@('initialized-exact-producer-fourteen')
$CompletionProgress.current='null-completion-refused'
$e=Get-CompletionEvidence $null
Assert (-not $e.valid -and $e.reason -ceq 'not-array' -and $null -eq $e.count)
$results+=@{name='null-completion-refused';outcome='PASS'}
$CompletionProgress.completed+=@('null-completion-refused')
$CompletionProgress.current='scalar-joined-list-refused'
$e=Get-CompletionEvidence ($expected -join '|')
Assert (-not $e.valid -and $e.reason -ceq 'not-array')
$results+=@{name='scalar-joined-list-refused';outcome='PASS'}
$CompletionProgress.completed+=@('scalar-joined-list-refused')
$CompletionProgress.current='short-prefix-refused'
$e=Get-CompletionEvidence @($expected[0..12])
Assert (-not $e.valid -and $e.count -eq 13 -and $e.reason -ceq 'count')
$results+=@{name='short-prefix-refused';outcome='PASS'}
$CompletionProgress.completed+=@('short-prefix-refused')
$CompletionProgress.current='reordered-identities-refused'
$x=$expected.Clone();$x[0]=$expected[1];$x[1]=$expected[0];$e=Get-CompletionEvidence $x
Assert (-not $e.valid -and $e.reason -ceq 'identity' -and $e.firstMismatch -eq 0)
$results+=@{name='reordered-identities-refused';outcome='PASS'}
$CompletionProgress.completed+=@('reordered-identities-refused')
$CompletionProgress.current='duplicate-identity-refused'
$x=$expected.Clone();$x[1]=$x[0];$e=Get-CompletionEvidence $x
Assert (-not $e.valid -and $e.reason -ceq 'duplicate' -and $e.firstMismatch -eq 1)
$results+=@{name='duplicate-identity-refused';outcome='PASS'}
$CompletionProgress.completed+=@('duplicate-identity-refused')
$CompletionProgress.current='case-changed-identity-refused'
$x=$expected.Clone();$x[0]=$x[0].ToUpperInvariant();$e=Get-CompletionEvidence $x
Assert (-not $e.valid -and $e.reason -ceq 'identity')
$results+=@{name='case-changed-identity-refused';outcome='PASS'}
$CompletionProgress.completed+=@('case-changed-identity-refused')
$CompletionProgress.current='null-slot-refused'
$x=$expected.Clone();$x[4]=$null;$e=Get-CompletionEvidence $x
Assert (-not $e.valid -and $e.reason -ceq 'null-element' -and $e.firstMismatch -eq 4 -and $e.nullIndices.Count -eq 1 -and $e.nullIndices[0] -eq 4)
$results+=@{name='null-slot-refused';outcome='PASS'}
$CompletionProgress.completed+=@('null-slot-refused')
$CompletionProgress.current='nonstring-element-refused'
$x=$expected.Clone();$x[0]=42;$e=Get-CompletionEvidence $x
Assert (-not $e.valid -and $e.reason -ceq 'element-type' -and $e.firstMismatch -eq 0)
$results+=@{name='nonstring-element-refused';outcome='PASS'}
$CompletionProgress.completed+=@('nonstring-element-refused')
$CompletionProgress.current='oversized-array-bounded-null-roundtrip'
$e=Get-CompletionEvidence ([object[]]::new(65));$json=$e|ConvertTo-Json -Depth 4 -Compress;$round=$json|ConvertFrom-Json
Assert (-not $e.valid -and $e.reason -ceq 'count-over-cap' -and $null -eq $round.count -and $round.nullIndices.Count -eq 0 -and $null -eq $round.firstMismatch)
$results+=@{name='oversized-array-bounded-null-roundtrip';outcome='PASS'}
$CompletionProgress.completed+=@('oversized-array-bounded-null-roundtrip')
$CompletionProgress.current='foreign-content-never-exported'
$x=$expected.Clone();$x[0]='PRIVATE_SENTINEL';$e=Get-CompletionEvidence $x;$json=$e|ConvertTo-Json -Depth 4 -Compress
Assert (-not $e.valid -and -not $json.Contains('PRIVATE_SENTINEL') -and $json.Length -lt 2048)
$results+=@{name='foreign-content-never-exported';outcome='PASS'}
$CompletionProgress.completed+=@('foreign-content-never-exported')
$results|ConvertTo-Json -Depth 4 -Compress

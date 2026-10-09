function Write-DependencyFailureMetadata([string]$Root) {
 # Only this new fixed metadata file is read; acceptance.log remains private.
 try {
  $path=Join-Path $Root 'dependency-failure.json'
  if(-not(Test-Path -LiteralPath $path -PathType Leaf)){return}
  $item=Get-Item -LiteralPath $path
  if(($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0){return}
  $stream=$null
  try {
   $stream=[IO.File]::Open($path,[IO.FileMode]::Open,[IO.FileAccess]::Read,[IO.FileShare]::Read)
   if($stream.Length -gt 512){return}
   $buffer=[byte[]]::new(513);$count=0
   while($count -lt 513){$got=$stream.Read($buffer,$count,513-$count);if($got -eq 0){break};$count+=$got}
   if($count -eq 0 -or $count -gt 512){return}
   $text=[Text.UTF8Encoding]::new($false,$true).GetString($buffer,0,$count)
  } finally {if($null -ne $stream){$stream.Dispose()}}
  $record=ConvertFrom-Json -InputObject $text -AsHashtable
  if($record -isnot [System.Collections.IDictionary] -or $record.Count -ne 6){return}
  foreach($key in @('schema','stage','group','code','outcome','behavioralInvocations')){if(-not($record.Keys -ccontains $key)){return}}
  foreach($key in @('schema','stage','group','code','outcome')){if($record[$key] -isnot [string]){return}}
  if($record.schema -cne 'dependency-failure-v1' -or $record.outcome -cne 'FAILED'){return}
  if($record.behavioralInvocations -isnot [int] -and $record.behavioralInvocations -isnot [long]){return}
  if($record.behavioralInvocations -ne 0){return}
  if(@('argument-resolution','platform','node-version','expected-lock-read','installed-lock-read','package-keys','package-path','package-metadata','integrity-attribution','tar-cache-key','tar-url-policy','tar-fetch','tar-response','tar-stream','tar-integrity','tar-inflate','tar-parse','installed-tar-bytes','installed-inventory','generated-files','mirror-absence','mirror-copy','mirror-inventory','entry-pin','resolution-preinventory','resolution-helper','resolution-child','resolution-output','resolution-identities','resolution-targets','resolution-postinventory','mirror-unmodified','result-write','unknown','tar-a01-checksum','tar-a02-octal-size','tar-a03-size-extent','tar-a04-package-path','tar-a05-windows-name','tar-a06-file-unique','tar-a07-file-count','tar-a08-entry-type','tar-a09-nonempty','tar-a04-prefix-eval','tar-a04-prefix-false','tar-a04-prefix-true','tar-a04-prefix-other','tar-a04-backslash-eval','tar-a04-backslash-false','tar-a04-backslash-true','tar-a04-backslash-other','tar-a04-traversal-eval','tar-a04-traversal-false','tar-a04-traversal-true','tar-a04-traversal-other','tar-a04-colon-eval','tar-a04-colon-false','tar-a04-colon-true','tar-a04-colon-other') -cnotcontains $record.stage -or @('none','adapter','pi','unknown') -cnotcontains $record.group -or @('ERR_ASSERTION','ENOENT','ENOTDIR','EACCES','EPERM','EEXIST','ERR_INVALID_ARG_TYPE','ERR_INVALID_ARG_VALUE','ERR_OUT_OF_RANGE','ABORT_ERR','ETIMEDOUT','ERR_BUFFER_TOO_LARGE','Z_DATA_ERROR','OTHER') -cnotcontains $record.code){return}
  $safe=[ordered]@{schema='dependency-failure-v1';stage=$record.stage;group=$record.group;code=$record.code;outcome='FAILED';behavioralInvocations=0}
  $line='DEPENDENCY_FAILURE_V1 '+($safe | ConvertTo-Json -Compress)
  if($line.Length -le 768){[Console]::Error.WriteLine($line)}
 } catch {} # Relay errors never change the original setup error, ownership or status.
}

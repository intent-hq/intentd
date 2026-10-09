param([Parameter(Mandatory)][string]$WorkRoot)
$ErrorActionPreference='Stop'
$stage='provenance';$diagnosticStage='provenance';$outcome='FAILED';$code=0;$script:ownedInvocationStarted=$false
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
# BEGIN FIXED DEPENDENCY FAILURE RELAY
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
  if(@('argument-resolution','platform','node-version','expected-lock-read','installed-lock-read','package-keys','package-path','package-metadata','integrity-attribution','tar-cache-key','tar-url-policy','tar-fetch','tar-response','tar-stream','tar-integrity','tar-inflate','tar-parse','installed-tar-bytes','installed-inventory','generated-files','mirror-absence','mirror-copy','mirror-inventory','entry-pin','resolution-preinventory','resolution-helper','resolution-child','resolution-output','resolution-identities','resolution-targets','resolution-postinventory','mirror-unmodified','result-write','unknown','tar-a01-checksum','tar-a02-octal-size','tar-a03-size-extent','tar-a04-package-path','tar-a05-windows-name','tar-a06-file-unique','tar-a07-file-count','tar-a08-entry-type','tar-a09-nonempty') -cnotcontains $record.stage -or @('none','adapter','pi','unknown') -cnotcontains $record.group -or @('ERR_ASSERTION','ENOENT','ENOTDIR','EACCES','EPERM','EEXIST','ERR_INVALID_ARG_TYPE','ERR_INVALID_ARG_VALUE','ERR_OUT_OF_RANGE','ABORT_ERR','ETIMEDOUT','ERR_BUFFER_TOO_LARGE','Z_DATA_ERROR','OTHER') -cnotcontains $record.code){return}
  $safe=[ordered]@{schema='dependency-failure-v1';stage=$record.stage;group=$record.group;code=$record.code;outcome='FAILED';behavioralInvocations=0}
  $line='DEPENDENCY_FAILURE_V1 '+($safe | ConvertTo-Json -Compress)
  if($line.Length -le 768){[Console]::Error.WriteLine($line)}
 } catch {} # Relay errors never change the original setup error, ownership or status.
}
# END FIXED DEPENDENCY FAILURE RELAY

if (-not $IsWindows -or (Test-Path -LiteralPath $WorkRoot)) { throw 'exclusive_native_setup_required' }
$null=New-Item -ItemType Directory -Path $WorkRoot
$upload=Join-Path $WorkRoot 'upload-private';$null=New-Item -ItemType Directory -Path $upload
$node=(Get-Command node.exe -CommandType Application -TotalCount 1).Source
$packet=$PSScriptRoot
$before=@{};$toolsEqual=$false;$toolsBefore=$null
function Inventory-Packet { $map=@{};Get-ChildItem -LiteralPath $packet -File -Recurse | ForEach-Object {$map[$_.FullName.Substring($packet.Length+1)]=(Get-FileHash -Algorithm SHA256 -LiteralPath $_.FullName).Hash.ToLowerInvariant()};return $map }
function Assert-Owned($r) { if($r.reason -ne 'completed' -or $r.originalExit -ne 0 -or -not $r.assigned -or -not $r.resumed -or -not $r.activeZero -or -not $r.rootSignalled -or -not $r.readerDone -or $r.readerError -or $r.overflow){throw 'setup_child_incomplete'} }
try {
 . (Join-Path $packet 'tool-provenance.ps1')
 $diagnosticStage='payload-verify'
 $manifest=Get-Content -Raw (Join-Path $packet 'payload-manifest.json') | ConvertFrom-Json -AsHashtable
 foreach($rel in $manifest.Keys){$path=Join-Path $packet $rel;if((Get-FileHash -Algorithm SHA256 -LiteralPath $path).Hash.ToLowerInvariant() -ne $manifest[$rel]){throw 'payload_hash_mismatch'}}
 $diagnosticStage='packet-inventory'
 $before=Inventory-Packet
 $npm=Join-Path (Split-Path $node -Parent) 'node_modules/npm/bin/npm-cli.js'
 $diagnosticStage='tool-provenance'
 $toolsBefore=Get-SetupTools $node $npm -CallerEvidence @{schema='npm-caller-context-v1';site='initial';rawNode=$node;rawNpm=$npm}
 $diagnosticStage='native-compile'
 Add-Type -Path (Join-Path $packet 'OwnedSetup.cs')
 $diagnosticStage='compiler-provenance'
 $compiler=Get-CompilerBindings $toolsBefore
 $diagnosticStage='latch-controls'
 & (Join-Path $packet 'latch-controls.ps1') -Output (Join-Path $WorkRoot 'latch-result.json')
 $versionReceipts=@()
 $diagnosticStage='node-version';$script:ownedInvocationStarted=$true
 $vr=[OwnedSetup]::Run($node,[string[]]@('--eval','console.log(JSON.stringify({version:process.version,arch:process.arch}))'),$WorkRoot,(Join-Path $WorkRoot 'node-version.log'),5000,8192,$false)
 $versionReceipts+=@{name='node-version';receipt=$vr};$versionReceipts | ConvertTo-Json -Depth 12 | Set-Content -Encoding utf8NoBOM (Join-Path $WorkRoot 'version-receipts.json');Assert-Owned $vr
 $nodeRuntime=Get-Content -Raw (Join-Path $WorkRoot 'node-version.log') | ConvertFrom-Json
 if($nodeRuntime.version -ne 'v24.21.0' -or $nodeRuntime.arch -ne 'x64'){throw 'node_runtime_mismatch'}
 $diagnosticStage='npm-version'
 $vr=[OwnedSetup]::Run($node,[string[]]@($npm,'--version'),$WorkRoot,(Join-Path $WorkRoot 'npm-version.log'),10000,8192,$false)
 $versionReceipts+=@{name='npm-version';receipt=$vr};$versionReceipts | ConvertTo-Json -Depth 12 | Set-Content -Encoding utf8NoBOM (Join-Path $WorkRoot 'version-receipts.json');Assert-Owned $vr
 $versionReceipts | ConvertTo-Json -Depth 12 | Set-Content -Encoding utf8NoBOM (Join-Path $WorkRoot 'version-receipts.json')
 $npmRuntime=(Get-Content -Raw (Join-Path $WorkRoot 'npm-version.log')).Trim()
 if($npmRuntime -ne $toolsBefore.npm.declaredVersion){throw 'npm_runtime_version_mismatch'}
 $native=Join-Path $WorkRoot 'native'
 $stage='native-controls';$diagnosticStage='native-controls'
 & (Join-Path $packet 'native-controls.ps1') -Node $node -OwnedRoot $native
 if(-not (Test-Path (Join-Path $native 'native-result.json'))){throw 'native_controls_no_receipt'}
 $policy=[OwnedSetup]::Run($node,[string[]]@('--test','--test-concurrency=1','--test-reporter=tap',(Join-Path $packet 'upload-policy-controls.mjs')),$WorkRoot,(Join-Path $WorkRoot 'policy-controls.tap'),30000,1048576,$false)
 $policy | ConvertTo-Json -Depth 12 | Set-Content -Encoding utf8NoBOM (Join-Path $WorkRoot 'policy-receipt.json')
 Assert-Owned $policy
 $policyLines=Get-Content (Join-Path $WorkRoot 'policy-controls.tap')
 $policyExpected=Get-Content -Raw (Join-Path $packet 'upload-expected.json') | ConvertFrom-Json
 $policyActual=@($policyLines | Where-Object {$_ -match '^ok [0-9]+ - '} | ForEach-Object {$_ -replace '^ok [0-9]+ - ',''})
 if(($policyActual | ConvertTo-Json -Compress) -ne ($policyExpected | ConvertTo-Json -Compress)){throw 'upload_controls_identity'}
 foreach($line in @(('1..'+$policyExpected.Count),('# tests '+$policyExpected.Count),('# pass '+$policyExpected.Count),'# fail 0','# cancelled 0','# skipped 0','# todo 0')){if(-not($policyLines -contains $line)){throw 'upload_controls_inventory'}}
 $resolver=[OwnedSetup]::Run($node,[string[]]@((Join-Path $packet 'resolution-control.mjs'),$WorkRoot),$WorkRoot,(Join-Path $WorkRoot 'resolution-control.log'),10000,8192,$false)
 $resolver | ConvertTo-Json -Depth 12 | Set-Content -Encoding utf8NoBOM (Join-Path $WorkRoot 'resolution-receipt.json');Assert-Owned $resolver
 $stage='dependency-install';$diagnosticStage='dependency-install' 
 $npm=Join-Path (Split-Path $node -Parent) 'node_modules/npm/bin/npm-cli.js'
 if(-not(Test-Path -LiteralPath $npm)){throw 'npm_entry_unbound'}
 $install=Join-Path $WorkRoot 'install';$null=New-Item -ItemType Directory -Path $install
 $commands=@(
  @{name='adapter-install';args=@($npm,'install','--prefix',(Join-Path $install '.pi-acp-test'),'--no-save','--ignore-scripts','--no-audit','--no-fund','pi-acp@0.0.34')},
  @{name='pi-install';args=@($npm,'install','--prefix',(Join-Path $install '.pi-runtime-test'),'--no-save','--ignore-scripts','--no-audit','--no-fund','@earendil-works/pi-coding-agent@0.81.0','@earendil-works/pi-agent-core@0.81.0','@earendil-works/pi-ai@0.81.0','@earendil-works/pi-tui@0.81.0')}
 )
 $installReceipts=@()
 foreach($c in $commands){$r=[OwnedSetup]::Run($node,[string[]]$c.args,$install,(Join-Path $WorkRoot ($c.name+'.log')),120000,8388608,$false);$installReceipts+=@{name=$c.name;receipt=$r};$installReceipts | ConvertTo-Json -Depth 12 | Set-Content -Encoding utf8NoBOM (Join-Path $WorkRoot 'install-receipts.json');Assert-Owned $r}
 $stage='dependency-acceptance';$diagnosticStage='dependency-acceptance'
 $r=[OwnedSetup]::Run($node,[string[]]@((Join-Path $packet 'accept-dependencies.mjs'),$packet,$install,$WorkRoot),$install,(Join-Path $WorkRoot 'acceptance.log'),120000,8388608,$false)
 $r | ConvertTo-Json -Depth 12 | Set-Content -Encoding utf8NoBOM (Join-Path $WorkRoot 'acceptance-receipt.json');Assert-Owned $r
 if(-not(Test-Path (Join-Path $WorkRoot 'dependency-result.json'))){throw 'dependency_acceptance_no_receipt'}
 $stage='final-packet-guards';$diagnosticStage='final-packet-guards';$after=Inventory-Packet
 if($before.Count -ne $after.Count){throw 'packet_inventory_changed'}
 foreach($key in $before.Keys){if($before[$key] -ne $after[$key]){throw 'packet_bytes_changed'}}
 $toolsAfter=Get-SetupTools $node $npm -CallerEvidence @{schema='npm-caller-context-v1';site='final';rawNode=$node;rawNpm=$npm}
 Assert-SetupToolsEqual $toolsBefore $toolsAfter;$toolsEqual=$true
 @{status='PASS';before=$toolsBefore;afterEqual=$true;nodeRuntime=$nodeRuntime;npmRuntimeVersion=$npmRuntime;compiler=$compiler} | ConvertTo-Json -Depth 14 | Set-Content -Encoding utf8NoBOM (Join-Path $WorkRoot 'tool-provenance.json')
 $outcome='PASS';$stage='setup-complete';$diagnosticStage='setup-complete' 
} catch {
 $code=$_.Exception.HResult; $outcome='FAILED'
 $originalFailure=New-SetupFailureRecord 'original' $diagnosticStage ([string]$_.CategoryInfo.Category) $code (Get-SetupOwnershipState)
 Write-SetupFailureRecord $originalFailure
 if($diagnosticStage -ceq 'dependency-acceptance' -and $originalFailure.ownership -ceq 'safe'){Write-DependencyFailureMetadata $WorkRoot}
}
finally {
 try {
 # A private local artifact journal remains available to the reviewer on the runner.
 # Upload only fixed-schema summaries and predeclared identities/digests, never npm output,
 # arbitrary exceptions, installed files, credentials, or entire runner temporary directories.
 $logs=@();foreach($name in @('adapter-install.log','pi-install.log','acceptance.log')){$f=Join-Path $WorkRoot $name;if(Test-Path -LiteralPath $f){$logs+=@{name=$name;bytes=(Get-Item -LiteralPath $f).Length;sha256=(Get-FileHash -LiteralPath $f -Algorithm SHA256).Hash.ToLowerInvariant()}}}
 $summary=@{stage=$stage;outcome=$outcome;errorHResult=$code;behavioralInvocations=0;nodeSha256=(Get-FileHash -LiteralPath $node -Algorithm SHA256).Hash.ToLowerInvariant();platform='win32';nodeVersion='24.21.0';logs=$logs;rawFailureOutputUploaded=$false;qualification='Setup-only. Failed setup output may be incomplete; no Windows behavioral or historical cause acceptance.'}
 $summary | ConvertTo-Json -Depth 10 | Set-Content -Encoding utf8NoBOM (Join-Path $upload 'setup-summary.json')
 foreach($name in @('native/native-receipts.json','native/native-result.json','install-receipts.json','acceptance-receipt.json','policy-receipt.json','latch-result.json','resolution-control-result.json','resolution-receipt.json','version-receipts.json')){
  $f=Join-Path $WorkRoot $name
  if(Test-Path -LiteralPath $f){if((Get-Item -LiteralPath $f).Length -gt 1048576){throw 'receipt_cap'};Copy-Item -LiteralPath $f -Destination (Join-Path $upload ($name -replace '/','-'))}
 }
 # Dependency acceptance output contains only package paths, registry URLs, hashes and fixed metadata.
 $dep=Join-Path $WorkRoot 'dependency-result.json';if($outcome -eq 'PASS' -and (Test-Path -LiteralPath $dep)){if((Get-Item -LiteralPath $dep).Length -gt 16777216){throw 'inventory_cap'};Copy-Item -LiteralPath $dep -Destination (Join-Path $upload 'dependency-result.json')}
 if($outcome -eq 'PASS'){
  $tap=Join-Path $WorkRoot 'native/controls.tap';$lines=Get-Content -LiteralPath $tap
  # Full schema/TAP reconciliation happens before promotion, with the reviewed validator.
  Copy-Item -LiteralPath $tap -Destination (Join-Path $upload 'controls.tap')
  Copy-Item -LiteralPath (Join-Path $WorkRoot 'policy-controls.tap') -Destination (Join-Path $upload 'policy-controls.tap')
  $toolFile=Join-Path $WorkRoot 'tool-provenance.json';if((Get-Item -LiteralPath $toolFile).Length -gt 8388608){throw 'tool_provenance_cap'}
  Copy-Item -LiteralPath $toolFile -Destination (Join-Path $upload 'tool-provenance.json')
 }
 $allowed=@('setup-summary.json','native-native-receipts.json','native-native-result.json','install-receipts.json','acceptance-receipt.json','dependency-result.json','controls.tap','policy-controls.tap','policy-receipt.json','latch-result.json','resolution-control-result.json','resolution-receipt.json','version-receipts.json','tool-provenance.json')
 $total=0;foreach($f in Get-ChildItem -LiteralPath $upload){if($f.PSIsContainer -or $f.Name -notin $allowed){throw 'unexpected_upload'};$total+=$f.Length};if($total -gt 25165824){throw 'upload_cap'}
 $ownership=Get-SetupOwnershipState
 if($ownership -ne 'safe'){
  # Includes not-initialized: no validator or any new process is launched.
  $outcome='FAILED'
  Write-SetupFailureRecord (New-SetupFailureRecord 'finalizer' $diagnosticStage 'ResourceUnavailable' $code $ownership)
 } else {
 $check=[OwnedSetup]::Run($node,[string[]]@((Join-Path $packet 'upload-policy.mjs'),$upload,(Join-Path $packet 'payload/diagnostic/expected-tests.json')),$WorkRoot,(Join-Path $WorkRoot 'upload-validator.log'),10000,65536,$false)
 $check | ConvertTo-Json -Depth 12 | Set-Content -Encoding utf8NoBOM (Join-Path $WorkRoot 'upload-validator-receipt.json')
 Assert-Owned $check
 if($null -ne $toolsBefore){$afterValidatorTools=Get-SetupTools $node $npm -CallerEvidence @{schema='npm-caller-context-v1';site='post-validator';rawNode=$node;rawNpm=$npm};Assert-SetupToolsEqual $toolsBefore $afterValidatorTools}
 Move-Item -LiteralPath $upload -Destination (Join-Path $WorkRoot 'upload')
 }
 } catch {
  $outcome='FAILED'
  Write-SetupFailureRecord (New-SetupFailureRecord 'finalizer' $diagnosticStage ([string]$_.CategoryInfo.Category) $_.Exception.HResult (Get-SetupOwnershipState))
 }
}
if($outcome -ne 'PASS'){exit 1}

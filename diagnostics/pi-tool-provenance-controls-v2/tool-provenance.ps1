# Bounded evidence only; no provider query, process, file output or raw exception serialization.
function Set-ToolProbe($Context,[string]$Stage,[string]$Predicate='none',[string]$Tree='none') {
 try { if($null -ne $Context){$Context.value=@{stage=$Stage;predicate=$Predicate;tree=$Tree}} } catch { }
}
function Write-ToolProbeFailure($Context,[string]$Category,[int]$HResult) {
 try {
  $stages=@('npm-entry','npm-root','npm-package','npm-identity','host-path','host-identity','image-read','image-shape','record-construction','tree-enumerate','tree-entry','tree-file','tree-hash')
  $predicates=@('none','npm_entry_missing','npm_package_identity','powershell_host_identity','hosted_image_metadata','tool_reparse_point','tool_inventory_cap')
  $categories=@('NotSpecified','OpenError','CloseError','DeviceError','DeadlockDetected','InvalidArgument','InvalidData','InvalidOperation','InvalidResult','InvalidType','MetadataError','NotImplemented','NotInstalled','ObjectNotFound','OperationStopped','OperationTimeout','SyntaxError','ParserError','PermissionDenied','ResourceBusy','ResourceExists','ResourceUnavailable','ReadError','WriteError','FromStdErr','SecurityError','ProtocolError','ConnectionError','AuthenticationError','LimitsExceeded','QuotaExceeded','NotEnabled')
  $stage='unknown';$predicate='unknown';$tree='unknown';$valid=$false
  if($Context.value.stage -in $stages -and $Context.value.predicate -in $predicates -and $Context.value.tree -in @('none','npm','powershell','unknown')){
   $stage=$Context.value.stage;$predicate=$Context.value.predicate;$tree=$Context.value.tree;$valid=$true
  }
  if($Category -notin $categories){$Category='Other'}
  $record=[ordered]@{schema='tool-probe-failure-v1';stage=$stage;predicate=$predicate;tree=$tree;markerValid=$valid;category=$Category;hresult=$HResult}
  $line='TOOL_PROBE_FAILURE_V1 '+($record | ConvertTo-Json -Compress)
  if($line.Length -le 1024){[Console]::Error.WriteLine($line)}
 } catch { }
}
function Get-BoundedTree([string]$Root,$Evidence=$null,[string]$TreeKind='unknown') {
 $map=@{};$total=0
 Set-ToolProbe $Evidence 'tree-enumerate' 'none' $TreeKind
 $items=Get-ChildItem -LiteralPath $Root -Recurse -Force
 foreach($f in $items){Set-ToolProbe $Evidence 'tree-entry' 'none' $TreeKind;if($f.Attributes -band [System.IO.FileAttributes]::ReparsePoint){Set-ToolProbe $Evidence 'tree-entry' 'tool_reparse_point' $TreeKind;throw 'tool_reparse_point'};if(-not $f.PSIsContainer){Set-ToolProbe $Evidence 'tree-file' 'none' $TreeKind;$total+=$f.Length;if($map.Count -ge 20000 -or $total -gt 2147483648 -or $f.Length -gt 268435456){Set-ToolProbe $Evidence 'tree-file' 'tool_inventory_cap' $TreeKind;throw 'tool_inventory_cap'};$rel=$f.FullName.Substring($Root.Length+1).Replace('\','/').ToLowerInvariant();Set-ToolProbe $Evidence 'tree-hash' 'none' $TreeKind;$map[$rel]=(Get-FileHash -Algorithm SHA256 -LiteralPath $f.FullName).Hash.ToLowerInvariant()}}
 Set-ToolProbe $Evidence 'record-construction'
 return $map
}
function Get-SetupTools([string]$Node,[string]$Npm) {
 $toolProbe=@{value=$null}
 try {
 Set-ToolProbe $toolProbe 'npm-entry'
 if(-not(Test-Path -LiteralPath $Npm)){Set-ToolProbe $toolProbe 'npm-entry' 'npm_entry_missing';throw 'npm_entry_missing'}
 Set-ToolProbe $toolProbe 'npm-root'
 $npmRoot=Split-Path (Split-Path $Npm -Parent) -Parent
 Set-ToolProbe $toolProbe 'npm-package'
 $npmPackage=Get-Content -Raw -LiteralPath (Join-Path $npmRoot 'package.json') | ConvertFrom-Json
 Set-ToolProbe $toolProbe 'npm-identity'
 if($npmPackage.name -ne 'npm' -or $npmPackage.version -notmatch '^[0-9]+\.[0-9]+\.[0-9]+$'){Set-ToolProbe $toolProbe 'npm-identity' 'npm_package_identity';throw 'npm_package_identity'}
 Set-ToolProbe $toolProbe 'host-path'
 $hostProcess=[Environment]::ProcessPath
 Set-ToolProbe $toolProbe 'host-identity'
 if((Split-Path $hostProcess -Leaf) -ne 'pwsh.exe'){Set-ToolProbe $toolProbe 'host-identity' 'powershell_host_identity';throw 'powershell_host_identity'}
 Set-ToolProbe $toolProbe 'image-read'
 $imageOS=[Environment]::GetEnvironmentVariable('ImageOS');$imageVersion=[Environment]::GetEnvironmentVariable('ImageVersion')
 Set-ToolProbe $toolProbe 'image-shape'
 foreach($v in @($imageOS,$imageVersion)){if($v -notmatch '^[A-Za-z0-9._-]{1,100}$'){Set-ToolProbe $toolProbe 'image-shape' 'hosted_image_metadata';throw 'hosted_image_metadata'}}
 Set-ToolProbe $toolProbe 'record-construction'
 return @{
  node=@{sha256=(Get-FileHash -LiteralPath $Node -Algorithm SHA256).Hash.ToLowerInvariant();fileName=(Split-Path $Node -Leaf)}
  npm=@{entry='bin/npm-cli.js';declaredVersion=$npmPackage.version;files=(Get-BoundedTree $npmRoot $toolProbe 'npm')}
  powershell=@{version=$PSVersionTable.PSVersion.ToString();edition=$PSVersionTable.PSEdition;hostSha256=(Get-FileHash -LiteralPath $hostProcess -Algorithm SHA256).Hash.ToLowerInvariant();files=(Get-BoundedTree $PSHOME $toolProbe 'powershell')}
  runtime=@{framework=[System.Runtime.InteropServices.RuntimeInformation]::FrameworkDescription;osDescription=[System.Runtime.InteropServices.RuntimeInformation]::OSDescription;osVersion=[Environment]::OSVersion.Version.ToString();processArchitecture=[System.Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture.ToString();osArchitecture=[System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString();imageOS=$imageOS;imageVersion=$imageVersion}
 }
 } catch {
  try { Write-ToolProbeFailure $toolProbe ([string]$_.CategoryInfo.Category) $_.Exception.HResult } catch { }
  throw
 }
}
function Assert-SetupToolsEqual($A,$B) {
 foreach($name in @('node','runtime')){foreach($key in $A[$name].Keys){if($A[$name][$key] -ne $B[$name][$key]){throw 'tool_provenance_changed'}}}
 foreach($name in @('npm','powershell')){foreach($key in $A[$name].Keys){if($key -eq 'files'){if($A[$name].files.Count -ne $B[$name].files.Count){throw 'tool_inventory_changed'};foreach($path in $A[$name].files.Keys){if($A[$name].files[$path] -ne $B[$name].files[$path]){throw 'tool_bytes_changed'}}}elseif($A[$name][$key] -ne $B[$name][$key]){throw 'tool_metadata_changed'}}}
}
function Get-CompilerBindings($Before) {
 $rows=@()
 foreach($a in [AppDomain]::CurrentDomain.GetAssemblies()){
  if($a.GetName().Name -in @('Microsoft.CodeAnalysis','Microsoft.CodeAnalysis.CSharp','System.Private.CoreLib')){
   $location=$a.Location
   if(-not $location.StartsWith($PSHOME+[IO.Path]::DirectorySeparatorChar,[StringComparison]::OrdinalIgnoreCase)){throw 'compiler_outside_bound_tooltree'}
   $relative=$location.Substring($PSHOME.Length+1).Replace('\','/').ToLowerInvariant()
   $hash=(Get-FileHash -LiteralPath $location -Algorithm SHA256).Hash.ToLowerInvariant()
   if($Before.powershell.files[$relative] -ne $hash){throw 'compiler_input_unbound'}
   $rows+=@{name=$a.GetName().Name;version=$a.GetName().Version.ToString();relativePath=$relative;sha256=$hash}
  }
 }
 if($rows.Count -ne 3){throw 'compiler_provenance_incomplete'}
 return $rows
}

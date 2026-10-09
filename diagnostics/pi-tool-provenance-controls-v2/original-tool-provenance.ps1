function Get-BoundedTree([string]$Root) {
 $map=@{};$total=0
 $items=Get-ChildItem -LiteralPath $Root -Recurse -Force
 foreach($f in $items){if($f.Attributes -band [System.IO.FileAttributes]::ReparsePoint){throw 'tool_reparse_point'};if(-not $f.PSIsContainer){$total+=$f.Length;if($map.Count -ge 20000 -or $total -gt 2147483648 -or $f.Length -gt 268435456){throw 'tool_inventory_cap'};$rel=$f.FullName.Substring($Root.Length+1).Replace('\','/').ToLowerInvariant();$map[$rel]=(Get-FileHash -Algorithm SHA256 -LiteralPath $f.FullName).Hash.ToLowerInvariant()}}
 return $map
}
function Get-SetupTools([string]$Node,[string]$Npm) {
 if(-not(Test-Path -LiteralPath $Npm)){throw 'npm_entry_missing'}
 $npmRoot=Split-Path (Split-Path $Npm -Parent) -Parent
 $npmPackage=Get-Content -Raw -LiteralPath (Join-Path $npmRoot 'package.json') | ConvertFrom-Json
 if($npmPackage.name -ne 'npm' -or $npmPackage.version -notmatch '^[0-9]+\.[0-9]+\.[0-9]+$'){throw 'npm_package_identity'}
 $hostProcess=[Environment]::ProcessPath
 if((Split-Path $hostProcess -Leaf) -ne 'pwsh.exe'){throw 'powershell_host_identity'}
 $imageOS=[Environment]::GetEnvironmentVariable('ImageOS');$imageVersion=[Environment]::GetEnvironmentVariable('ImageVersion')
 foreach($v in @($imageOS,$imageVersion)){if($v -notmatch '^[A-Za-z0-9._-]{1,100}$'){throw 'hosted_image_metadata'}}
 return @{
  node=@{sha256=(Get-FileHash -LiteralPath $Node -Algorithm SHA256).Hash.ToLowerInvariant();fileName=(Split-Path $Node -Leaf)}
  npm=@{entry='bin/npm-cli.js';declaredVersion=$npmPackage.version;files=(Get-BoundedTree $npmRoot)}
  powershell=@{version=$PSVersionTable.PSVersion.ToString();edition=$PSVersionTable.PSEdition;hostSha256=(Get-FileHash -LiteralPath $hostProcess -Algorithm SHA256).Hash.ToLowerInvariant();files=(Get-BoundedTree $PSHOME)}
  runtime=@{framework=[System.Runtime.InteropServices.RuntimeInformation]::FrameworkDescription;osDescription=[System.Runtime.InteropServices.RuntimeInformation]::OSDescription;osVersion=[Environment]::OSVersion.Version.ToString();processArchitecture=[System.Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture.ToString();osArchitecture=[System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString();imageOS=$imageOS;imageVersion=$imageVersion}
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

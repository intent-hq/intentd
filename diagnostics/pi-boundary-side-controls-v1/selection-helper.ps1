# Failure-only relationship probe; proposed artifact, not applied or executed.
function Write-NpmSelectionFailure([string]$SelectedNode,[string]$DerivedNpm) {
 try {
  $record=[ordered]@{schema='npm-selection-failure-v1';originalPredicate='npm_entry_missing';nodeShape='unknown';derivedRelation='unknown';command='unread';commandShape='unknown';parentRelation='unknown';siblingEntry='unread';probeStage='input-shape';category='none';hresult=0}
  try {
   if([IO.Path]::IsPathRooted($SelectedNode) -and [IO.Path]::GetFileName($SelectedNode) -ieq 'node.exe'){
    $record.nodeShape='rooted-node-exe'
    $nodeParent=[IO.Path]::GetDirectoryName($SelectedNode)
    $expected=[IO.Path]::GetFullPath([IO.Path]::Combine($nodeParent,'node_modules/npm/bin/npm-cli.js'))
    if([IO.Path]::IsPathRooted($DerivedNpm)){
     $record.derivedRelation=if([string]::Equals($expected,[IO.Path]::GetFullPath($DerivedNpm),[StringComparison]::OrdinalIgnoreCase)){'matches-node-parent'}else{'different'}
    }
   }
   $record.probeStage='npm-command'
   $commands=@(Get-Command npm.cmd -CommandType Application -ErrorAction Stop)
   if($commands.Count -eq 0){$record.command='missing'}
   elseif($commands.Count -ne 1){$record.command='multiple'}
   else {
    $record.command='one'
    $source=$commands[0].Source
    if($source -is [string] -and $source.Length -le 32768 -and [IO.Path]::IsPathRooted($source) -and [IO.Path]::GetFileName($source) -ieq 'npm.cmd'){
     $record.commandShape='rooted-npm-cmd'
     $record.probeStage='path-relation'
     $commandParent=[IO.Path]::GetDirectoryName($source)
     if($record.nodeShape -eq 'rooted-node-exe'){
      $record.parentRelation=if([string]::Equals([IO.Path]::GetFullPath($nodeParent),[IO.Path]::GetFullPath($commandParent),[StringComparison]::OrdinalIgnoreCase)){'same'}else{'different'}
     }
     $candidate=[IO.Path]::Combine($commandParent,'node_modules/npm/bin/npm-cli.js')
     $record.probeStage='sibling-existence'
     $record.siblingEntry=if(Test-Path -LiteralPath $candidate -PathType Leaf -ErrorAction Stop){'present'}else{'absent'}
    }
   }
   $record.probeStage='complete'
  } catch {
   $category=[string]$_.CategoryInfo.Category
   $record.category=if($category -in @('ObjectNotFound','PermissionDenied','InvalidArgument','InvalidData','ReadError','SecurityError')){$category}else{'Other'}
   $record.hresult=[int]$_.Exception.HResult
   if($record.probeStage -eq 'npm-command'){$record.command=if($category -eq 'ObjectNotFound'){'missing'}else{'error'}}
  }
  $line='NPM_SELECTION_FAILURE_V1 '+($record | ConvertTo-Json -Compress)
  if($line.Length -le 1024){[Console]::Error.WriteLine($line)}
 } catch { }
}

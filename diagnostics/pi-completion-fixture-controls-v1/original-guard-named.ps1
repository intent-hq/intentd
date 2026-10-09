function OriginalGuard {
 $manifest=Get-Content -Raw (Join-Path $PSScriptRoot 'payload-manifest.json')|ConvertFrom-Json -AsHashtable
 $expected=@($manifest.Keys)+@('payload-manifest.json')
 $actual=@(Get-ChildItem -LiteralPath $PSScriptRoot -File|ForEach-Object {$_.Name})
 if(@(Get-ChildItem -LiteralPath $PSScriptRoot -Directory).Count -ne 0 -or (($expected|Sort-Object)-join '|') -cne (($actual|Sort-Object)-join '|')){throw 'payload_inventory'}
 foreach($n in $manifest.Keys){if($n -notmatch '^[a-z0-9.-]+$' -or (Hash (Join-Path $PSScriptRoot $n)) -cne $manifest[$n]){throw 'payload_hash'}}
}

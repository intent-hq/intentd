# One-line installer for the intentd sitter (Windows x86_64).
#
#   powershell -c "irm https://github.com/intent-hq/intentd-releases/releases/download/sitter-latest/install.ps1 | iex"
#
# Downloads the intentd-x86_64-pc-windows-msvc.zip archive from the fixed
# sitter-latest release on the public intent-hq/intentd-releases mirror,
# verifies its .sha256 sidecar, installs intentd.exe (the self-updating
# sitter) to $env:INTENTD_INSTALL_DIR (default: $env:LOCALAPPDATA\intentd\bin),
# and adds the install dir to the user PATH. Idempotent: re-running replaces
# the installed binary.
$ErrorActionPreference = 'Stop'

$BaseUrl = 'https://github.com/intent-hq/intentd-releases/releases/download/sitter-latest'
$Archive = 'intentd-x86_64-pc-windows-msvc.zip'

$arch = $env:PROCESSOR_ARCHITECTURE
if ($arch -ne 'AMD64') {
    throw "install.ps1: unsupported architecture '$arch' (only x86_64/AMD64 Windows builds are published)"
}

$installDir = if ($env:INTENTD_INSTALL_DIR) {
    $env:INTENTD_INSTALL_DIR
} else {
    Join-Path $env:LOCALAPPDATA 'intentd\bin'
}
New-Item -ItemType Directory -Force -Path $installDir | Out-Null

$tmp = Join-Path ([System.IO.Path]::GetTempPath()) ('intentd-install-' + [System.IO.Path]::GetRandomFileName())
New-Item -ItemType Directory -Force -Path $tmp | Out-Null
try {
    Write-Host "install.ps1: downloading $Archive from the sitter-latest release..."
    $zipPath = Join-Path $tmp $Archive
    Invoke-WebRequest -Uri "$BaseUrl/$Archive" -OutFile $zipPath -UseBasicParsing
    Invoke-WebRequest -Uri "$BaseUrl/$Archive.sha256" -OutFile "$zipPath.sha256" -UseBasicParsing

    # Sidecar format is "HASH *NAME" (sha256sum --binary); the hash is the
    # first whitespace-delimited token.
    $expected = ((Get-Content "$zipPath.sha256" -Raw).Trim() -split '\s+')[0].ToLowerInvariant()
    $actual = (Get-FileHash -Algorithm SHA256 -Path $zipPath).Hash.ToLowerInvariant()
    if ($actual -ne $expected) {
        throw "install.ps1: sha256 verification failed for $Archive (expected $expected, got $actual)"
    }
    Write-Host 'install.ps1: sha256 verified'

    Expand-Archive -Path $zipPath -DestinationPath $tmp -Force
    $binary = Join-Path $tmp 'intentd.exe'
    if (-not (Test-Path $binary)) {
        throw 'install.ps1: archive did not contain intentd.exe'
    }
    Copy-Item $binary (Join-Path $installDir 'intentd.exe') -Force
} finally {
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
}

# Add the install dir to the user PATH (persisted) and the current session.
$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
$parts = @(($userPath -split ';') | Where-Object { $_ -ne '' })
if ($parts -notcontains $installDir) {
    [Environment]::SetEnvironmentVariable('Path', (($parts + $installDir) -join ';'), 'User')
    Write-Host "install.ps1: added $installDir to your user PATH (new terminals pick it up automatically)"
}
if (@($env:Path -split ';') -notcontains $installDir) {
    $env:Path = "$env:Path;$installDir"
}

Write-Host "install.ps1: installed intentd to $installDir\intentd.exe"
Write-Host ''
Write-Host 'Next steps:'
Write-Host '  intentd serve   # start the daemon (downloads the real daemon on first run)'

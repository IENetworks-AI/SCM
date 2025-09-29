<#
.archive_artifacts.ps1
Non-destructive archivist: copies large/old artifacts into artifacts/archived/<timestamp>/ and creates a zip.
Default: copy files larger than 10MB (configurable) excluding .git and the backup created earlier.
Does NOT delete originals unless -RemoveAfterArchive is set to $true.
#>
param(
    [int]$MinBytes = 10MB,
    [switch]$RemoveAfterArchive
)

Set-StrictMode -Version Latest
Push-Location $PSScriptRoot/.. | Out-Null

$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$dst = Join-Path -Path . -ChildPath "artifacts/archived/$ts"
New-Item -ItemType Directory -Path $dst -Force | Out-Null

Write-Output "Archiving files >= $MinBytes bytes to: $dst"

# Find large files
$large = Get-ChildItem -Path . -Recurse -Force -ErrorAction SilentlyContinue |
    Where-Object { -not $_.PSIsContainer -and $_.FullName -notmatch "\\.git\\" -and $_.Length -ge $MinBytes }

foreach ($f in $large) {
    $rel = $f.FullName.Substring((Get-Location).Path.Length).TrimStart('\')
    $targetDir = Join-Path $dst ([IO.Path]::GetDirectoryName($rel))
    if (!(Test-Path $targetDir)) { New-Item -ItemType Directory -Path $targetDir -Force | Out-Null }
    Copy-Item -Path $f.FullName -Destination (Join-Path $targetDir $f.Name) -Force
    Write-Output "Copied: $rel -> $targetDir"
}

# Create a zip of the archived folder
$zipPath = "$dst.zip"
if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
Compress-Archive -Path $dst\* -DestinationPath $zipPath -Force
Write-Output "Created archive: $zipPath"

if ($RemoveAfterArchive.IsPresent) {
    Write-Output "Removing originals after archive (RemoveAfterArchive=true)"
    foreach ($f in $large) { Remove-Item -Path $f.FullName -Force }
}

Pop-Location | Out-Null

Write-Output "Done. Review artifacts/archived/$ts and $zipPath"

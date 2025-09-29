<#
cleanup_non_destructive.ps1
Performs non-destructive cleanup steps on the cleanup branch:
 - archives common caches and build artifacts into artifacts/archived/
 - trims __pycache__ by compressing them (keeps originals in archive)
 - updates .gitignore (committed separately)
#>
Set-StrictMode -Version Latest
Push-Location $PSScriptRoot/.. | Out-Null

$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$archiveRoot = Join-Path -Path . -ChildPath "artifacts/archived/$ts"
New-Item -ItemType Directory -Path $archiveRoot -Force | Out-Null

Write-Output "Archiving caches to $archiveRoot"

$patterns = @('.cache','.pytest_cache','dist','build','__pycache__')
foreach ($p in $patterns) {
    $items = Get-ChildItem -Path . -Recurse -Force -ErrorAction SilentlyContinue | Where-Object { $_.PSIsContainer -and $_.Name -ieq $p }
    foreach ($d in $items) {
        $rel = $d.FullName.Substring((Get-Location).Path.Length).TrimStart('\')
        $target = Join-Path $archiveRoot $rel
        Write-Output "Archiving $rel -> $target"
        Copy-Item -Path $d.FullName -Destination $target -Recurse -Force
    }
}

Write-Output "Compressing archived folder"
$zip = "$archiveRoot.zip"
if (Test-Path $zip) { Remove-Item $zip -Force }
Compress-Archive -Path $archiveRoot\* -DestinationPath $zip -Force
Write-Output "Created $zip"

Pop-Location | Out-Null
Write-Output "Non-destructive cleanup finished. Review artifacts/archived/$ts"

# sync_db_to_server.ps1
# Uploads kbo_stats.db to the remote server via SCP.
# Call this script after any pipeline that modifies the DB.
#
# Usage (standalone):
#   .\scripts\sync_db_to_server.ps1
# Usage (from another script after Set-Location to project root):
#   & "$PSScriptRoot\sync_db_to_server.ps1"

param(
    [string] $DbPath     = "kbo_stats.db",
    [string] $RemoteUser = "testuesr",
    [string] $RemoteHost = "58.236.187.135",
    [int]    $SshPort    = 1003,
    [string] $RemotePath = "~/kbo_stat_BE/kbo_stats.db",
    [switch] $SkipSync
)

if ($SkipSync) {
    Write-Host "[sync] --skip-sync flag set; skipping DB upload." -ForegroundColor Yellow
    exit 0
}

if (-not (Test-Path $DbPath)) {
    Write-Host "[sync] ERROR: DB file not found at '$DbPath'. Skipping upload." -ForegroundColor Red
    exit 1
}

$sizeMB = [math]::Round((Get-Item $DbPath).Length / 1MB, 1)
Write-Host ""
Write-Host "------------------------------------------------------------" -ForegroundColor Cyan
Write-Host "  Syncing DB to server" -ForegroundColor Cyan
Write-Host "  Local : $DbPath ($sizeMB MB)" -ForegroundColor Cyan
Write-Host "  Remote: ${RemoteUser}@${RemoteHost}:${RemotePath} (port $SshPort)" -ForegroundColor Cyan
Write-Host "------------------------------------------------------------" -ForegroundColor Cyan

scp -P $SshPort "$DbPath" "${RemoteUser}@${RemoteHost}:${RemotePath}"

if ($LASTEXITCODE -ne 0) {
    Write-Host "[sync] ERROR: SCP upload FAILED (exit $LASTEXITCODE)." -ForegroundColor Red
    Write-Host "[sync] Check SSH key auth or network connectivity." -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host "[sync] DB uploaded successfully." -ForegroundColor Green

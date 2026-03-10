param(
    [int] $Season = 0,
    [string] $AsOfDate = "",
    [ValidateSet("prediction", "projection")]
    [string] $Mode = "prediction",
    [switch] $SkipCollect,
    [switch] $SkipSnapshot,
    [switch] $SkipVerify,
    [switch] $ReplaceExisting,
    [switch] $AllowMissingFeatures,
    [switch] $RunValidation,
    [switch] $DryRun
)

$ErrorActionPreference = 'Stop'
Set-Location (Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) "..")

# Single daily entry-point for hitter E2E pipeline.
# Runs: collect -> snapshot -> predict -> verify
function Assert-LastExit([string]$step) {
    if ($LASTEXITCODE -ne 0) { throw "$step failed with exit code $LASTEXITCODE" }
}

# 0 means auto-detect season from DB.
if ($Season -eq 0) {
    $Season = (
    @'
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

current_year = datetime.now(ZoneInfo("Asia/Seoul")).year
try:
    conn = sqlite3.connect("kbo_stats.db")
    cur = conn.cursor()
    row = cur.execute(
        "SELECT MAX(CAST(substr(game_date,1,4) AS INTEGER)) FROM hitter_game_logs WHERE CAST(substr(game_date,1,4) AS INTEGER) <= ?",
        (current_year,),
    ).fetchone()
    conn.close()
    print(int(row[0]) if row and row[0] else current_year)
except Exception:
    print(current_year)
'@ | python -
    ).Trim()
    Assert-LastExit "resolve season"
}

# Empty as-of means "today" in KST.
if ([string]::IsNullOrWhiteSpace($AsOfDate)) {
    $AsOfDate = (
    @'
from datetime import datetime
from zoneinfo import ZoneInfo
print(datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d"))
'@ | python -
    ).Trim()
    Assert-LastExit "resolve as-of date"
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  KBO Hitter Daily Pipeline" -ForegroundColor Cyan
Write-Host "  season=$Season  as_of=$AsOfDate  mode=$Mode  dry_run=$DryRun" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

$pyArgs = @(
    "-m", "prediction.orchestrate_daily",
    "--season", "$Season",
    "--as-of-date", "$AsOfDate",
    "--mode", "$Mode"
)

if ($SkipCollect) { $pyArgs += "--skip-collect" }
if ($SkipSnapshot) { $pyArgs += "--skip-snapshot" }
if ($SkipVerify) { $pyArgs += "--skip-verify" }
if ($ReplaceExisting) { $pyArgs += "--replace-existing" }
if ($AllowMissingFeatures) { $pyArgs += "--allow-missing-features" }
if ($RunValidation) { $pyArgs += "--run-validation" }
if ($DryRun) { $pyArgs += "--dry-run" }

python @pyArgs
$exitCode = $LASTEXITCODE

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "Daily pipeline COMPLETED successfully." -ForegroundColor Green
    Write-Host "  season=$Season  as_of=$AsOfDate  mode=$Mode" -ForegroundColor Green
}
else {
    Write-Host "Daily pipeline FAILED (exit $exitCode)." -ForegroundColor Red
    Write-Host "  Review the stage log above to identify the failing step." -ForegroundColor Red
    exit $exitCode
}

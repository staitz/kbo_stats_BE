커param(
    [int] $Season = 0,
    [string] $AsOfDate = "",
    [ValidateSet("prediction", "projection")]
    [string] $Mode = "prediction",
    [string[]] $BacktestDates = @(),
    [switch] $SkipQuality,
    [switch] $SkipBacktest,
    [switch] $SkipRegression
)

$ErrorActionPreference = 'Stop'
Set-Location (Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) "..")

# Hitter prediction validation suite wrapper.
function Write-Step([string]$msg, [string]$color = "Cyan") {
    Write-Host "`n$msg" -ForegroundColor $color
}

function Assert-LastExit([string]$step) {
    if ($LASTEXITCODE -ne 0) { throw "$step exited with code $LASTEXITCODE" }
}

if ($Season -eq 0) {
    $Season = (@'
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

current_year = datetime.now(ZoneInfo("Asia/Seoul")).year
try:
    conn = sqlite3.connect("kbo_stats.db")
    row = conn.execute(
        "SELECT MAX(CAST(substr(game_date,1,4) AS INTEGER)) FROM hitter_game_logs WHERE CAST(substr(game_date,1,4) AS INTEGER) <= ?",
        (current_year,),
    ).fetchone()
    conn.close()
    print(int(row[0]) if row and row[0] else current_year)
except Exception:
    print(current_year)
'@ | python -).Trim()
    Assert-LastExit "resolve season"
}

if ([string]::IsNullOrWhiteSpace($AsOfDate)) {
    $AsOfDate = (@'
from datetime import datetime
from zoneinfo import ZoneInfo
print(datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d"))
'@ | python -).Trim()
    Assert-LastExit "resolve as-of date"
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  KBO Hitter Prediction Validation Suite" -ForegroundColor Cyan
Write-Host "  season=$Season  as_of=$AsOfDate  mode=$Mode" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

$anyFailed = $false

if (-not $SkipQuality) {
    Write-Step "[1/3] QUALITY CHECK"
    python -m prediction.validate quality --season $Season --as-of-date $AsOfDate --mode $Mode
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  QUALITY CHECK FAILED" -ForegroundColor Red
        $anyFailed = $true
    }
    else {
        Write-Host "  QUALITY CHECK PASSED" -ForegroundColor Green
    }
}
else {
    Write-Step "[1/3] QUALITY CHECK skipped" "DarkGray"
}

if (-not $SkipBacktest) {
    Write-Step "[2/3] BACKTEST"
    $dates = if ($BacktestDates.Count -gt 0) { $BacktestDates } else { @($AsOfDate) }

    $cmd = @("-m", "prediction.validate", "backtest", "--season", "$Season", "--dates")
    $cmd += $dates
    python @cmd

    if ($LASTEXITCODE -ne 0) {
        Write-Host "  BACKTEST FAILED (performance below threshold)" -ForegroundColor Red
        $anyFailed = $true
    }
    else {
        Write-Host "  BACKTEST PASSED" -ForegroundColor Green
    }
}
else {
    Write-Step "[2/3] BACKTEST skipped" "DarkGray"
}

if (-not $SkipRegression) {
    Write-Step "[3/3] REGRESSION SMOKE TEST"
    python -m prediction.validate regression --season $Season
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  REGRESSION TEST FAILED" -ForegroundColor Red
        $anyFailed = $true
    }
    else {
        Write-Host "  REGRESSION TEST PASSED" -ForegroundColor Green
    }
}
else {
    Write-Step "[3/3] REGRESSION SMOKE TEST skipped" "DarkGray"
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
if ($anyFailed) {
    Write-Host "  VALIDATION FAILED - see checks above for details." -ForegroundColor Red
    exit 1
}
else {
    Write-Host "  VALIDATION PASSED" -ForegroundColor Green
}
Write-Host "============================================================" -ForegroundColor Cyan

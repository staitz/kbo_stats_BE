$ErrorActionPreference = 'Stop'
Set-Location (Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) "..")

function Assert-LastExit([string]$step) {
  if ($LASTEXITCODE -ne 0) {
    throw "$step failed with exit code $LASTEXITCODE"
  }
}

$PythonExe = Join-Path (Get-Location) "venv_win\Scripts\python.exe"
if (-not (Test-Path $PythonExe)) {
  $PythonExe = "python"
}

function Invoke-PythonStep([string]$step, [string[]]$arguments) {
  & $PythonExe @arguments
  Assert-LastExit $step
}

$season = (& $PythonExe -c "from datetime import datetime; from zoneinfo import ZoneInfo; print(datetime.now(ZoneInfo('Asia/Seoul')).year)").Trim()
Assert-LastExit "detect season"
$asOfDate = (& $PythonExe -c "from datetime import datetime; from zoneinfo import ZoneInfo; print(datetime.now(ZoneInfo('Asia/Seoul')).strftime('%Y-%m-%d'))").Trim()
Assert-LastExit "detect as-of date"
$env:KBO_SEASON = "$season"

# Crawl only the latest window from existing DB state.
Invoke-PythonStep "collector.run_auto_hitter" @("-m", "collector.run_auto_hitter", "--upsert")
Invoke-PythonStep "collector.run_auto_pitcher" @("-m", "collector.run_auto_pitcher", "--upsert")

# Sync team schedule from KBO official schedule API for current season
Invoke-PythonStep "collector.sync_team_schedule" @("-m", "collector.sync_team_schedule", "--db", "kbo_stats.db", "--season", "$season")

# Sync team standings snapshot from KBO official standings page
Invoke-PythonStep "collector.fetch_kbo_team_standings" @("-m", "collector.fetch_kbo_team_standings", "--db", "kbo_stats.db", "--season", "$season", "--source", "auto")

$range = @'
import os
import sqlite3

conn = sqlite3.connect("kbo_stats.db")
cur = conn.cursor()
cur.execute(
    "SELECT MIN(game_date), MAX(game_date) FROM hitter_game_logs WHERE substr(game_date,1,4)=?",
    (os.environ.get("KBO_SEASON", ""),),
)
row = cur.fetchone()
conn.close()
print(f"{row[0]},{row[1]}")
'@ | & $PythonExe -
Assert-LastExit "fetch season date range"
$parts = $range.Split(',')
$start = $parts[0]
$end = $parts[1]

if ([string]::IsNullOrWhiteSpace($start) -or [string]::IsNullOrWhiteSpace($end) -or
    $start -notmatch '^\d{8}$' -or $end -notmatch '^\d{8}$') {
  Write-Host "[warn] No game logs for season $season; skipping snapshots"
  exit 0
}

Invoke-PythonStep "prediction.build_hitter_snapshots" @("-m", "prediction.build_hitter_snapshots", "--db", "kbo_stats.db", "--season", "$season", "--start", "$start", "--end", "$end", "--upsert")
Invoke-PythonStep "prediction.build_hitter_season_totals" @("-m", "prediction.build_hitter_season_totals", "--db", "kbo_stats.db", "--season", "$season", "--upsert")
Invoke-PythonStep "prediction.build_pitcher_season_totals" @("-m", "prediction.build_pitcher_season_totals", "--db", "kbo_stats.db", "--season", "$season", "--upsert")
Invoke-PythonStep "prediction.orchestrate_daily" @("-m", "prediction.orchestrate_daily", "--season", "$season", "--as-of-date", "$asOfDate", "--mode", "prediction", "--replace-existing", "--skip-collect", "--skip-snapshot")

$ErrorActionPreference = 'Stop'
Set-Location (Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) "..")

function Assert-LastExit([string]$step) {
  if ($LASTEXITCODE -ne 0) {
    throw "$step failed with exit code $LASTEXITCODE"
  }
}

# Crawl: auto-detect season start from KBO schedule (current KST year)
python -m collector.run_range_hitter --auto-start --upsert
Assert-LastExit "collector.run_range_hitter"
python -m collector.run_range_pitcher --auto-start --upsert
Assert-LastExit "collector.run_range_pitcher"

# Snapshot range from existing logs for current season (KST year)
$season = (python -c "from datetime import datetime; from zoneinfo import ZoneInfo; print(datetime.now(ZoneInfo('Asia/Seoul')).year)").Trim()
$env:KBO_SEASON = "$season"

# Sync team schedule from KBO official schedule API for current season
python -m collector.sync_team_schedule --db kbo_stats.db --season $season
Assert-LastExit "collector.sync_team_schedule"

# Sync team standings snapshot from KBO official standings page
python -m collector.fetch_kbo_team_standings --db kbo_stats.db --season $season --source auto
Assert-LastExit "collector.fetch_kbo_team_standings"

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
'@ | python -
Assert-LastExit "fetch season date range"
$parts = $range.Split(',')
$start = $parts[0]
$end = $parts[1]

if ([string]::IsNullOrWhiteSpace($start) -or [string]::IsNullOrWhiteSpace($end) -or
    $start -notmatch '^\d{8}$' -or $end -notmatch '^\d{8}$') {
  Write-Host "[warn] No game logs for season $season; skipping snapshots"
  exit 0
}

python -m prediction.build_hitter_snapshots --db kbo_stats.db --season $season --start $start --end $end --upsert
Assert-LastExit "prediction.build_hitter_snapshots"
python -m prediction.build_hitter_season_totals --db kbo_stats.db --season $season --upsert
Assert-LastExit "prediction.build_hitter_season_totals"
python -m prediction.build_pitcher_season_totals --db kbo_stats.db --season $season --upsert
Assert-LastExit "prediction.build_pitcher_season_totals"

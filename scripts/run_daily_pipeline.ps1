$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $MyInvocation.MyCommand.Path)\..

# Crawl: auto-detect season start from KBO schedule (current KST year)
python run_range_hitter.py --auto-start --upsert

# Snapshot range from existing logs for current season (KST year)
$season = python -c "from datetime import datetime; from zoneinfo import ZoneInfo; print(datetime.now(ZoneInfo('Asia/Seoul')).year)"
$range = python -c "import sqlite3; conn=sqlite3.connect('kbo_stats.db'); cur=conn.cursor(); cur.execute(\"SELECT MIN(game_date), MAX(game_date) FROM hitter_game_logs WHERE substr(game_date,1,4)=?\", (str($season),)); row=cur.fetchone(); conn.close(); print(f'{row[0]},{row[1]}')"
$parts = $range.Split(',')
$start = $parts[0]
$end = $parts[1]

if ([string]::IsNullOrWhiteSpace($start) -or [string]::IsNullOrWhiteSpace($end)) {
  Write-Host "[warn] No game logs for season $season; skipping snapshots"
  exit 0
}

python build_hitter_snapshots.py --db kbo_stats.db --season $season --start $start --end $end --upsert

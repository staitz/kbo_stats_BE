$ErrorActionPreference = 'Stop'
Set-Location (Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) "..")

function Assert-LastExit([string]$step) {
  if ($LASTEXITCODE -ne 0) {
    throw "$step failed with exit code $LASTEXITCODE"
  }
}

$season = (
@'
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

current_year = datetime.now(ZoneInfo("Asia/Seoul")).year
conn = sqlite3.connect("kbo_stats.db")
cur = conn.cursor()
row = cur.execute(
    """
    SELECT MAX(CAST(substr(game_date, 1, 4) AS INTEGER))
    FROM hitter_game_logs
    WHERE CAST(substr(game_date, 1, 4) AS INTEGER) <= ?
    """,
    (current_year,),
).fetchone()
conn.close()
print(int(row[0]) if row and row[0] else current_year)
'@ | python -
).Trim()
Assert-LastExit "resolve season"

$asOfDate = (
@'
from datetime import datetime
from zoneinfo import ZoneInfo
print(datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d"))
'@ | python -
).Trim()
Assert-LastExit "resolve as-of date"

python -m prediction.mvp_pipeline.run_daily_mvp --season $season --as-of-date $asOfDate --replace-existing
Assert-LastExit "prediction.mvp_pipeline.run_daily_mvp"

Write-Host "Daily MVP pipeline completed for season=$season as_of_date=$asOfDate"

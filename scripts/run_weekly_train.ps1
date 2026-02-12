$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $MyInvocation.MyCommand.Path)\..

# Rebuild training rows for previous season (KST year-1 default)
python build_hitter_training_set.py --db kbo_stats.db --upsert

# Compute val_after as 80% time split based on KST year-1
$valAfter = python -c "from datetime import datetime; from zoneinfo import ZoneInfo; import sqlite3; season = datetime.now(ZoneInfo('Asia/Seoul')).year - 1; conn=sqlite3.connect('kbo_stats.db'); cur=conn.cursor(); cur.execute(\"SELECT DISTINCT as_of_date FROM hitter_training_rows WHERE train_season = ? ORDER BY as_of_date\", (season,)); dates=[r[0] for r in cur.fetchall()]; conn.close(); idx=max(int(len(dates)*0.8)-1,0) if dates else -1; print(dates[idx] if dates else '')"

if ([string]::IsNullOrWhiteSpace($valAfter)) {
  Write-Host '[warn] No training rows; skipping model training'
  exit 1
}

python train_hitter_models.py --db kbo_stats.db --model-dir models --val-after $valAfter

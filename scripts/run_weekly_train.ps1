$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $MyInvocation.MyCommand.Path)\..\

function Assert-LastExit([string]$step) {
  if ($LASTEXITCODE -ne 0) {
    throw "$step failed with exit code $LASTEXITCODE"
  }
}

# Resolve train season: use KST current year (in-season training)
# or override with $env:KBO_TRAIN_SEASON if set.
$trainSeason = if ($env:KBO_TRAIN_SEASON) {
  $env:KBO_TRAIN_SEASON
} else {
  (
  @'
from datetime import datetime
from zoneinfo import ZoneInfo
print(datetime.now(ZoneInfo("Asia/Seoul")).year)
'@ | python -
  ).Trim()
}
Assert-LastExit "resolve train season"

Write-Host "Training mvp_pipeline hitter models for season=$trainSeason ..."

# Train hitter models: OPS + HR + WAR (LightGBM)
# Reads directly from hitter_game_logs — no snapshot prep needed.
python -m prediction.mvp_pipeline.train --season $trainSeason
Assert-LastExit "prediction.mvp_pipeline.train"

Write-Host "Weekly training completed for season=$trainSeason"

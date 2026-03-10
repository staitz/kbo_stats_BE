$ErrorActionPreference = 'Stop'
Set-Location (Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) "..")

function Assert-LastExit([string]$step) {
  if ($LASTEXITCODE -ne 0) {
    throw "$step failed with exit code $LASTEXITCODE"
  }
}

# ---------------------------------------------------------------------------
# Projection Batch — Pre-season hitter projection
#
# Purpose:
#   Generate pre-season hitter projections using previous-year game log data.
#   Results are stored in hitter_predictions with prediction_mode='projection'.
#
# When to run:
#   Before the season starts (e.g., January/February), or whenever you want
#   a "no in-season data" baseline projection.
#
# Difference from run_daily_mvp.ps1:
#   --mode projection  → blend_weight forced to 0 (model_source = PROJECTION_ONLY)
#   --season           → the target season being projected INTO (e.g., 2026)
#   The underlying data read is from the PREVIOUS season's game logs.
# ---------------------------------------------------------------------------

# Target season: the season we are projecting FOR (default: KST current year)
$targetSeason = if ($env:KBO_PROJECTION_SEASON) {
  $env:KBO_PROJECTION_SEASON
} else {
  (
  @'
from datetime import datetime
from zoneinfo import ZoneInfo
print(datetime.now(ZoneInfo("Asia/Seoul")).year)
'@ | python -
  ).Trim()
}
Assert-LastExit "resolve target season"

# as_of_date for projection: use a fixed pre-season reference date or today
# Override with $env:KBO_PROJECTION_DATE if desired.
$asOfDate = if ($env:KBO_PROJECTION_DATE) {
  $env:KBO_PROJECTION_DATE
} else {
  (
  @'
from datetime import datetime
from zoneinfo import ZoneInfo
# Default: March 1st of the target season (canonical pre-season date)
import sys
year = int(sys.argv[1]) if len(sys.argv) > 1 else datetime.now(ZoneInfo("Asia/Seoul")).year
print(f"{year}-03-01")
'@ | python - $targetSeason
  ).Trim()
}
Assert-LastExit "resolve projection as-of date"

Write-Host "Running pre-season PROJECTION for season=$targetSeason as_of_date=$asOfDate ..."

python -m prediction.mvp_pipeline.run_daily_mvp `
  --season $targetSeason `
  --as-of-date $asOfDate `
  --mode projection `
  --replace-existing
Assert-LastExit "prediction.mvp_pipeline.run_daily_mvp (projection)"

Write-Host "Projection completed for season=$targetSeason as_of_date=$asOfDate"
Write-Host "  DB: hitter_predictions WHERE prediction_mode='projection' AND season=$targetSeason"

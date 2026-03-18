@echo off
echo Starting KBO Crawler...
cd /d "%~dp0"
python collector\run_auto_hitter.py --upsert
python collector\run_auto_pitcher.py --upsert
echo Crawler finished!
pause

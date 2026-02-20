# KBO Stat Project Structure

## Folders
- `collector/`: KBO 수집, 파서, 원천 DB 적재
- `prediction/`: 스냅샷 생성, 학습셋 생성, 모델 학습, 예측
- `reporting/`: 데이터 검증/품질/리더보드 리포트
- `app/`: Streamlit 대시보드
- `be/`: Django API 서버

## Run Examples (module mode)
- 수집: `python -m collector.run_range_hitter --auto-start --upsert`
- 스냅샷: `python -m prediction.build_hitter_snapshots --db kbo_stats.db --season 2025 --start 20250322 --end 20251004 --upsert`
- 학습셋: `python -m prediction.build_hitter_training_set --db kbo_stats.db --train-season 2025 --upsert`
- 모델학습: `python -m prediction.train_hitter_models --db kbo_stats.db --train-season 2025 --model-dir models`
- 예측: `python -m prediction.predict_hitter_ml --db kbo_stats.db --season 2026 --as-of 20260328 --upsert --preview 20`
- 팀순위 수집: `python -m collector.fetch_kbo_team_standings --db kbo_stats.db --season 2025`
- KBReport 타자 split 4종 수집(홈원정/좌우투/상대팀/월별):
  - `python -m collector.fetch_kbreport_hitter_splits --db kbo_stats.db --season 2025 --player-id 2231`
- 검증리포트: `python -m reporting.verify_db --date 20251004`
- 대시보드: `streamlit run app/dashboard.py`
- API 서버: `cd be && python manage.py runserver 0.0.0.0:8000`

## STATIZ / Test Stage (new)
- 테스트용 1경기 stage 적재(2025-06-12):
  - `python -m collector.ingest_test_game_stage --db kbo_stats.db --date 20250612`
- STATIZ 선수 마스터(3번) 적재:
  - `python -m collector.ingest_statiz_players --db kbo_stats.db --source-url "<STATIZ 선수목록 URL>"`
- STATIZ 선수-팀-시즌 이력(4번) 적재:
  - `python -m collector.ingest_statiz_team_history --db kbo_stats.db --source-url "<STATIZ 이력 URL>"`
- STATIZ 선수 split/matchup(5번) 적재:
  - `python -m collector.ingest_statiz_player_splits --db kbo_stats.db --season 2025 --split-group lr_home_away --player-name "<선수명>" --source-url "<STATIZ split URL>"`

Notes:
- STATIZ 크롤링 전 robots.txt/이용약관을 확인하세요.
- 현재 파서는 URL 내 HTML table 기반이며, 페이지 구조 변경 시 컬럼 매핑 보정이 필요합니다.

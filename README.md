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
- 검증리포트: `python -m reporting.verify_db --date 20251004`
- 대시보드: `streamlit run app/dashboard.py`
- API 서버: `cd be && python manage.py runserver 0.0.0.0:8000`

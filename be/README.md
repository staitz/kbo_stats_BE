# Django Backend (`be`)

## Run
```powershell
cd be
python manage.py runserver 0.0.0.0:8000
```

## API
- `GET /api/health`
- `GET /api/leaderboard?season=2025&metric=OPS&limit=20&min_pa=100`
- `GET /api/predictions/latest?season=2025`
- `GET /api/players/search?season=2025&q=노시환`

## Notes
- DB is wired to project-root `kbo_stats.db`.
- This is MVP query API (read-only) for frontend integration.

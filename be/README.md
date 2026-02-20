# Django Backend (`be`)

## Run
```powershell
cd be
python manage.py runserver 0.0.0.0:8000
```

## API
- `GET /api/health`
- `GET /api/standings?season=2026` (fallbacks to latest previous season when requested season has no rows)
- `GET /api/home/summary?season=2025&min_pa=100`
- `GET /api/leaderboard?season=2025&metric=OPS&limit=20&min_pa=100`
- `GET /api/predictions/latest?season=2025`
- `GET /api/players/search?season=2025&q=노시환&limit=30`
- `GET /api/players/{player_name}?season=2025`
- `GET /api/players/compare?season=2025&names=노시환,김도영`
- `GET /api/teams/{team}?season=2025`

## Notes
- DB is wired to project-root `kbo_stats.db`.
- This is hitter-only MVP query API (read-only) for frontend integration.

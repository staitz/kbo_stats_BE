from django.db import connection
from django.test import TestCase


class ApiEndpointsTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS hitter_season_totals (
                    season INTEGER NOT NULL,
                    team TEXT NOT NULL,
                    player_name TEXT NOT NULL,
                    games INTEGER NOT NULL DEFAULT 0,
                    PA INTEGER NOT NULL DEFAULT 0,
                    AB INTEGER NOT NULL DEFAULT 0,
                    H INTEGER NOT NULL DEFAULT 0,
                    "2B" INTEGER NOT NULL DEFAULT 0,
                    "3B" INTEGER NOT NULL DEFAULT 0,
                    HR INTEGER NOT NULL DEFAULT 0,
                    TB_adj INTEGER NOT NULL DEFAULT 0,
                    RBI INTEGER NOT NULL DEFAULT 0,
                    BB INTEGER NOT NULL DEFAULT 0,
                    SO INTEGER NOT NULL DEFAULT 0,
                    HBP INTEGER NOT NULL DEFAULT 0,
                    SH INTEGER NOT NULL DEFAULT 0,
                    SF INTEGER NOT NULL DEFAULT 0,
                    SB INTEGER NOT NULL DEFAULT 0,
                    CS INTEGER NOT NULL DEFAULT 0,
                    GDP INTEGER NOT NULL DEFAULT 0,
                    AVG REAL NOT NULL DEFAULT 0,
                    OBP REAL NOT NULL DEFAULT 0,
                    SLG REAL NOT NULL DEFAULT 0,
                    OPS REAL NOT NULL DEFAULT 0
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS hitter_game_logs (
                    game_date TEXT NOT NULL,
                    game_id TEXT NOT NULL,
                    team TEXT NOT NULL,
                    player_name TEXT NOT NULL,
                    AB INTEGER NOT NULL DEFAULT 0,
                    H INTEGER NOT NULL DEFAULT 0,
                    HR INTEGER NOT NULL DEFAULT 0,
                    BB INTEGER NOT NULL DEFAULT 0,
                    SO INTEGER NOT NULL DEFAULT 0,
                    "2B" INTEGER NOT NULL DEFAULT 0,
                    "3B" INTEGER NOT NULL DEFAULT 0,
                    HBP INTEGER NOT NULL DEFAULT 0,
                    SF INTEGER NOT NULL DEFAULT 0,
                    R INTEGER NOT NULL DEFAULT 0,
                    RBI INTEGER NOT NULL DEFAULT 0,
                    TB INTEGER NOT NULL DEFAULT 0,
                    PA INTEGER NOT NULL DEFAULT 0,
                    SB INTEGER NOT NULL DEFAULT 0,
                    CS INTEGER NOT NULL DEFAULT 0,
                    GDP INTEGER NOT NULL DEFAULT 0,
                    SH INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS pitcher_game_logs (
                    game_date TEXT NOT NULL,
                    game_id TEXT NOT NULL,
                    team TEXT NOT NULL,
                    player_name TEXT NOT NULL,
                    W INTEGER NOT NULL DEFAULT 0,
                    L INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS hitter_daily_snapshots (
                    season INTEGER NOT NULL,
                    as_of_date TEXT NOT NULL,
                    team TEXT NOT NULL,
                    player_name TEXT NOT NULL,
                    games INTEGER NOT NULL DEFAULT 0,
                    PA INTEGER NOT NULL DEFAULT 0,
                    AB INTEGER NOT NULL DEFAULT 0,
                    H INTEGER NOT NULL DEFAULT 0,
                    HR INTEGER NOT NULL DEFAULT 0,
                    OPS REAL NOT NULL DEFAULT 0,
                    OPS_7 REAL NOT NULL DEFAULT 0,
                    OPS_14 REAL NOT NULL DEFAULT 0
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS hitter_predictions (
                    season INTEGER NOT NULL,
                    as_of_date TEXT NOT NULL,
                    team TEXT NOT NULL,
                    player_name TEXT NOT NULL,
                    predicted_hr_final REAL NOT NULL DEFAULT 0,
                    predicted_ops_final REAL NOT NULL DEFAULT 0,
                    predicted_war_final REAL NOT NULL DEFAULT 0,
                    predicted_avg_final REAL NOT NULL DEFAULT 0,
                    confidence_level TEXT NOT NULL DEFAULT 'LOW',
                    confidence_score REAL NOT NULL DEFAULT 0,
                    model_season INTEGER NOT NULL DEFAULT 0,
                    model_version TEXT NOT NULL DEFAULT 'v1',
                    created_at TEXT NOT NULL DEFAULT '',
                    pa_to_date REAL NOT NULL DEFAULT 0,
                    blend_weight REAL NOT NULL DEFAULT 0,
                    model_source TEXT NOT NULL DEFAULT 'MODEL_ONLY'
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS team_standings (
                    season INTEGER NOT NULL,
                    as_of_date TEXT NOT NULL,
                    rank INTEGER NOT NULL,
                    team TEXT NOT NULL,
                    games INTEGER NOT NULL DEFAULT 0,
                    wins INTEGER NOT NULL DEFAULT 0,
                    losses INTEGER NOT NULL DEFAULT 0,
                    draws INTEGER NOT NULL DEFAULT 0,
                    win_pct REAL NOT NULL DEFAULT 0,
                    gb REAL NOT NULL DEFAULT 0,
                    recent_10 TEXT,
                    streak TEXT,
                    home_record TEXT,
                    away_record TEXT,
                    source TEXT NOT NULL DEFAULT 'KBO_OFFICIAL',
                    source_url TEXT,
                    collected_at TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (season, as_of_date, team)
                )
                """
            )

            cursor.execute("DELETE FROM hitter_season_totals")
            cursor.execute("DELETE FROM hitter_game_logs")
            cursor.execute("DELETE FROM pitcher_game_logs")
            cursor.execute("DELETE FROM hitter_daily_snapshots")
            cursor.execute("DELETE FROM hitter_predictions")
            cursor.execute("DELETE FROM team_standings")

            cursor.executemany(
                """
                INSERT INTO hitter_season_totals
                (season, team, player_name, games, PA, AB, H, "2B", "3B", HR, TB_adj, RBI, BB, SO, HBP, SH, SF, SB, CS, GDP, AVG, OBP, SLG, OPS)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (2025, "KIA", "홍길동", 100, 420, 380, 120, 20, 2, 18, 198, 75, 30, 80, 3, 2, 4, 5, 2, 8, 0.3158, 0.3711, 0.5211, 0.8922),
                    (2025, "LG", "김테스트", 98, 410, 370, 110, 18, 1, 22, 196, 82, 35, 70, 4, 1, 3, 8, 1, 6, 0.2973, 0.3636, 0.5297, 0.8933),
                    (2025, "KIA", "이샘플", 88, 320, 290, 75, 10, 0, 8, 109, 41, 24, 66, 2, 1, 2, 2, 1, 4, 0.2586, 0.3191, 0.3759, 0.6950),
                ],
            )
            cursor.executemany(
                """
                INSERT INTO hitter_game_logs
                (game_date, game_id, team, player_name, AB, H, HR, BB, SO, "2B", "3B", HBP, SF, R, RBI, TB, PA, SB, CS, GDP, SH)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    ("20250322", "20250322HTLG0", "KIA", "홍길동", 4, 2, 1, 1, 1, 0, 0, 0, 0, 2, 3, 6, 5, 0, 0, 0, 0),
                    ("20250410", "20250410HTLG0", "KIA", "홍길동", 4, 1, 0, 0, 1, 1, 0, 0, 0, 0, 1, 2, 4, 0, 0, 0, 0),
                    ("20250410", "20250410HTLG0", "LG", "김테스트", 4, 2, 1, 0, 1, 0, 0, 0, 0, 1, 2, 5, 4, 0, 0, 0, 0),
                ],
            )
            cursor.executemany(
                """
                INSERT INTO hitter_daily_snapshots
                (season, as_of_date, team, player_name, games, PA, AB, H, HR, OPS, OPS_7, OPS_14)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (2025, "20250322", "KIA", "홍길동", 1, 5, 4, 2, 1, 1.3000, 1.3000, 1.3000),
                    (2025, "20250410", "KIA", "홍길동", 2, 9, 8, 3, 1, 0.9500, 0.9000, 0.9200),
                ],
            )
            cursor.executemany(
                """
                INSERT INTO hitter_predictions
                (season, as_of_date, team, player_name, predicted_hr_final, predicted_ops_final,
                 confidence_level, confidence_score, model_season, model_version, created_at, pa_to_date, blend_weight, model_source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (2025, "20250410", "KIA", "홍길동", 20.0, 0.9000, "HIGH", 0.9, 2024, "v1", "2026-01-01T00:00:00Z", 180, 0.75, "BLENDED"),
                    (2025, "20250410", "LG", "김테스트", 24.0, 0.9100, "HIGH", 0.9, 2024, "v1", "2026-01-01T00:00:00Z", 200, 0.80, "BLENDED"),
                ],
            )
            cursor.executemany(
                """
                INSERT INTO team_standings
                (season, as_of_date, rank, team, games, wins, losses, draws, win_pct, gb, recent_10, streak, home_record, away_record, source, source_url, collected_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'KBO_OFFICIAL_KR', 'https://www.koreabaseball.com/record/teamrank/teamrankdaily.aspx', '2026-01-01T00:00:00Z')
                """,
                [
                    (2025, "20251004", 1, "KIA", 144, 82, 58, 4, 0.586, 0.0, "6승4패", "2연승", "40-30-2", "42-28-2"),
                    (2025, "20251004", 2, "LG", 144, 80, 60, 4, 0.571, 2.0, "5승5패", "1연패", "41-31-0", "39-29-4"),
                ],
            )

    def test_health(self):
        res = self.client.get("/api/health/")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["status"], "ok")

    def test_home_summary(self):
        res = self.client.get("/api/home/summary/?season=2025")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["season"], 2025)
        self.assertIn("leaderboards", data)
        self.assertIn("ops_top5", data["leaderboards"])
        self.assertIn("standings_preview", data)
        self.assertIn("min_pa_policy", data)

    def test_leaderboard(self):
        res = self.client.get("/api/leaderboard/?season=2025&metric=OPS&limit=2")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["metric"], "OPS")
        self.assertEqual(len(data["rows"]), 2)
        self.assertGreaterEqual(data["rows"][0]["OPS"], data["rows"][1]["OPS"])
        self.assertIn("effective_min_pa", data)
        self.assertIn("requested_min_pa", data)

    def test_predictions_latest(self):
        res = self.client.get("/api/predictions/latest/?season=2025")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["latest_date"], "20250410")
        self.assertTrue(len(data["rows"]) >= 1)

    def test_player_search(self):
        res = self.client.get("/api/players/search/?season=2025&q=홍")
        self.assertEqual(res.status_code, 200)
        rows = res.json()["rows"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["player_name"], "홍길동")

    def test_player_detail(self):
        res = self.client.get("/api/players/홍길동/?season=2025")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["player_name"], "홍길동")
        self.assertIn("season_rows", data)
        self.assertIn("monthly_splits", data)
        self.assertIn("vs_team_splits", data)
        self.assertIn("recent_game_logs", data)
        self.assertIn("kbreport_splits", data)
        self.assertIn("latest_prediction", data)
        self.assertIn("predicted_hits_final", data["latest_prediction"])
        self.assertIn("predicted_rbi_final", data["latest_prediction"])
        self.assertIn("golden_glove_probability", data["latest_prediction"])
        self.assertIn("mvp_probability", data["latest_prediction"])

    def test_player_detail_recent_n(self):
        res = self.client.get("/api/players/홍길동/?season=2025&recent_n=1")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["recent_n"], 1)
        self.assertLessEqual(len(data["recent_game_logs"]), 1)

    def test_player_detail_not_found(self):
        res = self.client.get("/api/players/없는선수/?season=2025")
        self.assertEqual(res.status_code, 404)
        self.assertEqual(res.json()["error"], "player_not_found")

    def test_team_detail(self):
        res = self.client.get("/api/teams/KIA/?season=2025")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["team"], "KIA")
        self.assertIn("leaders", data)
        self.assertIn("effective_min_pa", data)
        self.assertIn("requested_min_pa", data)

    def test_player_compare(self):
        res = self.client.get("/api/players/compare/?season=2025&names=홍길동,김테스트")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(len(data["rows"]), 2)

    def test_player_compare_requires_two_names(self):
        res = self.client.get("/api/players/compare/?season=2025&names=홍길동")
        self.assertEqual(res.status_code, 400)
        self.assertEqual(res.json()["error"], "at_least_two_names_required")

    def test_games_by_date(self):
        res = self.client.get("/api/games/?date=20250410")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertTrue(len(data["rows"]) >= 1)
        self.assertEqual(data["rows"][0]["game_id"], "20250410HTLG0")

    def test_game_boxscore(self):
        res = self.client.get("/api/games/20250410HTLG0/boxscore/")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["game_id"], "20250410HTLG0")
        self.assertTrue(len(data["hitter_rows"]) >= 1)

    def test_standings_match(self):
        res = self.client.get("/api/standings/?season=2025")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["requested_season"], 2025)
        self.assertEqual(data["effective_season"], 2025)
        self.assertEqual(data["mode"], "SEASON_MATCH")
        self.assertEqual(len(data["rows"]), 2)

    def test_standings_preseason_fallback(self):
        res = self.client.get("/api/standings/?season=2026")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["requested_season"], 2026)
        self.assertEqual(data["effective_season"], 2025)
        self.assertEqual(data["mode"], "PRESEASON_FALLBACK")

    def test_standings_use_freshest_available_log_source(self):
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO pitcher_game_logs (game_date, game_id, team, player_name, W, L)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                [
                    ("20250322", "20250322HTLG0", "KIA", "Pitcher A", 1, 0),
                    ("20250322", "20250322HTLG0", "LG", "Pitcher B", 0, 1),
                ],
            )

        res = self.client.get("/api/standings/?season=2025")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["as_of_date"], "20250410")
        self.assertEqual(len(data["rows"]), 2)
        self.assertEqual(data["rows"][0]["team"], "LG")
        self.assertEqual(data["rows"][0]["wins"], 1)


class ApiErrorHandlingTest(TestCase):
    def test_player_compare_error_format(self):
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS hitter_season_totals (
                    season INTEGER NOT NULL,
                    team TEXT NOT NULL,
                    player_name TEXT NOT NULL,
                    games INTEGER NOT NULL DEFAULT 0,
                    PA INTEGER NOT NULL DEFAULT 0,
                    AB INTEGER NOT NULL DEFAULT 0,
                    H INTEGER NOT NULL DEFAULT 0,
                    "2B" INTEGER NOT NULL DEFAULT 0,
                    "3B" INTEGER NOT NULL DEFAULT 0,
                    HR INTEGER NOT NULL DEFAULT 0,
                    TB_adj INTEGER NOT NULL DEFAULT 0,
                    RBI INTEGER NOT NULL DEFAULT 0,
                    BB INTEGER NOT NULL DEFAULT 0,
                    SO INTEGER NOT NULL DEFAULT 0,
                    HBP INTEGER NOT NULL DEFAULT 0,
                    SH INTEGER NOT NULL DEFAULT 0,
                    SF INTEGER NOT NULL DEFAULT 0,
                    SB INTEGER NOT NULL DEFAULT 0,
                    CS INTEGER NOT NULL DEFAULT 0,
                    GDP INTEGER NOT NULL DEFAULT 0,
                    AVG REAL NOT NULL DEFAULT 0,
                    OBP REAL NOT NULL DEFAULT 0,
                    SLG REAL NOT NULL DEFAULT 0,
                    OPS REAL NOT NULL DEFAULT 0
                )
                """
            )
        res = self.client.get("/api/players/compare/?season=2025&names=onlyone")
        self.assertEqual(res.status_code, 400)
        data = res.json()
        self.assertEqual(data["error"], "at_least_two_names_required")
        self.assertIn("detail", data)

    def test_missing_required_table_returns_non_500(self):
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS hitter_season_totals")
        res = self.client.get("/api/leaderboard/?season=2025")
        self.assertEqual(res.status_code, 503)
        data = res.json()
        self.assertEqual(data["error"], "missing_table")
        self.assertIn("detail", data)

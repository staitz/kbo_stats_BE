import sqlite3


def init_statiz_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS game_results_stage (
            game_id TEXT PRIMARY KEY,
            game_date TEXT NOT NULL,
            home_team TEXT,
            away_team TEXT,
            home_score INTEGER,
            away_score INTEGER,
            source TEXT NOT NULL DEFAULT 'STATIZ',
            source_url TEXT,
            collected_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS innings_scores_stage (
            game_id TEXT NOT NULL,
            inning_no INTEGER NOT NULL,
            away_runs INTEGER,
            home_runs INTEGER,
            source TEXT NOT NULL DEFAULT 'STATIZ',
            source_url TEXT,
            collected_at TEXT NOT NULL,
            PRIMARY KEY (game_id, inning_no)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS statiz_players (
            player_id TEXT PRIMARY KEY,
            player_name TEXT NOT NULL,
            birth_date TEXT,
            position TEXT,
            bats_throws TEXT,
            debut_year TEXT,
            salary_info TEXT,
            fa_info TEXT,
            source TEXT NOT NULL DEFAULT 'STATIZ',
            source_url TEXT,
            collected_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS statiz_player_team_history (
            player_id TEXT NOT NULL,
            player_name TEXT NOT NULL,
            season INTEGER NOT NULL,
            team TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'STATIZ',
            source_url TEXT,
            collected_at TEXT NOT NULL,
            PRIMARY KEY (player_id, season, team)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS statiz_player_splits (
            player_id TEXT NOT NULL,
            player_name TEXT NOT NULL,
            season INTEGER NOT NULL,
            split_group TEXT NOT NULL,
            split_key TEXT NOT NULL,
            games INTEGER,
            PA INTEGER,
            AB INTEGER,
            H INTEGER,
            HR INTEGER,
            BB INTEGER,
            SO INTEGER,
            AVG REAL,
            OBP REAL,
            SLG REAL,
            OPS REAL,
            source TEXT NOT NULL DEFAULT 'STATIZ',
            source_url TEXT,
            collected_at TEXT NOT NULL,
            PRIMARY KEY (player_id, season, split_group, split_key)
        )
        """
    )
    conn.commit()

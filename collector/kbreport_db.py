import sqlite3


def init_kbreport_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS kbreport_hitter_splits (
            season INTEGER NOT NULL,
            kbreport_player_id TEXT NOT NULL,
            player_name TEXT NOT NULL,
            split_group TEXT NOT NULL,
            split_key TEXT NOT NULL,
            split_label TEXT,
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
            source TEXT NOT NULL DEFAULT 'KBREPORT',
            source_url TEXT,
            collected_at TEXT NOT NULL,
            PRIMARY KEY (season, kbreport_player_id, split_group, split_key)
        )
        """
    )
    conn.commit()

import sqlite3
from typing import Any, Dict, Iterable, List, Tuple


DB_PATH = "kbo_stats.db"


REQUIRED_COLUMNS = {
    "SH": "INTEGER NOT NULL DEFAULT 0",
    "2B": "INTEGER NOT NULL DEFAULT 0",
    "3B": "INTEGER NOT NULL DEFAULT 0",
    "HBP": "INTEGER NOT NULL DEFAULT 0",
    "SF": "INTEGER NOT NULL DEFAULT 0",
    "R": "INTEGER NOT NULL DEFAULT 0",
    "RBI": "INTEGER NOT NULL DEFAULT 0",
    "TB": "INTEGER NOT NULL DEFAULT 0",
    "PA": "INTEGER NOT NULL DEFAULT 0",
    "SB": "INTEGER NOT NULL DEFAULT 0",
    "CS": "INTEGER NOT NULL DEFAULT 0",
    "GDP": "INTEGER NOT NULL DEFAULT 0",
}


def init_db(conn: sqlite3.Connection) -> None:
    # 기본 테이블 생성
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS hitter_game_logs (
            game_date TEXT NOT NULL,
            game_id TEXT NOT NULL,
            team TEXT NOT NULL,
            player_name TEXT NOT NULL,
            AB INTEGER NOT NULL,
            H INTEGER NOT NULL,
            HR INTEGER NOT NULL,
            BB INTEGER NOT NULL,
            SO INTEGER NOT NULL,
            UNIQUE (game_id, team, player_name)
        )
        """
    )
    conn.commit()


def migrate_columns(conn: sqlite3.Connection) -> None:
    # 기존 테이블에 신규 컬럼을 안전하게 추가
    existing = {
        row[1] for row in conn.execute("PRAGMA table_info(hitter_game_logs)").fetchall()
    }
    for col, col_def in REQUIRED_COLUMNS.items():
        if col in existing:
            continue
        # 숫자로 시작하는 컬럼명(예: 2B, 3B)은 반드시 따옴표로 감싸야 한다
        safe_col = f'"{col}"' if col[0].isdigit() else col
        conn.execute(f"ALTER TABLE hitter_game_logs ADD COLUMN {safe_col} {col_def}")
    conn.commit()


def _row_to_values(row: Dict[str, Any]) -> Tuple[Any, ...]:
    return (
        row.get("game_date"),
        row.get("game_id"),
        row.get("team"),
        row.get("player_name"),
        int(row.get("AB", 0)),
        int(row.get("H", 0)),
        int(row.get("HR", 0)),
        int(row.get("BB", 0)),
        int(row.get("SO", 0)),
        int(row.get("SH", 0)),
        int(row.get("2B", 0)),
        int(row.get("3B", 0)),
        int(row.get("HBP", 0)),
        int(row.get("SF", 0)),
        int(row.get("R", 0)),
        int(row.get("RBI", 0)),
        int(row.get("TB", 0)),
        int(row.get("PA", 0)),
        int(row.get("SB", 0)),
        int(row.get("CS", 0)),
        int(row.get("GDP", 0)),
    )


def insert_rows(
    conn: sqlite3.Connection,
    rows: Iterable[Dict[str, Any]],
    upsert: bool = False,
) -> int:
    values: List[Tuple[Any, ...]] = [_row_to_values(r) for r in rows]
    if not values:
        return 0

    cursor = conn.cursor()
    if not upsert:
        cursor.executemany(
        """
        INSERT OR IGNORE INTO hitter_game_logs
        (game_date, game_id, team, player_name, AB, H, HR, BB, SO, SH, "2B", "3B",
         HBP, SF, R, RBI, TB, PA, SB, CS, GDP)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        values,
    )
        conn.commit()
        return cursor.rowcount

    # SQLite UPSERT: 기존 row가 있으면 확장 컬럼까지 업데이트
    cursor.executemany(
        """
        INSERT INTO hitter_game_logs
        (game_date, game_id, team, player_name, AB, H, HR, BB, SO, SH, "2B", "3B",
         HBP, SF, R, RBI, TB, PA, SB, CS, GDP)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_id, team, player_name) DO UPDATE SET
            AB=excluded.AB,
            H=excluded.H,
            HR=excluded.HR,
            BB=excluded.BB,
            SO=excluded.SO,
            SH=excluded.SH,
            "2B"=excluded."2B",
            "3B"=excluded."3B",
            HBP=excluded.HBP,
            SF=excluded.SF,
            R=excluded.R,
            RBI=excluded.RBI,
            TB=excluded.TB,
            PA=excluded.PA,
            SB=excluded.SB,
            CS=excluded.CS,
            GDP=excluded.GDP
        """,
        values,
    )
    conn.commit()
    return cursor.rowcount

import argparse
import sqlite3
from typing import Dict, List


BB_WOBA_WEIGHT = 0.69
HBP_WOBA_WEIGHT = 0.72
SINGLE_WOBA_WEIGHT = 0.89
DOUBLE_WOBA_WEIGHT = 1.27
TRIPLE_WOBA_WEIGHT = 1.62
HR_WOBA_WEIGHT = 2.10
WOBA_SCALE_FALLBACK = 1.25
REP_RUNS_PER_PA = 0.03
RUNS_PER_WIN = 10.0


def safe_col(name: str) -> str:
    if not name:
        return name
    if name[0].isdigit() or "-" in name or " " in name:
        return f'"{name}"'
    return name


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
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
            OPS REAL NOT NULL DEFAULT 0,
            wOBA REAL NOT NULL DEFAULT 0,
            batter_war REAL NOT NULL DEFAULT 0,
            WAR REAL NOT NULL DEFAULT 0,
            UNIQUE (season, team, player_name)
        )
        """
    )
    existing = {row[1] for row in conn.execute("PRAGMA table_info(hitter_season_totals)").fetchall()}
    required = {
        "SH": "INTEGER NOT NULL DEFAULT 0",
        "wOBA": "REAL NOT NULL DEFAULT 0",
        "batter_war": "REAL NOT NULL DEFAULT 0",
        "WAR": "REAL NOT NULL DEFAULT 0",
    }
    for col, col_def in required.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE hitter_season_totals ADD COLUMN {safe_col(col)} {col_def}")
    conn.commit()


def _tb_adj(h: int, doubles: int, triples: int, hr: int, raw_tb: int) -> int:
    singles = max(h - doubles - triples - hr, 0)
    derived = singles + (2 * doubles) + (3 * triples) + (4 * hr)
    return raw_tb if raw_tb > 0 else derived


def _rate(num: float, den: float) -> float:
    return num / den if den > 0 else 0.0


def _woba(h: int, doubles: int, triples: int, hr: int, bb: int, hbp: int, ab: int, sf: int) -> float:
    singles = max(h - doubles - triples - hr, 0)
    numerator = (
        (BB_WOBA_WEIGHT * bb)
        + (HBP_WOBA_WEIGHT * hbp)
        + (SINGLE_WOBA_WEIGHT * singles)
        + (DOUBLE_WOBA_WEIGHT * doubles)
        + (TRIPLE_WOBA_WEIGHT * triples)
        + (HR_WOBA_WEIGHT * hr)
    )
    denominator = ab + bb + hbp + sf
    return _rate(numerator, denominator)


def _batter_war(woba: float, lg_woba: float, pa: int) -> float:
    # BsR, FieldingRuns, and PositionalAdjustment are not available in the current DB.
    # TODO: replace the zero placeholders once those source columns are ingested.
    wraa = ((woba - lg_woba) / WOBA_SCALE_FALLBACK) * pa if WOBA_SCALE_FALLBACK else 0.0
    rar = wraa + 0.0 + 0.0 + 0.0 + (pa * REP_RUNS_PER_PA)
    return round(rar / RUNS_PER_WIN, 2)


def _fetch_player_rows(conn: sqlite3.Connection, season: int, team: str | None) -> List[sqlite3.Row]:
    params: List[object] = [str(season)]
    where = ["substr(game_date, 1, 4) = ?"]
    if team:
        where.append("team = ?")
        params.append(team)
    sql = f"""
    SELECT
        ? AS season,
        team,
        player_name,
        COUNT(DISTINCT game_id) AS games,
        COALESCE(SUM(PA), 0) AS PA,
        COALESCE(SUM(AB), 0) AS AB,
        COALESCE(SUM(H), 0) AS H,
        COALESCE(SUM("2B"), 0) AS "2B",
        COALESCE(SUM("3B"), 0) AS "3B",
        COALESCE(SUM(HR), 0) AS HR,
        COALESCE(SUM(TB), 0) AS TB,
        COALESCE(SUM(RBI), 0) AS RBI,
        COALESCE(SUM(BB), 0) AS BB,
        COALESCE(SUM(SO), 0) AS SO,
        COALESCE(SUM(HBP), 0) AS HBP,
        COALESCE(SUM(SH), 0) AS SH,
        COALESCE(SUM(SF), 0) AS SF,
        COALESCE(SUM(SB), 0) AS SB,
        COALESCE(SUM(CS), 0) AS CS,
        COALESCE(SUM(GDP), 0) AS GDP
    FROM hitter_game_logs
    WHERE {' AND '.join(where)}
    GROUP BY team, player_name
    """
    return conn.execute(sql, [season] + params).fetchall()


def _fetch_league_row(conn: sqlite3.Connection, season: int) -> sqlite3.Row:
    return conn.execute(
        """
        SELECT
            COALESCE(SUM(PA), 0) AS PA,
            COALESCE(SUM(AB), 0) AS AB,
            COALESCE(SUM(H), 0) AS H,
            COALESCE(SUM("2B"), 0) AS "2B",
            COALESCE(SUM("3B"), 0) AS "3B",
            COALESCE(SUM(HR), 0) AS HR,
            COALESCE(SUM(BB), 0) AS BB,
            COALESCE(SUM(HBP), 0) AS HBP,
            COALESCE(SUM(SF), 0) AS SF
        FROM hitter_game_logs
        WHERE substr(game_date, 1, 4) = ?
        """,
        (str(season),),
    ).fetchone()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build hitter season totals.")
    parser.add_argument("--db", default="db.sqlite3")
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument("--team")
    parser.add_argument("--upsert", action="store_true")
    parser.add_argument("--preview", type=int, default=0)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    if not conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='hitter_game_logs'"
    ).fetchone():
        raise SystemExit("Missing table: hitter_game_logs")

    ensure_table(conn)
    player_rows = _fetch_player_rows(conn, args.season, args.team)
    league_row = _fetch_league_row(conn, args.season)
    lg_woba = _woba(
        h=int(league_row["H"] or 0),
        doubles=int(league_row["2B"] or 0),
        triples=int(league_row["3B"] or 0),
        hr=int(league_row["HR"] or 0),
        bb=int(league_row["BB"] or 0),
        hbp=int(league_row["HBP"] or 0),
        ab=int(league_row["AB"] or 0),
        sf=int(league_row["SF"] or 0),
    )

    rows_to_write: List[tuple] = []
    for row in player_rows:
        season = int(row["season"] or args.season)
        team = str(row["team"] or "")
        player_name = str(row["player_name"] or "")
        games = int(row["games"] or 0)
        pa = int(row["PA"] or 0)
        ab = int(row["AB"] or 0)
        h = int(row["H"] or 0)
        doubles = int(row["2B"] or 0)
        triples = int(row["3B"] or 0)
        hr = int(row["HR"] or 0)
        raw_tb = int(row["TB"] or 0)
        tb_adj = _tb_adj(h, doubles, triples, hr, raw_tb)
        rbi = int(row["RBI"] or 0)
        bb = int(row["BB"] or 0)
        so = int(row["SO"] or 0)
        hbp = int(row["HBP"] or 0)
        sh = int(row["SH"] or 0)
        sf = int(row["SF"] or 0)
        sb = int(row["SB"] or 0)
        cs = int(row["CS"] or 0)
        gdp = int(row["GDP"] or 0)

        avg = _rate(h, ab)
        obp = _rate(h + bb + hbp, ab + bb + hbp + sf)
        slg = _rate(tb_adj, ab)
        ops = obp + slg
        woba = _woba(h, doubles, triples, hr, bb, hbp, ab, sf)
        batter_war = _batter_war(woba, lg_woba, pa)

        rows_to_write.append(
            (
                season,
                team,
                player_name,
                games,
                pa,
                ab,
                h,
                doubles,
                triples,
                hr,
                tb_adj,
                rbi,
                bb,
                so,
                hbp,
                sh,
                sf,
                sb,
                cs,
                gdp,
                avg,
                obp,
                slg,
                ops,
                woba,
                batter_war,
                batter_war,
            )
        )

    sql = """
    INSERT OR REPLACE INTO hitter_season_totals (
        season, team, player_name, games, PA, AB, H, "2B", "3B", HR, TB_adj,
        RBI, BB, SO, HBP, SH, SF, SB, CS, GDP,
        AVG, OBP, SLG, OPS, wOBA, batter_war, WAR
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    conn.executemany(sql, rows_to_write)
    conn.commit()

    print(f"Built hitter_season_totals for season={args.season}, team={args.team or 'ALL'}")
    print(f"Rows written: {len(rows_to_write)}")

    if args.preview and args.preview > 0:
        preview_rows = conn.execute(
            """
            SELECT team, player_name, OPS, wOBA, batter_war
            FROM hitter_season_totals
            WHERE season = ?
            ORDER BY batter_war DESC, PA DESC
            LIMIT ?
            """,
            (args.season, args.preview),
        ).fetchall()
        print("Preview top batter_war")
        for row in preview_rows:
            print(
                f"{row['team']}\t{row['player_name']}\twOBA={row['wOBA']:.4f}\tbatter_war={row['batter_war']:.2f}"
            )

    conn.close()


if __name__ == "__main__":
    main()

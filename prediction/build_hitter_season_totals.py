import argparse
from typing import Dict, List

from db_support import connect_for_path, execute, executemany, is_postgres, row_value, table_columns, table_exists

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


def ensure_table(conn) -> None:
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
    existing = {col.lower() for col in table_columns(conn, "hitter_season_totals")}
    required = {
        "SH": "INTEGER NOT NULL DEFAULT 0",
        "wOBA": "REAL NOT NULL DEFAULT 0",
        "batter_war": "REAL NOT NULL DEFAULT 0",
        "WAR": "REAL NOT NULL DEFAULT 0",
    }
    for col, col_def in required.items():
        if col.lower() not in existing:
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


def _fetch_player_rows(conn, season: int, team: str | None) -> List[object]:
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
    return conn.execute(sql.replace("?", "%s") if is_postgres(conn) else sql, [season] + params).fetchall()


def _fetch_league_row(conn, season: int):
    return execute(
        conn,
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
        [str(season)],
    ).fetchone()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build hitter season totals.")
    parser.add_argument("--db", default="db.sqlite3")
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument("--team")
    parser.add_argument("--upsert", action="store_true")
    parser.add_argument("--preview", type=int, default=0)
    args = parser.parse_args()

    conn = connect_for_path(args.db)

    if not table_exists(conn, "hitter_game_logs"):
        raise SystemExit("Missing table: hitter_game_logs")

    ensure_table(conn)
    player_rows = _fetch_player_rows(conn, args.season, args.team)
    league_row = _fetch_league_row(conn, args.season)
    lg_woba = _woba(
        h=int(row_value(league_row, "H", row_value(league_row, "h", 0)) or 0),
        doubles=int(row_value(league_row, "2B", row_value(league_row, "2b", 0)) or 0),
        triples=int(row_value(league_row, "3B", row_value(league_row, "3b", 0)) or 0),
        hr=int(row_value(league_row, "HR", row_value(league_row, "hr", 0)) or 0),
        bb=int(row_value(league_row, "BB", row_value(league_row, "bb", 0)) or 0),
        hbp=int(row_value(league_row, "HBP", row_value(league_row, "hbp", 0)) or 0),
        ab=int(row_value(league_row, "AB", row_value(league_row, "ab", 0)) or 0),
        sf=int(row_value(league_row, "SF", row_value(league_row, "sf", 0)) or 0),
    )

    rows_to_write: List[tuple] = []
    for row in player_rows:
        season = int(row_value(row, "season", args.season) or args.season)
        team = str(row_value(row, "team", "") or "")
        player_name = str(row_value(row, "player_name", "") or "")
        games = int(row_value(row, "games", 0) or 0)
        pa = int(row_value(row, "PA", 0) or 0)
        ab = int(row_value(row, "AB", 0) or 0)
        h = int(row_value(row, "H", 0) or 0)
        doubles = int(row_value(row, "2B", 0) or 0)
        triples = int(row_value(row, "3B", 0) or 0)
        hr = int(row_value(row, "HR", 0) or 0)
        raw_tb = int(row_value(row, "TB", 0) or 0)
        tb_adj = _tb_adj(h, doubles, triples, hr, raw_tb)
        rbi = int(row_value(row, "RBI", 0) or 0)
        bb = int(row_value(row, "BB", 0) or 0)
        so = int(row_value(row, "SO", 0) or 0)
        hbp = int(row_value(row, "HBP", 0) or 0)
        sh = int(row_value(row, "SH", 0) or 0)
        sf = int(row_value(row, "SF", 0) or 0)
        sb = int(row_value(row, "SB", 0) or 0)
        cs = int(row_value(row, "CS", 0) or 0)
        gdp = int(row_value(row, "GDP", 0) or 0)

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
    INSERT INTO hitter_season_totals (
        season, team, player_name, games, PA, AB, H, "2B", "3B", HR, TB_adj,
        RBI, BB, SO, HBP, SH, SF, SB, CS, GDP,
        AVG, OBP, SLG, OPS, wOBA, batter_war, WAR
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(season, team, player_name) DO UPDATE SET
        games=excluded.games,
        PA=excluded.PA,
        AB=excluded.AB,
        H=excluded.H,
        "2B"=excluded."2B",
        "3B"=excluded."3B",
        HR=excluded.HR,
        TB_adj=excluded.TB_adj,
        RBI=excluded.RBI,
        BB=excluded.BB,
        SO=excluded.SO,
        HBP=excluded.HBP,
        SH=excluded.SH,
        SF=excluded.SF,
        SB=excluded.SB,
        CS=excluded.CS,
        GDP=excluded.GDP,
        AVG=excluded.AVG,
        OBP=excluded.OBP,
        SLG=excluded.SLG,
        OPS=excluded.OPS,
        wOBA=excluded.wOBA,
        batter_war=excluded.batter_war,
        WAR=excluded.WAR
    """
    executemany(conn, sql, rows_to_write)
    conn.commit()

    print(f"Built hitter_season_totals for season={args.season}, team={args.team or 'ALL'}")
    print(f"Rows written: {len(rows_to_write)}")

    if args.preview and args.preview > 0:
        preview_rows = execute(
            conn,
            """
            SELECT team, player_name, OPS, wOBA, batter_war
            FROM hitter_season_totals
            WHERE season = ?
            ORDER BY batter_war DESC, PA DESC
            LIMIT ?
            """,
            [args.season, args.preview],
        ).fetchall()
        print("Preview top batter_war")
        for row in preview_rows:
            if isinstance(row, dict):
                team_name = row_value(row, "team", "")
                player_name = row_value(row, "player_name", "")
                woba = float(row_value(row, "wOBA", 0) or 0)
                batter_war = float(row_value(row, "batter_war", 0) or 0)
            else:
                team_name = row[0]
                player_name = row[1]
                woba = float(row[3] or 0)
                batter_war = float(row[4] or 0)
            print(
                f"{team_name}\t{player_name}\twOBA={woba:.4f}\tbatter_war={batter_war:.2f}"
            )

    conn.close()


if __name__ == "__main__":
    main()

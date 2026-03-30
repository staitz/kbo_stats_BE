import argparse
import sys
from pathlib import Path
from typing import List

# Ensure the project root (kbo_stat_BE) is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db_support import connect_for_path, execute, executemany, is_postgres, row_value, table_columns, table_exists


RUNS_PER_WIN = 10.0
SP_REP_GAP = 0.8
RP_REP_GAP = 0.5


def safe_col(name: str) -> str:
    if not name:
        return name
    if name[0].isdigit() or "-" in name or " " in name:
        return f'"{name}"'
    return name


def ensure_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pitcher_season_totals (
            season INTEGER NOT NULL,
            team TEXT NOT NULL,
            player_name TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT '',
            games INTEGER NOT NULL DEFAULT 0,
            W INTEGER NOT NULL DEFAULT 0,
            L INTEGER NOT NULL DEFAULT 0,
            SV INTEGER NOT NULL DEFAULT 0,
            HLD INTEGER NOT NULL DEFAULT 0,
            BF INTEGER NOT NULL DEFAULT 0,
            NP INTEGER NOT NULL DEFAULT 0,
            OUTS INTEGER NOT NULL DEFAULT 0,
            IP REAL NOT NULL DEFAULT 0,
            H INTEGER NOT NULL DEFAULT 0,
            R INTEGER NOT NULL DEFAULT 0,
            ER INTEGER NOT NULL DEFAULT 0,
            BB INTEGER NOT NULL DEFAULT 0,
            SO INTEGER NOT NULL DEFAULT 0,
            HR INTEGER NOT NULL DEFAULT 0,
            HBP INTEGER NOT NULL DEFAULT 0,
            BK INTEGER NOT NULL DEFAULT 0,
            WP INTEGER NOT NULL DEFAULT 0,
            ERA REAL NOT NULL DEFAULT 0,
            WHIP REAL NOT NULL DEFAULT 0,
            K9 REAL NOT NULL DEFAULT 0,
            BB9 REAL NOT NULL DEFAULT 0,
            KBB REAL NOT NULL DEFAULT 0,
            FIP REAL NOT NULL DEFAULT 0,
            pitcher_war REAL NOT NULL DEFAULT 0,
            WAR REAL NOT NULL DEFAULT 0,
            UNIQUE (season, team, player_name)
        )
        """
    )
    existing = {col.lower() for col in table_columns(conn, "pitcher_season_totals")}
    required = {
        "role": "TEXT NOT NULL DEFAULT ''",
        "FIP": "REAL NOT NULL DEFAULT 0",
        "pitcher_war": "REAL NOT NULL DEFAULT 0",
        "WAR": "REAL NOT NULL DEFAULT 0",
    }
    for col, col_def in required.items():
        if col.lower() not in existing:
            conn.execute(f"ALTER TABLE pitcher_season_totals ADD COLUMN {safe_col(col)} {col_def}")
    # 구버전 테이블에 UNIQUE 인덱스가 없으면 ON CONFLICT 절이 실패하므로 보장
    if not is_postgres(conn):
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_pitcher_season_totals
            ON pitcher_season_totals (season, team, player_name)
            """
        )
    conn.commit()


def _rate(num: float, den: float, multiplier: float = 1.0) -> float:
    return (num / den) * multiplier if den > 0 else 0.0


def _fip(hr: float, bb: float, hbp: float, so: float, ip: float, fip_constant: float) -> float:
    if ip <= 0:
        return 0.0
    return (((13.0 * hr) + (3.0 * (bb + hbp)) - (2.0 * so)) / ip) + fip_constant


def _role_rep_gap(role: str) -> float:
    if role == "SP":
        return SP_REP_GAP
    if role == "RP":
        return RP_REP_GAP
    # TODO: if richer role data is ingested, apply RP gap to non-starters as well.
    return SP_REP_GAP


def _pitcher_war(fip: float, lg_fip: float, ip: float, role: str) -> float:
    if ip <= 0:
        return 0.0
    fip_rep = lg_fip + _role_rep_gap(role)
    # FIP-based WAR proxy:
    #   RAR_pitcher = ((FIP_rep - FIP) / 9) * IP
    #   WAR_pitcher = RAR_pitcher / RunsPerWin
    return round(((((fip_rep - fip) / 9.0) * ip) / RUNS_PER_WIN), 2)


def _fetch_player_rows(conn, season: int, team: str | None) -> List[object]:
    params: List[object] = [season, str(season)]
    where = ["substr(game_date, 1, 4) = ?"]
    if team:
        where.append("team = ?")
        params.append(team)
    sql = f"""
    SELECT
        ? AS season,
        team,
        player_name,
        COALESCE(MAX(NULLIF(role, '')), '') AS role,
        COUNT(DISTINCT game_id) AS games,
        COALESCE(SUM(W), 0) AS W,
        COALESCE(SUM(L), 0) AS L,
        COALESCE(SUM(SV), 0) AS SV,
        COALESCE(SUM(HLD), 0) AS HLD,
        COALESCE(SUM(BF), 0) AS BF,
        COALESCE(SUM(NP), 0) AS NP,
        COALESCE(SUM(OUTS), 0) AS OUTS,
        COALESCE(SUM(H), 0) AS H,
        COALESCE(SUM(R), 0) AS R,
        COALESCE(SUM(ER), 0) AS ER,
        COALESCE(SUM(BB), 0) AS BB,
        COALESCE(SUM(SO), 0) AS SO,
        COALESCE(SUM(HR), 0) AS HR,
        COALESCE(SUM(HBP), 0) AS HBP,
        COALESCE(SUM(BK), 0) AS BK,
        COALESCE(SUM(WP), 0) AS WP
    FROM pitcher_game_logs
    WHERE {' AND '.join(where)}
    GROUP BY team, player_name
    """
    return conn.execute(sql.replace("?", "%s") if is_postgres(conn) else sql, params).fetchall()


def _fetch_league_row(conn, season: int):
    return execute(
        conn,
        """
        SELECT
            COALESCE(SUM(OUTS), 0) AS OUTS,
            COALESCE(SUM(ER), 0) AS ER,
            COALESCE(SUM(BB), 0) AS BB,
            COALESCE(SUM(SO), 0) AS SO,
            COALESCE(SUM(HR), 0) AS HR,
            COALESCE(SUM(HBP), 0) AS HBP
        FROM pitcher_game_logs
        WHERE substr(game_date, 1, 4) = ?
        """,
        [str(season)],
    ).fetchone()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build pitcher season totals.")
    parser.add_argument("--db", default="db.sqlite3")
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument("--team")
    parser.add_argument("--upsert", action="store_true")
    parser.add_argument("--preview", type=int, default=0)
    args = parser.parse_args()

    conn = connect_for_path(args.db)

    if not table_exists(conn, "pitcher_game_logs"):
        raise SystemExit("Missing table: pitcher_game_logs")

    ensure_table(conn)
    player_rows = _fetch_player_rows(conn, args.season, args.team)
    league_row = _fetch_league_row(conn, args.season)

    league_outs = float(row_value(league_row, "OUTS", 0) or 0)
    league_ip = league_outs / 3.0 if league_outs > 0 else 0.0
    league_era = _rate(float(row_value(league_row, "ER", 0) or 0), league_ip, 9.0)
    league_fip_core = 0.0
    if league_ip > 0:
        league_fip_core = (
            (13.0 * float(row_value(league_row, "HR", 0) or 0))
            + (3.0 * (float(row_value(league_row, "BB", 0) or 0) + float(row_value(league_row, "HBP", 0) or 0)))
            - (2.0 * float(row_value(league_row, "SO", 0) or 0))
        ) / league_ip
    fip_constant = league_era - league_fip_core
    lg_fip = _fip(
        hr=float(row_value(league_row, "HR", 0) or 0),
        bb=float(row_value(league_row, "BB", 0) or 0),
        hbp=float(row_value(league_row, "HBP", 0) or 0),
        so=float(row_value(league_row, "SO", 0) or 0),
        ip=league_ip,
        fip_constant=fip_constant,
    )

    rows_to_write: List[tuple] = []
    for row in player_rows:
        season = int(row_value(row, "season", args.season) or args.season)
        team = str(row_value(row, "team", "") or "")
        player_name = str(row_value(row, "player_name", "") or "")
        role = str(row_value(row, "role", "") or "").strip()
        games = int(row_value(row, "games", 0) or 0)
        wins = int(row_value(row, "W", 0) or 0)
        losses = int(row_value(row, "L", 0) or 0)
        saves = int(row_value(row, "SV", 0) or 0)
        holds = int(row_value(row, "HLD", 0) or 0)
        batters_faced = int(row_value(row, "BF", 0) or 0)
        pitches = int(row_value(row, "NP", 0) or 0)
        outs = int(row_value(row, "OUTS", 0) or 0)
        hits = int(row_value(row, "H", 0) or 0)
        runs = int(row_value(row, "R", 0) or 0)
        earned_runs = int(row_value(row, "ER", 0) or 0)
        walks = int(row_value(row, "BB", 0) or 0)
        strikeouts = int(row_value(row, "SO", 0) or 0)
        home_runs = int(row_value(row, "HR", 0) or 0)
        hit_by_pitch = int(row_value(row, "HBP", 0) or 0)
        balks = int(row_value(row, "BK", 0) or 0)
        wild_pitches = int(row_value(row, "WP", 0) or 0)

        ip = outs / 3.0
        era = _rate(earned_runs, ip, 9.0)
        whip = _rate(walks + hits, ip, 1.0)
        k9 = _rate(strikeouts, ip, 9.0)
        bb9 = _rate(walks, ip, 9.0)
        kbb = _rate(strikeouts, walks, 1.0)
        fip = _fip(home_runs, walks, hit_by_pitch, strikeouts, ip, fip_constant)
        pitcher_war = _pitcher_war(fip, lg_fip, ip, role)

        rows_to_write.append(
            (
                season,
                team,
                player_name,
                role,
                games,
                wins,
                losses,
                saves,
                holds,
                batters_faced,
                pitches,
                outs,
                ip,
                hits,
                runs,
                earned_runs,
                walks,
                strikeouts,
                home_runs,
                hit_by_pitch,
                balks,
                wild_pitches,
                era,
                whip,
                k9,
                bb9,
                kbb,
                fip,
                pitcher_war,
                pitcher_war,
            )
        )

    sql = """
    INSERT INTO pitcher_season_totals (
        season, team, player_name, role, games, W, L, SV, HLD, BF, NP, OUTS, IP,
        H, R, ER, BB, SO, HR, HBP, BK, WP, ERA, WHIP, K9, BB9, KBB, FIP, pitcher_war, WAR
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(season, team, player_name) DO UPDATE SET
        role=excluded.role,
        games=excluded.games,
        W=excluded.W,
        L=excluded.L,
        SV=excluded.SV,
        HLD=excluded.HLD,
        BF=excluded.BF,
        NP=excluded.NP,
        OUTS=excluded.OUTS,
        IP=excluded.IP,
        H=excluded.H,
        R=excluded.R,
        ER=excluded.ER,
        BB=excluded.BB,
        SO=excluded.SO,
        HR=excluded.HR,
        HBP=excluded.HBP,
        BK=excluded.BK,
        WP=excluded.WP,
        ERA=excluded.ERA,
        WHIP=excluded.WHIP,
        K9=excluded.K9,
        BB9=excluded.BB9,
        KBB=excluded.KBB,
        FIP=excluded.FIP,
        pitcher_war=excluded.pitcher_war,
        WAR=excluded.WAR
    """
    executemany(conn, sql, rows_to_write)
    conn.commit()

    print(f"Built pitcher_season_totals for season={args.season}, team={args.team or 'ALL'}")
    print(f"Rows written: {len(rows_to_write)}")
    print(f"League FIP baseline: {lg_fip:.3f} (constant={fip_constant:.3f})")

    if args.preview and args.preview > 0:
        preview_rows = execute(
            conn,
            """
            SELECT team, player_name, role, IP, FIP, pitcher_war
            FROM pitcher_season_totals
            WHERE season = ?
            ORDER BY pitcher_war DESC, OUTS DESC
            LIMIT ?
            """,
            [args.season, args.preview],
        ).fetchall()
        print("Preview top pitcher_war")
        for row in preview_rows:
            if isinstance(row, dict):
                team_name = row_value(row, "team", "")
                player_name = row_value(row, "player_name", "")
                role = row_value(row, "role", "")
                innings = float(row_value(row, "IP", 0) or 0)
                fip = float(row_value(row, "FIP", 0) or 0)
                pitcher_war = float(row_value(row, "pitcher_war", 0) or 0)
            else:
                team_name = row[0]
                player_name = row[1]
                role = row[2]
                innings = float(row[3] or 0)
                fip = float(row[4] or 0)
                pitcher_war = float(row[5] or 0)
            print(
                f"{team_name}\t{player_name}\t{role}\tIP={innings:.1f}\tFIP={fip:.3f}\tpitcher_war={pitcher_war:.2f}"
            )

    conn.close()


if __name__ == "__main__":
    main()

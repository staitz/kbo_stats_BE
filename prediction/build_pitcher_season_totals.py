import argparse
import sqlite3
from typing import List


RUNS_PER_WIN = 10.0
SP_REP_GAP = 0.8
RP_REP_GAP = 0.5


def safe_col(name: str) -> str:
    if not name:
        return name
    if name[0].isdigit() or "-" in name or " " in name:
        return f'"{name}"'
    return name


def ensure_table(conn: sqlite3.Connection) -> None:
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
    existing = {row[1] for row in conn.execute("PRAGMA table_info(pitcher_season_totals)").fetchall()}
    required = {
        "role": "TEXT NOT NULL DEFAULT ''",
        "FIP": "REAL NOT NULL DEFAULT 0",
        "pitcher_war": "REAL NOT NULL DEFAULT 0",
        "WAR": "REAL NOT NULL DEFAULT 0",
    }
    for col, col_def in required.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE pitcher_season_totals ADD COLUMN {safe_col(col)} {col_def}")
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


def _fetch_player_rows(conn: sqlite3.Connection, season: int, team: str | None) -> List[sqlite3.Row]:
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
    return conn.execute(sql, params).fetchall()


def _fetch_league_row(conn: sqlite3.Connection, season: int) -> sqlite3.Row:
    return conn.execute(
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
        (str(season),),
    ).fetchone()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build pitcher season totals.")
    parser.add_argument("--db", default="db.sqlite3")
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument("--team")
    parser.add_argument("--upsert", action="store_true")
    parser.add_argument("--preview", type=int, default=0)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    if not conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='pitcher_game_logs'"
    ).fetchone():
        raise SystemExit("Missing table: pitcher_game_logs")

    ensure_table(conn)
    player_rows = _fetch_player_rows(conn, args.season, args.team)
    league_row = _fetch_league_row(conn, args.season)

    league_outs = float(league_row["OUTS"] or 0)
    league_ip = league_outs / 3.0 if league_outs > 0 else 0.0
    league_era = _rate(float(league_row["ER"] or 0), league_ip, 9.0)
    league_fip_core = 0.0
    if league_ip > 0:
        league_fip_core = (
            (13.0 * float(league_row["HR"] or 0))
            + (3.0 * (float(league_row["BB"] or 0) + float(league_row["HBP"] or 0)))
            - (2.0 * float(league_row["SO"] or 0))
        ) / league_ip
    fip_constant = league_era - league_fip_core
    lg_fip = _fip(
        hr=float(league_row["HR"] or 0),
        bb=float(league_row["BB"] or 0),
        hbp=float(league_row["HBP"] or 0),
        so=float(league_row["SO"] or 0),
        ip=league_ip,
        fip_constant=fip_constant,
    )

    rows_to_write: List[tuple] = []
    for row in player_rows:
        season = int(row["season"] or args.season)
        team = str(row["team"] or "")
        player_name = str(row["player_name"] or "")
        role = str(row["role"] or "").strip()
        games = int(row["games"] or 0)
        wins = int(row["W"] or 0)
        losses = int(row["L"] or 0)
        saves = int(row["SV"] or 0)
        holds = int(row["HLD"] or 0)
        batters_faced = int(row["BF"] or 0)
        pitches = int(row["NP"] or 0)
        outs = int(row["OUTS"] or 0)
        hits = int(row["H"] or 0)
        runs = int(row["R"] or 0)
        earned_runs = int(row["ER"] or 0)
        walks = int(row["BB"] or 0)
        strikeouts = int(row["SO"] or 0)
        home_runs = int(row["HR"] or 0)
        hit_by_pitch = int(row["HBP"] or 0)
        balks = int(row["BK"] or 0)
        wild_pitches = int(row["WP"] or 0)

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
    INSERT OR REPLACE INTO pitcher_season_totals (
        season, team, player_name, role, games, W, L, SV, HLD, BF, NP, OUTS, IP,
        H, R, ER, BB, SO, HR, HBP, BK, WP, ERA, WHIP, K9, BB9, KBB, FIP, pitcher_war, WAR
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    conn.executemany(sql, rows_to_write)
    conn.commit()

    print(f"Built pitcher_season_totals for season={args.season}, team={args.team or 'ALL'}")
    print(f"Rows written: {len(rows_to_write)}")
    print(f"League FIP baseline: {lg_fip:.3f} (constant={fip_constant:.3f})")

    if args.preview and args.preview > 0:
        preview_rows = conn.execute(
            """
            SELECT team, player_name, role, IP, FIP, pitcher_war
            FROM pitcher_season_totals
            WHERE season = ?
            ORDER BY pitcher_war DESC, OUTS DESC
            LIMIT ?
            """,
            (args.season, args.preview),
        ).fetchall()
        print("Preview top pitcher_war")
        for row in preview_rows:
            print(
                f"{row['team']}\t{row['player_name']}\t{row['role']}\tIP={row['IP']:.1f}\tFIP={row['FIP']:.3f}\tpitcher_war={row['pitcher_war']:.2f}"
            )

    conn.close()


if __name__ == "__main__":
    main()

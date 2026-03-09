from __future__ import annotations

import numpy as np
import pandas as pd


def make_mock_hitter_game_logs() -> pd.DataFrame:
    rows: list[dict] = []
    players = [
        ("P001", "Kim", "LG", 27),
        ("P002", "Lee", "KIA", 31),
    ]
    base_date = pd.Timestamp("2025-03-22")
    rng = np.random.default_rng(42)

    for player_id, player_name, team, age in players:
        for game_no in range(1, 31):
            ab = int(rng.integers(3, 6))
            h = int(rng.integers(0, min(4, ab + 1)))
            dbl = int(min(h, rng.integers(0, 2)))
            trp = 0
            hr = int(min(max(h - dbl, 0), rng.integers(0, 2)))
            bb = int(rng.integers(0, 2))
            so = int(rng.integers(0, 3))
            hbp = 0
            sf = int(rng.integers(0, 2))
            pa = ab + bb + hbp + sf
            singles = max(h - dbl - trp - hr, 0)
            tb = singles + 2 * dbl + 3 * trp + 4 * hr

            rows.append(
                {
                    "season": 2025,
                    "game_date": (base_date + pd.Timedelta(days=game_no)).strftime("%Y-%m-%d"),
                    "game_no": game_no,
                    "player_id": player_id,
                    "player_name": player_name,
                    "team": team,
                    "age": age,
                    "PA": pa,
                    "AB": ab,
                    "H": h,
                    "2B": dbl,
                    "3B": trp,
                    "HR": hr,
                    "BB": bb,
                    "SO": so,
                    "HBP": hbp,
                    "SF": sf,
                    "TB": tb,
                    "home_game": int(game_no % 2 == 0),
                    "park_factor": 1.02 if team == "LG" else 0.99,
                    "opponent_pitching_strength": float(rng.uniform(0.9, 1.1)),
                    "batting_order": int(rng.integers(1, 7)),
                    "game_ops": 0.0,
                    "war_game": float(rng.normal(0.05, 0.03)),
                }
            )

    df = pd.DataFrame(rows)
    obp_den = (df["AB"] + df["BB"] + df["HBP"] + df["SF"]).clip(lower=1)
    df["game_ops"] = ((df["H"] + df["BB"] + df["HBP"]) / obp_den) + (df["TB"] / df["AB"].clip(lower=1))
    return df


def make_mock_pitcher_game_logs() -> pd.DataFrame:
    rows: list[dict] = []
    base_date = pd.Timestamp("2025-03-22")
    rng = np.random.default_rng(7)
    for game_no in range(1, 21):
        ip = float(rng.choice([1.0, 5.0, 6.0]))
        er = int(rng.integers(0, 4))
        bb = int(rng.integers(0, 3))
        so = int(rng.integers(0, 8))
        rows.append(
            {
                "season": 2025,
                "game_date": (base_date + pd.Timedelta(days=game_no)).strftime("%Y-%m-%d"),
                "player_id": "T001",
                "player_name": "Park",
                "team": "KT",
                "role": "SP",
                "age": 29,
                "IP": ip,
                "ER": er,
                "BB": bb,
                "SO": so,
                "H": int(rng.integers(2, 9)),
            }
        )
    return pd.DataFrame(rows)

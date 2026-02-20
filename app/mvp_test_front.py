import json
from typing import Any

import pandas as pd
import requests
import streamlit as st


st.set_page_config(page_title="KBO MVP Test Front", layout="wide")


def api_get(base_url: str, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}{path}"
    try:
        resp = requests.get(url, params=params or {}, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        return {"_error": str(exc), "_url": url, "_params": params or {}}


def show_json(data: dict[str, Any]) -> None:
    st.code(json.dumps(data, ensure_ascii=False, indent=2), language="json")


def show_df(rows: Any) -> None:
    if not rows:
        st.info("No rows")
        return
    df = pd.DataFrame(rows)
    df.index = range(1, len(df) + 1)
    st.dataframe(df, width="stretch")


st.title("KBO MVP Test Front")
st.caption("Temporary test UI for API verification")

api_base = st.sidebar.text_input("API Base URL", value="http://127.0.0.1:8000/api")
season = st.sidebar.number_input("Season", value=2026, step=1)

tabs = st.tabs(
    [
        "Home",
        "Standings",
        "Leaderboard",
        "Player",
        "Team",
        "Games",
        "Predictions",
    ]
)

with tabs[0]:
    auto_min_pa = st.checkbox("AUTO min_pa", value=True, key="home_auto_min_pa")
    min_pa = st.number_input("min_pa (manual)", value=100, step=10, key="home_min_pa", disabled=auto_min_pa)
    params = {"season": int(season)}
    if not auto_min_pa:
        params["min_pa"] = int(min_pa)
    data = api_get(api_base, "/home/summary", params)
    if "_error" in data:
        st.error(data["_error"])
        show_json(data)
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Latest Game", str(data.get("latest_game_date")))
        c2.metric("Latest Pred", str(data.get("latest_prediction_date")))
        c3.metric("Players", str((data.get("totals") or {}).get("players")))
        st.subheader("OPS Top 5")
        show_df((data.get("leaderboards") or {}).get("ops_top5"))
        st.subheader("HR Top 5")
        show_df((data.get("leaderboards") or {}).get("hr_top5"))
        st.subheader("Standings Preview")
        show_df((data.get("standings_preview") or {}).get("rows"))

with tabs[1]:
    data = api_get(api_base, "/standings", {"season": int(season)})
    if "_error" in data:
        st.error(data["_error"])
        show_json(data)
    else:
        st.write(
            {
                "requested_season": data.get("requested_season"),
                "effective_season": data.get("effective_season"),
                "as_of_date": data.get("as_of_date"),
                "mode": data.get("mode"),
            }
        )
        show_df(data.get("rows"))

with tabs[2]:
    metric = st.selectbox("metric", ["OPS", "HR", "AVG", "OBP", "SLG", "RBI", "H"])
    team = st.text_input("team (optional)", value="")
    auto_min_pa = st.checkbox("AUTO min_pa", value=True, key="lb_auto_min_pa")
    min_pa = st.number_input("min_pa (manual)", value=100, step=10, key="lb_min_pa", disabled=auto_min_pa)
    limit = st.number_input("limit", value=20, step=5)
    params = {
        "season": int(season),
        "metric": metric,
        "team": team.strip() or None,
        "limit": int(limit),
        "offset": 0,
    }
    if not auto_min_pa:
        params["min_pa"] = int(min_pa)
    data = api_get(
        api_base,
        "/leaderboard",
        params,
    )
    if "_error" in data:
        st.error(data["_error"])
        show_json(data)
    else:
        st.write({"total": data.get("total"), "metric": data.get("metric")})
        show_df(data.get("rows"))

with tabs[3]:
    q = st.text_input("search q", value="")
    if st.button("Search Players"):
        data = api_get(api_base, "/players/search", {"season": int(season), "q": q, "limit": 20})
        if "_error" in data:
            st.error(data["_error"])
            show_json(data)
        else:
            show_df(data.get("rows"))

    player_name = st.text_input("player_name", value="")
    recent_n = st.slider("recent_n", min_value=1, max_value=30, value=10)
    if st.button("Load Player Detail"):
        data = api_get(api_base, f"/players/{player_name}", {"season": int(season), "recent_n": int(recent_n)})
        if "_error" in data:
            st.error(data["_error"])
            show_json(data)
        elif data.get("error"):
            st.warning(data)
        else:
            st.subheader("Season Aggregate")
            st.write(data.get("season_aggregate"))
            st.subheader("Season Rows")
            show_df(data.get("season_rows"))
            st.subheader("Monthly Splits")
            show_df(data.get("monthly_splits"))
            st.subheader("Vs Team Splits")
            show_df(data.get("vs_team_splits"))
            st.subheader("Recent Game Logs")
            show_df(data.get("recent_game_logs"))
            st.subheader("KBReport Home/Away Splits")
            show_df((data.get("kbreport_splits") or {}).get("homeaway"))
            st.subheader("KBReport Pitchside Splits")
            show_df((data.get("kbreport_splits") or {}).get("pitchside"))
            st.subheader("KBReport Opposite Splits")
            show_df((data.get("kbreport_splits") or {}).get("opposite"))
            st.subheader("KBReport Monthly Splits")
            show_df((data.get("kbreport_splits") or {}).get("month"))
            st.subheader("Latest Prediction")
            st.write(data.get("latest_prediction"))

with tabs[4]:
    team_name = st.text_input("team_name", value="KIA")
    auto_min_pa = st.checkbox("AUTO min_pa", value=True, key="team_auto_min_pa")
    min_pa = st.number_input("min_pa (manual)", value=100, step=10, key="team_min_pa", disabled=auto_min_pa)
    if st.button("Load Team Detail"):
        params = {"season": int(season)}
        if not auto_min_pa:
            params["min_pa"] = int(min_pa)
        data = api_get(api_base, f"/teams/{team_name}", params)
        if "_error" in data:
            st.error(data["_error"])
            show_json(data)
        elif data.get("error"):
            st.warning(data)
        else:
            st.write(
                {
                    "requested_min_pa": data.get("requested_min_pa"),
                    "effective_min_pa": data.get("effective_min_pa"),
                    "min_pa_policy": data.get("min_pa_policy"),
                }
            )
            st.subheader("Summary")
            st.write(data.get("summary"))
            st.subheader("OPS Leaders")
            show_df((data.get("leaders") or {}).get("ops_top10"))
            st.subheader("HR Leaders")
            show_df((data.get("leaders") or {}).get("hr_top10"))
            st.subheader("Monthly Trend")
            show_df(data.get("monthly_trend"))
            st.subheader("Recent Games")
            show_df(data.get("recent_games"))

with tabs[5]:
    game_date = st.text_input("date(YYYYMMDD)", value="")
    if st.button("Load Games"):
        params = {"season": int(season), "limit": 30}
        if game_date.strip():
            params["date"] = game_date.strip()
        data = api_get(api_base, "/games", params)
        if "_error" in data:
            st.error(data["_error"])
            show_json(data)
        else:
            show_df(data.get("rows"))

    game_id = st.text_input("game_id", value="")
    if st.button("Load Boxscore"):
        data = api_get(api_base, f"/games/{game_id}/boxscore")
        if "_error" in data:
            st.error(data["_error"])
            show_json(data)
        elif data.get("error"):
            st.warning(data)
        else:
            st.subheader("Teams")
            show_df(data.get("teams"))
            st.subheader("Hitter Rows")
            show_df(data.get("hitter_rows"))

with tabs[6]:
    data = api_get(api_base, "/predictions/latest", {"season": int(season)})
    if "_error" in data:
        st.error(data["_error"])
        show_json(data)
    else:
        st.write({"latest_date": data.get("latest_date")})
        show_df(data.get("rows"))

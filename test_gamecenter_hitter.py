from kbo_api import _make_driver, fetch_gamecenter_hitter_data
from kbo_hitter_parser import parse_hitter_rows


def main():
    game_date = "20250610"
    game_id = "20250610SSHT0"
    away_team = "삼성"
    home_team = "KIA"

    driver = _make_driver(headless=False)
    try:
        data = fetch_gamecenter_hitter_data(
            driver=driver,
            game_date=game_date,
            game_id=game_id,
            away_team=away_team,
            home_team=home_team,
            debug=True,
            run_parser=False,
        )
    finally:
        driver.quit()

    rows = parse_hitter_rows(
        data=data,
        game_date=game_date,
        game_id=game_id,
        away_team=away_team,
        home_team=home_team,
    )

    print(rows[:2])
    if rows:
        print("[ok] parse_hitter_rows returned non-empty rows")
    else:
        print("[fail] parse_hitter_rows returned empty rows")


if __name__ == "__main__":
    main()

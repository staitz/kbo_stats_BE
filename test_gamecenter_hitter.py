from kbo_api import _make_driver, _wait_ready, fetch_gamecenter_hitter_data


def main():
    game_date = "20250610"
    game_id = "20250610SSHT0"
    away_team = "삼성"
    home_team = "KIA"

    url = (
        "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"
        f"?gameDate={game_date}&gameId={game_id}"
    )

    driver = _make_driver(headless=False)
    try:
        driver.get(url)
        _wait_ready(driver, 15)

        _ = fetch_gamecenter_hitter_data(
            driver=driver,
            game_date=game_date,
            game_id=game_id,
            away_team=away_team,
            home_team=home_team
        )
    finally:
        driver.quit()


if __name__ == "__main__":
    main()



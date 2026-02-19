from collector.kbo_api import fetch_month_schedule, fetch_day_schedule


def main():
    month_result = fetch_month_schedule("2025", "06", debug=True, enrich_status=False)
    print("[month] count:", len(month_result))
    for g in month_result[:5]:
        print(g)

    day_result = fetch_day_schedule("20250610", debug=True, enrich_status=True, enrich_today_only=False)
    print("[day] count:", len(day_result))
    for g in day_result[:5]:
        print(g)


if __name__ == "__main__":
    main()

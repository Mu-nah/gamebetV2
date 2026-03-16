"""
Run: python debug_football.py
Shows exactly what date is being sent and what API returns.
"""
import requests
import os
from datetime import datetime, timezone, timedelta

WAT_OFFSET = timezone(timedelta(hours=1))
FOOTBALL_API_KEY = os.getenv("FOOTBALL_API_KEYS", "").split(",")[0].strip()
if not FOOTBALL_API_KEY:
    raise SystemExit("Missing FOOTBALL_API_KEYS in environment.")
HEADERS = {"x-apisports-key": FOOTBALL_API_KEY}
BASE_URL = "https://v3.football.api-sports.io"

now_wat  = datetime.now(WAT_OFFSET)
now_utc  = datetime.now(timezone.utc)
today_wat = now_wat.strftime("%Y-%m-%d")

print(f"Current time : {now_wat.strftime('%Y-%m-%d %H:%M WAT')}")
print(f"Date sent    : {today_wat}")
print()

# Test Premier League with both seasons
for league_id, name in [(39, "Premier League"), (140, "La Liga"), (135, "Serie A")]:
    for season in [2025, 2024]:
        r = requests.get(f"{BASE_URL}/fixtures", headers=HEADERS, params={
            "league": league_id, "season": season, "date": today_wat
        })
        count = len(r.json().get("response", [])) if r.status_code == 200 else 0
        status = r.status_code
        print(f"{name} season={season}: HTTP {status}, fixtures={count}")
    print()

# Also check API status / remaining requests
r = requests.get(f"{BASE_URL}/status", headers=HEADERS)
if r.status_code == 200:
    data = r.json().get("response", {})
    reqs = data.get("requests", {})
    print(f"API requests used today: {reqs.get('current', '?')} / {reqs.get('limit_day', '?')}")

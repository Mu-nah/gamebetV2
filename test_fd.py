import os
import requests
from datetime import datetime, timezone

# Load env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

FOOTBALL_DATA_KEYS = [
    k.strip() for k in os.getenv(
        "FOOTBALL_DATA_KEYS", ""
    ).split(",") if k.strip()
]

if not FOOTBALL_DATA_KEYS:
    print("No FOOTBALL_DATA_KEYS")
    exit()

key = FOOTBALL_DATA_KEYS[0]
url = "https://api.football-data.org/v4/competitions/PL/matches"
headers = {"X-Auth-Token": key}
params = {"dateFrom": "2026-03-16", "dateTo": "2026-03-16"}

session = requests.Session()
session.trust_env = False  # ignore broken HTTP(S)_PROXY env vars if present
resp = session.get(url, headers=headers, params=params, timeout=15)
print(f"Status: {resp.status_code}")
if resp.status_code == 200:
    data = resp.json()
    matches = data.get("matches", [])
    print(f"Matches: {len(matches)}")
    if matches:
        print("Sample:", matches[0]["homeTeam"]["name"], "vs", matches[0]["awayTeam"]["name"])
else:
    print("Error:", resp.text)

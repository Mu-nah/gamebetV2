"""
Run this once to see exactly what BallDontLie returns.
Output tells us how to fix the stats fetcher.

Usage:
    python debug_nba.py
"""
import requests
import json
import os

BALLDONTLIE_KEY = os.getenv("BALLDONTLIE_KEYS", "").split(",")[0].strip()
if not BALLDONTLIE_KEY:
    raise SystemExit("Missing BALLDONTLIE_KEYS in environment.")
HEADERS = {"Authorization": f"Bearer {BALLDONTLIE_KEY}"}

print("=" * 60)
print("TEST 1 — Season averages (new endpoint)")
print("=" * 60)
r = requests.get(
    "https://api.balldontlie.io/nba/v1/team_season_averages/general",
    headers=HEADERS,
    params={"season": 2024, "season_type": "regular", "type": "base", "per_page": 3},
    timeout=10
)
print(f"Status: {r.status_code}")
try:
    data = r.json()
    print(json.dumps(data, indent=2)[:2000])
except Exception as e:
    print(f"Parse error: {e}")
    print(r.text[:500])

print("\n" + "=" * 60)
print("TEST 2 — Season averages (v1 original endpoint)")
print("=" * 60)
r2 = requests.get(
    "https://api.balldontlie.io/v1/season_averages",
    headers=HEADERS,
    params={"season": 2024, "team_id": 1},
    timeout=10
)
print(f"Status: {r2.status_code}")
try:
    print(json.dumps(r2.json(), indent=2)[:2000])
except Exception:
    print(r2.text[:500])
print("\n" + "=" * 60)
print("TEST 5 — BDL team ID map (from multi_sport_bot)")
print("=" * 60)
from multi_sport_bot import _load_bdl_team_abbrev_map, _nba_bdl_id_map
_load_bdl_team_abbrev_map()
print(f"BDL map loaded: {len(_nba_bdl_id_map)} entries")
print(list(_nba_bdl_id_map.items())[:5])
print("\n" + "=" * 60)
print("TEST 3 — Teams list (confirm key + structure)")
print("=" * 60)
r3 = requests.get(
    "https://api.balldontlie.io/v1/teams",
    headers=HEADERS,
    params={"per_page": 3},
    timeout=10
)
print(f"Status: {r3.status_code}")
try:
    d = r3.json()
    print(json.dumps(d["data"][0], indent=2))
    print(f"... total teams in response: {len(d.get('data', []))}")
except Exception:
    print(r3.text[:500])

print("\n" + "=" * 60)
print("TEST 4 — Stats endpoint v1")
print("=" * 60)
r4 = requests.get(
    "https://api.balldontlie.io/v1/stats",
    headers=HEADERS,
    params={"seasons[]": 2024, "per_page": 1},
    timeout=10
)
print(f"Status: {r4.status_code}")
try:
    print(json.dumps(r4.json(), indent=2)[:1000])
except Exception:
    print(r4.text[:500])

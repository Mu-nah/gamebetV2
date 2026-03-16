from multi_sport_bot import football_client, FOOTBALL_URL
from datetime import datetime, timezone

today_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d')
print('UTC date', today_utc)
for season in [2024, 2025, 2026]:
    resp = football_client.get(
        f"{FOOTBALL_URL}/fixtures",
        params={"league": 39, "season": season, "date": today_utc},
    )
    cnt = 0
    if resp and resp.status_code == 200:
        cnt = len(resp.json().get('response', []))
    print('season', season, 'status', getattr(resp, 'status_code', None), 'fixtures', cnt)

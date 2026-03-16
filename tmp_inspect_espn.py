import requests

url = 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams'
params = {'limit': 5}
resp = requests.get(url, params=params, timeout=10)
print('status', resp.status_code)
data = resp.json()
teams = data.get('sports',[{}])[0].get('leagues',[{}])[0].get('teams', [])
print('team count', len(teams))
for t in teams[:1]:
    team = t.get('team', {})
    print('team name', team.get('displayName'), team.get('abbreviation'))
    # Print high-level keys and inspect for stats
    print('team keys:', sorted(team.keys()))
    if 'statistics' in team:
        print('statistics keys:', sorted(team.get('statistics', {}).keys()))
    record = team.get('record', {})
    print('record', record)
    items = record.get('items', [])
    print('items count', len(items))
    if items:
        stats = items[0].get('stats', [])
        print('stats names', [s.get('name') for s in stats])
        print('stats sample', stats[:10])

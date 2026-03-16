import requests
from datetime import datetime

date = datetime.utcnow().strftime('%Y%m%d')
url = 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard'
resp = requests.get(url, params={'dates': date}, timeout=10)
print('status', resp.status_code)
data = resp.json()
print('keys', list(data.keys()))
print('events', len(data.get('events', [])))
if data.get('events'):
    ev = data['events'][0]
    print('event keys', list(ev.keys()))
    comps = ev.get('competitions', [])
    print('competitions', len(comps))
    if comps:
        comp = comps[0]
        teams = comp.get('competitors', [])
        print('competitors', len(teams))
        if teams:
            t = teams[0]
            print('team keys', list(t.keys()))
            print('team name', t.get('team', {}).get('displayName'))
            print('records', t.get('records'))
            print('score', t.get('score'))

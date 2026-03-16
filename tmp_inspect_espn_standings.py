import requests

url = 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/standings'
resp = requests.get(url, timeout=10)
print('status', resp.status_code)
data = resp.json()
print('keys', list(data.keys()))
# find the first conference/standings
confs = data.get('children', [])
print('confs', len(confs))
if confs:
    first = confs[0]
    print('first conf keys', list(first.keys()))
    standings = first.get('standings', [])
    print('standings count', len(standings))
    if standings:
        team = standings[0].get('team', {})
        print('team sample', team.get('displayName'), team.get('abbreviation'))
        print('stat keys', [s.get('name') for s in standings[0].get('stats', [])][:20])
        print('stat sample', standings[0].get('stats', [])[:20])

"""
Live Data Module — ESPN Hidden API (no key required)
Provides real-time:
  🏀 NBA  — injuries, team form (last 5 games), back-to-back detection
  ⚽ Football — team form, standings position, recent results
  🎾 Tennis — live ATP/WTA rankings

All data cached per session to avoid repeat requests.
ESPN API: https://site.api.espn.com/apis/site/v2/sports/
"""

import requests
from datetime import datetime, timezone, timedelta

WAT_OFFSET = timezone(timedelta(hours=1))
ESPN_BASE  = "https://site.api.espn.com/apis/site/v2/sports"
ESPN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

# Session caches
_nba_injuries_cache    = {}   # {team_abbrev: [injury_list]}
_nba_form_cache        = {}   # {team_abbrev: form_dict}
_football_form_cache   = {}   # {team_id: form_dict}
_tennis_rankings_cache = {}   # {player_last_name: rank}
_cache_date            = None


def _reset_if_new_day():
    global _cache_date, _nba_injuries_cache, _nba_form_cache, _football_form_cache, _tennis_rankings_cache
    today = datetime.now(WAT_OFFSET).date()
    if _cache_date != today:
        _nba_injuries_cache    = {}
        _nba_form_cache        = {}
        _football_form_cache   = {}
        _tennis_rankings_cache = {}
        _cache_date = today


def _get(url, params=None, timeout=8):
    try:
        r = requests.get(url, headers=ESPN_HEADERS, params=params, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"[LIVE] Request failed {url}: {e}")
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# NBA — INJURIES
# Source: ESPN /nba/teams/{team}/injuries
# ═══════════════════════════════════════════════════════════════════════════════
# ESPN team slug map (NBA team name → ESPN slug)
NBA_ESPN_SLUGS = {
    "Atlanta Hawks":"atl","Boston Celtics":"bos","Brooklyn Nets":"bkn",
    "Charlotte Hornets":"cha","Chicago Bulls":"chi","Cleveland Cavaliers":"cle",
    "Dallas Mavericks":"dal","Denver Nuggets":"den","Detroit Pistons":"det",
    "Golden State Warriors":"gs","Houston Rockets":"hou","Indiana Pacers":"ind",
    "LA Clippers":"lac","Los Angeles Clippers":"lac","Los Angeles Lakers":"lal",
    "Memphis Grizzlies":"mem","Miami Heat":"mia","Milwaukee Bucks":"mil",
    "Minnesota Timberwolves":"min","New Orleans Pelicans":"no",
    "New York Knicks":"ny","Oklahoma City Thunder":"okc","Orlando Magic":"orl",
    "Philadelphia 76ers":"phi","Phoenix Suns":"phx","Portland Trail Blazers":"por",
    "Sacramento Kings":"sac","San Antonio Spurs":"sa","Toronto Raptors":"tor",
    "Utah Jazz":"utah","Washington Wizards":"wsh",
}


def get_nba_injuries(team_name):
    """
    Returns list of injured/doubtful players for a team.
    Each entry: {"name": str, "status": str, "reason": str}
    """
    _reset_if_new_day()
    if team_name in _nba_injuries_cache:
        return _nba_injuries_cache[team_name]

    slug = NBA_ESPN_SLUGS.get(team_name, "")
    if not slug:
        return []

    url  = f"{ESPN_BASE}/basketball/nba/teams/{slug}/injuries"
    data = _get(url)
    injuries = []

    if data:
        for item in data.get("injuries", []):
            athlete = item.get("athlete", {})
            injuries.append({
                "name":   athlete.get("displayName", "Unknown"),
                "status": item.get("status", ""),
                "reason": item.get("details", {}).get("type", ""),
            })

    _nba_injuries_cache[team_name] = injuries
    if injuries:
        out_count = sum(1 for i in injuries if i["status"].lower() == "out")
        q_count   = sum(1 for i in injuries if "question" in i["status"].lower())
        print(f"[LIVE] {team_name} injuries: {out_count} out, {q_count} questionable")
    return injuries


def get_nba_team_form(team_name):
    """
    Returns recent form for an NBA team using ESPN scoreboard.
    {"last5": "WWLWL", "win_streak": 2, "back_to_back": bool, "rest_days": int}
    """
    _reset_if_new_day()
    if team_name in _nba_form_cache:
        return _nba_form_cache[team_name]

    slug = NBA_ESPN_SLUGS.get(team_name, "")
    result = {"last5": "", "win_streak": 0, "back_to_back": False, "rest_days": 2}
    if not slug:
        _nba_form_cache[team_name] = result
        return result

    # Fetch last 10 games
    url  = f"{ESPN_BASE}/basketball/nba/teams/{slug}/schedule"
    data = _get(url)

    if data:
        events = data.get("events", [])
        results = []
        last_game_date = None

        for event in reversed(events[-10:]):
            competitions = event.get("competitions", [{}])
            comp = competitions[0] if competitions else {}
            for team in comp.get("competitors", []):
                if slug.lower() in team.get("team", {}).get("abbreviation", "").lower() or \
                   team_name.lower() in team.get("team", {}).get("displayName", "").lower():
                    winner = team.get("winner", False)
                    results.append("W" if winner else "L")
                    if last_game_date is None:
                        try:
                            last_game_date = datetime.fromisoformat(
                                event.get("date","").replace("Z","+00:00")
                            )
                        except Exception:
                            pass

        last5 = "".join(results[-5:]) if results else ""
        win_streak = 0
        for r in reversed(results):
            if r == "W":
                win_streak += 1
            else:
                break

        # Back-to-back detection
        back_to_back = False
        rest_days    = 2
        if last_game_date:
            now       = datetime.now(timezone.utc)
            delta     = (now - last_game_date).days
            rest_days = delta
            if delta == 0:
                back_to_back = True

        result = {
            "last5":        last5,
            "win_streak":   win_streak,
            "back_to_back": back_to_back,
            "rest_days":    rest_days,
        }
        if last5:
            print(f"[LIVE] {team_name} form: {last5} | B2B={back_to_back} | rest={rest_days}d")

    _nba_form_cache[team_name] = result
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# FOOTBALL — TEAM FORM via ESPN
# Source: ESPN /soccer/{league}/teams/{team}/schedule
# League slug map for ESPN
# ═══════════════════════════════════════════════════════════════════════════════
FOOTBALL_ESPN_LEAGUES = {
    "Premier League":        "eng.1",
    "La Liga":               "esp.1",
    "La Liga 2":             "esp.2",
    "Serie A":               "ita.1",
    "Serie B":               "ita.2",
    "Bundesliga":            "ger.1",
    "2. Bundesliga":         "ger.2",
    "Ligue 1":               "fra.1",
    "UEFA Champions League": "uefa.champions",
    "UEFA Europa League":    "uefa.europa",
    "UEFA Conference League":"uefa.europa.conf",
}


def get_football_team_form(team_name, league_name):
    """
    Fetch last 5 results for a football team from ESPN.
    Returns {"last5": "WDLLW", "scored": [1,2,0,1,3], "conceded": [0,1,2,0,1]}
    """
    _reset_if_new_day()
    cache_key = f"{team_name}:{league_name}"
    if cache_key in _football_form_cache:
        return _football_form_cache[cache_key]

    result = {"last5": "", "scored": [], "conceded": []}
    league_slug = FOOTBALL_ESPN_LEAGUES.get(league_name, "eng.1")

    # Search for team in ESPN
    search_url = f"{ESPN_BASE}/soccer/{league_slug}/teams"
    data = _get(search_url)

    espn_team_id = None
    if data:
        for team in data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", []):
            t = team.get("team", {})
            if team_name.lower() in t.get("displayName", "").lower() or \
               team_name.lower() in t.get("name", "").lower():
                espn_team_id = t.get("id")
                break

    if espn_team_id:
        sched_url = f"{ESPN_BASE}/soccer/{league_slug}/teams/{espn_team_id}/schedule"
        sched     = _get(sched_url)
        if sched:
            events  = sched.get("events", [])
            results = []
            scored_list = []
            conceded_list = []
            for event in reversed(events[-10:]):
                comp = (event.get("competitions") or [{}])[0]
                for team in comp.get("competitors", []):
                    if str(team.get("id")) == str(espn_team_id):
                        score    = int(team.get("score", 0) or 0)
                        winner   = team.get("winner")
                        home_away = team.get("homeAway", "home")
                        results.append("W" if winner else ("D" if winner is None else "L"))
                        scored_list.append(score)
                        # Get opponent score
                        for opp in comp.get("competitors", []):
                            if str(opp.get("id")) != str(espn_team_id):
                                conceded_list.append(int(opp.get("score", 0) or 0))
            result = {
                "last5":    "".join(results[-5:]),
                "scored":   scored_list[-5:],
                "conceded": conceded_list[-5:],
            }
            if result["last5"]:
                print(f"[LIVE] {team_name} football form: {result['last5']}")

    _football_form_cache[cache_key] = result
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# INJURY IMPACT SCORE
# Converts injury list into a probability adjustment (-10 to 0)
# ═══════════════════════════════════════════════════════════════════════════════
def injury_impact(injuries):
    """
    Returns a score from -15 to 0 based on how many key players are injured.
    Out players hurt more than questionable.
    """
    if not injuries:
        return 0.0
    impact = 0.0
    for inj in injuries:
        status = inj.get("status", "").lower()
        if "out" in status:
            impact -= 3.5     # definite out — big impact
        elif "doubtful" in status:
            impact -= 2.0
        elif "question" in status:
            impact -= 1.0
        elif "probable" in status:
            impact -= 0.5
    return max(impact, -15.0)


def back_to_back_impact(form_dict):
    """Returns penalty for back-to-back games (-4 points on win probability)."""
    if form_dict.get("back_to_back"):
        return -4.0
    if form_dict.get("rest_days", 2) == 0:
        return -2.0
    return 0.0
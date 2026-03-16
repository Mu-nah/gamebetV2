"""
Multi-Sport Prediction Bot v7
Sports: Football | NBA Basketball | Tennis
"""

import os
import sys
import requests
from datetime import datetime, timezone, timedelta

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from predictor            import FootballPredictor
from basketball_predictor import BasketballPredictor
from tennis_predictor     import TennisPredictor
from telegram_sender      import TelegramSender
from news_analyzer        import get_team_sentiment
from api_client           import RotatingClient

# WAT = UTC+1
WAT_OFFSET = timezone(timedelta(hours=1))

# ─── CONFIG ───────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID",   "")

# ─── API BASE URLS ────────────────────────────────────────────────────────────
FOOTBALL_URL    = "https://v3.football.api-sports.io"
FOOTBALL_DATA_URL = "https://api.football-data.org/v4"
BALLDONTLIE_URL = "https://api.balldontlie.io/v1"
API_TENNIS_URL  = "https://api.api-tennis.com/tennis/"

# ─── KEY POOLS ────────────────────────────────────────────────────────────────
# Separate multiple keys with commas in your .env file
FOOTBALL_KEYS = [
    k.strip() for k in os.getenv(
        "FOOTBALL_API_KEYS", ""
    ).split(",") if k.strip()
]

FOOTBALL_DATA_KEYS = [
    k.strip() for k in os.getenv(
        "FOOTBALL_DATA_KEYS", ""
    ).split(",") if k.strip()
]

BALLDONTLIE_KEYS = [
    k.strip() for k in os.getenv(
        "BALLDONTLIE_KEYS", ""
    ).split(",") if k.strip()
]

TENNIS_KEYS = [
    k.strip() for k in os.getenv(
        "TENNIS_API_KEYS", ""
    ).split(",") if k.strip()
]

# ─── ROTATING CLIENTS ─────────────────────────────────────────────────────────
football_client = RotatingClient(FOOTBALL_KEYS,   header_name="x-apisports-key")
football_data_client = RotatingClient(FOOTBALL_DATA_KEYS, header_name="X-Auth-Token") if FOOTBALL_DATA_KEYS else None
nba_client      = RotatingClient(BALLDONTLIE_KEYS, header_name="Authorization", bearer=True)

# Tennis uses query-param auth (not header) — handled manually in fetcher
FOOTBALL_API_KEY = FOOTBALL_KEYS[0]

# ─── FOOTBALL LEAGUES ─────────────────────────────────────────────────────────
FOOTBALL_LEAGUES = {
    39:  ("Premier League",         -1),
    140: ("La Liga",                -1),
    141: ("La Liga 2",              -1),   # Segunda División
    135: ("Serie A",                -1),
    136: ("Serie B",                -1),   # Serie B Italy
    78:  ("Bundesliga",             -1),
    79:  ("2. Bundesliga",          -1),   # 2nd division Germany
    61:  ("Ligue 1",                -1),
    2:   ("UEFA Champions League",  -1),
    3:   ("UEFA Europa League",     -1),
    848: ("UEFA Conference League", -1),
}

# Mapping from api-football league IDs to football-data.org competition codes
FOOTBALL_DATA_LEAGUES = {
    39:  "PL",      # Premier League
    140: "PD",      # Primera Division (La Liga)
    141: "SD",      # Segunda Division
    135: "SA",      # Serie A
    78:  "BL1",     # Bundesliga
    61:  "FL1",     # Ligue 1
    2:   "CL",      # Champions League
    3:   "EL",      # Europa League
    # Note: Serie B, 2. Bundesliga, Conference League not available in football-data.org free tier
}

MIN_MATCHES_PLAYED  = 10
VALUE_BET_THRESHOLD = 0.12
BOOKMAKER_ID        = 6


# ─── VALIDATION ───────────────────────────────────────────────────────────────
def validate_config():
    errors = []
    if not FOOTBALL_KEYS:
        errors.append("  ❌ FOOTBALL_API_KEYS is missing")
    if not BALLDONTLIE_KEYS:
        errors.append("  ❌ BALLDONTLIE_KEYS is missing")
    if not TENNIS_KEYS:
        errors.append("  ❌ TENNIS_API_KEYS is missing")
    if not TELEGRAM_BOT_TOKEN:
        errors.append("  ❌ TELEGRAM_BOT_TOKEN is missing")
    if not TELEGRAM_CHAT_ID:
        errors.append("  ❌ TELEGRAM_CHAT_ID is missing")
    if errors:
        print("\n[CONFIG ERROR] Fix the following in your .env file:")
        for e in errors:
            print(e)
        sys.exit(1)
    print(f"[INFO] Config loaded ✅  "
          f"({len(FOOTBALL_KEYS)} football key(s), "
          f"{len(BALLDONTLIE_KEYS)} NBA key(s), "
          f"{len(TENNIS_KEYS)} tennis key(s))")


# ═══════════════════════════════════════════════════════════════════════════════
# FOOTBALL FETCHERS
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_football_fixtures():
    # api-football uses UTC dates for the "date" parameter.
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    fixtures = []

    for league_id, (name, offset) in FOOTBALL_LEAGUES.items():
        season  = datetime.now().year + offset
        found   = False

        # Try current season, then fallback to previous if the key doesn't allow the current season
        for try_season in [season, season - 1]:
            resp = football_client.get(f"{FOOTBALL_URL}/fixtures", params={
                "league": league_id, "season": try_season, "date": today
            })
            if not resp or resp.status_code != 200:
                continue

            body = resp.json()
            if body.get("errors"):
                # If the free plan doesn't cover the season, try the previous season instead.
                if any("Free plans" in str(e) or "do not have access" in str(e) for e in body.get("errors", [])):
                    continue  # Try next season in the loop
                continue

            results = body.get("response", [])
            if results:
                for f in results:
                    fixtures.append({
                        "sport":      "football",
                        "fixture_id": f["fixture"]["id"],
                        "league":     name,
                        "league_id":  league_id,
                        "home_team":  f["teams"]["home"]["name"],
                        "away_team":  f["teams"]["away"]["name"],
                        "home_id":    f["teams"]["home"]["id"],
                        "away_id":    f["teams"]["away"]["id"],
                        "kickoff":    f["fixture"]["date"],
                        "venue":      (f["fixture"].get("venue") or {}).get("name", "TBC"),
                    })
                found = True
                break  # Got results — no need to try previous season

        if not found:
            print(f"[INFO] Football {name}: 0 fixtures today.")

    # Fallback to football-data.org if no fixtures from api-football and client available
    if not fixtures and football_data_client:
        print("[INFO] Trying football-data.org as alternative source...")
        for league_id, (name, offset) in FOOTBALL_LEAGUES.items():
            if league_id not in FOOTBALL_DATA_LEAGUES:
                continue  # Skip leagues not available in football-data.org
            code = FOOTBALL_DATA_LEAGUES[league_id]
            resp = football_data_client.get(f"{FOOTBALL_DATA_URL}/competitions/{code}/matches", params={
                "dateFrom": today, "dateTo": today
            })
            if not resp or resp.status_code != 200:
                continue
            body = resp.json()
            matches = body.get("matches", [])
            if matches:
                for m in matches:
                    fixtures.append({
                        "sport":      "football",
                        "fixture_id": f"fd_{m['id']}",  # prefix to distinguish
                        "league":     name,
                        "league_id":  league_id,
                        "home_team":  m["homeTeam"]["name"],
                        "away_team":  m["awayTeam"]["name"],
                        "home_id":    m["homeTeam"]["id"],
                        "away_id":    m["awayTeam"]["id"],
                        "kickoff":    m["utcDate"],
                        "venue":      "TBC",  # football-data.org doesn't provide venue in matches
                    })
                print(f"[INFO] Football-data.org {name}: {len(matches)} fixtures.")

    if not fixtures:
        print(
            "[WARN] No football fixtures could be loaded. "
            "This is often caused by using a free api-football key that does not cover the current season. "
            "You can either upgrade your plan or use an alternative fixture source."
        )

    print(f"[INFO] Football total: {len(fixtures)} fixtures today.")
    return fixtures


def fetch_football_team_stats(team_id, league_id):
    # Try current season -1, then -2 if free plan doesn't cover
    for offset in [1, 2]:
        season = datetime.now().year - offset
        resp   = football_client.get(f"{FOOTBALL_URL}/teams/statistics", params={
            "team": team_id, "league": league_id, "season": season
        })
        if resp and resp.status_code == 200:
            body = resp.json()
            if not body.get("errors"):
                return body.get("response", {})
            if any("Free plans" in str(e) for e in body.get("errors", [])):
                continue
        # No print for failures, fallback will handle
    
    # Fallback to football-data.org if available and league supported
    if football_data_client and league_id in FOOTBALL_DATA_LEAGUES:
        code = FOOTBALL_DATA_LEAGUES[league_id]
        return fetch_football_team_stats_fd(team_id, code)
    
    return {}


def fetch_football_team_stats_fd(team_id, code):
    # Fetch standings for the league
    resp = football_data_client.get(f"{FOOTBALL_DATA_URL}/competitions/{code}/standings")
    if not resp or resp.status_code != 200:
        return {}
    
    body = resp.json()
    standings = body.get("standings", [])
    if not standings:
        return {}
    
    table = standings[0].get("table", [])  # Assuming first standings is the main one
    
    team_data = None
    for t in table:
        if t["team"]["id"] == team_id:
            team_data = t
            break
    
    if not team_data:
        return {}
    
    played = team_data["playedGames"]
    if played == 0:
        return {}
    
    # Fetch last finished matches for form.
    # Without `status=FINISHED`, the API often returns scheduled games (no score),
    # which leads to an empty form string and `N/A` in the card output.
    resp2 = football_data_client.get(
        f"{FOOTBALL_DATA_URL}/teams/{team_id}/matches",
        params={"competitions": code, "status": "FINISHED", "limit": 10},
    )
    form_str = ""
    if resp2 and resp2.status_code == 200:
        matches = resp2.json().get("matches", []) or []
        # Be resilient to ordering; pick the most recent finished matches with a score.
        try:
            matches = sorted(matches, key=lambda m: m.get("utcDate", ""), reverse=True)
        except Exception:
            pass
        picked = 0
        for m in matches:
            if picked >= 5:
                break
            ft = (m.get("score") or {}).get("fullTime") or {}
            if not ft or ft.get("home") is None or ft.get("away") is None:
                continue  # Skip matches without scores
            home_score = ft["home"]
            away_score = ft["away"]
            if m["homeTeam"]["id"] == team_id:
                if home_score > away_score:
                    form_str += "W"
                elif home_score < away_score:
                    form_str += "L"
                else:
                    form_str += "D"
            else:
                if away_score > home_score:
                    form_str += "W"
                elif away_score < home_score:
                    form_str += "L"
                else:
                    form_str += "D"
            picked += 1
    
    # Construct stats dict similar to api-football
    goals_for = team_data["goalsFor"]
    goals_against = team_data["goalsAgainst"]
    avg_goals_for = goals_for / played if played else 0
    avg_goals_against = goals_against / played if played else 0
    
    # football-data doesn't split home/away scoring; using totals for both is
    # less biased than halving (which was pushing xG to the 0.3 floor).
    half_for = avg_goals_for
    half_against = avg_goals_against
    
    stats = {
        "fixtures": {
            "played": {"total": played}
        },
        "goals": {
            "for": {
                "average": {
                    "home": half_for,
                    "away": half_for,
                    "total": avg_goals_for
                }
            },
            "against": {
                "average": {
                    "home": half_against,
                    "away": half_against,
                    "total": avg_goals_against
                }
            }
        },
        "form": form_str,
        # Add other fields if needed, but this should suffice for the predictor
    }
    return stats


def fetch_h2h(home_id, away_id):
    resp = football_client.get(f"{FOOTBALL_URL}/fixtures/headtohead", params={
        "h2h": f"{home_id}-{away_id}", "last": 5
    })
    return resp.json().get("response", []) if resp and resp.status_code == 200 else []


def fetch_football_odds(fixture_id):
    resp = football_client.get(f"{FOOTBALL_URL}/odds", params={
        "fixture": fixture_id, "bookmaker": BOOKMAKER_ID, "bet": 1
    })
    if not resp or resp.status_code != 200:
        return None
    try:
        bets    = resp.json()["response"][0]["bookmakers"][0]["bets"][0]["values"]
        odd_map = {}
        for b in bets:
            v = float(b["odd"])
            if b["value"] == "Home":   odd_map["home"] = v
            elif b["value"] == "Draw": odd_map["draw"] = v
            elif b["value"] == "Away": odd_map["away"] = v
        return odd_map or None
    except (IndexError, KeyError, TypeError, ValueError):
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# BASKETBALL FETCHERS
# Stats source : ESPN hidden API — site.api.espn.com (free, no key, live)
# Games source : BallDontLie /v1/games (free)
# ═══════════════════════════════════════════════════════════════════════════════

ESPN_NBA_STANDINGS = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/standings"
ESPN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

# Cache: {abbreviation -> stats_dict}
_nba_stats_cache = {}    # keyed by team abbreviation e.g. "BOS"
_nba_bdl_id_map  = {}    # BallDontLie team_id -> abbreviation

# Hardcoded BallDontLie v1 team_id → abbreviation (IDs never change)
_BDL_ID_TO_ABBREV = {
    1:"ATL", 2:"BOS", 3:"BKN", 4:"CHA", 5:"CHI", 6:"CLE", 7:"DAL",
    8:"DEN", 9:"DET", 10:"GSW", 11:"HOU", 12:"IND", 13:"LAC", 14:"LAL",
    15:"MEM", 16:"MIA", 17:"MIL", 18:"MIN", 19:"NOP", 20:"NYK", 21:"OKC",
    22:"ORL", 23:"PHI", 24:"PHX", 25:"POR", 26:"SAC", 27:"SAS", 28:"OKC",
    29:"TOR", 30:"UTA", 31:"WAS",
}

# ESPN uses different abbreviations than most other sources (e.g. 'WSH' vs 'WAS').
# Map the "standard" abbreviation to the ESPN version used in the stats endpoint.
_ESPN_ABBREV_MAP = {
    "GSW": "GS",
    "WAS": "WSH",
    "NYK": "NY",
    "NOP": "NO",
    "SAS": "SA",
    "UTA": "UTAH",
}

# ── 2024-25 hardcoded fallback standings (used when NBA.com times out) ────────
# Updated to approximate mid-season standings. Bot uses this instantly if
# NBA.com is unreachable, then overwrites with live data on next successful call.
NBA_FALLBACK_STATS = {
    "BOS": {"ppg":120.5,"opp_ppg":108.4,"net_rating":12.1,"win_pct":0.780,"wins":64,"losses":18,"off_rating":118.5,"def_rating":107.6,"pace":98.2,"recent_trend":1.12},
    "OKC": {"ppg":119.6,"opp_ppg":109.2,"net_rating":10.4,"win_pct":0.756,"wins":62,"losses":20,"off_rating":117.6,"def_rating":108.3,"pace":99.1,"recent_trend":1.02},
    "CLE": {"ppg":114.3,"opp_ppg":105.8,"net_rating":8.5, "win_pct":0.720,"wins":59,"losses":23,"off_rating":112.9,"def_rating":105.2,"pace":97.5,"recent_trend":0.88},
    "NYK": {"ppg":117.2,"opp_ppg":109.1,"net_rating":8.1, "win_pct":0.695,"wins":57,"losses":25,"off_rating":115.5,"def_rating":108.2,"pace":97.8,"recent_trend":0.78},
    "IND": {"ppg":122.8,"opp_ppg":117.3,"net_rating":5.5, "win_pct":0.646,"wins":53,"losses":29,"off_rating":120.6,"def_rating":115.5,"pace":102.3,"recent_trend":0.58},
    "MIL": {"ppg":117.5,"opp_ppg":113.2,"net_rating":4.3, "win_pct":0.634,"wins":52,"losses":30,"off_rating":115.6,"def_rating":112.4,"pace":99.6,"recent_trend":0.44},
    "MIA": {"ppg":111.2,"opp_ppg":108.1,"net_rating":3.1, "win_pct":0.585,"wins":48,"losses":34,"off_rating":110.0,"def_rating":107.2,"pace":97.1,"recent_trend":0.34},
    "ORL": {"ppg":109.4,"opp_ppg":107.5,"net_rating":1.9, "win_pct":0.561,"wins":46,"losses":36,"off_rating":108.5,"def_rating":106.6,"pace":97.3,"recent_trend":0.24},
    "ATL": {"ppg":118.9,"opp_ppg":118.1,"net_rating":0.8, "win_pct":0.500,"wins":41,"losses":41,"off_rating":117.0,"def_rating":116.3,"pace":101.2,"recent_trend":0.08},
    "CHI": {"ppg":113.8,"opp_ppg":114.9,"net_rating":-1.1,"win_pct":0.476,"wins":39,"losses":43,"off_rating":112.3,"def_rating":113.4,"pace":98.5,"recent_trend":-0.19},
    "TOR": {"ppg":112.5,"opp_ppg":116.2,"net_rating":-3.7,"win_pct":0.427,"wins":35,"losses":47,"off_rating":111.3,"def_rating":114.7,"pace":97.9,"recent_trend":-0.53},
    "BKN": {"ppg":109.1,"opp_ppg":116.4,"net_rating":-7.3,"win_pct":0.354,"wins":29,"losses":53,"off_rating":107.8,"def_rating":114.7,"pace":97.4,"recent_trend":-0.98},
    "CHA": {"ppg":108.6,"opp_ppg":116.9,"net_rating":-8.3,"win_pct":0.317,"wins":26,"losses":56,"off_rating":107.3,"def_rating":115.2,"pace":98.7,"recent_trend":-1.06},
    "WAS": {"ppg":107.9,"opp_ppg":118.2,"net_rating":-10.3,"win_pct":0.268,"wins":22,"losses":60,"off_rating":106.6,"def_rating":116.6,"pace":97.8,"recent_trend":-1.45},
    # West
    "GSW": {"ppg":116.3,"opp_ppg":114.8,"net_rating":1.5, "win_pct":0.512,"wins":42,"losses":40,"off_rating":114.5,"def_rating":113.1,"pace":99.8,"recent_trend":0.10},
    "HOU": {"ppg":114.4,"opp_ppg":109.8,"net_rating":4.6, "win_pct":0.659,"wins":54,"losses":28,"off_rating":112.8,"def_rating":108.9,"pace":99.4,"recent_trend":0.47},
    "LAL": {"ppg":114.6,"opp_ppg":112.9,"net_rating":1.7, "win_pct":0.537,"wins":44,"losses":38,"off_rating":113.0,"def_rating":111.3,"pace":99.1,"recent_trend":0.15},
    "LAC": {"ppg":110.5,"opp_ppg":114.3,"net_rating":-3.8,"win_pct":0.415,"wins":34,"losses":48,"off_rating":109.3,"def_rating":112.7,"pace":98.0,"recent_trend":-0.54},
    "SAC": {"ppg":117.2,"opp_ppg":116.4,"net_rating":0.8, "win_pct":0.476,"wins":39,"losses":43,"off_rating":115.5,"def_rating":114.8,"pace":101.4,"recent_trend":0.08},
    "PHX": {"ppg":113.1,"opp_ppg":116.8,"net_rating":-3.7,"win_pct":0.390,"wins":32,"losses":50,"off_rating":111.8,"def_rating":115.1,"pace":99.6,"recent_trend":-0.52},
    "MIN": {"ppg":110.9,"opp_ppg":107.8,"net_rating":3.1, "win_pct":0.598,"wins":49,"losses":33,"off_rating":109.7,"def_rating":107.0,"pace":97.6,"recent_trend":0.38},
    "DEN": {"ppg":115.8,"opp_ppg":112.4,"net_rating":3.4, "win_pct":0.610,"wins":50,"losses":32,"off_rating":114.2,"def_rating":111.0,"pace":98.9,"recent_trend":0.44},
    "DAL": {"ppg":115.4,"opp_ppg":113.8,"net_rating":1.6, "win_pct":0.524,"wins":43,"losses":39,"off_rating":113.8,"def_rating":112.3,"pace":98.7,"recent_trend":0.10},
    "NOP": {"ppg":107.8,"opp_ppg":115.5,"net_rating":-7.7,"win_pct":0.317,"wins":26,"losses":56,"off_rating":106.5,"def_rating":113.9,"pace":97.1,"recent_trend":-1.08},
    "SAS": {"ppg":111.2,"opp_ppg":116.5,"net_rating":-5.3,"win_pct":0.415,"wins":34,"losses":48,"off_rating":109.9,"def_rating":114.9,"pace":99.1,"recent_trend":-0.68},
    "MEM": {"ppg":116.4,"opp_ppg":113.2,"net_rating":3.2, "win_pct":0.573,"wins":47,"losses":35,"off_rating":114.7,"def_rating":111.5,"pace":100.8,"recent_trend":0.46},
    "UTA": {"ppg":109.3,"opp_ppg":118.1,"net_rating":-8.8,"win_pct":0.293,"wins":24,"losses":58,"off_rating":108.1,"def_rating":116.5,"pace":99.3,"recent_trend":-1.25},
    "POR": {"ppg":107.6,"opp_ppg":116.8,"net_rating":-9.2,"win_pct":0.280,"wins":23,"losses":59,"off_rating":106.4,"def_rating":115.2,"pace":98.5,"recent_trend":-1.32},
    "DET": {"ppg":111.9,"opp_ppg":113.8,"net_rating":-1.9,"win_pct":0.476,"wins":39,"losses":43,"off_rating":110.6,"def_rating":112.5,"pace":99.4,"recent_trend":-0.25},
    "PHI": {"ppg":112.4,"opp_ppg":115.8,"net_rating":-3.4,"win_pct":0.402,"wins":33,"losses":49,"off_rating":111.2,"def_rating":114.3,"pace":98.1,"recent_trend":-0.47},
}


def _load_nba_stats_from_espn():
    """Fetch live NBA team stats from ESPN.

    ESPN's `/teams` list endpoint no longer includes per-team stats, so we now fetch
    each team's detail page to obtain its record and points totals.

    If this fails, we fall back to the scoreboard-based record extractor.
    """
    if _nba_stats_cache:
        return

    try:
        print("[INFO] Fetching NBA stats from ESPN (team details)...")

        resp = requests.get(
            "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams",
            headers=ESPN_HEADERS,
            params={"limit": 32},
            timeout=10,
        )
        if resp.status_code != 200:
            print(f"[WARN] ESPN teams list: HTTP {resp.status_code}")
            _load_nba_stats_from_espn_scoreboard()
            return

        data = resp.json()
        teams = (data.get("sports", [{}])[0]
                     .get("leagues", [{}])[0]
                     .get("teams", []))

        if not teams:
            print("[WARN] ESPN teams list returned no teams")
            _load_nba_stats_from_espn_scoreboard()
            return

        # Build abbreviation -> ESPN team ID map
        abbrev_to_id = {
            entry.get("team", {}).get("abbreviation"): entry.get("team", {}).get("id")
            for entry in teams
            if entry.get("team", {}).get("abbreviation") and entry.get("team", {}).get("id")
        }

        # Fetch each team's detail page to extract record stats
        for abbrev, espn_id in abbrev_to_id.items():
            try:
                resp = requests.get(
                    f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{espn_id}",
                    headers=ESPN_HEADERS,
                    timeout=10,
                )
                if resp.status_code != 200:
                    continue

                team_data = resp.json().get("team", {})
                record_items = team_data.get("record", {}).get("items", [])
                if not record_items:
                    continue

                total_item = next((i for i in record_items if i.get("type") == "total"), record_items[0])
                stats_raw = {s.get("name"): s.get("value") for s in total_item.get("stats", [])}

                w = float(stats_raw.get("wins", 0))
                l = float(stats_raw.get("losses", 0))
                gp = w + l
                w_pct = w / gp if gp > 0 else 0.5

                ppg = float(stats_raw.get("avgPointsFor", 113.0))
                opp_ppg = float(stats_raw.get("avgPointsAgainst", 113.0))
                diff = float(stats_raw.get("differential", ppg - opp_ppg))

                _nba_stats_cache[abbrev] = {
                    "ppg":          round(ppg, 1),
                    "opp_ppg":      round(opp_ppg, 1),
                    "net_rating":   round(diff, 1),
                    "off_rating":   round(ppg * 0.9 + 10, 1),
                    "def_rating":   round(opp_ppg * 0.9 + 10, 1),
                    "pace":         98.5,
                    "recent_trend": round((w_pct - 0.5) * 4, 2),
                    "wins":         int(w),
                    "losses":       int(l),
                    "win_pct":      round(w_pct, 3),
                }

            except Exception:
                continue

        if _nba_stats_cache:
            sample = list(_nba_stats_cache.items())[:3]
            for abbr, s in sample:
                print(f"[INFO] ESPN {abbr}: W={s['wins']} L={s['losses']} W%={s['win_pct']:.3f}")
            print(f"[INFO] ESPN NBA stats loaded: {len(_nba_stats_cache)} teams ✅")
        else:
            print("[WARN] ESPN team detail stats could not be loaded; falling back to scoreboard records.")
            _load_nba_stats_from_espn_scoreboard()

    except Exception as e:
        print(f"[WARN] ESPN NBA stats fetch failed: {e}")
        _load_nba_stats_from_espn_scoreboard()


def _load_nba_stats_from_espn_scoreboard():
    """Fallback: build W/L stats from ESPN scoreboard season records."""
    if _nba_stats_cache:
        return
    try:
        resp = requests.get(
            "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
            headers=ESPN_HEADERS, timeout=10
        )
        if resp.status_code != 200:
            return
        data   = resp.json()
        events = data.get("events", [])
        seen   = set()
        for event in events:
            for comp in event.get("competitions", []):
                for team in comp.get("competitors", []):
                    t      = team.get("team", {})
                    abbrev = t.get("abbreviation", "")
                    if not abbrev or abbrev in seen:
                        continue
                    seen.add(abbrev)
                    record = team.get("records", [{}])
                    w = l = 0
                    for r in record:
                        if r.get("type") == "total":
                            parts = r.get("summary","0-0").split("-")
                            try:
                                w, l = int(parts[0]), int(parts[1])
                            except Exception:
                                pass
                    gp    = w + l or 82
                    w_pct = w / gp
                    _nba_stats_cache[abbrev] = {
                        "ppg": 113.0, "opp_ppg": 113.0,
                        "net_rating": round((w_pct - 0.5) * 20, 1),
                        "off_rating": 112.0, "def_rating": 112.0,
                        "pace": 98.5,
                        "recent_trend": round((w_pct - 0.5) * 4, 2),
                        "wins": w, "losses": l,
                        "win_pct": round(w_pct, 3),
                    }
        if _nba_stats_cache:
            print(f"[INFO] ESPN scoreboard fallback: {len(_nba_stats_cache)} teams ✅")
    except Exception as e:
        print(f"[WARN] ESPN scoreboard fallback failed: {e}")

def _load_bdl_team_abbrev_map():
    """
    Build BallDontLie team_id → abbreviation map.
    First tries the API, falls back to hardcoded map if API fails or IDs mismatch.
    """
    global _nba_bdl_id_map
    if _nba_bdl_id_map:
        return
    try:
        resp = nba_client.get(f"{BALLDONTLIE_URL}/teams", params={"per_page": 100})
        if resp and resp.status_code == 200:
            for t in resp.json().get("data", []):
                tid   = t.get("id")
                abbr  = t.get("abbreviation", "")
                if tid and abbr:
                    _nba_bdl_id_map[tid] = abbr
            print(f"[INFO] BDL team map loaded from API: {len(_nba_bdl_id_map)} teams.")
    except Exception as e:
        print(f"[WARN] BDL team map API failed: {e}")

    # Always merge hardcoded map as safety net for any missing IDs
    for tid, abbr in _BDL_ID_TO_ABBREV.items():
        if tid not in _nba_bdl_id_map:
            _nba_bdl_id_map[tid] = abbr
    print(f"[INFO] BDL team map final size: {len(_nba_bdl_id_map)} teams.")


def fetch_nba_team_season_stats(team_id, season="2024-25", team_name=""):
    """Look up NBA stats for a team using its BallDontLie team_id.

    This function will ensure the stats cache is loaded (via ESPN) the first time
    it is called so callers don't need to worry about initialization order.
    Falls back to team name matching if ID lookup fails.
    """
    if not _nba_stats_cache:
        _ensure_nba_stats_loaded(season)

    abbrev = _nba_bdl_id_map.get(team_id, "")
    stats  = _nba_stats_cache.get(abbrev, {})

    # Fallback: match by team name if ID lookup failed
    if not stats and team_name:
        # Extract abbreviation from team name e.g. "Boston Celtics" → "BOS"
        NAME_TO_ABBREV = {
            "Atlanta Hawks":"ATL","Boston Celtics":"BOS","Brooklyn Nets":"BKN",
            "Charlotte Hornets":"CHA","Chicago Bulls":"CHI","Cleveland Cavaliers":"CLE",
            "Dallas Mavericks":"DAL","Denver Nuggets":"DEN","Detroit Pistons":"DET",
            "Golden State Warriors":"GSW","Houston Rockets":"HOU","Indiana Pacers":"IND",
            "LA Clippers":"LAC","Los Angeles Clippers":"LAC","Los Angeles Lakers":"LAL",
            "Memphis Grizzlies":"MEM","Miami Heat":"MIA","Milwaukee Bucks":"MIL",
            "Minnesota Timberwolves":"MIN","New Orleans Pelicans":"NOP",
            "New York Knicks":"NYK","Oklahoma City Thunder":"OKC","Orlando Magic":"ORL",
            "Philadelphia 76ers":"PHI","Phoenix Suns":"PHX","Portland Trail Blazers":"POR",
            "Sacramento Kings":"SAC","San Antonio Spurs":"SAS","Toronto Raptors":"TOR",
            "Utah Jazz":"UTA","Washington Wizards":"WAS",
        }
        abbrev = NAME_TO_ABBREV.get(team_name, "")
        if abbrev:
            stats = _nba_stats_cache.get(abbrev, {})
            if stats:
                print(f"[INFO] NBA: matched {team_name!r} → {abbrev}")

    if not stats:
        # Try ESPN abbreviation mapping (e.g. WAS -> WSH, GSW -> GS)
        espn_abbrev = _ESPN_ABBREV_MAP.get(abbrev, abbrev)
        if espn_abbrev != abbrev:
            stats = _nba_stats_cache.get(espn_abbrev, {})
            if stats:
                print(f"[INFO] NBA: matched {team_name!r} ({abbrev}) → ESPN {espn_abbrev}")

    if not stats:
        print(f"[WARN] NBA: no stats for team_id={team_id} name={team_name!r} abbrev={abbrev!r}")
    return stats


def _ensure_nba_stats_loaded(season="2024-25"):
    """
    Load NBA team stats ONCE before the game loop.
    Source: ESPN standings API (live, no key, no timeout issues).
    """
    _load_bdl_team_abbrev_map()
    _load_nba_stats_from_espn()
    if not _nba_stats_cache:
        print("[WARN] ESPN NBA stats failed — no stats available. Predictions will use defaults.")

def fetch_nba_fixtures():
    """
    Fetch NBA games for TODAY in WAT (midnight to midnight WAT).
    Day boundary = 23:59 WAT. Games at 00:00 WAT belong to the NEXT day.

    Logic:
      - WAT today = now_wat.date()
      - Only include games whose tip-off in WAT falls on today's WAT date
      - Skip finished games and games already started >2h ago
    """
    from datetime import timedelta as _td

    now_utc  = datetime.now(timezone.utc)
    now_wat  = now_utc.astimezone(WAT_OFFSET)

    # WAT day boundaries: 00:00 WAT → 23:59 WAT today
    today_wat_date = now_wat.date()
    day_start_wat  = datetime(today_wat_date.year, today_wat_date.month,
                              today_wat_date.day, 0, 0, tzinfo=WAT_OFFSET)
    day_end_wat    = datetime(today_wat_date.year, today_wat_date.month,
                              today_wat_date.day, 23, 59, 59, tzinfo=WAT_OFFSET)

    # Fetch UTC dates that overlap with today WAT
    # WAT is UTC+1, so WAT today spans UTC yesterday-evening to UTC tonight
    utc_dates = set()
    utc_dates.add(now_utc.strftime("%Y-%m-%d"))
    utc_dates.add((now_utc - _td(days=1)).strftime("%Y-%m-%d"))
    utc_dates.add((now_utc + _td(days=1)).strftime("%Y-%m-%d"))

    all_games = []
    for date_str in sorted(utc_dates):
        try:
            resp = nba_client.get(
                f"{BALLDONTLIE_URL}/games",
                params={"dates[]": date_str, "per_page": 50},
            )
        except Exception as e:
            print(f"[ERROR] BallDontLie ({date_str}): {e}")
            continue
        if not resp or resp.status_code != 200:
            print(f"[WARN] BallDontLie ({date_str}): HTTP {getattr(resp, 'status_code', '?')}")
            continue
        all_games.extend(resp.json().get("data", []))

    seen  = set()
    games = []

    for g in all_games:
        gid = g.get("id")
        if gid in seen:
            continue
        seen.add(gid)

        if g.get("status", "") in ("Final", "Finished"):
            continue

        home   = g.get("home_team",    {})
        away   = g.get("visitor_team", {})
        tipoff = g.get("datetime") or g.get("date")

        tip_dt = None
        if tipoff:
            try:
                tip_dt = datetime.fromisoformat(str(tipoff).replace("Z", "+00:00"))
                if tip_dt.tzinfo is None:
                    tip_dt = tip_dt.replace(tzinfo=timezone.utc)
            except Exception:
                tip_dt = None

        if tip_dt:
            tip_wat = tip_dt.astimezone(WAT_OFFSET)
            # Include today's games (00:00-23:59 WAT)
            # PLUS next day 00:00-00:59 WAT only (midnight hour, clearly labelled)
            next_day_midnight_cutoff = datetime(
                today_wat_date.year, today_wat_date.month, today_wat_date.day,
                tzinfo=WAT_OFFSET
            ) + _td(days=1, hours=1)   # next day up to 00:59 WAT only
            if tip_wat.date() == today_wat_date:
                pass   # today's game — include
            elif tip_wat < next_day_midnight_cutoff:
                pass   # 00:00-00:59 WAT next day — include, marked as next day
            else:
                continue   # 01:00 WAT onwards tomorrow — exclude
            if tip_dt < now_utc - _td(hours=2):
                continue   # already well underway

        games.append({
            "sport":      "basketball",
            "fixture_id": gid,
            "league":     "NBA",
            "home_team":  home.get("full_name", home.get("name", "Home")),
            "away_team":  away.get("full_name", away.get("name", "Away")),
            "home_id":    home.get("id"),
            "away_id":    away.get("id"),
            "kickoff":    tipoff,
            "venue":      "TBC",
            "win_prob":   None,
        })

    def _sort(g):
        try:
            return datetime.fromisoformat(str(g["kickoff"]).replace("Z", "+00:00"))
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)

    games.sort(key=_sort)
    print(f"[INFO] NBA: {len(games)} games in next 24h "
          f"(now {now_wat.strftime('%H:%M WAT')}).")
    return games

# ── Tennis player cache + embedded rankings ──────────────────────────────────
_tennis_player_cache = {}
_tennis_player_key_map = {}  # player_name (normalized) -> player_key from fixtures

# WTA top rankings (2024-25)
_WTA_RANKINGS = {
    "Swiatek":1,"Sabalenka":2,"Gauff":3,"Rybakina":4,"Pegula":5,
    "Zheng":6,"Andreeva":7,"Paolini":8,"Navarro":9,"Badosa":10,
    "Vekic":11,"Kasatkina":12,"Samsonova":13,"Svitolina":14,"Jabeur":15,
    "Fruhvirtova":21,"Andreescu":25,"Trevisan":28,"Sasnovich":30,
    "Jeanjean":32,"Starodubtseva":35,"Masarova":38,"Dolehide":40,
    "Rakhimova":42,"Brengle":45,"Golubic":48,"Zarazua":55,"Kalieva":60,
    "Stakusic":65,"Seidel":68,"Timofeeva":70,"Galfi":72,"Inglis":75,
    "Stefanini":78,"Bartunkova":80,"Radivojevic":85,"Kraus":90,
    "Ekstrand":95,"Osuigwe":98,"Hunter":100,"Parry":102,"Stoiana":108,
    "Yuan":112,"Gorgodze":115,"Gibson":118,"Birrell":120,
    "Jimenez Kasintseva":125,"Lepchenko":130,"Volynets":135,"Frodin":150,
    "Zakharova":155,"Tararudee":160,"Lamens":165,"Buse":170,"Echargui":175,
    "Sun":180,"Schoolkate":185,"Aksu":190,"Fery":200,"Burruchaga":110,
}

# ATP top rankings (2024-25)
_ATP_RANKINGS = {
    "Sinner":1,"Alcaraz":2,"Djokovic":3,"Zverev":4,"Medvedev":5,
    "Rune":6,"Ruud":7,"Hurkacz":8,"De Minaur":9,"Fritz":10,
    "Tsitsipas":11,"Draper":12,"Shelton":14,"Paul":15,"Rublev":16,
    "Dimitrov":17,"Jarry":26,"Bonzi":45,"O'Connell":42,"McDonald":43,
    "Hijikata":44,"Harris":41,"Garin":50,"Draxl":55,"Svrcina":60,
    "Basavareddy":70,"Dhamne":80,"Van Assche":35,"Riedi":40,
    "Willwerth":95,"Pinnington Jones":100,"Bolt":90,"Clarke":85,
    "Kypson":75,"Mejia":80,"Blockx":65,"Walton":110,"Bellucci":55,
    "Sweeny":95,"Vukic":70,"Tirante":85,"Merida Aguilar":90,
    "Miguel":100,"Jodar":105,"Landaluce":60,"Watanuki":75,
    "Svjada":88,"Shimabukuro":92,"Mochizuki":97,"Rodesch":115,
}


def _normalize_player_name(name: str) -> str:
    """Normalize player name for consistent mapping."""
    if not name:
        return ""
    return " ".join(name.strip().split()).lower()


def _rank_from_name(player_name):
    """Extract rank from embedded tables by matching player last name."""
    if not player_name:
        return 999
    name = player_name.strip()
    parts = name.split()
    # Candidates: last word, full name after initial, whole name
    candidates = set()
    if len(parts) >= 2:
        candidates.add(parts[-1])
        candidates.add(" ".join(parts[1:]))
        candidates.add(parts[0])
    candidates.add(name)
    for table in [_WTA_RANKINGS, _ATP_RANKINGS]:
        for cand in candidates:
            if cand in table:
                return table[cand]
            for key, rank in table.items():
                if key.lower() == cand.lower():
                    return rank
                if len(cand) > 3 and (cand.lower() in key.lower() or key.lower() in cand.lower()):
                    return rank
    return 999


def fetch_tennis_player_stats(player_key, surface="hard", player_name=""):
    """
    Fetch player stats. If player_key is empty (free tier), falls back to
    name-based ranking lookup from embedded ATP/WTA tables.
    """
    cache_key = player_key or player_name
    if not cache_key:
        return {}
    if cache_key in _tennis_player_cache:
        return _tennis_player_cache[cache_key]

    stats = {}

    # -- Try to resolve missing player_key using fixture-derived mapping --
    if not player_key and player_name:
        mapped = _tennis_player_key_map.get(_normalize_player_name(player_name))
        if mapped:
            print(f"[INFO] Tennis: resolved player_key for {player_name!r} via fixture cache")
            player_key = mapped

    # -- Try API first if we have a key --
    if player_key:
        for key in TENNIS_KEYS:
            try:
                r = requests.get(API_TENNIS_URL, params={
                    "method": "get_players", "player_key": player_key, "APIkey": key
                }, timeout=8)
                if r.status_code == 200:
                    body = r.json()
                    if body.get("success") == 1:
                        results = body.get("result", [])
                        if results:
                            p = results[0]
                            player_stats = p.get("stats", [])
                            if player_stats:
                                singles = [s for s in player_stats if s.get("type") == "singles"] or player_stats
                                singles.sort(key=lambda x: str(x.get("season", "0")), reverse=True)
                                latest   = singles[0]
                                rank     = int(latest.get("rank", 999) or 999)
                                won      = int(latest.get("matches_won",  0) or 0)
                                lost     = int(latest.get("matches_lost", 0) or 0)
                                total    = won + lost
                                surf_map = {"hard":("hard_won","hard_lost"),"clay":("clay_won","clay_lost"),"grass":("grass_won","grass_lost"),"indoor":("hard_won","hard_lost")}
                                sw_k, sl_k = surf_map.get(surface, ("hard_won","hard_lost"))
                                sw = int(latest.get(sw_k, 0) or 0)
                                sl = int(latest.get(sl_k, 0) or 0)
                                st = sw + sl
                                stats = {
                                    "rank": rank,
                                    "recent_wins": won, "recent_losses": lost,
                                    "serve_win_pct": round(won/total,3) if total>0 else 0.65,
                                    "first_serve_pct": 0.62,
                                    "break_pts_saved_pct": 0.63,
                                    "surface_win_pct": round(sw/st,3) if st>0 else (won/total if total>0 else 0.5),
                                    "days_since_last_match": 1,
                                }
                    break
            except Exception as e:
                print(f"[WARN] Tennis player API {player_key}: {e}")
                continue

    # -- Fallback: name-based rank lookup --
    if not stats and player_name:
        rank = _rank_from_name(player_name)
        if rank < 999:
            # Estimate stats from rank: top-10=elite, top-50=good, top-100=average
            win_pct = max(0.45, min(0.82, 0.82 - (rank - 1) * 0.003))
            stats = {
                "rank": rank,
                "recent_wins": int(win_pct * 10),
                "recent_losses": int((1-win_pct) * 10),
                "serve_win_pct": round(0.72 + (50-rank)*0.001, 3) if rank <= 50 else round(0.68 - (rank-50)*0.0005, 3),
                "first_serve_pct": 0.62,
                "break_pts_saved_pct": 0.63,
                "surface_win_pct": round(win_pct, 3),
                "days_since_last_match": 1,
            }
            print(f"[INFO] Tennis rank lookup: {player_name} → #{rank} (win_pct={win_pct:.2f})")
        else:
            print(f"[WARN] Tennis: no rank found for '{player_name}'")

    _tennis_player_cache[cache_key] = stats
    return stats


def _old_fetch_tennis_player_stats_DEPRECATED(player_key, surface="hard"):
    pass  # replaced above

def _call_tennis_api(params):
    """Helper — tries all TENNIS_KEYS, returns parsed result list or []."""
    for key in TENNIS_KEYS:
        try:
            r = requests.get(API_TENNIS_URL, params={**params, "APIkey": key}, timeout=10)
            if r.status_code == 200:
                body = r.json()
                if body.get("success") == 1:
                    return body.get("result", [])
                print(f"[WARN] Tennis API: {body.get('error', 'unknown error')}")
            elif r.status_code in (403, 429):
                print(f"[ROTATE] Tennis key {r.status_code} — trying next...")
            else:
                print(f"[WARN] Tennis API HTTP {r.status_code}")
        except Exception as e:
            print(f"[ROTATE] Tennis key error: {e} — trying next...")
    return []


def fetch_tennis_fixtures():
    today = datetime.now(WAT_OFFSET).strftime("%Y-%m-%d")  # WAT date

    MAJOR_KEYWORDS = [
        "atp", "wta", "grand slam", "masters", "challenger",
        "miami", "indian wells", "roland", "wimbledon",
        "us open", "australian", "french open",
        "500", "250", "1000",
    ]
    SKIP_KEYWORDS = ["itf", "utr", "futures", "junior"]

    raw_data = _call_tennis_api({
        "method":     "get_fixtures",
        "date_start": today,
        "date_stop":  today,
    })

    if not raw_data:
        print("[WARN] Tennis: no fixtures returned.")
        return []

    print(f"[INFO] Tennis: {len(raw_data)} raw fixtures from API.")
    matches  = []
    seen_ids = set()

    for g in raw_data:
        gid = g.get("event_key")
        if gid in seen_ids:
            continue
        seen_ids.add(gid)

        tournament = g.get("tournament_name", "") or g.get("event_type_type", "")
        t_lower    = tournament.lower()

        if any(kw in t_lower for kw in SKIP_KEYWORDS):
            continue
        if not any(kw in t_lower for kw in MAJOR_KEYWORDS):
            continue

        status = g.get("event_status", "")
        if status in ("Finished", "After Extra Time", "Walkover"):
            continue

        home_p      = g.get("event_first_player",  "Player 1")
        away_p      = g.get("event_second_player", "Player 2")
        home_pkey   = g.get("first_player_key",  "")
        away_pkey   = g.get("second_player_key", "")
        e_date      = g.get("event_date", today)
        e_time      = g.get("event_time", "00:00")
        kickoff     = f"{e_date}T{e_time}:00+00:00"

        # Cache player keys for later lookup (so stats can be fetched even if only name is known)
        if home_pkey:
            tennis_player_key = _normalize_player_name(home_p)
            _tennis_player_key_map[tennis_player_key] = home_pkey
        if away_pkey:
            tennis_player_key = _normalize_player_name(away_p)
            _tennis_player_key_map[tennis_player_key] = away_pkey

        matches.append({
            "sport":          "tennis",
            "fixture_id":     gid,
            "league":         tournament,
            "home_team":      home_p,
            "away_team":      away_p,
            "home_player_key": home_pkey,
            "away_player_key": away_pkey,
            "kickoff":        kickoff,
            "tournament":     tournament,
            "venue":          "TBC",
        })

    print(f"[INFO] Tennis: {len(matches)} qualifying matches today.")
    return matches


# ═══════════════════════════════════════════════════════════════════════════════
# CARD FORMATTERS
# ═══════════════════════════════════════════════════════════════════════════════
def ko_str(kickoff, reference_date=None):
    """Convert any ISO timestamp to WAT. Labels next-day early games clearly."""
    try:
        dt     = datetime.fromisoformat(str(kickoff).replace("Z", "+00:00"))
        dt_wat = dt.astimezone(WAT_OFFSET)
        today  = reference_date or datetime.now(WAT_OFFSET).date()
        if dt_wat.date() != today:
            # Next day early morning (00:00-06:00 WAT) — label the day
            day_name = dt_wat.strftime("%a")   # e.g. "Tue"
            return f"{dt_wat.strftime('%H:%M')} WAT ({day_name})"
        return dt_wat.strftime("%H:%M WAT")
    except Exception:
        return "TBC"


# ── Football card ──────────────────────────────────────────────────────────────
def format_football_card(fix, pred):
    w_e      = {"home": "🏠", "draw": "🤝", "away": "✈️"}.get(pred["winner"], "❓")
    btts_e   = "✅" if pred["btts"] == "Yes" else "❌"
    ou_e     = "⬆️" if pred["over_under"] == "Over 2.5" else "⬇️"
    conf_bar = "█" * (pred["confidence"] // 10) + "░" * (10 - pred["confidence"] // 10)

    vb_block = ""
    if pred.get("value_bets"):
        lines = ["\n🎰 *VALUE BETS*"]
        for vb in pred["value_bets"]:
            lines.append(f"   💎 *{vb['outcome']}* @ `{vb['odd']}` | Edge: `+{vb['value']}%`")
        vb_block = "\n".join(lines)

    return f"""
⚽ *FOOTBALL — {fix['league']}*
🆚 *{fix['home_team']}* vs *{fix['away_team']}*
⏰ `{ko_str(fix['kickoff'])}` 📍 _{fix.get('venue', 'TBC')}_

🎯 {pred['grade']} | {w_e} *{pred['winner_label']}* `{pred['confidence']}%`
`[{conf_bar}]`
🏠`{pred['prob_home']}%` 🤝`{pred['prob_draw']}%` ✈️`{pred['prob_away']}%`

{ou_e} *Goals:* `{pred['over_under']}` _({pred['ou_prob']}%)_  🔢 xG `{pred['expected_goals']}`
{btts_e} *BTTS:* `{pred['btts']}` _({pred['btts_prob']}%)_
🎯 *Score:* `{pred['correct_score']}` _({pred['score_prob']}%)_
{vb_block}
📊 🏠`{pred['home_form']}` ✈️`{pred['away_form']}`
💡 _{pred['key_factor']}_{"" if not pred.get("home_news") else chr(10) + "📰 🏠_" + pred.get("home_news","") + "_ ✈️_" + pred.get("away_news","") + "_"}
""".strip()


# ── Basketball card ────────────────────────────────────────────────────────────
def format_basketball_card(fix, pred):
    w_e      = "🏠" if pred["winner"] == "home" else "✈️"
    conf_bar = "█" * (pred["confidence"] // 10) + "░" * (10 - pred["confidence"] // 10)
    ou_e     = "⬆️" if "Over" in pred["over_under"] else "⬇️"

    return f"""
🏀 *NBA*
🆚 *{fix['home_team']}* vs *{fix['away_team']}*
⏰ `{ko_str(fix['kickoff'])}` 📍 _{fix.get('venue', 'TBC')}_

🎯 {pred['grade']} | {w_e} *{pred['winner_label']}* `{pred['confidence']}%`
`[{conf_bar}]`
🏠`{pred['prob_home']}%`  ✈️`{pred['prob_away']}%`

{ou_e} *Total Points:* `{pred['over_under']}`
   Predicted score: `{pred['pred_score']}`  Total: `{pred['pred_total']}`
📏 *Spread:* `{pred['spread']}`
🏃 *Game pace:* `{pred['game_pace']} poss/48min`

📊 *Team profile:*
   🏠 {fix['home_team']}: `{pred['home_profile']}`
   ✈️  {fix['away_team']}: `{pred['away_profile']}`

💡 _{pred['key_factor']}_{"" if not pred.get("injury_note") else chr(10) + "🏥 " + pred["injury_note"]}{"" if not pred.get("home_recent") else chr(10) + "📊 🏠`" + " ".join({"W":"✅","L":"❌"}.get(c,"⬜") for c in pred.get("home_recent","")) + "` ✈️`" + " ".join({"W":"✅","L":"❌"}.get(c,"⬜") for c in pred.get("away_recent","")) + "`"}
""".strip()


# ── Tennis card ────────────────────────────────────────────────────────────────
def format_tennis_card(fix, pred):
    conf_bar   = "█" * (pred["confidence"] // 10) + "░" * (10 - pred["confidence"] // 10)
    ou_e       = "⬆️" if "Over" in pred["over_under"] else "⬇️"
    rank_home  = pred.get("rank_home", 999)
    rank_away  = pred.get("rank_away", 999)
    rank_line  = ""
    if rank_home < 999 or rank_away < 999:
        rh = f"#{rank_home}" if rank_home < 999 else "NR"
        ra = f"#{rank_away}" if rank_away < 999 else "NR"
        rank_line = f"\n🏅 *Rankings:* 🏠`{rh}`  ✈️`{ra}`"

    return f"""
🎾 *TENNIS — {pred['tournament']}*
🆚 *{fix['home_team']}* vs *{fix['away_team']}*
⏰ `{ko_str(fix['kickoff'])}` 🏟️ _{pred['surface']} Court_{rank_line}

🎯 {pred['grade']} | 🎾 *{pred['winner_label']}* `{pred['confidence']}%`
`[{conf_bar}]`
🏠`{pred['prob_home']}%`  ✈️`{pred['prob_away']}%`

📋 *Format:* `{pred['sets_format']}` | Predicted: `{pred['pred_sets']}`
🎯 *Set handicap:* `{pred['handicap']}`
{ou_e} *Over/Under games:* `{pred['over_under']}` _(pred: {pred['pred_games']} games)_

🎾 *Serve profile:*
   🏠 {fix['home_team']}: `{pred['home_serve']}`
   ✈️  {fix['away_team']}: `{pred['away_serve']}`

💡 _{pred['key_factor']}_{"" if not pred.get("home_news") else chr(10) + "📰 🏠_" + pred.get("home_news","") + "_ ✈️_" + pred.get("away_news","") + "_"}
""".strip()


# ── Summary card ───────────────────────────────────────────────────────────────
def format_sport_summary(sport_emoji, sport_name, results, date_str):
    grade_e      = {"HIGH 🔥": "🔥", "MEDIUM ⚡": "⚡", "LOW 🌡️": "🌡️"}
    lines        = [
        "╔══════════════════════════╗",
        f"{sport_emoji} *{sport_name} PREDICTIONS*",
        f"📅 *{date_str}*",
        "╚══════════════════════════╝\n",
    ]
    value_alerts = []

    for fix, pred in results:
        g     = grade_e.get(pred.get("grade", ""), "")
        w_e   = {"home": "🏠", "draw": "🤝", "away": "✈️"}.get(pred.get("winner", ""), "🏅")
        ko    = ko_str(fix["kickoff"])
        label = pred.get("winner_label", "?")
        conf  = pred.get("confidence",  "?")

        if fix["sport"] == "football":
            ou   = pred.get("over_under", "")
            btts = pred.get("btts", "")
            lines.append(
                f"{g} *{fix['home_team']}* vs *{fix['away_team']}*\n"
                f"   {w_e} `{label}` ({conf}%) | "
                f"{'⬆️' if 'Over' in ou else '⬇️'}{ou} | BTTS:{btts} | ⏰`{ko}`\n"
            )
        elif fix["sport"] == "basketball":
            ou  = pred.get("over_under", "")
            pts = pred.get("pred_score", "")
            lines.append(
                f"{g} *{fix['home_team']}* vs *{fix['away_team']}*\n"
                f"   {w_e} `{label}` ({conf}%) | "
                f"{'⬆️' if 'Over' in ou else '⬇️'}{ou} | Score:`{pts}` | ⏰`{ko}`\n"
            )
        elif fix["sport"] == "tennis":
            surf = pred.get("surface", "")
            lines.append(
                f"{g} *{fix['home_team']}* vs *{fix['away_team']}*\n"
                f"   🎾 `{label}` ({conf}%) | {surf} | ⏰`{ko}`\n"
            )

        for vb in pred.get("value_bets", []):
            value_alerts.append(
                f"💎 *{fix['home_team']} v {fix['away_team']}* — "
                f"{vb['outcome']} @ `{vb['odd']}` (+{vb['value']}%)"
            )

    if value_alerts:
        lines.append("━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("🎰 *VALUE BETS*")
        lines += value_alerts

    lines.append("\n⚠️ _For entertainment only. Bet responsibly._")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# MATCHDAY SCHEDULE CARD
# ═══════════════════════════════════════════════════════════════════════════════
def format_matchday_schedule(date_str, football_fixtures, nba_fixtures, tennis_fixtures):
    """
    Sends one clean card showing all games for the day across all sports.
    No predictions — just teams + kickoff times in WAT.
    """
    lines = [
        "╔══════════════════════════╗",
        f"📅 *TODAY'S FIXTURES*",
        f"*{date_str}*",
        "╚══════════════════════════╝",
    ]

    # ── Football ──────────────────────────────────────────────────────────────
    if football_fixtures:
        lines.append(f"\n⚽ *FOOTBALL* ({len(football_fixtures)} games)")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━")
        # Group by league
        by_league = {}
        for f in football_fixtures:
            by_league.setdefault(f["league"], []).append(f)
        for league, games in by_league.items():
            lines.append(f"\n🏆 _{league}_")
            for f in games:
                lines.append(f"  ⏰`{ko_str(f['kickoff'])}` {f['home_team']} vs {f['away_team']}")

    # ── NBA ───────────────────────────────────────────────────────────────────
    if nba_fixtures:
        lines.append(f"\n🏀 *NBA* ({len(nba_fixtures)} games)")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━")
        for f in nba_fixtures:
            lines.append(f"  ⏰`{ko_str(f['kickoff'])}` {f['home_team']} vs {f['away_team']}")

    # ── Tennis ────────────────────────────────────────────────────────────────
    if tennis_fixtures:
        lines.append(f"\n🎾 *TENNIS* ({len(tennis_fixtures)} matches)")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━")
        # Group by tournament
        by_tournament = {}
        for f in tennis_fixtures:
            by_tournament.setdefault(f["tournament"], []).append(f)
        for tournament, matches in by_tournament.items():
            lines.append(f"\n🏟️ _{tournament}_")
            for f in matches:
                lines.append(f"  ⏰`{ko_str(f['kickoff'])}` {f['home_team']} vs {f['away_team']}")

    total = len(football_fixtures) + len(nba_fixtures) + len(tennis_fixtures)
    lines.append(f"\n━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📊 *{total} total games today*")
    lines.append("_Predictions follow below_ 👇")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════
def run_predictions():
    validate_config()
    sender = TelegramSender(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    f_pred = FootballPredictor()
    b_pred = BasketballPredictor()
    t_pred = TennisPredictor()

    print("[INFO] Testing Telegram connection...")
    if not sender.test_connection():
        print("[ERROR] Telegram unreachable.")
        sys.exit(1)

    now_wat  = datetime.now(WAT_OFFSET)
    date_str = now_wat.strftime("%A, %d %B %Y")

    # ── STEP 1: Fetch all fixtures first ──────────────────────────────────────
    print("\n[INFO] Fetching all fixtures...")
    football_fixtures = fetch_football_fixtures()
    nba_fixtures      = fetch_nba_fixtures()
    tennis_fixtures   = fetch_tennis_fixtures()

    total_games = len(football_fixtures) + len(nba_fixtures) + len(tennis_fixtures)

    # ── STEP 2: Send full matchday schedule ───────────────────────────────────
    print("[INFO] Sending matchday schedule...")
    sender.send_message(
        f"🤖 *DAILY SPORTS BOT*\n"
        f"📅 *{date_str}*\n"
        f"⚽ Football | 🏀 NBA | 🎾 Tennis\n"
        f"━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode="Markdown"
    )

    if total_games == 0:
        sender.send_message("📭 No games found for today across all sports.", parse_mode="Markdown")
        return

    sender.send_message(
        format_matchday_schedule(date_str, football_fixtures, nba_fixtures, tennis_fixtures),
        parse_mode="Markdown"
    )

    # ── STEP 3: Predictions per sport ─────────────────────────────────────────
    sender.send_message("🔮 *PREDICTIONS LOADING...*", parse_mode="Markdown")

    # ── Football ──────────────────────────────────────────────────────────────
    print("\n[INFO] === FOOTBALL PREDICTIONS ===")
    football_results = []
    football_skipped = []

    for fix in football_fixtures:
        hs   = fetch_football_team_stats(fix["home_id"], fix["league_id"])
        aws  = fetch_football_team_stats(fix["away_id"], fix["league_id"])
        h2h  = fetch_h2h(fix["home_id"], fix["away_id"])
        odds = fetch_football_odds(fix["fixture_id"])
        pred = f_pred.predict(fix, hs, aws, h2h, odds=odds)
        if pred.get("skip"):
            football_skipped.append(f"⏭ {fix['home_team']} v {fix['away_team']} — {pred['reason']}")
            continue
        # Apply live data adjustments (ESPN form)
        try:
            from live_data import get_football_team_form
            home_form = get_football_team_form(fix["home_team"], fix["league"])
            away_form = get_football_team_form(fix["away_team"], fix["league"])
            if home_form["last5"] or away_form["last5"]:
                # Count wins in last 5 for each team
                hw = home_form["last5"].count("W")
                aw = away_form["last5"].count("W")
                form_diff = (hw - aw) * 1.5   # each win diff = 1.5% shift
                pred["prob_home"] = min(92, max(5, pred["prob_home"] + form_diff))
                pred["prob_away"] = min(92, max(5, pred["prob_away"] - form_diff))
                pred["prob_draw"] = max(5, 100 - pred["prob_home"] - pred["prob_away"])
                if home_form["last5"]:
                    pred["home_form"] = " ".join(
                        {"W":"✅","D":"🟡","L":"❌"}.get(c,"⬜") for c in home_form["last5"]
                    )
                if away_form["last5"]:
                    pred["away_form"] = " ".join(
                        {"W":"✅","D":"🟡","L":"❌"}.get(c,"⬜") for c in away_form["last5"]
                    )
        except Exception:
            pass
        football_results.append((fix, pred))

    if football_results:
        sender.send_message(format_sport_summary("⚽", "FOOTBALL", football_results, date_str), parse_mode="Markdown")
        for fix, pred in football_results:
            sender.send_message(format_football_card(fix, pred), parse_mode="Markdown")
    elif football_fixtures:
        sender.send_message("⚽ Football fixtures found but no qualifying predictions (insufficient data).", parse_mode="Markdown")
    else:
        sender.send_message("⚽ No football matches today.", parse_mode="Markdown")

    # ── NBA ───────────────────────────────────────────────────────────────────
    print("\n[INFO] === NBA PREDICTIONS ===")
    nba_results = []

    # Load all team stats ONCE before the loop (NBA.com or fallback)
    _ensure_nba_stats_loaded()

    for fix in nba_fixtures:
        wp   = fix.pop("win_prob", None)
        hs   = fetch_nba_team_season_stats(fix.get("home_id"), team_name=fix.get("home_team",""))
        aws  = fetch_nba_team_season_stats(fix.get("away_id"), team_name=fix.get("away_team",""))
        pred = b_pred.predict(fix, home_stats=hs, away_stats=aws, api_win_prob=wp)
        # Apply live data adjustments (ESPN injuries + back-to-back)
        try:
            from live_data import (get_nba_injuries, get_nba_team_form,
                                   injury_impact, back_to_back_impact)
            home_inj  = get_nba_injuries(fix.get("home_team",""))
            away_inj  = get_nba_injuries(fix.get("away_team",""))
            home_form = get_nba_team_form(fix.get("home_team",""))
            away_form = get_nba_team_form(fix.get("away_team",""))

            home_adj = injury_impact(home_inj) + back_to_back_impact(home_form)
            away_adj = injury_impact(away_inj) + back_to_back_impact(away_form)

            pred["prob_home"] = min(92, max(5, pred["prob_home"] + home_adj - away_adj))
            pred["prob_away"] = 100 - pred["prob_home"]

            # Add injury note to card
            notes = []
            if home_inj:
                out = [i["name"].split()[-1] for i in home_inj if "out" in i.get("status","").lower()]
                if out: notes.append(f"🏠 Out: {', '.join(out[:3])}")
            if away_inj:
                out = [i["name"].split()[-1] for i in away_inj if "out" in i.get("status","").lower()]
                if out: notes.append(f"✈️ Out: {', '.join(out[:3])}")
            if home_form.get("back_to_back"):
                notes.append(f"🏠 Back-to-back fatigue")
            if away_form.get("back_to_back"):
                notes.append(f"✈️ Back-to-back fatigue")
            if notes:
                pred["injury_note"] = " | ".join(notes)
            if home_form.get("last5"):
                pred["home_recent"] = home_form["last5"]
            if away_form.get("last5"):
                pred["away_recent"] = away_form["last5"]
        except Exception as e:
            print(f"[WARN] Live NBA data: {e}")
        nba_results.append((fix, pred))

    if nba_results:
        sender.send_message(format_sport_summary("🏀", "NBA BASKETBALL", nba_results, date_str), parse_mode="Markdown")
        for fix, pred in nba_results:
            sender.send_message(format_basketball_card(fix, pred), parse_mode="Markdown")
    else:
        sender.send_message("🏀 No NBA games today.", parse_mode="Markdown")

    # ── Tennis ────────────────────────────────────────────────────────────────
    print("\n[INFO] === TENNIS PREDICTIONS ===")
    tennis_results = []

    for fix in tennis_fixtures:
        from tennis_predictor import TennisPredictor as _TP
        surface    = _TP()._detect_surface(fix.get("tournament", ""))
        # Pass both player_key AND player_name — name used as fallback when key is empty
        home_stats = fetch_tennis_player_stats(
            fix.get("home_player_key", ""), surface, player_name=fix.get("home_team", "")
        )
        away_stats = fetch_tennis_player_stats(
            fix.get("away_player_key", ""), surface, player_name=fix.get("away_team", "")
        )
        pred = t_pred.predict(fix, home_stats=home_stats, away_stats=away_stats)
        # Apply news sentiment adjustment
        try:
            from news_sentiment import get_team_sentiment, apply_sentiment_adjustment
            home_sent = get_team_sentiment(fix.get("home_team",""), "tennis")
            away_sent = get_team_sentiment(fix.get("away_team",""), "tennis")
            if home_sent["available"] or away_sent["available"]:
                pred["prob_home"] = apply_sentiment_adjustment(pred["prob_home"], home_sent["score"])
                pred["prob_away"] = apply_sentiment_adjustment(pred["prob_away"], away_sent["score"])
                pred["home_news"] = home_sent["summary"]
                pred["away_news"] = away_sent["summary"]
        except Exception:
            pass
        tennis_results.append((fix, pred))

    if tennis_results:
        sender.send_message(format_sport_summary("🎾", "TENNIS", tennis_results, date_str), parse_mode="Markdown")
        for fix, pred in tennis_results:
            sender.send_message(format_tennis_card(fix, pred), parse_mode="Markdown")
    else:
        sender.send_message("🎾 No major tennis matches today.", parse_mode="Markdown")

    # ── Done ──────────────────────────────────────────────────────────────────
    total_preds = len(football_results) + len(nba_results) + len(tennis_results)
    print(f"\n[DONE] {total_preds} predictions sent — "
          f"{len(football_results)} football, {len(nba_results)} NBA, {len(tennis_results)} tennis.")
    sender.send_message(
        f"✅ *All done!*\n"
        f"📊 `{total_games}` games today | `{total_preds}` predictions sent\n"
        f"⚽ `{len(football_results)}` football | "
        f"🏀 `{len(nba_results)}` NBA | "
        f"🎾 `{len(tennis_results)}` tennis",
        parse_mode="Markdown"
    )


if __name__ == "__main__":
    run_predictions()

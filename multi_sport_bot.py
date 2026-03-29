"""
Multi-Sport Prediction Bot v7
Sports: Football | NBA Basketball | Tennis
"""

import os
import sys
import socket
import requests
from datetime import datetime, timezone, timedelta, time as dt_time

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

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
# Include early next-day fixtures in today's slate up to this hour (WAT).
# Example: 3 means include 00:00�?"02:59 WAT fixtures from tomorrow in "today".
EARLY_NEXT_DAY_CUTOFF_HOUR_WAT = int(os.getenv("EARLY_NEXT_DAY_CUTOFF_HOUR_WAT", "3") or "3")


def _wat_window_end_for_today(wat_today):
    """
    End of the "today slate" window in WAT, inclusive of early-next-day games.
    Returns a datetime in WAT.
    """
    from datetime import timedelta as _td
    day_start = datetime(wat_today.year, wat_today.month, wat_today.day, 0, 0, 0, tzinfo=WAT_OFFSET)
    # Cutoff is tomorrow at EARLY_NEXT_DAY_CUTOFF_HOUR_WAT:00 WAT (exclusive).
    return day_start + _td(days=1, hours=EARLY_NEXT_DAY_CUTOFF_HOUR_WAT)

# One shared session for non-keyed endpoints (ESPN, tennis, etc.).
# Default: ignore HTTP(S)_PROXY env vars because they are often misconfigured on Windows.
_http = requests.Session()
try:
    v = os.getenv("REQUESTS_TRUST_ENV", "").strip().lower()
    _http.trust_env = True if v in ("1", "true", "yes") else False
except Exception:
    _http.trust_env = False

# �"?�"?�"? CONFIG �"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID",   "")

# �"?�"?�"? API BASE URLS �"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?
FOOTBALL_URL    = "https://v3.football.api-sports.io"
FOOTBALL_DATA_URL = "https://api.football-data.org/v4"
BALLDONTLIE_URL = "https://api.balldontlie.io/v1"
API_TENNIS_URL  = "https://api.api-tennis.com/tennis/"

# �"?�"?�"? KEY POOLS �"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?
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

# RapidAPI Tennis (ATP/WTA/ITF) �?" support multiple keys (comma-separated)
_rapidapi_keys_raw = os.getenv("RAPIDAPI_TENNIS_KEYS", "").strip()
if not _rapidapi_keys_raw:
    _rapidapi_keys_raw = os.getenv("RAPIDAPI_TENNIS_KEY", "").strip()
RAPIDAPI_TENNIS_KEYS = [k.strip() for k in _rapidapi_keys_raw.split(",") if k.strip()]
RAPIDAPI_TENNIS_HOST = os.getenv("RAPIDAPI_TENNIS_HOST", "tennis-api-atp-wta-itf.p.rapidapi.com").strip()
TENNIS_PROVIDER = os.getenv("TENNIS_PROVIDER", "rapidapi").strip().lower()
if not TENNIS_PROVIDER:
    TENNIS_PROVIDER = "rapidapi"
# Allow different providers for fixtures vs player stats
TENNIS_FIXTURES_PROVIDER = os.getenv("TENNIS_FIXTURES_PROVIDER", TENNIS_PROVIDER).strip().lower()
TENNIS_STATS_PROVIDER = os.getenv("TENNIS_STATS_PROVIDER", TENNIS_PROVIDER).strip().lower()

# �"?�"?�"? ROTATING CLIENTS (LAZY) �"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?
# Important for GitHub Actions: jobs may run a single sport without other API keys.
# So we must not require unrelated keys just by importing this module.
football_client = None
football_data_client = None
nba_client = None


def _get_football_client():
    global football_client
    if football_client is not None:
        return football_client
    if not FOOTBALL_KEYS:
        return None
    football_client = RotatingClient(FOOTBALL_KEYS, header_name="x-apisports-key")
    return football_client


def _get_football_data_client():
    global football_data_client
    if football_data_client is not None:
        return football_data_client
    if not FOOTBALL_DATA_KEYS:
        return None
    football_data_client = RotatingClient(FOOTBALL_DATA_KEYS, header_name="X-Auth-Token")
    return football_data_client


def _get_nba_client():
    global nba_client
    if nba_client is not None:
        return nba_client
    if not BALLDONTLIE_KEYS:
        return None
    nba_client = RotatingClient(BALLDONTLIE_KEYS, header_name="Authorization", bearer=True)
    return nba_client


# Tennis uses query-param auth (not header) �?" handled manually in fetcher
FOOTBALL_API_KEY = FOOTBALL_KEYS[0] if FOOTBALL_KEYS else ""

# �"?�"?�"? FOOTBALL LEAGUES �"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?
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

# Mapping from api-football league IDs to ESPN soccer league keys (free, no key).
# Used as a last-resort fallback when api-football has no fixtures and football-data.org is unreachable.
FOOTBALL_ESPN_LEAGUES = {
    39:  "eng.1",             # Premier League
    140: "esp.1",             # La Liga
    141: "esp.2",             # La Liga 2
    135: "ita.1",             # Serie A
    136: "ita.2",             # Serie B
    78:  "ger.1",             # Bundesliga
    79:  "ger.2",             # 2. Bundesliga
    61:  "fra.1",             # Ligue 1
    2:   "uefa.champions",    # UCL
    3:   "uefa.europa",       # Europa League
    848: "uefa.europa.conf",  # Conference League
}

# ESPN-only women's leagues (no api-football IDs required)
FOOTBALL_WOMEN_ESPN_LEAGUES = {
    "eng.w.1":        "Women Super League",
    "esp.w.1":        "Liga F",
    "uefa.wchampions":"UEFA Women's Champions League",
    "usa.nwsl":       "NWSL Women",
}

MIN_MATCHES_PLAYED  = 10
VALUE_BET_THRESHOLD = 0.12
BOOKMAKER_ID        = 6


# �"?�"?�"? VALIDATION �"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?
def validate_config(sport: str | None = None):
    errors = []
    s = (sport or "all").strip().lower()
    req_football = s in ("all", "football")
    req_nba = s in ("all", "nba")
    req_tennis = s in ("all", "tennis")

    if req_football and not FOOTBALL_KEYS:
        errors.append("  �O FOOTBALL_API_KEYS is missing")
    if req_nba and not BALLDONTLIE_KEYS:
        errors.append("  �O BALLDONTLIE_KEYS is missing")
    if req_tennis:
        fix_provider = (TENNIS_FIXTURES_PROVIDER or "").strip().lower()
        stats_provider = (TENNIS_STATS_PROVIDER or "").strip().lower()
        if fix_provider == "api-tennis":
            if not TENNIS_KEYS:
                errors.append("  ? TENNIS_API_KEYS is missing")
        else:
            if not RAPIDAPI_TENNIS_KEYS:
                errors.append("  ? RAPIDAPI_TENNIS_KEY(S) is missing")
        if stats_provider == "rapidapi":
            if not RAPIDAPI_TENNIS_KEYS:
                errors.append("  ? RAPIDAPI_TENNIS_KEY(S) is missing")
        elif stats_provider == "api-tennis":
            if not TENNIS_KEYS:
                errors.append("  ? TENNIS_API_KEYS is missing")

    if not TELEGRAM_BOT_TOKEN:
        errors.append("  �O TELEGRAM_BOT_TOKEN is missing")
    if not TELEGRAM_CHAT_ID:
        errors.append("  �O TELEGRAM_CHAT_ID is missing")
    if errors:
        print("\n[CONFIG ERROR] Fix the following in your .env file:")
        for e in errors:
            print(e)
        sys.exit(1)
    print(f"[INFO] Config loaded OK  "
          f"({len(FOOTBALL_KEYS)} football key(s), "
          f"{len(BALLDONTLIE_KEYS)} NBA key(s), "
          f"{len(TENNIS_KEYS)} tennis key(s))")
    if req_tennis:
        print(f"[INFO] Tennis fixtures provider: {TENNIS_FIXTURES_PROVIDER}")
        print(f"[INFO] Tennis stats provider: {TENNIS_STATS_PROVIDER}")
        if (TENNIS_STATS_PROVIDER or "").strip().lower() == "rapidapi" or (TENNIS_FIXTURES_PROVIDER or "").strip().lower() == "rapidapi":
            print(f"[INFO] RapidAPI tennis keys loaded: {len(RAPIDAPI_TENNIS_KEYS)}")
            print(f"[INFO] RapidAPI tennis host: {RAPIDAPI_TENNIS_HOST}")


# �.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.�
# FOOTBALL FETCHERS
# �.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.�
ESPN_SOCCER_URL = "https://site.api.espn.com/apis/site/v2/sports/soccer"
# Cache ESPN soccer *team* stats, pulled from the per-team endpoint.
# ESPN's `/standings` endpoint sometimes returns `{}` in some networks, so we avoid relying on it.
_espn_soccer_team_cache = {}  # {league_key: {team_id: stats_dict}}
_espn_soccer_form_cache = {}  # {league_key: {team_id: "WDLWW"}}


def _tcp_connectable(host: str, port: int, timeout: float = 1.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _espn_soccer_recent_form(league_key: str, team_id: str, n: int = 5) -> str:
    """
    ESPN soccer fallback: compute recent form (last N completed matches) from the team schedule endpoint.
    Returns a string like "WDLWW" or "" if unavailable.
    """
    if not league_key or not team_id:
        return ""
    league_key = str(league_key)
    team_id = str(team_id)
    league_cache = _espn_soccer_form_cache.setdefault(league_key, {})
    if team_id in league_cache:
        return league_cache.get(team_id, "") or ""

    def _score_val(x):
        if isinstance(x, dict):
            try:
                return float(x.get("value") or 0.0)
            except Exception:
                return 0.0
        try:
            return float(x or 0.0)
        except Exception:
            return 0.0

    url = f"{ESPN_SOCCER_URL}/{league_key}/teams/{team_id}/schedule"
    try:
        resp = _http.get(url, headers=ESPN_HEADERS, timeout=15)
        if resp.status_code != 200:
            return ""
        data = resp.json() or {}
    except Exception:
        return ""

    completed = []
    for ev in (data.get("events") or []):
        comp = (ev.get("competitions") or [{}])[0] or {}
        st = (comp.get("status") or {})
        state = ((st.get("type") or {}).get("state")) or ""
        if state != "post":
            continue
        competitors = comp.get("competitors") or []
        me = opp = None
        for c in competitors:
            if (c.get("team") or {}).get("id") == team_id:
                me = c
            else:
                opp = c
        if not me or not opp:
            continue
        my = _score_val(me.get("score"))
        op = _score_val(opp.get("score"))
        if my > op:
            res = "W"
        elif my < op:
            res = "L"
        else:
            res = "D"
        completed.append((ev.get("date") or "", res))

    completed.sort(key=lambda x: x[0])
    form = "".join(r for _, r in completed[-n:])
    league_cache[team_id] = form
    return form


def _espn_soccer_team_stats_from_record(record: dict) -> dict:
    """
    Convert ESPN soccer team `record.items[*].stats` into a structure compatible
    with our FootballPredictor expectations (api-football-like).
    """
    if not isinstance(record, dict):
        return {}
    items = record.get("items") or []
    if not items:
        return {}

    # Prefer the "total" record if present; otherwise take the first item.
    item = next((it for it in items if (it or {}).get("type") == "total"), None) or items[0] or {}
    stats_map = {}
    for s in item.get("stats") or []:
        name = (s or {}).get("name")
        if not name:
            continue
        val = (s or {}).get("value")
        try:
            stats_map[name] = float(val) if val is not None else None
        except Exception:
            stats_map[name] = None

    def _f(name: str, default: float = 0.0) -> float:
        try:
            v = stats_map.get(name)
            return float(v) if v is not None else default
        except Exception:
            return default

    played = int(_f("gamesPlayed", 0))
    if played <= 0:
        return {}

    # ESPN uses pointsFor/pointsAgainst for soccer (goals for/against).
    gf_total = _f("pointsFor", 0.0)
    ga_total = _f("pointsAgainst", 0.0)
    pts_total = _f("points", 0.0)

    home_gp = max(int(_f("homeGamesPlayed", 0)), 0)
    away_gp = max(int(_f("awayGamesPlayed", 0)), 0)
    home_gf = _f("homePointsFor", 0.0)
    home_ga = _f("homePointsAgainst", 0.0)
    away_gf = _f("awayPointsFor", 0.0)
    away_ga = _f("awayPointsAgainst", 0.0)

    avg_for_total = gf_total / played
    avg_against_total = ga_total / played

    # If splits are missing, fall back to total averages.
    avg_for_home = (home_gf / home_gp) if home_gp > 0 else avg_for_total
    avg_against_home = (home_ga / home_gp) if home_gp > 0 else avg_against_total
    avg_for_away = (away_gf / away_gp) if away_gp > 0 else avg_for_total
    avg_against_away = (away_ga / away_gp) if away_gp > 0 else avg_against_total

    return {
        "fixtures": {"played": {"total": played}},
        "goals": {
            "for": {"average": {"home": avg_for_home, "away": avg_for_away, "total": avg_for_total}},
            "against": {"average": {"home": avg_against_home, "away": avg_against_away, "total": avg_against_total}},
        },
        # ESPN team endpoint doesn't provide a simple last-5 form string.
        "form": "",
        # Extra signal used by our predictor (originally added for football-data.org fallback).
        "fd_ppg": (pts_total / played) if played > 0 else 0.0,
        "fd_gd_per_game": ((gf_total - ga_total) / played) if played > 0 else 0.0,
    }


def fetch_football_team_stats_espn(team_id, league_key: str):
    """
    ESPN fallback: per-team endpoint has reliable record/goals data even when `/standings` is empty.
    """
    if not team_id or not league_key:
        return {}

    league_cache = _espn_soccer_team_cache.setdefault(str(league_key), {})
    tid = str(team_id)
    if tid in league_cache:
        return league_cache.get(tid, {}) or {}

    url = f"{ESPN_SOCCER_URL}/{league_key}/teams/{tid}"
    try:
        resp = _http.get(url, headers=ESPN_HEADERS, timeout=10)
        if resp.status_code != 200:
            return {}
        data = resp.json() or {}
        team = (data.get("team") or {})
        record = (team.get("record") or {})
        stats = _espn_soccer_team_stats_from_record(record)
        # Add recent form when available so the football predictor can use it.
        if stats is not None:
            form = _espn_soccer_recent_form(league_key, tid, n=5)
            if form:
                stats["form"] = form
        if stats:
            league_cache[tid] = stats
        return stats or {}
    except Exception:
        return {}


def fetch_football_fixtures_espn(wat_today, league_keys=None, name_map=None, gender=None):
    ymd = wat_today.strftime("%Y%m%d")
    fixtures = []
    seen = set()

    if league_keys is None:
        league_keys = []
        name_map = {}
        for league_id, (name, _) in FOOTBALL_LEAGUES.items():
            league_key = FOOTBALL_ESPN_LEAGUES.get(league_id)
            if league_key:
                league_keys.append(league_key)
                name_map[league_key] = name

    for league_key in league_keys:
        name = (name_map or {}).get(league_key, league_key)
        if not league_key:
            continue

        url = f"{ESPN_SOCCER_URL}/{league_key}/scoreboard"
        try:
            resp = _http.get(url, headers=ESPN_HEADERS, params={"dates": ymd}, timeout=10)
        except Exception:
            continue
        if resp.status_code != 200:
            continue

        try:
            events = resp.json().get("events", []) or []
        except Exception:
            continue

        for ev in events:
            eid = ev.get("id")
            if not eid:
                continue
            fid = f"espn_{eid}"
            if fid in seen:
                continue
            comps = ev.get("competitions") or []
            comp = comps[0] if comps else {}
            competitors = comp.get("competitors") or []
            home = next((c for c in competitors if c.get("homeAway") == "home"), None)
            away = next((c for c in competitors if c.get("homeAway") == "away"), None)
            if not home or not away:
                continue

            kickoff = ev.get("date") or comp.get("date")
            venue = ((comp.get("venue") or {}).get("fullName")) or "TBC"

            fixtures.append({
                "sport": "football",
                "source": "espn",
                "fixture_id": fid,
                "league": name,
                "league_id": league_key,
                "gender": gender,
                "home_team": ((home.get("team") or {}).get("displayName")) or "Home",
                "away_team": ((away.get("team") or {}).get("displayName")) or "Away",
                "home_id": (home.get("team") or {}).get("id"),
                "away_id": (away.get("team") or {}).get("id"),
                "kickoff": kickoff,
                "venue": venue,
            })
            seen.add(fid)

    return fixtures


def fetch_football_fixtures():
    # We report fixtures in WAT, but api-football's `date=YYYY-MM-DD` is UTC-based.
    # Around midnight WAT, using only "today UTC" can pull the wrong matchday.
    wat_today = datetime.now(WAT_OFFSET).date()
    window_end_wat = _wat_window_end_for_today(wat_today)
    start_utc = datetime.combine(wat_today, dt_time.min, tzinfo=WAT_OFFSET).astimezone(timezone.utc).date()
    # Extend the window to include early-next-day fixtures (e.g., 00:00�?"02:59 WAT tomorrow).
    end_utc = (window_end_wat - timedelta(seconds=1)).astimezone(timezone.utc).date()
    utc_dates = sorted({start_utc.strftime("%Y-%m-%d"), end_utc.strftime("%Y-%m-%d")})
    date_from = utc_dates[0]
    date_to = utc_dates[-1]

    fixtures = []
    seen_ids = set()
    today = wat_today.strftime("%Y-%m-%d")

    fc = _get_football_client()
    if fc:
        for league_id, (name, offset) in FOOTBALL_LEAGUES.items():
            season  = datetime.now().year + offset
            found   = False

            # Try current season, then fallback to previous if the key doesn't allow the current season
            for try_season in [season, season - 1]:
                results = []
                covered = True

                for utc_day in utc_dates:
                    resp = fc.get(f"{FOOTBALL_URL}/fixtures", params={
                        "league": league_id, "season": try_season, "date": utc_day
                    })
                    if not resp or resp.status_code != 200:
                        continue

                    body = resp.json()
                    if body.get("errors"):
                        # If the free plan doesn't cover the season, try the previous season instead.
                        if any("Free plans" in str(e) or "do not have access" in str(e) for e in body.get("errors", [])):
                            covered = False
                            break
                        continue

                    results.extend(body.get("response", []) or [])

                if not covered:
                    continue

                if results:
                    for f in results:
                        fid = f["fixture"]["id"]
                        if fid in seen_ids:
                            continue

                        kickoff = f["fixture"]["date"]
                        try:
                            dt = datetime.fromisoformat(str(kickoff).replace("Z", "+00:00")).astimezone(WAT_OFFSET)
                            # Include today, plus early-next-day fixtures up to the cutoff.
                            if dt < datetime(wat_today.year, wat_today.month, wat_today.day, 0, 0, tzinfo=WAT_OFFSET) or dt >= window_end_wat:
                                continue
                        except Exception:
                            pass

                        fixtures.append({
                            "sport":      "football",
                            "source":     "api-football",
                            "fixture_id": fid,
                            "league":     name,
                            "league_id":  league_id,
                            "home_team":  f["teams"]["home"]["name"],
                            "away_team":  f["teams"]["away"]["name"],
                            "home_id":    f["teams"]["home"]["id"],
                            "away_id":    f["teams"]["away"]["id"],
                            "kickoff":    kickoff,
                            "venue":      (f["fixture"].get("venue") or {}).get("name", "TBC"),
                        })
                        seen_ids.add(fid)
                    found = True
                    break  # Got results �?" no need to try previous season

            if not found:
                print(f"[INFO] Football {name}: 0 fixtures today.")
    else:
        print("[WARN] FOOTBALL_API_KEYS missing �?" skipping API-Football fixtures.")

    # Fallback to football-data.org if no fixtures from api-football and client available
    fdc = _get_football_data_client()
    if not fixtures and fdc:
        if not _tcp_connectable("api.football-data.org", 443, timeout=1.5):
            print("[WARN] football-data.org unreachable (TCP 443). Skipping football-data fallback.")
        else:
            print("[INFO] Trying football-data.org as alternative source...")
            for league_id, (name, offset) in FOOTBALL_LEAGUES.items():
                if league_id not in FOOTBALL_DATA_LEAGUES:
                    continue  # Skip leagues not available in football-data.org
                code = FOOTBALL_DATA_LEAGUES[league_id]
                resp = fdc.get(f"{FOOTBALL_DATA_URL}/competitions/{code}/matches", params={
                    "dateFrom": date_from, "dateTo": date_to
                })
                if not resp or resp.status_code != 200:
                    continue
                body = resp.json()
                matches = body.get("matches", [])
                if matches:
                    for m in matches:
                        kickoff = m["utcDate"]
                        try:
                            dt = datetime.fromisoformat(str(kickoff).replace("Z", "+00:00")).astimezone(WAT_OFFSET)
                            if dt < datetime(wat_today.year, wat_today.month, wat_today.day, 0, 0, tzinfo=WAT_OFFSET) or dt >= window_end_wat:
                                continue
                        except Exception:
                            pass

                        fixtures.append({
                            "sport":      "football",
                            "source":     "football-data",
                            "fixture_id": f"fd_{m['id']}",  # prefix to distinguish
                            "league":     name,
                            "league_id":  league_id,
                            "home_team":  m["homeTeam"]["name"],
                            "away_team":  m["awayTeam"]["name"],
                            "home_id":    m["homeTeam"]["id"],
                            "away_id":    m["awayTeam"]["id"],
                            "kickoff":    kickoff,
                            "venue":      "TBC",  # football-data.org doesn't provide venue in matches
                        })
                    print(f"[INFO] Football-data.org {name}: {len(matches)} fixtures.")

    # Fallback to ESPN soccer if still empty (no key required).
    if not fixtures:
        espn_fixtures = fetch_football_fixtures_espn(wat_today)
        # Also fetch tomorrow and keep only early-next-day fixtures (up to cutoff).
        try:
            espn_next = fetch_football_fixtures_espn(wat_today + timedelta(days=1))
        except Exception:
            espn_next = []
        if espn_next:
            for f in espn_next:
                try:
                    dt = datetime.fromisoformat(str(f.get("kickoff")).replace("Z", "+00:00")).astimezone(WAT_OFFSET)
                    if dt < window_end_wat:
                        espn_fixtures.append(f)
                except Exception:
                    pass
        if espn_fixtures:
            fixtures.extend(espn_fixtures)
            print(f"[INFO] ESPN soccer: {len(espn_fixtures)} fixtures.")

    # Always add ESPN women's leagues (no api-football IDs required).
    try:
        women_keys = list(FOOTBALL_WOMEN_ESPN_LEAGUES.keys())
        women_map = FOOTBALL_WOMEN_ESPN_LEAGUES
        w_fixtures = fetch_football_fixtures_espn(wat_today, league_keys=women_keys, name_map=women_map, gender="women")
        # Also include early-next-day (up to cutoff) from tomorrow
        w_next = fetch_football_fixtures_espn(wat_today + timedelta(days=1), league_keys=women_keys, name_map=women_map, gender="women")
        if w_next:
            window_end_wat = _wat_window_end_for_today(wat_today)
            for f in w_next:
                try:
                    dt = datetime.fromisoformat(str(f.get("kickoff")).replace("Z", "+00:00")).astimezone(WAT_OFFSET)
                    if dt < window_end_wat:
                        w_fixtures.append(f)
                except Exception:
                    pass
        if w_fixtures:
            fixtures.extend(w_fixtures)
            print(f"[INFO] ESPN women's soccer: {len(w_fixtures)} fixtures.")
    except Exception:
        pass

    if not fixtures:
        print(
            "[WARN] No football fixtures could be loaded. "
            "This is often caused by using a free api-football key that does not cover the current season. "
            "You can either upgrade your plan or use an alternative fixture source."
        )

    print(f"[INFO] Football total: {len(fixtures)} fixtures today.")
    return fixtures


def fetch_football_team_stats(team_id, league_id):
    if isinstance(league_id, str):
        return fetch_football_team_stats_espn(team_id, league_id)

    fc = _get_football_client()
    # Try current season -1, then -2 if free plan doesn't cover
    if fc:
        for offset in [1, 2]:
            season = datetime.now().year - offset
            resp   = fc.get(f"{FOOTBALL_URL}/teams/statistics", params={
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
    fdc = _get_football_data_client()
    if fdc and league_id in FOOTBALL_DATA_LEAGUES:
        code = FOOTBALL_DATA_LEAGUES[league_id]
        return fetch_football_team_stats_fd(team_id, code)
    
    return {}


def fetch_football_team_stats_fd(team_id, code):
    fdc = _get_football_data_client()
    if not fdc:
        return {}
    # Fetch standings for the league
    resp = fdc.get(f"{FOOTBALL_DATA_URL}/competitions/{code}/standings")
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
    resp2 = fdc.get(
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

    # Some football-data standings include a `form` string (often comma-separated).
    if not form_str:
        try:
            raw_form = team_data.get("form")
            if raw_form:
                raw_form = str(raw_form).replace(",", "").replace(" ", "").upper()
                form_str = raw_form[-5:]
        except Exception:
            pass
    
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
        "fd_position": team_data.get("position"),
        "fd_points": team_data.get("points"),
        "fd_ppg": (team_data.get("points", 0) / played) if played else 0,
        "fd_gd_per_game": ((goals_for - goals_against) / played) if played else 0,
        # Add other fields if needed, but this should suffice for the predictor
    }
    return stats


def fetch_h2h(home_id, away_id):
    fc = _get_football_client()
    if not fc:
        return []
    resp = fc.get(f"{FOOTBALL_URL}/fixtures/headtohead", params={
        "h2h": f"{home_id}-{away_id}", "last": 5
    })
    return resp.json().get("response", []) if resp and resp.status_code == 200 else []


def fetch_football_odds(fixture_id):
    fc = _get_football_client()
    if not fc:
        return None
    resp = fc.get(f"{FOOTBALL_URL}/odds", params={
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


# �.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.�
# BASKETBALL FETCHERS
# Stats source : ESPN hidden API �?" site.api.espn.com (free, no key, live)
# Games source : BallDontLie /v1/games (free)
# �.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.�

ESPN_NBA_STANDINGS = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/standings"
ESPN_WNBA_STANDINGS = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/standings"
ESPN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

# Cache: {abbreviation -> stats_dict}
_nba_stats_cache = {}    # keyed by team abbreviation e.g. "BOS"
_nba_bdl_id_map  = {}    # BallDontLie team_id -> abbreviation
_wnba_stats_cache = {}   # keyed by team abbreviation e.g. "NYL"

# RapidAPI tennis caches
_rapidapi_rankings_cache = {}   # {tour: [rows]}
_rapidapi_rankings_failed = set()
_rapidapi_player_cache = {}     # {player_id: stats}
_rapidapi_last_status = {}      # {path: status}
_rapidapi_rankings_rate_limited = False
_rapidapi_disable_rankings = False
_rapidapi_disable_player_info = False
_rapidapi_known_ranks = {}      # {"id:123" or "name:foo bar": rank}
_rapidapi_key_index = 0

# Hardcoded BallDontLie v1 team_id �?' abbreviation (IDs never change)
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

# �"?�"? 2024-25 hardcoded fallback standings (used when NBA.com times out) �"?�"?�"?�"?�"?�"?�"?�"?
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

        resp = _http.get(
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
                resp = _http.get(
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
                    # User preference: treat offense/defense as simple points for/against per game.
                    # BasketballPredictor uses these as the primary inputs.
                    "off_rating":   round(ppg, 1),
                    "def_rating":   round(opp_ppg, 1),
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
            print(f"[INFO] ESPN NBA stats loaded: {len(_nba_stats_cache)} teams �o.")
        else:
            print("[WARN] ESPN team detail stats could not be loaded; falling back to scoreboard records.")
            _load_nba_stats_from_espn_scoreboard()

    except Exception as e:
        print(f"[WARN] ESPN NBA stats fetch failed: {e}")
        _load_nba_stats_from_espn_scoreboard()


def _load_wnba_stats_from_espn():
    """Fetch live WNBA team stats from ESPN."""
    if _wnba_stats_cache:
        return

    try:
        print("[INFO] Fetching WNBA stats from ESPN (team details)...")
        resp = _http.get(
            "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/teams",
            headers=ESPN_HEADERS,
            params={"limit": 32},
            timeout=10,
        )
        if resp.status_code != 200:
            print(f"[WARN] ESPN WNBA teams list: HTTP {resp.status_code}")
            return

        data = resp.json()
        teams = (data.get("sports", [{}])[0]
                     .get("leagues", [{}])[0]
                     .get("teams", []))
        if not teams:
            print("[WARN] ESPN WNBA teams list returned no teams")
            return

        abbrev_to_id = {
            entry.get("team", {}).get("abbreviation"): entry.get("team", {}).get("id")
            for entry in teams
            if entry.get("team", {}).get("abbreviation") and entry.get("team", {}).get("id")
        }

        for abbrev, espn_id in abbrev_to_id.items():
            try:
                resp = _http.get(
                    f"https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/teams/{espn_id}",
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

                ppg = float(stats_raw.get("avgPointsFor", 80.0))
                opp_ppg = float(stats_raw.get("avgPointsAgainst", 80.0))
                diff = float(stats_raw.get("differential", ppg - opp_ppg))

                _wnba_stats_cache[abbrev] = {
                    "ppg":          round(ppg, 1),
                    "opp_ppg":      round(opp_ppg, 1),
                    "net_rating":   round(diff, 1),
                    "off_rating":   round(ppg, 1),
                    "def_rating":   round(opp_ppg, 1),
                    "pace":         94.0,
                    "recent_trend": round((w_pct - 0.5) * 4, 2),
                    "wins":         int(w),
                    "losses":       int(l),
                    "win_pct":      round(w_pct, 3),
                }
            except Exception:
                continue

        if _wnba_stats_cache:
            sample = list(_wnba_stats_cache.items())[:3]
            for abbr, s in sample:
                print(f"[INFO] ESPN WNBA {abbr}: W={s['wins']} L={s['losses']} W%={s['win_pct']:.3f}")
            print(f"[INFO] ESPN WNBA stats loaded: {len(_wnba_stats_cache)} teams �o.")
        else:
            print("[WARN] ESPN WNBA stats could not be loaded.")
    except Exception as e:
        print(f"[WARN] ESPN WNBA stats fetch failed: {e}")


def _load_nba_stats_from_espn_scoreboard():
    """Fallback: build W/L stats from ESPN scoreboard season records."""
    if _nba_stats_cache:
        return
    try:
        resp = _http.get(
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
            print(f"[INFO] ESPN scoreboard fallback: {len(_nba_stats_cache)} teams �o.")
    except Exception as e:
        print(f"[WARN] ESPN scoreboard fallback failed: {e}")

def _load_bdl_team_abbrev_map():
    """
    Build BallDontLie team_id �?' abbreviation map.
    First tries the API, falls back to hardcoded map if API fails or IDs mismatch.
    """
    global _nba_bdl_id_map
    if _nba_bdl_id_map:
        return
    try:
        nc = _get_nba_client()
        if nc:
            resp = nc.get(f"{BALLDONTLIE_URL}/teams", params={"per_page": 100})
            if resp and resp.status_code == 200:
                for t in resp.json().get("data", []):
                    tid   = t.get("id")
                    abbr  = t.get("abbreviation", "")
                    if tid and abbr:
                        _nba_bdl_id_map[tid] = abbr
                print(f"[INFO] BDL team map loaded from API: {len(_nba_bdl_id_map)} teams.")
        else:
            print("[WARN] BALLDONTLIE_KEYS missing �?" using hardcoded NBA team map.")
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
        # Extract abbreviation from team name e.g. "Boston Celtics" �?' "BOS"
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
                print(f"[INFO] NBA: matched {team_name!r} �?' {abbrev}")

    if not stats:
        # Try ESPN abbreviation mapping (e.g. WAS -> WSH, GSW -> GS)
        espn_abbrev = _ESPN_ABBREV_MAP.get(abbrev, abbrev)
        if espn_abbrev != abbrev:
            stats = _nba_stats_cache.get(espn_abbrev, {})
            if stats:
                print(f"[INFO] NBA: matched {team_name!r} ({abbrev}) �?' ESPN {espn_abbrev}")

    if not stats:
        print(f"[WARN] NBA: no stats for team_id={team_id} name={team_name!r} abbrev={abbrev!r}")
    return stats


def _ensure_wnba_stats_loaded():
    _load_wnba_stats_from_espn()
    if not _wnba_stats_cache:
        print("[WARN] ESPN WNBA stats failed �?" no stats available. Predictions will use defaults.")


def fetch_wnba_team_season_stats(team_abbrev, team_name=""):
    if not _wnba_stats_cache:
        _ensure_wnba_stats_loaded()
    if not team_abbrev:
        return {}
    stats = _wnba_stats_cache.get(team_abbrev, {})
    if not stats and team_name:
        # Try match by team name (fallback)
        for abbr, s in _wnba_stats_cache.items():
            if team_name.lower().replace(" ", "") in abbr.lower():
                return s
    return stats


def _ensure_nba_stats_loaded(season="2024-25"):
    """
    Load NBA team stats ONCE before the game loop.
    Source: ESPN standings API (live, no key, no timeout issues).
    """
    _load_bdl_team_abbrev_map()
    _load_nba_stats_from_espn()
    if not _nba_stats_cache:
        print("[WARN] ESPN NBA stats failed �?" no stats available. Predictions will use defaults.")

def fetch_nba_fixtures():
    """
    Fetch NBA games for TODAY in WAT (midnight to midnight WAT),
    plus early-next-day fixtures up to EARLY_NEXT_DAY_CUTOFF_HOUR_WAT.

    Logic:
      - WAT today = now_wat.date()
      - Only include games whose tip-off in WAT falls on today's WAT date
      - Skip finished games and games already started >2h ago
    """
    from datetime import timedelta as _td

    nc = _get_nba_client()
    if not nc:
        print("[WARN] BALLDONTLIE_KEYS missing �?" cannot load NBA fixtures.")
        return []

    now_utc  = datetime.now(timezone.utc)
    now_wat  = now_utc.astimezone(WAT_OFFSET)

    # WAT day boundaries: 00:00 WAT today �?' cutoff tomorrow
    today_wat_date = now_wat.date()
    day_start_wat  = datetime(today_wat_date.year, today_wat_date.month,
                              today_wat_date.day, 0, 0, tzinfo=WAT_OFFSET)
    day_end_wat    = _wat_window_end_for_today(today_wat_date)

    # Fetch UTC dates that overlap with today WAT
    # WAT is UTC+1, so WAT today spans UTC yesterday-evening to UTC tonight
    utc_dates = set()
    utc_dates.add(now_utc.strftime("%Y-%m-%d"))
    utc_dates.add((now_utc - _td(days=1)).strftime("%Y-%m-%d"))
    utc_dates.add((now_utc + _td(days=1)).strftime("%Y-%m-%d"))

    all_games = []
    for date_str in sorted(utc_dates):
        try:
            resp = nc.get(
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
            # Include today's games, plus early-next-day games up to the cutoff.
            if tip_wat < day_start_wat or tip_wat >= day_end_wat:
                continue
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


def fetch_wnba_fixtures():
    """
    Fetch WNBA games for TODAY in WAT (midnight to midnight WAT),
    plus early-next-day fixtures up to EARLY_NEXT_DAY_CUTOFF_HOUR_WAT.
    """
    from datetime import timedelta as _td

    now_utc  = datetime.now(timezone.utc)
    now_wat  = now_utc.astimezone(WAT_OFFSET)

    today_wat_date = now_wat.date()
    day_start_wat  = datetime(today_wat_date.year, today_wat_date.month,
                              today_wat_date.day, 0, 0, tzinfo=WAT_OFFSET)
    day_end_wat    = _wat_window_end_for_today(today_wat_date)

    # Fetch UTC dates that overlap with today WAT
    utc_dates = set()
    utc_dates.add(now_utc.strftime("%Y%m%d"))
    utc_dates.add((now_utc - _td(days=1)).strftime("%Y%m%d"))
    utc_dates.add((now_utc + _td(days=1)).strftime("%Y%m%d"))

    events = []
    for date_str in sorted(utc_dates):
        try:
            resp = _http.get(
                "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/scoreboard",
                headers=ESPN_HEADERS,
                params={"dates": date_str},
                timeout=10,
            )
        except Exception:
            continue
        if resp.status_code != 200:
            continue
        try:
            events.extend(resp.json().get("events", []) or [])
        except Exception:
            continue

    seen = set()
    games = []
    for ev in events:
        eid = ev.get("id")
        if not eid or eid in seen:
            continue
        seen.add(eid)

        status = ev.get("status", {}).get("type", {})
        if status.get("state") in ("post", "postponed"):
            continue

        comps = ev.get("competitions") or []
        comp = comps[0] if comps else {}
        competitors = comp.get("competitors") or []
        home = next((c for c in competitors if c.get("homeAway") == "home"), None)
        away = next((c for c in competitors if c.get("homeAway") == "away"), None)
        if not home or not away:
            continue

        kickoff = ev.get("date") or comp.get("date")
        tip_dt = None
        if kickoff:
            try:
                tip_dt = datetime.fromisoformat(str(kickoff).replace("Z", "+00:00"))
                if tip_dt.tzinfo is None:
                    tip_dt = tip_dt.replace(tzinfo=timezone.utc)
            except Exception:
                tip_dt = None

        if tip_dt:
            tip_wat = tip_dt.astimezone(WAT_OFFSET)
            if tip_wat < day_start_wat or tip_wat >= day_end_wat:
                continue
            if tip_dt < now_utc - _td(hours=2):
                continue

        games.append({
            "sport":      "basketball",
            "league":     "WNBA",
            "fixture_id": f"wnba_{eid}",
            "home_team":  ((home.get("team") or {}).get("displayName")) or "Home",
            "away_team":  ((away.get("team") or {}).get("displayName")) or "Away",
            "home_abbrev": ((home.get("team") or {}).get("abbreviation")) or "",
            "away_abbrev": ((away.get("team") or {}).get("abbreviation")) or "",
            "kickoff":    kickoff,
            "venue":      ((comp.get("venue") or {}).get("fullName")) or "TBC",
            "gender":     "women",
        })

    def _sort(g):
        try:
            return datetime.fromisoformat(str(g["kickoff"]).replace("Z", "+00:00"))
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)

    games.sort(key=_sort)
    print(f"[INFO] WNBA: {len(games)} games in next 24h "
          f"(now {now_wat.strftime('%H:%M WAT')}).")
    return games

# �"?�"? Tennis player cache + embedded rankings �"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?
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


def fetch_tennis_player_stats(player_key, surface="hard", player_name="", known_rank=None):
    if (TENNIS_STATS_PROVIDER or "").strip().lower() == "rapidapi":
        return fetch_tennis_player_stats_rapidapi(
            player_key, surface=surface, player_name=player_name, known_rank=known_rank
        )
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
                r = _http.get(API_TENNIS_URL, params={
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
            print(f"[INFO] Tennis rank lookup: {player_name} �?' #{rank} (win_pct={win_pct:.2f})")
        else:
            print(f"[WARN] Tennis: no rank found for '{player_name}'")

    _tennis_player_cache[cache_key] = stats
    return stats


def _old_fetch_tennis_player_stats_DEPRECATED(player_key, surface="hard"):
    pass  # replaced above

def _call_tennis_api(params):
    """Helper �?" tries all TENNIS_KEYS, returns parsed result list or []."""
    for key in TENNIS_KEYS:
        try:
            r = _http.get(API_TENNIS_URL, params={**params, "APIkey": key}, timeout=10)
            if r.status_code == 200:
                body = r.json()
                if body.get("success") == 1:
                    return body.get("result", [])
                print(f"[WARN] Tennis API: {body.get('error', 'unknown error')}")
            elif r.status_code in (403, 429):
                print(f"[ROTATE] Tennis key {r.status_code} �?" trying next...")
            else:
                print(f"[WARN] Tennis API HTTP {r.status_code}")
        except Exception as e:
            print(f"[ROTATE] Tennis key error: {e} �?" trying next...")
    return []


def _call_rapidapi_tennis(path, params=None):
    global _rapidapi_key_index
    if not RAPIDAPI_TENNIS_KEYS or not RAPIDAPI_TENNIS_HOST:
        print("[WARN] RAPIDAPI_TENNIS_KEY(S)/HOST missing - cannot call RapidAPI tennis.")
        return None
    url = f"https://{RAPIDAPI_TENNIS_HOST}{path}"
    for _ in range(len(RAPIDAPI_TENNIS_KEYS)):
        key = RAPIDAPI_TENNIS_KEYS[_rapidapi_key_index % len(RAPIDAPI_TENNIS_KEYS)]
        headers = {
            "x-rapidapi-key": key,
            "x-rapidapi-host": RAPIDAPI_TENNIS_HOST,
        }
        try:
            r = _http.get(url, headers=headers, params=params or {}, timeout=12)
            _rapidapi_last_status[path] = r.status_code
            if r.status_code == 200:
                return r.json()
            if r.status_code in (403, 429):
                print(f"[ROTATE] RapidAPI key #{(_rapidapi_key_index % len(RAPIDAPI_TENNIS_KEYS)) + 1} failed ({r.status_code}). Trying next key...")
                _rapidapi_key_index += 1
                continue
            print(f"[WARN] RapidAPI tennis HTTP {r.status_code} for {path}")
            return None
        except Exception as e:
            print(f"[WARN] RapidAPI tennis error: {e}")
            _rapidapi_key_index += 1
            continue
    return None

def _rapidapi_rankings_candidates(tour: str):
    tour = (tour or "").lower()
    custom = os.getenv("RAPIDAPI_TENNIS_RANKINGS_PATH", "").strip()
    custom_tour = os.getenv(f"RAPIDAPI_TENNIS_RANKINGS_PATH_{tour.upper()}", "").strip()
    if custom_tour:
        return [custom_tour]
    if custom:
        return [custom.format(tour=tour)]
    return [
        f"/tennis/v2/{tour}/rankings/singles",
        f"/tennis/v2/{tour}/rankings",
        f"/tennis/v2/{tour}/singlesRanking",
        f"/tennis/v2/{tour}/ranking/singles",
        f"/tennis/v2/rankings/{tour}/singles",
        f"/tennis/v2/{tour}/rankings/singlesRanking",
        f"/tennis/v2/{tour}/rankings/singles-ranking",
        f"/tennis/v2/{tour}/rankings/singlesRankings",
        f"/tennis/v2/{tour}/rankings/singles-ranking/",
        f"/tennis/v2/rankings/singles/{tour}",
    ]


def _load_rapidapi_rankings(tour: str):
    global _rapidapi_disable_rankings, _rapidapi_rankings_rate_limited
    tour = (tour or "").lower()
    if _rapidapi_disable_rankings:
        return []
    if os.getenv("RAPIDAPI_TENNIS_DISABLE_RANKINGS", "").strip().lower() in ("1", "true", "yes"):
        _rapidapi_disable_rankings = True
        return []
    if _rapidapi_rankings_rate_limited:
        return []
    if tour in _rapidapi_rankings_cache:
        return _rapidapi_rankings_cache[tour]
    if tour in _rapidapi_rankings_failed:
        return []

    any_403_404 = False
    for path in _rapidapi_rankings_candidates(tour):
        data = _call_rapidapi_tennis(path)
        if not data:
            status = _rapidapi_last_status.get(path)
            if status == 429:
                _rapidapi_rankings_rate_limited = True
                break
            if status in (403, 404):
                any_403_404 = True
            continue
        if isinstance(data, dict):
            for k in ("data", "result", "results", "rankings", "players"):
                v = data.get(k)
                if isinstance(v, list) and v:
                    _rapidapi_rankings_cache[tour] = v
                    return v
        if isinstance(data, list) and data:
            _rapidapi_rankings_cache[tour] = data
            return data

    if any_403_404:
        _rapidapi_disable_rankings = True
        _rapidapi_rankings_failed.add(tour)
    else:
        _rapidapi_rankings_failed.add(tour)
    return []


def _rapidapi_player_info_candidates(tour: str, player_id: str):
    tour = (tour or "").lower()
    pid = str(player_id)
    custom = os.getenv("RAPIDAPI_TENNIS_PLAYER_PATH", "").strip()
    custom_tour = os.getenv(f"RAPIDAPI_TENNIS_PLAYER_PATH_{tour.upper()}", "").strip()
    if custom_tour:
        return [custom_tour.format(tour=tour, id=pid)]
    if custom:
        return [custom.format(tour=tour, id=pid)]
    return [
        f"/tennis/v2/{tour}/player/{pid}",
        f"/tennis/v2/{tour}/players/{pid}",
        f"/tennis/v2/{tour}/player/info/{pid}",
        f"/tennis/v2/player/{pid}",
        f"/tennis/v2/players/{pid}",
    ]


def _load_rapidapi_player_info(tour: str, player_id: str):
    global _rapidapi_disable_player_info
    if not player_id:
        return {}
    pid = str(player_id)
    if pid in _rapidapi_player_cache:
        return _rapidapi_player_cache[pid]
    if _rapidapi_disable_player_info:
        _rapidapi_player_cache[pid] = {}
        return {}
    any_403_404 = False
    for path in _rapidapi_player_info_candidates(tour, pid):
        data = _call_rapidapi_tennis(path)
        if not data:
            status = _rapidapi_last_status.get(path)
            if status in (403, 404):
                any_403_404 = True
            continue
        # Normalize to dict
        if isinstance(data, dict):
            _rapidapi_player_cache[pid] = data
            return data
    if any_403_404:
        _rapidapi_disable_player_info = True
    _rapidapi_player_cache[pid] = {}
    return {}


def fetch_tennis_fixtures():
    if (TENNIS_FIXTURES_PROVIDER or "").strip().lower() == "rapidapi":
        return fetch_tennis_fixtures_rapidapi()
    wat_today = datetime.now(WAT_OFFSET).date()
    window_end_wat = _wat_window_end_for_today(wat_today)
    today = wat_today.strftime("%Y-%m-%d")  # WAT date
    tomorrow = (wat_today + timedelta(days=1)).strftime("%Y-%m-%d")

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
        # Query tomorrow too, then filter to early-next-day window in WAT.
        "date_stop":  tomorrow,
    })

    # Fetch yesterday for back-to-back detection
    yesterday = (wat_today - timedelta(days=1)).strftime("%Y-%m-%d")
    raw_yesterday = _call_tennis_api({
        "method":     "get_fixtures",
        "date_start": yesterday,
        "date_stop":  yesterday,
    }) or []

    if not raw_data:
        print("[WARN] Tennis: no fixtures returned.")
        return []

    print(f"[INFO] Tennis: {len(raw_data)} raw fixtures from API.")
    matches  = []
    seen_ids = set()

    def _gender_from_text(text: str) -> str:
        t = (text or "").lower()
        if "mixed" in t:
            return "mixed"
        if "wta" in t or "women" in t:
            return "women"
        # Challenger is men; ATP is men. (ITF is skipped earlier.)
        if "atp" in t or "challenger" in t or "men" in t:
            return "men"
        return "unknown"

    def _event_format(ev_type: str, tournament: str) -> str:
        """
        Return one of: singles, doubles, mixed_doubles, unknown
        """
        t = f"{ev_type or ''} {tournament or ''}".lower()
        if "mixed" in t and "double" in t:
            return "mixed_doubles"
        if "double" in t:
            return "doubles"
        if "single" in t:
            return "singles"
        return "unknown"

    def _qualifies(t_lower: str) -> bool:
        if any(kw in t_lower for kw in SKIP_KEYWORDS):
            return False
        if not any(kw in t_lower for kw in MAJOR_KEYWORDS):
            return False
        return True

    def _extract_players(raw_list):
        players = set()
        for g in raw_list or []:
            tournament = g.get("tournament_name", "") or ""
            ev_type    = g.get("event_type_type", "") or ""
            t_lower    = f"{tournament} {ev_type}".lower()
            if not _qualifies(t_lower):
                continue
            home_p = g.get("event_first_player",  "Player 1")
            away_p = g.get("event_second_player", "Player 2")
            players.add(_normalize_player_name(home_p))
            players.add(_normalize_player_name(away_p))
        return players

    yesterday_players = _extract_players(raw_yesterday)
    if rate_limited_any:
        any_raw = any(raw_today_by_tour.values()) or any(raw_tomorrow_by_tour.values())
        if not any_raw:
            print("[WARN] RapidAPI tennis rate-limited (429) â�,��?� no fixtures returned.")
    if rate_limited_any:
        any_raw = any(raw_today_by_tour.values()) or any(raw_tomorrow_by_tour.values())
        if not any_raw:
            print("[WARN] RapidAPI tennis rate-limited (429) â�,��?� no fixtures returned.")

    for g in raw_data:
        gid = g.get("event_key")
        if gid in seen_ids:
            continue
        seen_ids.add(gid)

        tournament = g.get("tournament_name", "") or ""
        ev_type    = g.get("event_type_type", "") or ""
        # Use both fields for filtering (some tournaments are generic names like "Asuncion",
        # but ev_type contains "ATP/WTA/Challenger").
        t_lower    = f"{tournament} {ev_type}".lower()

        if not _qualifies(t_lower):
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
        # Filter to WAT today window (including early-next-day fixtures up to cutoff).
        try:
            dt_wat = datetime.fromisoformat(str(kickoff).replace("Z", "+00:00")).astimezone(WAT_OFFSET)
            if dt_wat < datetime(wat_today.year, wat_today.month, wat_today.day, 0, 0, tzinfo=WAT_OFFSET) or dt_wat >= window_end_wat:
                continue
        except Exception:
            pass

        # Cache player keys for later lookup (so stats can be fetched even if only name is known)
        if home_pkey:
            tennis_player_key = _normalize_player_name(home_p)
            _tennis_player_key_map[tennis_player_key] = home_pkey
        if away_pkey:
            tennis_player_key = _normalize_player_name(away_p)
            _tennis_player_key_map[tennis_player_key] = away_pkey

        tournament_full = tournament
        if ev_type and ev_type.lower() not in tournament.lower():
            tournament_full = f"{tournament} ({ev_type})" if tournament else ev_type

        gender = _gender_from_text(ev_type) if ev_type else _gender_from_text(tournament)
        fmt = _event_format(ev_type, tournament)

        b2b_home = _normalize_player_name(home_p) in yesterday_players
        b2b_away = _normalize_player_name(away_p) in yesterday_players

        matches.append({
            "sport":          "tennis",
            "fixture_id":     gid,
            "league":         tournament_full,
            "event_type":     ev_type,
            "gender":         gender,
            "match_format":   fmt,
            "home_team":      home_p,
            "away_team":      away_p,
            "home_player_key": home_pkey,
            "away_player_key": away_pkey,
            "kickoff":        kickoff,
            # Include ATP/WTA/Challenger in the tournament string so tier detection works.
            "tournament":     tournament_full,
            "venue":          "TBC",
            "back_to_back":   bool(b2b_home or b2b_away),
            "b2b_home":       bool(b2b_home),
            "b2b_away":       bool(b2b_away),
        })

    print(f"[INFO] Tennis: {len(matches)} qualifying matches today.")
    return matches


def fetch_tennis_fixtures_rapidapi():
    """
    RapidAPI Tennis API (ATP/WTA/ITF). Best-effort parsing.
    """
    wat_today = datetime.now(WAT_OFFSET).date()
    window_end_wat = _wat_window_end_for_today(wat_today)
    today = wat_today.strftime("%Y-%m-%d")
    tomorrow = (wat_today + timedelta(days=1)).strftime("%Y-%m-%d")
    yesterday = (wat_today - timedelta(days=1)).strftime("%Y-%m-%d")
    # Match legacy api-tennis behavior: default to ATP/WTA only (ITF excluded).
    tours = [t.strip().lower() for t in os.getenv("RAPIDAPI_TENNIS_TOURS", "atp,wta").split(",") if t.strip()]
    if not tours:
        tours = ["atp", "wta", "itf"]

    def _qualifies(t_lower: str) -> bool:
        if any(kw in t_lower for kw in ["itf", "utr", "futures", "junior"]):
            return False
        if not any(kw in t_lower for kw in ["atp", "wta", "grand slam", "masters", "challenger",
                                            "miami", "indian wells", "roland", "wimbledon",
                                            "us open", "australian", "french open",
                                            "500", "250", "1000"]):
            return False
        return True

    def _as_list(obj):
        if isinstance(obj, list):
            return obj
        if isinstance(obj, dict):
            for k in ("data", "result", "results", "fixtures", "events", "matches"):
                v = obj.get(k)
                if isinstance(v, list):
                    return v
        return []

    def _get(d, keys, default=None):
        for k in keys:
            if isinstance(d, dict) and k in d and d[k] is not None:
                return d[k]
        return default

    def _extract_name(v):
        if isinstance(v, dict):
            return v.get("name") or v.get("player") or v.get("fullname") or v.get("full_name")
        return v

    def _extract_id(v):
        if isinstance(v, dict):
            return v.get("id") or v.get("player_id")
        return None

    def _extract_rank(v):
        if isinstance(v, dict):
            for k in ("rank", "ranking", "position", "currentRank", "singlesRank"):
                if v.get(k) is not None:
                    try:
                        return int(v.get(k))
                    except Exception:
                        pass
        return None

    def _build_kickoff(item):
        def _parse_offset(tz):
            if not tz:
                return None
            t = str(tz).strip().upper()
            # IANA timezone (e.g., Europe/Paris)
            if "/" in t:
                return t
            # Common abbreviations
            if t in ("WAT",):
                return 1
            if t in ("UTC", "GMT"):
                return 0
            if t in ("CET", "BST", "WEST"):
                return 1
            if t in ("CEST", "EET", "EEST"):
                return 2
            if t in ("ET", "EST"):
                return -5
            if t in ("EDT",):
                return -4
            # UTC+X or GMT-3
            if t.startswith("UTC") or t.startswith("GMT"):
                t = t.replace("UTC", "").replace("GMT", "")
            if t.startswith("+") or t.startswith("-"):
                try:
                    if ":" in t:
                        sign = 1 if t.startswith("+") else -1
                        hh, mm = t[1:].split(":", 1)
                        return sign * (int(hh) + int(mm) / 60)
                    return int(t)
                except Exception:
                    return None
            return None

        # Prefer explicit timestamps
        for k in ("timestamp", "startTimestamp", "utcTimestamp"):
            v = _get(item, [k])
            if v is None:
                continue
            try:
                ts = int(v)
                if ts > 10_000_000_000:  # ms
                    ts = ts / 1000.0
                return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            except Exception:
                pass

        # Prefer explicit UTC date/time fields if present
        dt_utc = _get(item, ["utcDate", "dateUTC", "date_utc"])
        tm_utc = _get(item, ["utcTime", "timeUTC", "time_utc"])
        if isinstance(dt_utc, str):
            if "T" in dt_utc:
                return dt_utc if dt_utc.endswith("Z") or "+" in dt_utc else dt_utc + "+00:00"
            if tm_utc:
                return f"{dt_utc}T{tm_utc}:00+00:00"

        dt = _get(item, ["date", "start_date", "startTime", "start_time", "datetime", "start_datetime"])
        tm = _get(item, ["time", "start_time", "startTime", "timeGMT", "gmtTime"])

        if isinstance(dt, str) and "T" in dt:
            return dt if dt.endswith("Z") or "+" in dt else dt + "+00:00"

        # If timezone provided, adjust to UTC
        tz = _get(item, ["timezone", "timeZone", "tz", "time_zone"])
        offset = _parse_offset(tz)
        if isinstance(dt, str) and tm and offset is not None:
            try:
                base = datetime.fromisoformat(f"{dt}T{tm}:00")
                if isinstance(offset, str) and "/" in offset:
                    try:
                        from zoneinfo import ZoneInfo
                        z = ZoneInfo(offset)
                        utc_dt = base.replace(tzinfo=z).astimezone(timezone.utc)
                        return utc_dt.isoformat()
                    except Exception:
                        pass
                utc_dt = base - timedelta(hours=offset)
                return utc_dt.replace(tzinfo=timezone.utc).isoformat()
            except Exception:
                pass

        if isinstance(dt, str) and tm:
            return f"{dt}T{tm}:00+00:00"
        if isinstance(dt, str):
            return f"{dt}T00:00:00+00:00"
        return ""

    def _fetch_date(tour, date_str):
        # Example path: /tennis/v2/atp/fixtures/2024-02-07
        path = f"/tennis/v2/{tour}/fixtures/{date_str}"
        data = _call_rapidapi_tennis(path) or {}
        status = _rapidapi_last_status.get(path)
        return data, status

    def _extract_players(raw_list, require_qualify=True):
        players = set()
        for g in raw_list:
            tournament = str(_get(g, ["tournament", "tournament_name", "tournamentName"], "") or "")
            ev_type = str(_get(g, ["event_type_type", "type", "category", "tour"], "") or "")
            t_lower = f"{tournament} {ev_type}".lower()
            if require_qualify and not _qualifies(t_lower):
                continue
            home_p = _extract_name(_get(g, ["player1", "home", "player_home", "homePlayer", "player_1"], "Player 1"))
            away_p = _extract_name(_get(g, ["player2", "away", "player_away", "awayPlayer", "player_2"], "Player 2"))
            players.add(_normalize_player_name(home_p))
            players.add(_normalize_player_name(away_p))
        return players

    # Yesterday list for back-to-back (skip if rate-limited)
    raw_yesterday = []
    rate_limited_any = False
    raw_today_by_tour = {}
    raw_tomorrow_by_tour = {}
    raw_meta = {}
    b2b_days = int(os.getenv("RAPIDAPI_TENNIS_B2B_DAYS", "1") or "1")
    if b2b_days < 1:
        b2b_days = 1
    for tour in tours:
        data_today, status_today = _fetch_date(tour, today)
        if status_today == 429:
            rate_limited_any = True
            raw_today_by_tour[tour] = []
            raw_tomorrow_by_tour[tour] = []
            raw_meta[tour] = {"today_status": status_today, "today_len": 0}
            continue
        raw_today_list = _as_list(data_today)
        raw_today_by_tour[tour] = raw_today_list
        raw_meta[tour] = {"today_status": status_today, "today_len": len(raw_today_list)}

        # Fetch tomorrow for early-next-day window (if not rate-limited for this tour)
        data_tom, status_tom = _fetch_date(tour, tomorrow)
        if status_tom == 429:
            rate_limited_any = True
            raw_tomorrow_by_tour[tour] = []
            raw_meta[tour]["tomorrow_status"] = status_tom
            raw_meta[tour]["tomorrow_len"] = 0
        else:
            raw_tomorrow_by_tour[tour] = _as_list(data_tom)
            raw_meta[tour]["tomorrow_status"] = status_tom
            raw_meta[tour]["tomorrow_len"] = len(raw_tomorrow_by_tour[tour])

        # Fetch previous days for back-to-back only if today has matches
        if raw_today_list:
            for d in range(1, b2b_days + 1):
                day_str = (wat_today - timedelta(days=d)).strftime("%Y-%m-%d")
                data_y, status_y = _fetch_date(tour, day_str)
                if status_y == 429:
                    rate_limited_any = True
                    break
                raw_yesterday += _as_list(data_y)
    yesterday_players = _extract_players(raw_yesterday, require_qualify=False)
    if os.getenv("RAPIDAPI_TENNIS_DEBUG", "").strip().lower() in ("1", "true", "yes"):
        for tour, meta in raw_meta.items():
            print(f"[DEBUG] RapidAPI {tour} today: status={meta.get('today_status')} len={meta.get('today_len')} | tomorrow: status={meta.get('tomorrow_status')} len={meta.get('tomorrow_len')}")

    fixtures = []
    seen_ids = set()

    def _gender_from_text(text: str) -> str:
        t = (text or "").lower()
        if "mixed" in t:
            return "mixed"
        if "wta" in t or "women" in t:
            return "women"
        if "atp" in t or "challenger" in t or "men" in t:
            return "men"
        return "unknown"

    def _event_format(ev_type: str, tournament: str) -> str:
        t = f"{ev_type or ''} {tournament or ''}".lower()
        if "mixed" in t and "double" in t:
            return "mixed_doubles"
        if "double" in t:
            return "doubles"
        if "single" in t:
            return "singles"
        return "unknown"

    def _handle_list(raw_list, tour, relax_filters=False):
        nonlocal fixtures, seen_ids
        for g in raw_list:
            gid = _get(g, ["id", "event_id", "fixture_id", "match_id"]) or str(_get(g, ["slug"], ""))
            if not gid:
                continue
            if gid in seen_ids:
                continue
            seen_ids.add(gid)

            tournament_id = _get(g, ["tournamentId", "tournament_id"])
            tournament = str(_get(g, ["tournament", "tournament_name", "tournamentName"], "") or "")
            ev_type = str(_get(g, ["event_type_type", "type", "category", "tour"], "") or "") or tour
            if not tournament and tournament_id:
                tournament = f"Tournament {tournament_id}"
            t_lower = f"{tournament} {ev_type}".lower()
            if not relax_filters and not _qualifies(t_lower):
                continue

            home_raw = _get(g, ["player1", "home", "player_home", "homePlayer", "player_1"], None)
            away_raw = _get(g, ["player2", "away", "player_away", "awayPlayer", "player_2"], None)
            home_p = _extract_name(home_raw or "Player 1")
            away_p = _extract_name(away_raw or "Player 2")
            home_id = _extract_id(home_raw)
            away_id = _extract_id(away_raw)
            home_rank = _extract_rank(home_raw)
            away_rank = _extract_rank(away_raw)
            if home_rank is None:
                home_rank = _get(g, ["player1_rank", "player1Rank", "home_rank", "homeRank", "rank1", "rank_home"])
            if away_rank is None:
                away_rank = _get(g, ["player2_rank", "player2Rank", "away_rank", "awayRank", "rank2", "rank_away"])
            try:
                home_rank = int(home_rank) if home_rank is not None else None
            except Exception:
                home_rank = None
            try:
                away_rank = int(away_rank) if away_rank is not None else None
            except Exception:
                away_rank = None

            kickoff = _build_kickoff(g)
            # Optional manual offset (hours) to correct provider timezone issues
            try:
                off = os.getenv("RAPIDAPI_TENNIS_TIME_OFFSET_HOURS", "").strip()
                if kickoff and off:
                    delta = float(off)
                    dt_fix = datetime.fromisoformat(str(kickoff).replace("Z", "+00:00"))
                    kickoff = (dt_fix + timedelta(hours=delta)).isoformat()
            except Exception:
                pass
            if os.getenv("RAPIDAPI_TENNIS_DEBUG", "").strip().lower() in ("1", "true", "yes"):
                dbg = {
                    "id": gid,
                    "date": _get(g, ["date", "start_date", "dateUTC", "utcDate"]),
                    "time": _get(g, ["time", "start_time", "startTime", "timeGMT", "gmtTime", "utcTime"]),
                    "timezone": _get(g, ["timezone", "timeZone", "tz", "time_zone"]),
                    "timestamp": _get(g, ["timestamp", "startTimestamp", "utcTimestamp"]),
                    "kickoff": kickoff,
                }
                print(f"[DEBUG] RapidAPI fixture time: {dbg}")
            if kickoff:
                try:
                    dt_wat = datetime.fromisoformat(str(kickoff).replace("Z", "+00:00")).astimezone(WAT_OFFSET)
                    if dt_wat < datetime(wat_today.year, wat_today.month, wat_today.day, 0, 0, tzinfo=WAT_OFFSET) or dt_wat >= window_end_wat:
                        continue
                except Exception:
                    pass

            tournament_full = tournament
            if ev_type and ev_type.lower() not in tournament.lower():
                tournament_full = f"{tournament} ({ev_type})" if tournament else ev_type

            gender = _gender_from_text(ev_type) if ev_type else _gender_from_text(tournament)
            fmt = _event_format(ev_type, tournament)
            b2b_home = _normalize_player_name(home_p) in yesterday_players
            b2b_away = _normalize_player_name(away_p) in yesterday_players

            # Cache known ranks from fixtures (if provided)
            if home_rank:
                if home_id:
                    _rapidapi_known_ranks[f"id:{home_id}"] = home_rank
                _rapidapi_known_ranks[f"name:{_normalize_player_name(home_p)}"] = home_rank
            if away_rank:
                if away_id:
                    _rapidapi_known_ranks[f"id:{away_id}"] = away_rank
                _rapidapi_known_ranks[f"name:{_normalize_player_name(away_p)}"] = away_rank

            fixtures.append({
                "sport":           "tennis",
                "fixture_id":      str(gid),
                "league":          tournament_full,
                "event_type":      ev_type,
                "gender":          gender,
                "match_format":    fmt,
                "home_team":       home_p,
                "away_team":       away_p,
                "home_player_key": str(home_id or ""),
                "away_player_key": str(away_id or ""),
                "home_rank":       home_rank,
                "away_rank":       away_rank,
                "kickoff":         kickoff,
                "tournament":      tournament_full,
                "venue":           "TBC",
                "back_to_back":    bool(b2b_home or b2b_away),
                "b2b_home":        bool(b2b_home),
                "b2b_away":        bool(b2b_away),
            })

    for tour, lst in raw_today_by_tour.items():
        before = len(fixtures)
        _handle_list(lst, tour)
        added = len(fixtures) - before
        if added == 0 and lst and tour in ("atp", "wta"):
            # If everything got filtered out but API returned items, relax filters for ATP/WTA.
            _handle_list(lst, tour, relax_filters=True)
    for tour, lst in raw_tomorrow_by_tour.items():
        before = len(fixtures)
        _handle_list(lst, tour)
        added = len(fixtures) - before
        if added == 0 and lst and tour in ("atp", "wta"):
            _handle_list(lst, tour, relax_filters=True)

    print(f"[INFO] Tennis (RapidAPI): {len(fixtures)} qualifying matches today.")
    return fixtures


def fetch_tennis_player_stats_rapidapi(player_key, surface="hard", player_name="", known_rank=None):
    """
    Best-effort mapping for RapidAPI Tennis API.
    Uses singlesRanking for rank and falls back to name-based rank lookup if needed.
    """
    cache_key = f"rapidapi:{player_key or player_name}"
    if cache_key in _tennis_player_cache:
        return _tennis_player_cache[cache_key]

    stats = {}

    rank = 999
    if known_rank:
        try:
            rank = int(known_rank)
        except Exception:
            rank = 999

    if rank >= 999:
        if player_key:
            cached = _rapidapi_known_ranks.get(f"id:{player_key}")
            if cached:
                rank = int(cached)
        if rank >= 999 and player_name:
            cached = _rapidapi_known_ranks.get(f"name:{_normalize_player_name(player_name)}")
            if cached:
                rank = int(cached)

    if rank >= 999 and player_name:
        pname = _normalize_player_name(player_name)
        for tour in ("atp", "wta"):
            for row in _load_rapidapi_rankings(tour):
                name = row.get("name") or row.get("player") or row.get("playerName")
                if isinstance(name, dict):
                    name = name.get("name") or name.get("full_name") or name.get("displayName")
                if not name:
                    continue
                if _normalize_player_name(name) == pname:
                    r = row.get("rank") or row.get("position")
                    try:
                        rank = int(r)
                    except Exception:
                        pass
                    break
            if rank < 999:
                break

    # Try player info endpoint if we still don't have a rank and have player_id
    if rank >= 999 and player_key:
        for tour in ("atp", "wta", "itf"):
            info = _load_rapidapi_player_info(tour, player_key)
            if not info:
                continue
            # Try to extract a rank field
            for k in ("rank", "ranking", "position", "currentRank", "singlesRank"):
                v = info.get(k)
                if v is None and isinstance(info.get("data"), dict):
                    v = info["data"].get(k)
                if v is None and isinstance(info.get("player"), dict):
                    v = info["player"].get(k)
                if v is not None:
                    try:
                        rank = int(v)
                        break
                    except Exception:
                        pass
            if rank < 999:
                break

    if rank >= 999 and player_name:
        # Fallback to embedded ATP/WTA tables
        rank = _rank_from_name(player_name)

    if rank < 999:
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
    else:
        print(f"[WARN] RapidAPI Tennis: no rank found for '{player_name}'")

    _tennis_player_cache[cache_key] = stats
    return stats


# �.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.�
# CARD FORMATTERS
# �.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.�
def ko_str(kickoff, reference_date=None):
    """Convert any ISO timestamp to WAT. Labels next-day early games clearly."""
    try:
        dt     = datetime.fromisoformat(str(kickoff).replace("Z", "+00:00"))
        dt_wat = dt.astimezone(WAT_OFFSET)
        today  = reference_date or datetime.now(WAT_OFFSET).date()
        if dt_wat.date() != today:
            # Next day early morning (00:00-06:00 WAT) �?" label the day
            day_name = dt_wat.strftime("%a")   # e.g. "Tue"
            return f"{dt_wat.strftime('%H:%M')} WAT ({day_name})"
        return dt_wat.strftime("%H:%M WAT")
    except Exception:
        return "TBC"


# �"?�"? Football card �"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?
def format_football_card(fix, pred):
    gender = (fix.get("gender") or "").strip().lower()
    gender_tag = " (Women)" if gender == "women" else ""
    venue = fix.get("venue", "TBC")

    winner_map = {"home": "H", "draw": "D", "away": "A"}
    w_e = winner_map.get(pred.get("winner"), "?")
    conf = int(round(float(pred.get("confidence", 0) or 0)))
    conf = max(0, min(conf, 100))
    conf_bar = "#" * (conf // 10) + "-" * (10 - conf // 10)

    vb_block = ""
    if pred.get("value_bets"):
        lines = ["\nVALUE BETS"]
        for vb in pred["value_bets"]:
            lines.append(f"  {vb['outcome']} @ {vb['odd']} | Edge: +{vb['value']}%")
        vb_block = "\n".join(lines)

    note_line = f"\nNOTE: {pred['data_note']}" if pred.get("data_note") else ""

    return f"""
FOOTBALL{gender_tag} - {fix['league']}
{fix['home_team']} vs {fix['away_team']}
Time: {ko_str(fix['kickoff'])}  Venue: {venue}{note_line}

PREDICTION: {"HIGH" if "HIGH" in pred['grade'] else "MEDIUM" if "MEDIUM" in pred['grade'] else "LOW"} | {w_e} {pred['winner_label']} ({conf}%)
[{conf_bar}]
Home {pred['prob_home']}%  Draw {pred['prob_draw']}%  Away {pred['prob_away']}%

Goals: {pred['over_under']} ({pred['ou_prob']}%)  xG {pred['expected_goals']}
BTTS: {pred['btts']} ({pred['btts_prob']}%)
Score: {pred['correct_score']} ({pred['score_prob']}%)

Form: H {pred.get('home_form','N/A')}  A {pred.get('away_form','N/A')}
Key: {pred['key_factor']}{vb_block}
""".strip()

def format_basketball_card(fix, pred):
    conf = int(round(float(pred.get("confidence", 0) or 0)))
    conf = max(0, min(conf, 100))
    conf_bar = "#" * (conf // 10) + "-" * (10 - conf // 10)
    winner = "HOME" if pred.get("winner") == "home" else "AWAY"

    return f"""
NBA
{fix['home_team']} vs {fix['away_team']}
Time: {ko_str(fix['kickoff'])}  Venue: {fix.get('venue','TBC')}

PREDICTION: {"HIGH" if "HIGH" in pred['grade'] else "MEDIUM" if "MEDIUM" in pred['grade'] else "LOW"} | {winner} {pred['winner_label']} ({conf}%)
[{conf_bar}]
Home {pred['prob_home']}%  Away {pred['prob_away']}%

Total Points: {pred['over_under']}
Predicted score: {pred['pred_score']}  Total: {pred['pred_total']}
Spread: {pred['spread']}
Game pace: {pred['game_pace']} poss/48min

Team profile:
  Home: {pred['home_profile']}
  Away: {pred['away_profile']}

Key: {pred['key_factor']}{"" if not pred.get("injury_note") else "\nInjuries: " + pred["injury_note"]}{"" if not pred.get("home_recent") else "\nRecent: H " + " ".join({"W":"W","L":"L"}.get(c,"?") for c in pred.get("home_recent","")) + " / A " + " ".join({"W":"W","L":"L"}.get(c,"?") for c in pred.get("away_recent",""))}
""".strip()

def format_tennis_card(fix, pred):
    gender = (fix.get("gender") or "").strip().lower()
    fmt = (fix.get("match_format") or "").strip().lower()
    gender_tag = ""
    if gender == "women":
        gender_tag = " (Women)"
    elif gender == "men":
        gender_tag = " (Men)"
    elif gender == "mixed":
        gender_tag = " (Mixed)"

    format_tag = ""
    if fmt in ("mixed_doubles","doubles"):
        format_tag = " Doubles"

    conf = int(round(float(pred.get("confidence", 0) or 0)))
    conf = max(0, min(conf, 100))
    conf_bar = "#" * (conf // 10) + "-" * (10 - conf // 10)
    rank_home = pred.get("rank_home", 999)
    rank_away = pred.get("rank_away", 999)
    rank_line = ""
    if rank_home < 999 or rank_away < 999:
        rh = f"#{rank_home}" if rank_home < 999 else "NR"
        ra = f"#{rank_away}" if rank_away < 999 else "NR"
        rank_line = f"\nRankings: H {rh}  A {ra}"

    b2b_line = ""
    if fix.get("back_to_back"):
        if fix.get("b2b_home") and fix.get("b2b_away"):
            who = "Both players"
        elif fix.get("b2b_home"):
            who = f"{fix['home_team']}"
        else:
            who = f"{fix['away_team']}"
        b2b_line = f"\nBack-to-back: {who} played yesterday"

    return f"""
TENNIS{gender_tag}{format_tag} - {pred['tournament']}
{fix['home_team']} vs {fix['away_team']}
Time: {ko_str(fix['kickoff'])}  Surface: {pred['surface']}{rank_line}{b2b_line}

PREDICTION: {"HIGH" if "HIGH" in pred['grade'] else "MEDIUM" if "MEDIUM" in pred['grade'] else "LOW"} | {pred['winner_label']} ({conf}%)
[{conf_bar}]
Home {pred['prob_home']}%  Away {pred['prob_away']}%

Format: {pred['sets_format']} | Predicted: {pred['pred_sets']}
Set handicap: {pred['handicap']}
Over/Under games: {pred['over_under']} (pred: {pred['pred_games']} games)

Serve profile:
  Home: {pred['home_serve']}
  Away: {pred['away_serve']}

Key: {pred['key_factor']}{"" if not pred.get("home_news") else "\nNews H: " + pred.get("home_news","") + " | A: " + pred.get("away_news","")}
""".strip()

def format_sport_summary(sport_emoji, sport_name, results, date_str):
    lines = [
        "============================",
        f"{sport_name} PREDICTIONS",
        f"{date_str}",
        "============================\n",
    ]
    value_alerts = []

    def _grade_label(g):
        if "HIGH" in g:
            return "HIGH"
        if "MEDIUM" in g:
            return "MEDIUM"
        return "LOW"

    for fix, pred in results:
        g = _grade_label(pred.get("grade", ""))
        winner = pred.get("winner_label", "?")
        conf = pred.get("confidence", "?")
        ko = ko_str(fix["kickoff"])

        if fix["sport"] == "football":
            ou = pred.get("over_under", "")
            btts = pred.get("btts", "")
            gtag = " (Women)" if (fix.get("gender") or "").lower() == "women" else ""
            lines.append(
                f"{fix['home_team']} vs {fix['away_team']}{gtag}\n"
                f"  {g} {winner} ({conf}%) | {ou} | BTTS:{btts} | {ko}\n"
            )
        elif fix["sport"] == "basketball":
            ou = pred.get("over_under", "")
            pts = pred.get("pred_score", "")
            lines.append(
                f"{fix['home_team']} vs {fix['away_team']}\n"
                f"  {g} {winner} ({conf}%) | {ou} | Score:{pts} | {ko}\n"
            )
        elif fix["sport"] == "tennis":
            surf = pred.get("surface", "")
            lines.append(
                f"{fix['home_team']} vs {fix['away_team']}\n"
                f"  {g} {winner} ({conf}%) | {surf} | {ko}\n"
            )

        for vb in pred.get("value_bets", []):
            value_alerts.append(
                f"{fix['home_team']} v {fix['away_team']} - {vb['outcome']} @ {vb['odd']} (+{vb['value']}%)"
            )

    if value_alerts:
        lines.append("----------------------------")
        lines.append("VALUE BETS")
        lines += value_alerts

    lines.append("\nFor entertainment only. Bet responsibly.")
    return "\n".join(lines)

def format_matchday_schedule(date_str, football_fixtures, nba_fixtures, tennis_fixtures):
    lines = [
        "============================",
        "TODAY'S FIXTURES",
        f"{date_str}",
        "============================",
    ]

    if football_fixtures:
        lines.append(f"\nFOOTBALL ({len(football_fixtures)} games)")
        by_league = {}
        for f in football_fixtures:
            by_league.setdefault(f["league"], []).append(f)
        for league, games in by_league.items():
            lines.append(f"\n{league}")
            for f in games:
                lines.append(f"  {ko_str(f['kickoff'])} {f['home_team']} vs {f['away_team']}")

    if nba_fixtures:
        lines.append(f"\nNBA ({len(nba_fixtures)} games)")
        for f in nba_fixtures:
            lines.append(f"  {ko_str(f['kickoff'])} {f['home_team']} vs {f['away_team']}")

    if tennis_fixtures:
        lines.append(f"\nTENNIS ({len(tennis_fixtures)} matches)")
        by_tournament = {}
        for f in tennis_fixtures:
            by_tournament.setdefault(f["tournament"], []).append(f)
        for tournament, matches in by_tournament.items():
            lines.append(f"\n{tournament}")
            for f in matches:
                lines.append(f"  {ko_str(f['kickoff'])} {f['home_team']} vs {f['away_team']}")

    total = len(football_fixtures) + len(nba_fixtures) + len(tennis_fixtures)
    lines.append("\n----------------------------")
    lines.append(f"{total} total games today")
    lines.append("Predictions follow below")

    return "\n".join(lines)


# �.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.�
# MAIN PIPELINE
# �.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.��.�
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

    # �"?�"? STEP 1: Fetch all fixtures first �"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?
    print("\n[INFO] Fetching all fixtures...")
    football_fixtures = fetch_football_fixtures()
    nba_fixtures      = fetch_nba_fixtures()
    tennis_fixtures   = fetch_tennis_fixtures()

    total_games = len(football_fixtures) + len(nba_fixtures) + len(tennis_fixtures)

    # �"?�"? STEP 2: Send full matchday schedule �"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?
    print("[INFO] Sending matchday schedule...")
    sender.send_message(
        f"DAILY SPORTS BOT\n"
        f"{date_str}\n"
        f"Football | NBA | Tennis\n"
        f"----------------------------",
        parse_mode="Markdown"
    )
        away_stats = fetch_tennis_player_stats(
            fix.get("away_player_key", ""), surface, player_name=fix.get("away_team", ""),
            known_rank=fix.get("away_rank")
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
        sender.send_message(format_sport_summary("�YZ�", "TENNIS", tennis_results, date_str), parse_mode="Markdown")
        for fix, pred in tennis_results:
            sender.send_message(format_tennis_card(fix, pred), parse_mode="Markdown")
    else:
        sender.send_message("�YZ� No major tennis matches today.", parse_mode="Markdown")

    # �"?�"? Done �"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?�"?
    total_preds = len(football_results) + len(nba_results) + len(tennis_results)
    print(f"\n[DONE] {total_preds} predictions sent �?" "
          f"{len(football_results)} football, {len(nba_results)} NBA, {len(tennis_results)} tennis.")
    sender.send_message(
        f"�o. *All done!*\n"
        f"�Y"S `{total_games}` games today | `{total_preds}` predictions sent\n"
        f"�s� `{len(football_results)}` football | "
        f"�Y�? `{len(nba_results)}` NBA | "
        f"�YZ� `{len(tennis_results)}` tennis",
        parse_mode="Markdown"
    )


if __name__ == "__main__":
    run_predictions()














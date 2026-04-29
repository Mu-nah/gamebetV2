"""
Tennis Engine v7 — WTA Prediction Engine
=========================================
RUN_MODE (set by GitHub Actions env var):
  normal       — every 3hr run:  dedup ON,  HIGH+MEDIUM filter ON,  marks sent
  daily_reset  — 1am WAT run:    dedup OFF, HIGH+MEDIUM filter ON, marks sent
  force        — manual trigger: dedup OFF, HIGH+MEDIUM filter ON, does NOT mark sent

SERVE STATS:
  TennisAbstract live scraping removed — it's unreliable (site structure
  changes, timeouts, connection failures). Replaced with a static embedded
  table of WTA serve win % sourced from 2024-25 season averages.
  Players not in the table get a surface-aware tour average (hard 68%,
  clay 65%, grass 72%, indoors 70%). The serve signal only affects 6% of
  the final score so missing data has minimal impact.
"""

from playwright.sync_api import sync_playwright
import requests
import random
import math
import time
import re
import os
import json
from datetime import datetime, timedelta, timezone

BASE = "https://www.tennisexplorer.com"
WAT  = timezone(timedelta(hours=1))

# ── Credentials ───────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID   = os.getenv("CHAT_ID",   "")

# ── Run mode ──────────────────────────────────────────────────
RUN_MODE = os.getenv("RUN_MODE", "normal").strip().lower()
if RUN_MODE not in ("normal", "daily_reset", "force"):
    RUN_MODE = "normal"
print(f"[MODE] RUN_MODE = {RUN_MODE.upper()}")

PREFETCH_LIMIT     = 20
SURFACE_COL        = {"clay": 2, "hard": 3, "indoors": 4, "grass": 5}

SURFACE_ELO_DB: dict = {}
_player_cache:  dict = {}
_h2h_cache:     dict = {}
_http = requests.Session()

_RANK_RE      = re.compile(r"singles:\s*(\d+)\.")
_ITF_PREFIXES = re.compile(r"^\s*(w15|w25|w35|w50|w60|w100|itf)\b", re.IGNORECASE)

# Surface constants
SURFACE_SERVE_HOLD = {
    "hard": 0.72, "clay": 0.68, "grass": 0.77, "indoors": 0.74,
}
SURFACE_AVG_GAMES = {
    "hard": 9.8, "clay": 10.2, "grass": 9.4, "indoors": 9.6,
}
# Tour average serve win % by surface — used when player not in table
SURFACE_TOUR_AVG_SERVE = {
    "hard": 0.68, "clay": 0.65, "grass": 0.72, "indoors": 0.70, "unknown": 0.67,
}


# ══════════════════════════════════════════════════════════════
# STATIC SERVE STATS TABLE
#
# Source: WTA 2024-25 season averages (tennisabstract.com)
# Key:    tennisexplorer slug (lowercase, with ID suffix where present)
# Values: serve_win_pct  — service points won %
#         first_serve_pct — 1st serve in %
#
# Players NOT in this table fall back to SURFACE_TOUR_AVG_SERVE.
# Update this table periodically (start of each season is sufficient).
# ══════════════════════════════════════════════════════════════
_SERVE_STATS: dict[str, dict] = {
    # slug                    serve_win  first_in
    "swiatek":              {"s": 0.700, "f": 0.680},
    "sabalenka":            {"s": 0.730, "f": 0.640},
    "gauff":                {"s": 0.680, "f": 0.650},
    "rybakina":             {"s": 0.740, "f": 0.660},
    "pegula":               {"s": 0.670, "f": 0.650},
    "zheng":                {"s": 0.660, "f": 0.660},
    "andreeva-7d55d":       {"s": 0.640, "f": 0.640},
    "paolini":              {"s": 0.650, "f": 0.650},
    "badosa":               {"s": 0.660, "f": 0.650},
    "vekic":                {"s": 0.680, "f": 0.660},
    "kasatkina":            {"s": 0.640, "f": 0.630},
    "samsonova":            {"s": 0.670, "f": 0.640},
    "svitolina":            {"s": 0.650, "f": 0.640},
    "jabeur":               {"s": 0.650, "f": 0.650},
    "keys":                 {"s": 0.720, "f": 0.640},
    "kostyuk-ea2bf":        {"s": 0.660, "f": 0.660},
    "krejcikova":           {"s": 0.670, "f": 0.640},
    "ostapenko":            {"s": 0.680, "f": 0.610},
    "muchova":              {"s": 0.660, "f": 0.640},
    "collins":              {"s": 0.710, "f": 0.650},
    "haddad-maia":          {"s": 0.650, "f": 0.650},
    "bencic":               {"s": 0.660, "f": 0.650},
    "cirstea":              {"s": 0.660, "f": 0.640},
    "maria-8ad07":          {"s": 0.640, "f": 0.640},
    "andreescu":            {"s": 0.680, "f": 0.640},
    "vondrousova":          {"s": 0.640, "f": 0.640},
    "sorribes-tormo":       {"s": 0.620, "f": 0.680},
    "chwalinska":           {"s": 0.650, "f": 0.640},
    "montgomery-8a7c9":     {"s": 0.660, "f": 0.650},
    "marcinko":             {"s": 0.640, "f": 0.640},
    "boulter":              {"s": 0.680, "f": 0.650},
    "shnaider":             {"s": 0.690, "f": 0.640},
    "fernandez":            {"s": 0.660, "f": 0.660},
    "alexandrova":          {"s": 0.670, "f": 0.650},
    "potapova":             {"s": 0.650, "f": 0.640},
    "noskova":              {"s": 0.680, "f": 0.640},
    "navarro":              {"s": 0.660, "f": 0.650},
    "townsend":             {"s": 0.660, "f": 0.650},
    "fruhvirtova":          {"s": 0.650, "f": 0.650},
    "sherif":               {"s": 0.640, "f": 0.650},
    "kvitova":              {"s": 0.720, "f": 0.640},
    "halep":                {"s": 0.660, "f": 0.660},
    "azarenka":             {"s": 0.680, "f": 0.640},
    "wozniacki":            {"s": 0.650, "f": 0.650},
    "stosur":               {"s": 0.700, "f": 0.620},
    "svitolina":            {"s": 0.650, "f": 0.640},
    "krueger":              {"s": 0.650, "f": 0.650},
    "kostyuk-ea2bf":        {"s": 0.660, "f": 0.660},
    "tomova":               {"s": 0.650, "f": 0.640},
    "bouchard":             {"s": 0.670, "f": 0.640},
    "sakkari":              {"s": 0.670, "f": 0.650},
    "kontaveit":            {"s": 0.680, "f": 0.650},
    "ruse":                 {"s": 0.640, "f": 0.650},
    "niemeier":             {"s": 0.660, "f": 0.640},
    "linette":              {"s": 0.640, "f": 0.650},
    "minnen":               {"s": 0.650, "f": 0.650},
    "burel":                {"s": 0.640, "f": 0.640},
    "errani":               {"s": 0.590, "f": 0.690},
    "siegemund":            {"s": 0.640, "f": 0.640},
    "putintseva":           {"s": 0.640, "f": 0.650},
    "zarazua":              {"s": 0.650, "f": 0.640},
    "dolehide":             {"s": 0.660, "f": 0.640},
    "mcnally":              {"s": 0.650, "f": 0.650},
    "golubic":              {"s": 0.660, "f": 0.640},
    "parrizas-diaz":        {"s": 0.630, "f": 0.660},
    "tauson":               {"s": 0.660, "f": 0.640},
    "kovinic":              {"s": 0.650, "f": 0.640},
    "bogdan":               {"s": 0.640, "f": 0.650},
    "rakhimova":            {"s": 0.650, "f": 0.640},
    "pera":                 {"s": 0.650, "f": 0.650},
    "hibino":               {"s": 0.640, "f": 0.650},
    "schmiedlova":          {"s": 0.640, "f": 0.650},
    "pigato":               {"s": 0.640, "f": 0.640},
    "gasanova":             {"s": 0.640, "f": 0.640},
    "grabher":              {"s": 0.650, "f": 0.650},
    "siniaková":            {"s": 0.660, "f": 0.650},
    "siniakova":            {"s": 0.660, "f": 0.650},
    "brengle":              {"s": 0.640, "f": 0.650},
    "zidansek":             {"s": 0.640, "f": 0.640},
    "begu":                 {"s": 0.640, "f": 0.650},
    "bara":                 {"s": 0.640, "f": 0.640},
    "vikhlyantseva":        {"s": 0.640, "f": 0.640},
    "podrez":               {"s": 0.640, "f": 0.640},
    "kraus-e1937":          {"s": 0.640, "f": 0.650},
}

# ── First-name lookup for live TA fallback ────────────────────
# Key: "surname_initial" (lowercase)   Value: first name (proper case)
_INITIAL_TO_FIRST: dict[str, str] = {
    "swiatek_i":"Iga","sabalenka_a":"Aryna","gauff_c":"Coco",
    "rybakina_e":"Elena","pegula_j":"Jessica","zheng_q":"Qinwen",
    "andreeva_m":"Mirra","paolini_j":"Jasmine","badosa_p":"Paula",
    "vekic_d":"Donna","kasatkina_d":"Daria","samsonova_l":"Ludmila",
    "svitolina_e":"Elina","jabeur_o":"Ons","keys_m":"Madison",
    "kostyuk_m":"Marta","krejcikova_b":"Barbora","ostapenko_j":"Jelena",
    "muchova_k":"Katerina","collins_d":"Danielle","haddad_b":"Beatriz",
    "bencic_b":"Belinda","cirstea_s":"Sorana","maria_t":"Tatjana",
    "andreescu_b":"Bianca","vondrousova_m":"Marketa",
    "sorribes_s":"Sara","chwalinska_m":"Maja","montgomery_r":"Rebecca",
    "marcinko_p":"Patricia","podrez_v":"Veronika","boulter_k":"Katie",
    "shnaider_d":"Diana","fernandez_l":"Leylah",
    "alexandrova_e":"Ekaterina","potapova_a":"Anastasia",
    "fruhvirtova_l":"Linda","noskova_l":"Linda","navarro_e":"Emma",
    "townsend_t":"Taylor","sherif_m":"Mayar","sakkari_m":"Maria",
    "kontaveit_a":"Anett","kvitova_p":"Petra","halep_s":"Simona",
    "azarenka_v":"Victoria","wozniacki_c":"Caroline",
    "niemeier_j":"Jule","linette_m":"Magda","tauson_c":"Clara",
    "putintseva_y":"Yulia","golubic_v":"Viktorija",
    "rakhimova_k":"Kamilla","grabher_j":"Julia","siniakova_k":"Katerina",
    "burel_c":"Clara","siegemund_l":"Laura","bogdan_a":"Ana",
    "errani_s":"Sara","ruse_e":"Elena","minnen_g":"Greet",
    "pera_b":"Bernarda","hibino_n":"Nao","pigato_l":"Lisa",
    "zidansek_t":"Tamara","begu_i":"Irina","kovinic_d":"Danka",
    "dolehide_c":"Caroline","mcnally_c":"Catherine","zarazua_r":"Renata",
    "krueger_a":"Alison","tomova_v":"Viktoriya","gasanova_a":"Amina",
    "bara_i":"Irina","vikhlyantseva_n":"Natalia","schmiedlova_a":"Anna",
    "parrizas-diaz_s":"Sara","diaz_s":"Sara",
}

_TA_BASE        = "https://www.tennisabstract.com/cgi-bin/wplayer.cgi"
_TA_HEADERS     = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "text/html,application/xhtml+xml",
    "Referer":    "https://www.tennisabstract.com/",
}
_ta_live_cache: dict = {}


def _build_ta_name(slug: str, display_name: str) -> str:
    """
    Build TennisAbstract search name (FirstnameSurname).
    display_name format from tennisexplorer: "Kostyuk M." / "Sorribes Tormo S."
    TA format: "MartaKostyuk" / "SaraSorribesTormo"
    """
    parts   = (display_name or "").strip().split()
    surname = parts[0].rstrip(".") if parts else ""
    initial = parts[1].rstrip(".").upper() if len(parts) >= 2 else ""
    key     = f"{surname.lower()}_{initial}".rstrip("_")
    first   = _INITIAL_TO_FIRST.get(key, "")
    if first:
        return f"{first}{surname.capitalize()}"
    # surname only — TA fuzzy-matches unique surnames
    return surname.capitalize()


def _parse_ta_html(html: str) -> dict:
    stats = {}
    for key, pat in (
        ("serve_win_pct",    re.compile(r"var\s+sp\s*=\s*([\d.]+)")),
        ("first_serve_pct",  re.compile(r"var\s+fsp\s*=\s*([\d.]+)")),
    ):
        m = pat.search(html)
        if m:
            val = float(m.group(1))
            stats[key] = round(val / 100.0, 4) if val > 1 else val
    return stats


def _fetch_ta_live(slug: str, display_name: str) -> dict:
    """Live TA fetch with 5s timeout. Cached per slug. Never raises."""
    if slug in _ta_live_cache:
        return _ta_live_cache[slug]
    ta_name = _build_ta_name(slug, display_name)
    if not ta_name:
        _ta_live_cache[slug] = {}
        return {}
    try:
        resp = _http.get(f"{_TA_BASE}?p={ta_name}", headers=_TA_HEADERS, timeout=5)
        if resp.status_code == 200:
            bad = ("no player found", "no results found", "<title>error")
            if any(b in resp.text.lower()[:500] for b in bad):
                # Retry with surname only
                surname = (display_name or "").split()[0].rstrip(".")
                resp = _http.get(
                    f"{_TA_BASE}?p={surname.capitalize()}",
                    headers=_TA_HEADERS, timeout=5,
                )
            stats = _parse_ta_html(resp.text)
            if stats:
                print(f"   [TA live] {display_name}: "
                      f"serve={round(stats.get('serve_win_pct',0)*100)}%")
                _ta_live_cache[slug] = stats
                return stats
    except Exception as e:
        print(f"   [TA live] {display_name}: {type(e).__name__}")
    _ta_live_cache[slug] = {}
    return {}


def get_serve_stats(slug: str, surface: str, display_name: str = "") -> dict:
    """
    3-tier serve stats. Never fails, never blocks the run.

    Tier 1 — Static table  (~80 players, 2024-25 averages, instant)
    Tier 2 — Live TA fetch  (unknown players only, 5s timeout)
              Uses display_name "Surname I." + _INITIAL_TO_FIRST map
              to build the correct FirstnameSurname TA URL.
    Tier 3 — Surface tour average  (hard 68%, clay 65%, grass 72%)
    """
    # Tier 1
    if slug in _SERVE_STATS:
        row = _SERVE_STATS[slug]
        return {"serve_win_pct": row["s"], "first_serve_pct": row["f"], "source": "table"}

    # Tier 2
    if display_name:
        live = _fetch_ta_live(slug, display_name)
        if live.get("serve_win_pct"):
            return {
                "serve_win_pct":   live["serve_win_pct"],
                "first_serve_pct": live.get("first_serve_pct", 0.65),
                "source":          "ta_live",
            }

    # Tier 3
    avg = SURFACE_TOUR_AVG_SERVE.get(surface, 0.67)
    return {"serve_win_pct": avg, "first_serve_pct": 0.65, "source": "avg"}


# ══════════════════════════════════════════════════════════════
# SURFACE ELO
# ══════════════════════════════════════════════════════════════
def get_surface_elo(slug: str, surface: str) -> float:
    return SURFACE_ELO_DB.get(slug, {}).get(surface, 1500.0)

def update_surface_elo(winner_slug: str, loser_slug: str, surface: str):
    k  = 32
    r1 = get_surface_elo(winner_slug, surface)
    r2 = get_surface_elo(loser_slug,  surface)
    e1 = 1.0 / (1.0 + 10.0 ** ((r2 - r1) / 400.0))
    SURFACE_ELO_DB.setdefault(winner_slug, {})[surface] = r1 + k * (1 - e1)
    SURFACE_ELO_DB.setdefault(loser_slug,  {})[surface] = r2 - k * e1


# ══════════════════════════════════════════════════════════════
# DEDUP
# ══════════════════════════════════════════════════════════════
_SENT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sent_matches.json")

def _load_sent() -> dict:
    try:
        with open(_SENT_FILE) as f:
            return json.load(f)
    except Exception:
        return {}

def _save_sent(data: dict):
    try:
        with open(_SENT_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"[WARN] Could not save sent_matches.json: {e}")

def _today_wat() -> str:
    return datetime.now(WAT).strftime("%Y-%m-%d")

def _match_key(p1: str, p2: str) -> str:
    return "_vs_".join(sorted([p1.strip().lower(), p2.strip().lower()]))

def is_already_sent(p1: str, p2: str) -> bool:
    return _match_key(p1, p2) in _load_sent().get(_today_wat(), [])

def mark_as_sent(picks: list):
    sent  = _load_sent()
    today = _today_wat()
    sent.setdefault(today, [])
    for pk in picks:
        key = _match_key(pk["m"]["p1"], pk["m"]["p2"])
        if key not in sent[today]:
            sent[today].append(key)
    cutoff = (datetime.now(WAT) - timedelta(days=3)).strftime("%Y-%m-%d")
    _save_sent({k: v for k, v in sent.items() if k >= cutoff})


# ══════════════════════════════════════════════════════════════
# RUN MODE FILTERS
# ══════════════════════════════════════════════════════════════
def apply_mode_filters(picks: list) -> list:
    if RUN_MODE == "force":
        filtered = [pk for pk in picks if pk.get("grade") in ("HIGH", "MEDIUM")]
        print(f"[MODE] FORCE — {len(filtered)} HIGH/MEDIUM picks (dedup ignored)")
        return filtered
        filtered = [pk for pk in picks if pk.get("grade") in ("HIGH", "MEDIUM")]
        print(f"[MODE] DAILY_RESET — {len(filtered)} HIGH/MEDIUM picks (dedup ignored)")
        print(f"[MODE] DAILY_RESET — {len(filtered)} HIGH picks (dedup ignored)")
        return filtered
    before = len(picks)
    picks  = [pk for pk in picks
              if not is_already_sent(pk["m"]["p1"], pk["m"]["p2"])]
    picks  = [pk for pk in picks if pk.get("grade") in ("HIGH", "MEDIUM")]
    print(f"[MODE] NORMAL — {len(picks)} HIGH/MEDIUM picks ({before - len(picks)} deduped)")
    return picks

def should_mark_sent() -> bool:
    return RUN_MODE in ("normal", "daily_reset")


# ══════════════════════════════════════════════════════════════
# TELEGRAM
# ══════════════════════════════════════════════════════════════
def send_telegram(msg: str):
    if not BOT_TOKEN:
        print("⚠ BOT_TOKEN not set")
        return
    if not CHAT_ID:
        print("⚠ CHAT_ID not set")
        return
    try:
        for chunk in [msg[i:i+4000] for i in range(0, len(msg), 4000)]:
            resp = _http.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                data={"chat_id": CHAT_ID, "text": chunk},
                timeout=15,
            )
            if resp.status_code != 200:
                print(f"⚠ Telegram HTTP {resp.status_code}: {resp.text[:200]}")
            else:
                print(f"📩 Sent ({len(chunk)} chars)")
    except Exception as e:
        print(f"Telegram error: {e}")


# ══════════════════════════════════════════════════════════════
# BROWSER
# ══════════════════════════════════════════════════════════════
def launch():
    pw      = sync_playwright().start()
    browser = pw.chromium.launch(
        headless=True,
        args=["--disable-blink-features=AutomationControlled"],
    )
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 800},
    )
    context.set_default_timeout(15000)
    context.set_default_navigation_timeout(30000)
    context.route(
        "**/*",
        lambda route: route.abort()
        if route.request.resource_type in ("image", "font", "media")
        else route.continue_(),
    )
    return pw, browser, context


# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════
def _parse_surface(title: str) -> str:
    t = (title or "").lower()
    if "clay"   in t: return "clay"
    if "grass"  in t: return "grass"
    if "indoor" in t: return "indoors"
    if "hard"   in t: return "hard"
    return "unknown"

def _surface_from_name(name: str) -> str:
    n = (name or "").lower()
    CLAY  = ["roland","french","clay","madrid","rome","barcelona","prague",
             "bucharest","bogota","marrakech","istanbul","rabat","strasbourg",
             "parma","hamburg","warsaw","rouen","oeiras","estoril"]
    GRASS = ["wimbledon","grass","eastbourne","birmingham","bad homburg",
             "rosmalen","s-hertogenbosch","nottingham"]
    IND   = ["indoor","doha","dubai","abu dhabi","st. petersburg","linz",
             "luxembourg","ostrava","guadalajara indoor"]
    if any(k in n for k in CLAY):  return "clay"
    if any(k in n for k in GRASS): return "grass"
    if any(k in n for k in IND):   return "indoors"
    return "hard"

def _safe_float(txt: str):
    try:    return float((txt or "").strip())
    except: return None

def _wl(txt: str):
    txt = (txt or "").strip()
    if not txt or txt == "-" or "/" not in txt:
        return None, None
    try:
        w, l = txt.split("/", 1)
        return int(w.strip()), int(l.strip())
    except:
        return None, None

def _parse_match_date(txt: str) -> datetime | None:
    """
    Handle tennisexplorer date formats:
      "19.04."     — DD.MM. only (inject current year)
      "19.04.2026" — full date
      "2026-04-19" — ISO
    """
    txt = (txt or "").strip()
    if not txt:
        return None
    year = datetime.now().year
    m = re.match(r"^(\d{1,2})\.(\d{1,2})\.$", txt)
    if m:
        try:
            return datetime(year, int(m.group(2)), int(m.group(1)))
        except Exception:
            return None
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(txt, fmt)
        except Exception:
            continue
    return None

def is_allowed_tournament(name: str, check_date: bool = False) -> bool:
    """
    Whitelist approach — only pass genuine WTA tour events.

    ALLOW:  tournaments containing a known WTA keyword OR
            an explicit WTA tier label (WTA, Grand Slam etc.)
    BLOCK:  ITF, UTR, futures, challengers, W15-W100, and any
            unrecognised event (avoids random 125K/250 domestic cups)

    ACTIVE WINDOW CHECK (check_date=True):
        Cross-references the tournament name against the known 2026 calendar
        windows. If today's date is clearly outside a tournament's window
        (e.g. Charleston appearing in late April), the match is rejected.
        Used for tomorrow-fetch validation to catch stale ghost matches.
    """
    n  = (name or "").strip()
    nl = n.lower()

    if not n:
        return False

    # Hard blocks — always reject these regardless of anything else
    if "itf"        in nl: return False
    if "utr"        in nl: return False
    if "futures"    in nl: return False
    if "challenger" in nl: return False
    if _ITF_PREFIXES.search(n): return False

    # Explicit WTA tier labels
    WTA_LABELS = ("wta", "grand slam", "wimbledon", "us open",
                  "australian open", "french open", "roland garros")
    if any(k in nl for k in WTA_LABELS):
        pass   # still check date window below if requested
    elif not any(k in nl for k in [
        # Grand Slams
        "wimbledon", "us open", "australian open", "french open", "roland",
        # WTA 1000
        "indian wells", "miami", "madrid", "rome", "montreal", "toronto",
        "cincinnati", "beijing", "wuhan", "guangzhou", "doha", "dubai",
        # WTA 500
        "abu dhabi", "adelaide", "auckland", "berlin", "birmingham",
        "bad homburg", "eastbourne", "strasbourg", "lyon", "tokyo",
        "osaka", "san jose", "washington", "cleveland", "guadalajara",
        "linz", "ostrava", "chicago",
        # WTA 250
        "hobart", "hua hin", "monterrey", "bogota", "budapest",
        "rabat", "hamburg", "hertogenbosch", "rosmalen", "nottingham",
        "palermo", "lausanne", "san diego", "granby",
        "tashkent", "seoul", "zhengzhou", "nanchang", "tianjin",
        "luxembourg", "tallinn", "porsche", "stuttgart", "charleston",
        "prague", "bucharest", "istanbul", "marrakech", "estoril",
        "oeiras", "rouen", "saint-malo",
    ]):
        print(f"   FILTER: unrecognised tournament blocked → {n}")
        return False

    # ── Active window check ───────────────────────────────────────
    # Each entry: (keyword_in_name, earliest_month, earliest_day,
    #                                latest_month,  latest_day)
    # Gives a ±3 day buffer around the real schedule.
    # Only blocks obvious mismatches — a tournament can appear
    # 3 days before its start (qualifying) or 3 days after its end.
    if check_date:
        today = datetime.now()

        WINDOWS: list[tuple] = [
            # (name_keyword,  start_mm, start_dd, end_mm, end_dd)
            ("charleston",     3, 27,  4,  8),   # late Mar – early Apr
            ("bogota",         3, 27,  4,  8),   # same week as Charleston
            ("stuttgart",      4, 10,  4, 22),
            ("rouen",          4, 10,  4, 22),
            ("madrid",         4, 17,  5,  6),
            ("oeiras",         4, 24,  5,  6),
            ("saint-malo",     4, 24,  5,  6),
            ("rome",           5,  1,  5, 20),
            ("strasbourg",     5, 15,  5, 27),
            ("rabat",          5, 15,  5, 27),
            ("roland",         5, 22,  6, 10),
            ("french open",    5, 22,  6, 10),
            ("birmingham",     6,  5,  6, 17),
            ("hertogenbosch",  6,  5,  6, 17),
            ("berlin",         6, 12,  6, 24),
            ("nottingham",     6, 12,  6, 24),
            ("bad homburg",    6, 19,  7,  1),
            ("eastbourne",     6, 19,  7,  1),
            ("wimbledon",      6, 26,  7, 14),
            ("iasi",           7, 10,  7, 22),
            ("hamburg",        7, 17,  7, 29),
            ("prague",         7, 17,  7, 29),
            ("washington",     7, 24,  8,  5),
            ("memphis",        7, 24,  8,  5),
            ("toronto",        8,  1, 8, 12),
            ("montreal",       8,  1, 8, 12),
            ("cincinnati",     8,  8, 8, 20),
            ("monterrey",      8, 21,  9,  2),
            ("us open",        8, 28,  9, 15),
            ("guadalajara",    9, 11,  9, 23),
            ("beijing",        9, 25, 10, 14),
            ("wuhan",         10,  9, 10, 21),
            ("ningbo",        10, 16, 10, 28),
            ("osaka",         10, 16, 10, 28),
            ("tokyo",         10, 23, 11,  4),
            ("guangzhou",     10, 23, 11,  4),
            ("chennai",       10, 30, 11, 10),
            ("hong kong",     10, 30, 11, 10),
            ("singapore",      9, 18,  9, 30),
            ("seoul",          9, 18,  9, 30),
            ("doha",           2,  6,  2, 18),
            ("dubai",          2, 13,  2, 25),
            ("abu dhabi",      1, 30,  2, 11),
            ("linz",           1, 30,  2, 11),
            ("indian wells",   3,  1,  3, 17),
            ("miami",          3, 14,  3, 31),
            ("australian open",1, 16,  2,  3),
        ]

        for keyword, s_mo, s_day, e_mo, e_day in WINDOWS:
            if keyword not in nl:
                continue
            # Build start/end with 3-day buffer
            year = today.year
            try:
                start = datetime(year, s_mo, s_day) - timedelta(days=3)
                end   = datetime(year, e_mo, e_day) + timedelta(days=3)
                if not (start <= today <= end):
                    print(
                        f"   FILTER: {n} outside active window "
                        f"({s_day:02d}/{s_mo:02d}–{e_day:02d}/{e_mo:02d}) → blocked"
                    )
                    return False
            except Exception:
                pass   # date arithmetic edge case — don't block
            break      # matched a window, no need to check more

    return True

_ERROR_TITLES = ("just a moment","access denied","429","too many requests","error 429")

def _is_error_page(page) -> bool:
    try:
        if any(e in page.url.lower() for e in ("chrome-error://","cloudflare","/cdn-cgi/")):
            return True
        if any(e in page.title().lower() for e in _ERROR_TITLES):
            return True
    except Exception:
        pass
    return False

def _safe_goto(page, url: str, context, retries: int = 2) -> bool:
    for attempt in range(retries + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=20000)
            if _is_error_page(page):
                wait_ms = (2 ** attempt) * 3000 + random.randint(500, 1500)
                print(f"   ⚠ Rate limit — backing off {wait_ms//1000}s...")
                page.wait_for_timeout(wait_ms)
                continue
            return True
        except Exception as e:
            err = str(e)
            if any(x in err for x in ("ERR_CONNECTION_RESET","interrupted","net::")):
                wait_ms = (2 ** attempt) * 4000 + random.randint(1000, 2000)
                print(f"   ⚠ Connection reset (attempt {attempt+1}) — waiting {wait_ms//1000}s...")
                try:
                    page.wait_for_timeout(wait_ms)
                    page.goto("about:blank", wait_until="domcontentloaded", timeout=5000)
                except Exception:
                    pass
            else:
                print(f"   ❌ Nav error: {e}")
                return False
    return False


# ══════════════════════════════════════════════════════════════
# STEP 1 — GET MATCHES
# ══════════════════════════════════════════════════════════════
def get_matches(context, day: str = "today", label: str = "") -> list:
    page    = context.new_page()
    matches = []
    tag     = f"[{label}] " if label else ""
    print(f"🔍 {tag}Loading matches (day={day})...")
    try:
        if not _safe_goto(page, f"{BASE}/matches/?type=wta-single&day={day}", context):
            print(f"❌ {tag}Failed to load matches page.")
            return []
        try:
            page.wait_for_selector("td.first.time", timeout=15000)
        except Exception:
            pass
        try:
            page.wait_for_selector("td.s-color span[title]", timeout=5000)
        except Exception:
            pass
        page.wait_for_timeout(600)
        print(f"✅ {tag}Page loaded")

        rows = page.query_selector_all("tr")
        print(f"   Total tr rows: {len(rows)}")

        current_tournament = "Unknown"
        current_surface    = "hard"

        i = 0
        while i < len(rows) - 1:
            row1 = rows[i]

            # ── Tournament header ────────────────────────────
            try:
                t_name_td = row1.query_selector("td.t-name")
                if t_name_td:
                    player_links = [
                        l for l in t_name_td.query_selector_all("a")
                        if "/player/" in (l.get_attribute("href") or "")
                    ]
                    if not player_links:
                        a = t_name_td.query_selector("a")
                        if a:
                            current_tournament = a.inner_text().strip()
                        s = row1.query_selector("td.s-color span[title]")
                        if s:
                            current_surface = _parse_surface(s.get_attribute("title") or "")
                        else:
                            inf = _surface_from_name(current_tournament)
                            if inf != "unknown":
                                current_surface = inf
                        i += 1
                        continue
            except Exception:
                pass

            # ── Match row ────────────────────────────────────
            try:
                time_el = row1.query_selector("td.first.time")
                p1_el   = row1.query_selector("td.t-name a[href*='/player/']")
                if not time_el or not p1_el:
                    i += 1
                    continue

                row2  = rows[i + 1]
                p2_el = row2.query_selector("td.t-name a[href*='/player/']")
                if not p2_el:
                    i += 1
                    continue

                p1_name = p1_el.inner_text().strip()
                p2_name = p2_el.inner_text().strip()
                slug1   = (p1_el.get_attribute("href") or "").strip("/").split("/")[-1]
                slug2   = (p2_el.get_attribute("href") or "").strip("/").split("/")[-1]

                # ── Date validation ───────────────────────────
                # td.first.time contains "29.04. 19:00" or just "19:00"
                # For tomorrow fetches (day="1"), reject matches whose date
                # doesn't match actual tomorrow — this filters out stale
                # completed matches from closed tournaments that tennisexplorer
                # still serves on the &day=1 page.
                time_raw   = time_el.inner_text().strip()
                match_time = time_raw   # default display value

                if day != "today":
                    expected = datetime.now() + timedelta(days=int(day) if day.lstrip("-").isdigit() else 1)
                    # Parse "DD.MM. HH:MM" — extract date part if present
                    date_match = re.match(r"(\d{1,2})\.(\d{1,2})\.", time_raw)
                    if date_match:
                        m_day = int(date_match.group(1))
                        m_mon = int(date_match.group(2))
                        if m_day != expected.day or m_mon != expected.month:
                            print(f"   SKIP (wrong date {m_day:02d}.{m_mon:02d} ≠ expected {expected.day:02d}.{expected.month:02d}): {p1_name} vs {p2_name}")
                            i += 2
                            continue
                        # Strip date prefix from display time
                        time_part = re.sub(r"^\d{1,2}\.\d{1,2}\.\s*", "", time_raw).strip()
                        if time_part:
                            match_time = time_part

                surface = current_surface
                s_span  = row1.query_selector("td.s-color span[title]")
                if s_span:
                    parsed = _parse_surface(s_span.get_attribute("title") or "")
                    if parsed != "unknown":
                        surface = parsed

                oc      = row1.query_selector_all("td.course")
                odds_p1 = _safe_float(oc[0].inner_text()) if len(oc) > 0 else None
                odds_p2 = _safe_float(oc[1].inner_text()) if len(oc) > 1 else None

                if not is_allowed_tournament(current_tournament, check_date=(day != "today")):
                    i += 2
                    continue

                round_label = ""
                try:
                    rt = row1.query_selector("td.round, td.r")
                    if rt:
                        rm = {"F":"🏆 Final","SF":"🥈 Semi","QF":"⚡ QF",
                              "R16":"R16","R32":"R32","R64":"R64","R128":"R128",
                              "1R":"R1","2R":"R2","3R":"R3"}
                        round_label = rm.get(rt.inner_text().strip().upper(),
                                             rt.inner_text().strip())
                except Exception:
                    pass

                matches.append({
                    "p1": p1_name, "p2": p2_name,
                    "slug1": slug1, "slug2": slug2,
                    "tournament": current_tournament,
                    "surface": surface,
                    "time": match_time,
                    "odds_p1": odds_p1, "odds_p2": odds_p2,
                    "round": round_label,
                })
                i += 2

            except Exception:
                i += 1

    finally:
        page.close()

    print(f"🎾 {tag}Matches parsed: {len(matches)}")
    return matches


# ══════════════════════════════════════════════════════════════
# STEP 2 — PLAYER DATA
# ══════════════════════════════════════════════════════════════
def get_player_data(context, slug: str, display_name: str,
                    surface: str = "hard", page=None) -> dict:
    if slug in _player_cache:
        return _player_cache[slug]

    # Serve stats: static table or tour average — no network call needed
    serve = get_serve_stats(slug, surface, display_name)
    src   = serve["source"]
    print(f"   Fetching {display_name} ({slug})  [serve src={src}]...")

    default = {
        "rank": 500, "sw": {}, "sl": {},
        "recent_wins": 0, "recent_total": 0,
        "recent_surface_wins": {}, "recent_surface_total": {},
        "streak": 0, "days_rest": 3, "matches_30d": 0,
        "serve_win_pct":   serve["serve_win_pct"],
        "first_serve_pct": serve["first_serve_pct"],
    }
    if not slug:
        return default

    owns_page = page is None
    if owns_page:
        page = context.new_page()

    try:
        ok = _safe_goto(page, f"{BASE}/player/{slug}/", context, retries=1)
        if not ok or _is_error_page(page):
            print(f"   ❌ Could not load {display_name} — using defaults")
            return default
        try:
            page.wait_for_selector("table.plDetail", timeout=5000)
        except Exception:
            return default

        # ── Rank ──────────────────────────────────────────────
        rank = 500
        try:
            for div in page.query_selector_all("table.plDetail td div.date"):
                txt = div.inner_text()
                if "singles" in txt and "rank" in txt.lower():
                    m = _RANK_RE.search(txt)
                    if m:
                        rank = int(m.group(1))
                        break
        except Exception as e:
            print(f"   rank err: {e}")

        # ── Surface W/L (2-year weighted) ─────────────────────
        sw, sl = {}, {}
        try:
            btables = page.query_selector_all("table.result.balance")
            if btables:
                tbody_rows = btables[0].query_selector_all("tbody tr")
                for yr_idx, weight in [(0, 2), (1, 1)]:
                    if yr_idx >= len(tbody_rows):
                        break
                    cells = tbody_rows[yr_idx].query_selector_all("td")
                    for surf, col in SURFACE_COL.items():
                        if col >= len(cells):
                            continue
                        w, l = _wl(cells[col].inner_text())
                        if w is None:
                            continue
                        sw[surf] = sw.get(surf, 0) + w * weight
                        sl[surf] = sl.get(surf, 0) + l * weight
        except Exception as e:
            print(f"   balance err: {e}")

        # ── Recent form + streak + fatigue ────────────────────
        recent_wins = recent_total = streak = matches_30d = 0
        days_rest   = 3
        streak_locked = False
        recent_surface_wins  = {}
        recent_surface_total = {}
        today_dt = datetime.now()

        name_parts = display_name.lower().split() if display_name else []
        first_nm   = name_parts[0].rstrip(".") if name_parts else ""
        last_nm    = name_parts[-1].rstrip(".") if len(name_parts) > 1 else ""

        try:
            year = today_dt.year
            mdiv = (page.query_selector(f"div#matches-{year}-1-data") or
                    page.query_selector(f"div#matches-{year-1}-1-data"))
            if mdiv:
                for idx, mrow in enumerate(
                    mdiv.query_selector_all("tr.one, tr.two")[:20]
                ):
                    t_td = mrow.query_selector("td.t-name")
                    if not t_td:
                        continue
                    strong = t_td.query_selector("strong")
                    if not strong:
                        continue
                    score_td = mrow.query_selector("td.score, td.tl")
                    if not score_td or not score_td.inner_text().strip():
                        continue

                    s_span = mrow.query_selector("td.s-color span[title]")
                    m_surf = _parse_surface(s_span.get_attribute("title") if s_span else "")

                    strong_txt = strong.inner_text().lower()
                    won = bool(first_nm) and (
                        (first_nm in strong_txt and last_nm in strong_txt)
                        if last_nm else first_nm in strong_txt
                    )

                    # streak_locked checked BEFORE incrementing (bug fix)
                    if not streak_locked:
                        if idx == 0:
                            streak = 1 if won else -1
                        elif won and streak > 0:
                            streak += 1
                        elif not won and streak < 0:
                            streak -= 1
                        else:
                            streak_locked = True
                    streak = max(-10, min(10, streak))

                    date_td = (mrow.query_selector("td.date") or
                               mrow.query_selector("td.first.date") or
                               mrow.query_selector("td:first-child"))
                    match_date = _parse_match_date(date_td.inner_text()) if date_td else None

                    if idx == 0 and match_date:
                        days_rest = max(0, (today_dt - match_date).days)
                    if match_date and (today_dt - match_date).days <= 30:
                        matches_30d += 1

                    if won:
                        recent_wins += 1
                        recent_surface_wins[m_surf]  = recent_surface_wins.get(m_surf, 0) + 1
                    recent_total += 1
                    recent_surface_total[m_surf] = recent_surface_total.get(m_surf, 0) + 1

        except Exception as e:
            print(f"   form err: {e}")

        data = {
            "rank":                 rank,
            "sw":                   sw,
            "sl":                   sl,
            "recent_wins":          recent_wins,
            "recent_total":         recent_total,
            "recent_surface_wins":  recent_surface_wins,
            "recent_surface_total": recent_surface_total,
            "streak":               streak,
            "days_rest":            days_rest,
            "matches_30d":          matches_30d,
            "serve_win_pct":        serve["serve_win_pct"],
            "first_serve_pct":      serve["first_serve_pct"],
        }
        _player_cache[slug] = data
        return data

    except Exception as e:
        print(f"   ❌ {display_name}: {e}")
        return default
    finally:
        if owns_page:
            page.close()


# ══════════════════════════════════════════════════════════════
# PREFETCH
# ══════════════════════════════════════════════════════════════
def _prefetch(context, matches):
    seen, uniq = set(), []
    for m in matches[:50]:
        for sk, nk, surf_key in (("slug1","p1","surface"), ("slug2","p2","surface")):
            s = (m.get(sk) or "").strip()
            if s and s not in seen:
                seen.add(s)
                uniq.append((s, m.get(nk) or s, m.get(surf_key, "hard")))
            if len(uniq) >= PREFETCH_LIMIT:
                break
        if len(uniq) >= PREFETCH_LIMIT:
            break

    if not uniq:
        return
    print(f"[PREFETCH] Warming {len(uniq)} players...")
    page   = context.new_page()
    errors = 0
    try:
        for idx, (slug, name, surface) in enumerate(uniq):
            page.wait_for_timeout(random.randint(900, 1600))
            if errors >= 3:
                print("   ♻ Recreating page...")
                try: page.close()
                except Exception: pass
                page   = context.new_page()
                errors = 0
                page.wait_for_timeout(6000)
            ok = _safe_goto(page, f"{BASE}/player/{slug}/", context, retries=1)
            if not ok:
                errors += 1
                print(f"   SKIP: {name}")
                continue
            try:
                page.wait_for_selector("table.plDetail", timeout=5000)
                get_player_data(context, slug, name, surface=surface, page=page)
                errors = 0
                print(f"   ✅ [{idx+1}/{len(uniq)}] {name}")
            except Exception as e:
                errors += 1
                print(f"   ⚠ {name}: {e}")
    finally:
        try: page.close()
        except Exception: pass


# ══════════════════════════════════════════════════════════════
# SCORING MODEL
# ══════════════════════════════════════════════════════════════
def score_player(data: dict, surface: str, slug: str = "") -> float:
    rank      = data.get("rank", 500)
    sw        = data.get("sw", {})
    sl        = data.get("sl", {})
    rw        = data.get("recent_wins", 0)
    rt        = data.get("recent_total", 0)
    streak    = data.get("streak", 0)
    days_rest = data.get("days_rest", 3)
    serve_pct = data.get("serve_win_pct")
    elo_surf  = get_surface_elo(slug, surface) if slug else 1500.0

    rank_score = max(0.0, 1.0 - rank / 500.0)

    w = sw.get(surface, 0)
    l = sl.get(surface, 0)
    surf_score = (w / (w + l)) if (w + l) >= 5 else 0.5

    form_score = (rw / rt) if rt >= 4 else 0.5
    if serve_pct is not None:
        form_score = form_score * 0.6 + serve_pct * 0.4

    elo_score    = min(1.0, elo_surf / 2000.0)
    streak_score = 0.5 + max(-0.4, min(0.4, streak * 0.08))

    if   days_rest == 0: fatigue_score = 0.30
    elif days_rest == 1: fatigue_score = 0.55
    elif days_rest <= 3: fatigue_score = 0.80
    elif days_rest <= 7: fatigue_score = 0.70
    else:                fatigue_score = 0.60

    return (
        rank_score    * 0.30 +
        surf_score    * 0.25 +
        form_score    * 0.15 +
        elo_score     * 0.15 +
        streak_score  * 0.08 +
        fatigue_score * 0.07
    )


def h2h_adjustment(prob: float, h2h_wins: int, h2h_total: int) -> float:
    if h2h_total < 2:
        return prob
    weight   = min(0.08, h2h_total * 0.008)
    adjusted = prob * (1 - weight) + (h2h_wins / h2h_total) * weight
    return max(0.10, min(0.90, adjusted))


def odds_blend(model_prob: float, odds_p1, odds_p2) -> tuple:
    if not odds_p1 or not odds_p2:
        return model_prob, 1.0
    raw_p1    = 1.0 / odds_p1
    raw_p2    = 1.0 / odds_p2
    total     = raw_p1 + raw_p2
    market_p1 = raw_p1 / total
    gap       = abs(model_prob - market_p1)
    conf_mult = 0.75 if gap > 0.20 else (0.90 if gap > 0.10 else 1.00)
    return model_prob * 0.70 + market_p1 * 0.30, conf_mult


def win_prob(s1: float, s2: float) -> float:
    return 1.0 / (1.0 + math.exp(-4.0 * (s1 - s2)))

def confidence_pct(prob: float, mult: float = 1.0) -> int:
    base = 50 + abs(prob - 0.5) * 120
    return int(min(80, base * mult))

def market_edge(model_prob: float, fav_odds, other_odds=None) -> float:
    if not fav_odds or fav_odds <= 1.0:
        return 0.0
    raw_fav   = 1.0 / fav_odds
    raw_other = 1.0 / other_odds if other_odds else None
    fair_fav  = raw_fav / (raw_fav + raw_other) if raw_other else raw_fav
    return round(model_prob - fair_fav, 4)

def grade(conf: int) -> str:
    return "HIGH" if conf >= 70 else ("MEDIUM" if conf >= 65 else "LOW")

def grade_icon(g: str) -> str:
    return {"HIGH": "🔥", "MEDIUM": "⚡", "LOW": "🌡️"}.get(g, "")


# ══════════════════════════════════════════════════════════════
# PREDICTION EXTRAS
# ══════════════════════════════════════════════════════════════
def _is_bo5(tournament: str) -> bool:
    t = (tournament or "").lower()
    return any(x in t for x in ["wimbledon","us open","australian","french","roland"])

def _predict_sets(conf: int, bo5: bool) -> str:
    if bo5:
        return "3-0" if conf >= 75 else ("3-1" if conf >= 67 else "3-2")
    return "2-0" if conf >= 73 else "2-1"

def _pred_games(sets_str: str, surface: str) -> int:
    try:
        w = int(sets_str.split("-")[0])
        l = int(sets_str.split("-")[1])
        return round(SURFACE_AVG_GAMES.get(surface, 9.8) * (w + l))
    except Exception:
        return 22

def _ou_line(bo5: bool) -> float:
    return 38.5 if bo5 else 21.5

def _serve_label(serve_pct, surface: str, first_in_pct=None, source="table") -> str:
    base    = SURFACE_SERVE_HOLD.get(surface, 0.72)
    pct_str = f"{round(serve_pct*100)}%"
    if first_in_pct:
        pct_str = f"{round(serve_pct*100)}% pts / {round(first_in_pct*100)}% in"
    suffix = " (avg)" if source == "avg" else ""
    if serve_pct >= base + 0.05: return f"Strong ({pct_str}){suffix}"
    if serve_pct <= base - 0.05: return f"Weak ({pct_str}){suffix}"
    return f"Average ({pct_str}){suffix}"

def _set_handicap(conf: int, winner: str) -> str:
    if conf >= 73: return f"{winner} -1.5 sets"
    if conf >= 65: return f"{winner} -1.5 sets (marginal)"
    return "Skip set handicap"

def _key_factor(d1: dict, d2: dict, p1: str, p2: str, surface: str) -> str:
    s1 = d1.get("serve_win_pct"); s2 = d2.get("serve_win_pct")
    if s1 and s2:
        if s1 > s2 + 0.08: return f"{p1} serve dominates on {surface}"
        if s2 > s1 + 0.08: return f"{p2} serve dominates on {surface}"
    sw1 = d1.get("sw",{}).get(surface,0); sl1 = d1.get("sl",{}).get(surface,0)
    sw2 = d2.get("sw",{}).get(surface,0); sl2 = d2.get("sl",{}).get(surface,0)
    r1  = sw1/(sw1+sl1) if (sw1+sl1) >= 5 else None
    r2  = sw2/(sw2+sl2) if (sw2+sl2) >= 5 else None
    if r1 and r2:
        if r1 > r2 + 0.15: return f"{p1} dominant on {surface} ({round(r1*100)}% win rate)"
        if r2 > r1 + 0.15: return f"{p2} dominant on {surface} ({round(r2*100)}% win rate)"
    r1d, r2d = d1.get("rank",500), d2.get("rank",500)
    if abs(r1d - r2d) > 100:
        return f"Ranking gap — {p1 if r1d < r2d else p2} is the clear favourite"
    st1, st2 = d1.get("streak",0), d2.get("streak",0)
    if st1 >= 4: return f"{p1} on a {st1}-win streak"
    if st2 >= 4: return f"{p2} on a {st2}-win streak"
    return {"clay":"Clay rewards baseline stamina",
            "grass":"Grass favours big servers",
            "indoors":"Indoor slightly boosts serving",
            "hard":"Recent form decides"}.get(surface, "Form decides")


# ══════════════════════════════════════════════════════════════
# EVALUATE MATCHES
# ══════════════════════════════════════════════════════════════
def evaluate_matches(matches: list, context) -> list:
    picks = []
    tag   = "TODAY"

    for m in matches[:30]:
        try:
            print(f"\n── [{tag}] {m['p1']} vs {m['p2']}  [{m['surface']}]  {m['tournament']}")

            d1 = get_player_data(context, m["slug1"], m["p1"], m["surface"])
            d2 = get_player_data(context, m["slug2"], m["p2"], m["surface"])

            s1   = score_player(d1, m["surface"], m["slug1"])
            s2   = score_player(d2, m["surface"], m["slug2"])
            prob = win_prob(s1, s2)

            h2h_key      = f"{min(m['slug1'],m['slug2'])}_{max(m['slug1'],m['slug2'])}"
            h2h_w, h2h_t = _h2h_cache.get(h2h_key, (0, 0))
            prob         = h2h_adjustment(prob, h2h_w, h2h_t)

            blended, conf_mult = odds_blend(prob, m["odds_p1"], m["odds_p2"])
            conf       = confidence_pct(blended, conf_mult)
            favourite  = m["p1"] if blended > 0.5 else m["p2"]
            fav_prob   = max(blended, 1 - blended)
            fav_odds   = m["odds_p1"] if blended > 0.5 else m["odds_p2"]
            other_odds = m["odds_p2"] if blended > 0.5 else m["odds_p1"]
            edge       = market_edge(fav_prob, fav_odds, other_odds)
            pick_grade = grade(conf)

            print(
                f"   {m['p1']:22s} rank={d1['rank']:>3}  streak={d1['streak']:+d}"
                f"  rest={d1['days_rest']}d  score={s1:.3f}\n"
                f"   {m['p2']:22s} rank={d2['rank']:>3}  streak={d2['streak']:+d}"
                f"  rest={d2['days_rest']}d  score={s2:.3f}\n"
                f"   H2H {h2h_w}/{h2h_t}  prob={blended:.3f}  "
                f"conf={conf}%  edge={edge:+.3f}  grade={pick_grade}"
            )

            if conf < 65:
                print(f"   SKIP: conf {conf}%"); continue
            if abs(blended - 0.5) < 0.07:
                print(f"   SKIP: too close"); continue

            bo5        = _is_bo5(m["tournament"])
            pred_sets  = _predict_sets(conf, bo5)
            pred_games = _pred_games(pred_sets, m["surface"])
            ou_val     = _ou_line(bo5)

            # Serve label — show (avg) tag so user knows when it's estimated
            srv_data1 = get_serve_stats(m["slug1"], m["surface"], m["p1"])
            srv_data2 = get_serve_stats(m["slug2"], m["surface"], m["p2"])

            picks.append({
                "m": m, "d1": d1, "d2": d2,
                "s1": s1, "s2": s2,
                "prob": fav_prob, "conf": conf, "grade": pick_grade,
                "winner": favourite, "winner_odds": fav_odds,
                "edge": edge,
                "streak1":    d1.get("streak", 0),
                "streak2":    d2.get("streak", 0),
                "pred_sets":  pred_sets,
                "pred_games": pred_games,
                "over_under": ("Over" if pred_games >= ou_val else "Under") + f" {ou_val}",
                "handicap":   _set_handicap(conf, favourite),
                "serve1":     _serve_label(srv_data1["serve_win_pct"], m["surface"],
                                           srv_data1["first_serve_pct"],
                                           srv_data1["source"]),
                "serve2":     _serve_label(srv_data2["serve_win_pct"], m["surface"],
                                           srv_data2["first_serve_pct"],
                                           srv_data2["source"]),
                "key_factor": _key_factor(d1, d2, m["p1"], m["p2"], m["surface"]),
            })

        except Exception as e:
            print(f"   ERROR: {e}")

    return picks


# ══════════════════════════════════════════════════════════════
# FORMAT ONE PICK
# ══════════════════════════════════════════════════════════════
def format_pick(pk: dict) -> str:
    m        = pk["m"]
    icon     = grade_icon(pk["grade"])
    odds_str = f" @ {pk['winner_odds']}" if pk["winner_odds"] else ""
    bar      = "█" * (pk["conf"] // 10) + "░" * (10 - pk["conf"] // 10)
    rnd      = f"  · {m['round']}" if m.get("round") else ""
    return (
        f"─────────────────────────\n"
        f"🏟  {m['tournament']}  ({m['surface'].title()}){rnd}\n"
        f"⚔️  {m['p1']} vs {m['p2']}\n"
        f"{icon} {pk['grade']}  ✅  {pk['winner']}{odds_str}\n"
        f"[{bar}] {pk['conf']}%\n"
        f"Ranks: #{pk['d1']['rank']} vs #{pk['d2']['rank']}\n"
        f"Scores: {pk['s1']:.2f} vs {pk['s2']:.2f}\n"
        f"Streak: {pk['streak1']:+d} vs {pk['streak2']:+d}\n"
        f"📊 Sets: {pk['pred_sets']}  |  {pk['over_under']} games\n"
        f"🎯 Handicap: {pk['handicap']}\n"
        f"🏓 Serve: {pk['serve1']} vs {pk['serve2']}\n"
        f"💡 {pk['key_factor']}\n"
        f"Edge: {pk['edge']:+.2f}  ⏰ {m['time']}\n"
    )


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
def run():
    t0 = time.time()
    pw, browser, context = launch()

    try:
        # ── TODAY ────────────────────────────────────────────
        today_matches = get_matches(context, day="today", label="TODAY")
        if not today_matches:
            print("❌ No matches found today.")
            send_telegram("No WTA matches found today.")
            return

        _prefetch(context, today_matches)
        today_raw   = evaluate_matches(today_matches, context)
        today_picks = apply_mode_filters(today_raw)
        # HIGH picks first, then MEDIUM — both sorted by conf descending within tier
        today_picks.sort(key=lambda x: (0 if x["grade"] == "HIGH" else 1, -x["conf"]))

        if not today_picks:
            print(f"[{RUN_MODE.upper()}] No qualifying picks today.")
            return

        mode_label = {
            "normal":      "",
            "daily_reset": "🌅 Daily Signals  ",
            "force":       "⚡ Manual Run  ",
        }.get(RUN_MODE, "")

        today_str = datetime.now(WAT).strftime("%A %d %B %Y")
        lines     = [f"🎾 WTA ENGINE v7  {mode_label}\n📅 {today_str}\n"]

        has_medium = any(pk["grade"] == "MEDIUM" for pk in today_picks)
        has_high   = any(pk["grade"] == "HIGH"   for pk in today_picks)

        prev_grade = None
        for pk in today_picks[:6]:
            if has_high and has_medium and prev_grade == "HIGH" and pk["grade"] == "MEDIUM":
                lines.append("── ⚡ medium confidence ──\n")
            lines.append(format_pick(pk))
            prev_grade = pk["grade"]

        sent_time = datetime.now(WAT).strftime("%H:%M WAT")
        lines.append(f"⚠️ For entertainment only.\n🕐 Sent at {sent_time}")
        msg = "\n".join(lines)
        print("\n" + msg)
        send_telegram(msg)

        if should_mark_sent():
            mark_as_sent(today_picks[:6])

    finally:
        browser.close()
        pw.stop()
        print(f"\n⚡ Done in {round(time.time() - t0, 1)}s")


if __name__ == "__main__":
    run()

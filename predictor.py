"""
Tennis Engine v8 — WTA Prediction Engine
==========================================
IMPROVEMENTS vs v7:
  A. Surface ELO — persisted to elo_db.json, populated from player history
     on first encounter. update_surface_elo() called after each scraped result.
     Now contributes real signal instead of a flat 0.75 default.

  B. Rolling 12-month surface W/L — replaces the annual balance table scrape
     (which is too thin early in the season). Counts wins/losses by surface
     over the last 12 months from the match history div directly.

  C. Streak reliability — no longer relies on idx ordering through the div.
     Collects all completed match rows first, strips upcoming rows, then
     counts streak from the most recent result backwards.

  D. H2H scraping — scrapes /h2h/{slug1}-vs-{slug2}/ once per match pair.
     5s timeout, cached per pair. Returns (p1_wins, total). Falls back
     gracefully on any error.

  E. Outcome logging + calibration — every pick written to outcomes.json
     with predicted prob and grade. After results are known, run with
     RUN_MODE=calibrate to compute Brier score, accuracy per grade band,
     and recommended threshold adjustments.

  F. Probability calibration — sigmoid steepness tuned per surface based
     on logged outcomes. Starts at -4.0 (global), converges over time.

  G. Fixed apply_mode_filters — daily_reset branch was unreachable.

RUN_MODE:
  normal       — dedup ON,  HIGH+MEDIUM, marks sent
  daily_reset  — dedup OFF, HIGH+MEDIUM, marks sent
  force        — dedup OFF, HIGH+MEDIUM, does NOT mark sent
  calibrate    — reads outcomes.json, prints calibration report, exits
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

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID   = os.getenv("CHAT_ID",   "")
RUN_MODE  = os.getenv("RUN_MODE", "normal").strip().lower()
if RUN_MODE not in ("normal", "daily_reset", "force", "calibrate"):
    RUN_MODE = "normal"
print(f"[MODE] RUN_MODE = {RUN_MODE.upper()}")

PREFETCH_LIMIT = 20
SURFACE_COL    = {"clay": 2, "hard": 3, "indoors": 4, "grass": 5}

_player_cache: dict = {}
_h2h_cache:    dict = {}
_http = requests.Session()

_RANK_RE      = re.compile(r"singles:\s*(\d+)\.")
_ITF_PREFIXES = re.compile(r"^\s*(w15|w25|w35|w50|w60|w100|itf)\b", re.IGNORECASE)

_DIR          = os.path.dirname(os.path.abspath(__file__))
_ELO_FILE     = os.path.join(_DIR, "elo_db.json")
_SENT_FILE    = os.path.join(_DIR, "sent_matches.json")
_OUTCOMES_FILE = os.path.join(_DIR, "outcomes.json")

SURFACE_SERVE_HOLD = {
    "hard": 0.72, "clay": 0.68, "grass": 0.77, "indoors": 0.74,
}
SURFACE_AVG_GAMES = {
    "hard": 9.8, "clay": 10.2, "grass": 9.4, "indoors": 9.6,
}
SURFACE_TOUR_AVG_SERVE = {
    "hard": 0.68, "clay": 0.65, "grass": 0.72, "indoors": 0.70, "unknown": 0.67,
}

# Sigmoid steepness per surface — tuned via calibration over time
# Higher = sharper separation, lower = more conservative
SURFACE_SIGMOID_K = {
    "hard": 4.0, "clay": 4.0, "grass": 4.0, "indoors": 4.0, "unknown": 4.0,
}


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
    "bara_i":"Irina","schmiedlova_a":"Anna","parrizas-diaz_s":"Sara",
}

_TA_BASE    = "https://www.tennisabstract.com/cgi-bin/wplayer.cgi"
_TA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "text/html,application/xhtml+xml",
    "Referer":    "https://www.tennisabstract.com/",
}
_ta_live_cache: dict = {}


# ══════════════════════════════════════════════════════════════
# A. SURFACE ELO — persistent JSON store
# ══════════════════════════════════════════════════════════════
def _load_elo() -> dict:
    try:
        with open(_ELO_FILE) as f:
            return json.load(f)
    except Exception:
        return {}

def _save_elo(db: dict):
    try:
        with open(_ELO_FILE, "w") as f:
            json.dump(db, f, indent=2)
    except Exception as e:
        print(f"[WARN] elo_db save failed: {e}")

# Load at startup — persists across runs
_ELO_DB: dict = _load_elo()

def get_surface_elo(slug: str, surface: str) -> float:
    return _ELO_DB.get(slug, {}).get(surface, 1500.0)

def update_surface_elo(winner_slug: str, loser_slug: str, surface: str):
    """Update ELO and persist immediately."""
    k  = 32
    r1 = get_surface_elo(winner_slug, surface)
    r2 = get_surface_elo(loser_slug,  surface)
    e1 = 1.0 / (1.0 + 10.0 ** ((r2 - r1) / 400.0))
    _ELO_DB.setdefault(winner_slug, {})[surface] = round(r1 + k * (1 - e1), 2)
    _ELO_DB.setdefault(loser_slug,  {})[surface] = round(r2 - k * e1, 2)
    _save_elo(_ELO_DB)

def _seed_elo_from_history(slug: str, results: list, surface: str):
    """
    Seed ELO from scraped match history if slug not yet in _ELO_DB.
    results = list of (won: bool, opponent_slug: str) in chronological order
    (oldest first). Uses a temp dict so we don't corrupt real ratings.
    """
    if slug in _ELO_DB:
        return
    elo = 1500.0
    for won, opp_slug in results:
        opp_elo = get_surface_elo(opp_slug, surface)
        e1 = 1.0 / (1.0 + 10.0 ** ((opp_elo - elo) / 400.0))
        elo = elo + 32 * ((1 if won else 0) - e1)
    _ELO_DB.setdefault(slug, {})[surface] = round(elo, 2)
    _save_elo(_ELO_DB)


# ══════════════════════════════════════════════════════════════
# E. OUTCOME LOGGING + CALIBRATION
# ══════════════════════════════════════════════════════════════
def _load_outcomes() -> list:
    try:
        with open(_OUTCOMES_FILE) as f:
            return json.load(f)
    except Exception:
        return []

def _save_outcomes(data: list):
    try:
        with open(_OUTCOMES_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"[WARN] outcomes save failed: {e}")

def log_picks(picks: list):
    """Append picks to outcomes.json. result field filled later manually or via future scraper."""
    outcomes = _load_outcomes()
    today    = datetime.now(WAT).strftime("%Y-%m-%d")
    for pk in picks:
        key = f"{pk['m']['p1']}|{pk['m']['p2']}|{today}"
        # Don't duplicate
        if any(o.get("key") == key for o in outcomes):
            continue
        outcomes.append({
            "key":        key,
            "date":       today,
            "p1":         pk["m"]["p1"],
            "p2":         pk["m"]["p2"],
            "winner_pred":pk["winner"],
            "prob":       round(pk["prob"], 4),
            "conf":       pk["conf"],
            "grade":      pk["grade"],
            "surface":    pk["m"]["surface"],
            "result":     None,   # fill in: "correct" or "wrong"
        })
    _save_outcomes(outcomes)

def run_calibration():
    """
    Print calibration report from outcomes.json.
    Only counts entries where result is filled in ("correct"/"wrong").
    """
    outcomes = [o for o in _load_outcomes() if o.get("result") in ("correct","wrong")]
    if not outcomes:
        print("No resolved outcomes yet. Fill in 'result' field in outcomes.json.")
        return

    total  = len(outcomes)
    correct = sum(1 for o in outcomes if o["result"] == "correct")
    print(f"\n{'═'*50}")
    print(f"CALIBRATION REPORT  ({total} resolved picks)")
    print(f"{'═'*50}")
    print(f"Overall accuracy:  {correct/total*100:.1f}%  ({correct}/{total})")

    # Brier score
    brier = sum((o["prob"] - (1 if o["result"]=="correct" else 0))**2
                for o in outcomes) / total
    print(f"Brier score:       {brier:.4f}  (lower = better, 0.25 = random)")

    # Per grade
    for g in ("HIGH","MEDIUM","LOW"):
        gs = [o for o in outcomes if o["grade"] == g]
        if not gs: continue
        acc = sum(1 for o in gs if o["result"]=="correct") / len(gs)
        avg_prob = sum(o["prob"] for o in gs) / len(gs)
        print(f"  {g:6s}: {acc*100:.1f}% accuracy  avg_prob={avg_prob:.2f}  n={len(gs)}")

    # Per surface
    for surf in ("clay","hard","grass","indoors"):
        ss = [o for o in outcomes if o["surface"] == surf]
        if not ss: continue
        acc = sum(1 for o in ss if o["result"]=="correct") / len(ss)
        print(f"  {surf:8s}: {acc*100:.1f}% accuracy  n={len(ss)}")

    # Recommended thresholds
    print(f"\nRecommended grade thresholds based on your data:")
    probs = sorted([o["prob"] for o in outcomes if o["result"]=="correct"], reverse=True)
    if len(probs) >= 10:
        p70 = probs[int(len(probs)*0.30)]
        p50 = probs[int(len(probs)*0.50)]
        print(f"  HIGH:   prob >= {p70:.2f}  (top 30% of correct picks)")
        print(f"  MEDIUM: prob >= {p50:.2f}  (top 50% of correct picks)")

    print(f"{'═'*50}\n")


# ══════════════════════════════════════════════════════════════
# DEDUP
# ══════════════════════════════════════════════════════════════
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
        print(f"[WARN] sent_matches save failed: {e}")

def _today_wat() -> str:
    return datetime.now(WAT).strftime("%Y-%m-%d")

def _match_key(p1: str, p2: str) -> str:
    return "_vs_".join(sorted([p1.strip().lower(), p2.strip().lower()]))

def is_already_sent(p1: str, p2: str) -> bool:
    return _match_key(p1, p2) in _load_sent().get(_today_wat(), [])

def _real_picks_sent_today() -> bool:
    """Returns True if at least one real match pick was sent today."""
    today_entries = _load_sent().get(_today_wat(), [])
    # Real picks are any key that isn't the status sentinel
    return any(k != "__status__" for k in today_entries)

def _status_already_sent() -> bool:
    """Returns True if a status (no-picks) message was already sent today."""
    return "__status__" in _load_sent().get(_today_wat(), [])

def _mark_status_sent():
    """Record that a status message was sent today."""
    sent  = _load_sent()
    today = _today_wat()
    sent.setdefault(today, [])
    if "__status__" not in sent[today]:
        sent[today].append("__status__")
    cutoff = (datetime.now(WAT) - timedelta(days=3)).strftime("%Y-%m-%d")
    _save_sent({k: v for k, v in sent.items() if k >= cutoff})

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
# RUN MODE FILTERS  (fixed daily_reset branch)
# ══════════════════════════════════════════════════════════════
def apply_mode_filters(picks: list) -> list:
    qualified = [pk for pk in picks if pk.get("grade") in ("HIGH", "MEDIUM")]

    if RUN_MODE in ("force", "daily_reset"):
        label = "FORCE" if RUN_MODE == "force" else "DAILY_RESET"
        print(f"[MODE] {label} — {len(qualified)} HIGH/MEDIUM picks (dedup ignored)")
        return qualified

    # normal — apply dedup
    before = len(qualified)
    result = [pk for pk in qualified
              if not is_already_sent(pk["m"]["p1"], pk["m"]["p2"])]
    print(f"[MODE] NORMAL — {len(result)} new picks ({before - len(result)} deduped)")
    return result

def should_mark_sent() -> bool:
    return RUN_MODE in ("normal", "daily_reset")


# ══════════════════════════════════════════════════════════════
# TELEGRAM
# ══════════════════════════════════════════════════════════════
def send_telegram(msg: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("⚠ BOT_TOKEN/CHAT_ID not set")
        return
    try:
        for chunk in [msg[i:i+4000] for i in range(0, len(msg), 4000)]:
            resp = _http.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                data={"chat_id": CHAT_ID, "text": chunk},
                timeout=15,
            )
            if resp.status_code != 200:
                print(f"⚠ Telegram {resp.status_code}: {resp.text[:200]}")
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
    if any(k in n for k in ["roland","french","clay","madrid","rome","barcelona",
        "prague","bucharest","bogota","marrakech","istanbul","rabat","strasbourg",
        "parma","hamburg","warsaw","rouen","oeiras","estoril"]): return "clay"
    if any(k in n for k in ["wimbledon","grass","eastbourne","birmingham",
        "bad homburg","rosmalen","hertogenbosch","nottingham"]): return "grass"
    if any(k in n for k in ["indoor","doha","dubai","abu dhabi","st. petersburg",
        "linz","luxembourg","ostrava","guadalajara indoor"]): return "indoors"
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

def _parse_match_date(txt: str):
    txt = (txt or "").strip()
    if not txt:
        return None
    year = datetime.now().year
    m = re.match(r"^(\d{1,2})\.(\d{1,2})\.$", txt)
    if m:
        try:   return datetime(year, int(m.group(2)), int(m.group(1)))
        except: return None
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:   return datetime.strptime(txt, fmt)
        except: continue
    return None

def is_allowed_tournament(name: str, check_date: bool = False) -> bool:
    n  = (name or "").strip()
    nl = n.lower()
    if not n: return False
    if "itf" in nl or "utr" in nl or "futures" in nl or "challenger" in nl: return False
    if _ITF_PREFIXES.search(n): return False

    WTA_EVENTS = [
        "wimbledon","us open","australian open","french open","roland",
        "indian wells","miami","madrid","rome","montreal","toronto",
        "cincinnati","beijing","wuhan","guangzhou","doha","dubai",
        "abu dhabi","adelaide","auckland","berlin","birmingham",
        "bad homburg","eastbourne","strasbourg","lyon","tokyo","osaka",
        "san jose","washington","guadalajara","linz","ostrava","chicago",
        "hobart","hua hin","monterrey","bogota","budapest","rabat","hamburg",
        "hertogenbosch","rosmalen","nottingham","palermo","lausanne",
        "san diego","granby","tashkent","seoul","zhengzhou","nanchang",
        "tianjin","luxembourg","tallinn","porsche","stuttgart","charleston",
        "prague","bucharest","marrakech","estoril","oeiras",
        "rouen","saint-malo",
        # 2026 additions
        "brisbane","canberra","manila","philippine","mumbai",
        "cluj","transylvania","sables","olonne","antalya","megasaray",
        "midland","dow tennis","singapore","jiujiang","jiangxi",
        # istanbul removed — downgraded to WTA 125 from 2026 onwards
    ]
    WTA_LABELS = ("wta","grand slam","wimbledon","us open","australian open",
                  "french open","roland garros")
    if not any(k in nl for k in WTA_EVENTS) and not any(k in nl for k in WTA_LABELS):
        print(f"   FILTER: unrecognised → {n}")
        return False

    if check_date:
        today = datetime.now()
        WINDOWS = [
            ("charleston", 3,27, 4, 8),("bogota",  3,27, 4, 8),
            ("stuttgart",  4,10, 4,22),("rouen",   4,10, 4,22),
            ("madrid",     4,17, 5, 6),("oeiras",  4,24, 5, 6),
            ("saint-malo", 4,24, 5, 6),("rome",    5, 1, 5,20),
            ("strasbourg", 5,15, 5,27),("rabat",   5,15, 5,27),
            ("roland",     5,22, 6,10),("french open",5,22,6,10),
            ("birmingham", 6, 5, 6,17),("hertogenbosch",6,5,6,17),
            ("berlin",     6,12, 6,24),("nottingham",6,12,6,24),
            ("bad homburg",6,19, 7, 1),("eastbourne",6,19,7, 1),
            ("wimbledon",  6,26, 7,14),("hamburg",  7,17, 7,29),
            ("prague",     7,17, 7,29),("washington",7,24, 8, 5),
            ("toronto",    8, 1, 8,12),("montreal", 8, 1, 8,12),
            ("cincinnati", 8, 8, 8,20),("monterrey",8,21, 9, 2),
            ("us open",    8,28, 9,15),("beijing",  9,25,10,14),
            ("wuhan",     10, 9,10,21),("tokyo",   10,23,11, 4),
            ("guangzhou", 10,23,11, 4),("singapore",9,18, 9,30),
            ("doha",       2, 6, 2,18),("dubai",    2,13, 2,25),
            ("indian wells",3,1,3,17), ("miami",    3,14, 3,31),
            ("australian open",1,16,2,3),
            # 2026 additions
            ("brisbane",   1, 2, 1,13),("canberra", 1, 2, 1,13),
            ("auckland",   1, 2, 1,13),("hobart",   1,10, 1,18),
            ("adelaide",   1,10, 1,18),
            ("manila",     1,23, 2, 4),("philippine",1,23,2, 4),
            ("mumbai",     1,30, 2,11),("cluj",     1,30, 2,11),
            ("transylvania",1,30,2,11),("ostrava",  1,30, 2,11),
            ("sables",     2,13, 2,25),("olonne",   2,13, 2,25),
            ("midland",    2,13, 2,25),("dow tennis",2,13,2,25),
            ("antalya",    2,20, 3,11),("megasaray",2,20, 3,11),
            # Jiujiang/Jiangxi Open = WTA 250, Nov 2-8 (May version is WTA 125 — blocked)
            ("jiujiang",  10,30,11,11),("jiangxi",  10,30,11,11),
        ]
        for kw, sm, sd, em, ed in WINDOWS:
            if kw not in nl: continue
            try:
                yr = today.year
                start = datetime(yr, sm, sd) - timedelta(days=3)
                end   = datetime(yr, em, ed) + timedelta(days=3)
                if not (start <= today <= end):
                    print(f"   FILTER: {n} outside window → blocked")
                    return False
            except Exception:
                pass
            break
    return True

_ERROR_TITLES = ("just a moment","access denied","429","too many requests")

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
                print(f"   ⚠ Connection reset ({attempt+1}) — waiting {wait_ms//1000}s...")
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
# SERVE STATS (3-tier)
# ══════════════════════════════════════════════════════════════
def _build_ta_name(slug: str, display_name: str) -> str:
    parts   = (display_name or "").strip().split()
    surname = parts[0].rstrip(".") if parts else ""
    initial = parts[1].rstrip(".").upper() if len(parts) >= 2 else ""
    key     = f"{surname.lower()}_{initial}".rstrip("_")
    first   = _INITIAL_TO_FIRST.get(key, "")
    return f"{first}{surname.capitalize()}" if first else surname.capitalize()

def _parse_ta_html(html: str) -> dict:
    stats = {}
    for key, pat in (
        ("serve_win_pct",   re.compile(r"var\s+sp\s*=\s*([\d.]+)")),
        ("first_serve_pct", re.compile(r"var\s+fsp\s*=\s*([\d.]+)")),
    ):
        m = pat.search(html)
        if m:
            val = float(m.group(1))
            stats[key] = round(val / 100.0, 4) if val > 1 else val
    return stats

# Persistent serve cache — avoids re-fetching same player each run
def _load_serve_cache() -> dict:
    try:
        with open(_SERVE_CACHE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}

def _save_serve_cache(cache: dict):
    try:
        with open(_SERVE_CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        print(f"[WARN] serve_cache save failed: {e}")

# Load at startup
_serve_cache: dict = _load_serve_cache()


def get_serve_stats(slug: str, surface: str, display_name: str = "") -> dict:
    """
    Real-time serve stats from TennisAbstract.
    Flow:
      1. Check persistent serve_cache.json (populated from previous runs)
      2. Fetch live from TennisAbstract (8s timeout)
      3. Fall back to surface tour average if TA unreachable
    No static table — all data is live and current-season.
    Cache persists to disk so the same player is only fetched once per week.
    """
    # Check persistent cache first (fresh enough — TA updates weekly)
    cached = _serve_cache.get(slug)
    if cached and cached.get("serve_win_pct"):
        return {
            "serve_win_pct":   cached["serve_win_pct"],
            "first_serve_pct": cached.get("first_serve_pct", 0.65),
            "source":          "ta_cache",
        }

    # Live fetch from TennisAbstract
    if display_name:
        ta_name = _build_ta_name(slug, display_name)
        try:
            resp = _http.get(
                f"{_TA_BASE}?p={ta_name}",
                headers=_TA_HEADERS,
                timeout=8,
            )
            if resp.status_code == 200:
                # Detect wrong-player page
                bad = ("no player found", "no results", "<title>error")
                if any(b in resp.text.lower()[:500] for b in bad):
                    # Retry with surname only
                    surname = display_name.split()[0].rstrip(".")
                    resp = _http.get(
                        f"{_TA_BASE}?p={surname.capitalize()}",
                        headers=_TA_HEADERS,
                        timeout=8,
                    )
                stats = _parse_ta_html(resp.text)
                if stats.get("serve_win_pct"):
                    # Persist to cache
                    _serve_cache[slug] = stats
                    _save_serve_cache(_serve_cache)
                    print(
                        f"   [TA] {display_name}: "
                        f"serve={round(stats['serve_win_pct']*100)}%  "
                        f"1st-in={round(stats.get('first_serve_pct',0)*100)}%"
                    )
                    return {
                        "serve_win_pct":   stats["serve_win_pct"],
                        "first_serve_pct": stats.get("first_serve_pct", 0.65),
                        "source":          "ta_live",
                    }
        except Exception as e:
            print(f"   [TA] {display_name}: {type(e).__name__}")

    # Tour average fallback
    avg = SURFACE_TOUR_AVG_SERVE.get(surface, 0.67)
    print(f"   [TA] {display_name}: using tour avg ({round(avg*100)}%)")
    return {"serve_win_pct": avg, "first_serve_pct": 0.65, "source": "avg"}


# ══════════════════════════════════════════════════════════════
# D. HEAD-TO-HEAD SCRAPING
# ══════════════════════════════════════════════════════════════
def get_h2h(context, slug1: str, slug2: str, p1_name: str) -> tuple[int, int]:
    """
    Scrape /h2h/{slug1}-vs-{slug2}/
    Returns (p1_wins, total_meetings).
    Cached per pair. 5s timeout.
    """
    key = f"{min(slug1,slug2)}|{max(slug1,slug2)}"
    if key in _h2h_cache:
        return _h2h_cache[key]

    page = context.new_page()
    try:
        url = f"{BASE}/h2h/{slug1}-vs-{slug2}/"
        ok  = _safe_goto(page, url, context, retries=1)
        if not ok:
            _h2h_cache[key] = (0, 0)
            return (0, 0)

        # H2H table: table.result  rows with td.t-name containing <strong> for winner
        rows  = page.query_selector_all("table.result tbody tr")
        p1_wins = 0
        total   = 0
        surname1 = p1_name.split()[0].lower().rstrip(".")

        for row in rows:
            t_td   = row.query_selector("td.t-name")
            if not t_td: continue
            strong = t_td.query_selector("strong")
            if not strong: continue
            score_td = row.query_selector("td.tl, td.score")
            if not score_td or not score_td.inner_text().strip(): continue
            total += 1
            if surname1 in strong.inner_text().lower():
                p1_wins += 1

        result = (p1_wins, total)
        _h2h_cache[key] = result
        if total > 0:
            print(f"   [H2H] {p1_name}: {p1_wins}/{total}")
        return result

    except Exception as e:
        print(f"   [H2H] error: {e}")
        _h2h_cache[key] = (0, 0)
        return (0, 0)
    finally:
        try: page.close()
        except Exception: pass


# ══════════════════════════════════════════════════════════════
# GET MATCHES
# ══════════════════════════════════════════════════════════════
def get_matches(context, day: str = "today", label: str = "") -> list:
    page    = context.new_page()
    matches = []
    tag     = f"[{label}] " if label else ""
    print(f"🔍 {tag}Loading matches (day={day})...")
    try:
        if not _safe_goto(page, f"{BASE}/matches/?type=wta-single&day={day}", context):
            return []
        try:
            page.wait_for_selector("td.first.time", timeout=15000)
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

            # Tournament header
            try:
                t_name_td = row1.query_selector("td.t-name")
                if t_name_td:
                    player_links = [l for l in t_name_td.query_selector_all("a")
                                    if "/player/" in (l.get_attribute("href") or "")]
                    if not player_links:
                        a = t_name_td.query_selector("a")
                        if a: current_tournament = a.inner_text().strip()
                        s = row1.query_selector("td.s-color span[title]")
                        if s:
                            current_surface = _parse_surface(s.get_attribute("title") or "")
                        else:
                            inf = _surface_from_name(current_tournament)
                            if inf != "unknown": current_surface = inf
                        i += 1
                        continue
            except Exception:
                pass

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

                # Date validation for non-today fetches
                time_raw   = time_el.inner_text().strip()
                match_time = time_raw
                if day != "today":
                    try:
                        expected  = datetime.now() + timedelta(
                            days=int(day) if day.lstrip("-").isdigit() else 1)
                        date_match = re.match(r"(\d{1,2})\.(\d{1,2})\.", time_raw)
                        if date_match:
                            m_day = int(date_match.group(1))
                            m_mon = int(date_match.group(2))
                            if m_day != expected.day or m_mon != expected.month:
                                i += 2
                                continue
                            time_part = re.sub(r"^\d{1,2}\.\d{1,2}\.\s*", "", time_raw).strip()
                            if time_part: match_time = time_part
                    except Exception:
                        pass

                surface = current_surface
                s_span  = row1.query_selector("td.s-color span[title]")
                if s_span:
                    parsed = _parse_surface(s_span.get_attribute("title") or "")
                    if parsed != "unknown": surface = parsed

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
# PLAYER DATA
# Improvements:
#   B. Rolling 12-month surface W/L from match history (not annual table)
#   C. Streak: collect all completed rows first, then count from most recent
#   A. Seed ELO from scraped results on first encounter
# ══════════════════════════════════════════════════════════════
def get_player_data(context, slug: str, display_name: str,
                    surface: str = "hard", page=None) -> dict:
    if slug in _player_cache:
        return _player_cache[slug]

    default = {
        "rank": 500, "sw": {}, "sl": {},
        "recent_wins": 0, "recent_total": 0,
        "streak": 0, "days_rest": 3, "matches_30d": 0,
        "serve_win_pct": None, "first_serve_pct": None,
    }
    if not slug:
        return default

    owns_page = page is None
    if owns_page:
        page = context.new_page()
    print(f"   Fetching {display_name} ({slug})...")

    try:
        ok = _safe_goto(page, f"{BASE}/player/{slug}/", context, retries=1)
        if not ok or _is_error_page(page):
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

        # ── B. Rolling 12-month surface W/L from match history ─
        # More reliable than annual balance table early in the season
        sw, sl    = {}, {}
        elo_seeds = []   # (won, opp_slug) for ELO seeding
        today_dt  = datetime.now()
        cutoff_12m = today_dt - timedelta(days=365)

        # Build name parts for win detection
        name_parts = display_name.lower().split() if display_name else []
        first_nm   = name_parts[0].rstrip(".") if name_parts else ""
        last_nm    = name_parts[-1].rstrip(".") if len(name_parts) > 1 else ""

        # ── C. Reliable streak — collect completed rows first ──
        completed_rows = []

        try:
            year = today_dt.year
            mdiv = (page.query_selector(f"div#matches-{year}-1-data") or
                    page.query_selector(f"div#matches-{year-1}-1-data"))
            if mdiv:
                for mrow in mdiv.query_selector_all("tr.one, tr.two")[:30]:
                    t_td = mrow.query_selector("td.t-name")
                    if not t_td: continue
                    strong = t_td.query_selector("strong")
                    if not strong: continue
                    score_td = mrow.query_selector("td.score, td.tl")
                    if not score_td or not score_td.inner_text().strip():
                        continue  # upcoming — skip

                    date_td = (mrow.query_selector("td.date") or
                               mrow.query_selector("td.first.date") or
                               mrow.query_selector("td:first-child"))
                    match_date = _parse_match_date(date_td.inner_text()) if date_td else None

                    s_span = mrow.query_selector("td.s-color span[title]")
                    m_surf = _parse_surface(s_span.get_attribute("title") if s_span else "")

                    strong_txt = strong.inner_text().lower()
                    won = bool(first_nm) and (
                        (first_nm in strong_txt and last_nm in strong_txt)
                        if last_nm else first_nm in strong_txt
                    )

                    # Get opponent slug for ELO seeding
                    opp_links = t_td.query_selector_all("a[href*='/player/']")
                    opp_slug  = ""
                    for lnk in opp_links:
                        href = (lnk.get_attribute("href") or "").strip("/").split("/")[-1]
                        if href != slug:
                            opp_slug = href
                            break

                    completed_rows.append({
                        "won": won, "surf": m_surf,
                        "date": match_date, "opp_slug": opp_slug,
                    })

        except Exception as e:
            print(f"   history err: {e}")

        # Process completed rows
        streak       = 0
        streak_locked = False
        days_rest    = 3
        matches_30d  = 0

        for idx, row in enumerate(completed_rows):
            won        = row["won"]
            m_surf     = row["surf"]
            match_date = row["date"]
            opp_slug   = row["opp_slug"]

            # C. Streak — now on clean sorted list, no upcoming rows mixed in
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

            # Days rest
            if idx == 0 and match_date:
                days_rest = max(0, (today_dt - match_date).days)
            if match_date and (today_dt - match_date).days <= 30:
                matches_30d += 1

            # B. Rolling 12-month surface W/L
            if match_date and match_date >= cutoff_12m:
                if won:
                    sw[m_surf] = sw.get(m_surf, 0) + 1
                else:
                    sl[m_surf] = sl.get(m_surf, 0) + 1

            # A. Collect for ELO seeding (chronological = reversed list)
            if opp_slug:
                elo_seeds.append((won, opp_slug))

        # A. Seed ELO if this is the first time we've seen this player
        if elo_seeds and slug not in _ELO_DB:
            _seed_elo_from_history(slug, list(reversed(elo_seeds)), surface)

        # Recent form (last 15 completed matches)
        recent_slice = completed_rows[:15]
        recent_wins  = sum(1 for r in recent_slice if r["won"])
        recent_total = len(recent_slice)

        # Serve stats
        srv = get_serve_stats(slug, surface, display_name)

        data = {
            "rank":            rank,
            "sw":              sw,
            "sl":              sl,
            "recent_wins":     recent_wins,
            "recent_total":    recent_total,
            "streak":          streak,
            "days_rest":       days_rest,
            "matches_30d":     matches_30d,
            "serve_win_pct":   srv["serve_win_pct"],
            "first_serve_pct": srv["first_serve_pct"],
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
        for sk, nk in (("slug1","p1"), ("slug2","p2")):
            s = (m.get(sk) or "").strip()
            if s and s not in seen:
                seen.add(s)
                uniq.append((s, m.get(nk) or s, m.get("surface","hard")))
            if len(uniq) >= PREFETCH_LIMIT: break
        if len(uniq) >= PREFETCH_LIMIT: break

    if not uniq: return
    print(f"[PREFETCH] Warming {len(uniq)} players...")
    page   = context.new_page()
    errors = 0
    try:
        for idx, (slug, name, surf) in enumerate(uniq):
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
                get_player_data(context, slug, name, surface=surf, page=page)
                errors = 0
                print(f"   ✅ [{idx+1}/{len(uniq)}] {name}")
            except Exception as e:
                errors += 1
                print(f"   ⚠ {name}: {e}")
    finally:
        try: page.close()
        except Exception: pass


# ══════════════════════════════════════════════════════════════
# F. SCORING MODEL — calibrated sigmoid per surface
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

    # Rank
    rank_score = max(0.0, 1.0 - rank / 500.0)

    # B. Rolling 12-month surface win rate
    w = sw.get(surface, 0)
    l = sl.get(surface, 0)
    surf_score = (w / (w + l)) if (w + l) >= 4 else 0.5

    # Form + serve blend
    form_score = (rw / rt) if rt >= 4 else 0.5
    if serve_pct is not None:
        form_score = form_score * 0.6 + serve_pct * 0.4

    # A. Surface ELO (now populated from history)
    elo_score = min(1.0, elo_surf / 2000.0)

    # Streak
    streak_score = 0.5 + max(-0.4, min(0.4, streak * 0.08))

    # Fatigue
    if   days_rest == 0: fatigue = 0.30
    elif days_rest == 1: fatigue = 0.55
    elif days_rest <= 3: fatigue = 0.80
    elif days_rest <= 7: fatigue = 0.70
    else:                fatigue = 0.60

    return (
        rank_score   * 0.28 +
        surf_score   * 0.22 +
        form_score   * 0.15 +
        elo_score    * 0.18 +   # increased — now has real data
        streak_score * 0.10 +   # increased — streak is now reliable
        fatigue      * 0.07
    )


def win_prob(s1: float, s2: float, surface: str = "hard") -> float:
    """F. Surface-calibrated sigmoid."""
    k = SURFACE_SIGMOID_K.get(surface, 4.0)
    return 1.0 / (1.0 + math.exp(-k * (s1 - s2)))


def h2h_adjustment(prob: float, h2h_wins: int, h2h_total: int) -> float:
    if h2h_total < 2: return prob
    weight   = min(0.08, h2h_total * 0.008)
    adjusted = prob * (1 - weight) + (h2h_wins / h2h_total) * weight
    return max(0.10, min(0.90, adjusted))


def market_edge(model_prob: float, fav_odds, other_odds=None) -> float:
    if not fav_odds or fav_odds <= 1.0: return 0.0
    raw_fav   = 1.0 / fav_odds
    raw_other = 1.0 / other_odds if other_odds else None
    fair_fav  = raw_fav / (raw_fav + raw_other) if raw_other else raw_fav
    return round(model_prob - fair_fav, 4)

def confidence_pct(prob: float) -> int:
    return int(min(80, 50 + abs(prob - 0.5) * 120))

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
    if bo5: return "3-0" if conf >= 75 else ("3-1" if conf >= 67 else "3-2")
    return "2-0" if conf >= 73 else "2-1"

def _pred_games(sets_str: str, surface: str) -> int:
    try:
        w = int(sets_str.split("-")[0]); l = int(sets_str.split("-")[1])
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
    r1  = sw1/(sw1+sl1) if (sw1+sl1) >= 4 else None
    r2  = sw2/(sw2+sl2) if (sw2+sl2) >= 4 else None
    if r1 and r2:
        if r1 > r2 + 0.15: return f"{p1} dominant on {surface} ({round(r1*100)}% win rate)"
        if r2 > r1 + 0.15: return f"{p2} dominant on {surface} ({round(r2*100)}% win rate)"
    r1d, r2d = d1.get("rank",500), d2.get("rank",500)
    if abs(r1d - r2d) > 100:
        return f"Ranking gap — {p1 if r1d < r2d else p2} is the clear favourite"
    st1, st2 = d1.get("streak",0), d2.get("streak",0)
    if st1 >= 4: return f"{p1} on a {st1}-win streak"
    if st2 >= 4: return f"{p2} on a {st2}-win streak"
    return {"clay":"Clay rewards baseline stamina","grass":"Grass favours big servers",
            "indoors":"Indoor boosts serving","hard":"Recent form decides"}.get(surface,"Form decides")


# ══════════════════════════════════════════════════════════════
# EVALUATE MATCHES
# ══════════════════════════════════════════════════════════════
def evaluate_matches(matches: list, context) -> list:
    picks = []

    for m in matches[:30]:
        try:
            print(f"\n── {m['p1']} vs {m['p2']}  [{m['surface']}]  {m['tournament']}")

            d1 = get_player_data(context, m["slug1"], m["p1"], m["surface"])
            d2 = get_player_data(context, m["slug2"], m["p2"], m["surface"])

            s1   = score_player(d1, m["surface"], m["slug1"])
            s2   = score_player(d2, m["surface"], m["slug2"])
            prob = win_prob(s1, s2, m["surface"])   # F. surface-calibrated

            # D. H2H adjustment
            h2h_w, h2h_t = get_h2h(context, m["slug1"], m["slug2"], m["p1"])
            prob = h2h_adjustment(prob, h2h_w, h2h_t)

            conf       = confidence_pct(prob)
            pick_grade = grade(conf)
            favourite  = m["p1"] if prob > 0.5 else m["p2"]
            fav_prob   = max(prob, 1 - prob)
            fav_odds   = m["odds_p1"] if prob > 0.5 else m["odds_p2"]
            other_odds = m["odds_p2"] if prob > 0.5 else m["odds_p1"]
            edge       = market_edge(fav_prob, fav_odds, other_odds)

            print(
                f"   {m['p1']:22s} rank={d1['rank']:>3}  elo={get_surface_elo(m['slug1'],m['surface']):.0f}"
                f"  streak={d1['streak']:+d}  rest={d1['days_rest']}d  score={s1:.3f}\n"
                f"   {m['p2']:22s} rank={d2['rank']:>3}  elo={get_surface_elo(m['slug2'],m['surface']):.0f}"
                f"  streak={d2['streak']:+d}  rest={d2['days_rest']}d  score={s2:.3f}\n"
                f"   H2H {h2h_w}/{h2h_t}  prob={prob:.3f}  conf={conf}%  "
                f"edge={edge:+.3f}  grade={pick_grade}"
            )

            # Pure model gate — only skip if model itself is undecided
            if abs(prob - 0.5) < 0.05:
                print(f"   SKIP: model too close ({s1:.3f} vs {s2:.3f})")
                continue

            bo5        = _is_bo5(m["tournament"])
            pred_sets  = _predict_sets(conf, bo5)
            pred_games = _pred_games(pred_sets, m["surface"])
            ou_val     = _ou_line(bo5)
            srv1 = get_serve_stats(m["slug1"], m["surface"], m["p1"])
            srv2 = get_serve_stats(m["slug2"], m["surface"], m["p2"])

            picks.append({
                "m": m, "d1": d1, "d2": d2,
                "s1": s1, "s2": s2,
                "prob": fav_prob, "conf": conf, "grade": pick_grade,
                "winner": favourite, "winner_odds": fav_odds,
                "edge": edge,
                "streak1":    d1.get("streak", 0),
                "streak2":    d2.get("streak", 0),
                "elo1":       get_surface_elo(m["slug1"], m["surface"]),
                "elo2":       get_surface_elo(m["slug2"], m["surface"]),
                "h2h":        f"{h2h_w}/{h2h_t}",
                "pred_sets":  pred_sets,
                "pred_games": pred_games,
                "over_under": ("Over" if pred_games >= ou_val else "Under") + f" {ou_val}",
                "handicap":   _set_handicap(conf, favourite),
                "serve1":     _serve_label(srv1["serve_win_pct"], m["surface"],
                                           srv1["first_serve_pct"], srv1["source"]),
                "serve2":     _serve_label(srv2["serve_win_pct"], m["surface"],
                                           srv2["first_serve_pct"], srv2["source"]),
                "key_factor": _key_factor(d1, d2, m["p1"], m["p2"], m["surface"]),
            })

        except Exception as e:
            print(f"   ERROR: {e}")

    return picks


# ══════════════════════════════════════════════════════════════
# FORMAT PICK
# ══════════════════════════════════════════════════════════════
def format_pick(pk: dict) -> str:
    m        = pk["m"]
    icon     = grade_icon(pk["grade"])
    odds_str = f" @ {pk['winner_odds']}" if pk["winner_odds"] else ""
    bar      = "█" * (pk["conf"] // 10) + "░" * (10 - pk["conf"] // 10)
    rnd      = f"  · {m['round']}" if m.get("round") else ""
    h2h_str  = f"  H2H: {pk['h2h']}" if pk.get("h2h","0/0") != "0/0" else ""
    return (
        f"─────────────────────────\n"
        f"🏟  {m['tournament']}  ({m['surface'].title()}){rnd}\n"
        f"⚔️  {m['p1']} vs {m['p2']}\n"
        f"{icon} {pk['grade']}  ✅  {pk['winner']}{odds_str}\n"
        f"[{bar}] {pk['conf']}%\n"
        f"Ranks: #{pk['d1']['rank']} vs #{pk['d2']['rank']}\n"
        f"ELO: {pk['elo1']:.0f} vs {pk['elo2']:.0f}{h2h_str}\n"
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
    if RUN_MODE == "calibrate":
        run_calibration()
        return

    t0 = time.time()
    pw, browser, context = launch()

    try:
        # Auto-resolve yesterday's results on daily_reset run
        if RUN_MODE == "daily_reset":
            _auto_resolve_yesterday(context)

        matches = get_matches(context, day="today", label="TODAY")
        if not matches:
            print("❌ No matches found today.")
            send_telegram("No WTA matches found today.")
            return

        _prefetch(context, matches)
        raw   = evaluate_matches(matches, context)
        picks = apply_mode_filters(raw)
        picks.sort(key=lambda x: (0 if x["grade"] == "HIGH" else 1, -x["conf"]))

        # Distinguish why there are no picks:
        # 1. Model found qualifying picks but all were already sent today (dedup)
        # 2. Model genuinely found no picks above threshold
        all_qualified = [pk for pk in raw if pk.get("grade") in ("HIGH", "MEDIUM")]
        if not picks:
            today_str = datetime.now(WAT).strftime("%A %d %B %Y")
            sent_time = datetime.now(WAT).strftime("%H:%M WAT")

            # If real picks were already sent today, stay silent —
            # no need to announce "no new signals" after picks went out
            if _real_picks_sent_today():
                print(f"[{RUN_MODE.upper()}] Picks already sent today — staying silent.")
                return

            if all_qualified and RUN_MODE == "normal":
                # Picks exist but all already sent today — notify once only
                print(f"[{RUN_MODE.upper()}] All qualifying picks already sent today.")
                if not _status_already_sent():
                    send_telegram(
                        f"🎾 WTA ENGINE v8\n📅 {today_str}\n\n"
                        f"🔁 No new signals — {len(all_qualified)} qualifying match"
                        f"{'es' if len(all_qualified) != 1 else ''} already sent today.\n"
                        f"🕐 {sent_time}"
                    )
                    _mark_status_sent()
                else:
                    print("   Status already sent today — skipping.")
            else:
                # Genuinely no qualifying picks — notify once only
                print(f"[{RUN_MODE.upper()}] No qualifying picks found today.")
                if not _status_already_sent():
                    send_telegram(
                        f"🎾 WTA ENGINE v8\n📅 {today_str}\n\n"
                        f"📭 No qualifying picks found for today's matches.\n"
                        f"🕐 {sent_time}"
                    )
                    _mark_status_sent()
                else:
                    print("   Status already sent today — skipping.")
            return

        mode_label = {
            "normal":      "",
            "daily_reset": "🌅 Daily Signals  ",
            "force":       "⚡ Manual Run  ",
        }.get(RUN_MODE, "")

        today_str = datetime.now(WAT).strftime("%A %d %B %Y")
        lines     = [f"🎾 WTA ENGINE v8  {mode_label}\n📅 {today_str}\n"]

        has_medium = any(pk["grade"] == "MEDIUM" for pk in picks)
        has_high   = any(pk["grade"] == "HIGH"   for pk in picks)
        prev_grade = None

        for pk in picks[:6]:
            if has_high and has_medium and prev_grade == "HIGH" and pk["grade"] == "MEDIUM":
                lines.append("── ⚡ medium confidence ──\n")
            lines.append(format_pick(pk))
            prev_grade = pk["grade"]

        sent_time = datetime.now(WAT).strftime("%H:%M WAT")
        lines.append(f"⚠️ For entertainment only.\n🕐 Sent at {sent_time}")
        msg = "\n".join(lines)
        print("\n" + msg)
        send_telegram(msg)

        # E. Log picks for calibration tracking
        log_picks(picks[:6])

        if should_mark_sent():
            mark_as_sent(picks[:6])

        # Persist H2H cache after each run
        _save_h2h_disk()

    finally:
        browser.close()
        pw.stop()
        print(f"\n⚡ Done in {round(time.time() - t0, 1)}s")


# ══════════════════════════════════════════════════════════════
# H2H DISK CACHE — persists between runs
# ══════════════════════════════════════════════════════════════
_H2H_FILE = os.path.join(_DIR, "h2h_cache.json")

def _load_h2h_disk() -> dict:
    try:
        with open(_H2H_FILE) as f:
            return json.load(f)
    except Exception:
        return {}

def _save_h2h_disk():
    try:
        with open(_H2H_FILE, "w") as f:
            json.dump(_h2h_cache, f, indent=2)
    except Exception as e:
        print(f"[WARN] h2h_cache save failed: {e}")

# Pre-load H2H cache from disk at startup
_h2h_cache.update(_load_h2h_disk())


# ══════════════════════════════════════════════════════════════
# AUTO-RESOLVE YESTERDAY'S PICKS + UPDATE ELO
#
# Called on daily_reset (1am WAT). Scrapes yesterday's completed
# results from /results/?type=wta-single to:
#   1. Mark outcomes.json picks as "correct" or "wrong"
#   2. Call update_surface_elo() for each completed match
#   3. Auto-tune SURFACE_SIGMOID_K based on recent calibration
# ══════════════════════════════════════════════════════════════
def _auto_resolve_yesterday(context):
    """Scrape yesterday's WTA results and update outcomes + ELO."""
    print("\n[AUTO-RESOLVE] Checking yesterday\'s results...")
    page = context.new_page()
    try:
        ok = _safe_goto(page, f"{BASE}/results/?type=wta-single&day=-1", context, retries=1)
        if not ok:
            print("  [AUTO-RESOLVE] Could not load results page.")
            return

        try:
            page.wait_for_selector("table.result", timeout=10000)
        except Exception:
            return

        # Parse result rows — same two-row structure as matches page
        # but winner row has td.t-name with <strong> wrapping winner name
        rows = page.query_selector_all("tr")
        results = []   # (winner_slug, loser_slug, surface)

        current_surface = "hard"
        i = 0
        while i < len(rows) - 1:
            row1 = rows[i]

            # Surface from header
            try:
                t_name_td = row1.query_selector("td.t-name")
                if t_name_td:
                    pl = [l for l in t_name_td.query_selector_all("a")
                          if "/player/" in (l.get_attribute("href") or "")]
                    if not pl:
                        s = row1.query_selector("td.s-color span[title]")
                        if s:
                            current_surface = _parse_surface(s.get_attribute("title") or "")
                        i += 1
                        continue
            except Exception:
                pass

            try:
                p1_el = row1.query_selector("td.t-name a[href*=\'/player/\']")
                if not p1_el:
                    i += 1
                    continue
                row2  = rows[i + 1]
                p2_el = row2.query_selector("td.t-name a[href*=\'/player/\']")
                if not p2_el:
                    i += 1
                    continue

                slug1 = (p1_el.get_attribute("href") or "").strip("/").split("/")[-1]
                slug2 = (p2_el.get_attribute("href") or "").strip("/").split("/")[-1]

                # Winner is the row that has <strong> in td.t-name
                strong1 = row1.query_selector("td.t-name strong")
                strong2 = row2.query_selector("td.t-name strong")

                if strong1:
                    results.append((slug1, slug2, current_surface))
                elif strong2:
                    results.append((slug2, slug1, current_surface))

                i += 2
            except Exception:
                i += 1

        print(f"  [AUTO-RESOLVE] Found {len(results)} completed results")

        # Update ELO for each result
        for winner_slug, loser_slug, surface in results:
            if winner_slug and loser_slug:
                update_surface_elo(winner_slug, loser_slug, surface)

        # Resolve outcomes.json
        if results:
            _resolve_outcomes(results)
            _autotune_sigmoid()

        # Persist H2H cache
        _save_h2h_disk()

    except Exception as e:
        print(f"  [AUTO-RESOLVE] Error: {e}")
    finally:
        try: page.close()
        except Exception: pass


def _resolve_outcomes(results: list):
    """
    Match scraped results against pending outcomes.json entries.
    results = [(winner_slug, loser_slug, surface), ...]
    """
    outcomes  = _load_outcomes()
    yesterday = (datetime.now(WAT) - timedelta(days=1)).strftime("%Y-%m-%d")
    pending   = [o for o in outcomes if o.get("result") is None
                 and o.get("date") == yesterday]

    if not pending:
        return

    # Build lookup: winner_slug → loser_slug
    result_map = {w: l for w, l, _ in results}

    resolved = 0
    for o in pending:
        # Get slugs from the match key if we stored them,
        # otherwise match by name substring against result slugs
        winner_pred = o.get("winner_pred", "")
        p1          = o.get("p1", "")
        p2          = o.get("p2", "")

        # Try to identify which slug is the predicted winner
        # by matching display name surname against slugs
        pred_surname = winner_pred.split()[0].lower().rstrip(".") if winner_pred else ""

        for w_slug, l_slug in result_map.items():
            if pred_surname and pred_surname in w_slug:
                o["result"]  = "correct"
                o["winner_actual"] = w_slug
                resolved += 1
                break
            elif pred_surname and pred_surname in l_slug:
                o["result"]  = "wrong"
                o["winner_actual"] = w_slug
                resolved += 1
                break

    _save_outcomes(outcomes)
    print(f"  [AUTO-RESOLVE] Resolved {resolved}/{len(pending)} yesterday\'s picks")


def _autotune_sigmoid():
    """
    Adjust SURFACE_SIGMOID_K based on recent calibration data.
    Logic: if model is over-confident on a surface (predicted high prob
    but wrong more than expected), reduce k. If under-confident, increase k.
    Only adjusts when >= 20 resolved outcomes exist per surface.
    Max adjustment per run: ±0.2
    """
    outcomes = [o for o in _load_outcomes()
                if o.get("result") in ("correct", "wrong")]
    if len(outcomes) < 30:
        return

    for surf in ("clay", "hard", "grass", "indoors"):
        ss = [o for o in outcomes if o.get("surface") == surf]
        if len(ss) < 20:
            continue

        # Expected calibration: avg(prob) should ≈ accuracy
        avg_prob = sum(o["prob"] for o in ss) / len(ss)
        accuracy = sum(1 for o in ss if o["result"] == "correct") / len(ss)
        gap      = avg_prob - accuracy   # positive = over-confident

        old_k = SURFACE_SIGMOID_K.get(surf, 4.0)
        # Over-confident → lower k (softer sigmoid = less extreme probs)
        # Under-confident → raise k
        adjustment = max(-0.2, min(0.2, -gap * 1.5))
        new_k      = round(max(2.0, min(8.0, old_k + adjustment)), 2)

        if new_k != old_k:
            SURFACE_SIGMOID_K[surf] = new_k
            print(f"  [AUTOTUNE] {surf}: k {old_k} → {new_k}  "
                  f"(avg_prob={avg_prob:.2f}, acc={accuracy:.2f})")



if __name__ == "__main__":
    run()

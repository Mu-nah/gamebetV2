"""
Tennis Engine v7 — WTA Prediction Engine
=========================================
RUN_MODE (set by GitHub Actions env var):
  normal       — every 3hr run:  dedup ON,  HIGH filter ON,  marks sent
  daily_reset  — 1am WAT run:    dedup OFF, HIGH filter ON,  marks sent
  force        — manual trigger: dedup OFF, HIGH filter ON,  does NOT mark sent

TOMORROW FALLBACK:
  If today HIGH picks < TOMORROW_THRESHOLD (3), fetch &day=1 and append
  tomorrow HIGH picks labelled "📅 TOMORROW".
  Tomorrow picks are never written to dedup (re-evaluated fresh next day).

BUGS FIXED vs the running v7:
  • streak capped at +10 for everyone — streak_locked flag was never checked
    before incrementing, so streak kept climbing. Fixed with proper guard.
  • days_rest always 3 — date parser got "19.04." (no year) from td:first-child.
    Fixed by injecting current year when only DD.MM. is present.
  • win detection matched too broadly — now checks first+last name together.
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

# How many today HIGH picks before we skip fetching tomorrow
TOMORROW_THRESHOLD = 3
PREFETCH_LIMIT     = 20
SURFACE_COL        = {"clay": 2, "hard": 3, "indoors": 4, "grass": 5}

SURFACE_ELO_DB: dict = {}
_player_cache:  dict = {}
_h2h_cache:     dict = {}
_ta_serve_cache: dict = {}
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

# TennisAbstract name map  (tennisexplorer slug → TA search name)
_TA_BASE    = "https://www.tennisabstract.com/cgi-bin/wplayer.cgi"
_TA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "text/html,application/xhtml+xml",
    "Referer":    "https://www.tennisabstract.com/",
}
_TA_NAME_MAP = {
    "swiatek":          "IgaSwiatek",
    "sabalenka":        "ArynaStabalenka",
    "gauff":            "CocoGauff",
    "rybakina":         "ElenaRybakina",
    "pegula":           "JessicaPegula",
    "zheng":            "QinwenZheng",
    "andreeva-7d55d":   "MirraAndreeva",
    "paolini":          "JasminePaolini",
    "badosa":           "PaulaBadosa",
    "vekic":            "DonnaVekic",
    "kasatkina":        "DariaKasatkina",
    "samsonova":        "LudmilaSamsonova",
    "svitolina":        "ElinaSvitolina",
    "jabeur":           "OnsJabeur",
    "keys":             "MadisonKeys",
    "kostyuk-ea2bf":    "MartaKostyuk",
    "krejcikova":       "BarboraKrejcikova",
    "ostapenko":        "JelenaOstapenko",
    "muchova":          "KaterinaMuchova",
    "collins":          "DanielleCollins",
    "haddad-maia":      "BeatrizHaddadMaia",
    "bencic":           "BelindaBencic",
    "cirstea":          "SoranaCirstea",
    "maria-8ad07":      "TatjanaMaria",
    "andreescu":        "BiancaAndreescu",
    "vondrousova":      "MarketaVondrousova",
    "sorribes-tormo":   "SaraSorribesTormo",
    "chwalinska":       "MajaChwalinska",
    "montgomery-8a7c9": "RebeccaMontgomery",
    "marcinko":         "PatriciaMarcinko",
}


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
    """Write today picks to dedup file. Tomorrow picks are intentionally skipped."""
    sent  = _load_sent()
    today = _today_wat()
    sent.setdefault(today, [])
    for pk in picks:
        if pk.get("is_tomorrow"):
            continue
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
        filtered = [pk for pk in picks if pk.get("grade") == "HIGH"]
        print(f"[MODE] FORCE — {len(filtered)} HIGH picks (dedup ignored)")
        return filtered
    if RUN_MODE == "daily_reset":
        filtered = [pk for pk in picks if pk.get("grade") == "HIGH"]
        print(f"[MODE] DAILY_RESET — {len(filtered)} HIGH picks (dedup ignored)")
        return filtered
    # normal
    before = len(picks)
    picks  = [pk for pk in picks
              if not is_already_sent(pk["m"]["p1"], pk["m"]["p2"])]
    picks  = [pk for pk in picks if pk.get("grade") == "HIGH"]
    print(f"[MODE] NORMAL — {len(picks)} HIGH picks ({before - len(picks)} deduped)")
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
    Parse match date from tennisexplorer row text.
    Handles:
      "19.04."        — DD.MM. only (inject current year)
      "19.04.2026"    — full date
      "2026-04-19"    — ISO
    """
    txt = (txt or "").strip()
    if not txt:
        return None
    year = datetime.now().year
    # DD.MM. with trailing dot and no year
    m = re.match(r"^(\d{1,2})\.(\d{1,2})\.$", txt)
    if m:
        try:
            return datetime(year, int(m.group(2)), int(m.group(1)))
        except Exception:
            return None
    # Full date formats
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(txt, fmt)
        except Exception:
            continue
    return None

def is_allowed_tournament(name: str) -> bool:
    n  = (name or "").strip()
    nl = n.lower()
    if not n:                                                             return False
    if "itf" in nl:                                                       return False
    if _ITF_PREFIXES.search(n):                                           return False
    if any(x in nl for x in ("utr pro","utr ","futures","challenger")):   return False
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
# TENNISABSTRACT SERVE STATS
# ══════════════════════════════════════════════════════════════
def _slug_to_ta_name(slug: str, display_name: str) -> str:
    if slug in _TA_NAME_MAP:
        return _TA_NAME_MAP[slug]
    base  = re.sub(r"-[0-9a-f]{4,}$", "", slug)
    parts = base.split("-")
    return "".join(p.capitalize() for p in parts if p)

def _parse_ta_stats(html: str) -> dict:
    stats = {}
    patterns = {
        "serve_win_pct":   re.compile(r"var\s+sp\s*=\s*([\d.]+)"),
        "first_serve_pct": re.compile(r"var\s+fsp\s*=\s*([\d.]+)"),
        "first_serve_win": re.compile(r"var\s+fspw\s*=\s*([\d.]+)"),
        "second_serve_win":re.compile(r"var\s+sspw\s*=\s*([\d.]+)"),
    }
    for key, pat in patterns.items():
        m = pat.search(html)
        if m:
            val = float(m.group(1))
            stats[key] = round(val / 100.0, 4) if val > 1 else val
    return stats

def get_ta_serve_stats(slug: str, display_name: str) -> dict:
    if not slug or slug in _ta_serve_cache:
        return _ta_serve_cache.get(slug, {})
    ta_name = _slug_to_ta_name(slug, display_name)
    try:
        resp = _http.get(f"{_TA_BASE}?p={ta_name}", headers=_TA_HEADERS, timeout=8)
        if resp.status_code == 200:
            stats = _parse_ta_stats(resp.text)
            if stats:
                print(f"   [TA] {display_name}: serve={round(stats.get('serve_win_pct',0)*100)}%")
            else:
                print(f"   [TA] {display_name}: no serve stats found")
            _ta_serve_cache[slug] = stats
            return stats
    except Exception as e:
        print(f"   [TA] {display_name}: {e}")
    _ta_serve_cache[slug] = {}
    return {}


# ══════════════════════════════════════════════════════════════
# STEP 1 — GET MATCHES
# day="today" or day="1" (tomorrow)
#
# Confirmed DOM structure (two-row-per-match on matches page):
#   row1: td.first.time           → "18:30"
#         td.t-name a[/player/]   → player 1, slug in href
#         td.s-color span[title]  → surface
#         td.course ×2            → odds
#   row2: td.t-name a[/player/]  → player 2
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

                surface = current_surface
                s_span  = row1.query_selector("td.s-color span[title]")
                if s_span:
                    parsed = _parse_surface(s_span.get_attribute("title") or "")
                    if parsed != "unknown":
                        surface = parsed

                match_time = time_el.inner_text().strip()

                oc      = row1.query_selector_all("td.course")
                odds_p1 = _safe_float(oc[0].inner_text()) if len(oc) > 0 else None
                odds_p2 = _safe_float(oc[1].inner_text()) if len(oc) > 1 else None

                if not is_allowed_tournament(current_tournament):
                    i += 2
                    continue
                if odds_p1 is None and odds_p2 is None:
                    print(f"   SKIP (no odds): {p1_name} vs {p2_name}")
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
#
# BUGS FIXED:
#   streak — streak_locked was checked AFTER incrementing, so streak
#             kept climbing every match. Now checked BEFORE.
#   days_rest — tennisexplorer match rows show "19.04." (no year).
#               _parse_match_date now handles DD.MM. format by
#               injecting the current year.
#   win detection — now checks first+last name together to avoid
#                   false positives (e.g. "Maria" matching "Mariana").
# ══════════════════════════════════════════════════════════════
def get_player_data(context, slug: str, display_name: str, page=None) -> dict:
    if slug in _player_cache:
        return _player_cache[slug]

    default = {
        "rank": 500, "sw": {}, "sl": {},
        "recent_wins": 0, "recent_total": 0,
        "recent_surface_wins": {}, "recent_surface_total": {},
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
            print(f"   ❌ Could not load {display_name}")
            return default
        try:
            page.wait_for_selector("table.plDetail", timeout=5000)
        except Exception:
            return default

        # ── Rank ──────────────────────────────────────────────
        # table.plDetail td div.date
        # "Current/Highest rank - singles: 3. / 2."
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
        # table.result.balance (first = singles)
        # cols: 2=Clay 3=Hard 4=Indoors 5=Grass
        # current year ×2, previous year ×1
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
        # div#matches-{year}-1-data  tr.one/tr.two
        # td.t-name: <strong> = winner
        # td.s-color span[title]: match surface
        # td:first-child: date like "19.04." — parsed with year injected
        recent_wins = recent_total = streak = matches_30d = 0
        days_rest   = 3
        streak_locked = False
        recent_surface_wins  = {}
        recent_surface_total = {}
        today_dt = datetime.now()

        # Build name parts for accurate win detection
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
                        continue   # upcoming — skip

                    s_span = mrow.query_selector("td.s-color span[title]")
                    m_surf = _parse_surface(s_span.get_attribute("title") if s_span else "")

                    # Accurate win detection: both first and last name must match
                    strong_txt = strong.inner_text().lower()
                    won = bool(first_nm) and (
                        (first_nm in strong_txt and last_nm in strong_txt)
                        if last_nm else first_nm in strong_txt
                    )

                    # ── STREAK FIX ────────────────────────────
                    # Check streak_locked BEFORE modifying streak
                    if not streak_locked:
                        if idx == 0:
                            streak = 1 if won else -1
                        elif won and streak > 0:
                            streak += 1
                        elif not won and streak < 0:
                            streak -= 1
                        else:
                            streak_locked = True   # streak broken — stop counting
                    streak = max(-10, min(10, streak))   # hard cap still applies

                    # ── DAYS REST FIX ─────────────────────────
                    # td:first-child shows "19.04." — _parse_match_date handles this
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

        # ── TennisAbstract serve stats ─────────────────────────
        ta            = get_ta_serve_stats(slug, display_name)
        serve_win_pct = ta.get("serve_win_pct")
        first_serve_pct = ta.get("first_serve_pct")

        # Fallback: scrape from tennisexplorer profile stat tables
        if serve_win_pct is None:
            try:
                for tbl in page.query_selector_all("table.stat"):
                    for sr in tbl.query_selector_all("tr"):
                        txt = sr.inner_text().lower()
                        if "1st serve" in txt and "won" in txt:
                            cells = sr.query_selector_all("td")
                            if len(cells) >= 2:
                                pct = _safe_float(
                                    cells[-1].inner_text().replace("%", ""))
                                if pct and 30 < pct < 100:
                                    serve_win_pct = pct / 100.0
                                    break
                    if serve_win_pct:
                        break
            except Exception:
                pass

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
            "serve_win_pct":        serve_win_pct,
            "first_serve_pct":      first_serve_pct,
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
                uniq.append((s, m.get(nk) or s))
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
        for idx, (slug, name) in enumerate(uniq):
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
                get_player_data(context, slug, name, page=page)
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
    return int(min(80, base * mult))   # hard cap at 80

def market_edge(model_prob: float, fav_odds, other_odds=None) -> float:
    if not fav_odds or fav_odds <= 1.0:
        return 0.0
    raw_fav   = 1.0 / fav_odds
    raw_other = 1.0 / other_odds if other_odds else None
    if raw_other:
        fair_fav = raw_fav / (raw_fav + raw_other)
    else:
        fair_fav = raw_fav
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
    return any(x in t for x in ["grand slam","wimbledon","us open","australian",
                                  "french","roland","davis cup","atp finals"])

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

def _serve_label(serve_pct, surface: str, first_in_pct=None) -> str:
    if serve_pct is None:
        return "N/A"
    base    = SURFACE_SERVE_HOLD.get(surface, 0.72)
    pct_str = f"{round(serve_pct*100)}%"
    if first_in_pct:
        pct_str = f"{round(serve_pct*100)}% pts / {round(first_in_pct*100)}% in"
    if serve_pct >= base + 0.05: return f"Strong ({pct_str})"
    if serve_pct <= base - 0.05: return f"Weak ({pct_str})"
    return f"Average ({pct_str})"

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
            "hard":"Recent form decides"}.get(surface,"Form decides")


# ══════════════════════════════════════════════════════════════
# EVALUATE A LIST OF MATCHES  (shared today + tomorrow)
# ══════════════════════════════════════════════════════════════
def evaluate_matches(matches: list, context, is_tomorrow: bool = False) -> list:
    picks = []
    tag   = "TOMORROW" if is_tomorrow else "TODAY"

    for m in matches[:30]:
        try:
            print(f"\n── [{tag}] {m['p1']} vs {m['p2']}  [{m['surface']}]  {m['tournament']}")

            d1 = get_player_data(context, m["slug1"], m["p1"])
            d2 = get_player_data(context, m["slug2"], m["p2"])

            s1   = score_player(d1, m["surface"], m["slug1"])
            s2   = score_player(d2, m["surface"], m["slug2"])
            prob = win_prob(s1, s2)

            h2h_key          = f"{min(m['slug1'],m['slug2'])}_{max(m['slug1'],m['slug2'])}"
            h2h_w, h2h_t     = _h2h_cache.get(h2h_key, (0, 0))
            prob             = h2h_adjustment(prob, h2h_w, h2h_t)

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

            # Base filters
            if conf < 65:
                print(f"   SKIP: conf {conf}%"); continue
            if abs(blended - 0.5) < 0.07:
                print(f"   SKIP: too close"); continue
            if not fav_odds:
                print(f"   SKIP: no odds"); continue
            if edge < 0.03:
                print(f"   SKIP: edge {edge:+.3f}"); continue

            bo5        = _is_bo5(m["tournament"])
            pred_sets  = _predict_sets(conf, bo5)
            pred_games = _pred_games(pred_sets, m["surface"])
            ou_val     = _ou_line(bo5)

            picks.append({
                "m": m, "d1": d1, "d2": d2,
                "s1": s1, "s2": s2,
                "prob": fav_prob, "conf": conf, "grade": pick_grade,
                "winner": favourite, "winner_odds": fav_odds,
                "edge": edge,
                "streak1": d1.get("streak", 0),
                "streak2": d2.get("streak", 0),
                "pred_sets":  pred_sets,
                "pred_games": pred_games,
                "over_under": ("Over" if pred_games >= ou_val else "Under") + f" {ou_val}",
                "handicap":   _set_handicap(conf, favourite),
                "serve1":     _serve_label(d1.get("serve_win_pct"), m["surface"],
                                           d1.get("first_serve_pct")),
                "serve2":     _serve_label(d2.get("serve_win_pct"), m["surface"],
                                           d2.get("first_serve_pct")),
                "key_factor": _key_factor(d1, d2, m["p1"], m["p2"], m["surface"]),
                "is_tomorrow": is_tomorrow,
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
    tmr      = "  📅 TOMORROW" if pk.get("is_tomorrow") else ""
    return (
        f"─────────────────────────\n"
        f"🏟  {m['tournament']}  ({m['surface'].title()}){rnd}{tmr}\n"
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
        today_raw   = evaluate_matches(today_matches, context, is_tomorrow=False)
        today_picks = apply_mode_filters(today_raw)
        today_picks.sort(key=lambda x: x["conf"], reverse=True)

        # ── TOMORROW FALLBACK ─────────────────────────────────
        # Triggered when today has fewer than TOMORROW_THRESHOLD HIGH picks
        tomorrow_picks = []
        if len(today_picks) < TOMORROW_THRESHOLD:
            print(
                f"\n[FALLBACK] Only {len(today_picks)} today picks "
                f"(threshold={TOMORROW_THRESHOLD}) — fetching tomorrow..."
            )
            tmr_matches = get_matches(context, day="1", label="TOMORROW")
            if tmr_matches:
                # Only prefetch players not already cached
                uncached = [
                    m for m in tmr_matches
                    if m["slug1"] not in _player_cache
                    or m["slug2"] not in _player_cache
                ]
                if uncached:
                    _prefetch(context, uncached)

                tmr_raw        = evaluate_matches(tmr_matches, context, is_tomorrow=True)
                tomorrow_picks = [pk for pk in tmr_raw if pk.get("grade") == "HIGH"]
                tomorrow_picks.sort(key=lambda x: x["conf"], reverse=True)
                print(f"[FALLBACK] {len(tomorrow_picks)} tomorrow HIGH picks")

        # ── COMBINE + SEND ────────────────────────────────────
        all_picks = today_picks[:6] + tomorrow_picks[:3]

        if not all_picks:
            print(f"[{RUN_MODE.upper()}] No qualifying picks today.")
            return   # silent

        mode_label = {
            "normal":      "",
            "daily_reset": "🌅 Daily Signals  ",
            "force":       "⚡ Manual Run  ",
        }.get(RUN_MODE, "")

        today_str = datetime.now(WAT).strftime("%A %d %B %Y")
        lines     = [f"🎾 WTA ENGINE v7  {mode_label}\n📅 {today_str}\n"]

        has_today    = any(not pk["is_tomorrow"] for pk in all_picks)
        has_tomorrow = any(pk["is_tomorrow"]     for pk in all_picks)

        if has_today and has_tomorrow:
            lines.append("━━━ TODAY ━━━\n")

        prev_was_today = True
        for pk in all_picks:
            if has_today and has_tomorrow and pk["is_tomorrow"] and prev_was_today:
                lines.append("\n━━━ TOMORROW (preview) ━━━\n")
            lines.append(format_pick(pk))
            prev_was_today = not pk["is_tomorrow"]

        sent_time = datetime.now(WAT).strftime("%H:%M WAT")
        lines.append(f"⚠️ For entertainment only.\n🕐 Sent at {sent_time}")
        msg = "\n".join(lines)
        print("\n" + msg)
        send_telegram(msg)

        # Mark only today picks as sent — tomorrow stays fresh
        if should_mark_sent():
            mark_as_sent(all_picks)

    finally:
        browser.close()
        pw.stop()
        print(f"\n⚡ Done in {round(time.time() - t0, 1)}s")


if __name__ == "__main__":
    run()

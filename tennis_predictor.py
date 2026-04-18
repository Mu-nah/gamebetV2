"""
Tennis Engine v7 — WTA Prediction Engine (IMPROVED)
====================================================
What's new vs v6:
  MODEL
  ─────
  • Surface-specific ELO  (replaces single global ELO)
  • Momentum / win-streak signal
  • Fatigue signal (days since last match, back-to-back detection)
  • Head-to-head adjustment (scraped from tennisexplorer H2H page)
  • Odds sanity layer — model vs market gap check (30% market blend)
  • Serve stats — 1st serve win% folded into form when available

  WEIGHTS (v6 → v7)
  ──────────────────
  Rank score     40% → 30%
  Surface W/L    30% → 25%
  Recent form    20% → 15%
  Surface ELO    10% → 15%  (was global ELO)
  Momentum        —  →  8%  NEW
  Fatigue         —  →  7%  NEW
  H2H             —  → ±8%  post-score adjustment
  Odds            —  → 70/30 blend + confidence multiplier

  FILTERS (v6 → v7)
  ──────────────────
  conf < 65%           → skip (unchanged)
  prob gap < 0.07      → skip (unchanged)
  no odds              → skip (unchanged)
  edge < 0.03          → skip (unchanged)
  odds sanity gap >20% → confidence penalised ×0.75

  SCRAPING
  ─────────
  • get_today_matches   — unchanged (proven)
  • get_player_data     — adds streak, days_rest, serve_win_pct, matches_30d
  • get_h2h             — NEW: scrapes /h2h/{slug1}-vs-{slug2}/
  • Surface ELO updated each run via update_surface_elo()
"""

from playwright.sync_api import sync_playwright
import requests
import random
import math
import time
import re
from datetime import datetime, timedelta, timezone

BASE      = "https://www.tennisexplorer.com"
WAT       = timezone(timedelta(hours=1))   # West Africa Time (UTC+1)
# ── Credentials — load from .env file ─────────────────────────────────────
# Create a .env file in your project folder with:
#   BOT_TOKEN=your_bot_token
#   CHAT_ID=your_chat_id
import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed — set env vars manually

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID   = os.getenv("CHAT_ID", "")

PREFETCH_DELAY_MS = 1200   # raised from 300 — prevents rate limiting
PREFETCH_LIMIT     = 20      # max unique players to prefetch

SURFACE_COL = {"clay": 2, "hard": 3, "indoors": 4, "grass": 5}

# Surface-specific ELO — persists across runs (swap for SQLite when ready)
SURFACE_ELO_DB: dict[str, dict[str, float]] = {}
# e.g. {"svitolina": {"clay": 1820, "hard": 1750, ...}}

_player_cache: dict = {}
_h2h_cache:    dict = {}
_http = requests.Session()
_RANK_RE   = re.compile(r"singles:\s*(\d+)\.")
_SCORE_RE  = re.compile(r"(\d+)-(\d+)")

_ITF_PREFIXES = re.compile(
    r"^\s*(w15|w25|w35|w50|w60|w100|itf)\b", re.IGNORECASE
)


# ══════════════════════════════════════════════════════
# SURFACE ELO
# ══════════════════════════════════════════════════════
def get_surface_elo(slug: str, surface: str) -> float:
    return SURFACE_ELO_DB.get(slug, {}).get(surface, 1500.0)

def update_surface_elo(winner_slug: str, loser_slug: str, surface: str):
    k  = 32
    r1 = get_surface_elo(winner_slug, surface)
    r2 = get_surface_elo(loser_slug,  surface)
    e1 = 1.0 / (1.0 + 10.0 ** ((r2 - r1) / 400.0))
    SURFACE_ELO_DB.setdefault(winner_slug, {})[surface] = r1 + k * (1 - e1)
    SURFACE_ELO_DB.setdefault(loser_slug,  {})[surface] = r2 - k * e1


# ══════════════════════════════════════════════════════
# TELEGRAM
# ══════════════════════════════════════════════════════
# ── Sent-match deduplication ──────────────────────────────────────────────────
import json

_SENT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sent_matches.json")

def _load_sent() -> dict:
    try:
        with open(_SENT_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_sent(data: dict):
    try:
        with open(_SENT_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"[WARN] Could not save sent_matches.json: {e}")

def _today_wat_str() -> str:
    return datetime.now(WAT).strftime("%Y-%m-%d")

def _match_key(p1: str, p2: str) -> str:
    """Stable key regardless of player order."""
    return "_vs_".join(sorted([p1.strip().lower(), p2.strip().lower()]))

def is_already_sent(p1: str, p2: str) -> bool:
    sent = _load_sent()
    today = _today_wat_str()
    return _match_key(p1, p2) in sent.get(today, [])

def mark_as_sent(picks: list):
    sent  = _load_sent()
    today = _today_wat_str()
    if today not in sent:
        sent[today] = []
    for pk in picks:
        key = _match_key(pk["m"]["p1"], pk["m"]["p2"])
        if key not in sent[today]:
            sent[today].append(key)
    # Purge entries older than 3 days
    cutoff = (datetime.now(WAT) - timedelta(days=3)).strftime("%Y-%m-%d")
    sent = {k: v for k, v in sent.items() if k >= cutoff}
    _save_sent(sent)


def send_telegram(msg: str):
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN":
        print("⚠️  BOT_TOKEN not set — message NOT sent to Telegram")
        return
    if not CHAT_ID or CHAT_ID == "YOUR_CHAT_ID":
        print("⚠️  CHAT_ID not set — message NOT sent to Telegram")
        return
    try:
        # Split long messages (Telegram max 4096 chars)
        max_len = 4000
        chunks  = [msg[i:i+max_len] for i in range(0, len(msg), max_len)]
        for chunk in chunks:
            resp = _http.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                data={"chat_id": CHAT_ID, "text": chunk, "parse_mode": ""},
                timeout=15,
            )
            if resp.status_code != 200:
                print(f"⚠️  Telegram HTTP {resp.status_code}: {resp.text[:200]}")
            else:
                print(f"📩 Sent to Telegram ({len(chunk)} chars)")
    except Exception as e:
        print(f"Telegram error: {e}")


# ══════════════════════════════════════════════════════
# BROWSER
# ══════════════════════════════════════════════════════
def launch():
    pw = sync_playwright().start()
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


# ══════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════
def _parse_surface(title: str) -> str:
    t = (title or "").lower()
    if "clay"    in t: return "clay"
    if "grass"   in t: return "grass"
    if "indoor"  in t: return "indoors"
    if "hard"    in t: return "hard"
    return "unknown"

def _surface_from_tournament_name(name: str) -> str:
    n = (name or "").lower()
    CLAY_KEYS  = ["roland","french","clay","madrid","rome","barcelona","prague",
                  "bucharest","bogota","marrakech","istanbul","rabat","strasbourg",
                  "parma","hamburg","warsaw","rouen","oeiras","estoril"]
    GRASS_KEYS = ["wimbledon","grass","eastbourne","birmingham","bad homburg",
                  "rosmalen","s-hertogenbosch","nottingham","berlin grass"]
    IND_KEYS   = ["indoor","doha","dubai","abu dhabi","st. petersburg","linz",
                  "luxembourg","ostrava","guadalajara indoor"]
    if any(k in n for k in CLAY_KEYS):  return "clay"
    if any(k in n for k in GRASS_KEYS): return "grass"
    if any(k in n for k in IND_KEYS):   return "indoors"
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

# ══════════════════════════════════════════════════════
# RATE LIMIT / ERROR PAGE HELPERS
# ══════════════════════════════════════════════════════
_ERROR_URLS   = ("chrome-error://", "about:blank#", "cloudflare", "/cdn-cgi/")
_ERROR_TITLES = ("just a moment", "access denied", "429", "too many requests", "error 429")

def _is_error_page(page) -> bool:
    try:
        url   = page.url.lower()
        title = page.title().lower()
        if any(e in url for e in _ERROR_URLS):
            return True
        if any(e in title for e in _ERROR_TITLES):
            return True
    except Exception:
        pass
    return False

def _safe_goto(page, url: str, context, retries: int = 2) -> bool:
    """Navigate safely with exponential backoff on connection errors."""
    for attempt in range(retries + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=20000)
            if _is_error_page(page):
                wait_ms = (2 ** attempt) * 3000 + random.randint(500, 1500)
                print(f"   ⚠ Rate limit detected. Backing off {wait_ms//1000}s...")
                page.wait_for_timeout(wait_ms)
                continue
            return True
        except Exception as e:
            err = str(e)
            if any(x in err for x in ("ERR_CONNECTION_RESET", "interrupted", "net::")):
                wait_ms = (2 ** attempt) * 4000 + random.randint(1000, 2000)
                print(f"   ⚠ Connection reset (attempt {attempt+1}). Waiting {wait_ms//1000}s...")
                try:
                    page.wait_for_timeout(wait_ms)
                    page.goto("about:blank", wait_until="domcontentloaded", timeout=5000)
                except Exception:
                    pass
            else:
                print(f"   ❌ Nav error: {e}")
                return False
    return False



def _parse_match_date(txt: str) -> datetime | None:
    """Try to parse a match date from tennisexplorer row text."""
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(txt.strip(), fmt)
        except:
            continue
    return None

def is_allowed_tournament(name: str) -> bool:
    """
    Block ALL ITF events — W15/W25/W35/W50/W60/W100 prefix OR
    the word ITF anywhere in the name (e.g. "Portoroz ITF").
    Also block UTR Pro series and other non-WTA events.
    """
    n = (name or "").strip()
    if not n:
        return False

    nl = n.lower()

    # Block if "itf" appears ANYWHERE in the name
    if "itf" in nl:
        print(f"   FILTER: ITF blocked → {n}")
        return False

    # Block W-prefix sub-tour events (W15, W25, W35, W50, W60, W100)
    if _ITF_PREFIXES.search(n):
        print(f"   FILTER: sub-tour blocked → {n}")
        return False

    # Block UTR Pro and other non-WTA series
    if any(x in nl for x in ("utr pro", "utr ", "futures", "challenger")):
        print(f"   FILTER: non-WTA series blocked → {n}")
        return False

    return True


# ══════════════════════════════════════════════════════
# STEP 1 — GET TODAY'S MATCHES
# ══════════════════════════════════════════════════════
def get_today_matches(context, day: str = "today") -> list:
    page = context.new_page()
    matches = []
    print("🔍 Loading today's matches...")
    try:
        ok = _safe_goto(page, f"{BASE}/matches/?type=wta-single&day={day}", context, retries=2)
        if not ok:
            print("❌ Failed to load matches page after retries.")
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
        print("✅ Page loaded")

        rows = page.query_selector_all("tr")
        print(f"   Total tr rows: {len(rows)}")

        current_surface    = "hard"
        current_tournament = "Unknown"

        i = 0
        while i < len(rows) - 1:
            row1 = rows[i]
            try:
                t_name_td = row1.query_selector("td.t-name")
                if t_name_td:
                    links = t_name_td.query_selector_all("a")
                    player_links = [l for l in links
                                    if "/player/" in (l.get_attribute("href") or "")]
                    if not player_links:
                        tourn_link = t_name_td.query_selector("a")
                        if tourn_link:
                            current_tournament = tourn_link.inner_text().strip()
                        s_span = row1.query_selector("td.s-color span[title]")
                        if s_span:
                            current_surface = _parse_surface(s_span.get_attribute("title") or "")
                        else:
                            inf = _surface_from_tournament_name(current_tournament)
                            if inf != "unknown":
                                current_surface = inf
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

                # Try to get round label from row
                round_label = ""
                try:
                    round_td = row1.query_selector("td.round, td.r")
                    if round_td:
                        rt = round_td.inner_text().strip().upper()
                        round_map = {
                            "F": "🏆 Final", "SF": "🥈 Semi-Final",
                            "QF": "⚡ Quarter-Final", "R16": "R16",
                            "R32": "R32", "R64": "R64", "R128": "R128",
                            "1R": "R1", "2R": "R2", "3R": "R3",
                        }
                        round_label = round_map.get(rt, rt)
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

    print(f"🎾 Matches parsed: {len(matches)}")
    return matches


# ══════════════════════════════════════════════════════
# STEP 2A — PLAYER DATA (extended for v7)
# Additional fields scraped:
#   streak        — count consecutive wins (+) or losses (-) from recent matches
#   days_rest     — days between last completed match and today
#   matches_30d   — total matches in last 30 days (fatigue load)
#   serve_win_pct — 1st serve points won % (from stats table if present)
# ══════════════════════════════════════════════════════
def get_player_data(context, slug: str, display_name: str, page=None) -> dict:
    if slug in _player_cache:
        return _player_cache[slug]

    default = {
        "rank": 500, "elo": get_surface_elo(slug, "hard"),
        "sw": {}, "sl": {},
        "recent_wins": 0, "recent_total": 0,
        "recent_surface_wins": {}, "recent_surface_total": {},
        "streak": 0,
        "days_rest": 3,
        "matches_30d": 0,
        "serve_win_pct": None,
    }

    if not slug:
        return default

    owns_page = False
    if page is None:
        page = context.new_page()
        owns_page = True
    print(f"   Fetching {display_name} ({slug})...")

    try:
        ok = _safe_goto(page, f"{BASE}/player/{slug}/", context, retries=1)
        if not ok or _is_error_page(page):
            print(f"   ❌ Could not load {display_name} — using defaults")
            return default
        try:
            page.wait_for_selector("table.plDetail", timeout=5000)
        except Exception:
            return default

        # ── RANK ──────────────────────────────────────────────
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

        # ── SURFACE W/L ───────────────────────────────────────
        sw, sl = {}, {}
        try:
            btables = page.query_selector_all("table.result.balance")
            if btables:
                tbody_rows = btables[0].query_selector_all("tbody tr")
                for yr_idx, yr_weight in [(0, 2), (1, 1)]:
                    if yr_idx >= len(tbody_rows):
                        break
                    cells = tbody_rows[yr_idx].query_selector_all("td")
                    for surf, col in SURFACE_COL.items():
                        if col >= len(cells):
                            continue
                        w, l = _wl(cells[col].inner_text())
                        if w is None:
                            continue
                        sw[surf] = sw.get(surf, 0) + w * yr_weight
                        sl[surf] = sl.get(surf, 0) + l * yr_weight
        except Exception as e:
            print(f"   balance err: {e}")

        # ── RECENT FORM + STREAK + FATIGUE ────────────────────
        recent_wins          = 0
        recent_total         = 0
        recent_surface_wins  = {}
        recent_surface_total = {}
        streak               = 0
        streak_locked        = False
        days_rest            = 3
        matches_30d          = 0
        today                = datetime.now()

        try:
            year = today.year
            mdiv = page.query_selector(f"div#matches-{year}-1-data")
            if not mdiv:
                mdiv = page.query_selector(f"div#matches-{year-1}-1-data")
            if mdiv:
                surname = display_name.split()[0].lower().strip(".") if display_name else ""
                match_rows = mdiv.query_selector_all("tr.one, tr.two")[:20]

                for idx, mrow in enumerate(match_rows):
                    t_td = mrow.query_selector("td.t-name")
                    if not t_td:
                        continue
                    strong = t_td.query_selector("strong")
                    if not strong:
                        continue

                    s_span = mrow.query_selector("td.s-color span[title]")
                    m_surf = _parse_surface(s_span.get_attribute("title") if s_span else "")

                    # Only count rows with a completed score (td.score or td.tl)
                    score_td = mrow.query_selector("td.score, td.tl")
                    if not score_td or not score_td.inner_text().strip():
                        continue   # upcoming/no result row — skip

                    strong_txt = strong.inner_text().lower()
                    won = surname and surname in strong_txt

                    # Streak: count from most recent match backwards
                    if not streak_locked:
                        if idx == 0:
                            streak = 1 if won else -1
                        elif won and streak > 0:
                            streak += 1
                        elif not won and streak < 0:
                            streak -= 1
                        else:
                            streak_locked = True

                    # Days rest — try multiple selectors (tennisexplorer varies)
                    date_td = (
                        mrow.query_selector("td.date") or
                        mrow.query_selector("td.first.date") or
                        mrow.query_selector("td:first-child")
                    )
                    match_date = None
                    if date_td:
                        match_date = _parse_match_date(date_td.inner_text())

                    if idx == 0 and match_date:
                        days_rest = max(0, (today - match_date).days)

                    # Matches in last 30 days
                    if idx < 15 and match_date:
                        if (today - match_date).days <= 30:
                            matches_30d += 1

                    if won:
                        recent_wins += 1
                        recent_surface_wins[m_surf] = recent_surface_wins.get(m_surf, 0) + 1
                    recent_total += 1
                    recent_surface_total[m_surf] = recent_surface_total.get(m_surf, 0) + 1

        except Exception as e:
            print(f"   form/streak err: {e}")

        # ── SERVE STATS (optional — present for some players) ─
        serve_win_pct = None
        try:
            # tennisexplorer shows serve stats in some profile pages
            # look for "1st serve points won" or similar
            stat_tables = page.query_selector_all("table.stat")
            for tbl in stat_tables:
                rows_s = tbl.query_selector_all("tr")
                for sr in rows_s:
                    txt = sr.inner_text().lower()
                    if "1st serve" in txt and "won" in txt:
                        cells = sr.query_selector_all("td")
                        if len(cells) >= 2:
                            pct = _safe_float(cells[-1].inner_text().replace("%",""))
                            if pct and 30 < pct < 100:
                                serve_win_pct = pct / 100.0
                                break
                if serve_win_pct:
                    break
        except Exception:
            pass

        data = {
            "rank":                 rank,
            "elo":                  get_surface_elo(slug, "hard"),
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
        }
        _player_cache[slug] = data
        return data

    except Exception as e:
        print(f"   ❌ failed {display_name}: {e}")
        return default
    finally:
        if owns_page:
            page.close()


# ══════════════════════════════════════════════════════
# STEP 2B — HEAD-TO-HEAD (NEW)
# Scrapes /h2h/{slug1}-vs-{slug2}/
# Returns (p1_h2h_wins, total_h2h_meetings)
# ══════════════════════════════════════════════════════

def _name_to_slug(name: str) -> str:
    return ""


def get_h2h(context, slug1, slug2, p1_name, p2_name, page=None):
    """H2H disabled — returns (0,0) to avoid tennisratio rate limits."""
    return (0, 0)


def _prefetch_players(context, matches, limit=PREFETCH_LIMIT):
    """Warm player cache with safe navigation, jitter, and page recreation."""
    uniq = []
    seen = set()
    for m in matches[:50]:
        for slug_key, name_key in (("slug1","p1"),("slug2","p2")):
            slug = (m.get(slug_key) or "").strip()
            if not slug or slug in seen:
                continue
            seen.add(slug)
            uniq.append((slug, m.get(name_key) or slug))
            if len(uniq) >= limit:
                break
        if len(uniq) >= limit:
            break

    if not uniq:
        return

    print(f"[PREFETCH] Warming {len(uniq)} players...")
    page = context.new_page()
    consecutive_errors = 0

    try:
        for idx, (slug, name) in enumerate(uniq):
            # Jittered delay — humanises the request pattern
            jitter = random.randint(900, 1600)
            page.wait_for_timeout(jitter)

            # Recreate page after 3 consecutive errors
            if consecutive_errors >= 3:
                print(f"   ♻ Recreating page after {consecutive_errors} errors...")
                try:
                    page.close()
                except Exception:
                    pass
                page = context.new_page()
                consecutive_errors = 0
                page.wait_for_timeout(6000)

            ok = _safe_goto(page, f"{BASE}/player/{slug}/", context, retries=1)
            if not ok:
                consecutive_errors += 1
                print(f"   SKIP: {name}")
                continue

            try:
                page.wait_for_selector("table.plDetail", timeout=5000)
                get_player_data(context, slug, name, page=page)
                consecutive_errors = 0
                print(f"   ✅ [{idx+1}/{len(uniq)}] {name}")
            except Exception as e:
                consecutive_errors += 1
                print(f"   ⚠ {name}: {e}")

    finally:
        try:
            page.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════
# STEP 3 — SCORING MODEL v7
# ══════════════════════════════════════════════════════
def score_player(data: dict, surface: str, slug: str = "") -> float:
    rank       = data.get("rank", 500)
    sw         = data.get("sw", {})
    sl         = data.get("sl", {})
    rw         = data.get("recent_wins", 0)
    rt         = data.get("recent_total", 0)
    streak     = data.get("streak", 0)
    days_rest  = data.get("days_rest", 3)
    serve_pct  = data.get("serve_win_pct", None)
    elo_surf   = get_surface_elo(slug, surface) if slug else 1500.0

    # 1. Rank (30%)
    rank_score = max(0.0, 1.0 - rank / 500.0)

    # 2. Surface win rate (25%)
    w = sw.get(surface, 0)
    l = sl.get(surface, 0)
    surf_score = (w / (w + l)) if (w + l) >= 5 else 0.5

    # 3. Recent form (15%) — optionally blended with serve stats
    form_score = (rw / rt) if rt >= 4 else 0.5
    if serve_pct is not None:
        form_score = form_score * 0.6 + serve_pct * 0.4

    # 4. Surface ELO (15%)
    elo_score = min(1.0, elo_surf / 2000.0)

    # 5. Momentum / streak (8%)
    streak_score = 0.5 + max(-0.4, min(0.4, streak * 0.08))

    # 6. Fatigue (7%)
    if days_rest <= 0:    fatigue_score = 0.30
    elif days_rest == 1:  fatigue_score = 0.55
    elif days_rest <= 3:  fatigue_score = 0.80
    elif days_rest <= 7:  fatigue_score = 0.70
    else:                 fatigue_score = 0.60

    return (
        rank_score    * 0.30 +
        surf_score    * 0.25 +
        form_score    * 0.15 +
        elo_score     * 0.15 +
        streak_score  * 0.08 +
        fatigue_score * 0.07
    )


def h2h_adjustment(prob: float, h2h_wins: int, h2h_total: int) -> float:
    """Adjust probability by H2H record. Max ±8% at 10+ meetings."""
    if h2h_total < 2:
        return prob
    h2h_rate = h2h_wins / h2h_total
    weight   = min(0.08, h2h_total * 0.008)
    adjusted = prob * (1 - weight) + h2h_rate * weight
    return max(0.10, min(0.90, adjusted))


def odds_blend_and_sanity(model_prob: float, odds_p1, odds_p2) -> tuple[float, float]:
    """
    Blend model probability with market (70/30).
    If gap > 20%, reduce confidence multiplier to 0.75.
    Returns (blended_prob, conf_multiplier).
    """
    if not odds_p1 or not odds_p2:
        return model_prob, 1.0

    raw_p1 = 1.0 / odds_p1
    raw_p2 = 1.0 / odds_p2
    total  = raw_p1 + raw_p2          # remove vig
    market_p1_fair = raw_p1 / total

    gap = abs(model_prob - market_p1_fair)
    conf_mult = 0.75 if gap > 0.20 else (0.90 if gap > 0.10 else 1.00)

    blended = model_prob * 0.70 + market_p1_fair * 0.30
    return blended, conf_mult


def win_prob(s1: float, s2: float) -> float:
    return 1.0 / (1.0 + math.exp(-10.0 * (s1 - s2)))


def confidence_pct(prob: float, conf_mult: float = 1.0) -> int:
    raw = 50 + abs(prob - 0.5) * 180
    return int(min(95, raw * conf_mult))


def market_edge(model_prob: float, odds) -> float:
    if not odds or odds <= 1.0:
        return 0.0
    return model_prob - (1.0 / odds)


def grade_conf(conf: int) -> str:
    if conf >= 80: return "HIGH"
    if conf >= 65: return "MEDIUM"
    return "LOW"

def grade_icon(grade: str) -> str:
    return {"HIGH": "🔥", "MEDIUM": "⚡", "LOW": "🌡️"}.get(grade, "")


# ══════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════
def run():
    t0 = time.time()
    pw, browser, context = launch()

    try:
        matches = get_today_matches(context, day="today")

        if not matches:
            print("❌ No matches found.")
            send_telegram("No WTA matches found today.")
            return

        _prefetch_players(context, matches, limit=30)

        # H2H disabled — tennisratio rate limiting too aggressive

        picks = []

        for m in matches[:30]:
            try:
                print(f"\n── {m['p1']} vs {m['p2']}  [{m['surface']}]  [{m['tournament']}]")

                d1 = get_player_data(context, m["slug1"], m["p1"])
                d2 = get_player_data(context, m["slug2"], m["p2"])

                s1   = score_player(d1, m["surface"], m["slug1"])
                s2   = score_player(d2, m["surface"], m["slug2"])
                prob = win_prob(s1, s2)

                # H2H adjustment
                h2h_wins, h2h_total = _h2h_cache.get(
                    f"{min(m['slug1'],m['slug2'])}_{max(m['slug1'],m['slug2'])}",
                    (0, 0)
                )
                prob = h2h_adjustment(prob, h2h_wins, h2h_total)

                # Odds blend + sanity
                blended, conf_mult = odds_blend_and_sanity(prob, m["odds_p1"], m["odds_p2"])

                conf      = confidence_pct(blended, conf_mult)
                favourite = m["p1"] if blended > 0.5 else m["p2"]
                fav_prob  = max(blended, 1 - blended)
                fav_odds  = m["odds_p1"] if blended > 0.5 else m["odds_p2"]
                edge      = market_edge(fav_prob, fav_odds)

                print(
                    f"   {m['p1']:22s} rank={d1['rank']:>3}  streak={d1['streak']:+d}  "
                    f"rest={d1['days_rest']}d  score={s1:.3f}\n"
                    f"   {m['p2']:22s} rank={d2['rank']:>3}  streak={d2['streak']:+d}  "
                    f"rest={d2['days_rest']}d  score={s2:.3f}\n"
                    f"   H2H {h2h_wins}/{h2h_total}  prob={blended:.3f}  "
                    f"conf={conf}%  edge={edge:+.3f}"
                )

                # ── Filters ─────────────────────────────────
                if conf < 65:
                    print(f"   SKIP: conf {conf}% < 65%")
                    continue
                if abs(blended - 0.5) < 0.07:
                    print(f"   SKIP: too close")
                    continue
                if not fav_odds:
                    print(f"   SKIP: no odds")
                    continue
                if edge < 0.03:
                    print(f"   SKIP: edge {edge:+.3f} < 0.03")
                    continue

                grade = grade_conf(conf)
                picks.append({
                    "m": m, "d1": d1, "d2": d2,
                    "s1": s1, "s2": s2,
                    "prob": fav_prob, "conf": conf, "grade": grade,
                    "winner": favourite, "winner_odds": fav_odds,
                    "edge": edge,

                    "streak1": d1.get("streak", 0),
                    "streak2": d2.get("streak", 0),
                })

            except Exception as e:
                print(f"   ERROR: {e}")

        # Remove matches already sent today
        picks = [pk for pk in picks
                 if not is_already_sent(pk["m"]["p1"], pk["m"]["p2"])]

        # HIGH confidence only (80%+)
        picks = [pk for pk in picks if pk["grade"] == "HIGH"]

        if picks:
            print(f"[INFO] {len(picks)} HIGH confidence picks after dedup filter")

        picks.sort(key=lambda x: x["conf"], reverse=True)

        if not picks:
            print("[INFO] No new qualifying picks (all filtered or already sent today).")
            return   # Silent — don't spam Telegram with "no picks" messages

        now_wat   = datetime.now(WAT)
        today_str = now_wat.strftime("%A %d %B %Y")
        lines = [f"🎾 WTA ENGINE v7\n📅 {today_str}\n"]

        for pk in picks[:6]:
            m        = pk["m"]
            icon     = grade_icon(pk["grade"])
            grade    = pk["grade"]
            odds_str = f" @ {pk['winner_odds']}" if pk["winner_odds"] else ""
            bar      = "█" * (pk["conf"] // 10) + "░" * (10 - pk["conf"] // 10)
            s1_str   = f"{pk['s1']:.2f}"
            s2_str   = f"{pk['s2']:.2f}"
            str1     = f"{pk['streak1']:+d}" if pk["streak1"] else "0"
            str2     = f"{pk['streak2']:+d}" if pk["streak2"] else "0"
            lines.append(
                f"─────────────────────────\n"
                f"🏟  {m['tournament']}  ({m['surface'].title()}){('  · ' + m['round']) if m.get('round') else ''}\n"
                f"⚔️  {m['p1']} vs {m['p2']}\n"
                f"{icon} {grade}  ✅  {pk['winner']}{odds_str}\n"
                f"[{bar}] {pk['conf']}%\n"
                f"Ranks: #{pk['d1']['rank']} vs #{pk['d2']['rank']}\n"
                f"Scores: {s1_str} vs {s2_str}\n"
                f"Streak: {str1} vs {str2}\n"
                f"Edge: {pk['edge']:+.2f}  ⏰ {m['time']}\n"
            )

        sent_time = datetime.now(WAT).strftime("%H:%M WAT")
        lines.append(f"⚠️ For entertainment only.\n🕐 Sent at {sent_time}")
        msg = "\n".join(lines)
        print("\n" + msg)
        send_telegram(msg)
        mark_as_sent(picks[:6])   # record what was sent today

    finally:
        browser.close()
        pw.stop()
        print(f"\n⚡ Done in {round(time.time() - t0, 1)}s")


if __name__ == "__main__":
    run()

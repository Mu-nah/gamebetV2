"""
Scheduler v3

  ⚽ Football  → 2x per day  (08:00 and 13:00 UTC)
  🏀 NBA       → every 6h    (00:00, 06:00, 12:00, 18:00 UTC)
  🎾 Tennis    → every 4h    (08:00, 12:00, 16:00, 20:00 UTC)

Usage:
  python scheduler.py              # Full schedule
  python scheduler.py --now        # Run all sports immediately
  python scheduler.py --sport football
  python scheduler.py --sport nba
  python scheduler.py --sport tennis
"""

import sys
import time
import os
import json
from datetime import datetime, timezone, timedelta
from news_analyzer import get_team_sentiment

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

WAT_OFFSET = timezone(timedelta(hours=1))


# Track fixture IDs already sent today — reset at midnight WAT
_sent_fixture_ids = set()
_sent_date        = None

STATE_DIR = ".state"


def _state_path(sport: str, day) -> str:
    safe_sport = "".join(c for c in (sport or "sport") if c.isalnum() or c in ("-", "_")).lower()
    return os.path.join(STATE_DIR, f"sent_{safe_sport}_{day}.json")


def _load_sent_ids(sport: str, day) -> set:
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        path = _state_path(sport, day)
        if not os.path.exists(path):
            return set()
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set(str(x) for x in data)
    except Exception:
        pass
    return set()


def _save_sent_ids(sport: str, day, ids_set: set) -> None:
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        path = _state_path(sport, day)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(sorted(list(set(str(x) for x in ids_set))), f)
    except Exception:
        # Best-effort; don't crash the scheduler if state can't be written.
        pass


def _notice_path(sport: str, day) -> str:
    safe_sport = "".join(c for c in (sport or "sport") if c.isalnum() or c in ("-", "_")).lower()
    return os.path.join(STATE_DIR, f"notice_{safe_sport}_{day}.json")


def _load_notice_sent(sport: str, day) -> bool:
    """
    Whether we've already sent a "no HIGH predictions" notice today for this sport.
    Stored on disk so cron runs don't spam Telegram.
    """
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        path = _notice_path(sport, day)
        if not os.path.exists(path):
            return False
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return bool(data.get("sent")) if isinstance(data, dict) else bool(data)
    except Exception:
        return False


def _save_notice_sent(sport: str, day) -> None:
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        path = _notice_path(sport, day)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"sent": True}, f)
    except Exception:
        pass


def _default_dedupe_mode() -> bool:
    """
    Default behavior:
    - On GitHub Actions: dedupe on (prevents repeated sends every cron run)
    - Locally: if user runs `--now`, default to resend (dedupe off) unless overridden
    """
    try:
        return str(os.getenv("GITHUB_ACTIONS", "")).strip().lower() in ("1", "true", "yes")
    except Exception:
        return False


def run_football(dedupe: bool = True):
    global _sent_fixture_ids, _sent_date

    from multi_sport_bot import (
        validate_config, fetch_football_fixtures,
        fetch_football_team_stats, fetch_h2h, fetch_football_odds,
        format_football_card, format_sport_summary,
        TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    )
    from predictor import FootballPredictor
    from telegram_sender import TelegramSender

    validate_config("football")
    sender   = TelegramSender(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    f_pred   = FootballPredictor()
    now_wat  = datetime.now(WAT_OFFSET)
    date_str = now_wat.strftime("%A, %d %B %Y")
    today    = now_wat.date()

    # Reset sent tracker at the start of each new WAT day
    if _sent_date != today:
        _sent_fixture_ids = _load_sent_ids("football", today) if dedupe else set()
        _sent_date = today

    fixtures = fetch_football_fixtures()
    results, skipped = [], []
    for fix in fixtures:
        fid = fix["fixture_id"]
        if fid in _sent_fixture_ids:
            print(f"[SKIP] Already sent: {fix['home_team']} v {fix['away_team']}")
            continue
        hs   = fetch_football_team_stats(fix["home_id"], fix["league_id"])
        aws  = fetch_football_team_stats(fix["away_id"], fix["league_id"])
        source = fix.get("source", "api-football")
        if source != "api-football":
            h2h = []
            odds = None
        else:
            h2h  = fetch_h2h(fix["home_id"], fix["away_id"])
            odds = fetch_football_odds(fix["fixture_id"])
        # Get news sentiment for dynamic adjustment
        sentiment_home = get_team_sentiment(fix["home_team"], "football")["score"]
        sentiment_away = get_team_sentiment(fix["away_team"], "football")["score"]
        pred = f_pred.predict(fix, hs, aws, h2h, odds=odds, sentiment_home=sentiment_home, sentiment_away=sentiment_away)
        if pred.get("skip"):
            skipped.append(f"⏭ {fix['home_team']} v {fix['away_team']} — {pred['reason']}")
        else:
            results.append((fix, pred))

    # Only send HIGH confidence predictions
    results = [(f, p) for f, p in results if p.get("grade") == "HIGH 🔥"]

    if results:
        sender.send_message(format_sport_summary("⚽", "FOOTBALL", results, date_str), parse_mode="Markdown")
        for fix, pred in results:
            sender.send_message(format_football_card(fix, pred), parse_mode="Markdown")
            _sent_fixture_ids.add(fix["fixture_id"])  # mark as sent
        # Always persist what we sent today so scheduled runs don't spam,
        # but only use this state for filtering when `dedupe=True`.
        _save_sent_ids("football", today, _sent_fixture_ids)
    elif fixtures:
        # Send the "no HIGH picks" notice at most once per day.
        # Use persisted sent IDs even when dedupe is off (manual --resend) so we don't spam notices.
        sent_any = _load_sent_ids("football", today)
        if not sent_any and not _load_notice_sent("football", today):
            sender.send_message("⚽ No HIGH confidence football predictions right now.", parse_mode="Markdown")
            _save_notice_sent("football", today)
    elif not fixtures:
        pass  # silent — no fixtures
    print(f"[FOOTBALL] {len(results)} new predictions sent, {len(skipped)} skipped, "
          f"{len(_sent_fixture_ids)} total sent today.")


def run_nba(dedupe: bool = True):
    from multi_sport_bot import (
        validate_config, fetch_nba_fixtures,
        fetch_nba_team_season_stats, _ensure_nba_stats_loaded,
        format_basketball_card, format_sport_summary,
        TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    )
    from basketball_predictor import BasketballPredictor
    from telegram_sender import TelegramSender

    validate_config("nba")
    sender   = TelegramSender(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    b_pred   = BasketballPredictor()
    date_str = datetime.now(WAT_OFFSET).strftime("%A, %d %B %Y")

    # Load team stats ONCE before the loop
    _ensure_nba_stats_loaded()

    now_wat = datetime.now(WAT_OFFSET)
    today = now_wat.date()
    sent_ids = _load_sent_ids("nba", today) if dedupe else set()

    fixtures = fetch_nba_fixtures()
    results  = []
    for fix in fixtures:
        fid = str(fix.get("fixture_id") or "")
        if fid and fid in sent_ids:
            continue
        wp  = fix.pop("win_prob", None)
        hs  = fetch_nba_team_season_stats(fix.get("home_id"), team_name=fix.get("home_team", ""))
        aws = fetch_nba_team_season_stats(fix.get("away_id"), team_name=fix.get("away_team", ""))
        # Get news sentiment for dynamic adjustment
        sentiment_home = get_team_sentiment(fix["home_team"], "basketball")["score"]
        sentiment_away = get_team_sentiment(fix["away_team"], "basketball")["score"]
        pred = b_pred.predict(fix, home_stats=hs, away_stats=aws, api_win_prob=wp, sentiment_home=sentiment_home, sentiment_away=sentiment_away)
        results.append((fix, pred))

    # Only send HIGH confidence predictions
    results = [(f, p) for f, p in results if p.get("grade") == "HIGH 🔥"]

    if results:
        sender.send_message(format_sport_summary("🏀", "NBA BASKETBALL", results, date_str), parse_mode="Markdown")
        for fix, pred in results:
            sender.send_message(format_basketball_card(fix, pred), parse_mode="Markdown")
            if fix.get("fixture_id") is not None:
                sent_ids.add(str(fix["fixture_id"]))
        _save_sent_ids("nba", today, sent_ids)
    else:
        # Don't spam "no qualifying" if we've already sent any picks today.
        sent_any = _load_sent_ids("nba", today)
        if not sent_any and not _load_notice_sent("nba", today):
            sender.send_message("🏀 No HIGH confidence NBA predictions right now.", parse_mode="Markdown")
            _save_notice_sent("nba", today)
    print(f"[NBA] {len(results)} sent (LOW confidence filtered out).")


def run_tennis(dedupe: bool = True):
    from multi_sport_bot import (
        validate_config, fetch_tennis_fixtures,
        fetch_tennis_player_stats,
        format_tennis_card, format_sport_summary,
        TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    )
    from tennis_predictor import TennisPredictor
    from telegram_sender import TelegramSender

    validate_config("tennis")
    sender   = TelegramSender(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    t_pred   = TennisPredictor()
    date_str = datetime.now(WAT_OFFSET).strftime("%A, %d %B %Y")

    now_wat = datetime.now(WAT_OFFSET)
    today = now_wat.date()
    sent_ids = _load_sent_ids("tennis", today) if dedupe else set()
    if dedupe:
        print(f"[INFO] Tennis dedupe ON — {len(sent_ids)} fixture(s) already sent today.")

    fixtures = fetch_tennis_fixtures()
    results  = []
    skipped_already_sent = 0
    for fix in fixtures:
        fid = str(fix.get("fixture_id") or "")
        if fid and fid in sent_ids:
            skipped_already_sent += 1
            continue
        surface    = t_pred._detect_surface(fix.get("tournament", ""))
        home_stats = fetch_tennis_player_stats(
            fix.get("home_player_key", ""), surface,
            player_name=fix.get("home_team", "")
        )
        away_stats = fetch_tennis_player_stats(
            fix.get("away_player_key", ""), surface,
            player_name=fix.get("away_team", "")
        )
        # Get news sentiment for dynamic adjustment
        sentiment_home = get_team_sentiment(fix["home_team"], "tennis")["score"]
        sentiment_away = get_team_sentiment(fix["away_team"], "tennis")["score"]
        pred = t_pred.predict(fix, home_stats=home_stats, away_stats=away_stats, sentiment_home=sentiment_home, sentiment_away=sentiment_away)
        results.append((fix, pred))

    # Only send HIGH confidence predictions
    results = [(f, p) for f, p in results if p.get("grade") == "HIGH 🔥"]

    if results:
        sender.send_message(format_sport_summary("🎾", "TENNIS", results, date_str), parse_mode="Markdown")
        for fix, pred in results:
            sender.send_message(format_tennis_card(fix, pred), parse_mode="Markdown")
            if fix.get("fixture_id") is not None:
                sent_ids.add(str(fix["fixture_id"]))
        _save_sent_ids("tennis", today, sent_ids)
    else:
        sent_any = _load_sent_ids("tennis", today)
        if not sent_any and not _load_notice_sent("tennis", today):
            sender.send_message("🎾 No HIGH confidence tennis predictions right now.", parse_mode="Markdown")
            _save_notice_sent("tennis", today)
    if skipped_already_sent:
        print(f"[INFO] Tennis: skipped {skipped_already_sent} already-sent fixture(s) today.")
    print(f"[TENNIS] {len(results)} sent (LOW confidence filtered out).")


def run_all():
    run_football()
    run_nba()
    run_tennis()


def main():
    sport = None
    if "--sport" in sys.argv:
        idx   = sys.argv.index("--sport")
        sport = sys.argv[idx + 1].lower() if idx + 1 < len(sys.argv) else None

    # Dedupe flags
    dedupe = _default_dedupe_mode()
    if "--dedupe" in sys.argv:
        dedupe = True
    if "--resend" in sys.argv or "--no-dedupe" in sys.argv:
        dedupe = False

    def _run():
        if sport == "football":
            return run_football(dedupe=dedupe)
        if sport == "nba":
            return run_nba(dedupe=dedupe)
        if sport == "tennis":
            return run_tennis(dedupe=dedupe)
        # all sports
        return (run_football(dedupe=dedupe), run_nba(dedupe=dedupe), run_tennis(dedupe=dedupe))

    run_fn = _run

    label  = sport.upper() if sport else "ALL SPORTS"

    if "--now" in sys.argv:
        print(f"[SCHEDULER] Running {label} immediately...")
        # Local manual runs: default to resend everything unless the user asked for dedupe.
        if str(os.getenv("GITHUB_ACTIONS", "")).strip().lower() not in ("1", "true", "yes") and "--dedupe" not in sys.argv and "--resend" not in sys.argv and "--no-dedupe" not in sys.argv:
            dedupe = False
        _run()
        return

    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        scheduler = BlockingScheduler(timezone="UTC")

        if sport == "football" or sport is None:
            # Run every 2 hours — catches fixtures as soon as API-Football publishes them
            scheduler.add_job(run_football, "cron", hour="*/2", minute=0, id="football")
            print("[SCHEDULER] ⚽ Football: every 2 hours (catches fixtures ~2h before kickoff)")

        if sport == "nba" or sport is None:
            scheduler.add_job(run_nba, "cron", hour="0,6,12,18", minute=0, id="nba")
            print("[SCHEDULER] 🏀 NBA: 00:00, 06:00, 12:00, 18:00 UTC")

        if sport == "tennis" or sport is None:
            scheduler.add_job(run_tennis, "cron", hour="8,12,16,20", minute=0, id="tennis")
            print("[SCHEDULER] 🎾 Tennis: 08:00, 12:00, 16:00, 20:00 UTC")

        print("[SCHEDULER] Running. Ctrl+C to stop.\n")
        scheduler.start()

    except ImportError:
        print("[WARN] APScheduler not installed — simple loop fallback.")
        while True:
            run_fn()
            sleep_hrs = 4 if sport == "tennis" else 6
            print(f"[SCHEDULER] Sleeping {sleep_hrs}h...")
            time.sleep(sleep_hrs * 3600)


if __name__ == "__main__":
    main()

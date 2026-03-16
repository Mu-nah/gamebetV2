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
from datetime import datetime, timezone, timedelta
from news_analyzer import get_team_sentiment

WAT_OFFSET = timezone(timedelta(hours=1))


# Track fixture IDs already sent today — reset at midnight WAT
_sent_fixture_ids = set()
_sent_date        = None


def run_football():
    global _sent_fixture_ids, _sent_date

    from multi_sport_bot import (
        validate_config, fetch_football_fixtures,
        fetch_football_team_stats, fetch_h2h, fetch_football_odds,
        format_football_card, format_sport_summary,
        TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    )
    from predictor import FootballPredictor
    from telegram_sender import TelegramSender

    validate_config()
    sender   = TelegramSender(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    f_pred   = FootballPredictor()
    now_wat  = datetime.now(WAT_OFFSET)
    date_str = now_wat.strftime("%A, %d %B %Y")
    today    = now_wat.date()

    # Reset sent tracker at the start of each new WAT day
    if _sent_date != today:
        _sent_fixture_ids = set()
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
        if source == "football-data":
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

    # Filter out LOW confidence predictions
    results = [(f, p) for f, p in results if p.get("grade") != "LOW 🌡️"]

    if results:
        sender.send_message(format_sport_summary("⚽", "FOOTBALL", results, date_str), parse_mode="Markdown")
        for fix, pred in results:
            sender.send_message(format_football_card(fix, pred), parse_mode="Markdown")
            _sent_fixture_ids.add(fix["fixture_id"])  # mark as sent
    elif not fixtures:
        pass  # silent — no need to spam "no matches" every 2 hours
    print(f"[FOOTBALL] {len(results)} new predictions sent, {len(skipped)} skipped, "
          f"{len(_sent_fixture_ids)} total sent today.")


def run_nba():
    from multi_sport_bot import (
        validate_config, fetch_nba_fixtures,
        fetch_nba_team_season_stats, _ensure_nba_stats_loaded,
        format_basketball_card, format_sport_summary,
        TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    )
    from basketball_predictor import BasketballPredictor
    from telegram_sender import TelegramSender

    validate_config()
    sender   = TelegramSender(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    b_pred   = BasketballPredictor()
    date_str = datetime.now(WAT_OFFSET).strftime("%A, %d %B %Y")

    # Load team stats ONCE before the loop
    _ensure_nba_stats_loaded()

    fixtures = fetch_nba_fixtures()
    results  = []
    for fix in fixtures:
        wp  = fix.pop("win_prob", None)
        hs  = fetch_nba_team_season_stats(fix.get("home_id"), team_name=fix.get("home_team", ""))
        aws = fetch_nba_team_season_stats(fix.get("away_id"), team_name=fix.get("away_team", ""))
        # Get news sentiment for dynamic adjustment
        sentiment_home = get_team_sentiment(fix["home_team"], "basketball")["score"]
        sentiment_away = get_team_sentiment(fix["away_team"], "basketball")["score"]
        pred = b_pred.predict(fix, home_stats=hs, away_stats=aws, api_win_prob=wp, sentiment_home=sentiment_home, sentiment_away=sentiment_away)
        results.append((fix, pred))

    # Filter out LOW confidence predictions
    results = [(f, p) for f, p in results if p.get("grade") != "LOW 🌡️"]

    if results:
        sender.send_message(format_sport_summary("🏀", "NBA BASKETBALL", results, date_str), parse_mode="Markdown")
        for fix, pred in results:
            sender.send_message(format_basketball_card(fix, pred), parse_mode="Markdown")
    else:
        sender.send_message("🏀 No qualifying NBA predictions today.", parse_mode="Markdown")
    print(f"[NBA] {len(results)} sent (LOW confidence filtered out).")


def run_tennis():
    from multi_sport_bot import (
        validate_config, fetch_tennis_fixtures,
        fetch_tennis_player_stats,
        format_tennis_card, format_sport_summary,
        TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    )
    from tennis_predictor import TennisPredictor
    from telegram_sender import TelegramSender

    validate_config()
    sender   = TelegramSender(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    t_pred   = TennisPredictor()
    date_str = datetime.now(WAT_OFFSET).strftime("%A, %d %B %Y")

    fixtures = fetch_tennis_fixtures()
    results  = []
    for fix in fixtures:
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

    # Filter out LOW confidence predictions
    results = [(f, p) for f, p in results if p.get("grade") != "LOW 🌡️"]

    if results:
        sender.send_message(format_sport_summary("🎾", "TENNIS", results, date_str), parse_mode="Markdown")
        for fix, pred in results:
            sender.send_message(format_tennis_card(fix, pred), parse_mode="Markdown")
    else:
        sender.send_message("🎾 No qualifying tennis predictions today.", parse_mode="Markdown")
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

    run_fn = {"football": run_football, "nba": run_nba, "tennis": run_tennis}.get(sport, run_all)
    label  = sport.upper() if sport else "ALL SPORTS"

    if "--now" in sys.argv:
        print(f"[SCHEDULER] Running {label} immediately...")
        run_fn()
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

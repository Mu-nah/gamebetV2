# 🤖 Multi-Sport Prediction Bot v8

A production-ready prediction bot covering Football, NBA Basketball, and Tennis.
Fetches today's fixtures automatically, runs rule-based predictions, and sends
formatted cards to Telegram — all times shown in **West Africa Time (WAT)**.

---

## 📦 Features

- ✅ **Football** — Premier League, La Liga, La Liga 2, Serie A, Serie B, Bundesliga, 2. Bundesliga, Ligue 1, UCL, Europa League, Conference League
- ✅ **NBA Basketball** — All games with real team stats (NBA.com + fallback standings)
- ✅ **Tennis** — ATP / WTA with ranking-based predictions (Miami Open, Indian Wells, Challengers, Grand Slams)
- ✅ **API Key Rotation** — Multiple keys per API; auto-rotates on 403/429/error
- ✅ **WAT Timestamps** — All match times in West Africa Time (UTC+1)
- ✅ **Matchday Schedule** — Full fixture list sent first, predictions follow after
- ✅ **24h Game Window** — NBA games fetched for the next 24 hours, not by calendar date
- ✅ **Season Fallback** — Tries current and previous season if fixtures return empty
- ✅ **xG Model** — Poisson-based expected goals for football
- ✅ **Real NBA Stats** — PPG, Net Rating, Win% from NBA.com with hardcoded 2024-25 fallback
- ✅ **Tennis Rankings** — Embedded ATP/WTA rankings matched by player name
- ✅ **Confidence Grading** — HIGH 🔥 / MEDIUM ⚡ / LOW 🌡️
- ✅ **Value Bet Detection** — Flags bets where model edge > 12% vs bookmaker odds
- ✅ **Telegram Retry** — Auto-retries on timeout, handles rate limits gracefully
- ✅ **Sport-Specific Schedules** — Football 2x/day, NBA every 6h, Tennis every 4h

---

## 🗂️ Project Structure

```
football_bot/
├── multi_sport_bot.py       # Main pipeline — Football + NBA + Tennis
├── api_client.py            # RotatingClient — auto key rotation on failure
├── predictor.py             # Football prediction engine (xG + Poisson)
├── basketball_predictor.py  # NBA engine (off/def rating, pace, spread)
├── tennis_predictor.py      # ATP/WTA engine (ranking, serve %, handicap)
├── telegram_sender.py       # Telegram messaging with retry logic
├── scheduler.py             # Sport-specific daily scheduler
├── setup.py                 # Interactive first-time setup wizard
├── requirements.txt         # Python dependencies
├── .env.example             # Environment variables template
└── README.md
```

---

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

No additional packages needed — everything uses `requests`, `apscheduler`, and `python-dotenv`.

### 2. Configure Your Keys

```bash
cp .env.example .env
```

Edit `.env` — add **multiple keys per API** separated by commas:

```env
# Football — add up to 5 free keys (100 req/day each)
FOOTBALL_API_KEYS=key1,key2,key3

# NBA — BallDontLie free (game schedules only — stats from NBA.com, no key needed)
BALLDONTLIE_KEYS=your-uuid-key

# Tennis — api-tennis.com free key
TENNIS_API_KEYS=your-hex-key

# Telegram
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

## 🤖 GitHub Actions (YAML Scheduler)

This repo includes GitHub Actions workflows that run the bot on a schedule:

- Football: every 2 hours (`.github/workflows/football.yml`)
- NBA: 00:00, 06:00, 12:00, 18:00 UTC (`.github/workflows/nba.yml`)
- Tennis: 08:00, 12:00, 16:00, 20:00 UTC (`.github/workflows/tennis.yml`)

To enable them, add these **Repository Secrets** in GitHub:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `FOOTBALL_API_KEYS`
- `FOOTBALL_DATA_KEYS` (optional, but recommended as a fallback source)
- `BALLDONTLIE_KEYS`
- `TENNIS_API_KEYS`

Notes:
- Cron times are in UTC (GitHub Actions limitation).
- The workflows run `python scheduler.py --sport <sport> --now` (a single run), not the long-running scheduler loop.

### 3. Get Your API Keys

| API | Sport | URL | Cost | Notes |
|---|---|---|---|---|
| api-football.com | ⚽ Football | https://www.api-football.com | Free | 100 req/key/day |
| balldontlie.io | 🏀 NBA schedules | https://app.balldontlie.io | Free | Games only |
| stats.nba.com | 🏀 NBA stats | Built-in, no signup | Free | Auto-fetched, no key |
| api-tennis.com | 🎾 Tennis | https://api-tennis.com | Free | Fixtures + player data |
| @BotFather | 📱 Telegram | Telegram app | Free | — |

### 4. Run the Bot

```bash
# Run all sports immediately (best for first test)
python scheduler.py --now

# Run a specific sport only
python scheduler.py --sport football
python scheduler.py --sport nba
python scheduler.py --sport tennis

# Start full daily schedule
python scheduler.py
```

---

## ⚠️ Important — Football Fixture Timing

**API-Football free tier only shows fixtures close to kickoff time** — typically
2-4 hours before the match. If you run the bot in the morning and see 0 football
fixtures, this is normal. Run again in the afternoon (around 18:00-19:00 WAT)
for evening kickoffs and fixtures will appear.

| Time to run | What you get |
|---|---|
| Morning (06:00-12:00 WAT) | NBA night games + Tennis |
| Afternoon (14:00-18:00 WAT) | Football starts appearing + all sports |
| Evening (18:00+ WAT) | Full slate — Football + NBA + Tennis |

---

## 📊 Data Sources

### ⚽ Football
| Data | Source |
|---|---|
| Fixtures | api-football.com (tries season N and N-1 automatically) |
| Team stats | api-football.com |
| Head-to-head | api-football.com |
| Odds (value bets) | api-football.com via Bet365 |

### 🏀 NBA Basketball
| Data | Source | Key needed? |
|---|---|---|
| Game schedule | BallDontLie `/v1/games` | ✅ Free |
| Team abbreviations | BallDontLie `/v1/teams` | ✅ Free |
| Season stats (PPG, Net Rating, W%) | NBA.com `leaguedashteamstats` | ❌ None |
| Fallback stats | Hardcoded 2024-25 standings | ❌ None |

NBA.com stats are fetched in one call for all 30 teams and cached. If NBA.com
times out, the bot instantly loads hardcoded mid-season standings — predictions
always work regardless.

### 🎾 Tennis
| Data | Source |
|---|---|
| Fixtures | api-tennis.com `get_fixtures` |
| Player rankings | Embedded ATP/WTA top-200 table (matched by last name) |
| Player stats (when available) | api-tennis.com `get_players` |

---

## 📊 Prediction Engines

### ⚽ Football — xG + Poisson Model

| Factor | Weight |
|---|---|
| Recent Form (last 5) | 30% |
| Home Advantage | 10% |
| Attack Strength (xG) | 20% |
| Defensive Strength | 15% |
| Head-to-Head | 15% |
| League Position proxy | 10% |

```
xG Home = home_attack × away_defense_weakness × league_avg_goals
xG Away = away_attack × home_defense_weakness × league_avg_goals
```

Predictions: Match Winner (1X2) · Over/Under 2.5 · BTTS · Correct Score · Value Bets

**League baselines:**
```
Premier League: 2.65  |  La Liga: 2.55  |  Serie A: 2.65
Bundesliga: 3.10      |  Ligue 1: 2.60  |  UCL: 2.75
```

### 🏀 NBA Basketball — Pace + Rating Model

```
Predicted Home Pts = ((home_off + away_def) / 2) × (pace / 100) + home_advantage
Win Probability    = blend(net_rating_diff, win_pct_differential) + home_boost
```

Predictions: Winner · Spread · Over/Under 220.5 · Predicted Score · Team Profile

### 🎾 Tennis — Ranking-Based Model

Win probability calculated from:
- ATP/WTA ranking difference (primary signal)
- Surface-specific win rate estimated from rank
- Serve win % estimated from rank tier
- H2H bonus when available

```
Confidence ceiling by tier:
  Grand Slam → 75%  |  Masters 1000 → 72%  |  ATP/WTA 500 → 70%
  ATP/WTA 250 → 68% |  Challenger → 65%
```

Predictions: Winner · Set Handicap · Over/Under Games · Predicted Sets

---

## 🔄 API Key Rotation

```
Request → Key #1 fails (403/429/timeout)?
  → Try Key #2 → Try Key #3 → ...
  → Success: use this key going forward
  → All fail: log warning, skip fixture gracefully
```

Add more keys — no code changes needed:
```env
FOOTBALL_API_KEYS=key1,key2,key3,key4,key5
```

---

## 📱 Telegram Message Flow

```
1. Bot header + date
2. Full matchday schedule (all games, no predictions yet)
3. "Predictions loading..."
4. Football summary card + individual match cards
5. NBA summary card + individual game cards
6. Tennis summary card + individual match cards
7. Final count summary
```

Telegram sender retries automatically on timeout (3 attempts, 5s delay).
Rate limits (429) are handled with proper wait times.

---

## ⏱️ Schedule

| Sport | Frequency | UTC times | WAT times |
|---|---|---|---|
| ⚽ Football | 2x/day | 08:00, 13:00 | 09:00, 14:00 |
| 🏀 NBA | Every 6h | 00:00, 06:00, 12:00, 18:00 | 01:00, 07:00, 13:00, 19:00 |
| 🎾 Tennis | Every 4h | 08:00, 12:00, 16:00, 20:00 | 09:00, 13:00, 17:00, 21:00 |

---

## 🎰 Value Bet Detection (Football)

```
Edge = Model Probability − Bookmaker Implied Probability
Alert threshold: Edge > 12%
```

Example:
```
Model: 65%  |  Odds: 2.20  →  Book: 45%  |  Edge: +20% ✅ VALUE BET
```

---

## 📈 Expected Accuracy

| Sport | Expected | Key signal |
|---|---|---|
| ⚽ Football | 55–63% | xG + Poisson |
| 🏀 Basketball | 60–68% | Net Rating + Win% |
| 🎾 Tennis | 62–72% | Player ranking + surface |

---

## ⚙️ Customisation

### Add Football Leagues
```python
# multi_sport_bot.py → FOOTBALL_LEAGUES
307: ("NPFL", 0),          # Nigerian Premier League (current year)
3:   ("Europa League", -1),
94:  ("Primeira Liga", -1), # Portugal
```

### Change Value Bet Threshold
```python
VALUE_BET_THRESHOLD = 0.10   # lower = more alerts
```

### Change NBA Over/Under Line
```python
# basketball_predictor.py
ou_line = 225.5
```

### Adjust Football Weights
```python
# predictor.py
W_FORM = 0.35
W_H2H  = 0.20
```

---

## ⚠️ Disclaimer

For educational and entertainment purposes only. Predictions are statistical
estimates and not financial advice. Always gamble responsibly.

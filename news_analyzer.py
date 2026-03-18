"""
Sports News Sentiment Analyser
Uses Google News RSS (free, no key) + FinBERT to analyse team/player news.

Sentiment score adjusts prediction confidence:
  - Strong negative news (injury, suspension, poor form) → lower win probability
  - Strong positive news (return from injury, winning streak) → higher win probability

Sentiment cache: results stored per team per day to avoid repeat fetches.
"""

import re
import time
import os
import requests
from datetime import datetime, timezone, timedelta
from urllib.parse import quote

try:
    import feedparser
    FEEDPARSER_AVAILABLE = True
except ImportError:
    FEEDPARSER_AVAILABLE = False
    print("[WARN] feedparser not installed. Run: pip install feedparser")

try:
    import torch
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    FINBERT_AVAILABLE = True
except ImportError:
    FINBERT_AVAILABLE = False
    print("[INFO] transformers/torch not installed — sentiment analysis disabled.")
    print("[INFO] Run: pip install transformers torch feedparser")


WAT_OFFSET = timezone(timedelta(hours=1))

# ── Cache to avoid fetching same team twice in one run ─────────────────────────
_sentiment_cache = {}   # {"team_name:date": sentiment_dict}
_finbert_loaded  = False
_finbert_failed  = False
_tokenizer       = None
_model           = None


def _load_finbert():
    """Load FinBERT model once and cache it."""
    global _finbert_loaded, _finbert_failed, _tokenizer, _model
    if _finbert_loaded:
        return True
    if _finbert_failed:
        return False
    if not FINBERT_AVAILABLE:
        return False
    try:
        # Respect any caller-provided HuggingFace cache directories (e.g. GitHub Actions cache).
        # If not set, default to a persistent location under the user's home.
        os.environ.setdefault("HF_HOME", os.path.join(os.path.expanduser("~"), ".cache", "huggingface"))
        os.environ.setdefault("TRANSFORMERS_CACHE", os.environ["HF_HOME"])
        print("[INFO] Loading FinBERT sentiment model (first run may take 30s)...")
        # Try slow tokenizer first (most robust). If it fails (some CI envs), fall back to fast.
        try:
            _tokenizer = AutoTokenizer.from_pretrained(
                "yiyanghkust/finbert-tone", use_fast=False
            )
        except Exception:
            try:
                _tokenizer = AutoTokenizer.from_pretrained(
                    "yiyanghkust/finbert-tone", use_fast=True
                )
            except Exception:
                # Last-resort: force a known slow tokenizer class to avoid any fast<->slow conversion.
                from transformers import BertTokenizer
                _tokenizer = BertTokenizer.from_pretrained("yiyanghkust/finbert-tone")
        _model = AutoModelForSequenceClassification.from_pretrained(
            "yiyanghkust/finbert-tone"
        )
        _model.eval()
        _finbert_loaded = True
        print("[INFO] FinBERT loaded ✅")
        return True
    except Exception as e:
        # Don't retry dozens of times per run; if load fails once, treat sentiment as disabled.
        _finbert_failed = True
        print(f"[WARN] FinBERT load failed (sentiment disabled for this run): {e}")
        return False


def _fetch_news_headlines(query, n=10):
    """Fetch headlines from Google News RSS for a given query."""
    if not FEEDPARSER_AVAILABLE:
        return []
    try:
        encoded = quote(query)
        url     = f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"
        feed    = feedparser.parse(url)
        titles  = [entry.title for entry in feed.entries[:n]]
        return titles
    except Exception as e:
        print(f"[WARN] News fetch failed for '{query}': {e}")
        return []


def _classify_headline(title):
    """Run FinBERT on a single headline. Returns (positive%, negative%, neutral%)."""
    if not _finbert_loaded:
        return 0.33, 0.33, 0.34
    try:
        import torch
        inputs = _tokenizer(
            title, return_tensors="pt",
            truncation=True, max_length=128
        )
        with torch.no_grad():
            outputs = _model(**inputs)
        probs = torch.softmax(outputs.logits, dim=1).numpy()[0]
        # FinBERT labels: [Positive, Negative, Neutral]
        return float(probs[0]), float(probs[1]), float(probs[2])
    except Exception:
        return 0.33, 0.33, 0.34


def _sentiment_score(headlines):
    """
    Returns a net sentiment score from -1.0 to +1.0.
      +1.0 = all positive news
      -1.0 = all negative news
       0.0 = neutral or no news
    """
    if not headlines:
        return 0.0
    total_pos = total_neg = 0.0
    for title in headlines:
        pos, neg, neu = _classify_headline(title)
        total_pos += pos
        total_neg += neg
    n = len(headlines)
    return round((total_pos - total_neg) / n, 3)


# ── Sport-specific query builders ─────────────────────────────────────────────
def _football_query(team_name):
    """Build a query that surfaces injury/form/lineup news."""
    return f"{team_name} injury lineup form football"


def _nba_query(team_name):
    """Build a query that surfaces injury/rest/trade news."""
    return f"{team_name} NBA injury out doubtful roster"


def _tennis_query(player_name):
    """Build a query that surfaces withdrawal/injury/form news."""
    # Use just last name for better results
    parts = player_name.strip().split()
    last  = parts[-1] if parts else player_name
    return f"{last} tennis injury withdraw form 2026"


# ── Public API ─────────────────────────────────────────────────────────────────
def get_team_sentiment(team_name, sport="football"):
    """
    Fetch and analyse news sentiment for a team/player.
    Returns dict with:
      score      : float -1.0 to +1.0
      headlines  : list of fetched headlines
      available  : bool (False if FinBERT not installed)
      summary    : human-readable string
    """
    today     = datetime.now(WAT_OFFSET).strftime("%Y-%m-%d")
    cache_key = f"{team_name}:{today}:{sport}"

    if cache_key in _sentiment_cache:
        return _sentiment_cache[cache_key]

    result = {
        "score":     0.0,
        "headlines": [],
        "available": False,
        "summary":   "Sentiment N/A",
    }

    # Allow disabling sentiment (useful on CI/GitHub Actions to avoid slow model downloads).
    try:
        if str(os.getenv("DISABLE_SENTIMENT", "")).strip().lower() in ("1", "true", "yes"):
            _sentiment_cache[cache_key] = result
            return result
    except Exception:
        pass

    if not _load_finbert():
        _sentiment_cache[cache_key] = result
        return result

    # Build query
    query_fns = {
        "football":   _football_query,
        "basketball": _nba_query,
        "tennis":     _tennis_query,
    }
    query     = query_fns.get(sport, _football_query)(team_name)
    headlines = _fetch_news_headlines(query, n=8)

    if not headlines:
        _sentiment_cache[cache_key] = result
        return result

    score = _sentiment_score(headlines)

    if score > 0.2:
        summary = f"Positive news ({score:+.2f})"
    elif score < -0.2:
        summary = f"Negative news ({score:+.2f})"
    else:
        summary = f"Neutral news ({score:+.2f})"

    result = {
        "score":     score,
        "headlines": headlines[:3],   # top 3 for display
        "available": True,
        "summary":   summary,
    }
    _sentiment_cache[cache_key] = result
    print(f"[SENTIMENT] {team_name}: {summary}")
    return result


def apply_sentiment_adjustment(prob, sentiment_score, weight=0.08):
    """
    Adjust a win probability based on sentiment score.
    weight=0.08 means sentiment can shift probability by up to ±8%.
    Returns adjusted probability clamped to [5, 95].
    """
    adjustment = sentiment_score * weight * 100   # e.g. -0.5 * 0.08 * 100 = -4%
    adjusted   = prob + adjustment
    return round(max(5, min(95, adjusted)))

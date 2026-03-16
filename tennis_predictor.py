"""
Tennis Prediction Engine v2
Uses player-specific metrics:
  - Surface win rate (hard / clay / grass / indoor)
  - Serve win % (1st serve %, ace rate)
  - Break points saved %
  - Head-to-head record
  - Recent match fatigue (days since last match)
  - Tournament tier weighting

Predictions:
  - Match winner with confidence %
  - Set handicap (e.g. -1.5 sets favourite)
  - Over/Under games (total games in match)
  - Predicted sets score
"""


# ─── SURFACE BASELINES ────────────────────────────────────────────────────────
# Avg serve hold % by surface (higher = serve dominates more)
SURFACE_SERVE_HOLD = {
    "hard":   0.72,
    "clay":   0.68,
    "grass":  0.77,
    "indoor": 0.74,
}

# Avg games per set by surface
SURFACE_AVG_GAMES_PER_SET = {
    "hard":   9.8,
    "clay":   10.2,
    "grass":  9.4,
    "indoor": 9.6,
}

# Surface detection keywords
SURFACE_MAP = {
    "clay":   ["clay", "roland", "french", "rome", "madrid", "monte", "barcelona", "hamburg", "munich"],
    "grass":  ["grass", "wimbledon", "halle", "queens", "eastbourne", "hertogenbosch", "nottingham"],
    "indoor": ["indoor", "paris", "rotterdam", "vienna", "marseille", "sofia", "montpellier", "dubai"],
}

# Tournament tier — affects confidence ceiling
TIER_CONFIDENCE_CEILING = {
    "grand_slam": 75,
    "masters":    72,
    "500":        70,
    "250":        68,
    "challenger": 65,
    "other":      60,
}


class TennisPredictor:

    def predict(self, fixture, home_stats=None, away_stats=None, h2h=None, sentiment_home=0.0, sentiment_away=0.0):
        """
        Predict tennis match outcome.
        home_stats / away_stats: dicts with keys:
            rank, serve_win_pct, first_serve_pct, break_pts_saved_pct,
            surface_win_pct, recent_wins, recent_losses, days_since_last_match
        h2h: {"home_wins": int, "away_wins": int, "total": int}
        """
        tournament = fixture.get("tournament", "")
        surface    = self._detect_surface(tournament)
        tier       = self._detect_tier(tournament)
        conf_ceil  = TIER_CONFIDENCE_CEILING.get(tier, 60)

        # ── Ranking-based adjustment (most reliable signal we have) ───────────
        home_rank = int((home_stats or {}).get("rank", 999))
        away_rank = int((away_stats or {}).get("rank", 999))
        rank_bonus = 0.0
        if home_rank < 999 and away_rank < 999 and home_rank != away_rank:
            # Lower rank = better player. Normalise difference into a score bonus.
            rank_diff  = away_rank - home_rank   # positive = home is better ranked
            rank_bonus = max(-0.5, min(0.5, rank_diff / 100))

        # ── Player strength scores ────────────────────────────────────────────
        home_score = self._player_score(home_stats, surface, is_home=True)
        away_score = self._player_score(away_stats, surface, is_home=False)

        # ── Adjust for news sentiment ─────────────────────────────────────────
        home_score += sentiment_home * 0.2
        away_score += sentiment_away * 0.2

        # Apply ranking bonus
        home_score += rank_bonus
        away_score -= rank_bonus

        # ── H2H adjustment ────────────────────────────────────────────────────
        h2h_bonus = self._h2h_bonus(h2h)
        home_score += h2h_bonus
        away_score -= h2h_bonus

        # ── Win probability ───────────────────────────────────────────────────
        total     = home_score + away_score
        prob_home = round((home_score / total) * 100) if total > 0 else 52
        prob_away = 100 - prob_home

        # Boost confidence ceiling when we have real ranking data
        has_real_data = home_rank < 999 and away_rank < 999
        if has_real_data:
            conf_ceil = min(conf_ceil + 10, 82)   # real data = higher ceiling

        confidence = max(prob_home, prob_away)
        confidence = min(confidence, conf_ceil)

        winner        = "home" if prob_home > prob_away else "away"
        winner_label  = fixture["home_team"] if winner == "home" else fixture["away_team"]
        loser_label   = fixture["away_team"] if winner == "home" else fixture["home_team"]

        # ── Confidence grade ──────────────────────────────────────────────────
        if confidence >= 68:
            grade = "HIGH 🔥"
        elif confidence >= 55:
            grade = "MEDIUM ⚡"
        else:
            grade = "LOW 🌡️"

        # ── Match format ──────────────────────────────────────────────────────
        is_bo5      = self._is_best_of_5(tournament)
        sets_format = "Best of 5" if is_bo5 else "Best of 3"
        max_sets    = 5 if is_bo5 else 3

        # ── Predicted sets score ──────────────────────────────────────────────
        pred_sets = self._predict_sets(confidence, max_sets)

        # ── Set handicap ──────────────────────────────────────────────────────
        # If confidence >= 65, favourite is -1.5 sets (wins in straight sets)
        # If 55-65, favourite is -1.5 with lower confidence
        if confidence >= 65:
            handicap = f"{winner_label} -1.5 sets"
        elif confidence >= 55:
            handicap = f"{winner_label} -1.5 sets (lower confidence)"
        else:
            handicap = "Too close — avoid set handicap"

        # ── Over/Under games ──────────────────────────────────────────────────
        avg_gpset  = SURFACE_AVG_GAMES_PER_SET.get(surface, 9.8)
        pred_sets_total = int(pred_sets.split("-")[0]) + int(pred_sets.split("-")[1])
        pred_games = round(avg_gpset * pred_sets_total)

        # OU line: Best of 3 ≈ 21.5 games, Best of 5 ≈ 38.5 games
        ou_line    = 38.5 if is_bo5 else 21.5
        over_under = f"{'Over' if pred_games >= ou_line else 'Under'} {ou_line} games"

        # ── Serve profile ─────────────────────────────────────────────────────
        home_serve = self._serve_label(home_stats, surface)
        away_serve = self._serve_label(away_stats, surface)

        # ── Key factor ────────────────────────────────────────────────────────
        key_factor = self._key_factor(
            home_stats, away_stats, h2h, h2h_bonus, surface, fixture
        )

        return {
            "skip":         False,
            "winner":       winner,
            "winner_label": winner_label,
            "loser_label":  loser_label,
            "confidence":   confidence,
            "grade":        grade,
            "prob_home":    prob_home,
            "prob_away":    prob_away,
            "rank_home":    home_rank,
            "rank_away":    away_rank,
            "surface":      surface.capitalize(),
            "tier":         tier,
            "sets_format":  sets_format,
            "pred_sets":    pred_sets,
            "handicap":     handicap,
            "over_under":   over_under,
            "pred_games":   pred_games,
            "ou_line":      ou_line,
            "home_serve":   home_serve,
            "away_serve":   away_serve,
            "tournament":   tournament,
            "key_factor":   key_factor,
        }

    # ─── PLAYER STRENGTH ──────────────────────────────────────────────────────
    def _player_score(self, stats, surface, is_home):
        score = 1.0   # baseline

        if not stats:
            return score

        # Serve win % on this surface (most important in tennis)
        serve_base = SURFACE_SERVE_HOLD.get(surface, 0.72)
        serve_pct  = self._sf(stats, "serve_win_pct", serve_base)
        score     += (serve_pct - serve_base) * 3.0

        # 1st serve % (consistency)
        first_srv  = self._sf(stats, "first_serve_pct", 0.62)
        score     += (first_srv - 0.62) * 1.5

        # Break points saved % (clutch under pressure)
        bp_saved   = self._sf(stats, "break_pts_saved_pct", 0.63)
        score     += (bp_saved - 0.63) * 2.0

        # Surface-specific win rate
        surf_win   = self._sf(stats, "surface_win_pct", 0.50)
        score     += (surf_win - 0.50) * 2.5

        # Recent form (wins - losses in last 5)
        recent_w   = self._sf(stats, "recent_wins",   2.5)
        recent_l   = self._sf(stats, "recent_losses", 2.5)
        form_score = (recent_w - recent_l) / max(recent_w + recent_l, 1)
        score     += form_score * 0.4

        # Fatigue penalty (matches played in last 3 days)
        days_rest  = self._sf(stats, "days_since_last_match", 2)
        if days_rest < 1:
            score -= 0.2    # played yesterday — fatigue
        elif days_rest >= 2:
            score += 0.05   # well rested

        return max(score, 0.3)

    # ─── H2H BONUS ────────────────────────────────────────────────────────────
    def _h2h_bonus(self, h2h):
        if not h2h:
            return 0.0
        hw    = h2h.get("home_wins", 0)
        aw    = h2h.get("away_wins", 0)
        total = h2h.get("total", hw + aw)
        if total == 0:
            return 0.0
        # Returns -0.3 to +0.3
        return ((hw - aw) / total) * 0.3

    # ─── PREDICTED SETS SCORE ─────────────────────────────────────────────────
    def _predict_sets(self, confidence, max_sets):
        if max_sets == 5:
            if confidence >= 70: return "3-0"
            elif confidence >= 62: return "3-1"
            else: return "3-2"
        else:
            if confidence >= 68: return "2-0"
            else: return "2-1"

    # ─── SERVE LABEL ──────────────────────────────────────────────────────────
    def _serve_label(self, stats, surface):
        if not stats:
            return "N/A"
        base = SURFACE_SERVE_HOLD.get(surface, 0.72)
        pct  = self._sf(stats, "serve_win_pct", base)
        if pct >= base + 0.05:
            return f"Strong ({round(pct*100)}%)"
        elif pct <= base - 0.05:
            return f"Weak ({round(pct*100)}%)"
        else:
            return f"Average ({round(pct*100)}%)"

    # ─── SURFACE DETECTION ────────────────────────────────────────────────────
    def _detect_surface(self, tournament):
        t = tournament.lower()
        for surface, keywords in SURFACE_MAP.items():
            if any(kw in t for kw in keywords):
                return surface
        return "hard"   # default — most tournaments are hard court

    # ─── TIER DETECTION ───────────────────────────────────────────────────────
    def _detect_tier(self, tournament):
        t = tournament.lower()
        if any(x in t for x in ["grand slam", "wimbledon", "us open", "australian", "french"]):
            return "grand_slam"
        elif any(x in t for x in ["1000", "masters", "miami", "indian wells", "rome", "madrid",
                                   "montreal", "toronto", "cincinnati", "shanghai", "paris", "canada"]):
            return "masters"
        elif "500" in t:
            return "500"
        elif "250" in t or "atp" in t or "wta" in t:
            return "250"
        elif "challenger" in t:
            return "challenger"
        return "other"

    # ─── IS BEST OF 5 ─────────────────────────────────────────────────────────
    def _is_best_of_5(self, tournament):
        t = tournament.lower()
        return any(x in t for x in ["grand slam", "wimbledon", "us open", "australian", "french",
                                     "davis cup", "atp finals"])

    # ─── KEY FACTOR ───────────────────────────────────────────────────────────
    def _key_factor(self, home_s, away_s, h2h, h2h_bonus, surface, fix):
        if abs(h2h_bonus) > 0.15:
            dom = fix["home_team"] if h2h_bonus > 0 else fix["away_team"]
            hw  = (h2h or {}).get("home_wins" if h2h_bonus > 0 else "away_wins", "?")
            return f"{dom} leads the H2H record ({hw} wins)"
        home_srv = self._sf(home_s, "serve_win_pct", SURFACE_SERVE_HOLD.get(surface, 0.72))
        away_srv = self._sf(away_s, "serve_win_pct", SURFACE_SERVE_HOLD.get(surface, 0.72))
        if home_srv > away_srv + 0.08:
            return f"{fix['home_team']} has a dominant serve advantage on {surface}"
        elif away_srv > home_srv + 0.08:
            return f"{fix['away_team']} wins more service games on {surface}"
        surf_note = {
            "clay":   "Clay rewards consistency and baseline stamina",
            "grass":  "Grass favours big servers — expect fewer breaks",
            "indoor": "Indoor hard court slightly boosts serving",
            "hard":   "Hard court is balanced — form is the deciding factor",
        }
        return surf_note.get(surface, "Form and fitness will decide this match")

    def _sf(self, stats, key, default):
        if not stats:
            return default
        try:
            return float(stats.get(key, default) or default)
        except (TypeError, ValueError):
            return default
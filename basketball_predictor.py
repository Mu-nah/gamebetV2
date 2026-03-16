"""
Basketball Prediction Engine v2
Uses NBA-specific metrics:
  - Offensive Rating (points scored per 100 possessions)
  - Defensive Rating (points allowed per 100 possessions)
  - Pace (possessions per 48 mins)
  - Recent scoring trend (last 5 games)
  - Home court advantage
  - Net Rating differential

Predictions:
  - Match winner with confidence %
  - Spread (predicted margin)
  - Game total Over/Under
  - Predicted score
"""


# ─── NBA LEAGUE BASELINES (2024-25 season averages) ───────────────────────────
NBA_AVG_PACE        = 98.5     # possessions per 48 mins
NBA_AVG_OFF_RATING  = 113.5    # points per 100 possessions
NBA_AVG_DEF_RATING  = 113.5
NBA_AVG_PPG         = 113.0    # points per game
NBA_HOME_ADVANTAGE  = 3.2      # home teams score ~3.2 more pts on average
NBA_OU_LINE         = 220.5    # default over/under line


class BasketballPredictor:

    HIGH_CONFIDENCE   = 70
    MEDIUM_CONFIDENCE = 50

    def predict(self, fixture, home_stats=None, away_stats=None, api_win_prob=None, sentiment_home=0.0, sentiment_away=0.0):
        """
        Predict NBA game.
        home_stats / away_stats: dicts with keys:
            off_rating, def_rating, pace, ppg, opp_ppg, last5_scores, net_rating
        api_win_prob: {"home": float, "away": float} 0-100, optional
        """

        # ── Pull stats with safe fallbacks ────────────────────────────────────
        home_off  = self._s(home_stats, "off_rating",  NBA_AVG_OFF_RATING)
        home_def  = self._s(home_stats, "def_rating",  NBA_AVG_DEF_RATING)
        home_pace = self._s(home_stats, "pace",         NBA_AVG_PACE)
        home_ppg  = self._s(home_stats, "ppg",          NBA_AVG_PPG)
        home_net  = self._s(home_stats, "net_rating",   0.0)
        home_trnd = self._s(home_stats, "recent_trend", 0.0)  # +ve = improving

        away_off  = self._s(away_stats, "off_rating",  NBA_AVG_OFF_RATING)
        away_def  = self._s(away_stats, "def_rating",  NBA_AVG_DEF_RATING)
        away_pace = self._s(away_stats, "pace",         NBA_AVG_PACE)
        away_ppg  = self._s(away_stats, "ppg",          NBA_AVG_PPG)
        away_net  = self._s(away_stats, "net_rating",   0.0)
        away_trnd = self._s(away_stats, "recent_trend", 0.0)

        # ── Predicted score using pace + rating model ─────────────────────────
        # Expected pace for this game = average of both teams
        game_pace = (home_pace + away_pace) / 2

        # Home points = (home off rating vs away def rating) * pace / 100 + home edge
        home_pts_raw = ((home_off + away_def) / 2) * (game_pace / 100) + NBA_HOME_ADVANTAGE / 2
        away_pts_raw = ((away_off + home_def) / 2) * (game_pace / 100) - NBA_HOME_ADVANTAGE / 2

        # Apply recent scoring trend (small weight)
        home_pts_raw += home_trnd * 0.3
        away_pts_raw += away_trnd * 0.3

        pred_home_pts = round(home_pts_raw)
        pred_away_pts = round(away_pts_raw)
        pred_total    = pred_home_pts + pred_away_pts

        # ── Win probability ───────────────────────────────────────────────────
        if api_win_prob:
            prob_home = round(api_win_prob.get("home", 50))
            prob_away = 100 - prob_home
        else:
            # Use net rating differential + home court advantage
            # net_diff of +10 pts ≈ +20% win probability boost
            net_diff  = (home_net - away_net) + NBA_HOME_ADVANTAGE
            raw_prob  = 50 + (net_diff * 2.0)

            # Blend with win percentage if available (more reliable signal)
            home_wpc = self._s(home_stats, "win_pct", 0.5)
            away_wpc = self._s(away_stats, "win_pct", 0.5)
            if home_wpc != 0.5 or away_wpc != 0.5:
                # Normalise win pcts + home boost
                total_wpc  = home_wpc + away_wpc
                wpc_prob   = (home_wpc / total_wpc * 100) + (NBA_HOME_ADVANTAGE * 1.5)
                raw_prob   = (raw_prob + wpc_prob) / 2   # blend both signals

            prob_home = round(max(25, min(82, raw_prob)))
            prob_away = 100 - prob_home

        # ── Adjust for news sentiment ─────────────────────────────────────────
        sentiment_diff = sentiment_home - sentiment_away
        prob_home += sentiment_diff * 5  # Up to 5% adjustment
        prob_home = max(25, min(82, prob_home))
        prob_away = 100 - prob_home

        winner        = "home" if prob_home > prob_away else "away"
        winner_label  = fixture["home_team"] if winner == "home" else fixture["away_team"]
        confidence    = max(prob_home, prob_away)
        confidence    = min(confidence, 92)

        # ── Confidence grade ──────────────────────────────────────────────────
        if confidence >= self.HIGH_CONFIDENCE:
            grade = "HIGH 🔥"
        elif confidence >= self.MEDIUM_CONFIDENCE:
            grade = "MEDIUM ⚡"
        else:
            grade = "LOW 🌡️"

        # ── Spread ────────────────────────────────────────────────────────────
        spread      = pred_home_pts - pred_away_pts
        spread_pick = fixture["home_team"] if spread > 0 else fixture["away_team"]
        spread_val  = abs(spread)
        spread_label = f"{spread_pick} -{spread_val}" if spread_val > 0 else "Pick'em"

        # ── Over/Under ────────────────────────────────────────────────────────
        ou_line    = NBA_OU_LINE
        over_under = f"{'Over' if pred_total >= ou_line else 'Under'} {ou_line}"
        ou_margin  = abs(pred_total - ou_line)

        # ── Offensive / Defensive summary ─────────────────────────────────────
        home_profile = self._team_profile(home_off, home_def, NBA_AVG_OFF_RATING, NBA_AVG_DEF_RATING)
        away_profile = self._team_profile(away_off, away_def, NBA_AVG_OFF_RATING, NBA_AVG_DEF_RATING)

        # ── Key factor ────────────────────────────────────────────────────────
        key_factor = self._key_factor(
            home_net, away_net, home_pace, away_pace,
            home_off, away_def, fixture
        )

        return {
            "skip":          False,
            "winner":        winner,
            "winner_label":  winner_label,
            "confidence":    confidence,
            "grade":         grade,
            "prob_home":     prob_home,
            "prob_away":     prob_away,
            "pred_score":    f"{pred_home_pts} - {pred_away_pts}",
            "pred_total":    pred_total,
            "over_under":    over_under,
            "ou_margin":     round(ou_margin, 1),
            "spread":        spread_label,
            "home_profile":  home_profile,
            "away_profile":  away_profile,
            "game_pace":     round(game_pace, 1),
            "key_factor":    key_factor,
        }

    # ─── TEAM PROFILE LABEL ───────────────────────────────────────────────────
    def _team_profile(self, off, def_, avg_off, avg_def):
        """Classify team as Offensive / Defensive / Balanced / Weak."""
        off_above = off  > avg_off + 3
        def_above = def_ < avg_def - 3   # lower def rating = better defense
        if off_above and def_above:
            return "Elite 🌟"
        elif off_above:
            return "Offensive 🔥"
        elif def_above:
            return "Defensive 🛡️"
        elif off < avg_off - 3 and def_ > avg_def + 3:
            return "Struggling 📉"
        else:
            return "Balanced ⚖️"

    # ─── KEY FACTOR ───────────────────────────────────────────────────────────
    def _key_factor(self, home_net, away_net, home_pace, away_pace, home_off, away_def, fix):
        net_gap   = abs(home_net - away_net)
        pace_gap  = abs(home_pace - away_pace)
        if net_gap > 8:
            fav = fix["home_team"] if home_net > away_net else fix["away_team"]
            return f"{fav} has a dominant net rating advantage (+{round(net_gap, 1)})"
        elif pace_gap > 5:
            faster = fix["home_team"] if home_pace > away_pace else fix["away_team"]
            return f"Pace mismatch — {faster} pushes tempo, expect a high-scoring game"
        elif home_off > 118:
            return f"{fix['home_team']} is one of the league's elite offences this season"
        elif away_def < 108:
            return f"{fix['away_team']} brings elite defense on the road"
        else:
            return "Competitive matchup — form and momentum will decide this"

    def _s(self, stats, key, default):
        if not stats:
            return default
        try:
            return float(stats.get(key, default) or default)
        except (TypeError, ValueError):
            return default
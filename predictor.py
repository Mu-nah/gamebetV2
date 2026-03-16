"""
Football Prediction Engine v2
Upgrades:
  - Expected Goals (xG) model using attack/defense/league-avg formula
  - Confidence grading: HIGH / MEDIUM / LOW
  - Match filtering: min 10 games played, top leagues only
  - Value bet detection: model prob vs bookmaker implied prob
  - Poisson-based correct score distribution
"""

import math


# ─── LEAGUE AVERAGE GOALS (historical baselines) ──────────────────────────────
LEAGUE_AVG_GOALS = {
    "Premier League":        2.65,
    "La Liga":               2.55,
    "Serie A":               2.65,
    "Bundesliga":            3.10,
    "Ligue 1":               2.60,
    "UEFA Champions League": 2.75,
    "default":               2.60,
}

MIN_MATCHES_PLAYED   = 10
VALUE_BET_THRESHOLD  = 0.12   # 12% edge required


class FootballPredictor:

    W_FORM     = 0.30
    W_HOME_ADV = 0.10
    W_ATTACK   = 0.20
    W_DEFENSE  = 0.15
    W_H2H      = 0.15
    W_POS      = 0.10

    def predict(self, fixture, home_stats, away_stats, h2h, odds=None, sentiment_home=0.0, sentiment_away=0.0):
        # ── Filter: skip if too few games played ──────────────────────────────
        home_played = self._games_played(home_stats)
        away_played = self._games_played(away_stats)
        if home_played < MIN_MATCHES_PLAYED or away_played < MIN_MATCHES_PLAYED:
            return {
                "skip": True,
                "reason": f"Not enough data — {home_played}/{away_played} games played (min {MIN_MATCHES_PLAYED})"
            }

        # ── Team strength ─────────────────────────────────────────────────────
        home_score = self._team_strength(home_stats, is_home=True)
        away_score = self._team_strength(away_stats, is_home=False)
        
        # ── Adjust for news sentiment ─────────────────────────────────────────
        home_score += sentiment_home * 0.2  # Positive news boosts home advantage
        away_score += sentiment_away * 0.2
        h2h_adj    = self._h2h_score(h2h, fixture["home_id"])
        home_score += h2h_adj * self.W_H2H
        away_score -= h2h_adj * self.W_H2H

        # ── 1X2 probabilities ─────────────────────────────────────────────────
        total     = home_score + away_score + 1.0
        prob_home = round((home_score / total) * 100)
        prob_away = round((away_score / total) * 100)
        prob_draw = max(5, 100 - prob_home - prob_away)
        prob_draw += 100 - (prob_home + prob_away + prob_draw)   # rebalance

        if prob_home > prob_away and prob_home > prob_draw:
            winner, winner_label, confidence = "home", f"{fixture['home_team']} Win", prob_home
        elif prob_away > prob_home and prob_away > prob_draw:
            winner, winner_label, confidence = "away", f"{fixture['away_team']} Win", prob_away
        else:
            winner, winner_label, confidence = "draw", "Draw", prob_draw

        confidence = min(confidence, 90)

        # ── Confidence grade ──────────────────────────────────────────────────
        if confidence >= 70:
            grade = "HIGH 🔥"
        elif confidence >= 50:
            grade = "MEDIUM ⚡"
        else:
            grade = "LOW 🌡️"

        # ── xG model ──────────────────────────────────────────────────────────
        league_name = fixture.get("league", "default")
        xg_home, xg_away = self._expected_goals(home_stats, away_stats, league_name)

        # ── Over/Under via Poisson ────────────────────────────────────────────
        over_prob      = self._over_probability(xg_home, xg_away, line=2.5)
        over_under     = "Over 2.5" if over_prob >= 50 else "Under 2.5"
        ou_prob        = over_prob if over_prob >= 50 else 100 - over_prob

        # ── BTTS via Poisson ──────────────────────────────────────────────────
        btts_prob = self._btts_prob_poisson(xg_home, xg_away)
        btts      = "Yes" if btts_prob >= 50 else "No"

        # ── Correct score ─────────────────────────────────────────────────────
        correct_score, score_prob = self._best_correct_score(xg_home, xg_away)

        # ── Value bet detection ───────────────────────────────────────────────
        value_bets = []
        if odds:
            model_probs = {
                "home": prob_home / 100,
                "draw": prob_draw / 100,
                "away": prob_away / 100
            }
            labels = {
                "home": f"{fixture['home_team']} Win",
                "draw": "Draw",
                "away": f"{fixture['away_team']} Win"
            }
            for outcome, decimal_odd in odds.items():
                if decimal_odd and float(decimal_odd) > 1.0:
                    book_prob  = round(1 / float(decimal_odd), 4)
                    model_prob = model_probs.get(outcome, 0)
                    value      = round(model_prob - book_prob, 4)
                    if value >= VALUE_BET_THRESHOLD:
                        value_bets.append({
                            "outcome":    labels.get(outcome, outcome),
                            "odd":        decimal_odd,
                            "book_prob":  round(book_prob * 100),
                            "model_prob": round(model_prob * 100),
                            "value":      round(value * 100),
                        })

        return {
            "skip":           False,
            "winner":         winner,
            "winner_label":   winner_label,
            "confidence":     confidence,
            "grade":          grade,
            "prob_home":      prob_home,
            "prob_draw":      prob_draw,
            "prob_away":      prob_away,
            "xg_home":        xg_home,
            "xg_away":        xg_away,
            "expected_goals": f"{xg_home} - {xg_away}",
            "over_under":     over_under,
            "ou_prob":        ou_prob,
            "btts":           btts,
            "btts_prob":      btts_prob,
            "correct_score":  correct_score,
            "score_prob":     score_prob,
            "home_form":      self._form_string(home_stats),
            "away_form":      self._form_string(away_stats),
            "key_factor":     self._key_factor(home_score, away_score, h2h_adj, xg_home, xg_away, fixture),
            "value_bets":     value_bets,
            "home_played":    home_played,
            "away_played":    away_played,
        }

    # ─── xG MODEL ─────────────────────────────────────────────────────────────
    def _expected_goals(self, home_stats, away_stats, league_name):
        lg = LEAGUE_AVG_GOALS.get(league_name, LEAGUE_AVG_GOALS["default"])
        half = lg / 2

        home_scored   = self._safe_avg(home_stats, "goals", "for",     "average", "home", default=half)
        home_conceded = self._safe_avg(home_stats, "goals", "against", "average", "home", default=half)
        away_scored   = self._safe_avg(away_stats, "goals", "for",     "average", "away", default=half)
        away_conceded = self._safe_avg(away_stats, "goals", "against", "average", "away", default=half)

        home_attack  = home_scored   / half
        home_defense = home_conceded / half
        away_attack  = away_scored   / half
        away_defense = away_conceded / half

        xg_home = round(max(0.3, min(home_attack * away_defense * half, 4.5)), 2)
        xg_away = round(max(0.3, min(away_attack * home_defense * half, 4.5)), 2)
        return xg_home, xg_away

    # ─── POISSON ──────────────────────────────────────────────────────────────
    def _poisson_prob(self, lam, k):
        try:
            return (math.exp(-lam) * (lam ** k)) / math.factorial(k)
        except (OverflowError, ValueError):
            return 0.0

    def _over_probability(self, xg_home, xg_away, line=2.5):
        under_prob = 0.0
        cap = int(line)
        for h in range(cap + 1):
            for a in range(cap + 1 - h):
                under_prob += self._poisson_prob(xg_home, h) * self._poisson_prob(xg_away, a)
        return round((1 - under_prob) * 100)

    def _btts_prob_poisson(self, xg_home, xg_away):
        p_home = 1 - self._poisson_prob(xg_home, 0)
        p_away = 1 - self._poisson_prob(xg_away, 0)
        return round(p_home * p_away * 100)

    def _best_correct_score(self, xg_home, xg_away, max_g=5):
        best_p, best_s = 0.0, "1 - 1"
        for h in range(max_g + 1):
            for a in range(max_g + 1):
                p = self._poisson_prob(xg_home, h) * self._poisson_prob(xg_away, a)
                if p > best_p:
                    best_p, best_s = p, f"{h} - {a}"
        return best_s, round(best_p * 100, 1)

    # ─── TEAM STRENGTH ────────────────────────────────────────────────────────
    def _team_strength(self, stats, is_home):
        if not stats:
            return 1.0 + (0.15 if is_home else 0.0)
        score      = 0.0
        form_str   = stats.get("form", "")[-5:] if stats.get("form") else ""
        form_score = sum({"W": 3, "D": 1, "L": 0}.get(c, 1) for c in form_str)
        score     += (form_score / 15) * self.W_FORM
        if is_home:
            score += self.W_HOME_ADV
        venue         = "home" if is_home else "away"
        avg_scored    = self._safe_avg(stats, "goals", "for",     "average", venue, default=1.2)
        avg_conceded  = self._safe_avg(stats, "goals", "against", "average", venue, default=1.2)
        score        += min(avg_scored / 3.0, 1.0) * self.W_ATTACK
        score        += (1.0 - min(avg_conceded / 3.0, 1.0)) * self.W_DEFENSE
        return max(score, 0.1)

    def _h2h_score(self, h2h, home_team_id):
        if not h2h:
            return 0.0
        hw = aw = 0
        for m in h2h[-5:]:
            if m.get("score", {}).get("fulltime"):
                ht  = m["teams"]["home"]["id"]
                h_w = m["teams"]["home"].get("winner")
                a_w = m["teams"]["away"].get("winner")
                if ht == home_team_id:
                    if h_w: hw += 1
                    elif a_w: aw += 1
                else:
                    if a_w: hw += 1
                    elif h_w: aw += 1
        total = hw + aw
        return 0.0 if total == 0 else (hw - aw) / total

    def _games_played(self, stats):
        try:
            return int(stats.get("fixtures", {}).get("played", {}).get("total", 0))
        except (AttributeError, TypeError, ValueError):
            return 0

    def _form_string(self, stats):
        if not stats:
            return "N/A"
        form = stats.get("form", "")[-5:]
        if not form:
            return "N/A"
        icons = {"W": "✅", "D": "🟡", "L": "❌"}
        return " ".join(icons.get(c, "⬜") for c in form)

    def _key_factor(self, hs, as_, h2h_adj, xg_h, xg_a, fix):
        if abs(hs - as_) > 0.3:
            s = fix["home_team"] if hs > as_ else fix["away_team"]
            return f"{s} holds a clear statistical advantage"
        elif abs(h2h_adj) > 0.4:
            d = fix["home_team"] if h2h_adj > 0 else fix["away_team"]
            return f"{d} dominates the head-to-head record"
        elif xg_h > 2.0:
            return f"{fix['home_team']} projects as a high-scoring home side (xG {xg_h})"
        elif xg_a > 1.8:
            return f"{fix['away_team']} travels in potent attacking form (xG {xg_a})"
        return "Closely matched — expect a tight, competitive game"

    def _safe_avg(self, stats, *keys, default=1.0):
        try:
            val = stats
            for k in keys:
                val = val[k]
            return float(val) if val else default
        except (KeyError, TypeError, ValueError):
            return default

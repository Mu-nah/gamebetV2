"""
Unit tests for the pure logic in predictor.py — no network, no browser.
Run:  pytest -q
"""
import json
from datetime import datetime, timedelta

import pytest

import predictor as p


# ──────────────────────────────────────────────────────────────
# is_slam_qualifying  /  tournament_tier
# ──────────────────────────────────────────────────────────────
@pytest.mark.parametrize("name,rnd,expected", [
    ("US Open - Qualification", "",     True),
    ("Wimbledon Qualifying",    "",     True),
    ("Australian Open",         "qual.",True),
    ("French Open",             "Q1",   True),
    ("US Open",                 "Q3",   True),
    ("US Open",                 "q",    True),
    ("US Open",                 "q-1",  True),
    ("US Open",                 "1q",   True),
    ("US Open",                 "R32",  False),   # main draw
    ("US Open",                 "QF",   False),   # quarter-final, not qualifying
    ("Wimbledon",               "1R",   False),
    ("WTA Cincinnati",          "Q1",   False),   # not a slam
    ("",                        "",     False),
])
def test_is_slam_qualifying(name, rnd, expected):
    assert p.is_slam_qualifying(name, rnd) is expected


@pytest.mark.parametrize("name,rnd,tier", [
    ("US Open - Qualification", "",    "slam_qual"),
    ("Roland Garros",           "R64", "slam_main"),
    ("Wimbledon",               "",    "slam_main"),
    ("Iasi WTA 125",            "",    "wta125"),
    ("WTA Cincinnati",          "SF",  "wta_tour"),
    ("Some Random 250",         "",    "wta_tour"),
])
def test_tournament_tier(name, rnd, tier):
    assert p.tournament_tier(name, rnd) == tier


# ──────────────────────────────────────────────────────────────
# _pair_key  /  _dedupe_by_pair
# ──────────────────────────────────────────────────────────────
def test_pair_key_is_order_independent():
    assert p._pair_key("Vekic D.", "Cengiz B.") == p._pair_key("Cengiz B.", "vekic d.")


def test_dedupe_by_pair_keeps_last_copy():
    rows = [
        {"p1": "A B", "p2": "C D", "date": "2026-05-05", "result": "correct",
         "pair": p._pair_key("A B", "C D")},
        {"p1": "C D", "p2": "A B", "date": "2026-05-06", "result": "wrong",
         "pair": p._pair_key("C D", "A B")},          # same match, resent
        {"p1": "E F", "p2": "G H", "date": "2026-05-06", "result": "correct",
         "pair": p._pair_key("E F", "G H")},
    ]
    out = p._dedupe_by_pair(rows)
    assert len(out) == 2
    ab = [o for o in out if o["pair"] == p._pair_key("A B", "C D")][0]
    assert ab["result"] == "wrong"          # last copy wins


@pytest.mark.parametrize("r1,r2,e1,e2,expected", [
    (10, 100, 1500, 1800, (True, "p1", "p2")),    # disagree, both gaps large
    (100, 10, 1800, 1500, (True, "p2", "p1")),    # mirror
    (10, 100, 1800, 1500, (False, "p1", "p1")),   # agree
    (10, 100, 1550, 1500, (False, "p1", "p1")),   # elo gap < 200
    (10, 40, 1500, 1800, (False, "p1", "p2")),    # rank gap < 50
])
def test_rank_elo_conflict(r1, r2, e1, e2, expected):
    assert p.rank_elo_conflict(r1, r2, e1, e2) == expected


def test_dedupe_by_pair_falls_back_when_pair_missing():
    rows = [
        {"p1": "A B", "p2": "C D", "result": "correct"},
        {"p1": "c d", "p2": "a b", "result": "wrong"},
    ]
    assert len(p._dedupe_by_pair(rows)) == 1


# ──────────────────────────────────────────────────────────────
# _resolve_outcomes  — the self-healing catch-up window
# ──────────────────────────────────────────────────────────────
def _write_outcomes(path, rows):
    path.write_text(json.dumps(rows))


@pytest.fixture
def outcomes_file(tmp_path, monkeypatch):
    f = tmp_path / "outcomes.json"
    monkeypatch.setattr(p, "_OUTCOMES_FILE", str(f))
    return f


def _day(offset):
    return (datetime.now(p.WAT) - timedelta(days=offset)).strftime("%Y-%m-%d")


def test_resolve_outcomes_marks_correct_and_wrong(outcomes_file):
    _write_outcomes(outcomes_file, [
        {"p1": "Vekic D.", "p2": "Cengiz B.", "winner_pred": "Vekic D.",
         "date": _day(2), "result": None},
        {"p1": "Ito A.", "p2": "Selekhmeteva O.", "winner_pred": "Ito A.",
         "date": _day(3), "result": None},
    ])
    results = [
        ("vekic-donna",   "cengiz-basak",        "hard"),   # Vekic won -> correct
        ("selekhmeteva-o", "ito-aoi",            "clay"),   # Ito lost  -> wrong
    ]
    p._resolve_outcomes(results)

    saved = json.loads(outcomes_file.read_text())
    assert saved[0]["result"] == "correct"
    assert saved[1]["result"] == "wrong"


def test_resolve_outcomes_ignores_rows_outside_10_day_window(outcomes_file):
    _write_outcomes(outcomes_file, [
        {"p1": "Vekic D.", "p2": "Cengiz B.", "winner_pred": "Vekic D.",
         "date": _day(30), "result": None},
    ])
    p._resolve_outcomes([("vekic-donna", "cengiz-basak", "hard")])
    assert json.loads(outcomes_file.read_text())[0]["result"] is None


def test_resolve_outcomes_requires_both_players_to_match(outcomes_file):
    # only one player of the pending pick is in the result pair -> no false resolve
    _write_outcomes(outcomes_file, [
        {"p1": "Ma Y.", "p2": "Ngounoue C.", "winner_pred": "Ma Y.",
         "date": _day(1), "result": None},
    ])
    p._resolve_outcomes([("ma-yexin", "someone-else", "hard")])
    assert json.loads(outcomes_file.read_text())[0]["result"] is None


def test_calibration_reports_rank_vs_elo_split(outcomes_file, capsys):
    base = {"grade": "HIGH", "surface": "hard", "tier": "wta_tour", "prob": 0.65}
    _write_outcomes(outcomes_file, [
        {**base, "p1": "A", "p2": "B", "pair": "a|b", "result": "wrong",
         "rank_elo_conflict": True, "conflict_sided_with": "rank"},
        {**base, "p1": "C", "p2": "D", "pair": "c|d", "result": "correct",
         "rank_elo_conflict": True, "conflict_sided_with": "elo"},
        {**base, "p1": "E", "p2": "F", "pair": "e|f", "result": "correct",
         "rank_elo_conflict": True, "conflict_sided_with": "elo"},
        {**base, "p1": "G", "p2": "H", "pair": "g|h", "result": "correct",
         "rank_elo_conflict": False, "conflict_sided_with": None},
    ])
    p.run_calibration()
    out = capsys.readouterr().out
    assert "rank vs elo" in out
    assert "sided w/ rank: 0.0%  n=1" in out
    assert "sided w/ elo : 100.0%  n=2" in out


def test_resolve_outcomes_no_pending_is_noop(outcomes_file):
    _write_outcomes(outcomes_file, [
        {"p1": "A B", "p2": "C D", "winner_pred": "A B",
         "date": _day(1), "result": "correct"},
    ])
    p._resolve_outcomes([("a-b", "c-d", "hard")])
    assert json.loads(outcomes_file.read_text())[0]["result"] == "correct"


# ──────────────────────────────────────────────────────────────
# grading helpers
# ──────────────────────────────────────────────────────────────
def test_confidence_pct_bounds():
    assert p.confidence_pct(0.5) == 50
    assert p.confidence_pct(0.99) <= 80
    assert p.confidence_pct(0.01) <= 80


@pytest.mark.parametrize("conf,g", [(75, "HIGH"), (70, "HIGH"),
                                    (66, "MEDIUM"), (64, "LOW"), (50, "LOW")])
def test_grade_thresholds(conf, g):
    assert p.grade(conf) == g


def test_win_prob_symmetry():
    assert p.win_prob(0.6, 0.4) == pytest.approx(1 - p.win_prob(0.4, 0.6))
    assert p.win_prob(0.5, 0.5) == pytest.approx(0.5)


# ──────────────────────────────────────────────────────────────
# _key_factor — phrased from the pick's side, never contradicts it
# ──────────────────────────────────────────────────────────────
def test_key_factor_streak_on_faded_player_is_upset_risk():
    df = {"serve_win_pct": 0.7, "sw": {}, "sl": {}, "rank": 60, "streak": 5}
    dd = {"serve_win_pct": 0.7, "sw": {}, "sl": {}, "rank": 144, "streak": 2}
    # pick Sakatsume, but the streak belongs to Kalinina (p1)
    out = p._key_factor(df, dd, "Kalinina A.", "Sakatsume H.", "hard", fav="Sakatsume H.")
    assert out == "Upset risk — Kalinina A. on a 5-win streak"


def test_key_factor_streak_on_pick_is_plain():
    df = {"serve_win_pct": 0.7, "sw": {}, "sl": {}, "rank": 60, "streak": 5}
    dd = {"serve_win_pct": 0.7, "sw": {}, "sl": {}, "rank": 144, "streak": 2}
    out = p._key_factor(df, dd, "Kalinina A.", "Sakatsume H.", "hard", fav="Kalinina A.")
    assert out == "Kalinina A. on a 5-win streak"


def test_key_factor_ranking_gap_names_the_pick():
    df = {"sw": {}, "sl": {}, "rank": 20}
    dd = {"sw": {}, "sl": {}, "rank": 200}
    assert "Kenin S." in p._key_factor(df, dd, "Kenin S.", "Doe J.", "hard", fav="Kenin S.")


# ──────────────────────────────────────────────────────────────
# tournament allow-list
# ──────────────────────────────────────────────────────────────
@pytest.mark.parametrize("name,allowed", [
    ("WTA Cincinnati", True),
    ("US Open", True),
    ("ITF W35 Cairo", False),
    ("Some Challenger 100", False),
    ("W15 Antalya", False),
    ("", False),
])
def test_is_allowed_tournament(name, allowed):
    assert p.is_allowed_tournament(name, check_date=False) is allowed


# ──────────────────────────────────────────────────────────────
# market edge / h2h adjustment
# ──────────────────────────────────────────────────────────────
def test_market_edge_positive_when_model_beats_market():
    # model 70%, fair market ~50% -> positive edge
    assert p.market_edge(0.70, 2.0, 2.0) > 0


def test_market_edge_zero_for_bad_odds():
    assert p.market_edge(0.7, None) == 0.0
    assert p.market_edge(0.7, 1.0) == 0.0


def test_h2h_adjustment_needs_two_meetings():
    assert p.h2h_adjustment(0.6, 5, 1) == 0.6          # total < 2 -> unchanged
    assert p.h2h_adjustment(0.6, 2, 2) != 0.6

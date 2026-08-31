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
    ("US Open",                 "R32",  False),   # main draw
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

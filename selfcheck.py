"""
Dependency-free sanity checks for predictor.py logic.
No pytest, no network, no browser.

Run:  python selfcheck.py
"""
import json
import os
import tempfile
from datetime import datetime, timedelta

import predictor as p

_passed = 0
_failed = 0


def check(label, got, want):
    global _passed, _failed
    if got == want:
        _passed += 1
        print(f"  ok   {label}")
    else:
        _failed += 1
        print(f"  FAIL {label}\n         got={got!r}\n         want={want!r}")


def _day(offset):
    return (datetime.now(p.WAT) - timedelta(days=offset)).strftime("%Y-%m-%d")


# ── is_slam_qualifying ────────────────────────────────────────
print("is_slam_qualifying")
check("US Open - Qualification",     p.is_slam_qualifying("US Open - Qualification", ""),  True)
check("Wimbledon Qualifying",        p.is_slam_qualifying("Wimbledon Qualifying", ""),     True)
check("Australian Open / qual.",     p.is_slam_qualifying("Australian Open", "qual."),     True)
check("French Open / Q1",            p.is_slam_qualifying("French Open", "Q1"),            True)
check("US Open main draw / R32",     p.is_slam_qualifying("US Open", "R32"),               False)
check("WTA 1000 quali (not slam)",   p.is_slam_qualifying("WTA Cincinnati", "Q1"),         False)
check("empty",                       p.is_slam_qualifying("", ""),                         False)

# ── tournament_tier ──────────────────────────────────────────
print("tournament_tier")
check("slam_qual", p.tournament_tier("US Open - Qualification", ""),  "slam_qual")
check("slam_main", p.tournament_tier("Roland Garros", "R64"),         "slam_main")
check("wta125",    p.tournament_tier("Iasi WTA 125", ""),             "wta125")
check("wta_tour",  p.tournament_tier("WTA Cincinnati", "SF"),         "wta_tour")

# ── _pair_key / _dedupe_by_pair ──────────────────────────────
print("pair key + dedupe")
check("pair key order-independent",
      p._pair_key("Vekic D.", "Cengiz B."), p._pair_key("Cengiz B.", "vekic d."))

rows = [
    {"p1": "A B", "p2": "C D", "date": "2026-05-05", "result": "correct",
     "pair": p._pair_key("A B", "C D")},
    {"p1": "C D", "p2": "A B", "date": "2026-05-06", "result": "wrong",
     "pair": p._pair_key("C D", "A B")},
    {"p1": "E F", "p2": "G H", "date": "2026-05-06", "result": "correct",
     "pair": p._pair_key("E F", "G H")},
]
deduped = p._dedupe_by_pair(rows)
check("dedupe collapses resend", len(deduped), 2)
check("dedupe keeps last copy",
      [o for o in deduped if o["pair"] == p._pair_key("A B", "C D")][0]["result"], "wrong")
check("dedupe fallback without pair field",
      len(p._dedupe_by_pair([{"p1": "A B", "p2": "C D"}, {"p1": "c d", "p2": "a b"}])), 1)

# ── _resolve_outcomes (temp file) ────────────────────────────
print("_resolve_outcomes")
_orig_file = p._OUTCOMES_FILE
_tmp = os.path.join(tempfile.gettempdir(), "selfcheck_outcomes.json")
try:
    p._OUTCOMES_FILE = _tmp

    def _seed(data):
        with open(_tmp, "w") as f:
            json.dump(data, f)

    def _load():
        with open(_tmp) as f:
            return json.load(f)

    _seed([
        {"p1": "Vekic D.", "p2": "Cengiz B.", "winner_pred": "Vekic D.",
         "date": _day(2), "result": None},
        {"p1": "Ito A.", "p2": "Selekhmeteva O.", "winner_pred": "Ito A.",
         "date": _day(3), "result": None},
    ])
    p._resolve_outcomes([
        ("vekic-donna", "cengiz-basak", "hard"),
        ("selekhmeteva-o", "ito-aoi", "clay"),
    ])
    saved = _load()
    check("predicted winner won  -> correct", saved[0]["result"], "correct")
    check("predicted winner lost -> wrong",   saved[1]["result"], "wrong")

    _seed([{"p1": "Vekic D.", "p2": "Cengiz B.", "winner_pred": "Vekic D.",
            "date": _day(30), "result": None}])
    p._resolve_outcomes([("vekic-donna", "cengiz-basak", "hard")])
    check("outside 10-day window -> untouched", _load()[0]["result"], None)

    _seed([{"p1": "Ma Y.", "p2": "Ngounoue C.", "winner_pred": "Ma Y.",
            "date": _day(1), "result": None}])
    p._resolve_outcomes([("ma-yexin", "someone-else", "hard")])
    check("only one player matches -> untouched", _load()[0]["result"], None)
finally:
    p._OUTCOMES_FILE = _orig_file
    if os.path.exists(_tmp):
        os.remove(_tmp)

# ── grading helpers ─────────────────────────────────────────
print("grading")
check("confidence_pct(0.5)==50", p.confidence_pct(0.5), 50)
check("confidence_pct capped at 80", p.confidence_pct(0.99) <= 80, True)
check("grade 75 -> HIGH",   p.grade(75), "HIGH")
check("grade 70 -> HIGH",   p.grade(70), "HIGH")
check("grade 66 -> MEDIUM", p.grade(66), "MEDIUM")
check("grade 64 -> LOW",    p.grade(64), "LOW")
check("win_prob 0.5/0.5 -> 0.5", round(p.win_prob(0.5, 0.5), 6), 0.5)
check("win_prob symmetric",
      round(p.win_prob(0.6, 0.4) + p.win_prob(0.4, 0.6), 6), 1.0)

# ── is_allowed_tournament ───────────────────────────────────
print("is_allowed_tournament")
check("WTA Cincinnati allowed", p.is_allowed_tournament("WTA Cincinnati", False), True)
check("US Open allowed",        p.is_allowed_tournament("US Open", False), True)
check("ITF W35 rejected",       p.is_allowed_tournament("ITF W35 Cairo", False), False)
check("Challenger rejected",    p.is_allowed_tournament("Some Challenger 100", False), False)
check("W15 prefix rejected",    p.is_allowed_tournament("W15 Antalya", False), False)
check("empty rejected",         p.is_allowed_tournament("", False), False)

# ── market edge / h2h ──────────────────────────────────────
print("market edge + h2h")
check("edge positive when model beats market", p.market_edge(0.70, 2.0, 2.0) > 0, True)
check("edge 0 for missing odds", p.market_edge(0.7, None), 0.0)
check("edge 0 for odds <= 1.0",  p.market_edge(0.7, 1.0), 0.0)
check("h2h unchanged when total < 2", p.h2h_adjustment(0.6, 5, 1), 0.6)
check("h2h adjusts when total >= 2",  p.h2h_adjustment(0.6, 2, 2) != 0.6, True)

# ── summary ────────────────────────────────────────────────
print(f"\n{'='*40}\n{_passed} passed, {_failed} failed\n{'='*40}")
raise SystemExit(1 if _failed else 0)

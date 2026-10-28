import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))

from events import BudgetTracker, InvalidEvent, parse_event  # noqa: E402


def event(**overrides):
    base = {
        "transaction_id": "t1",
        "user_id": 7,
        "timestamp": "2026-10-28T09:00:00+00:00",
        "merchant": "TESCO STORES",
        "category": "Groceries",
        "amount_gbp": 50.0,
        "type": "debit",
    }
    base.update(overrides)
    return json.dumps(base).encode()


# ── parse_event ──────────────────────────────────────────────────────────────

def test_parses_a_good_event():
    tx = parse_event(event())
    assert tx.user_id == 7
    assert tx.amount_gbp == 50.0
    assert tx.month == "2026-10"


@pytest.mark.parametrize("raw, reason", [
    (b"{not json", "not valid JSON"),
    (b"[1, 2, 3]", "not a JSON object"),
    (event(amount_gbp=-5), "must be positive"),
    (event(amount_gbp=0), "must be positive"),
    (event(type="refund"), "unknown type"),
    (event(timestamp="yesterday"), "bad value"),
    (event(user_id="seven"), "bad value"),
])
def test_rejects_bad_events(raw, reason):
    with pytest.raises(InvalidEvent, match=reason):
        parse_event(raw)


def test_rejects_missing_fields():
    data = json.loads(event())
    del data["amount_gbp"]
    with pytest.raises(InvalidEvent, match="missing fields"):
        parse_event(json.dumps(data).encode())


# ── BudgetTracker ────────────────────────────────────────────────────────────

def tx(id, amount, category="Groceries", user=1, ts="2026-10-05T12:00:00+00:00", type="debit"):
    return parse_event(event(transaction_id=id, amount_gbp=amount, category=category,
                             user_id=user, timestamp=ts, type=type))


def test_alerts_at_80_then_100_percent_once_each():
    t = BudgetTracker()  # Groceries budget is £300
    assert t.add(tx("a", 200)) == []                     # £200, 67%
    alerts = t.add(tx("b", 50))                           # £250, 83%
    assert [a["level"] for a in alerts] == ["close"]
    assert t.add(tx("c", 10)) == []                      # £260, already warned
    alerts = t.add(tx("d", 60))                           # £320, over
    assert [a["level"] for a in alerts] == ["over"]
    assert alerts[0]["spent_gbp"] == 320
    assert t.add(tx("e", 5)) == []                       # already told


def test_jumping_straight_past_100_sends_one_alert():
    t = BudgetTracker()
    assert [a["level"] for a in t.add(tx("a", 350))] == ["over"]
    assert t.add(tx("b", 10)) == []


def test_spending_is_tracked_per_user_category_and_month():
    t = BudgetTracker()
    t.add(tx("a", 250, user=1))
    t.add(tx("b", 250, user=2))
    t.add(tx("c", 250, category="Shopping", user=1))
    t.add(tx("d", 250, user=1, ts="2026-11-01T08:00:00+00:00"))
    assert t.spend[(1, "2026-10", "Groceries")] == 250
    assert t.spend[(2, "2026-10", "Groceries")] == 250
    assert t.spend[(1, "2026-10", "Shopping")] == 250
    assert t.spend[(1, "2026-11", "Groceries")] == 250


def test_credits_and_unbudgeted_categories_dont_count():
    t = BudgetTracker()
    assert t.add(tx("a", 500, type="credit")) == []
    assert t.add(tx("b", 500, category="Utilities")) == []
    assert t.spend == {}


def test_duplicates_are_recognised():
    t = BudgetTracker()
    first = tx("same-id", 100)
    assert not t.is_duplicate(first)
    t.add(first)
    assert t.is_duplicate(tx("same-id", 100))

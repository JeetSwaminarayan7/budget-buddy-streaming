"""
The logic for turning a raw Kafka message into something useful. Nothing
in here touches Kafka, so it can all be tested on its own.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime

# Monthly budget per category, the same for every user to keep things simple
BUDGETS_GBP = {
    "Groceries": 300,
    "Transport": 120,
    "Dining": 150,
    "Entertainment": 60,
    "Shopping": 200,
}

# Alert when spending reaches 80% of a budget, and again at 100%
THRESHOLDS = [(1.0, "over"), (0.8, "close")]

REQUIRED_FIELDS = {"transaction_id", "user_id", "timestamp", "merchant", "category", "amount_gbp", "type"}


class InvalidEvent(ValueError):
    """The message can't be processed. It goes to the dead letter topic."""


@dataclass
class Transaction:
    transaction_id: str
    user_id: int
    timestamp: datetime
    merchant: str
    category: str
    amount_gbp: float
    type: str

    @property
    def month(self) -> str:
        return self.timestamp.strftime("%Y-%m")


def parse_event(raw: bytes) -> Transaction:
    """Turn raw message bytes into a Transaction, or raise InvalidEvent."""
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise InvalidEvent(f"not valid JSON: {e}") from None

    if not isinstance(data, dict):
        raise InvalidEvent("message is not a JSON object")

    missing = REQUIRED_FIELDS - data.keys()
    if missing:
        raise InvalidEvent(f"missing fields: {sorted(missing)}")

    if data["type"] not in ("debit", "credit"):
        raise InvalidEvent(f"unknown type: {data['type']!r}")

    try:
        amount = float(data["amount_gbp"])
        user_id = int(data["user_id"])
        ts = datetime.fromisoformat(data["timestamp"])
    except (TypeError, ValueError) as e:
        raise InvalidEvent(f"bad value: {e}") from None

    if amount <= 0:
        raise InvalidEvent(f"amount must be positive, got {amount}")

    return Transaction(
        transaction_id=str(data["transaction_id"]),
        user_id=user_id,
        timestamp=ts,
        merchant=str(data["merchant"]),
        category=str(data["category"]),
        amount_gbp=round(amount, 2),
        type=data["type"],
    )


@dataclass
class BudgetTracker:
    """
    Running spend per user, per category, per month, and which alerts have
    already gone out. Kept in memory, so it's lost if the consumer restarts.
    """

    spend: dict = field(default_factory=dict)        # (user, month, category) -> £
    alerted: set = field(default_factory=set)        # (user, month, category, level)
    seen_ids: set = field(default_factory=set)       # transaction ids already counted

    def is_duplicate(self, tx: Transaction) -> bool:
        return tx.transaction_id in self.seen_ids

    def add(self, tx: Transaction) -> list[dict]:
        """Count a transaction. Returns any new alerts it triggers."""
        self.seen_ids.add(tx.transaction_id)

        if tx.type != "debit" or tx.category not in BUDGETS_GBP:
            return []

        key = (tx.user_id, tx.month, tx.category)
        self.spend[key] = round(self.spend.get(key, 0) + tx.amount_gbp, 2)

        budget = BUDGETS_GBP[tx.category]
        spent = self.spend[key]
        for share, level in THRESHOLDS:
            if spent >= budget * share:
                alert_key = key + (level,)
                if alert_key in self.alerted:
                    return []
                self.alerted.add(alert_key)
                # Crossing 100% in one go also counts as having passed 80%
                if level == "over":
                    self.alerted.add(key + ("close",))
                return [{
                    "user_id": tx.user_id,
                    "month": tx.month,
                    "category": tx.category,
                    "level": level,
                    "spent_gbp": spent,
                    "budget_gbp": budget,
                    "triggered_by": tx.transaction_id,
                }]
        return []

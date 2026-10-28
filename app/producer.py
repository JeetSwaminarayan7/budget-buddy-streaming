"""
Sends fake card transactions to the `transactions` topic, like a payments
feed would.

Each message is keyed by user_id. Kafka sends every message with the same
key to the same partition, so one user's transactions always arrive in
the order they were sent.

On purpose, a few messages are broken (they should end up in the dead
letter topic) and a few are sent twice (the consumer should ignore the
second copy).

  python producer.py                  # 5 a second, forever
  python producer.py --count 500 --rate 50
"""

import argparse
import json
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.environ.get("TRANSACTIONS_TOPIC", "transactions")

MERCHANTS = {
    "Groceries": (["TESCO STORES", "SAINSBURYS", "LIDL GB", "WAITROSE"], 5, 80),
    "Transport": (["TFL TRAVEL CHARGE", "UBER TRIP", "TRAINLINE"], 2.8, 40),
    "Dining": (["PRET A MANGER", "NANDOS", "DELIVEROO", "COSTA COFFEE"], 4, 35),
    "Entertainment": (["NETFLIX.COM", "SPOTIFY UK", "VUE CINEMAS"], 5, 25),
    "Shopping": (["AMAZON.CO.UK", "ARGOS", "PRIMARK", "BOOTS"], 6, 90),
}

running = True


def stop(*_):
    global running
    running = False


def make_event() -> dict:
    category = random.choice(list(MERCHANTS))
    merchants, lo, hi = MERCHANTS[category]
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 25),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "merchant": random.choice(merchants),
        "category": category,
        "amount_gbp": round(random.uniform(lo, hi), 2),
        "type": "debit",
    }


def break_event(event: dict) -> bytes:
    """Return a deliberately broken message."""
    kind = random.choice(["not_json", "missing_field", "negative", "bad_type"])
    if kind == "not_json":
        return b"{this is not json"
    event = dict(event)
    if kind == "missing_field":
        del event["amount_gbp"]
    elif kind == "negative":
        event["amount_gbp"] = -event["amount_gbp"]
    else:
        event["type"] = "refund"
    return json.dumps(event).encode()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=0, help="stop after this many messages (0 = never)")
    parser.add_argument("--rate", type=float, default=5, help="messages per second")
    parser.add_argument("--bad-rate", type=float, default=0.03, help="share of messages to break")
    parser.add_argument("--dup-rate", type=float, default=0.02, help="share of messages to send twice")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        # Wait until the broker has safely stored each message...
        "acks": "all",
        # ...and if a send is retried, don't let Kafka store it twice
        "enable.idempotence": True,
        "client.id": "transactions-producer",
    })

    sent = {"ok": 0, "broken": 0, "duplicate": 0}

    def on_delivery(err, msg):
        if err:
            print(f"Delivery failed: {err}", flush=True)
        elif msg.offset() % 50 == 0:
            print(f"Delivered to partition {msg.partition()} at offset {msg.offset()}", flush=True)

    print(f"Producing to {TOPIC} on {BOOTSTRAP}", flush=True)
    n = 0
    while running and (args.count == 0 or n < args.count):
        event = make_event()
        key = str(event["user_id"]).encode()

        if random.random() < args.bad_rate:
            producer.produce(TOPIC, key=key, value=break_event(event), on_delivery=on_delivery)
            sent["broken"] += 1
        else:
            value = json.dumps(event).encode()
            producer.produce(TOPIC, key=key, value=value, on_delivery=on_delivery)
            sent["ok"] += 1
            # The same transaction arriving twice, as if an upstream system retried
            if random.random() < args.dup_rate:
                producer.produce(TOPIC, key=key, value=value, on_delivery=on_delivery)
                sent["duplicate"] += 1

        n += 1
        producer.poll(0)  # runs delivery callbacks
        time.sleep(1 / args.rate)

    producer.flush(10)
    print(f"Done. Sent: {json.dumps(sent)}", flush=True)


if __name__ == "__main__":
    main()

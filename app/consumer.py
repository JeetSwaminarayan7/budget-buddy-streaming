"""
Reads the `transactions` topic, keeps a running total of each user's
spending per category this month, and sends an alert to `budget-alerts`
when they reach 80% and 100% of a budget.

  - Broken messages go to `transactions.dlq` with the reason in a header,
    instead of crashing the consumer or being silently dropped.
  - A transaction seen before (same transaction_id) is skipped.
  - Offsets are committed only after a message has been fully handled,
    so if the consumer dies halfway, the message is read again on restart
    (at-least-once delivery).

Run more than one copy with the same group id and Kafka splits the
partitions between them.

  python consumer.py
  python consumer.py --exit-when-idle 15     # stop after 15s with no messages
"""

import argparse
import json
import os
import signal
import socket
import time

from confluent_kafka import Consumer, KafkaException, Producer

from events import BudgetTracker, InvalidEvent, parse_event

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.environ.get("TRANSACTIONS_TOPIC", "transactions")
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "budget-alerts")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "transactions.dlq")
GROUP_ID = os.environ.get("CONSUMER_GROUP", "budget-tracker")
NAME = socket.gethostname()

running = True


def stop(*_):
    global running
    running = False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--exit-when-idle", type=float, default=0,
                        help="stop after this many seconds with no messages (0 = never)")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        # A brand-new group starts from the oldest message, not just new ones
        "auto.offset.reset": "earliest",
        # We commit ourselves, after each message is handled
        "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "enable.idempotence": True})

    def on_assign(_, partitions):
        print(f"[{NAME}] assigned partitions {[p.partition for p in partitions]}", flush=True)

    def on_revoke(_, partitions):
        print(f"[{NAME}] giving up partitions {[p.partition for p in partitions]}", flush=True)

    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)

    tracker = BudgetTracker()
    stats = {"processed": 0, "duplicates": 0, "dead_lettered": 0, "alerts": 0}
    last_message = time.monotonic()

    print(f"[{NAME}] consuming {TOPIC} as group {GROUP_ID}", flush=True)
    try:
        while running:
            msg = consumer.poll(1.0)

            if msg is None:
                if args.exit_when_idle and time.monotonic() - last_message > args.exit_when_idle:
                    print(f"[{NAME}] idle for {args.exit_when_idle}s, stopping", flush=True)
                    break
                continue
            if msg.error():
                raise KafkaException(msg.error())

            last_message = time.monotonic()

            try:
                tx = parse_event(msg.value())
            except InvalidEvent as e:
                # Keep the original bytes and say what was wrong with them
                producer.produce(DLQ_TOPIC, key=msg.key(), value=msg.value(), headers={
                    "error": str(e),
                    "source_topic": TOPIC,
                    "source_partition": str(msg.partition()),
                    "source_offset": str(msg.offset()),
                })
                stats["dead_lettered"] += 1
                print(f"[{NAME}] dead-lettered offset {msg.offset()}: {e}", flush=True)
            else:
                if tracker.is_duplicate(tx):
                    stats["duplicates"] += 1
                else:
                    for alert in tracker.add(tx):
                        producer.produce(ALERTS_TOPIC, key=str(alert["user_id"]).encode(),
                                         value=json.dumps(alert).encode())
                        stats["alerts"] += 1
                        print(f"[{NAME}] ALERT user {alert['user_id']} is {alert['level']} on "
                              f"{alert['category']}: £{alert['spent_gbp']} of £{alert['budget_gbp']}",
                              flush=True)
                    stats["processed"] += 1

            # Make sure anything we sent has reached Kafka, then mark this
            # message as done. Committing first would risk losing it.
            producer.flush(10)
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()  # leaves the group cleanly so others take over at once
        print(f"[{NAME}] SUMMARY {json.dumps(stats)}", flush=True)


if __name__ == "__main__":
    main()

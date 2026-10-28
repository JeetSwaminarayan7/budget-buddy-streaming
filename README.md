# Budget Buddy streaming (Kafka)

A small Kafka pipeline for [Budget Buddy](https://github.com/JeetSwaminarayan7/budget-buddy), my personal finance app. Card transactions stream in, a consumer keeps a running total of each user's spending, and it sends an alert the moment someone reaches 80% or 100% of a monthly budget, instead of waiting for a nightly batch.

Everything runs in Docker Compose.

```
                     transactions topic
producer  ──────▶  ┌───────────────────┐  ──────▶  consumer  ──────▶  budget-alerts
(card payments,    │ partition 0       │          (budget-tracker       (80% and 100%
 keyed by user)    │ partition 1       │           group)                warnings)
                   │ partition 2       │              │
                   └───────────────────┘              └──────────▶  transactions.dlq
                                                                    (broken messages,
                                                                     with the reason)
```

## What's in it

| Service | What it does |
|---|---|
| `kafka` | One Kafka 4.1 broker in KRaft mode, so no ZooKeeper |
| `kafka-init` | Creates the three topics, then exits |
| `producer` | Sends about 5 fake card transactions a second |
| `consumer` | Tracks spending and sends alerts |
| `kafka-ui` | A web page for browsing topics, messages and consumer groups, at http://localhost:8080 |

The logic lives in `app/events.py` with no Kafka code in it, so it's tested on its own (`tests/test_events.py`). `app/producer.py` and `app/consumer.py` are the Kafka parts.

## The Kafka ideas it shows

**Keys and partitions.** Every message is keyed by `user_id`. Kafka always sends the same key to the same partition, and keeps messages in order within a partition. So one user's transactions arrive in the order they happened, and only one consumer ever handles a given user, which is what makes the running totals correct.

**Consumer groups.** Run more than one consumer with the same `group.id` and Kafka shares the partitions out between them. Start a second one and Kafka rebalances: each consumer logs which partitions it now has. Stop one and the others take its partitions over.

**At-least-once delivery.** Auto-commit is off. The consumer only commits a message's offset after it has handled it and its alert has reached Kafka. If the consumer crashes halfway, the message is read again on restart instead of being lost.

**Idempotence.** At-least-once means a message can be read twice, and upstream systems sometimes send the same thing twice anyway. The producer has `enable.idempotence` on so its own retries don't create copies, and the consumer skips any `transaction_id` it has already counted. The producer sends about 2% duplicates on purpose to test this.

**Dead letter queue.** About 3% of messages are broken on purpose: bad JSON, a missing amount, a negative amount, an unknown type. Instead of crashing or silently dropping them, the consumer sends the original bytes to `transactions.dlq` with headers saying what was wrong and where it came from, so someone can look at them and replay them later.

**acks=all.** The producer waits for the broker to confirm it has safely stored each message.

## Running it

You need Docker Desktop.

```bash
docker compose up --build
```

After a minute you'll see the producer sending and the consumer logging alerts like:

```
consumer-1  | [a1b2c3] ALERT user 9 is over on Entertainment: £61.12 of £60
consumer-1  | [a1b2c3] dead-lettered offset 23: amount must be positive, got -31.4
```

Open http://localhost:8080 to look around: `Topics` shows messages in each topic (look at the headers on `transactions.dlq`), and `Consumers` shows the `budget-tracker` group, which partitions each consumer has, and the lag.

`Ctrl+C` stops it. `docker compose down -v` also deletes the stored messages.

## Things to try

Scale to three consumers and watch the partitions get shared out:

```bash
docker compose up -d --scale consumer=3
docker compose logs -f consumer
```

Each consumer logs `assigned partitions [...]`. Try `--scale consumer=4`: one of them gets nothing, because there are only three partitions. That's why the partition count limits how far a consumer group can scale.

Stop the consumer for a minute and start it again. In the UI, the group's lag climbs while it's stopped, then it catches up and carries on from where it left off, because the offsets were committed.

Read the topics from the command line:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic budget-alerts --from-beginning

docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group budget-tracker
```

Run the producer or consumer from your laptop instead of Docker. They connect to `localhost:9094` by default:

```bash
pip install -r app/requirements.txt
cd app
python consumer.py
```

## Limits worth knowing

The running totals and the list of seen transaction ids live in the consumer's memory. If it restarts, or a rebalance moves a partition to another consumer, that state starts again from zero. A real system would keep it somewhere durable: Kafka Streams state stores, Flink, a compacted Kafka topic, or a database like Redis. The consumer also waits for each alert to be confirmed before committing, which is safe but slow; batching commits would handle far more messages a second.

## Tests

```bash
pip install pytest -r app/requirements.txt
pytest -v tests
```

14 tests cover parsing and rejecting messages and the budget alert logic.

CI (`.github/workflows/ci.yml`) also runs the whole thing end to end: it starts Kafka in Docker, sends 300 messages with 5% broken and 5% duplicated, consumes them, and checks every good message was processed once, every duplicate skipped and every broken message dead-lettered.

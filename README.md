# Kafka Streams — Fraud Detection Demo

Detects suspicious activity by counting user events within a rolling 5-minute window.
If a user exceeds 5 events in a window, an alert is emitted to the `alerts` topic.

## Topics

| Topic | Direction | Description |
|---|---|---|
| `user-events` | Input | User activity events (Jackson-serialized JSON) |
| `alerts` | Output | Fraud alerts when threshold exceeded |

## How to Run Kafka (Docker)

A `docker-compose.yml` is included. It runs Kafka in KRaft mode (no Zookeeper) and auto-creates topics on first use.

```bash
docker compose up -d
```

To stop:

```bash
docker compose down
```

## How to Run the App

```bash
./gradlew bootRun
```

Or build and run the jar:

```bash
./gradlew build
java -jar build/libs/stream-0.0.1-SNAPSHOT.jar
```

## Testing via REST API

The app exposes two endpoints on `http://localhost:8080`.

### Send a single event (normal activity)

```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","amount":120}'
```

Repeat this fewer than 5 times within a 5-minute window — no alert should appear.

### Simulate fraud (instantly triggers an alert)

```bash
curl -X POST http://localhost:8080/events/simulate-fraud/user-1
```

This sends 6 events back-to-back for `user-1`. The threshold (5) is exceeded immediately and an alert is emitted to the `alerts` topic.

---

## Produce Sample Events (via Kafka console — alternative)

The app uses Jackson to deserialize events, so plain JSON from the console producer still works.
Prefer the REST API above for normal testing.

```bash
docker compose exec -it kafka /opt/bitnami/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic user-events

# paste these one by one (or all at once):
{"userId":"user-1","amount":120,"timestamp":1710000000000}
{"userId":"user-1","amount":85,"timestamp":1710000030000}
{"userId":"user-1","amount":200,"timestamp":1710000060000}
{"userId":"user-1","amount":50,"timestamp":1710000090000}
{"userId":"user-1","amount":300,"timestamp":1710000120000}
{"userId":"user-2","amount":75,"timestamp":1710000010000}
{"userId":"user-2","amount":90,"timestamp":1710000040000}
```

## Observe Results

Alerts appear in two places:

**1. Application logs** — `AlertConsumer` listens on the `alerts` topic and logs every alert directly in the Spring Boot console:

```
>>>>>>>>>> ALERT received on alerts topic: {"userId":"user-1",...} <<<<<<<<<<
```

**2. Kafka console consumer** — inspect the raw topic output:

```bash
docker compose exec kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic alerts --from-beginning
```

Expected alert output (after user-1 reaches 5 events):

```json
{"userId":"user-1","windowStart":1710000000000,"windowEnd":1710000300000,"eventCount":5}
```

## Run Tests

```bash
./gradlew test
```

Tests use `TopologyTestDriver` — no running Kafka required.

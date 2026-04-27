# Kafka Streams — Fraud Detection Demo

Detects suspicious activity by counting user events within a rolling 5-minute window.
If a user exceeds 5 events in a window, an alert is emitted to the `alerts` topic.

## Topics

| Topic | Direction | Description |
|---|---|---|
| `user-events` | Input | User activity events (Jackson-serialized JSON) |
| `alerts` | Output | Fraud alerts when threshold exceeded |

## How to Run Kafka (Docker)

```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  apache/kafka:3.7.0
```

## Create Topics

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 --topic user-events --partitions 1 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 --topic alerts --partitions 1 --replication-factor 1
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
docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
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

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
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

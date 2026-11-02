# ShipPulse — Real-Time Vessel Tracking Pipeline

A Scala streaming pipeline that simulates a fleet of vessels, publishes their positions to Apache Kafka, and persists the events to PostgreSQL.

---

## Architecture

```
ShipSimulator (Scala)
      │  publishes 1 VesselPosition event/sec
      ▼
Kafka topic: vessel.positions.raw  (3 partitions)
      │
      ├─► SimpleConsumer  →  stdout (dev inspection)
      │
      └─► [Consumer → PostgreSQL]
                vessel_positions        (all events)
                vessel_position_rejects (failed validation)
```

---

## Project Structure

```
src/main/scala/com/shippulse/
  ShipSimulator.scala       # Simulates 5 vessels, sends 1 event/sec to Kafka
  SimpleProducer.scala      # Standalone Kafka producer (manual testing)
  SimpleConsumer.scala      # Kafka consumer that prints events to stdout
  VesselPosition.scala      # Event data model (case class)

infra/postgres/init/
  01_schema.sql             # Creates vessel_positions and vessel_position_rejects tables

build.sbt                   # Project dependencies and assembly config
```

---

## Data Model

```scala
VesselPosition(
  eventId:         String,   // UUID, unique per event
  vesselId:        String,   // e.g. "vessel-01"
  latitude:        Double,
  longitude:       Double,
  speedKnots:      Double,
  eventTimeMillis: Long      // epoch ms
)
```

---

## Prerequisites

- Docker (runs Kafka and PostgreSQL)
- Java 11+
- sbt 1.x

---

## Quick Start

### 1. Start infrastructure

```bash
docker-compose up -d
```

### 2. Create the Kafka topic

```bash
docker exec -it shippulse-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create \
  --topic vessel.positions.raw --partitions 3 --replication-factor 1
```

### 3. Apply the PostgreSQL schema

```bash
docker exec -i shippulse-postgres psql -U shippulse -d shippulse \
  < infra/postgres/init/01_schema.sql
```

### 4. Run the simulator

```bash
sbt "runMain com.shippulse.ShipSimulator"
```

Publishes **one vessel position per second**, cycling through 5 vessels across three North Sea ports (Rotterdam, Antwerp, Hamburg).

### 5. Inspect messages (optional)

```bash
docker exec -it shippulse-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic vessel.positions.raw \
  --from-beginning --max-messages 10 --timeout-ms 5000
```

---

## Building a Fat JAR

```bash
sbt assembly
```

The output JAR is placed under `target/scala-2.13/`.

---

## Resetting State

| What to reset | Command |
|---|---|
| Clear all vessel rows | `docker exec -it shippulse-postgres psql -U shippulse -d shippulse -c "TRUNCATE TABLE vessel_positions;"` |
| Delete Kafka topic | `docker exec -it shippulse-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic vessel.positions.raw` |
| Check topic offsets | `docker exec -it shippulse-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic vessel.positions.raw` |

---

## Dependencies

| Library | Version | Purpose |
|---|---|---|
| kafka-clients | 3.6.1 | Kafka producer / consumer |
| jackson-module-scala | 2.17.0 | JSON serialization |
| logback-classic | 1.5.6 | Logging |
| postgresql | 42.7.4 | JDBC driver for PostgreSQL |

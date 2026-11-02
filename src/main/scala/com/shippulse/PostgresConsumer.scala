package com.shippulse

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.sql.{Connection, PreparedStatement, DriverManager}
import java.time.Duration
import java.util.{Collections, Properties}
import scala.util.Try

object PostgresConsumer {

  def main(args: Array[String]): Unit = {Class.forName("org.postgresql.Driver")

    // ---- 1. Kafka consumer config ----
    val props = new Properties()
    props.put("bootstrap.servers",  "localhost:9092")
    props.put("group.id",           "shippulse-postgres-writers")
    props.put("key.deserializer",   "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset",  "earliest")
    props.put("enable.auto.commit", "false")

    // ---- 2. JSON mapper ----
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    // ---- 3. Postgres connection and prepared statement ----
    val conn: Connection =
      DriverManager.getConnection("jdbc:postgresql://localhost:5432/shippulse", "shippulse", "shippulse")

    val stmt: PreparedStatement = conn.prepareStatement(
      """INSERT INTO vessel_positions
         (event_id, vessel_id, latitude, longitude, speed_knots, event_time_millis)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT (event_id) DO NOTHING"""
    )

    // ---- 4. Kafka consumer ----
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList("vessel.positions.raw"))
    println("Subscribed. Writing to Postgres...")

    try {
      while (true) {
        val records = consumer.poll(Duration.ofSeconds(1))
        val it      = records.iterator()

        while (it.hasNext) {
          val r    = it.next()
          val json = r.value()

          val result: Try[Unit] = Try {
            val event = mapper.readValue(json, classOf[VesselPosition])

            stmt.setString(1, event.eventId)
            stmt.setString(2, event.vesselId)
            stmt.setDouble(3, event.latitude)
            stmt.setDouble(4, event.longitude)
            stmt.setDouble(5, event.speedKnots)
            stmt.setLong(6,   event.eventTimeMillis)

            val affected = stmt.executeUpdate()
            if (affected == 1) println(s"INSERTED  ${event.eventId} (${event.vesselId})")
            else               println(s"DUPLICATE ${event.eventId} (ignored)")
          }

          result.failed.foreach(e =>
            println(s"SKIP ${r.key()}: ${e.getClass.getSimpleName}: ${e.getMessage}")
          )
        }

        // Commit offsets AFTER all messages in this batch were written
        consumer.commitSync()
      }
    } finally {
      consumer.close()
      stmt.close()
      conn.close()
    }
  }
}
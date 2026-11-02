package com.shippulse

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.time.Duration
import java.util.{Collections, Properties}
import scala.util.{Failure, Success, Try}

final case class RejectedEvent(
  eventId:          String,
  vesselId:         Option[String],
  rawValue:         String,
  reason:           String,
  rejectedAtMillis: Long
)

object ValidatingConsumer {

  /** Pure validation — no side effects, easy to test. */
  def validate(e: VesselPosition): Either[String, VesselPosition] = {
    if (e.eventId == null || e.eventId.trim.isEmpty)           Left("empty_event_id")
    else if (e.vesselId == null || e.vesselId.trim.isEmpty)    Left("empty_vessel_id")
    else if (e.latitude  < -90.0 || e.latitude  > 90.0)        Left(s"invalid_latitude:${e.latitude}")
    else if (e.longitude < -180.0 || e.longitude > 180.0)      Left(s"invalid_longitude:${e.longitude}")
    else if (e.speedKnots < 0.0)                               Left(s"negative_speed:${e.speedKnots}")
    else if (e.speedKnots > 60.0)                              Left(s"impossible_speed:${e.speedKnots}")
    else if (e.eventTimeMillis <= 0L)                          Left(s"invalid_event_time:${e.eventTimeMillis}")
    else                                                       Right(e)
  }

  def main(args: Array[String]): Unit = {
    Class.forName("org.postgresql.Driver")

    // ---- Kafka consumer ----
    val cprops = new Properties()
    cprops.put("bootstrap.servers",  "localhost:9092")
    cprops.put("group.id",           "shippulse-validating-writers")
    cprops.put("key.deserializer",   "org.apache.kafka.common.serialization.StringDeserializer")
    cprops.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    cprops.put("auto.offset.reset",  "earliest")
    cprops.put("enable.auto.commit", "false")

    // ---- Kafka producer (only used for the rejected topic) ----
    val pprops = new Properties()
    pprops.put("bootstrap.servers",  "localhost:9092")
    pprops.put("key.serializer",     "org.apache.kafka.common.serialization.StringSerializer")
    pprops.put("value.serializer",   "org.apache.kafka.common.serialization.StringSerializer")
    pprops.put("acks",               "all")
    pprops.put("enable.idempotence", "true")

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val consumer        = new KafkaConsumer[String, String](cprops)
    val rejectProducer  = new KafkaProducer[String, String](pprops)
    consumer.subscribe(Collections.singletonList("vessel.positions.raw"))

    // ---- Postgres ----
    val conn: Connection = DriverManager.getConnection(
      "jdbc:postgresql://localhost:5432/shippulse", "shippulse", "shippulse")

    val insertPos = conn.prepareStatement(
      """INSERT INTO vessel_positions
         (event_id, vessel_id, latitude, longitude, speed_knots, event_time_millis)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT (event_id) DO NOTHING""")

    val insertReject = conn.prepareStatement(
      """INSERT INTO vessel_position_rejects
         (event_id, vessel_id, raw_value, reject_reason)
         VALUES (?, ?, ?, ?)
         ON CONFLICT (event_id) DO NOTHING""")

    println("ValidatingConsumer started: raw -> (positions | rejected)")

    try {
      while (true) {
        val records = consumer.poll(Duration.ofSeconds(1))
        val it      = records.iterator()

        while (it.hasNext) {
          val r      = it.next()
          val rawJson = r.value()

          // Parse the JSON first
          Try(mapper.readValue(rawJson, classOf[VesselPosition])) match {

            case Failure(ex) =>
              // Malformed JSON: no event_id available. Synthesize one from the
              // Kafka coordinates so retries are idempotent on (partition, offset).
              val syntheticId = s"reject-${r.partition()}-${r.offset()}"
              handleReject(syntheticId, None, rawJson,
                s"malformed_json:${ex.getClass.getSimpleName}",
                rejectProducer, insertReject, mapper)

            case Success(event) =>
              validate(event) match {
                case Left(reason) =>
                  handleReject(event.eventId, Some(event.vesselId), rawJson,
                    reason, rejectProducer, insertReject, mapper)

                case Right(valid) =>
                  insertPos.setString(1, valid.eventId)
                  insertPos.setString(2, valid.vesselId)
                  insertPos.setDouble(3, valid.latitude)
                  insertPos.setDouble(4, valid.longitude)
                  insertPos.setDouble(5, valid.speedKnots)
                  insertPos.setLong  (6, valid.eventTimeMillis)
                  val n = insertPos.executeUpdate()
                  if (n == 1) println(s"OK      ${valid.eventId} (${valid.vesselId})")
                  else        println(s"DUP     ${valid.eventId} (${valid.vesselId})")
              }
          }
        }

        // Commit AFTER every message has been routed to its sink.
        consumer.commitSync()
      }
    } finally {
      consumer.close()
      rejectProducer.close()
      insertPos.close()
      insertReject.close()
      conn.close()
    }
  }

  private def handleReject(
    eventId:   String,
    vesselId:  Option[String],
    rawValue:  String,
    reason:    String,
    producer:  KafkaProducer[String, String],
    insertStmt: PreparedStatement,
    mapper:    ObjectMapper
  ): Unit = {
    val reject = RejectedEvent(
      eventId          = eventId,
      vesselId         = vesselId,
      rawValue         = rawValue,
      reason           = reason,
      rejectedAtMillis = System.currentTimeMillis()
    )

    // 1. Publish to the rejected topic. Key = eventId so the reject for a given
    //    event is always on the same partition (helps downstream dedup).
    producer.send(new ProducerRecord[String, String](
      "vessel.positions.rejected",
      eventId,
      mapper.writeValueAsString(reject)
    )).get()

    // 2. Persist the reject in Postgres.
    insertStmt.setString(1, eventId)
    insertStmt.setString(2, vesselId.orNull)
    insertStmt.setString(3, rawValue)
    insertStmt.setString(4, reason)
    insertStmt.executeUpdate()

    println(s"REJECT  $reason  eventId=$eventId")
  }
}
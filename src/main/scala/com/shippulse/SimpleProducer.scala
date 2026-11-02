package com.shippulse

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties

object SimpleProducer {

  def main(args: Array[String]): Unit = {

    // ---- 1. Producer configuration ----
    val props = new Properties()
    props.put("bootstrap.servers",  "localhost:9092")
    props.put("key.serializer",     "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",   "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks",               "all")
    props.put("enable.idempotence", "true")

    // ---- 2. JSON mapper (created once) ----
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    // ---- 3. Create the producer ----
    val producer = new KafkaProducer[String, String](props)

    try {
      // ---- 4. Send 10 events ----
      for (i <- 1 to 10) {
        val event = VesselPosition(
          eventId         = s"evt-$i",
          vesselId        = s"vessel-${i % 3}",
          latitude        = 51.90 + i * 0.01,
          longitude       = 4.10  + i * 0.01,
          speedKnots      = 10.0  + i,
          eventTimeMillis = System.currentTimeMillis()
        )

        val json   = mapper.writeValueAsString(event)
        val record = new ProducerRecord[String, String](
          "vessel.positions.raw",
          event.vesselId,   // key
          json              // value
        )

        val meta = producer.send(record).get()
        println(s"sent ${event.eventId} -> partition=${meta.partition()} offset=${meta.offset()}")
      }
    } finally {
      producer.close()   // flush + close
    }
  }
}
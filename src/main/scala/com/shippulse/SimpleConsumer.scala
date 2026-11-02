package com.shippulse

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.{Collections, Properties}

object SimpleConsumer {

  def main(args: Array[String]): Unit = {

    // ---- 1. Configuration ----
    val props = new Properties()
    props.put("bootstrap.servers",  "localhost:9092")
    props.put("group.id",           "shippulse-readers")
    props.put("key.deserializer",   "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset",  "earliest")
    props.put("enable.auto.commit", "false")

    // ---- 2. Create and subscribe ----
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList("vessel.positions.raw"))

    println("Subscribed. Waiting for messages (Ctrl+C to stop)...")

    try {
      // ---- 3. Poll loop ----
      while (true) {
        val records = consumer.poll(Duration.ofSeconds(1))
        val it      = records.iterator()
        while (it.hasNext) {
          val r = it.next()
          println(s"partition=${r.partition()} offset=${r.offset()} key=${r.key()} value=${r.value()}")
        }
        consumer.commitSync()
      }
    } finally {
      consumer.close()
    }
  }
}
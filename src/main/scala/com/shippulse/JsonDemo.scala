package com.shippulse

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonDemo {
  def main(args: Array[String]): Unit = {

    // ---- 1. Create the mapper ONCE (it's thread-safe and reusable) ----
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    // ---- 2. Original instance ----
    val original = VesselPosition(
      eventId         = "evt-001",
      vesselId        = "vessel-A",
      latitude        = 51.90,
      longitude       = 4.10,
      speedKnots      = 12.5,
      eventTimeMillis = 1700000000000L
    )

    // ---- 3. Object -> JSON string ----
    val json: String = mapper.writeValueAsString(original)
    println(s"JSON: $json")

    // ---- 4. JSON string -> Object ----
    val parsed: VesselPosition =
      mapper.readValue(json, classOf[VesselPosition])
    println(s"Parsed: $parsed")

    // ---- 5. Round-trip equality ----
    println(s"Round-trip equal: ${original == parsed}")
  }
}
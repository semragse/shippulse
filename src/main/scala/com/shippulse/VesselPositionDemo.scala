package com.shippulse

object VesselPositionDemo {
  def main(args: Array[String]): Unit = {

    // ---- 1. Create an instance — note: NO `new` keyword ----
    val p1 = VesselPosition(
      eventId         = "evt-001",
      vesselId        = "vessel-A",
      latitude        = 51.90,
      longitude       = 4.10,
      speedKnots      = 12.5,
      eventTimeMillis = 1700000000000L
    )

    println(s"Created: $p1")

    // ---- 2. copy() — produces a NEW instance, original untouched ----
    val p2 = p1.copy(speedKnots = 0.0)

    println(s"Original speed: ${p1.speedKnots}")
    println(s"Copied  speed:  ${p2.speedKnots}")

    // ---- 3. Structural equality — == compares FIELDS, not references ----
    val p3 = VesselPosition("evt-001", "vessel-A", 51.90, 4.10, 12.5, 1700000000000L)
    println(s"p1 == p3 (same fields): ${p1 == p3}")
    println(s"p1 == p2 (diff. speed): ${p1 == p2}")
  }
}
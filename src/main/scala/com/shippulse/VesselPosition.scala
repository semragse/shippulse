package com.shippulse

final case class VesselPosition(
  eventId:         String,
  vesselId:        String,
  latitude:        Double,
  longitude:       Double,
  speedKnots:      Double,
  eventTimeMillis: Long
)
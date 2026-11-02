package com.shippulse

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.{Properties, UUID}
import scala.util.Random

final case class Port(name: String, latitude: Double, longitude: Double)

final case class ShipState(
  vesselId:   String,
  latitude:   Double,
  longitude:  Double,
  targetPort: Port,
  speedKnots: Double
)

object ShipSimulator {

  val Ports: List[Port] = List(
    Port("Rotterdam", 51.949, 4.145),
    Port("Antwerp",   51.261, 4.399),
    Port("Hamburg",   53.549, 9.968)
  )

  val VesselCount  = 5
  val TickMillis   = 1000L

  def main(args: Array[String]): Unit = {
    val random = new Random(42)   // fixed seed: reproducible simulations

    // ---- Initial fleet: each ship starts at a port, heads to another ----
    var ships: Map[String, ShipState] =
      (1 to VesselCount).map { i =>
        val start  = Ports(i % Ports.size)
        val target = Ports((i + 1) % Ports.size)
        val id     = f"vessel-$i%02d"
        id -> ShipState(id, start.latitude, start.longitude, target, 10.0)
      }.toMap

    // ---- Kafka producer ----
    val props = new Properties()
    props.put("bootstrap.servers",  "localhost:9092")
    props.put("key.serializer",     "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",   "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks",               "all")
    props.put("enable.idempotence", "true")

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val producer = new KafkaProducer[String, String](props)
    println(s"ShipSimulator started: ${VesselCount} vessels, ${Ports.size} ports, tick=${TickMillis}ms")

    val vesselIds = ships.keys.toVector.sorted
    var tickIndex = 0

    try {
      while (true) {
        val vesselId = vesselIds(tickIndex % vesselIds.size)
        val ship     = ships(vesselId)
        val updated  = advance(ship, random)
        ships = ships.updated(vesselId, updated)

        val event = VesselPosition(
          eventId         = UUID.randomUUID().toString,
          vesselId        = vesselId,
          latitude        = round6(updated.latitude),
          longitude       = round6(updated.longitude),
          speedKnots      = round2(updated.speedKnots),
          eventTimeMillis = System.currentTimeMillis()
        )

        val json   = mapper.writeValueAsString(event)
        val record = new ProducerRecord[String, String]("vessel.positions.raw", vesselId, json)
        producer.send(record)

        println(
          f"${event.eventId.take(8)}  ${event.vesselId}  " +
          f"(${event.latitude}%.4f, ${event.longitude}%.4f)  " +
          f"${event.speedKnots}%5.1f kn  -> ${updated.targetPort.name}"
        )

        tickIndex += 1
        Thread.sleep(TickMillis)
      }
    } finally {
      println("Closing producer...")
      producer.close()
    }
  }

  /** Move the ship a fraction of the way toward its target port. */
  def advance(ship: ShipState, random: Random): ShipState = {
    val step = 0.10   // 10% of remaining distance per tick

    val dLat  = (ship.targetPort.latitude  - ship.latitude)  * step
    val dLon  = (ship.targetPort.longitude - ship.longitude) * step
    val newLat = ship.latitude  + dLat
    val newLon = ship.longitude + dLon

    // Arrived within ~0.05 degrees (~5 km)? Pick a new random destination.
    val remaining = math.sqrt(
      math.pow(ship.targetPort.latitude  - newLat, 2) +
      math.pow(ship.targetPort.longitude - newLon, 2)
    )

    if (remaining < 0.05) {
      val newTarget = Ports(random.nextInt(Ports.size))
      ship.copy(
        latitude   = ship.targetPort.latitude,
        longitude  = ship.targetPort.longitude,
        targetPort = newTarget
      )
    } else {
      ship.copy(latitude = newLat, longitude = newLon)
    }
  }

  def round6(d: Double): Double = BigDecimal(d).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
  def round2(d: Double): Double = BigDecimal(d).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
}








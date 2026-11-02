-- ShipPulse schema, v0.1
-- One row per processed vessel position event.

CREATE TABLE IF NOT EXISTS vessel_positions (
    event_id          TEXT         PRIMARY KEY,
    vessel_id         TEXT         NOT NULL,
    latitude          DOUBLE PRECISION NOT NULL,
    longitude         DOUBLE PRECISION NOT NULL,
    speed_knots       DOUBLE PRECISION NOT NULL,
    event_time_millis BIGINT       NOT NULL,
    ingested_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_vessel_positions_vessel_time
    ON vessel_positions (vessel_id, event_time_millis DESC);






    
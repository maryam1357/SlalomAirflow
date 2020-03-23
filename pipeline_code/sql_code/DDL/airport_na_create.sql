DROP TABLE IF EXISTS
  `practice-271500.sandbox.airport_na`;
CREATE TABLE IF NOT EXISTS
  `practice-271500.sandbox.airport_na`(ident STRING,
    type STRING,
    name STRING,
    elevation_ft INT64,
    continent STRING,
    iso_country STRING,
    iso_region STRING,
    municipality STRING,
    gps_code STRING,
    iata_code STRING,
    local_code STRING,
    coordinates STRING )
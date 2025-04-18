{{
    config(materialized="table")
}}

with measurements_24h as(
SELECT *
FROM {{ref("dim_measurements_with_pollution_ratio")}}
WHERE CAST(datetime as TIMESTAMP) > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
)
SELECT
  measurements.*
FROM
  (SELECT
     location_id, parameter, MAX(datetime) AS created_at
   FROM
     measurements_24h
   GROUP BY
     1, 2) AS latest_measurements_per_sensor
INNER JOIN
  measurements_24h as measurements
ON
  measurements.location_id = latest_measurements_per_sensor.location_id AND
  measurements.parameter = latest_measurements_per_sensor.parameter AND
  measurements.datetime = latest_measurements_per_sensor.created_at
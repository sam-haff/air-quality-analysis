{{
    config(materialized="table")
}}

SELECT
  measurements.*
FROM
  (SELECT
     location_id, parameter, MAX(datetime) AS created_at
   FROM
     {{ref("dim_measurements_with_pollution_ratio")}}
   GROUP BY
     1, 2) AS latest_measurements_per_sensor
INNER JOIN
  {{ref("dim_measurements_with_pollution_ratio")}} as measurements
ON
  measurements.location_id = latest_measurements_per_sensor.location_id AND
  measurements.parameter = latest_measurements_per_sensor.parameter AND
  measurements.datetime = latest_measurements_per_sensor.created_at

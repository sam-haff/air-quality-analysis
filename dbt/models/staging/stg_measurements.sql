{{
    config(materialized="view")
}}

select *
from {{ source("staging", 'm_measurements') }}
where value >= 0.0 and units = 'µg/m³'

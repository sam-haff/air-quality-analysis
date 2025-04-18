{{
    config(materialized="view")
}}

select m.*, l.country_name
from {{ source("staging", 'm_measurements') }} as m
join {{ ref("stg_locs")}} as l
on l.id = m.location_id
where value >= 0.0 and units = 'µg/m³'

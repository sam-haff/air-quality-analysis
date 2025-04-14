{{
    config(materialized="view")
}}

select *
from {{ source("staging", 'sensors_topology_locs') }}
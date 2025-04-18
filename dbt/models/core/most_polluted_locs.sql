{{
    config(materialized="table")
}}

select location, country_name, parameter, pollution_ratio
from {{ref("fact_latest_measurements")}}
order by pollution_ratio desc
limit 100
{{
    config(materialized="table")
}}

with polluted_locs_count as (
    select country_name, count(*) as cnt
    from {{ref("fact_latest_measurements")}}
    where pollution_ratio > 0.5
    group by country_name
),
locs_count as (
    select country_name, count(distinct location_id) as locs_cnt
    from {{ref("fact_latest_measurements")}} 
    group by country_name
)
select p.country_name, p.cnt/m.locs_cnt as polluted_locs_count_ratio
from polluted_locs_count as p join locs_count as m
on p.country_name = m.country_name
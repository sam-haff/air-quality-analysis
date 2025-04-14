{{
    config(materialized="table")
}}

select *, value/({{get_parameter_max_safe_value( "parameter" )}}) as pollution_ratio
from {{ref("stg_measurements")}}
where parameter in {{ get_important_pollutants() }}
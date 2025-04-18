{#
Returns maximum safe value for the selected pollutant
#}

{% macro get_parameter_max_safe_value(param) -%}
    case {{param}}
        when 'co' then 11500
        when 'o3' then 140
        when 'pm10' then 54
        when 'no2' then 200
        when 'pm25' then 50
        when 'so2' then 197
    end
{%-endmacro%}
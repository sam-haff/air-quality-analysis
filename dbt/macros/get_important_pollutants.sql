{#
Returns maximum safe value for the selected pollutant
#}

{% macro get_important_pollutants(param) -%}
    ('co', 'o3', 'pm10', 'no2','pm25', 'so2')
{%-endmacro%}
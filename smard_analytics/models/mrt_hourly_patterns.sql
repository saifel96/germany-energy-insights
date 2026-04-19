{{ config(materialized='table') }}

select
    hour,
    round(avg(solar), 2) as avg_solar_mw,
    round(avg(wind_onshore + wind_offshore), 2) as avg_wind_mw,
    round(avg(brown_coal + hard_coal), 2) as avg_coal_mw,
    round(avg(total_consumption), 2) as avg_load_mw
from {{ ref('fct_renewable_share') }}
group by 1
order by 1
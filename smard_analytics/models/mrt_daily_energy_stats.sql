{{ config(materialized='table') }}

select
    date,
    round(avg(renewable_share_pct), 2) as avg_renewable_share_pct,
    round(sum(total_renewables), 2) as total_renewables_mw,
    round(sum(total_consumption), 2) as total_consumption_mw,
    -- Eine einfache Logik für das Dashboard
    case 
        when avg(renewable_share_pct) >= 60 then 'High Renewables'
        when avg(renewable_share_pct) >= 40 then 'Balanced'
        else 'Fossil Dominant'
    end as day_type
from {{ ref('fct_renewable_share') }}
group by 1
order by 1
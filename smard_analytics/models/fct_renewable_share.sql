{{ config(materialized='table') }}

with staging as (
    select * from {{ ref('stg_energy_data') }}
),

pivoted as (
    select
        timestamp,
        date,
        hour,
        -- Erneuerbare
        coalesce(max(case when category = 'Wind Onshore' then megawatt end), 0) as wind_onshore,
        coalesce(max(case when category = 'Wind Offshore' then megawatt end), 0) as wind_offshore,
        coalesce(max(case when category = 'Photovoltaik' then megawatt end), 0) as solar,
        coalesce(max(case when category = 'Biomasse' then megawatt end), 0) as biomass,
        coalesce(max(case when category = 'Wasserkraft' then megawatt end), 0) as hydro,
        coalesce(max(case when category = 'Sonstige Erneuerbare' then megawatt end), 0) as other_renewables,
        
        -- Fossile / Konventionelle
        coalesce(max(case when category = 'Braunkohle' then megawatt end), 0) as brown_coal,
        coalesce(max(case when category = 'Steinkohle' then megawatt end), 0) as hard_coal,
        coalesce(max(case when category = 'Erdgas' then megawatt end), 0) as gas,
        coalesce(max(case when category = 'Kernenergie' then megawatt end), 0) as nuclear,
        coalesce(max(case when category = 'Pumpspeicher' then megawatt end), 0) as pumped_storage,
        coalesce(max(case when category = 'Sonstige Konventionelle' then megawatt end), 0) as other_conventional,
        
        -- Last
        coalesce(max(case when category = 'Gesamt (Netzlast)' then megawatt end), 0) as total_consumption
    from staging
    group by 1, 2, 3
)

select
    *,
    -- Komplette Erneuerbare Summe
    (wind_onshore + wind_offshore + solar + biomass + hydro + other_renewables) as total_renewables,
    
    -- Komplette Fossile Summe
    (brown_coal + hard_coal + gas + nuclear + other_conventional) as total_fossils,
    
    -- Der wahre Anteil
    round(
        safe_divide(
            (wind_onshore + wind_offshore + solar + biomass + hydro + other_renewables), 
            total_consumption
        ) * 100, 2
    ) as renewable_share_pct
from pivoted
-- Wir filtern nur Zeilen aus, wo wir wirklich Verbrauchsdaten haben
where total_consumption > 0
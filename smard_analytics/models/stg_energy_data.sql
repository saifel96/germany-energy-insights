{{ config(materialized='view') }}

with source_data as (
    select 
        category,
        timeseries,
        -- Wir benennen es direkt hier um, um Konflikte zu vermeiden
        value as mw_value
    from {{ source('smard_raw_data', 'smard_partitioned') }}
)

select
    category,
    timeseries as timestamp,
    mw_value as megawatt,
    extract(date from timeseries) as date,
    extract(hour from timeseries) as hour
from source_data
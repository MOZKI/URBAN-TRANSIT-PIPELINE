with source as (
    select * from {{ source('staging', 'stg_bus_stops_raw') }}
),

deduped as (
    select
        *,
        row_number() over (partition by bus_stop_code order by bus_stop_code) as rn
    from source
    where bus_stop_code is not null
)

select
    bus_stop_code,
    road_name,
    description as stop_name,
    latitude,
    longitude
from deduped
where rn = 1
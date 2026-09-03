{% snapshot dim_bus_stops_snapshot %}

{{
    config(
        target_schema='gold',
        unique_key="service_no || '-' || direction || '-' || bus_stop_code",
        strategy='check',
        check_cols=['stop_sequence', 'road_name', 'stop_name', 'latitude', 'longitude'],
    )
}}

select
    r.service_no,
    r.direction,
    r.bus_stop_code,
    r.stop_sequence,
    s.road_name,
    s.stop_name,
    s.latitude,
    s.longitude
from {{ ref('stg_bus_routes') }} r
left join {{ ref('stg_bus_stops') }} s
    on r.bus_stop_code = s.bus_stop_code

{% endsnapshot %}
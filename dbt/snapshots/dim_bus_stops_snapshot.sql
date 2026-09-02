{% snapshot dim_bus_stops_snapshot %}

{{
    config(
        target_schema='gold',
        unique_key="service_no || '-' || direction || '-' || bus_stop_code",
        strategy='check',
        check_cols=['stop_sequence'],
    )
}}

select
    service_no,
    direction,
    bus_stop_code,
    stop_sequence
from {{ ref('stg_bus_routes') }}

{% endsnapshot %}
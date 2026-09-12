select
    service_no,
    direction,
    bus_stop_code,
    stop_sequence,
    road_name,
    stop_name,
    latitude,
    longitude,
    dbt_valid_from as effective_since
from {{ ref('dim_bus_stops_snapshot') }}
where dbt_valid_to is null
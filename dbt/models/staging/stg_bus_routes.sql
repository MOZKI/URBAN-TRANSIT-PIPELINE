select
    service_no,
    direction,
    bus_stop_code,
    stop_sequence
from {{ source('staging', 'stg_bus_routes_raw') }}
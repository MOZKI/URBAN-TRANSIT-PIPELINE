select
    arrival_id,
    bus_stop_code,
    service_no,
    direction,
    event_ts,
    date_trunc('hour', event_ts) as window_ts,
    period_bucket,
    headway_actual_seconds,
    headway_gap_seconds,
    expected_low_minutes,
    expected_high_minutes,
    is_within_reliable_range,
    next_bus_load
from {{ ref('stg_bus_arrival') }}
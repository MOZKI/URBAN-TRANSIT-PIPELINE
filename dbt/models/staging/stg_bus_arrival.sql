with source as (
    select * from {{ source('staging', 'stg_bus_arrival_raw') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by bus_stop_code, service_no, next_bus_eta, ingested_at
            order by event_ts desc
        ) as rn
    from source
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['bus_stop_code', 'service_no', 'next_bus_eta', 'ingested_at']
        ) }} as arrival_id,

        bus_stop_code,
        service_no,
        direction,
        operator,
        origin_code,
        destination_code,
        next_bus_load,

        cast(next_bus_eta as timestamp)  as next_bus_eta_ts,
        cast(next_bus2_eta as timestamp) as next_bus2_eta_ts,
        event_ts,

        period_bucket,
        headway_actual_seconds,
        headway_gap_seconds,
        expected_high_minutes,
        case period_bucket
            when 'am_peak'     then am_peak_freq_low
            when 'am_offpeak'  then am_offpeak_freq_low
            when 'pm_peak'     then pm_peak_freq_low
            else pm_offpeak_freq_low
        end as expected_low_minutes

    from deduped
    where rn = 1
)

select
    *,
    case
        when expected_low_minutes is null or expected_high_minutes is null then null
        when headway_actual_seconds between (expected_low_minutes * 60) and (expected_high_minutes * 60) then true
        else false
    end as is_within_reliable_range
from renamed
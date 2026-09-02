with source as (
    select * from {{ source('staging', 'stg_bus_services_raw') }}
)

select
    service_no,
    direction,
    category,
    loop_desc,
    cast(split_part(am_peak_freq, '-', 1) as double)     as am_peak_freq_low,
    cast(split_part(am_peak_freq, '-', 2) as double)     as am_peak_freq_high,
    cast(split_part(am_offpeak_freq, '-', 1) as double)  as am_offpeak_freq_low,
    cast(split_part(am_offpeak_freq, '-', 2) as double)  as am_offpeak_freq_high,
    cast(split_part(pm_peak_freq, '-', 1) as double)     as pm_peak_freq_low,
    cast(split_part(pm_peak_freq, '-', 2) as double)     as pm_peak_freq_high,
    cast(split_part(pm_offpeak_freq, '-', 1) as double)  as pm_offpeak_freq_low,
    cast(split_part(pm_offpeak_freq, '-', 2) as double)  as pm_offpeak_freq_high
from source
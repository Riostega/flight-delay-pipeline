{{ config(severity='warn') }}

-- No arrival in the data is more than 56 minutes late. The limit is in the raw
-- AviationStack payload, not in dbt: arrival.actual, actual_runway and the
-- source's own delay all stop there. The most likely cause is that the API does
-- not list very late flights as "landed" (not yet confirmed), which means late
-- rates are lower bounds.
--
-- This WARNS (it does not fail) when an arrival more than 60 minutes late shows
-- up, which would mean the API's behaviour changed and the censoring caveat in
-- the docs needs revisiting. Warnings do not fail the DAG or alert Slack, so
-- look for it in the dbt build output, e.g. before an analysis re-run.
select max(arrival_delay_minutes) as max_arrival_delay
from {{ ref('fct_flight_events') }}
where not has_suspect_times
having max(arrival_delay_minutes) > 60

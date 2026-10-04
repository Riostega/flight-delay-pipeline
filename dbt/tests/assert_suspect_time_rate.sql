-- Records whose times cannot be trusted as a pair are a known defect in the
-- source, not a fault here, so the fact table flags them (has_suspect_times)
-- rather than dropping them or failing the build. Most are flights whose arrival
-- schedule the source overwrote with the actual landing time after a long
-- departure delay; a few carry a schedule from a different leg or day; on a
-- daylight-saving night some carry a local time in the repeated hour. What has
-- to stay true is that they remain rare.
--
-- has_retimed_arrival_schedule (the milder re-timing cases) is deliberately not
-- counted here: it is a heuristic, and far more common in cargo-heavy pulls.
--
-- A handful of bad records is the source having a bad day. One percent would
-- mean something systematic — a timezone conversion gone wrong, a parsing fault,
-- or the API changing how it reports schedules — and that is worth stopping for.
--
-- Deliberately a rate rather than a count: the table grows every other day, so a
-- fixed threshold would tighten over time until it fired for no reason.
with scored as (

    select
        count(*)                                                   as total_flights,
        sum(case when has_suspect_times then 1 else 0 end)          as suspect_flights
    from {{ ref('fct_flight_events') }}

)

select
    total_flights,
    suspect_flights,
    round(100.0 * suspect_flights / nullif(total_flights, 0), 2)    as suspect_pct
from scored
where suspect_flights > 5
  and suspect_flights > 0.01 * total_flights

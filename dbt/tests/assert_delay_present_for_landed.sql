-- assert_delay_minutes_realistic uses NOT BETWEEN, which evaluates to NULL and
-- therefore never fires for a NULL delay. Nothing else asserts the delay columns
-- are populated, so a record reaching the mart with no actual arrival time is
-- invisible: delay is NULL, is_delayed_arrival is coalesced to false, and the
-- flight is counted as ON TIME. That biases every delay rate downward, in the
-- direction that flatters the result.
--
-- Every row here comes from flight_status = 'landed', so an actual arrival time
-- should exist. A small tolerance is allowed for genuine source gaps; a
-- systematic loss is what this catches.

with counted as (
    select
        count(*)                                                         as total,
        sum(case when arrival_delay_minutes is null then 1 else 0 end)   as missing_arrival,
        sum(case when departure_delay_minutes is null then 1 else 0 end) as missing_departure
    from {{ ref('fct_flight_events') }}
)
select *, round(100.0 * missing_arrival / nullif(total, 0), 2) as pct_missing_arrival
from counted
where total > 0
  and (missing_arrival > total * 0.05 or missing_departure > total * 0.05)

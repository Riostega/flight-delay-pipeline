-- Every other test in this project asks "are these rows wrong?" and passes when
-- there are no rows to be wrong. An empty fct_flight_events therefore passes
-- every other test while the pipeline has silently produced nothing — a broken filter,
-- an empty staging view, or a load that never ran all look identical to health.
--
-- This asserts the one thing the others cannot: that the table exists AND that
-- the weather join produced at least one match. A zero-match table means the
-- ASOF join or the staleness threshold broke, which no other test would notice
-- because every weather column would simply be NULL and every NULL is allowed.
--
-- On a genuinely fresh clone with no data loaded yet, this fails by design.
-- That is the correct signal: a warehouse with no rows is not ready.

select
    count(*)                                              as total_rows,
    sum(case when has_weather_match then 1 else 0 end)    as weather_matched_rows
from {{ ref('fct_flight_events') }}
having count(*) = 0
    or sum(case when has_weather_match then 1 else 0 end) = 0

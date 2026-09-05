-- A flight cannot arrive before it departs. Compared in UTC, because the source
-- reports local wall time carrying a misleading "+00:00" offset — under that
-- reading, trans-Pacific flights appeared to land before take-off, and the same
-- error silently matched every flight to weather four to seven hours from its
-- real arrival.
--
-- This test is the tripwire for that class of bug returning.
select
    flight_event_key,
    departure_airport,
    arrival_airport,
    departure_actual_utc,
    arrival_actual_utc
from {{ ref('fct_flight_events') }}
where arrival_actual_utc < departure_actual_utc
   or arrival_scheduled_utc < departure_scheduled_utc

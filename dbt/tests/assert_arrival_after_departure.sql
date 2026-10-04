-- A flight cannot arrive before it departs. Compared in UTC, because the source
-- reports local wall time carrying a misleading "+00:00" offset — under that
-- reading, trans-Pacific flights appeared to land before take-off, and the same
-- error silently matched every flight to weather four to seven hours from its
-- real arrival.
--
-- This test is the tripwire for that class of bug returning.
--
-- What it cannot see: a row with a NULL *_utc value compares as NULL, so it is
-- never checked. That happens when the origin sends no timezone (under 1% of
-- rows; assert_missing_timezone_rate keeps it small) and when a local time
-- falls in a repeated or skipped daylight-saving hour (has_dst_ambiguous_time).
-- The second is also excluded explicitly. On the November fall-back night a
-- short hop landing in the SECOND 01:xx would otherwise be read as landing an
-- hour earlier, before it took off, and because S3 replays the row forever the
-- build would fail on every run until the code changed.
select
    flight_event_key,
    departure_airport,
    arrival_airport,
    departure_actual_utc,
    arrival_actual_utc
from {{ ref('fct_flight_events') }}
where not has_dst_ambiguous_time
  and (arrival_actual_utc < departure_actual_utc
    or arrival_scheduled_utc < departure_scheduled_utc)

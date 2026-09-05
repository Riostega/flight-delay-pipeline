-- Delay values far outside operational reality indicate a parsing fault rather
-- than a real event, and this catches them before they reach an average.
--
-- The bounds come from the observed distribution rather than intuition. Across
-- the sample the median arrival is 13 minutes EARLY — airlines pad published
-- schedules — and 99% of flights fall between 60 minutes early and 43 late. The
-- floor sits well outside that at -240, because genuine outliers exist: a DHL
-- freighter scheduled 12h17m for Nagoya to Los Angeles flew it in 9h25m with a
-- jet-stream tailwind and arrived 195 minutes early. Its UTC-derived delay
-- matched its stored delay exactly, so the data was sound and a tighter floor
-- would have been rejecting reality.
--
-- This test is no longer the tripwire for timezone faults. That job belongs to
-- assert_arrival_after_departure, which catches the actual failure mode
-- directly rather than inferring it from an implausible magnitude.
select
    flight_event_key,
    departure_airport,
    arrival_airport,
    departure_delay_minutes,
    arrival_delay_minutes
from {{ ref('fct_flight_events') }}
where departure_delay_minutes not between -240 and 1440
   or arrival_delay_minutes   not between -240 and 1440

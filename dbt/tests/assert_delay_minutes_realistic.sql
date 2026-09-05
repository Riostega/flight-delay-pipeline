-- Delays outside this band indicate a parsing or timezone fault rather than a
-- real operational event: a flight departing more than 3 hours early, or more
-- than 24 hours late, is not a plausible reading.
select
    flight_event_key,
    departure_delay_minutes,
    arrival_delay_minutes
from {{ ref('fct_flight_events') }}
where departure_delay_minutes not between -180 and 1440
   or arrival_delay_minutes   not between -180 and 1440

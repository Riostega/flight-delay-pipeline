-- The fact table's grain key is built from flight designators. If any designator
-- reaches it with inconsistent casing, one physical flight silently becomes two
-- rows, and the `unique` test on flight_event_key cannot detect it because the
-- two keys are genuinely different strings.
--
-- This happened: AviationStack returns the codeshare designator lowercase while
-- every other designator is uppercase, which split 155 of 1,097 flights. The
-- fix normalises in staging; this test is what would have caught it, and what
-- will catch the next designator that arrives in an unexpected case.
--
-- Fails if two rows share a grain key under case folding but not exactly.

select
    upper(operating_flight_iata) as normalized_flight,
    departure_scheduled_local,
    count(*)                                  as rows_sharing_normalized_key,
    count(distinct operating_flight_iata)     as distinct_raw_casings
from {{ ref('fct_flight_events') }}
group by 1, 2
having count(distinct operating_flight_iata) > 1

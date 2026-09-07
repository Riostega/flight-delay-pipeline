-- A row flagged as having a weather match must not carry a stale observation,
-- and a row without a match must not carry weather values. Either would mean
-- the staleness threshold silently stopped being applied.
--
-- HONEST SCOPE: as the model stands, none of these three predicates can be
-- satisfied by any data. has_weather_match is DEFINED as lag <= threshold, the
-- weather columns are all gated on that same flag, and the ASOF match_condition
-- (arrival >= observed) makes a negative lag impossible. So this is a MODEL
-- invariant test, not a data test — it cannot detect bad data, only a future
-- edit that breaks the coupling between the flag, the columns and the join.
--
-- That is still worth having, but it must not be mistaken for coverage of the
-- weather join itself. assert_fact_table_not_empty is what catches the join
-- silently matching nothing.
--
-- The threshold comes from the same variable the model uses. Hardcoding it here
-- would make this test fail on correct data the moment that variable changed.
select
    flight_event_key,
    weather_lag_minutes,
    has_weather_match,
    weather_main
from {{ ref('fct_flight_events') }}
where (has_weather_match and weather_lag_minutes > {{ var('weather_max_lag_minutes', 120) }})
   or (not has_weather_match and weather_main is not null)
   or weather_lag_minutes < 0

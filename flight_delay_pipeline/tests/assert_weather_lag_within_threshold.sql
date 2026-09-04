-- A row flagged as having a weather match must not carry a stale observation,
-- and a row without a match must not carry weather values. Either would mean
-- the staleness threshold silently stopped being applied.
select
    flight_event_key,
    weather_lag_minutes,
    has_weather_match,
    weather_main
from {{ ref('fct_flight_events') }}
where (has_weather_match and weather_lag_minutes > 120)
   or (not has_weather_match and weather_main is not null)
   or weather_lag_minutes < 0

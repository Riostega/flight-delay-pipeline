{{ config(severity='warn') }}

-- Every delay in this project rests on one fact about the source: its "actual"
-- departure and arrival times are RUNWAY times (wheels-off / wheels-on). In
-- every record checked, departure.actual equals departure.actual_runway and
-- arrival.actual equals arrival.actual_runway.
--
-- If the source ever started filling "actual" with gate times, every delay
-- would silently change meaning mid-series: departures would lose the taxi-out,
-- arrivals would gain the taxi-in. This warns when any record disagrees.
--
-- A warning, not an error, because a failing test on the source would stop the
-- fact table being rebuilt at all. Warnings do not alert Slack, so look for it
-- in the dbt build output.
select
    r.source_file,
    f.value:flight.iata::string        as flight_iata,
    f.value:departure.actual::string   as departure_actual,
    f.value:departure.actual_runway::string as departure_actual_runway,
    f.value:arrival.actual::string     as arrival_actual,
    f.value:arrival.actual_runway::string   as arrival_actual_runway
from {{ source('raw', 'stg_flights_raw') }} r,
lateral flatten(input => r.raw_data:data) f
where f.value:departure.actual::string <> f.value:departure.actual_runway::string
   or f.value:arrival.actual::string   <> f.value:arrival.actual_runway::string

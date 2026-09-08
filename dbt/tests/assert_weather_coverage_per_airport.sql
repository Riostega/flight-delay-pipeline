-- assert_fact_table_not_empty only requires ONE matched row in the whole table,
-- so a total weather outage at a single airport passes it. That is a realistic
-- failure: fetch_weather returns None for one bad coordinate or one per-location
-- error, run_weather tolerates it, and the extract still exits 0. The airport
-- then quietly drops out of every weather comparison while its flights keep
-- arriving, and the headline weather-vs-operational question silently narrows
-- to four airports without anything saying so.
--
-- Requires each in-scope airport with a reasonable number of flights to have
-- matched weather on at least a quarter of them. Airports with few flights are
-- exempt so a newly added airport does not fail before its first weather run.

select
    arrival_airport,
    count(*)                                            as flights,
    sum(case when has_weather_match then 1 else 0 end)  as matched,
    round(100.0 * avg(case when has_weather_match then 1 else 0 end), 1) as pct
from {{ ref('fct_flight_events') }}
group by 1
having count(*) >= 50
   and avg(case when has_weather_match then 1 else 0 end) < 0.25

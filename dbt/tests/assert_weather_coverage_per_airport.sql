-- assert_fact_table_not_empty only requires ONE matched row in the whole table,
-- so a total weather outage at a single airport passes it. That is a realistic
-- failure: fetch_weather returns None for one bad coordinate or one per-location
-- error, run_weather tolerates it, and the extract still exits 0. The airport
-- then quietly drops out of every weather comparison while its flights keep
-- arriving, and the headline weather-vs-operational question silently narrows
-- to four airports without anything saying so.
--
-- Requires each in-scope airport to have matched weather on at least half of
-- its flights over the most recent week of data. The window matters: computed
-- over all history, months of good matches would hide a new outage for weeks.
-- The week is measured back from the newest arrival in the table, not from
-- today, so a rebuild of old data (or a stalled flight pull) does not fail it.
-- Airports with fewer than 20 flights in the window are exempt, so a newly
-- added airport does not fail before its first weather run.
--
-- assert_weather_fresh_per_airport catches an outage that is happening right
-- now; this one catches the join itself failing for one airport.

select
    arrival_airport,
    count(*)                                            as flights,
    sum(case when has_weather_match then 1 else 0 end)  as matched,
    round(100.0 * avg(case when has_weather_match then 1 else 0 end), 1) as pct
from {{ ref('fct_flight_events') }}
where arrival_actual_utc >= dateadd(
    day, -7, (select max(arrival_actual_utc) from {{ ref('fct_flight_events') }})
)
group by 1
having count(*) >= 20
   and avg(case when has_weather_match then 1 else 0 end) < 0.5

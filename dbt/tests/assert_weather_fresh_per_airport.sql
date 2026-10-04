-- Severity warn: as an error, this test would make dbt build skip
-- fct_flight_events for ALL airports during a one-airport weather outage, though
-- the fact table already handles missing weather and the watchdog alerts on it.
{{ config(severity='warn') }}

-- Fails when one airport's weather has stopped arriving while the others carry on.
--
-- fetch_weather can fail for one location (a bad coordinate, a per-location
-- error) while the run still exits 0. The airport's flights keep arriving, find
-- no weather, and quietly drop out of every weather comparison. The watchdog
-- alerts on stale weather against the clock; this is the build-time check, so a
-- dbt build never reports green on top of a one-airport outage.
--
-- Compared with the newest observation at any airport rather than with the
-- clock, so a rebuild of old data (or CI) does not fail it, and an outage of
-- every airport at once is left to the watchdog. Weather is fetched hourly, so
-- six hours behind means several fetches in a row have failed. An airport in
-- dim_airports with no weather at all also fails.
select
    a.iata_code,
    max(w.observed_at) as last_observation
from {{ ref('dim_airports') }} a
left join {{ ref('stg_weather') }} w
    on w.iata_code = a.iata_code
group by a.iata_code
having max(w.observed_at) is null
    or max(w.observed_at) < dateadd(
        hour, -6, (select max(observed_at) from {{ ref('stg_weather') }})
    )

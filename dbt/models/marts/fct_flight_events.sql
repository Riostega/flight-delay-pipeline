{{ config(materialized='table') }}

-- Grain: one row per physical flight, per scheduled departure.
--
-- AviationStack returns one record per *marketing* flight number, so a single
-- aircraft appears once for every airline selling seats on it. In one sample,
-- Qatar Airways showed a 41-minute delay on a flight Virgin Australia actually
-- operated. Attributing one aircraft's delay to every marketing carrier would
-- corrupt carrier-level reliability, which is the project's central question,
-- so this model collapses to the operating flight.
--
-- The same key removes re-pull duplicates for free: the extract is a live
-- snapshot, so consecutive runs return overlapping flights, and the same
-- aircraft pulled twice resolves to one row.

with scoped as (

    -- Scope follows dim_airports, which is also what the extract reads. Adding
    -- an airport to that seed brings it into scope on both sides at once.
    select *
    from {{ ref('stg_flights') }}
    where arrival_airport in (select iata_code from {{ ref('dim_airports') }})
      -- Drop records carrying no flight identifier at all. AviationStack
      -- returns airline_name = 'empty' with every designator null when it
      -- cannot identify a flight; such a row has no carrier and no flight
      -- number, so it cannot be attributed to anything this table is about,
      -- and it would null the grain key. assert_unidentified_flight_rate
      -- fails if these ever stop being rare.
      and coalesce(codeshare_flight_iata, flight_iata, flight_icao) is not null

),

attributed as (

    select
        *,
        -- The carrier that actually flew the aircraft. With no codeshare, the
        -- flight operates itself. Private operators carry no IATA designator,
        -- so fall back to ICAO rather than dropping the row.
        coalesce(codeshare_flight_iata, flight_iata, flight_icao) as operating_flight_iata,
        initcap(coalesce(codeshare_airline_name, airline_name))   as operating_carrier_name,

        -- Codeshare names arrive lowercase while direct names are title case;
        -- without initcap the same carrier splits into two groups.
        initcap(airline_name)                                     as marketing_carrier_name,
        coalesce(flight_iata, flight_icao)                        as marketing_flight_iata
    from scoped

),

deduplicated as (

    -- Remove exact re-pulls: same marketing label, same scheduled departure.
    select *
    from attributed
    qualify row_number() over (
        partition by marketing_flight_iata, departure_scheduled_local
        order by arrival_actual_local desc nulls last
    ) = 1

),

physical_flights as (

    -- Collapse marketing labels into one row per physical flight, counting the
    -- labels before discarding them so the codeshare relationship survives as a
    -- measure rather than as duplicated rows.
    select
        *,
        count(*) over (
            partition by operating_flight_iata, departure_scheduled_local
        ) as marketing_label_count
    from deduplicated
    qualify row_number() over (
        partition by operating_flight_iata, departure_scheduled_local
        order by marketing_flight_iata
    ) = 1

),

flight_events as (

    select
        operating_flight_iata || '_' || to_varchar(departure_scheduled_local) as flight_event_key,

        flight_date,
        operating_flight_iata,
        operating_carrier_name,
        marketing_flight_iata,
        marketing_carrier_name,
        marketing_label_count,
        marketing_label_count > 1                                as has_codeshare_partners,
        aircraft_icao24,

        departure_airport,
        arrival_airport,

        departure_timezone,
        arrival_timezone,

        -- Local wall time is what the source reports and what delays are
        -- measured in; UTC is what makes times comparable across airports and
        -- joinable to weather. Both are kept, explicitly named, because
        -- conflating them is precisely the bug this pair exists to prevent.
        departure_scheduled_local,
        departure_actual_local,
        departure_scheduled_utc,
        departure_actual_utc,
        departure_delay_minutes,

        arrival_scheduled_local,
        arrival_actual_local,
        arrival_scheduled_utc,
        arrival_actual_utc,
        arrival_delay_minutes,

        -- Flights routinely recover time in the air because airlines pad
        -- published schedules; measuring arrival delay alone hides origin-side
        -- operational failures.
        departure_delay_minutes - arrival_delay_minutes          as minutes_recovered,

        -- 15 minutes is the US DOT / BTS on-time threshold, which keeps these
        -- figures comparable to published industry statistics.
        coalesce(departure_delay_minutes > 15, false)            as is_delayed_departure,
        coalesce(arrival_delay_minutes > 15, false)              as is_delayed_arrival

    from physical_flights

),

with_weather as (

    -- ASOF JOIN takes the most recent observation at or before each arrival:
    -- weather after landing cannot have caused the delay. It preserves rows
    -- with no match, so flights are never dropped for want of weather.
    select
        f.*,
        w.observed_at                                            as weather_observed_at,
        w.weather_main,
        w.weather_description,
        w.temp_f,
        w.humidity,
        w.wind_speed,
        w.wind_gust,
        w.visibility_m,
        w.cloud_cover_pct,

        -- How stale the matched observation was. Kept on every row so the
        -- quality of each match is visible rather than assumed.
        -- Both sides are true UTC. Previously the flight side was local wall
        -- time treated as UTC, which matched every flight to weather four to
        -- seven hours from its real arrival while this lag still read as
        -- healthy, because both sides were consistently wrong.
        datediff('minute', w.observed_at, f.arrival_actual_utc)   as weather_lag_minutes

    from flight_events f
    asof join {{ ref('stg_weather') }} w
        match_condition (f.arrival_actual_utc >= w.observed_at)
        on f.arrival_airport = w.iata_code

),

scored as (

    -- The staleness threshold is applied once, here. Every weather column below
    -- keys off this flag rather than repeating the comparison, so the columns
    -- and the flag cannot drift apart if the threshold changes.
    select
        *,
        coalesce(
            weather_lag_minutes <= {{ var('weather_max_lag_minutes', 120) }},
            false
        ) as has_weather_match
    from with_weather

)

select
    flight_event_key,
    flight_date,

    operating_flight_iata,
    operating_carrier_name,
    marketing_flight_iata,
    marketing_carrier_name,
    marketing_label_count,
    has_codeshare_partners,
    aircraft_icao24,

    departure_airport,
    arrival_airport,
    departure_timezone,
    arrival_timezone,

    departure_scheduled_local,
    departure_actual_local,
    departure_scheduled_utc,
    departure_actual_utc,
    departure_delay_minutes,

    arrival_scheduled_local,
    arrival_actual_local,
    arrival_scheduled_utc,
    arrival_actual_utc,
    arrival_delay_minutes,
    minutes_recovered,
    is_delayed_departure,
    is_delayed_arrival,

    -- Weather conditions at the arrival airport around landing.
    --
    -- Observations older than the threshold are discarded rather than reported:
    -- weather is collected hourly, so a healthy gap is under an hour, and a
    -- reading many hours stale describes a different time of day entirely. It
    -- would look identical to a good one in the data. weather_lag_minutes is
    -- retained regardless so the freshness of every match stays inspectable.
    weather_observed_at,
    weather_lag_minutes,
    has_weather_match,

    case when has_weather_match then weather_main        end      as weather_main,
    case when has_weather_match then weather_description end      as weather_description,
    case when has_weather_match then temp_f              end      as weather_temp_f,
    case when has_weather_match then humidity            end      as weather_humidity,
    case when has_weather_match then wind_speed          end      as weather_wind_speed,
    case when has_weather_match then wind_gust           end      as weather_wind_gust,
    case when has_weather_match then visibility_m        end      as weather_visibility_m,
    case when has_weather_match then cloud_cover_pct     end      as weather_cloud_cover_pct

from scored

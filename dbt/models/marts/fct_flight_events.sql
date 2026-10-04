-- Grain: one row per physical flight, per scheduled departure.
--
-- AviationStack returns one record per *marketing* flight number, so a single
-- aircraft appears once for every airline selling seats on it. In one sample,
-- Qatar Airways showed a 41-minute delay on a flight Virgin Australia actually
-- operated. Attributing one aircraft's delay to every marketing carrier would
-- corrupt carrier-level reliability, which is the project's central question,
-- so this model collapses to the operating flight.
--
-- Duplicates arrive from two directions and are removed in two steps, because
-- collapsing them together would give the wrong answer:
--
--   1. deduplicated     the extract is a live snapshot, so consecutive runs
--                       return overlapping flights and the same marketing label
--                       is stored more than once. Removed on the marketing key.
--   2. physical_flights several marketing labels describe one aircraft. Removed
--                       on the operating key, counting the labels first.
--
-- Doing only the second would still yield one row per flight, but
-- marketing_label_count would count re-pulls as extra carriers: a flight seen
-- twice under five labels would report six.

with scoped as (

    -- Scope follows dim_airports, which is also what the extract reads. Adding
    -- an airport to that seed brings it into scope on both sides at once.
    select *
    from {{ ref('stg_flights') }}
    where arrival_airport in (select iata_code from {{ ref('dim_airports') }})
      -- Drop records carrying no flight identifier at all. When AviationStack
      -- cannot identify a flight it usually returns airline_name = 'empty'
      -- with every designator null; such a row has no carrier and no flight
      -- number, so it cannot be attributed to anything this table is about,
      -- and it would null the grain key. assert_unidentified_flight_rate
      -- fails if these ever stop being rare.
      -- Not every 'empty' row is dropped: a few keep a designator (XN / XAR),
      -- so they stay, with the carrier name set to 'Unknown' below.
      and coalesce(codeshare_flight_iata, flight_iata, flight_icao) is not null

),

attributed as (

    select
        *,
        -- The flight that actually flew the aircraft. With no codeshare, the
        -- flight operates itself. Private operators carry no IATA designator,
        -- so fall back to ICAO rather than dropping the row. Some private and
        -- cargo designators carry no flight number at all ('1I', 'CAO'), so
        -- their key is just the prefix plus the scheduled minute;
        -- assert_grain_key_single_route fails if that ever merges two flights.
        coalesce(codeshare_flight_iata, flight_iata, flight_icao) as operating_flight_iata,

        -- The code of the airline that actually flew. This, not the name, is
        -- the carrier identity: AviationStack puts the mainline BRAND in the
        -- name fields, so a Republic (YX) or SkyWest (OO) regional flight is
        -- named "United Airlines" or "American Airlines", while its code says
        -- who operated it. Codes also merge livery variants that the free-text
        -- name splits ("Alaska Airlines (Oneworld Livery)").
        -- Usually the two-letter IATA code; for operators with none it is the
        -- three-letter ICAO code (Flexjet LXJ, PlaneSense CNS).
        case
            when codeshare_flight_iata is not null
                then coalesce(codeshare_airline_iata, left(codeshare_flight_iata, 2))
            else coalesce(airline_iata, airline_icao, left(flight_iata, 2), left(flight_icao, 3))
        end                                                       as operating_carrier_code,

        -- The brand the operating flight is sold under, for display. Codeshare
        -- names arrive lowercase while direct names are title case; without
        -- initcap the same carrier splits into two groups. The source's
        -- placeholder 'empty' becomes 'Unknown' rather than a carrier called
        -- "Empty".
        coalesce(
            initcap(nullif(lower(coalesce(codeshare_airline_name, airline_name)), 'empty')),
            'Unknown'
        )                                                         as operating_carrier_name,

        coalesce(initcap(nullif(lower(airline_name), 'empty')), 'Unknown') as marketing_carrier_name,
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

weather as (

    -- OpenWeatherMap reports the station's observation time, not the fetch
    -- time, so fetching more often than the station updates stores the same
    -- measurement repeatedly. Readings have always agreed, but leaving several
    -- identical rows per timestamp would make the as-of match pick among them
    -- arbitrarily. One row per airport per observation, newest file wins.
    select *
    from {{ ref('stg_weather') }}
    where iata_code is not null
    qualify row_number() over (
        partition by iata_code, observed_at
        order by source_file desc
    ) = 1

),

flight_events as (

    select
        -- The timestamp format is spelled out so the key does not depend on the
        -- session's TIMESTAMP_NTZ_OUTPUT_FORMAT. It is the format the keys have
        -- always had, so keys saved in exports still join.
        operating_flight_iata || '_'
            || to_varchar(departure_scheduled_local, 'YYYY-MM-DD HH24:MI:SS.FF3') as flight_event_key,

        flight_date,
        operating_flight_iata,
        operating_carrier_code,
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

        -- Local wall time is what the source reports; UTC is what makes times
        -- comparable across airports and joinable to weather, and what delays
        -- are measured in when the zone is known. Both are kept, explicitly
        -- named, because conflating them is precisely the bug this pair exists
        -- to prevent. *_actual_* are runway times, *_scheduled_* gate times.
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

        source_departure_delay_minutes,
        source_arrival_delay_minutes,
        has_dst_ambiguous_time,

        -- Rows whose times cannot be trusted as a pair. The times are
        -- individually plausible and jointly impossible, so the row is flagged
        -- rather than dropped. Three causes:
        --
        --   (a) a schedule that belongs to a different leg or a different day.
        --       PXG210 on 2026-09-25 "departed" five and a half hours early —
        --       it flew the night before, against the next night's schedule.
        --       UA7 was scheduled thirteen hours for a flight it made in three.
        --   (b) the source re-timing arrival.scheduled to the actual landing
        --       after a long departure delay. The flight leaves hours late and
        --       "arrives on time" (GB3190: 333 min late off, 0 min late on).
        --       This is most of the flagged rows; the milder cases are marked
        --       by has_retimed_arrival_schedule below instead.
        --   (c) a local time in the hour a daylight-saving change repeats or
        --       skips, so the true instant, and any delay using it, may be off
        --       by an hour (has_dst_ambiguous_time, from staging).
        --
        -- Two rules catch (a) and (b), each with its own reason:
        --
        --   a departure three hours early is not an operational event. An hour
        --   is: freight and charter regularly leave ahead of schedule once the
        --   load is closed, and those rows are internally consistent, arriving
        --   early by about as much as they left early. Three hours is not.
        --
        --   arrivals genuinely can be hours early on a tailwind, so those are
        --   judged on elapsed time instead: if the duration the schedule
        --   implies differs from the duration actually flown by more than four
        --   hours, the schedule is describing a different journey.
        --   (actual elapsed - scheduled elapsed) is the same number as
        --   (arrival delay - departure delay), because each airport's own clock
        --   offset cancels out. Writing it with the delays means it also works
        --   for the rows whose origin sends no timezone, which a UTC version
        --   silently skipped (6R4260 on 2026-09-04 left 266 min late and
        --   "arrived on time").
        --
        -- Four hours clears the widest genuine case in the sample: a freighter
        -- scheduled 12h17m that flew 9h25m with the jet stream behind it.
        coalesce(departure_delay_minutes < -180, false)
        or coalesce(abs(arrival_delay_minutes - departure_delay_minutes) > 240, false)
        or has_dst_ambiguous_time                                as has_suspect_times,

        -- Cause (b) above, below the four-hour line. The signature is a flight
        -- that left an hour or more late yet "arrived" within a minute of its
        -- schedule, because the source moved the schedule onto the landing.
        -- Arrival delays of exactly 0 or 1 minute are two to three times as
        -- common as their neighbours, almost all of the excess from cargo and
        -- business-jet operators. A heuristic: a genuine flight that made up
        -- an hour and landed on the minute is also caught. Kept separate from
        -- has_suspect_times so that flag's meaning and the 1% rate test do
        -- not change; using it in an analysis is a deliberate choice.
        coalesce(
            departure_delay_minutes >= 60 and abs(arrival_delay_minutes) <= 1,
            false
        )                                                        as has_retimed_arrival_schedule,

        -- Scheduled gate-to-gate time minus actual runway-to-runway time
        -- (because the actual times are wheels-off and wheels-on). So it is NOT
        -- time made up in the air: it includes the taxi-out and the taxi-in,
        -- plus whatever padding the schedule has. Taxiing is most of the
        -- typical value, so treat it as an upper bound on schedule padding.
        departure_delay_minutes - arrival_delay_minutes          as minutes_recovered,

        -- 15 minutes is the US DOT / BTS threshold, and the comparison is >=
        -- rather than >: BTS counts a flight as delayed when it is 15 minutes
        -- OR MORE behind schedule, so exactly 15 minutes late is late.
        --
        -- The threshold matches BTS, the measurement does not. BTS compares
        -- GATE arrival with the schedule; this table can only compare WHEELS-ON,
        -- which is earlier by the taxi-in time. So is_delayed_arrival reads
        -- lower than a BTS delay rate for the same flights, and taxi-in
        -- differs by airport. To benchmark against BTS, compare with BTS
        -- WheelsOn minus CRSArrTime, not ArrDelay.
        --
        -- is_delayed_departure is wheels-off 15+ minutes after the scheduled
        -- push-back. That includes the whole taxi-out, so most flights qualify
        -- and it is not a lateness measure.
        coalesce(departure_delay_minutes >= 15, false)           as is_delayed_departure,
        coalesce(arrival_delay_minutes >= 15, false)             as is_delayed_arrival

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
    asof join weather w
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
            weather_lag_minutes <= {{ var('weather_max_lag_minutes') }},
            false
        ) as has_weather_match
    from with_weather

)

select
    flight_event_key,
    flight_date,

    operating_flight_iata,
    operating_carrier_code,
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

    -- The source's own delay figures, kept for reference. They are not an
    -- independent reading: see the note in stg_flights.
    source_departure_delay_minutes,
    source_arrival_delay_minutes,
    has_suspect_times,
    has_retimed_arrival_schedule,
    has_dst_ambiguous_time,

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

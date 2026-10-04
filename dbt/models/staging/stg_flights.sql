-- One row per marketing flight label per file.
--
-- Staging only flattens and casts. Deduplication, codeshare collapse, and
-- carrier attribution are modelling decisions and live in fct_flight_events.
--
-- Source quirks handled or documented here:
--
-- 1. A JSON null inside a VARIANT is not SQL NULL, so codeshare fields are cast
--    with ::string before any null check. Without the cast, `codeshared IS NOT
--    NULL` reports every record as a codeshare.
--
-- 2. AviationStack timestamps carry a "+00:00" offset but are NOT UTC. They are
--    local wall time at the airport, and the real zone is in a separate
--    `timezone` field. Casting them straight to timestamp_tz therefore produces
--    times that are wrong by the airport's offset — which made trans-Pacific
--    flights appear to arrive before they departed, and matched flights to
--    weather four to seven hours away from their real arrival.
--
--    Both readings are exposed rather than one:
--      *_local  the wall time as reported, always present
--      *_utc    true UTC, for comparing across airports and joining to weather.
--               Null when the source omits the timezone (a small number of
--               departure records), or when the wall time falls in the hour a
--               daylight-saving change repeats or skips (see macros/local_time.sql).
--
-- 3. The "actual" times are RUNWAY times, while the scheduled times are GATE
--    times. In every raw record checked (10,010 of 10,010 on 2026-10-04)
--    departure.actual equals departure.actual_runway and arrival.actual equals
--    arrival.actual_runway. So:
--      departure delay = wheels-off minus scheduled push-back. It includes the
--                        whole taxi-out, which is why most departures look late.
--      arrival delay   = wheels-on minus scheduled gate arrival. It leaves out
--                        taxi-in, so flights look a few minutes earlier than they
--                        reached the gate.
--    The feed carries no actual gate times: departure.estimated just repeats the
--    schedule, and arrival.estimated is close to the runway time.
--    assert_actual_is_runway_time warns if this ever changes.
--
-- 4. No arrival in the raw data is more than 56 minutes late. The limit is in
--    the payload itself (arrival.actual, actual_runway and delay all stop
--    there), not created here, and it does not depend on how long before the
--    pull a flight was scheduled. The most likely cause is that the API never
--    lists very late flights as "landed"; that is not yet confirmed. Either way
--    the heaviest delays are missing, so late rates are lower bounds.

with flattened as (

    select
        f.value:flight_date::date                            as flight_date,
        f.value:flight_status::string                        as flight_status,

        f.value:airline.name::string                         as airline_name,
        f.value:airline.iata::string                         as airline_iata,
        -- ICAO code of the record's airline. Private operators often have no
        -- IATA code, so this is the fallback identity for them.
        f.value:airline.icao::string                         as airline_icao,
        f.value:flight.iata::string                          as flight_iata,
        f.value:flight.icao::string                          as flight_icao,

        -- Populated when this record is a marketing label for a flight operated
        -- by another carrier.
        -- Upper-cased at the boundary. AviationStack returns the codeshare
        -- designator lowercase ('dl1589') while every sibling designator —
        -- flight_iata, flight_icao, airline_iata — arrives uppercase. IATA
        -- designators are uppercase by definition, so this is a source defect, not
        -- a modelling choice, and normalising it here fixes every consumer at once.
        -- Left as-is it silently splits the grain: the fact table keys on
        -- coalesce(codeshare_flight_iata, flight_iata, ...), so one physical flight
        -- became 'dl1589_...' in some rows and 'DL1589_...' in others, and the
        -- unique test on flight_event_key could not see it because those are
        -- genuinely different strings.
        upper(f.value:flight.codeshared.flight_iata::string) as codeshare_flight_iata,
        f.value:flight.codeshared.airline_name::string       as codeshare_airline_name,
        -- The operating airline's code, upper-cased for the same reason. Unlike
        -- the name, this names the airline that actually flew: for a regional
        -- flight the name says "united airlines" while the code says YX
        -- (Republic) or OO (SkyWest).
        upper(f.value:flight.codeshared.airline_iata::string) as codeshare_airline_iata,

        f.value:aircraft.icao24::string                      as aircraft_icao24,

        f.value:departure.iata::string                       as departure_airport,
        f.value:departure.timezone::string                   as departure_timezone,
        f.value:arrival.iata::string                         as arrival_airport,
        f.value:arrival.timezone::string                     as arrival_timezone,

        -- Local wall time, with the misleading offset discarded by casting to NTZ.
        -- *_actual_* are runway times (wheels-off / wheels-on); see note 3.
        f.value:departure.scheduled::string::timestamp_ntz   as departure_scheduled_local,
        f.value:departure.actual::string::timestamp_ntz      as departure_actual_local,
        f.value:arrival.scheduled::string::timestamp_ntz     as arrival_scheduled_local,
        f.value:arrival.actual::string::timestamp_ntz        as arrival_actual_local,

        -- The source publishes its own delay figures. They are kept for
        -- reference only, as source_* columns, and are NOT an independent check
        -- on the computed delays:
        --   departure.delay often repeats the ARRIVAL delay instead (408 of
        --     1,936 populated rows in fct on 2026-10-04).
        --   arrival.delay is floored at 0, so it disagrees with the computed
        --     value for every early arrival and never otherwise.
        -- The source can also be internally inconsistent: UA7 on 2026-09-16
        -- reported a delay of 0 alongside a scheduled arrival that implied the
        -- flight landed ten hours early.
        try_to_number(f.value:departure.delay::string)       as source_departure_delay_minutes,
        try_to_number(f.value:arrival.delay::string)         as source_arrival_delay_minutes,

        -- Lineage back to the exact S3 object this row came from.
        r.source_file

    from {{ source('raw', 'stg_flights_raw') }} r,
    lateral flatten(input => r.raw_data:data) f

),

converted as (

    -- The same instants in true UTC, using the zone the API reports separately.
    -- local_to_utc returns NULL instead of guessing when the zone is missing or
    -- the wall time falls in a repeated or skipped daylight-saving hour.
    select
        *,
        {{ local_to_utc('departure_timezone', 'departure_scheduled_local') }} as departure_scheduled_utc,
        {{ local_to_utc('departure_timezone', 'departure_actual_local') }}    as departure_actual_utc,
        {{ local_to_utc('arrival_timezone', 'arrival_scheduled_local') }}     as arrival_scheduled_utc,
        {{ local_to_utc('arrival_timezone', 'arrival_actual_local') }}        as arrival_actual_utc,

        -- True when any of the four times falls in such an hour. Its true
        -- instant is unknowable from this feed, so any delay involving it may
        -- be off by an hour. fct_flight_events counts these as suspect.
        {{ is_dst_ambiguous('departure_timezone', 'departure_scheduled_local') }}
        or {{ is_dst_ambiguous('departure_timezone', 'departure_actual_local') }}
        or {{ is_dst_ambiguous('arrival_timezone', 'arrival_scheduled_local') }}
        or {{ is_dst_ambiguous('arrival_timezone', 'arrival_actual_local') }}  as has_dst_ambiguous_time
    from flattened

)

select
    flight_date,
    flight_status,

    airline_name,
    airline_iata,
    airline_icao,
    flight_iata,
    flight_icao,
    codeshare_flight_iata,
    codeshare_airline_name,
    codeshare_airline_iata,

    aircraft_icao24,

    departure_airport,
    departure_timezone,
    arrival_airport,
    arrival_timezone,

    departure_scheduled_local,
    departure_actual_local,
    arrival_scheduled_local,
    arrival_actual_local,

    departure_scheduled_utc,
    departure_actual_utc,
    arrival_scheduled_utc,
    arrival_actual_utc,
    has_dst_ambiguous_time,

    -- Delay is measured in UTC where possible, falling back to local wall time
    -- when the zone is missing. Scheduled and actual share one airport, so on
    -- almost every row the two give the same answer. They differ on a night
    -- the clocks change: scheduled 00:50 and landed 02:10 on the November
    -- fall-back is 140 real minutes, but only 80 on the wall clock. If either
    -- time is in the repeated or skipped hour, UTC is NULL and the local
    -- difference is used; has_dst_ambiguous_time marks those rows.
    -- Both delays compare a runway time with a gate time (see note 3).
    coalesce(
        datediff('minute', departure_scheduled_utc, departure_actual_utc),
        datediff('minute', departure_scheduled_local, departure_actual_local)
    )                                                        as departure_delay_minutes,
    coalesce(
        datediff('minute', arrival_scheduled_utc, arrival_actual_utc),
        datediff('minute', arrival_scheduled_local, arrival_actual_local)
    )                                                        as arrival_delay_minutes,

    source_departure_delay_minutes,
    source_arrival_delay_minutes,

    source_file

from converted

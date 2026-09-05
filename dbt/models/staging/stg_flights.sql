-- One row per marketing flight label per file.
--
-- Staging only flattens and casts. Deduplication, codeshare collapse, and
-- carrier attribution are modelling decisions and live in fct_flight_events.
--
-- Two source quirks are handled here:
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
--      *_local  the wall time as reported, always present, and what a delay is
--               naturally measured in (both sides share one airport and zone)
--      *_utc    true UTC, for comparing across airports and joining to weather.
--               Null when the source omits the timezone, which happens for a
--               small number of departure records.

select
    f.value:flight_date::date                            as flight_date,
    f.value:flight_status::string                        as flight_status,

    f.value:airline.name::string                         as airline_name,
    f.value:airline.iata::string                         as airline_iata,
    f.value:flight.iata::string                          as flight_iata,
    f.value:flight.icao::string                          as flight_icao,

    -- Populated when this record is a marketing label for a flight operated
    -- by another carrier.
    f.value:flight.codeshared.flight_iata::string        as codeshare_flight_iata,
    f.value:flight.codeshared.airline_name::string       as codeshare_airline_name,

    f.value:aircraft.icao24::string                      as aircraft_icao24,

    f.value:departure.iata::string                       as departure_airport,
    f.value:departure.timezone::string                   as departure_timezone,
    f.value:arrival.iata::string                         as arrival_airport,
    f.value:arrival.timezone::string                     as arrival_timezone,

    -- Local wall time, with the misleading offset discarded by casting to NTZ.
    f.value:departure.scheduled::string::timestamp_ntz   as departure_scheduled_local,
    f.value:departure.actual::string::timestamp_ntz      as departure_actual_local,
    f.value:arrival.scheduled::string::timestamp_ntz     as arrival_scheduled_local,
    f.value:arrival.actual::string::timestamp_ntz        as arrival_actual_local,

    -- The same instants in true UTC, using the zone the API reports separately.
    convert_timezone(f.value:departure.timezone::string, 'UTC',
        f.value:departure.scheduled::string::timestamp_ntz)  as departure_scheduled_utc,
    convert_timezone(f.value:departure.timezone::string, 'UTC',
        f.value:departure.actual::string::timestamp_ntz)     as departure_actual_utc,
    convert_timezone(f.value:arrival.timezone::string, 'UTC',
        f.value:arrival.scheduled::string::timestamp_ntz)    as arrival_scheduled_utc,
    convert_timezone(f.value:arrival.timezone::string, 'UTC',
        f.value:arrival.actual::string::timestamp_ntz)       as arrival_actual_utc,

    -- Delay is measured in local time on purpose. Scheduled and actual share an
    -- airport and therefore a zone, so the difference is identical either way —
    -- but local is always available, while UTC is not when the zone is missing.
    -- AviationStack's own `delay` field is inconsistently populated and unused.
    datediff('minute', f.value:departure.scheduled::string::timestamp_ntz,
                       f.value:departure.actual::string::timestamp_ntz)  as departure_delay_minutes,
    datediff('minute', f.value:arrival.scheduled::string::timestamp_ntz,
                       f.value:arrival.actual::string::timestamp_ntz)    as arrival_delay_minutes,

    -- Lineage back to the exact S3 object this row came from.
    r.source_file

from {{ source('raw', 'stg_flights_raw') }} r,
lateral flatten(input => r.raw_data:data) f

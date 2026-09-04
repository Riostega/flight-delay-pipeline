-- One row per marketing flight label per file.
--
-- Staging only flattens and casts. Deduplication, codeshare collapse, and
-- carrier attribution are modelling decisions and live in fct_flight_events.
--
-- Note: codeshare fields are cast with ::string before use. A JSON null inside
-- a VARIANT is not SQL NULL, so `codeshared IS NOT NULL` is true even when the
-- API returned null; casting first is what makes null checks behave.

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
    f.value:departure.scheduled::timestamp_tz            as departure_scheduled,
    f.value:departure.actual::timestamp_tz               as departure_actual,

    f.value:arrival.iata::string                         as arrival_airport,
    f.value:arrival.scheduled::timestamp_tz              as arrival_scheduled,
    f.value:arrival.actual::timestamp_tz                 as arrival_actual,

    -- AviationStack's own `delay` field is inconsistently populated, so delay
    -- is derived from timestamps instead. Positive = late, negative = early.
    datediff('minute', f.value:departure.scheduled::timestamp_tz,
                       f.value:departure.actual::timestamp_tz)  as departure_delay_minutes,
    datediff('minute', f.value:arrival.scheduled::timestamp_tz,
                       f.value:arrival.actual::timestamp_tz)    as arrival_delay_minutes,

    -- Lineage back to the exact S3 object this row came from.
    r.source_file

from {{ source('raw', 'stg_flights_raw') }} r,
lateral flatten(input => r.raw_data:data) f

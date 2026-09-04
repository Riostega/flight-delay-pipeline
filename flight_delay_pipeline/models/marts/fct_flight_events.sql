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
        partition by marketing_flight_iata, departure_scheduled
        order by arrival_actual desc nulls last
    ) = 1

),

physical_flights as (

    -- Collapse marketing labels into one row per physical flight, counting the
    -- labels before discarding them so the codeshare relationship survives as a
    -- measure rather than as duplicated rows.
    select
        *,
        count(*) over (
            partition by operating_flight_iata, departure_scheduled
        ) as marketing_label_count
    from deduplicated
    qualify row_number() over (
        partition by operating_flight_iata, departure_scheduled
        order by marketing_flight_iata
    ) = 1

)

select
    operating_flight_iata || '_' || to_varchar(departure_scheduled) as flight_event_key,

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

    departure_scheduled,
    departure_actual,
    departure_delay_minutes,

    arrival_scheduled,
    arrival_actual,
    arrival_delay_minutes,

    -- Flights routinely recover time in the air because airlines pad published
    -- schedules; measuring arrival delay alone hides origin-side failures.
    departure_delay_minutes - arrival_delay_minutes          as minutes_recovered,

    -- 15 minutes is the US DOT / BTS on-time threshold, which keeps these
    -- figures comparable to published industry statistics.
    coalesce(departure_delay_minutes > 15, false)            as is_delayed_departure,
    coalesce(arrival_delay_minutes > 15, false)              as is_delayed_arrival

from physical_flights

-- The grain key is flight designator + scheduled departure minute. That is
-- unique for a numbered flight, but some private and cargo operators send a
-- designator with no flight number ('1I' for NetJets, 'CAO'). Two such flights
-- scheduled for the same minute would share a key, and the dedup in
-- fct_flight_events would silently merge them into one row; the unique test on
-- flight_event_key cannot see a merge, only a duplicate.
--
-- A merged key would cover two different routes, so fail when one key (on
-- either the operating or the marketing designator, the two keys the model
-- deduplicates on) appears with more than one origin/destination pair. If this
-- ever fires, add the origin to the key for flights with no number.
with scoped as (
    select *
    from {{ ref('stg_flights') }}
    where arrival_airport in (select iata_code from {{ ref('dim_airports') }})
      and coalesce(codeshare_flight_iata, flight_iata, flight_icao) is not null
),

keys as (
    select 'operating' as key_type,
           coalesce(codeshare_flight_iata, flight_iata, flight_icao) as designator,
           departure_scheduled_local, departure_airport, arrival_airport
    from scoped
    union all
    select 'marketing',
           coalesce(flight_iata, flight_icao),
           departure_scheduled_local, departure_airport, arrival_airport
    from scoped
)

select
    key_type,
    designator,
    departure_scheduled_local,
    count(distinct departure_airport, arrival_airport) as routes
from keys
group by 1, 2, 3
having count(distinct departure_airport, arrival_airport) > 1

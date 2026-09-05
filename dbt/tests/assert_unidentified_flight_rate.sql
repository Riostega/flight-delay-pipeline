-- fct_flight_events drops records with no flight identifier. That is fine while
-- they are rare, but silent dropping hides a degrading source. Fail if they
-- exceed 1% of in-scope records, so the exclusion stays a documented decision
-- rather than an unnoticed loss.
with scoped as (
    select
        count(*) as total,
        count_if(coalesce(codeshare_flight_iata, flight_iata, flight_icao) is null) as unidentified
    from {{ ref('stg_flights') }}
    where arrival_airport in (select iata_code from {{ ref('dim_airports') }})
)
select total, unidentified, round(100.0 * unidentified / nullif(total, 0), 2) as pct
from scoped
where unidentified > 0.01 * total

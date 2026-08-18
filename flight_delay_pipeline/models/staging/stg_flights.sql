select
    f.value:flight_date::date as flight_date,
    f.value:flight_status::string as flight_status,
    f.value:airline.name::string as airline_name,
    f.value:flight.iata::string as flight_number,
    f.value:departure.iata::string as departure_airport,
    f.value:departure.scheduled::timestamp_tz as departure_scheduled,
    f.value:departure.actual::timestamp_tz as departure_actual,
    f.value:arrival.iata::string as arrival_airport,
    f.value:arrival.scheduled::timestamp_tz as arrival_scheduled,
    f.value:arrival.actual::timestamp_tz as arrival_actual,
    datediff('minute', f.value:departure.scheduled::timestamp_tz, f.value:departure.actual::timestamp_tz) as departure_delay_minutes,
    datediff('minute', f.value:arrival.scheduled::timestamp_tz, f.value:arrival.actual::timestamp_tz) as arrival_delay_minutes
from {{ source('raw', 'stg_flights_raw') }} r,
lateral flatten(input => r.raw_data:data) f
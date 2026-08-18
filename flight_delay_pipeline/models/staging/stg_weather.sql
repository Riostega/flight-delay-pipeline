select
    r.raw_data:name::string as station_name,
    r.raw_data:coord.lat::float as lat,
    r.raw_data:coord.lon::float as lon,
    r.raw_data:main.temp::float as temp_f,
    r.raw_data:main.feels_like::float as feels_like_f,
    r.raw_data:main.humidity::int as humidity,
    r.raw_data:wind.speed::float as wind_speed,
    r.raw_data:weather[0].main::string as weather_main,
    r.raw_data:weather[0].description::string as weather_description,
    to_timestamp_ntz(r.raw_data:dt::int) as observed_at
from {{ source('raw', 'stg_weather_raw') }} r
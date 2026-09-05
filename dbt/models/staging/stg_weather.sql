-- One row per weather observation per airport per file.
--
-- iata_code comes from the S3 filename, not from the payload. OpenWeatherMap
-- returns the nearest station's coordinates and name rather than the ones
-- requested, and which station it resolves to drifts between calls: the same
-- point came back as both "College Park" and "Woodland Hills Mobile Home Park",
-- with three different coordinate pairs. The filename records what was actually
-- requested, so it is the only stable identifier.

select
    regexp_substr(r.source_file, '/([A-Z]{3})_', 1, 1, 'e', 1)  as iata_code,

    r.raw_data:name::string                          as station_name,
    r.raw_data:coord.lat::float                      as station_lat,
    r.raw_data:coord.lon::float                      as station_lon,

    r.raw_data:main.temp::float                      as temp_f,
    r.raw_data:main.feels_like::float                as feels_like_f,
    r.raw_data:main.humidity::int                    as humidity,
    r.raw_data:main.pressure::int                    as pressure,
    r.raw_data:wind.speed::float                     as wind_speed,
    r.raw_data:wind.gust::float                      as wind_gust,
    r.raw_data:visibility::int                       as visibility_m,
    r.raw_data:clouds.all::int                       as cloud_cover_pct,
    r.raw_data:weather[0].main::string               as weather_main,
    r.raw_data:weather[0].description::string        as weather_description,

    -- dt is Unix epoch seconds, so this is UTC wall time carried as NTZ.
    -- Downstream joins convert the flight timestamps to UTC to match.
    to_timestamp_ntz(r.raw_data:dt::int)             as observed_at,

    r.source_file

from {{ source('raw', 'stg_weather_raw') }} r

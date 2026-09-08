-- Every weather measure is unit-dependent and none of them carried a range test.
-- The unit is set in one place: params={"units": "imperial"} in fetch_weather.
-- Drop or typo that key and OpenWeatherMap returns Kelvin, so temp_f becomes
-- ~295 for a mild day. not_null passes, the load passes, the join passes, and
-- every temperature-based conclusion is silently wrong by a factor.
--
-- Bounds are deliberately loose: the point is to catch a unit change, not to
-- adjudicate weather.

select flight_event_key, weather_temp_f, weather_humidity, weather_cloud_cover_pct
from {{ ref('fct_flight_events') }}
where has_weather_match
  and (   weather_temp_f          not between -60 and 140
       or weather_humidity        not between 0 and 100
       or weather_cloud_cover_pct not between 0 and 100 )

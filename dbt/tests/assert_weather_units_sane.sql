-- Temperature and wind are unit-dependent; humidity and cloud cover are
-- percentages, checked here only as range sanity.
-- The unit is set in one place: params={"units": "imperial"} in fetch_weather.
-- Drop or typo that key and OpenWeatherMap returns Kelvin, so temp_f becomes
-- ~295 for a mild day. not_null passes, the load passes, the join passes, and
-- every temperature-based conclusion is silently wrong by a factor.
--
-- Bounds are deliberately loose: the point is to catch a unit change, not to
-- adjudicate weather. They catch Kelvin but NOT a switch to metric, because
-- Celsius readings (about 10-35) sit inside them;
-- assert_weather_temp_is_fahrenheit covers that case.

select flight_event_key, weather_temp_f, weather_humidity, weather_cloud_cover_pct
from {{ ref('fct_flight_events') }}
where has_weather_match
  and (   weather_temp_f          not between -60 and 140
       or weather_humidity        not between 0 and 100
       or weather_cloud_cover_pct not between 0 and 100 )

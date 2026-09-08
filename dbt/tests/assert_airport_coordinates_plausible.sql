-- dim_airports is the ONLY source of the coordinates sent to OpenWeatherMap
-- (extract_pipeline.fetch_weather), and the observation that comes back is
-- attributed to that airport with no check that it describes the right place.
-- A flipped sign or a transposed pair therefore poisons every weather-derived
-- conclusion silently: the join still matches, the lag still looks healthy, the
-- weather is simply from somewhere else. not_null cannot see that.
--
-- Bounds are the continental US plus margin. All five scoped airports are
-- domestic; widen this deliberately if that ever stops being true.

select iata_code, latitude, longitude
from {{ ref('dim_airports') }}
where latitude  not between 24 and 50
   or longitude not between -125 and -66

-- Catches OpenWeatherMap switching from imperial to metric units.
--
-- assert_weather_units_sane catches Kelvin (~295), but Celsius readings (about
-- 10-35) fall inside any sensible Fahrenheit range, so a metric switch would
-- pass it while every temperature dropped by ~40 "degrees F" and every wind
-- speed shrank by 2.2x (the same parameter controls both).
--
-- So check a warm place instead of a range: a DAILY AVERAGE below 40 F at Miami
-- or Los Angeles essentially never happens, while their Celsius daily averages
-- are always below 40. Per day rather than overall, so it fails on the first
-- affected day instead of after weeks of diluted averages.
select
    iata_code,
    observed_at::date as observed_date,
    avg(temp_f)       as avg_temp
from {{ ref('stg_weather') }}
where iata_code in ('MIA', 'LAX')
group by 1, 2
having avg(temp_f) < 40

-- A row with no timezone has NULL *_utc times, so assert_arrival_after_departure
-- cannot check it and it gets no weather. Today that is a handful of origins
-- that never send a zone (NLU, CYA), under 1% of rows. If the source started
-- dropping timezones more widely, the timezone tripwire would go blind without
-- anything failing. This makes that loss loud.
--
-- 3% rather than the 1% used elsewhere: the rate is already around 0.7%, and the
-- worst single week has been about 1%, so a 1% line would fire on normal noise.
with counted as (
    select
        count(*)                                                              as total,
        count_if(departure_timezone is null or arrival_timezone is null)      as missing_zone
    from {{ ref('fct_flight_events') }}
)
select total, missing_zone, round(100.0 * missing_zone / nullif(total, 0), 2) as pct
from counted
where missing_zone > 0.03 * total

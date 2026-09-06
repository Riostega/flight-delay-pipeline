"""Freshness thresholds, in one place because they are coupled to the schedules.

These numbers are derived from the DAG schedules, and every consumer that warns
about stale data must use the same ones. They previously lived as literals in
both the watchdog and the dashboard, and when the flights DAG moved from daily
to every other day, neither was updated — so both reported a false outage for
the back half of every collection cycle.

A monitor that cries wolf on a schedule is worse than no monitor: it teaches you
to ignore the channel that is supposed to carry real failures.

If you change a schedule in dags/, change the matching value here in the same
commit. That is the whole reason this module exists.
"""

# weather_hourly — "0 * * * *". One missed run plus slack.
WEATHER_STALE_HOURS = 3
WEATHER_STALE_MINUTES = WEATHER_STALE_HOURS * 60

# flight_pipeline_daily — "0 9 */2 * *". Every other day, so a 48-hour gap is
# normal; 6 hours of slack surfaces a genuinely missed run without firing during
# healthy operation. (The DAG keeps its historical name; the schedule changed to
# fit AviationStack's 100-request monthly quota.)
FLIGHTS_STALE_HOURS = 54

# How the flights schedule reads in prose, for user-facing messages. Keeping the
# wording here stops the UI from claiming "daily" after the schedule changed.
FLIGHTS_CADENCE = "every other day"

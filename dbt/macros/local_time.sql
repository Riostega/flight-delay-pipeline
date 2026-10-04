{#
  Converting an airport's local wall time to UTC, without guessing.

  AviationStack reports local wall time with no UTC offset. For almost every
  minute of the year a wall time plus a zone names exactly one instant. Twice a
  year it does not:

    fall back  (first Sunday of November in the US): 01:00-01:59 happens twice,
               once in daylight time and once in standard time. "01:30" could
               be either, an hour apart, and the feed does not say which.
               Snowflake always picks the first, so an arrival in the second
               01:xx comes out an hour too early in UTC.
    spring forward (second Sunday of March in the US): 02:00-02:59 never
               happens, so a time in it is not a real instant at all.

  The check: convert the time and the time one hour later. On a normal hour the
  two instants are 60 minutes apart. In the repeated hour they are 120 apart,
  and in the skipped hour 0 apart. Anything other than 60 means "this wall time
  does not name one instant". It works for any zone, not just US ones.
#}

{# True when the wall time falls in a repeated or skipped hour. False when the
   zone or the time is missing (there is nothing to be ambiguous about). #}
{% macro is_dst_ambiguous(zone, wall_time) %}
    coalesce(
        datediff('minute',
            convert_timezone({{ zone }}, 'UTC', {{ wall_time }}),
            convert_timezone({{ zone }}, 'UTC', dateadd('hour', 1, {{ wall_time }}))
        ) <> 60,
        false
    )
{% endmacro %}

{# The UTC instant, or NULL when it cannot be known: the zone is missing, or the
   wall time is in a repeated or skipped hour. NULL rather than Snowflake's
   guess, so a wrong instant can never reach the weather join or a test. #}
{% macro local_to_utc(zone, wall_time) %}
    case
        when not {{ is_dst_ambiguous(zone, wall_time) }}
        then convert_timezone({{ zone }}, 'UTC', {{ wall_time }})
    end
{% endmacro %}

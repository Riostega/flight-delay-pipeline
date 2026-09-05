{#
  Drops the throwaway schema a CI run builds into, so test runs do not
  accumulate objects in the warehouse.

  Guarded: it refuses unless the active target schema is literally named CI.
  A macro that drops schemas should be incapable of dropping the wrong one,
  regardless of which profile happens to be active when it runs.
#}
{% macro drop_ci_schema() %}
    {% if target.schema | lower == 'ci' %}
        {% set sql %}
            drop schema if exists {{ target.database }}.{{ target.schema }} cascade
        {% endset %}
        {% do run_query(sql) %}
        {{ log("Dropped " ~ target.database ~ "." ~ target.schema, info=True) }}
    {% else %}
        {{ log("Refusing to drop schema '" ~ target.schema ~ "' - only CI is droppable", info=True) }}
    {% endif %}
{% endmacro %}

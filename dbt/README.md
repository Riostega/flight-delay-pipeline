# dbt project

Transforms the raw JSON landed in Snowflake into a tested fact table. See the
[repository README](../README.md) for the pipeline as a whole.

## Layout

```
models/staging/    sources.yml, stg_flights, stg_weather   (views)
models/marts/      fct_flight_events                       (table)
seeds/             dim_airports.csv — airport scope and coordinates
tests/             singular tests
macros/            local_time — local wall time to UTC, refusing to guess on DST nights
                   drop_ci_schema — teardown for CI runs
```

`dim_airports.csv` is read by dbt **and** by `pipeline/extract_pipeline.py`, so the pipeline
and the warehouse cannot disagree about which airports are in scope.

## Grain

`fct_flight_events` holds **one row per physical flight per scheduled departure**.

AviationStack returns one record per *marketing* flight number, so a single aircraft
appears once for every airline selling seats on it — up to ten (AA2690 into LAX,
4 Sep 2026). Attributing one aircraft's delay to ten carriers would corrupt the carrier-reliability analysis
this project exists to produce, so models collapse onto the operating flight, keyed on
`coalesce(codeshare_flight_iata, flight_iata, flight_icao) + departure_scheduled`.

Duplicates arrive from two directions, so the mart removes them in two steps. The extract
is a live snapshot, so consecutive runs return overlapping flights: first it removes re-pulled
copies of the same marketing label (marketing designator + scheduled departure). Then it
collapses the remaining labels onto the operating key, counting them first. The order matters:
collapsing on the operating key alone would still give one row per flight, but
`marketing_label_count` would count re-pulls as extra carriers.

The carrier identity is `operating_carrier_code` (IATA, or ICAO for operators without one).
`operating_carrier_name` is the brand the source reports, which for regional flights is the
mainline partner: a SkyWest (OO) flight is named "American Airlines".

## Time semantics

- `*_local` is the wall time the source reports; `*_utc` is converted with the timezone it
  sends separately. Delays are computed in UTC where the zone is known, otherwise locally.
- The source's "actual" times are **runway** times (wheels-off / wheels-on); "scheduled" is the
  published **gate** time. Departure delay includes taxi-out; arrival delay excludes taxi-in, so
  `is_delayed_arrival` uses the BTS 15-minute threshold but is not directly comparable to BTS
  gate-based on-time rates. `minutes_recovered` includes taxi time, not just schedule padding.
- No arrival is ever more than about 56 minutes late; the limit is in the raw payload, so late
  rates are lower bounds.
- Daylight-saving nights: a local time in the repeated hour (US: 01:00–01:59 on the first Sunday
  of November) or the skipped one (02:00–02:59 on the second Sunday of March) names no single
  instant. Its `*_utc` value is left NULL, the row gets `has_dst_ambiguous_time`, and it counts as
  `has_suspect_times`.

Staging deliberately does none of this. It flattens and casts; deduplication, carrier
attribution and the weather join are modelling decisions and live in the mart.

## Running

```bash
dbt seed      # load dim_airports
dbt build     # run models and tests in dependency order
dbt docs generate && dbt docs serve --port 8081
```

Requires a `flight_delay_pipeline` profile in `~/.dbt/profiles.yml`. `dbt build` is
preferred over `run` then `test`, since it tests each model as it is built and stops
before downstream models are constructed on bad data.

## Tests

Generic tests in the schema YAML files plus singular tests in `tests/`, across staging
and the mart. `dbt ls --resource-type test` lists them all.

The load-bearing one is `unique` on `flight_event_key` — the executable proof
that the grain argument holds. If codeshares or re-pulls stopped collapsing
correctly it fails immediately, rather than quietly producing wrong averages.

Staging carries tests too, and they are what makes `dbt build` protective: a
staging model that fails its tests never becomes the input to the fact table.
Testing only the mart would let a bad source rebuild it before anything noticed.

Singular tests guard invariants a column test cannot express. Each file's header
explains its reason. By purpose:

- **Timestamps and delays:** no flight arrives before it departs (the tripwire for
  timezone handling), missing timezones stay rare, delay minutes stay in a plausible
  band, landed flights have a delay, suspect-time rows stay rare.
- **Grain:** no key splits by letter case, no key merges two routes, records dropped
  for carrying no flight identifier stay rare, the fact table is not empty.
- **Weather:** coordinates are plausible, units are Fahrenheit (not Kelvin or
  Celsius), every airport's weather is current, each airport's recent flights match
  weather, the freshness flag agrees with the columns it governs.
- **Source behaviour (warn only):** "actual" is still the runway time, and no arrival
  exceeds the ~56-minute ceiling. Warnings do not fail the DAG or alert Slack, so check
  the build output for them.

# dbt project

Transforms the raw JSON landed in Snowflake into a tested fact table. See the
[repository README](../README.md) for the pipeline as a whole.

## Layout

```
models/staging/    sources.yml, stg_flights, stg_weather   (views)
models/marts/      fct_flight_events                       (table)
seeds/             dim_airports.csv — airport scope and coordinates
tests/             singular tests
macros/            drop_ci_schema — teardown for CI runs
```

`dim_airports.csv` is read by dbt **and** by `pipeline/extract_pipeline.py`, so the pipeline
and the warehouse cannot disagree about which airports are in scope.

## Grain

`fct_flight_events` holds **one row per physical flight per scheduled departure**.

AviationStack returns one record per *marketing* flight number, so a single aircraft
appears once for every airline selling seats on it — in one sample, nine. Attributing
one aircraft's delay to nine carriers would corrupt the carrier-reliability analysis
this project exists to produce, so models collapse onto the operating flight, keyed on
`coalesce(codeshare_flight_iata, flight_iata, flight_icao) + departure_scheduled`.

That key does double duty: because the extract is a live snapshot, consecutive runs
return overlapping flights, and the same aircraft pulled twice resolves to the same key.

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

34 in total, across staging and the mart.

The load-bearing one is `unique` on `flight_event_key` — the executable proof
that the grain argument holds. If codeshares or re-pulls stopped collapsing
correctly it fails immediately, rather than quietly producing wrong averages.

Staging carries tests too, and they are what makes `dbt build` protective: a
staging model that fails its tests never becomes the input to the fact table.
Testing only the mart would let a bad source rebuild it before anything noticed.

Three singular tests guard invariants a column test cannot express: that no
flight arrives before it departs (the tripwire for timezone handling), that
delay minutes stay in a plausible band, and that the weather freshness flag
always agrees with the weather columns it governs.

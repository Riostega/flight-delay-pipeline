"""Flight reliability dashboard.

Reads the modelled layer in Snowflake — fct_flight_events and the staging views —
and presents airport and carrier reliability alongside the weather conditions
recorded at arrival.

Run with:  streamlit run dashboard/app.py

Launch it from the repository root, as above. Streamlit reads
.streamlit/config.toml from the working directory, and that file pins the
surface the chart palette was validated against.
"""

import os
import re
import sys
from decimal import Decimal
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

# Freshness thresholds are shared with the watchdog rather than duplicated here.
# They are derived from the DAG schedules, and a copy in this file drifted out of
# date the last time a schedule changed.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from pipeline.thresholds import (  # noqa: E402
    WEATHER_STALE_MINUTES,
    FLIGHTS_STALE_HOURS,
    FLIGHTS_CADENCE,
)

def theme_mode() -> str:
    """Which validated palette to draw with.

    Read from Streamlit's configured theme rather than sniffed from the browser:
    st.context.theme.type returns None until the frontend reports back, which
    silently produced light-mode charts on a dark background. .streamlit/config.toml
    pins the surface, so this is deterministic — but only when Streamlit finds
    that file, which it looks for in the working directory. Launched from
    anywhere but the repo root, theme.base is unset and the guess below can be
    wrong, so say so on the page rather than drawing the wrong palette quietly.
    """
    base = st.get_option("theme.base")
    if base in ("dark", "light"):
        return base
    st.warning(
        "Theme config not loaded, so chart colours may not match the page. "
        "Launch from the repository root: streamlit run dashboard/app.py"
    )
    return "dark" if getattr(st.context.theme, "type", None) == "dark" else "light"


# Before theme_mode(), which may put a warning on the page.
st.set_page_config(page_title="Flight Reliability", page_icon="✈", layout="wide")

MODE = theme_mode()

# Both modes are selected, not derived: the dark steps are chosen for the dark
# surface and validated against it, rather than being a flip of the light ones.
# Each trio was checked with the palette validator (all-pairs CVD separation 9.2
# light / 9.4 dark, normal-vision 24.0 / 20.9 — both clear of the floors).
if MODE == "dark":
    SURFACE = "#1a1a19"
    INK, INK_MUTED, GRID = "#ffffff", "#c3c2b7", "#383835"
    BLUE, ORANGE, AQUA = "#3987e5", "#d95926", "#199e70"
    # On a dark surface the darkest step recedes toward the background, so the
    # ramp runs the other way: brighter means more. The darkest step used still
    # clears 2:1 against the surface.
    SEQ = ["#184f95", "#256abf", "#2a78d6", "#3987e5", "#5598e7", "#86b6ef", "#cde2fb"]
    PAIR_STRONG, PAIR_SOFT = "#3987e5", "#cde2fb"
else:
    SURFACE = "#fcfcfb"
    INK, INK_MUTED, GRID = "#0b0b0b", "#52514e", "#e8e7e3"
    BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
    SEQ = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
    PAIR_STRONG, PAIR_SOFT = "#184f95", "#6da7ec"

# Three validated categorical slots, assigned in fixed order to the three most
# common weather conditions; everything rarer folds into "Other" in the
# de-emphasis ink. A fourth generated hue would be indistinguishable under CVD,
# and the vocabulary is open-ended — fog, snow and thunderstorms all appear.
CONDITION_SLOTS = [BLUE, ORANGE, AQUA]
OTHER = INK_MUTED

# In light mode aqua sits below 3:1 on the surface, so the relief rule applies:
# every chart carries visible value labels and a table view. Kept in both modes
# for consistency.

# Snowflake raises these when the session is no longer usable, as opposed to
# when the SQL itself is wrong. Only the former is worth reconnecting for.
_CONNECTION_ERRORS = ("390114", "390111", "390104", "08001", "250002")


def _is_connection_error(e: Exception) -> bool:
    text = f"{getattr(e, 'errno', '')} {getattr(e, 'sqlstate', '')} {e}"
    return any(code in text for code in _CONNECTION_ERRORS)


# The raw VARIANT tables (stg_flights_raw, stg_weather_raw) are loaded by COPY
# INTO, never built by dbt, so they exist only in the production schema. Every
# other table is read from SNOWFLAKE_SCHEMA. CI points SNOWFLAKE_SCHEMA at the
# models it just built and sets this to PUBLIC. Same default as raw_schema in
# dbt/dbt_project.yml. Checked because it is pasted into SQL.
RAW_SCHEMA = (os.getenv("SNOWFLAKE_RAW_SCHEMA") or "PUBLIC").strip()
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", RAW_SCHEMA):
    raise SystemExit(f"SNOWFLAKE_RAW_SCHEMA is not a plain schema name: {RAW_SCHEMA!r}")


@st.cache_resource
def connect():
    g = lambda k: (os.getenv(k) or "").strip()
    conn = snowflake.connector.connect(
        account=g("SNOWFLAKE_ACCOUNT"), user=g("SNOWFLAKE_USER"),
        password=g("SNOWFLAKE_PASSWORD"), warehouse=g("SNOWFLAKE_WAREHOUSE"),
        database=g("SNOWFLAKE_DATABASE"), schema=g("SNOWFLAKE_SCHEMA"),
        # The connection is cached for the life of the session. Without
        # heartbeats Snowflake expires it while the dashboard sits idle, and
        # every query afterwards fails until the app is restarted.
        client_session_keep_alive=True,
    )
    # Snowflake's default search path is "$current, $public": a table missing
    # from the configured schema is silently read from PUBLIC instead. In CI
    # that would let a PR that drops or renames a model pass by rendering
    # production's copy. Unqualified names now resolve only in SNOWFLAKE_SCHEMA;
    # in production that is PUBLIC, so nothing changes there. (Set after login:
    # passing it as a login session parameter is rejected by Snowflake.)
    cur = conn.cursor()
    try:
        cur.execute("ALTER SESSION SET SEARCH_PATH = '$current'")
    finally:
        cur.close()
    return conn


def _run(sql: str) -> pd.DataFrame:
    cur = connect().cursor()
    try:
        cur.execute(sql)
        return pd.DataFrame(cur.fetchall(), columns=[c[0] for c in cur.description])
    finally:
        cur.close()


@st.cache_data(ttl=300)
def q(sql: str) -> pd.DataFrame:
    try:
        df = _run(sql)
    except snowflake.connector.errors.Error as e:
        # The connection is cached for the life of the session, so once it dies
        # it stays dead: every later query fails and refreshing the page changes
        # nothing, because the broken object is what gets handed back. Tokens do
        # expire, networks do drop. Clear the cached connection and try once more
        # so the dashboard heals itself instead of needing a restart.
        if not _is_connection_error(e):
            raise
        connect.clear()
        df = _run(sql)
    # Snowflake returns NUMBER as Decimal, which will not multiply with a float
    # and silently breaks axis padding and formatting. Coerce once here rather
    # than defending against it at every call site.
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, Decimal)).any():
            df[col] = df[col].astype(float)
    return df


def base_layout(fig, height=380, xtitle="", ytitle=""):
    """Recessive axes and grid; text in ink tokens, never a series colour."""
    fig.update_layout(
        height=height, margin=dict(l=8, r=8, t=8, b=8),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color=INK_MUTED, size=13),
        xaxis=dict(title=xtitle, gridcolor=GRID, zerolinecolor=GRID, linecolor=GRID),
        yaxis=dict(title=ytitle, gridcolor=GRID, zerolinecolor=GRID, linecolor=GRID),
        legend=dict(orientation="h", yanchor="bottom", y=1.0, x=0, title=""),
        hoverlabel=dict(bgcolor=SURFACE, bordercolor=GRID,
                        font=dict(color=INK, size=13)),
    )
    return fig


def seq_scale(values):
    """Map magnitudes onto the single-hue ramp.

    SEQ is ordered so that the last step is the most prominent against whichever
    surface is active — darker on light, brighter on dark — so more always reads
    as more.
    """
    if len(values) == 0:
        return []
    lo, hi = min(values), max(values)
    span = (hi - lo) or 1
    return [SEQ[int(round((v - lo) / span * (len(SEQ) - 1)))] for v in values]


st.title("Flight Delay Reliability")
st.caption(
    "Which airports and carriers are systematically less reliable — and how much "
    "of that is explained by weather rather than operations."
)

tab_overview, tab_airports, tab_weather, tab_pipeline = st.tabs(
    ["Overview", "Airports", "Weather vs operations", "Pipeline health"]
)

# ---------------------------------------------------------------- Overview
# The source's "actual" times are RUNWAY times (wheels-off, wheels-on), while
# the schedule is gate times. So a late arrival here is wheels-on 15+ minutes
# after the scheduled gate arrival: the same 15-minute line BTS uses, but not
# the same measure, so these rates are not comparable to published on-time
# statistics. And "recovered" time is partly taxiing, not just padding.
#
# Delay and recovery figures leave out has_suspect_times rows: records whose
# schedule belongs to a different flight, so their delays are nonsense (one
# "arrives" 604 minutes early). The analysis notebooks drop the same rows, so
# both report the same numbers. Counts of flights still include them.
with tab_overview:
    k = q("""
        SELECT COUNT(*) AS flights,
               SUM(CASE WHEN has_suspect_times THEN 1 ELSE 0 END) AS suspect,
               ROUND(100.0 * SUM(CASE WHEN is_delayed_arrival AND NOT has_suspect_times THEN 1 ELSE 0 END)
                     / NULLIF(SUM(CASE WHEN NOT has_suspect_times THEN 1 ELSE 0 END), 0), 1) AS pct_late,
               ROUND(100.0 * SUM(CASE WHEN has_weather_match THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS pct_weather,
               ROUND(AVG(CASE WHEN NOT has_suspect_times THEN minutes_recovered END), 1) AS avg_recovered
        FROM fct_flight_events
    """).iloc[0]

    # An empty fact table is a real state (a rebuilt warehouse before its first
    # load), and formatting its NULL averages would crash the page. if/else, not
    # st.stop(), so the Pipeline health tab, the useful view then, still renders.
    if int(k.FLIGHTS) == 0:
        st.info("fct_flight_events is empty. Load the raw files (snowflake_load.sql), run dbt build, "
                "then check the Pipeline health tab.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Flights tracked", f"{int(k.FLIGHTS):,}", help="One row per physical flight, codeshares collapsed")
        c2.metric("Late arrivals", f"{k.PCT_LATE}%", help="Wheels-on 15+ minutes after the scheduled gate arrival. Same 15-minute line as BTS, "
                       "but measured at the runway rather than the gate, so not comparable to published rates")
        c3.metric("Recovered en route",
                  f"{k.AVG_RECOVERED:.0f} min" if pd.notna(k.AVG_RECOVERED) else "n/a",
                  help="Departure delay minus arrival delay. Both are runway times against a gate schedule, "
                       "so this mixes schedule padding with taxi time: taxi-out counts as departure delay, "
                       "and taxi-in is never counted")
        c4.metric("Weather coverage", f"{k.PCT_WEATHER}%", help="Flights matched to an observation within 120 minutes of arrival")
        st.caption(
            f"{int(k.SUSPECT or 0):,} records whose schedule belongs to a different flight "
            "are left out of the delay figures on every tab, as in the analysis notebooks."
        )

        st.divider()
        st.subheader("Late arrivals by airport")

        d = q("""
            SELECT arrival_airport AS airport, COUNT(*) AS flights,
                   ROUND(100.0 * SUM(CASE WHEN is_delayed_arrival THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_late
            FROM fct_flight_events
            WHERE NOT has_suspect_times
            GROUP BY 1 ORDER BY pct_late DESC
        """)
        fig = go.Figure(go.Bar(
            x=d.PCT_LATE, y=d.AIRPORT, orientation="h",
            marker=dict(color=seq_scale(d.PCT_LATE.tolist()), cornerradius=4),
            text=[f"{v}%" for v in d.PCT_LATE], textposition="outside",
            textfont=dict(color=INK_MUTED),
            customdata=d.FLIGHTS,
            hovertemplate="<b>%{y}</b><br>%{x}% late<br>%{customdata} flights<extra></extra>",
        ))
        fig.update_yaxes(autorange="reversed")
        fig.update_xaxes(range=[0, max(d.PCT_LATE.max() * 1.25, 1)], ticksuffix="%")
        st.plotly_chart(base_layout(fig, 300, "Share of arrivals 15+ min late"), width='stretch')
        with st.expander("Table view"):
            st.dataframe(d, hide_index=True, width='stretch')

# ---------------------------------------------------------------- Airports
with tab_airports:
    st.subheader("Time recovered between departure and arrival, by arrival airport")
    # Computed rather than typed into the caption: a hard-coded figure here went
    # stale while the page around it kept reading live data.
    scoped = q("""
        SELECT ROUND(100.0 * SUM(CASE WHEN departure_airport IN (SELECT iata_code FROM dim_airports)
                                      THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 0) AS pct
        FROM fct_flight_events
    """).iloc[0]
    scoped_text = f"only {scoped.PCT:.0f}% of" if pd.notna(scoped.PCT) else "few"
    st.caption(
        "Both points are the same inbound flights, grouped by where they LANDED. "
        "The departure figure is how late those flights left their own origins — it is "
        f"not a measure of this airport's own departure performance, because {scoped_text} "
        "collected flights depart from one of the five scoped airports. The gap between "
        "the points is time made up en route. It is partly schedule padding and partly "
        "taxiing: the source's times are wheels-off and wheels-on, so taxi-out counts as "
        "departure delay and taxi-in is never counted, which makes the gap look bigger."
    )

    # Grouped by arrival_airport on purpose: the two points must describe the SAME
    # flights or the gap between them is not recovery. Grouping the departure point
    # by departure_airport instead would compare two different populations, and the
    # per-origin samples are much smaller than the per-arrival ones.
    d = q("""
        SELECT arrival_airport AS airport,
               ROUND(AVG(departure_delay_minutes), 1) AS avg_dep,
               ROUND(AVG(arrival_delay_minutes), 1) AS avg_arr,
               COUNT(*) AS flights
        FROM fct_flight_events
        WHERE NOT has_suspect_times
        GROUP BY 1 ORDER BY avg_dep DESC
    """)

    # Dumbbell: before -> after per item, one hue in two shades.
    fig = go.Figure()
    for _, r in d.iterrows():
        fig.add_trace(go.Scatter(
            x=[r.AVG_DEP, r.AVG_ARR], y=[r.AIRPORT, r.AIRPORT], mode="lines",
            line=dict(color=GRID, width=2), showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(
        x=d.AVG_DEP, y=d.AIRPORT, mode="markers", name="Departed late by",
        marker=dict(color=PAIR_STRONG, size=13, line=dict(color=SURFACE, width=2)),
        hovertemplate="<b>%{y}</b><br>left origin %{x} min late<extra></extra>"))
    fig.add_trace(go.Scatter(
        x=d.AVG_ARR, y=d.AIRPORT, mode="markers", name="Arrived late by",
        marker=dict(color=PAIR_SOFT, size=13, line=dict(color=SURFACE, width=2)),
        hovertemplate="<b>%{y}</b><br>arrived %{x} min late<extra></extra>"))
    fig.add_vline(x=0, line_width=2, line_color=INK_MUTED, opacity=0.35)
    fig.update_yaxes(autorange="reversed")
    st.plotly_chart(base_layout(fig, 340, "Average delay, minutes (0 = on time)"), width='stretch')
    with st.expander("Table view"):
        st.dataframe(d, hide_index=True, width='stretch')

    st.divider()
    st.subheader("Operating carriers")
    st.caption("Attributed to the carrier that actually flew the aircraft, not the one that sold the seat.")

    min_flights = st.slider("Minimum flights to include", 1, 25, 5)
    # Special liveries arrive as separate names ("Alaska Airlines (Oneworld
    # Livery)"), which split one carrier into several small groups that the
    # minimum-flights filter then drops. Merged here the same way
    # analysis/flights_data.py merges them, anchored on "Livery" so any other
    # bracketed name is left alone. Belongs in fct_flight_events eventually, so
    # both read one definition; once it is there this is a no-op.
    c = q(f"""
        SELECT REGEXP_REPLACE(operating_carrier_name, ' *[(][^)]*Livery[)]$', '') AS carrier,
               COUNT(*) AS flights,
               ROUND(AVG(arrival_delay_minutes), 1) AS avg_arr_delay,
               ROUND(100.0 * SUM(CASE WHEN is_delayed_arrival THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_late
        FROM fct_flight_events
        WHERE NOT has_suspect_times
        GROUP BY 1
        HAVING COUNT(*) >= {min_flights} ORDER BY pct_late DESC LIMIT 15
    """)
    if c.empty:
        st.info("No carriers meet that threshold yet — the sample is still small.")
    else:
        fig = go.Figure(go.Bar(
            x=c.PCT_LATE, y=c.CARRIER, orientation="h",
            marker=dict(color=seq_scale(c.PCT_LATE.tolist()), cornerradius=4),
            text=[f"{v}%" for v in c.PCT_LATE], textposition="outside",
            textfont=dict(color=INK_MUTED), customdata=c.FLIGHTS,
            hovertemplate="<b>%{y}</b><br>%{x}% late<br>%{customdata} flights<extra></extra>"))
        fig.update_yaxes(autorange="reversed")
        fig.update_xaxes(range=[0, max(c.PCT_LATE.max() * 1.3, 1)], ticksuffix="%")
        st.plotly_chart(base_layout(fig, max(280, 34 * len(c)), "Share of arrivals 15+ min late"),
                        width='stretch')
        with st.expander("Table view"):
            st.dataframe(c, hide_index=True, width='stretch')

# ------------------------------------------------- Weather vs operations
with tab_weather:
    st.subheader("Delay by weather condition at arrival")

    cov = q("SELECT COUNT(*) AS n, SUM(CASE WHEN has_weather_match THEN 1 ELSE 0 END) AS matched FROM fct_flight_events").iloc[0]
    # SUM over an empty table is NULL, not 0.
    matched = int(cov.MATCHED or 0)
    if matched == 0:
        st.warning(
            "No flights are matched to weather yet. Weather history only extends back to "
            "when hourly collection began, and AviationStack reports arrivals with a lag, "
            "so the two windows take time to overlap."
        )
    else:
        st.caption(
            f"{matched:,} of {int(cov.N):,} flights matched to an observation within "
            "120 minutes of arrival. Readings staler than that are discarded rather than reported."
        )

    # Ranking and folding happen in SQL so the average stays a true average over
    # flights rather than an average of per-condition averages.
    w = q("""
        WITH ranked AS (
            SELECT weather_main,
                   ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
            FROM fct_flight_events
            WHERE has_weather_match AND NOT has_suspect_times
            GROUP BY 1
        )
        SELECT f.arrival_airport AS airport,
               CASE WHEN r.rn <= 3 THEN f.weather_main ELSE 'Other' END AS condition,
               -- Capped at 4 so "Other" has the same rank at every airport, even
               -- when each airport's rarest condition is a different one.
               LEAST(MIN(r.rn), 4) AS rank,
               COUNT(*) AS flights,
               ROUND(AVG(f.arrival_delay_minutes), 1) AS avg_arr_delay
        FROM fct_flight_events f
        JOIN ranked r ON r.weather_main = f.weather_main
        WHERE f.has_weather_match AND NOT f.has_suspect_times
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)

    if w.empty:
        st.info("Nothing to plot until the weather join populates.")
    else:
        # Colour follows the condition's overall rank, not its position in this
        # chart, so filtering never repaints the survivors.
        # One row per condition, so "Other" is drawn once with one legend entry.
        order = w[["CONDITION", "RANK"]].drop_duplicates("CONDITION").sort_values("RANK")
        colours = {
            row.CONDITION: (CONDITION_SLOTS[int(row.RANK) - 1] if row.CONDITION != "Other" else OTHER)
            for row in order.itertuples()
        }
        fig = go.Figure()
        for cond in order.CONDITION:
            sub = w[w.CONDITION == cond]
            fig.add_trace(go.Bar(
                name=cond, x=sub.AIRPORT, y=sub.AVG_ARR_DELAY,
                marker=dict(color=colours[cond], cornerradius=4,
                            line=dict(color=SURFACE, width=2)),
                text=[f"{v:.0f}" for v in sub.AVG_ARR_DELAY], textposition="outside",
                textfont=dict(color=INK_MUTED), customdata=sub.FLIGHTS,
                hovertemplate="<b>%{x} — " + cond + "</b><br>%{y} min average<br>%{customdata} flights<extra></extra>"))
        fig.update_layout(barmode="group", bargap=0.28, bargroupgap=0.06)
        fig.add_hline(y=0, line_width=2, line_color=INK_MUTED, opacity=0.35)
        st.plotly_chart(base_layout(fig, 400, "", "Average arrival delay, minutes"), width='stretch')

        st.caption(
            "Negative means arriving early. The finding worth looking for is an airport "
            "whose delays do **not** track its weather — that is an operational story, "
            "not a meteorological one."
        )
        with st.expander("Table view"):
            st.dataframe(w.drop(columns=["RANK"]), hide_index=True, width='stretch')

# ------------------------------------------------------- Pipeline health
with tab_pipeline:
    st.subheader("Pipeline health")

    f = q(f"""
        SELECT
          (SELECT COUNT(*) FROM {RAW_SCHEMA}.stg_flights_raw)  AS flight_files,
          (SELECT COUNT(*) FROM {RAW_SCHEMA}.stg_weather_raw)  AS weather_files,
          (SELECT COUNT(*) FROM stg_flights)      AS staged_rows,
          (SELECT COUNT(*) FROM fct_flight_events) AS physical_flights,
          (SELECT MAX(observed_at) FROM stg_weather) AS last_weather
    """).iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Raw flight files", f"{int(f.FLIGHT_FILES):,}")
    c2.metric("Raw weather files", f"{int(f.WEATHER_FILES):,}")
    c3.metric("Staged rows", f"{int(f.STAGED_ROWS):,}")
    c4.metric("Physical flights", f"{int(f.PHYSICAL_FLIGHTS):,}",
              delta=f"-{int(f.STAGED_ROWS) - int(f.PHYSICAL_FLIGHTS):,} collapsed",
              delta_color="off", help="Rows in staging that are not distinct physical flights in scope: codeshare labels, re-pull duplicates, arrivals at out-of-scope airports, and records with no flight identifier")
    # Slack (on_failure_callback) reports failed tasks, and pipeline/watchdog.py
    # reports stalled runs and stale data judged by when the newest raw FILE
    # landed. This panel measures something different: how recent the newest
    # in-scope ARRIVAL in fct_flight_events is. So it can go red while Slack is
    # quiet, when files keep landing but bring no new in-scope flights (only
    # re-pulled duplicates, older flights, out-of-scope airports) or the fact
    # table was not rebuilt. It is not an alert; it puts the symptom where the
    # data is actually looked at.
    fresh = q("""
        SELECT DATEDIFF('minute', (SELECT MAX(observed_at) FROM stg_weather), SYSDATE()) AS weather_age_min,
               DATEDIFF('hour',   (SELECT MAX(arrival_actual_utc) FROM fct_flight_events), SYSDATE()) AS flights_age_hr
    """).iloc[0]

    wx_age, fl_age = fresh.WEATHER_AGE_MIN, fresh.FLIGHTS_AGE_HR
    cols = st.columns(2)
    # Thresholds come from pipeline/thresholds.py so this panel and the watchdog
    # use the same limits, and neither can be left behind by a schedule change.
    # The flight ages they compare are measured differently (see above), so the
    # two can still disagree.
    if wx_age is not None and wx_age > WEATHER_STALE_MINUTES:
        cols[0].error(f"Weather is {wx_age/60:.1f}h stale — hourly collection may have stopped")
    else:
        cols[0].success(f"Weather current ({wx_age:.0f} min old)" if wx_age is not None else "No weather yet")
    if fl_age is not None and fl_age > FLIGHTS_STALE_HOURS:
        cols[1].error(f"Newest flight arrival is {fl_age:.0f}h old — collection may have stopped")
    else:
        cols[1].success(
            f"Flights current (newest arrival {fl_age:.0f}h ago, collected {FLIGHTS_CADENCE})"
            if fl_age is not None else "No flights yet"
        )

    st.caption(f"Most recent weather observation: {f.LAST_WEATHER} UTC")

    st.divider()
    st.subheader("Codeshare collapse")
    st.caption(
        "One aircraft can appear under many airline flight numbers. Without collapsing "
        "them, a single delayed flight would be counted once per carrier that sold seats on it."
    )

    cs = q("""
        SELECT marketing_label_count AS labels, COUNT(*) AS flights
        FROM fct_flight_events GROUP BY 1 ORDER BY 1
    """)
    fig = go.Figure(go.Bar(
        x=cs.LABELS, y=cs.FLIGHTS,
        marker=dict(color=seq_scale(cs.LABELS.tolist()), cornerradius=4),
        text=cs.FLIGHTS, textposition="outside", textfont=dict(color=INK_MUTED),
        hovertemplate="<b>%{x} marketing label(s)</b><br>%{y} physical flights<extra></extra>"))
    st.plotly_chart(base_layout(fig, 320, "Marketing flight numbers per physical flight", "Flights"),
                    width='stretch')

    st.divider()
    st.subheader("Weather match freshness")
    st.caption("How stale the matched observation was. Anything beyond the threshold is discarded.")

    # Buckets key off has_weather_match rather than repeating the threshold, so
    # this stays correct if the dbt variable changes.
    lag = q("""
        SELECT CASE
                 WHEN NOT has_weather_match     THEN 'd. too stale, discarded'
                 WHEN weather_lag_minutes <= 30 THEN 'a. 0-30 min'
                 WHEN weather_lag_minutes <= 60 THEN 'b. 31-60 min'
                 ELSE                                'c. 61 min to threshold'
               END AS bucket,
               COUNT(*) AS flights
        FROM fct_flight_events WHERE weather_lag_minutes IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """)
    if lag.empty:
        st.info("No weather matches yet.")
    else:
        st.dataframe(lag, hide_index=True, width='stretch')

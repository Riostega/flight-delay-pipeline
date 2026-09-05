"""Flight reliability dashboard.

Reads the modelled layer in Snowflake — fct_flight_events and the staging views —
and presents airport and carrier reliability alongside the weather conditions
recorded at arrival.

Run with:  streamlit run dashboard/app.py
"""

import os
from decimal import Decimal
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

def theme_mode() -> str:
    """Which validated palette to draw with.

    Read from Streamlit's configured theme rather than sniffed from the browser:
    st.context.theme.type returns None until the frontend reports back, which
    silently produced light-mode charts on a dark background. .streamlit/config.toml
    pins the surface, so this is deterministic.
    """
    base = st.get_option("theme.base")
    if base in ("dark", "light"):
        return base
    return "dark" if getattr(st.context.theme, "type", None) == "dark" else "light"


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

st.set_page_config(page_title="Flight Reliability", page_icon="✈", layout="wide")


@st.cache_resource
def connect():
    g = lambda k: (os.getenv(k) or "").strip()
    return snowflake.connector.connect(
        account=g("SNOWFLAKE_ACCOUNT"), user=g("SNOWFLAKE_USER"),
        password=g("SNOWFLAKE_PASSWORD"), warehouse=g("SNOWFLAKE_WAREHOUSE"),
        database=g("SNOWFLAKE_DATABASE"), schema=g("SNOWFLAKE_SCHEMA"),
        # The connection is cached for the life of the session. Without
        # heartbeats Snowflake expires it while the dashboard sits idle, and
        # every query afterwards fails until the app is restarted.
        client_session_keep_alive=True,
    )


@st.cache_data(ttl=300)
def q(sql: str) -> pd.DataFrame:
    cur = connect().cursor()
    cur.execute(sql)
    df = pd.DataFrame(cur.fetchall(), columns=[c[0] for c in cur.description])
    cur.close()
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
with tab_overview:
    k = q("""
        SELECT COUNT(*) AS flights,
               COUNT(DISTINCT arrival_airport) AS airports,
               COUNT(DISTINCT operating_carrier_name) AS carriers,
               ROUND(100.0 * SUM(CASE WHEN is_delayed_arrival THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_late,
               ROUND(100.0 * SUM(CASE WHEN has_weather_match THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_weather,
               ROUND(AVG(minutes_recovered), 1) AS avg_recovered
        FROM fct_flight_events
    """).iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Flights tracked", f"{int(k.FLIGHTS):,}", help="One row per physical flight, codeshares collapsed")
    c2.metric("Late arrivals", f"{k.PCT_LATE}%", help="More than 15 minutes late — the US DOT threshold")
    c3.metric("Recovered in air", f"{k.AVG_RECOVERED:.0f} min", help="Departure delay minus arrival delay: schedule padding")
    c4.metric("Weather coverage", f"{k.PCT_WEATHER}%", help="Flights matched to an observation within 120 minutes of arrival")

    st.divider()
    st.subheader("Late arrivals by airport")

    d = q("""
        SELECT arrival_airport AS airport, COUNT(*) AS flights,
               ROUND(100.0 * SUM(CASE WHEN is_delayed_arrival THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_late
        FROM fct_flight_events GROUP BY 1 ORDER BY pct_late DESC
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
    st.plotly_chart(base_layout(fig, 300, "Share of arrivals more than 15 min late"), width='stretch')
    with st.expander("Table view"):
        st.dataframe(d, hide_index=True, width='stretch')

# ---------------------------------------------------------------- Airports
with tab_airports:
    st.subheader("Departure delay against arrival delay")
    st.caption(
        "Flights routinely recover time in the air, because airlines pad published "
        "schedules. Measuring arrival alone hides problems at the origin."
    )

    d = q("""
        SELECT arrival_airport AS airport,
               ROUND(AVG(departure_delay_minutes), 1) AS avg_dep,
               ROUND(AVG(arrival_delay_minutes), 1) AS avg_arr,
               COUNT(*) AS flights
        FROM fct_flight_events GROUP BY 1 ORDER BY avg_dep DESC
    """)

    # Dumbbell: before -> after per item, one hue in two shades.
    fig = go.Figure()
    for _, r in d.iterrows():
        fig.add_trace(go.Scatter(
            x=[r.AVG_DEP, r.AVG_ARR], y=[r.AIRPORT, r.AIRPORT], mode="lines",
            line=dict(color=GRID, width=2), showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(
        x=d.AVG_DEP, y=d.AIRPORT, mode="markers", name="Departure",
        marker=dict(color=PAIR_STRONG, size=13, line=dict(color=SURFACE, width=2)),
        hovertemplate="<b>%{y}</b><br>departure %{x} min<extra></extra>"))
    fig.add_trace(go.Scatter(
        x=d.AVG_ARR, y=d.AIRPORT, mode="markers", name="Arrival",
        marker=dict(color=PAIR_SOFT, size=13, line=dict(color=SURFACE, width=2)),
        hovertemplate="<b>%{y}</b><br>arrival %{x} min<extra></extra>"))
    fig.add_vline(x=0, line_width=2, line_color=INK_MUTED, opacity=0.35)
    fig.update_yaxes(autorange="reversed")
    st.plotly_chart(base_layout(fig, 340, "Average delay, minutes (0 = on time)"), width='stretch')
    with st.expander("Table view"):
        st.dataframe(d, hide_index=True, width='stretch')

    st.divider()
    st.subheader("Operating carriers")
    st.caption("Attributed to the carrier that actually flew the aircraft, not the one that sold the seat.")

    min_flights = st.slider("Minimum flights to include", 1, 25, 5)
    c = q(f"""
        SELECT operating_carrier_name AS carrier, COUNT(*) AS flights,
               ROUND(AVG(arrival_delay_minutes), 1) AS avg_arr_delay,
               ROUND(100.0 * SUM(CASE WHEN is_delayed_arrival THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_late
        FROM fct_flight_events GROUP BY 1
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
        st.plotly_chart(base_layout(fig, max(280, 34 * len(c)), "Share of arrivals more than 15 min late"),
                        width='stretch')
        with st.expander("Table view"):
            st.dataframe(c, hide_index=True, width='stretch')

# ------------------------------------------------- Weather vs operations
with tab_weather:
    st.subheader("Delay by weather condition at arrival")

    cov = q("SELECT COUNT(*) AS n, SUM(CASE WHEN has_weather_match THEN 1 ELSE 0 END) AS matched FROM fct_flight_events").iloc[0]
    if int(cov.MATCHED) == 0:
        st.warning(
            "No flights are matched to weather yet. Weather history only extends back to "
            "when hourly collection began, and AviationStack reports arrivals with a lag, "
            "so the two windows take time to overlap."
        )
    else:
        st.caption(
            f"{int(cov.MATCHED):,} of {int(cov.N):,} flights matched to an observation within "
            "120 minutes of arrival. Readings staler than that are discarded rather than reported."
        )

    # Ranking and folding happen in SQL so the average stays a true average over
    # flights rather than an average of per-condition averages.
    w = q("""
        WITH ranked AS (
            SELECT weather_main,
                   ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
            FROM fct_flight_events
            WHERE has_weather_match
            GROUP BY 1
        )
        SELECT f.arrival_airport AS airport,
               CASE WHEN r.rn <= 3 THEN f.weather_main ELSE 'Other' END AS condition,
               MIN(r.rn) AS rank,
               COUNT(*) AS flights,
               ROUND(AVG(f.arrival_delay_minutes), 1) AS avg_arr_delay
        FROM fct_flight_events f
        JOIN ranked r ON r.weather_main = f.weather_main
        WHERE f.has_weather_match
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)

    if w.empty:
        st.info("Nothing to plot until the weather join populates.")
    else:
        # Colour follows the condition's overall rank, not its position in this
        # chart, so filtering never repaints the survivors.
        order = w[["CONDITION", "RANK"]].drop_duplicates().sort_values("RANK")
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

    f = q("""
        SELECT
          (SELECT COUNT(*) FROM stg_flights_raw)  AS flight_files,
          (SELECT COUNT(*) FROM stg_weather_raw)  AS weather_files,
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
              delta_color="off", help="Codeshare labels and re-pull duplicates removed")
    # Collection failures are otherwise invisible: Airflow marks the run red in
    # a UI nobody watches continuously. Surfacing staleness here means the
    # symptom shows up where the data is actually looked at.
    fresh = q("""
        SELECT DATEDIFF('minute', (SELECT MAX(observed_at) FROM stg_weather), SYSDATE()) AS weather_age_min,
               DATEDIFF('hour',   (SELECT MAX(arrival_actual_utc) FROM fct_flight_events), SYSDATE()) AS flights_age_hr
    """).iloc[0]

    wx_age, fl_age = fresh.WEATHER_AGE_MIN, fresh.FLIGHTS_AGE_HR
    cols = st.columns(2)
    # Weather runs hourly and flights daily, so these are roughly one missed
    # run's worth of slack before something is genuinely wrong.
    if wx_age is not None and wx_age > 120:
        cols[0].error(f"Weather is {wx_age/60:.1f}h stale — hourly collection may have stopped")
    else:
        cols[0].success(f"Weather current ({wx_age:.0f} min old)" if wx_age is not None else "No weather yet")
    if fl_age is not None and fl_age > 30:
        cols[1].error(f"Newest flight arrival is {fl_age:.0f}h old — daily collection may have stopped")
    else:
        cols[1].success(f"Flights current (newest arrival {fl_age:.0f}h ago)" if fl_age is not None else "No flights yet")

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

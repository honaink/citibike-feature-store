"""Live dashboard: next-hour demand per station next to live availability."""

from __future__ import annotations

import os

import altair as alt
import pandas as pd
import pydeck as pdk
import requests
import streamlit as st

API_URL = os.environ.get("API_URL", "http://localhost:8000")
REFRESH_SECONDS = 30

SERIES_BLUE = "#2a78d6"
STATUS_CRITICAL = "#d03b3b"

# Feature views shown in the freshness strip, with the age past which they are stale.
FRESHNESS = {
    "station_recent_activity": ("Trip activity (stream)", 30 * 60),
    "station_live_status": ("Bike availability (stream)", 10 * 60),
    "weather_hourly": ("Weather (stream)", 3 * 3600),
    "station_hourly_profile": ("Weekly profile (batch)", 90 * 86400),
}

st.set_page_config(page_title="Citi Bike demand", page_icon="🚲", layout="wide")


def fetch(path: str, method: str = "get", **kwargs) -> dict | list | None:
    try:
        resp = requests.request(method, f"{API_URL}{path}", timeout=30, **kwargs)
    except requests.RequestException as exc:
        st.error(f"Cannot reach the API at {API_URL}: {exc}")
        return None
    if resp.status_code != 200:
        st.warning(f"{path}: {resp.json().get('detail', resp.text)}")
        return None
    return resp.json()


def describe_age(seconds: float | None) -> str:
    if seconds is None:
        return "no data"
    if seconds < 120:
        return f"{seconds:.0f} s old"
    if seconds < 7200:
        return f"{seconds / 60:.0f} min old"
    if seconds < 172800:
        return f"{seconds / 3600:.0f} h old"
    return f"{seconds / 86400:.0f} days old"


def freshness_strip(ages: dict[str, float | None]) -> None:
    cols = st.columns(len(FRESHNESS))
    for col, (view, (label, limit)) in zip(cols, FRESHNESS.items(), strict=True):
        age = ages.get(view)
        if age is None:
            icon, state = "⚠️", "no data yet"
        elif age > limit:
            icon, state = "⚠️", f"stale · {describe_age(age)}"
        else:
            icon, state = "✅", f"fresh · {describe_age(age)}"
        col.markdown(f"**{label}**  \n{icon} {state}")


def station_frame(payload: dict) -> pd.DataFrame:
    rows = []
    for s in payload["stations"]:
        f = s["features"]
        rows.append(
            {
                "Station": s["name"] or s["station_id"],
                "station_id": s["station_id"],
                "lat": s["lat"],
                "lon": s["lon"],
                "Predicted next hour": s["predicted_departures_next_hour"],
                "Bikes now": s["bikes_available"],
                "Docks now": s["docks_available"],
                "Departures last hour": f.get("departures_1h") or 0,
                "Typical for this hour": f.get("avg_departures_4w") or 0,
                "Risk": "⚠️ may run out" if s["stockout_risk"] else "✅ ok",
                "at_risk": s["stockout_risk"],
            }
        )
    return pd.DataFrame(rows)


def demand_map(df: pd.DataFrame) -> None:
    located = df.dropna(subset=["lat", "lon"]).copy()
    if located.empty:
        return
    located["color"] = located["at_risk"].map(
        {True: [208, 59, 59, 220], False: [42, 120, 214, 200]}
    )
    located["radius"] = 25 + 18 * located["Predicted next hour"]
    layer = pdk.Layer(
        "ScatterplotLayer",
        located,
        get_position=["lon", "lat"],
        get_radius="radius",
        get_fill_color="color",
        get_line_color=[255, 255, 255, 230],
        line_width_min_pixels=1,
        stroked=True,
        pickable=True,
    )
    view = pdk.ViewState(latitude=located["lat"].mean(), longitude=located["lon"].mean(), zoom=12.5)
    st.pydeck_chart(
        pdk.Deck(
            layers=[layer],
            initial_view_state=view,
            map_style=None,
            tooltip={
                "text": "{Station}\nPredicted next hour: {Predicted next hour}\n"
                "Bikes now: {Bikes now}\n{Risk}"
            },
        ),
        height=460,
    )
    st.markdown(
        f"<span style='color:{SERIES_BLUE}'>●</span> enough bikes &nbsp;&nbsp; "
        f"<span style='color:{STATUS_CRITICAL}'>●</span> ⚠️ more departures expected than "
        "bikes on hand &nbsp;&nbsp; circle size = predicted departures",
        unsafe_allow_html=True,
    )


def top_stations_chart(df: pd.DataFrame, n: int = 15) -> None:
    top = df.nlargest(n, "Predicted next hour")
    chart = (
        alt.Chart(top)
        .mark_bar(color=SERIES_BLUE, cornerRadiusEnd=4, size=14)
        .encode(
            x=alt.X("Predicted next hour:Q", title="Predicted departures, next hour"),
            y=alt.Y("Station:N", sort="-x", title=None, axis=alt.Axis(labelLimit=240)),
            tooltip=[
                "Station",
                alt.Tooltip("Predicted next hour:Q", format=".1f"),
                "Departures last hour",
                alt.Tooltip("Typical for this hour:Q", format=".1f"),
                "Bikes now",
            ],
        )
        .properties(height=alt.Step(22))
        .configure_axis(grid=False)
        .configure_view(strokeWidth=0)
    )
    st.altair_chart(chart, use_container_width=True)


@st.fragment(run_every=REFRESH_SECONDS)
def live_view() -> None:
    payload = fetch("/predict", method="post", json={})
    if not payload:
        return
    df = station_frame(payload)

    freshness_strip(payload["feature_age_seconds"])
    st.divider()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Stations", len(df))
    k2.metric("Departures expected, next hour", f"{df['Predicted next hour'].sum():.0f}")
    k3.metric("Stations that may run out", int(df["at_risk"].sum()))
    k4.metric("Feature lookup", f"{payload['feature_retrieval_ms']:.0f} ms")

    left, right = st.columns([3, 2])
    with left:
        demand_map(df)
    with right:
        st.markdown("**Busiest stations, next hour**")
        top_stations_chart(df)

    st.dataframe(
        df.sort_values("Predicted next hour", ascending=False)[
            [
                "Station",
                "Predicted next hour",
                "Bikes now",
                "Docks now",
                "Departures last hour",
                "Typical for this hour",
                "Risk",
            ]
        ],
        hide_index=True,
        use_container_width=True,
        column_config={
            "Predicted next hour": st.column_config.NumberColumn(format="%.1f"),
            "Typical for this hour": st.column_config.NumberColumn(format="%.1f"),
        },
    )
    st.caption(
        f"Updated {pd.Timestamp(payload['generated_at']).tz_convert('America/New_York'):%H:%M:%S} "
        f"New York time · refreshes every {REFRESH_SECONDS} s"
    )


st.title("Citi Bike: departures in the next hour")
st.caption(
    "Predictions use features served by Feast: trip activity and bike availability "
    "computed by Spark Structured Streaming, a weekly demand profile computed in batch, "
    "and live weather."
)
live_view()

with st.expander("Model"):
    info = fetch("/model")
    if info:
        metrics = pd.DataFrame(info["metrics"]).T.rename(
            index={
                "model": "Model",
                "baseline_profile": "Typical for this hour (batch feature only)",
                "baseline_last_hour": "Same as the last hour",
            }
        )
        st.markdown(
            f"Trained {info['trained_at'][:16].replace('T', ' ')} UTC on "
            f"{info['rows']['train']:,} rows; validated on {info['rows']['validation']:,} "
            f"rows from {info['validation_from'][:10]} onwards."
        )
        st.dataframe(
            metrics.rename(columns={"mae": "MAE", "rmse": "RMSE"}).style.format("{:.3f}"),
            use_container_width=True,
        )

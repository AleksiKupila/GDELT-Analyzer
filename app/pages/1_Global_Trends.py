import streamlit as st
import pydeck as pdk
import pandas as pd

from core.queries import get_ui_data, get_tone_extremes

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Global Trends · GDELT Analyzer",
    page_icon="🗺️",
    layout="wide",
)

# ── Custom CSS ───────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    .block-container { padding-top: 2rem; padding-bottom: 3rem; }
    hr { border-color: #2E3450; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Header ───────────────────────────────────────────────────────────────────
st.markdown("# 🗺️ Global Trends")
st.markdown(
    "Insight into global patterns in news reporting based on publicly available "
    "data from the [GDELT Project](https://www.gdeltproject.org/)."
)
st.markdown("---")

# ── Data loading ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner="Loading data from database…")
def load_global_data():
    """Load all global-trend datasets; returns a tuple of DataFrames / dicts."""
    top_events_df      = get_ui_data("top_events")
    events_per_country = get_ui_data("events_per_country", limit=15)
    tone_extremes      = get_tone_extremes(limit=15)
    return top_events_df, events_per_country, tone_extremes


try:
    top_events_df, events_per_country, tone_extremes = load_global_data()
except Exception as exc:
    st.error(
        f"❌ Could not connect to the database: {exc}\n\n"
        "Make sure MongoDB is running and the GDELT pipeline has been executed."
    )
    st.stop()

pos_tone = tone_extremes.get("most_positive", [])
neg_tone = tone_extremes.get("most_negative", [])

pos_events = pd.DataFrame(pos_tone)
neg_events = pd.DataFrame(neg_tone)

# ── Section 1: Heatmap ───────────────────────────────────────────────────────
st.subheader("Heatmap of top reported events from the last 7 days")

if top_events_df.empty:
    st.warning("No top-event data found. Run the pipeline to populate the database.")
else:
    required_cols = {"lon", "lat"}
    if required_cols.issubset(top_events_df.columns):
        layer = pdk.Layer(
            "HeatmapLayer",
            data=top_events_df,
            get_position="[lon, lat]",
            threshold=0.2,
            opacity=0.85,
        )
        st.pydeck_chart(
            pdk.Deck(
                layers=[layer],
                initial_view_state=pdk.ViewState(
                    latitude=20, longitude=0, zoom=1.4, pitch=40
                ),
            )
        )
    else:
        st.warning("Top-event data is missing `lon`/`lat` columns — cannot render heatmap.")

    with st.expander("Show raw top-event data"):
        st.dataframe(top_events_df, use_container_width=True, hide_index=True)

st.markdown("---")

# ── Section 2: Events per country ────────────────────────────────────────────
st.subheader("📊 Total reported events per country  (top 15)")

if events_per_country.empty:
    st.warning("No events-per-country data found.")
elif {"Country_Name", "Total_Events"}.issubset(events_per_country.columns):
    st.bar_chart(
        events_per_country,
        x="Country_Name",
        y="Total_Events",
        x_label="Country",
        y_label="Total events",
        sort="-Total_Events",
        color="#4FC3F7",
        use_container_width=True,
    )
    with st.expander("Show raw events-per-country data"):
        st.dataframe(events_per_country, use_container_width=True, hide_index=True)
else:
    st.warning("Events-per-country data is missing expected columns.")

st.markdown("---")

# ── Section 3: Tone analysis ─────────────────────────────────────────────────
col_pos, col_neg = st.columns(2, gap="large")

with col_pos:
    st.subheader("Most positive average tone")
    if pos_events.empty:
        st.info("No tone data available.")
    else:
        st.bar_chart(
            pos_events,
            x="Country_Name",
            y="Average_Tone",
            x_label="Country",
            y_label="Average tone",
            sort="-Average_Tone",
            color="#4CAF50",
            use_container_width=True,
        )

with col_neg:
    st.subheader("Most negative average tone")
    if neg_events.empty:
        st.info("No tone data available.")
    else:
        st.bar_chart(
            neg_events,
            x="Country_Name",
            y="Average_Tone",
            x_label="Country",
            y_label="Average tone",
            sort="Average_Tone",
            color="#EF5350",
            use_container_width=True,
        )

if not pos_events.empty or not neg_events.empty:
    with st.expander("Show raw average-tone data"):
        st.write("**Most positive**")
        st.dataframe(pos_events, use_container_width=True, hide_index=True)
        st.write("**Most negative**")
        st.dataframe(neg_events, use_container_width=True, hide_index=True)

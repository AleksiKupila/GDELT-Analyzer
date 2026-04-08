import streamlit as st 
from core.queries import *
from core.mongo_utils import *
from datetime import datetime, timedelta

st.title("Search by country")
st.subheader("Select events:")

country = st.text_input("Country", "United States")

actor1 = st.text_input("Actor 1", "United States")

actor2 = st.text_input("Actor 2", "Iran")

date_min = st.datetime_input(
    "From date:",
    datetime.now()-timedelta(hours=48),
    )

date_max = st.datetime_input(
    "To date:",
    datetime.now(),
    )

g_min, g_max = st.select_slider(
        "Goldstein scale range",
        options=range(-10, 11),
        value=(-10, 10),
    )

t_min, t_max = st.select_slider(
        "Tone scale range",
        options=range(-10, 11),
        value=(-10, 10),
    )

#st.write(date_min, date_max, g_min, g_max)
if st.button("Query for results"):
    goldstein_min, goldstein_max = sorted((g_min, g_max))
    tone_min, tone_max = sorted((t_min, t_max))

    country_filter = country.strip() or None
    actor1_filter = actor1.strip() or None
    actor2_filter = actor2.strip() or None

    result = get_user_queried_events(
        country=country_filter,
        actor1=actor1_filter,
        actor2=actor2_filter,
        date_min=date_min,
        date_max=date_max,
        goldstein_min=goldstein_min,
        goldstein_max=goldstein_max,
        tone_min=tone_min,
        tone_max=tone_max,
    )
    st.write(result)


# ---------------------------------------------------------------------------
# Event Spike Detection
# ---------------------------------------------------------------------------
st.divider()
st.header("🚨 Country Event Spike Detection")
st.markdown(
    """
    Compares recent event activity (last **24 h**) against a **6-day hourly
    baseline** to surface countries with abnormally high event volumes.

    | Column | Meaning |
    |---|---|
    | `spike_events` | Raw events recorded in the spike window |
    | `baseline_events` | Total events recorded in the baseline window |
    | `hourly_baseline_rate` | Average events per hour during the baseline |
    | `expected_spike_events` | Events expected in the spike window at the baseline rate |
    | `spike_score` | Relative excess: `(actual − expected) / expected` |
    | `is_spike` | `True` when `spike_score > 0.5` (>50 % above baseline) |
    """
)

col_filter, col_limit = st.columns([2, 1])
with col_filter:
    only_spikes = st.checkbox("Show only flagged spikes (is_spike = True)", value=False)
with col_limit:
    spike_limit = st.number_input("Max rows", min_value=5, max_value=200, value=50, step=5)

if st.button("Load spike data"):
    spike_df = get_country_event_spikes(only_spikes=only_spikes, limit=spike_limit)

    if spike_df.empty:
        st.warning(
            "No spike data found in MongoDB. "
            "Run the pipeline first so that `country_event_spike` is computed and stored."
        )
    else:
        flagged = spike_df[spike_df["is_spike"] == True]
        total = len(spike_df)
        n_flagged = len(flagged)

        st.metric("Countries loaded", total)
        st.metric("Countries flagged as spike 🚨", n_flagged)

        if n_flagged:
            st.subheader("🚨 Flagged countries")
            st.dataframe(
                flagged[
                    ["ActionGeo_CountryCode", "spike_events", "expected_spike_events",
                     "spike_score", "hourly_baseline_rate", "baseline_events"]
                ].style.format(
                    {
                        "spike_score": "{:.2%}",
                        "hourly_baseline_rate": "{:.2f}",
                        "expected_spike_events": "{:.1f}",
                    }
                ),
                use_container_width=True,
            )

        st.subheader("All countries by spike score")
        st.dataframe(
            spike_df[
                ["ActionGeo_CountryCode", "spike_events", "expected_spike_events",
                 "spike_score", "hourly_baseline_rate", "baseline_events", "is_spike"]
            ].style.format(
                {
                    "spike_score": "{:.2%}",
                    "hourly_baseline_rate": "{:.2f}",
                    "expected_spike_events": "{:.1f}",
                }
            ),
            use_container_width=True,
        )

        # Bar chart of the top-N spike scores
        chart_df = spike_df.set_index("ActionGeo_CountryCode")[["spike_score"]].head(20)
        st.subheader("Top 20 countries by spike score")
        st.bar_chart(chart_df)

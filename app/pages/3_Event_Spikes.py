import streamlit as st

from core.queries import get_country_event_spikes

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Event Spikes · GDELT Analyzer",
    page_icon="📈",
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
st.markdown("# 📈 Event Spike Detection")
st.markdown(
    "Identify countries with **abnormally high event volumes** by comparing "
    "recent 24-hour activity against a rolling 6-day hourly baseline."
)
st.markdown("---")

# ── Column reference ─────────────────────────────────────────────────────────
with st.expander("ℹ️  Column reference"):
    st.markdown(
        """
        | Column | Meaning |
        |---|---|
        | `spike_events` | Raw events recorded in the spike window (last 24 h) |
        | `baseline_events` | Total events recorded in the 6-day baseline window |
        | `hourly_baseline_rate` | Average events per hour during the baseline |
        | `expected_spike_events` | Events expected in the spike window at the baseline rate |
        | `spike_score` | Relative excess: `(actual − expected) / expected` |
        | `is_spike` | `True` when `spike_score > 0.5` (> 50 % above baseline) |
        """
    )

# ── Controls ─────────────────────────────────────────────────────────────────
col_filter, col_limit = st.columns([2, 1], gap="medium")

with col_filter:
    only_spikes = st.checkbox(
        "Show only flagged spikes  (`is_spike = True`)",
        value=False,
        help="Filters the result set to countries that exceeded 50 % of their baseline rate.",
    )
with col_limit:
    spike_limit = st.number_input(
        "Max rows",
        min_value=5,
        max_value=500,
        value=50,
        step=5,
        help="Maximum number of country rows to load.",
    )

st.markdown(" ")
load_btn = st.button("🔄  Load spike data", use_container_width=False)

# ── Results ───────────────────────────────────────────────────────────────────
if load_btn:
    with st.spinner("Querying database…"):
        try:
            spike_df = get_country_event_spikes(
                only_spikes=only_spikes,
                limit=int(spike_limit),
            )
        except Exception as exc:
            st.error(
                f"❌ Database query failed: {exc}\n\n"
                "Make sure MongoDB is running and the pipeline has been executed."
            )
            st.stop()

    st.markdown("---")

    if spike_df.empty:
        st.warning(
            "No spike data found in MongoDB. "
            "Run the pipeline first so that the `country_event_spike` "
            "collection is computed and stored."
        )
        st.stop()

    # ── Summary metrics ───────────────────────────────────────────────────
    flagged    = spike_df[spike_df["is_spike"] == True]
    total      = len(spike_df)
    n_flagged  = len(flagged)
    #pct_spiked = (n_flagged / total * 100) if total > 0 else 0

    m1, m2, m3 = st.columns(3, gap="medium")
    m1.metric("Countries loaded",          total)
    m2.metric("Countries flagged as spike", n_flagged)
    #m3.metric("Spike rate",                f"{pct_spiked:.1f}%")

    # ── Flagged countries ─────────────────────────────────────────────────
    if n_flagged:
        st.markdown("### Flagged countries")

        FLAGGED_COLS = [
            "ActionGeo_CountryCode", "spike_events", "expected_spike_events",
            "spike_score", "hourly_baseline_rate", "baseline_events",
        ]
        available_flagged = [c for c in FLAGGED_COLS if c in flagged.columns]

        st.dataframe(
            flagged[available_flagged].style.format(
                {
                    "spike_score":            "{:.2%}",
                    "hourly_baseline_rate":   "{:.2f}",
                    "expected_spike_events":  "{:.1f}",
                },
                na_rep="—",
            ),
            use_container_width=True,
            hide_index=True,
        )

    # ── All countries table ───────────────────────────────────────────────
    st.markdown("### All countries by spike score")

    ALL_COLS = [
        "ActionGeo_CountryCode", "spike_events", "expected_spike_events",
        "spike_score", "hourly_baseline_rate", "baseline_events", "is_spike",
    ]
    available_all = [c for c in ALL_COLS if c in spike_df.columns]

    st.dataframe(
        spike_df[available_all].style.format(
            {
                "spike_score":            "{:.2%}",
                "hourly_baseline_rate":   "{:.2f}",
                "expected_spike_events":  "{:.1f}",
            },
            na_rep="—",
        ),
        use_container_width=True,
        hide_index=True,
    )

    # ── Bar chart ─────────────────────────────────────────────────────────
    if "ActionGeo_CountryCode" in spike_df.columns and "spike_score" in spike_df.columns:
        st.markdown("### Top 20 countries by spike score")
        chart_df = (
            spike_df
            .set_index("ActionGeo_CountryCode")[["spike_score"]]
            .head(20)
        )
        st.bar_chart(chart_df, color="#4FC3F7", use_container_width=True)

    # ── Download ──────────────────────────────────────────────────────────
    csv_data = spike_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️  Download spike data as CSV",
        data=csv_data,
        file_name="gdelt_spikes.csv",
        mime="text/csv",
    )

import time as _time
import streamlit as st
from datetime import datetime, timedelta

from core.queries import get_user_queried_events

_PAGE_START = _time.perf_counter()

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Country Insights · GDELT Analyzer",
    page_icon="🔍",
    layout="wide",
)

# ── Custom CSS ───────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    .block-container { padding-top: 2rem; padding-bottom: 3rem; }
    hr { border-color: #2E3450; }
    .result-meta { color: #7A8099; font-size: 0.85rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Header ───────────────────────────────────────────────────────────────────
st.markdown("# 🔍 Country Insights")
st.markdown(
    "Query events by country, actors, date range, and numeric filters. "
    "Results are sorted by article count (most-covered events first)."
)
st.markdown("---")

# ── Filter form ──────────────────────────────────────────────────────────────
with st.form("event_query_form"):

    # --- Row 1: text filters ------------------------------------------------
    st.markdown("#### 🌍 Location & Actors")
    col_country, col_actor1, col_actor2 = st.columns(3, gap="medium")

    with col_country:
        country = st.text_input(
            "Country / Location",
            placeholder="e.g. United States",
            help="Partial match is supported (case-insensitive).",
        )
    with col_actor1:
        actor1 = st.text_input(
            "Actor 1",
            placeholder="e.g. UNITED STATES",
            help="Leave blank to include all actors.",
        )
    with col_actor2:
        actor2 = st.text_input(
            "Actor 2",
            placeholder="e.g. IRAN",
            help="Leave blank to include all actors.",
        )

    st.markdown("#### 📅 Date Range")
    col_date1, col_date2 = st.columns(2, gap="medium")

    with col_date1:
        date_min = st.datetime_input(
            "From date",
            value=datetime(year=2026, month=4, day=5, hour=23,minute=0),
            help="Start of the event date window.",
        )
    with col_date2:
        date_max = st.datetime_input(
            "To date",
            value=datetime.now(),
            help="End of the event date window.",
        )

    st.markdown("#### 🎚️ Numeric Filters")

    col_gs, col_tone = st.columns(2, gap="medium")

    with col_gs:
        g_min, g_max = st.select_slider(
            "Goldstein Scale range  (−10 … +10)",
            options=[x / 10 for x in range(-100, 101)],
            value=(-10.0, 10.0),
            help=(
                "The Goldstein Scale measures the theoretical potential impact "
                "of an event on country stability. −10 = most destabilising, "
                "+10 = most stabilising."
            ),
        )

    with col_tone:
        t_min, t_max = st.select_slider(
            "Average Tone range  (−100 … +100)",
            options=list(range(-100, 101)),
            value=(-100, 100),
            help=(
                "Average tone of news articles covering this event. "
                "Negative = hostile / negative framing, "
                "Positive = cooperative / positive framing."
            ),
        )

    col_ment, col_limit = st.columns(2, gap="medium")

    with col_ment:
        mentions_min = st.number_input(
            "Minimum mentions",
            min_value=0,
            max_value=100_000,
            value=0,
            step=10,
            help="Only return events mentioned at least this many times across all sources.",
        )
        mentions_max_toggle = st.checkbox("Set a maximum mentions cap", value=False)
        mentions_max: int | None = None
        if mentions_max_toggle:
            mentions_max = st.number_input(
                "Maximum mentions",
                min_value=int(mentions_min),
                max_value=100_000,
                value=max(int(mentions_min) + 100, 500),
                step=50,
            )

    with col_limit:
        result_limit = st.number_input(
            "Max results",
            min_value=1,
            max_value=500,
            value=50,
            step=10,
            help="Maximum number of events returned. Higher values may increase query time.",
        )

    # --- Submit -------------------------------------------------------------
    st.markdown(" ")
    submitted = st.form_submit_button("🔎  Search events", width="stretch")

# ── Query + results ──────────────────────────────────────────────────────────
if submitted:
    # Ensure date order
    if date_min > date_max:
        st.error("⚠️ 'From date' must be earlier than 'To date'. Please adjust the date range.")
        st.stop()

    goldstein_min, goldstein_max = sorted((g_min, g_max))
    tone_min, tone_max = sorted((t_min, t_max))

    country_filter = country.strip() or None
    actor1_filter  = actor1.strip()  or None
    actor2_filter  = actor2.strip()  or None

    with st.spinner("Querying database…"):
        try:
            result_df = get_user_queried_events(
                country=country_filter,
                actor1=actor1_filter,
                actor2=actor2_filter,
                date_min=date_min,
                date_max=date_max,
                goldstein_min=goldstein_min,
                goldstein_max=goldstein_max,
                tone_min=float(tone_min),
                tone_max=float(tone_max),
                mentions_min=int(mentions_min),
                mentions_max=int(mentions_max) if mentions_max is not None else None,
                limit=int(result_limit),
            )
        except Exception as exc:
            st.error(f"❌ Database query failed: {exc}")
            st.stop()

    st.markdown("---")

    if result_df.empty:
        st.info(
            "No events matched your filters. "
            "Try broadening the date range, relaxing numeric thresholds, "
            "or clearing one of the actor fields."
        )
    else:
        n = len(result_df)
        st.success(f"Found **{n}** event{'s' if n != 1 else ''}.")

        # ── Summary metrics ──────────────────────────────────────────────
        if {"num_mentions", "num_articles", "goldstein_scale", "avg_tone"}.issubset(result_df.columns):
            m1, m2, m3, m4 = st.columns(4, gap="medium")
            m1.metric("Total mentions",  int(result_df["num_mentions"].sum()))
            m2.metric("Total articles",  int(result_df["num_articles"].sum()))
            m3.metric(
                "Avg Goldstein",
                f"{result_df['goldstein_scale'].mean():.2f}",
            )
            m4.metric("Avg Tone", f"{result_df['avg_tone'].mean():.2f}")

        st.markdown(" ")

        # ── Results table ────────────────────────────────────────────────
        # Re-order columns for readability
        preferred_cols = [
            "event_date", "ActionGeo_FullName", "Actor1Name", "Actor2Name",
            "EventDescription", "QuadClass",
            "num_mentions", "num_articles",
            "goldstein_scale", "avg_tone",
            "SOURCEURL",
        ]
        display_cols = [c for c in preferred_cols if c in result_df.columns]
        extra_cols   = [c for c in result_df.columns if c not in display_cols and c not in ("lon", "lat")]
        display_df   = result_df[display_cols + extra_cols]

        st.dataframe(display_df, width="stretch", hide_index=True)

        # ── Download ─────────────────────────────────────────────────────
        csv_data = display_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️  Download results as CSV",
            data=csv_data,
            file_name="gdelt_events.csv",
            mime="text/csv",
        )

# ── Page load timer ───────────────────────────────────────────────────────────
_elapsed = _time.perf_counter() - _PAGE_START
st.markdown(
    f"<p style='text-align:right;color:#4A5068;font-size:0.75rem;margin-top:2rem;'>"
    f"⏱ Page rendered in {_elapsed:.2f} s</p>",
    unsafe_allow_html=True,
)

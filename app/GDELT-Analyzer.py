import time as _time
import streamlit as st

_PAGE_START = _time.perf_counter()

# ── Page config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="GDELT Analyzer",
    page_icon="🌐",
    layout="wide",
)

# ── Custom CSS – subtle refinements on top of the dark theme ─────────────────
st.markdown(
    """
    <style>
    /* Reduce top padding on the main block */
    .block-container { padding-top: 2rem; padding-bottom: 3rem; }

    /* Feature cards */
    .feature-card {
        background: #1C2130;
        border: 1px solid #2E3450;
        border-radius: 12px;
        padding: 1.4rem 1.6rem;
        height: 100%;
    }
    .feature-card h3 { margin-top: 0; color: #4FC3F7; }
    .feature-card p  { color: #B0B8CC; font-size: 0.93rem; line-height: 1.55; }

    /* Divider */
    hr { border-color: #2E3450; }

    /* Badge pill */
    .badge {
        display: inline-block;
        background: #1C3A50;
        color: #4FC3F7;
        border-radius: 20px;
        padding: 2px 12px;
        font-size: 0.78rem;
        margin-right: 6px;
        margin-bottom: 4px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Hero section ─────────────────────────────────────────────────────────────
st.markdown("# 🌐 GDELT Analyzer")
st.markdown(
    "##### Real-time intelligence from the **Global Database of Events, Language and Tone**"
)
st.markdown("---")

# ── About section ────────────────────────────────────────────────────────────
col_about, col_badges = st.columns([3, 1], gap="large")

with col_about:
    st.markdown(
        """
        **GDELT Analyzer** is a data pipeline and analytics dashboard that ingests
        event data from the [GDELT Project](https://www.gdeltproject.org/) — one of the
        largest open datasets of human society, tracking news media across the globe in
        near real-time.

        The pipeline processes raw GDELT files using **Apache Spark**, stores the results
        in **MongoDB**, and surfaces insights through this interactive dashboard.
        """
    )

with col_badges:
    st.markdown("<br>", unsafe_allow_html=True)
    for badge in ["Apache Spark", "MongoDB", "Streamlit", "Python", "GDELT"]:
        st.markdown(f'<span class="badge">{badge}</span>', unsafe_allow_html=True)

st.markdown("---")

# ── Feature cards ─────────────────────────────────────────────────────────────
st.markdown("### 📊 Explore the Dashboard")
st.markdown(" ")

c1, c2, c3 = st.columns(3, gap="medium")

with c1:
    st.markdown(
        """
        <div class="feature-card">
            <h3>🗺️ Global Trends</h3>
            <p>
                Explore worldwide event distributions through an interactive heatmap,
                top-event rankings, and tone analysis across all reporting countries.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(" ")
    st.page_link("pages/1_Global_Trends.py", label="Open Global Trends →", icon="🗺️")

with c2:
    st.markdown(
        """
        <div class="feature-card">
            <h3>🔍 Country Insights</h3>
            <p>
                Drill down into specific events by country, actor, date range,
                Goldstein scale, tone, and mention count to surface the stories
                that matter most.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(" ")
    st.page_link("pages/2_Country_Insights.py", label="Open Country Insights →", icon="🔍")

with c3:
    st.markdown(
        """
        <div class="feature-card">
            <h3>📈 Event Spikes</h3>
            <p>
                Detect countries experiencing abnormal surges in event reporting
                by comparing recent 24-hour activity against a 6-day hourly baseline.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(" ")
    st.page_link("pages/3_Event_Spikes.py", label="Open Event Spikes →", icon="📈")

st.markdown("---")

# ── Data source ───────────────────────────────────────────────────────────────
st.markdown(
    """
    #### 📡 Data Source
    All data is sourced from the **[GDELT Project](https://www.gdeltproject.org/)** —
    an open, real-time dataset of global human society going back to 1979, updated
    every 15 minutes and available for free under an open license.
    """,
    unsafe_allow_html=False,
)

st.caption("GDELT Analyzer — University project · Data © The GDELT Project")

# ── Page load timer ───────────────────────────────────────────────────────────
_elapsed = _time.perf_counter() - _PAGE_START
st.markdown(
    f"<p style='text-align:right;color:#4A5068;font-size:0.75rem;margin-top:1rem;'>"
    f"⏱ Page rendered in {_elapsed:.2f} s</p>",
    unsafe_allow_html=True,
)

import streamlit as st 
from core.queries import *
from core.mongo_utils import *
from datetime import datetime, timedelta

st.write("Select events:")

country = st.text_input("Country", "United States")

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
    result = get_user_queried_events(
                country=country,
                date_min=date_min,
                date_max=date_max,
                goldstein_min=goldstein_min,
                goldstein_max=goldstein_max,
                tone_min = tone_min,
                tone_max = tone_max
            )
    st.write(result)
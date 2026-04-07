import streamlit as st 
import pydeck as pdk
from core.queries import *
from core.mongo_utils import *

client = get_mongodb_client()

st.title('Global trends')
st.write(
    "This section offers insight into global trends in reporting." \
    "The information is based on publicly available data from the GDELT project."
)

top_events_df = get_ui_data("top_events")
events_per_country = get_ui_data("events_per_country", 15)
tone_by_country = get_tone_extremes(15)
pos_events = tone_by_country["most_positive"]
neg_events = tone_by_country["most_negative"]

st.subheader("Map of top reported events worldwide")

layer = pdk.Layer(
    "HeatmapLayer",
    data=top_events_df,
    get_position="[lon, lat]",
    threshold=0.2,
)

st.pydeck_chart(pdk.Deck(
    map_style=None,
    layers=[layer],
    initial_view_state=pdk.ViewState(latitude=20, longitude=0, zoom=1, pitch=50),
))

if st.checkbox("Show raw top event data"):
    st.subheader("Raw data of top events")
    st.write(top_events_df)

st.subheader("Total reported events per country")

st.bar_chart(
    events_per_country, 
    x="Country_Name", 
    y="Total_Events",
    x_label="Location / country",
    y_label="Total events",
    sort="-Total_Events",
    
    )

if st.checkbox("Show raw data of events per country"):
    st.subheader("Raw data of events per country")
    st.write(events_per_country)

st.subheader("Countries with most positive average tone in reporting")

st.bar_chart(
    pos_events,
    x = "Country_Name",
    y = "Average_Tone",
    x_label= "Location / country",
    y_label= "Average tone",
    sort= "-Average_Tone"
)

st.subheader("Countries with most negative average tone in reporting")

st.bar_chart(
    neg_events,
    x = "Country_Name",
    y = "Average_Tone",
    x_label= "Location / country",
    y_label= "Average tone",
    sort= "Average_Tone",
    color= "red"

)

if st.checkbox("Show raw data of average tone"):
    st.subheader("Raw data of average tone by country")
    st.write(tone_by_country)
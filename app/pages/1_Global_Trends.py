from pymongo import MongoClient
import streamlit as st 
import pandas as pd
import pydeck as pdk
from numpy.random import default_rng as rng

@st.cache_resource
def get_mongodb_client():
    # Standard MongoDB URI
    return MongoClient("mongodb://localhost:27017/")

def get_ui_data(collection_name, limit=100):
    client = get_mongodb_client()
    db = client["gdelt"]
    collection = db[collection_name]
    
    # Fetch data and convert to Pandas immediately
    # We use list() because Pandas can consume a list of dicts directly
    cursor = collection.find().limit(limit)
    df = pd.DataFrame(list(cursor))
    
    # Clean up MongoDB internal ID for cleaner display
    if not df.empty and '_id' in df.columns:
        df.drop(columns=['_id'], inplace=True)
        
    return df

all_events_df = get_ui_data("events")
top_impact_locations = get_ui_data("top_impact_event_locations")

st.subheader("Events with largest impact worldwide")
st.write(all_events_df)

st.subheader("Heatmap of most reported events worldwide")

layer = pdk.Layer(
    "HeatmapLayer",
    data=all_events_df,
    get_position="[lon, lat]",
    threshold=0.1,
)

st.pydeck_chart(pdk.Deck(
    map_style=None,
    layers=[layer],
    initial_view_state=pdk.ViewState(latitude=20, longitude=0, zoom=1, pitch=50),
))

st.map(all_events_df)
from pymongo import MongoClient
import streamlit as st 
import pandas as pd
import pydeck as pdk
from numpy.random import default_rng as rng

@st.cache_resource
def get_mongodb_client():
    # Standard MongoDB URI
    return MongoClient("mongodb://localhost:27017/")

def get_ui_data(collection_name, limit=1000):
    client = get_mongodb_client()
    db = client["gdelt"]
    collection = db[collection_name]
    
    # Fetch data and convert to Pandas DF
    cursor = collection.find().limit(limit)
    df = pd.DataFrame(list(cursor))
    
    # Clean up MongoDB internal ID for cleaner display
    if not df.empty and '_id' in df.columns:
        df.drop(columns=['_id'], inplace=True)
    
    return df

top_events_df = get_ui_data("top_events")
events_per_country = get_ui_data("events_per_country", 15)

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

st.subheader("Total reported events per country")

st.bar_chart(
    events_per_country, 
    x="ActionGeo_CountryCode", 
    y="total_events",
    sort="-total_events",
    
    )
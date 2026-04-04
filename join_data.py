
def join_cameo_df(cameo_df, event_df):
    return event_df.join(cameo_df, on="EventCode", how="inner")

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

client = MongoClient()
db = client.cricketDatabase
matches = db.matches
deliveries = db.deliveries


@st.cache_data
def get_top_scorers():
    pipeline = [
        {
            "$lookup": {
                "from": "deliveries",
                "localField": "id",
                "foreignField": "match_id",
                "as": "deliveries",
                "pipeline": [
                    {"$project": {"batsman": 1, "batsman_runs": 1, "match_id": 1}}
                ]
            }
        },
        {
            "$unwind": "$deliveries"
        },
        {
            "$group": {
                "_id": {
                    "season": "$season",
                    "batsman": "$deliveries.batsman"
                },
                "totalRuns": {"$sum": "$deliveries.batsman_runs"}
            }
        },
        {
            "$sort": {
                "_id.season": 1,
                "totalRuns": -1
            }
        },
        {
            "$group": {
                "_id": "$_id.season",
                "topScorer": {"$first": "$_id.batsman"},
                "runs": {"$first": "$totalRuns"}
            }
        },
        {
            "$sort": {"_id": 1}
        }
    ]
    return list(matches.aggregate(pipeline))


# --------------
# front
st.title("Cricket statistics")

st.divider()
st.header("Performance-based statistics")

st.subheader("Top scorer each season")

top_scorers = get_top_scorers()
top_scorers_df = pd.DataFrame(top_scorers)
top_scorers_df['season'] = top_scorers_df["_id"]
top_scorers_df.drop(columns=["_id"], inplace=True)
st.dataframe(top_scorers_df)
st.bar_chart(top_scorers_df.set_index("topScorer")["runs"])

st.subheader("Top bowler")
st.subheader("Top fielder")

st.divider()
st.header("Team performance statistics")

st.subheader("Team analysis")
st.subheade("Match analysis")

st.divider()
st.header("Trend & Comparative analysis")

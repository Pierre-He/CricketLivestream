import streamlit as st
from pymongo import MongoClient


client = MongoClient()
db = client["bigdata_lab"]
orders = db.orders

# --------------
# front

st.title("Cricket statistics")
st.write(orders.find_one())

# which hour did the winning team score the most runs
# who was the top scorer, how many did he score, and what hour did he peak at

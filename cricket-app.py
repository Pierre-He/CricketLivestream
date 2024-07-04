import sqlite3
import pandas as pd
import streamlit as st 

#run with streamlit run cricket_app.py

st.write('Cricket Data')

#Reader of cricket_data Database
def run_query(query):
    with sqlite3.connect('cricket_data.db') as conn:
        return pd.read_sql_query(query, conn)


#dropdown to choose between team/players and matches/deliveries
option = st.selectbox(
    'Select data to display',
    ('Matches', 'Deliveries')
)
statistics_type = st.selectbox(
    'Select Statistics Type',
    ('Teams','Players')
)

#dataframes of deliveries and matches
df = pd.read_csv('deliveries.csv')
df_matches = pd.read_csv('matches.csv')

# unique team finder, 
unique_teams = pd.concat([df['batting_team'], df['bowling_team']]).unique()
unique_teams = sorted(unique_teams)
unique_players = pd.unique(df[['batsman','non_striker','bowler']].values.ravel())
unique_players = sorted(filter(pd.notnull, unique_players)) 

# Display Teams or Players stats 
if statistics_type == 'Teams':
    st.header('Teams Data')
    st.write(unique_teams)

elif statistics_type == 'Players':
    st.header('Players Data')
    st.write(unique_players)

if option == 'Matches':
    st.header('List of Matches')
    query = "SELECT * FROM matches LIMIT 10"
    data = run_query(query)
    st.dataframe(data)

elif option == 'Deliveries':
    # Display deliveries TO BE CHANGED
    st.header('List of Deliveries')
    query = "SELECT * FROM deliveries LIMIT 10"
    data = run_query(query)
    st.dataframe(data)
    

# Matches info display
st.header('List of Matches & Search')
#j'ai déplacé le df en haut
selected_matches = df_matches[['id', 'season', 'team1', 'team2', 'win_by_runs', 'win_by_wickets', 'winner', 'player_of_match']]
st.write(selected_matches) 


# Typing an ID to show a match
searched_match = st.number_input("Enter a Match ID to spectate it",0, int(df_matches['id'].max()))
st.write("The current match id is ", searched_match)

if (searched_match > 0):
    match_details = df_matches[df_matches['id'] == searched_match]
    st.write(match_details)

    current_match_details = "TBA"
    st.write("Sequences of events : ")
    if st.button("Next ball"):
    
        st.write("New ball!")
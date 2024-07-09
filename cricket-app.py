import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from aggregations import *

st.title("Cricket statistics")
st.divider()

# -------------------------------------------

st.header("General statistics")
col1, col2 = st.columns(2)

df_deliveries = pd.read_csv("deliveries.csv")
df_matches = pd.read_csv("matches.csv")

all_teams = sorted(
    pd.concat([df_deliveries['batting_team'], df_deliveries['bowling_team']]).unique())
all_matches = df_matches[['id', 'season', 'team1', 'team2',
                          'win_by_runs', 'win_by_wickets', 'winner', 'player_of_match']]

col1.subheader("All teams")
col1.table(all_teams)
col2.subheader("All matches")
col2.write(all_matches)

# -------------------------------------------

st.divider()
st.header("Team & Player Performance-Based Statistics")

# -------------------------------------------

st.subheader("Who was the top scorer each season?")

top_scorers = get_top_scorers()
top_scorers_df = pd.DataFrame(top_scorers)
top_scorers_df['season'] = top_scorers_df["_id"]
top_scorers_df['season_topScorer'] = top_scorers_df['season'].astype(
    str) + " - " + top_scorers_df['topScorer']
top_scorers_df.drop(columns=["_id"], inplace=True)
st.bar_chart(top_scorers_df.set_index("season_topScorer")["runs"])

# -------------------------------------------

st.divider()
st.subheader("Which team won the most matches each season?")
col1, col2 = st.columns(2)

top_teams = get_top_teams()
top_teams_df = pd.DataFrame(top_teams)
top_teams_df['season'] = top_teams_df["_id"]
# top_teams_df['season_topTeam'] = top_teams_df['season'].astype(
#     str) + " - " + top_teams_df['topTeam']
top_teams_df.drop(columns=["_id"], inplace=True)
col1.dataframe(top_teams_df)
col2.bar_chart(top_teams_df.set_index("season")["wins"])

# -------------------------------------------

st.divider()
st.subheader("Which match had the highest total runs scored?")

highest_scoring_match = get_highest_scoring_match()
highest_match = highest_scoring_match[0]

col1, col2, col3 = st.columns(3)
with col1:
    st.metric(label="Total Runs", value=highest_match["total_runs"])
with col2:
    st.markdown(
        f"**Teams:** {highest_match['team1']} vs {highest_match['team2']}")
    st.markdown(f"**Date:** {highest_match['date']}")
with col3:
    st.markdown(f"**City:** {highest_match['city']}")
    st.markdown(f"**Venue:** {highest_match['venue']}")

# -------------------------------------------

st.divider()
st.subheader("Which match had the most wickets taken?")
col1, col2, col3 = st.columns(3)

most_wickets_match = get_most_wickets_match()
most_wickets = most_wickets_match[0]

with col1:
    st.metric(label="Total Wickets", value=most_wickets["wickets"])
with col2:
    st.markdown(
        f"**Teams:** {most_wickets['team1']} vs {most_wickets['team2']}")
    st.markdown(f"**Date:** {most_wickets['date']}")
with col3:
    st.markdown(f"**City:** {most_wickets['city']}")
    st.markdown(f"**Venue:** {most_wickets['venue']}")

# -------------------------------------------

st.divider()
st.subheader("Who won the most 'Player of the Match' awards each season?")
col1, col2 = st.columns(2)

top_players_of_match = get_top_players_of_match()
top_players_of_match_df = pd.DataFrame(top_players_of_match)
top_players_of_match_df['season'] = top_players_of_match_df["_id"]
top_players_of_match_df['season_pom'] = top_players_of_match_df['season'].astype(
    str) + " - " + top_players_of_match_df["topPlayer"]
top_players_of_match_df.drop(columns=["_id"], inplace=True)
col1.dataframe(top_players_of_match_df)
col2.bar_chart(top_players_of_match_df.set_index("season_pom")["awards"])

# -------------------------------------------

st.divider()
st.header("Trend & Comparative analysis")

# -------------------------------------------

st.subheader("How did the average team score evolve over the seasons?")
col1, col2 = st.columns(2)

avg_score_evol = get_average_team_score_evolution()
avg_score_evol_df = pd.DataFrame(avg_score_evol)
avg_score_evol_df["season"] = avg_score_evol_df["_id"].apply(
    lambda x: x["season"])
avg_score_evol_df["team"] = avg_score_evol_df["_id"].apply(lambda x: x["team"])
avg_score_evol_df.drop(columns=["_id"], inplace=True)
avg_score_evol_df.set_index(["season", "team"], inplace=True)

fig, ax = plt.subplots(figsize=(10, 6))
for team in avg_score_evol_df.index.get_level_values('team').unique():
    team_data = avg_score_evol_df.xs(team, level='team')
    ax.plot(team_data.index,
            team_data["average_team_score"], marker='o', linestyle='-', label=team)

ax.set_title('Average Team Score Evolution')
ax.set_xlabel('Season')
ax.set_ylabel('Average Team Score')
ax.legend(title='Team')
ax.grid(True)
st.pyplot(fig)

# -------------------------------------------

st.divider()
st.subheader(
    "How does the performance of teams in the first inning compare to the second inning?")

inning_performance = get_inning_performance()
inning_performance_df = pd.DataFrame(inning_performance)
st.bar_chart(inning_performance_df.set_index("_id")[
    ["first_inning_runs", "second_inning_runs"]])

# -------------------------------------------

st.divider()
st.subheader("What is each team's win/loss ratio?")
col1, col2 = st.columns(2)

win_loss_totals = get_win_loss_totals()
win_loss_totals_df = pd.DataFrame(win_loss_totals)
st.dataframe(win_loss_totals_df)
st.bar_chart(win_loss_totals_df.set_index(
    "_id")[["total_wins", "total_losses"]])

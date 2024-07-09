import streamlit as st
from pymongo import MongoClient

db = MongoClient().cricketDatabase


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
    return list(db.matches.aggregate(pipeline))


@st.cache_data
def get_top_teams():
    pipeline = [
        {
            "$group": {
                "_id": {"season": "$season", "winner": "$winner"},
                "wins": {"$sum": 1}
            }
        },
        {
            "$sort": {"_id.season": 1, "wins": -1}
        },
        {
            "$group": {
                "_id": "$_id.season",
                "topTeam": {"$first": "$_id.winner"},
                "wins": {"$first": "$wins"}
            }
        }
    ]
    return list(db.matches.aggregate(pipeline))


@st.cache_data
def get_highest_scoring_match():
    pipeline = [
        {
            "$group": {
                "_id": "$match_id",
                "total_runs": {"$sum": "$total_runs"}
            }
        },
        {
            "$sort": {"total_runs": -1}
        },
        {
            "$limit": 1
        },
        {
            "$lookup": {
                "from": "matches",
                "localField": "_id",
                "foreignField": "id",
                "as": "match_info"
            }
        },
        {
            "$unwind": "$match_info"
        },
        {
            "$project": {
                "match_id": "$_id",
                "total_runs": 1,
                "team1": "$match_info.team1",
                "team2": "$match_info.team2",
                "date": "$match_info.date",
                "city": "$match_info.city",
                "venue": "$match_info.venue"
            }
        }
    ]
    return list(db.deliveries.aggregate(pipeline))


@st.cache_data
def get_most_wickets_match():
    pipeline = [
        {
            "$match": {
                "dismissal_kind": {"$ne": "run out"}
            }
        },
        {
            "$group": {
                "_id": "$match_id",
                "wickets": {"$sum": 1}
            }
        },
        {
            "$sort": {"wickets": -1}
        },
        {
            "$limit": 1
        },
        {
            "$lookup": {
                "from": "matches",
                "localField": "_id",
                "foreignField": "id",
                "as": "match_info"
            }
        },
        {
            "$unwind": "$match_info"
        },
        {
            "$project": {
                "match_id": "$_id",
                "wickets": 1,
                "team1": "$match_info.team1",
                "team2": "$match_info.team2",
                "date": "$match_info.date",
                "city": "$match_info.city",
                "venue": "$match_info.venue"
            }
        }
    ]
    return list(db.deliveries.aggregate(pipeline))


@st.cache_data
def get_top_players_of_match():
    pipeline = [
        {
            "$group": {
                "_id": {"season": "$season", "player_of_match": "$player_of_match"},
                "awards": {"$sum": 1}
            }
        },
        {
            "$sort": {"_id.season": 1, "awards": -1}
        },
        {
            "$group": {
                "_id": "$_id.season",
                "topPlayer": {"$first": "$_id.player_of_match"},
                "awards": {"$first": "$awards"}
            }
        }
    ]
    return list(db.matches.aggregate(pipeline))


@st.cache_data
def get_average_team_score_evolution():
    pipeline = [
        {
            "$lookup": {
                "from": "matches",
                "localField": "match_id",
                "foreignField": "id",
                "as": "match_info"
            }
        },
        {
            "$unwind": "$match_info"
        },
        {
            "$project": {
                "season": "$match_info.season",
                "team": "$batting_team",
                "total_runs": "$total_runs"
            }
        },
        {
            "$group": {
                "_id": {"season": "$season", "team": "$team"},
                "total_runs": {"$sum": "$total_runs"},
                "matches_played": {"$sum": 1}
            }
        },
        {
            "$group": {
                "_id": {"season": "$_id.season", "team": "$_id.team"},
                "average_team_score": {"$avg": "$total_runs"}
            }
        },
        {
            "$sort": {"_id.season": 1, "_id.team": 1}
        }
    ]

    return list(db.deliveries.aggregate(pipeline))


@st.cache_data
def get_inning_performance():
    pipeline = [
        {
            "$group": {
                "_id": {"inning": "$inning", "batting_team": "$batting_team"},
                "total_runs": {"$sum": "$total_runs"}
            }
        },
        {
            "$group": {
                "_id": "$_id.batting_team",
                "first_inning_runs": {"$sum": {"$cond": [{"$eq": ["$_id.inning", 1]}, "$total_runs", 0]}},
                "second_inning_runs": {"$sum": {"$cond": [{"$eq": ["$_id.inning", 2]}, "$total_runs", 0]}}
            }
        }
    ]
    return list(db.deliveries.aggregate(pipeline))


@st.cache_data
def get_win_loss_totals():
    pipeline = [
        {
            "$group": {
                "_id": "$city",
                "total_matches": {"$sum": 1},
                "total_wins": {"$sum": {"$cond": [{"$eq": ["$winner", "$team1"]}, 1, 0]}},
                "total_losses": {"$sum": {"$cond": [{"$eq": ["$winner", "$team2"]}, 1, 0]}}
            }
        }
    ]
    return list(db.matches.aggregate(pipeline))

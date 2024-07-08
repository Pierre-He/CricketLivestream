import pandas as pd
import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, min, max
from pymongo import MongoClient

# Create a Spark session
spark = SparkSession.builder.appName(
    "Cricket Data Analysis and MongoDBBridge").getOrCreate()

# Load CSV files into Spark DataFrames
deliveries_df = spark.read.csv(
    'deliveries.csv', header=True, inferSchema=True, nullValue='nan')
matches_df = spark.read.csv(
    'matches.csv', header=True, inferSchema=True, nullValue='nan')

# Replace nulls with 'Unknown' and handle venue-city logic
matches_df = matches_df.withColumn('umpire1', when(
    col('umpire1').isNull(), lit("Unknown")).otherwise(col('umpire1')))
matches_df = matches_df.withColumn('umpire2', when(
    col('umpire2').isNull(), lit("Unknown")).otherwise(col('umpire2')))
matches_df = matches_df.withColumn('winner', when(
    col('winner').isNull(), lit("Unknown")).otherwise(col('winner')))
matches_df = matches_df.withColumn('player_of_match', when(col(
    'player_of_match').isNull(), lit("Unknown")).otherwise(col('player_of_match')))
matches_df = matches_df.withColumn('city', when(
    matches_df['venue'] == 'Dubai International Cricket Stadium', 'Dubai').otherwise(matches_df['city']))
matches_df = matches_df.drop('umpire3')
deliveries_df = deliveries_df.drop('player_dismissed').drop(
    'dismissal_kind').drop('fielder')

# Function to count NULL values in each column of a DataFrame


def count_nulls(df):
    null_counts = {}
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        null_counts[column] = null_count
    return null_counts


# Count NULL values for deliveries DataFrame
deliveries_null_counts = count_nulls(deliveries_df)

# Count NULL values for matches DataFrame
matches_null_counts = count_nulls(matches_df)

# Print NULL values count for deliveries.csv
print("Results for 'deliveries.csv':")
for column, count in deliveries_null_counts.items():
    print(f"Column '{column}': {count} NULL")

# Print NULL values count for matches.csv
print("\nResults for 'matches.csv':")
for column, count in matches_null_counts.items():
    print(f"Column '{column}': {count} NULL")

# Convert date columns to string


def convert_date_to_string(df):
    for column in df.columns:
        if "date" in column.lower():
            df = df.withColumn(column, col(column).cast("string"))
    return df


matches_df = convert_date_to_string(matches_df)
deliveries_df = convert_date_to_string(deliveries_df)

# Convert Spark DataFrames to Pandas DataFrames
deliveries_pd = deliveries_df.toPandas()
matches_pd = matches_df.toPandas()

# Convert Pandas DataFrames to list of dictionaries
deliveries_dict = deliveries_pd.to_dict("records")
matches_dict = matches_pd.to_dict("records")

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client.cricketDatabase

# Insert data into MongoDB collections
db.deliveries.insert_many(deliveries_dict)
db.matches.insert_many(matches_dict)

print("Deliveries Collection Sample:")
print(db.deliveries.find_one())
print("\nMatches Collection Sample:")
print(db.matches.find_one())

# Add indexes to MongoDB collections
db.deliveries.create_index([("match_id"), ("batsman")])
db.matches.create_index([("id"), ("season")])

# Filter and display venues where city is NULL
venues_with_null_city = matches_df.filter(
    matches_df['city'].isNull()).select('venue')
print("Venues when city is null:")
venues_with_null_city.show(truncate=False)

# Columns to check for min and max in matches
matches_columns_to_check = [
    "season", "dl_applied", "win_by_runs", "win_by_wickets"]
matches_min = matches_df.select(
    [min(col).alias(f"min_{col}") for col in matches_columns_to_check])
matches_max = matches_df.select(
    [max(col).alias(f"max_{col}") for col in matches_columns_to_check])

# Show min and max results for matches
matches_min.show(truncate=False)
matches_max.show(truncate=False)

# Columns to check for min and max in deliveries
deliveries_columns_to_check = ["match_id", "inning", "over", "ball", "is_super_over", "wide_runs", "bye_runs",
                               "legbye_runs", "noball_runs", "penalty_runs", "batsman_runs", "extra_runs", "total_runs"]
deliveries_min = deliveries_df.select(
    [min(col).alias(f"min_{col}") for col in deliveries_columns_to_check])
deliveries_max = deliveries_df.select(
    [max(col).alias(f"max_{col}") for col in deliveries_columns_to_check])

# Show min and max results for deliveries
deliveries_min.show(truncate=False)
deliveries_max.show(truncate=False)

# Stop the Spark session
spark.stop()

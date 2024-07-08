import pandas as pd
import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, min, max

# Load CSV files into pandas DataFrames
deliveries = pd.read_csv('consumed_deliveries.csv')
matches = pd.read_csv('consumed_matches.csv')

# Save DataFrames to SQLite database
connect_db = sqlite3.connect('cricket_data.db')
deliveries.to_sql('deliveries', connect_db, if_exists='replace', index=False)
matches.to_sql('matches', connect_db, if_exists='replace', index=False)
connect_db.commit()

# Create a Spark session
spark = SparkSession.builder.appName("Cricket Data Analysis").getOrCreate()

# Load CSV files into Spark DataFrames
deliveries_df = spark.read.csv('deliveries.csv', header=True, inferSchema=True, nullValue='nan')
matches_df = spark.read.csv('matches.csv', header=True, inferSchema=True, nullValue='nan')

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

# Drop columns with too many NULL values
matches_df = matches_df.drop('umpire3')
deliveries_df = deliveries_df.drop('player_dismissal').drop('dismissal_kind').drop('fielder')

# Print updated schema for verification
print("Schema of deliveries_df after dropping columns:")
deliveries_df.printSchema()

print("\nSchema of matches_df after dropping 'umpire3':")
matches_df.printSchema()

# Filter and display venues where city is NULL
venues_with_null_city = matches_df.filter(matches_df['city'].isNull()).select('venue')
print("Venues when city is null:")
venues_with_null_city.show(truncate=False)

# Update city where venue is 'Dubai International Cricket Stadium'
matches_df = matches_df.withColumn('city', when(matches_df['venue'] == 'Dubai International Cricket Stadium', 'Dubai').otherwise(matches_df['city']))

# Columns to check for min and max in matches
matches_columns_to_check = ["season", "dl_applied", "win_by_runs", "win_by_wickets"]
matches_min = matches_df.select([min(col).alias(f"min_{col}") for col in matches_columns_to_check])
matches_max = matches_df.select([max(col).alias(f"max_{col}") for col in matches_columns_to_check])

# Show min and max results for matches
matches_min.show(truncate=False)
matches_max.show(truncate=False)

# Columns to check for min and max in deliveries
deliveries_columns_to_check = ["match_id", "inning", "over", "ball", "is_super_over", "wide_runs", "bye_runs",
                               "legbye_runs", "noball_runs", "penalty_runs", "batsman_runs", "extra_runs", "total_runs"]
deliveries_min = deliveries_df.select([min(col).alias(f"min_{col}") for col in deliveries_columns_to_check])
deliveries_max = deliveries_df.select([max(col).alias(f"max_{col}") for col in deliveries_columns_to_check])

# Show min and max results for deliveries
deliveries_min.show(truncate=False)
deliveries_max.show(truncate=False)

# Stop the Spark session
spark.stop()

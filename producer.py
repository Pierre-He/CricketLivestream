import pandas as pd
from kafka import KafkaProducer
import json
import time

# Load CSV files
matches_df = pd.read_csv('./matches.csv')
deliveries_df = pd.read_csv('./deliveries.csv')

matches_records = matches_df.to_dict(orient='records')
deliveries_records = deliveries_df.to_dict(orient='records')

# Setup Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Function to simulate data collection and send to Kafka
def send_data_to_kafka(topic, records):
    for record in records:
        producer.send(topic, record)

# Sending matches data to Kafka topic 'matches'
send_data_to_kafka('matches', matches_records)

# Sending deliveries data to Kafka topic 'deliveries'
send_data_to_kafka('deliveries', deliveries_records)

producer.flush()

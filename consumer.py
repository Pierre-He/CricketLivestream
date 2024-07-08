from kafka import KafkaConsumer
import csv
import json
import pandas as pd

# Load CSV files to get the headers
matches_df = pd.read_csv('./matches.csv')
deliveries_df = pd.read_csv('./deliveries.csv')

# Setup Kafka consumer
consumer = KafkaConsumer(
    'matches',
    'deliveries',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Function to consume data and write to CSV
def consume_data_and_write_to_csv():
    with open('./consumed_matches.csv', mode='w', newline='') as matches_file, \
         open('./consumed_deliveries.csv', mode='w', newline='') as deliveries_file:

        matches_writer = csv.writer(matches_file)
        deliveries_writer = csv.writer(deliveries_file)

        matches_writer.writerow(matches_df.columns)  # Writing header for matches CSV
        deliveries_writer.writerow(deliveries_df.columns)  # Writing header for deliveries CSV

        try:
            for message in consumer:
                if message.topic == 'matches':
                    matches_writer.writerow([message.value.get(col) for col in matches_df.columns])
                elif message.topic == 'deliveries':
                    deliveries_writer.writerow([message.value.get(col) for col in deliveries_df.columns])
        except KeyboardInterrupt:
            # Gracefully handle script interruption
            print("Data consumption has been interrupted.")

    print("All messages have been processed and written to CSV files.")

consume_data_and_write_to_csv()

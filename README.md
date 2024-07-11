<h1 align="center"> Cricket Livestream </h1>

<p align="center">
  This project simulates live-streaming of cricket matches using Kafka, cleans the data using Spark, stores it into MongoDB, and displays in on a Streamlit dashboard.

![image](https://github.com/Pierre-He/CricketLivestream/assets/71611172/3c56d04b-8343-44ed-9e28-353e981b6384)


</p>

## Technologies used

- **Front-end**: Streamlit (Python)
- **Data streaming**: Kafka
- **Database**: MongoDB
- **Data cleaning**: Spark
- **Python libraries**: PyMongo, Pandas, Matplotlib

## Prerequisites

- Kafka
- MongoDB
- Python (up to 3.11.4)
- pip (python)

## Getting Started

1. Start Kafka

```sh
cd Path\to\kafka\directory

# Start zookeeper
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

# Start Kafka server
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

2. Create Kafka topics

```sh
# matches topic creation
.\bin\windows\kafka-topics.bat --create --topic matches --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

# deliveries topic creation
.\bin\windows\kafka-topics.bat --create --topic deliveries --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
```

3. Start MongoDB server

```sh
# go to mongodb server directory
cd Path\to\kafka\directory\Server\7.0\bin

# start mongodb server
mongod --dbpath "Path\to\store\mongodb\database"

```

4. Clone this repository

```sh
git clone https://github.com/Pierre-He/CricketLivestream.git
cd CricketLivestream
```

5. Create a virtual environment

\*_Make sure to have Python and `pip` installed on your machine._

```sh
python -m venv .venv
```

6. Start venv and install dependencies

```sh
# activate virtual environment
.venv\Scripts\activate # Windows
source .venv/Scripts/activate # Linux/macOS

# install required dependencies
pip install -r requirements.txt
```

7. Run the pipeline

```sh
# run producer
python .\producer.py

# run consumer (can ctrl + c after a few second to stop the consumer from listening)
python .\consumer.py

# run spark cleaning script
python .\spark.py

# run streamlit
streamlit run .\cricket-app.py

```

## Members

- Asma ABIDI
- Laurent HAN
- Pierre HE
- Alexandra WONYU

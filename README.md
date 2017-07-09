# Spark Movie Analyzer

## Setup

You will need to have the following dependencies:
* Python3
* Kafka
* Spark Notebook version 0.7.0
* Scikit-learn for python
* Kafka-python for python
* Scala 2.11

## Setup

The project contains several files and folder:
* **start-kafka.sh**: bash script launching kafka and zookeeper on the local host machine, on port.
* **python-processing/fetcher.py**: python script writing data to kafka from themoviedb api or from a single file.
* **python-processing/analysis.py**: analyzes json from themoviedb API taking its input from stdin and writing to stdout the result.
* **spark-consumer**: fetches data from the topic written by the fetcher.py and script, and calls *analysis.py* on each Spark Node.
* **hdfs-writer**: gets processed movie from *spark-consumer* and writes output to a given folder. This process is used to make the data persistent.

## Run

### Server

Run the custom Kafka script on the machine hosting Kafka and
Zookeeper:
```sh
$ ./start-kafka.sh -i <path_to_kafka_folder>
```
This script will launch the kafka instance, the zookeeper instance, and create the two following topics (if they do not exist):
* **movie-topic**: contains raw movie from themoviedb API.
* **movie-analyzed**: contains the result of the movie on which sentiment analysis has been applied.

Start the analyzer process:
```sh
cd spark-consumer
sbt "run -b localhost:9092 -gid test -c movie-topic -p movie-analyzed"
```

Start the fetcher fetchning permanently from the API:
```sh
cd python-processing
./fetcher.py
```

Start the fetcher from a file:
```sh
cd python-processing
./fetcher.py -i <json_database>
```

### Notebook

You can launch the notebook on another machine or on the server.
In order to do so, you have to go to the notebook directory and execute:

```sh
./bin/spark-notebook
```

Do not forget to modify the **broker** value with either **localhost** if the notebook is on the server, or with the server ip adress.

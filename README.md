# Spark Movie Analyzer

## Setup

You will need to have the following dependencies:
* Kafka
* Docker

### Installation

Download the spark-notebook image:
```sh
$ sudo docker pull andypetrella/spark-notebook:0.7.0-scala-2.11.8-spark-2.1.1-hadoop-2.7.2
```

Create the docker instance:
```sh
sudo docker run --name="docker-notebook" -p 9001:9001  andypetrella/spark-notebook:0.7.0-scala-2.11.8-spark-2.1.1-hadoop-2.7.2
```

## Run

### Start the spark-notebook

In order to start the notebook:
```sh
sudo docker start docker-notebook
```

In order to stop the notebook:
```sh
sudo docker stop docker-notebook
```

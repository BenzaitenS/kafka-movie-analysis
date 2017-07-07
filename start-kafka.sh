#!/usr/bin/env bash

check_arg()
{
    if [ -z "$2" ]; then
        echo "Error: argument '$1' requires a value"
    fi
}

#########################
#   ARGUMENTS PARSING   #
#########################
KAFKA_FOLDER_ARG=""
BROKERS_LIST_ARG=""
FETCH_FROM_API_ARG="false"
LEARNING_DB_FILE_ARG=""

while [[ $# -ne 0 ]] ; do
    key="$1"
    case $key in
        -i|--input)
            KAFKA_FOLDER_ARG="$2"
            check_arg $key $KAFKA_FOLDER_ARG
            shift
        ;;
        -b|--broker)
            BROKERS_LIST_ARG="$2"
            check_arg $key $BROKERS_LIST_ARG
            shift
        ;;
        --use-file-for-fetch)
            FETCH_FROM_API_ARG="$2"
            check_arg $key $FETCH_FROM_API_ARG
            shift
        ;;
        -db|--use-learning-db)
            LEARNING_DB_FILE_ARG="$2"
            check_arg $key $LEARNING_DB_FILE_ARG
            shift
        ;;
        *)
        ;;
    esac
    shift
done

if [ -z "$KAFKA_FOLDER_ARG" ]; then
    echo "Error: you have to provide your kafka folder using --input/-i"
    exit 1
fi

#########################
#       KEEPS PIDs      #
#########################

# Zookeeper / Kafka
zoo_server_pid=""
kafka_server_pid=""

# Python processing
python_analyzer_pid=""
python_fetcher_pid=""


#########################
#       RUNS KAFKA      #
#########################
TOPIC_RAW_MOVIE="movie-topic"
TOPIC_MOVIE_ANALYZED="movie-analyzed"
TOPIC_PROCESSED_MOVIE="movie-processed"

KAFKA_BIN="$KAFKA_FOLDER_ARG"/bin
KAFKA_CONFIG="$KAFKA_FOLDER_ARG"/config

KAFKA_STOP_SCRIPT="$KAFKA_BIN"/kafka-server-stop.sh
ZOO_STOP_SCRIPT="$KAFKA_BIN"/zookeeper-server-stop.sh

# Starts Zookeeper service, enabling synchronization accross clusters
"$KAFKA_BIN"/zookeeper-server-start.sh "$KAFKA_CONFIG"/zookeeper.properties &
zoo_server_pid="$!"
# Starts kafka service
"$KAFKA_BIN"/kafka-server-start.sh "$KAFKA_CONFIG"/server.properties &
kafka_server_pid="$!"

# Creates the movie topic, where raw movie data
# are written from the Python producer script.
"$KAFKA_BIN"/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic "$TOPIC_RAW_MOVIE" &
# Creates the movie analyzed topc, where movie with
# the associated sentiment analysis are written from the Python analyzer script.
"$KAFKA_BIN"/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic "$TOPIC_MOVIE_ANALYZED" &
# Creates the procesed movie topic, where processed movie data
# are written from the Spark producer.
"$KAFKA_BIN"/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic "$TOPIC_PROCESSED_MOVIE" &

finish() {
    # Kills kafka and zookeper
    sh $KAFKA_STOP_SCRIPT
    sleep 5
    sh $ZOO_STOP_SCRIPT

    # Kills python scripts
    kill "$python_fetcher_pid"

    # Stops the notebook
    sudo docker stop docker-notebook
}

trap finish SIGINT SIGTERM EXIT

# ###########################
# #       RUNS FETCHER      #
# ###########################
# if [ -z "$FETCH_FROM_API_ARG" ]; then
#     python3 ./python-processing/fetcher.py
# else
#     python3 ./python-processing/fetcher.py -i "$FETCH_FROM_API_ARG"
# fi
# python_fetcher_pid="$!"

# ###########################
# #      RUNS NOTEBOOK      #
# ###########################
# sudo docker start docker-notebook

wait "$kafka_server_pid"

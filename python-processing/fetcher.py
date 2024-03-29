#!/usr/bin/env python3

import signal

import argparse
import json

from kafka import SimpleProducer, KafkaClient

from utils.kafka_producer import KafkaProducerWrapper
from fetcher_lib.crawler import imdb_crawler

def parse_args():
    description =   """
                    Fetches data from a file or from an API and stream the
                    Json response to kafka
                    """

    arg_parser = argparse.ArgumentParser(description=description)

    # Register arguments
    arg_parser.add_argument('-i', '--input',
                            help='Input file used to stream content in kafka',
                            required=False)
    arg_parser.add_argument('-o', '--output',
                            help='Write crawling ouput to the given file',
                            required=False)
    arg_parser.add_argument('-b', '--broker',
                            help='Broker on which the connection is established.',
                            required=False)
    arg_parser.add_argument('-p', '--producer',
                            help='Id of the Kafka topic to produce to. \'movie-topic\' by default.',
                            required=False)
    arg_parser.add_argument('-n', '--number',
                            help='Sets the max number of film to write when -i is used.',
                            required=False)

    return arg_parser.parse_args()

def stream_file(args, kafka_handler, topic):
    file_path = args.input
    nb = 0
    with open(file_path, "r") as f:
        for line in f:
            data = json.loads(line)
            kafka_handler.produce(json.dumps(data), topic)
            if not (args.number is None) and nb >= int(args.number):
                break
        kafka_handler.flush()

def stream_from_api(kafka_handler, topic):
    THEMOVIEDB_API_TOKEN = "2cde1ceaa291c9271e32272dc26200fe"

    MOVIES_ROUTE_API = "https://api.themoviedb.org/3/discover/movie"
    REVIEW_ROUTE_API = "https://api.themoviedb.org/3/movie/{}/reviews"
    routes = {
        "movies_list": MOVIES_ROUTE_API,
        "review": REVIEW_ROUTE_API
    }

    # Defines callback triggered each time a movie is
    # fetched from the api
    def callback(movies):
        for movie in movies:
            string = json.dumps(movie)
            print("[KAFKA] Movie `{}` pushed to topic \'{}\'".format(movie["title"], topic))
            kafka_handler.produce(string, topic)
        kafka_handler.flush()

    MIN_YEAR = 2015
    MAX_YEAR = 2017

    crawler = imdb_crawler.IMDBCrawler(routes, THEMOVIEDB_API_TOKEN, args.output)
    crawler.setMinMaxYear(MIN_YEAR, MAX_YEAR)

    crawler.start()
    crawler.crawl_sync(callback)
    crawler.stop()

if __name__ == "__main__":
    args = parse_args()

    if args.broker is None:
        args.broker = 'localhost:9092'
    if args.producer is None:
        args.producer = 'movie-topic'

    kafka_handler = KafkaProducerWrapper(args.broker)

    # Catch SIGINT to stop the script properly
    def exit_script(signal, frame):
        print('[FETCHER] Stopping the fetch...')
        kafka_handler.flush()
        exit(0)
    signal.signal(signal.SIGINT, exit_script)

    if not(args.input is None):
        stream_file(args, kafka_handler, args.producer)
    else:
        stream_from_api(kafka_handler, args.producer)

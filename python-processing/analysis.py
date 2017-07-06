#!/usr/bin/env python3

import os
import signal
import argparse
import json

from sentiment_analysis.linear_svm import LinearSVM
from utils.kafka_consumer import KafkaConsummerWrapper
from utils.kafka_producer import KafkaProducerWrapper

def parse_args():
    description =   """
                    Consummes data from a given kafka topic and makes
                    sentiment analysis on its reviews.
                    """
    
    arg_parser = argparse.ArgumentParser(description=description)
    
    # Register arguments
    arg_parser.add_argument('-b', '--broker',
                            help='Broker on which the connection is established.',
                            required=False)
    arg_parser.add_argument('-c', '--consummer',
                            help='Id of the Kafka topic to consumme from. \'movie-topic\' by default.',
                            required=False)
    arg_parser.add_argument('-p', '--producer',
                            help='Id of the Kafka topic to produce to. \'movie-analyzed\' by default.',
                            required=False)
    arg_parser.add_argument('-pf', '--positive',
                            help='Path to the training positive folder.',
                            required=False)
    arg_parser.add_argument('-nf', '--negative',
                            help='Path to the training negative folder.',
                            required=False)
    arg_parser.add_argument('-s', '--save_db',
                            help='Saves the learning database after training.',
                            required=False)
    arg_parser.add_argument('-l', '--load_db',
                            help='Load the learning database previously trained.',
                            required=False)

    return arg_parser.parse_args()

if __name__ == "__main__":

    # Catch SIGINT to stop the script properly
    def exit_script(signal, frame):
        print('[ANALYSER] Stopping the fetch...')
        exit(0)

    signal.signal(signal.SIGINT, exit_script)

    DATA_ROOT_FOLDER = "data/reviews/train/data/"
    #DATA_ROOT_FOLDER = "data/txt_sentoken/"
    POS_FOLDER = DATA_ROOT_FOLDER + "pos"
    NEG_FOLDER = DATA_ROOT_FOLDER + "neg"

    args = parse_args()
    if args.consummer is None:
        args.consummer = "movie-topic"
    if args.producer is None:
        args.producer = "movie-analyzed"
    if args.positive is None:
        args.positive = POS_FOLDER
    if args.negative is None:
        args.negative = NEG_FOLDER
    if args.broker is None:
        args.broker = "localhost:9092"

    # Prints message describing script inputs
    print('[ANALYSER] Starting script with inputs:')
    print('[ANALYSER]\t- broker: {}'.format(args.broker))
    print('[ANALYSER]\t- consumer topic: {}'.format(args.consummer))
    print('[ANALYSER]\t- producer topic: {}'.format(args.producer))

    # Setups Kafka consumer and producer
    consumer = KafkaConsummerWrapper(args.consummer, args.broker)
    producer = KafkaProducerWrapper(args.broker)

    # The user did not provide a previsouly trained model,
    # the script needs to train from any possible data folder.
    svm = None
    if not(args.load_db):
        svm = LinearSVM()
        svm.train(args.positive, args.negative)

        if args.save_db:
            svm.dump(args.save_db)
    else:
        svm = LinearSVM.load(args.load_db)

    # Consumer callback
    def callback(message):
        try:
            movie = json.loads(message.value.decode("utf-8"))
            movie["review_analysis"] = []

            # Adds a new list to the movie, composed of
            # 1 for positive review, and 0 for negative ones.
            for r in movie["reviews"]:
                content = r["content"]
                val = 1 if svm.project([content]) == "pos" else 0
                movie["review_analysis"].append(val)


            # Writes processed movie with sentiment analyses to
            # output topic.
            string_movie = json.dumps(movie)
            producer.produce(string_movie, args.producer)
            producer.flush()

            print(movie)
        except:
            pass

    # Consumes from the given entry topic,
    # and produce to the given output topic.
    consumer.consume(callback)

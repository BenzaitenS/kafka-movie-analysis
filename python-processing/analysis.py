#!/usr/bin/env python3

import pickle

import sys
import os
import signal
import argparse
import json

from sklearn.svm import LinearSVC
from sklearn.feature_extraction.text import TfidfVectorizer

class ProcessingContainer:
    def __init__(self, vectorizer, svm):
        self._vectorizer = vectorizer
        self._svm = svm

class LinearSVM:
    def train(self, positive_folder_path, negative_folder_path):
        vectorizer = TfidfVectorizer(min_df=3, max_df=0.95, sublinear_tf=True, use_idf=True)
        svm = LinearSVC(C=1000)
        self._processing_container = ProcessingContainer(vectorizer, svm)

        train_data = []
        train_labels = []
        # Loads pos folder
        for root, sub, files in os.walk(positive_folder_path):
            for file_name in files:
                url = os.path.join(root, file_name)
                content = open(url, 'r', encoding='utf-8', errors='ignore').read()
                train_data.append(content)
                train_labels.append('pos')

        len_positive = len(train_data)
        if len_positive == 0:
            #print('[SVM] The positive folder has not been found or is empty.')
            exit(1)

        # Loads pos folder
        for root, sub, files in os.walk(negative_folder_path):
            for file_name in files:
                url = os.path.join(root, file_name)
                content = open(url, 'r', encoding='utf-8', errors='ignore').read()
                train_data.append(content)
                train_labels.append('neg')

        if len_positive == len(train_data):
            #print('[SVM] The negative folder has not been found or is empty.')
            exit(1)

        # Computes frequencies
        train_vectors = self._processing_container._vectorizer.fit_transform(train_data)
        
        # Trains the SVM
        self._processing_container._svm.fit(train_vectors, train_labels)

        #print('[SVM] Training completed!')

    def project(self, string):
        test_vect = self._processing_container._vectorizer.transform(string)
        return self._processing_container._svm.predict(test_vect)

    def dump(self, path):
        with open(path, 'wb') as output:
            try:
                pickle.dump(self._processing_container, output)
            except:
                exit(1)


    def set_linear_svm(self, linear_svm):
        self._svm = linear_svm

    @staticmethod
    def load(path):
        #print('[SVM] Starting to load trained data...')
        #try:
        val = sys.argv[0].rsplit('/', 1)[0] + '/' + path
        with open(val, 'rb') as input:
            dump = pickle.load(input)
            res = LinearSVM()
            res._processing_container = dump
            return res
        #except:
         #   print('[SVM]: The dump \'{}\' could not be loaded.'.format(path))
         #   exit(1)


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
    arg_parser.add_argument('-noin', '--no_stdin',
                            help='Computes a movie piped from kafka.',
                            required=False)

    return arg_parser.parse_args()

def process_from_pipe():
    # The user did not provide a previsouly trained model,
    # the script needs to train from any possible data folder.
    svm = LinearSVM.load("learning_database.pkl")

    for message in sys.stdin:
        try:
            movie = json.loads(message)
            #movie["review_analysis"] = []
            # Adds a new list to the movie, composed of
            # 1 for positive review, and 0 for negative ones.
            for r in movie["reviews"]:
                content = r["content"]
                val = 1 if svm.project([content]) == "pos" else 0
                r["analysis"] = val                
            # Writes processed movie with sentiment analyses to
            # output topic.
            string_movie = json.dumps(movie)
            print(string_movie)
        except:
            pass

if __name__ == "__main__":
    args = parse_args()

    if args.no_stdin is None:
        process_from_pipe()
        exit(0)

    # Catch SIGINT to stop the script properly
    # def exit_script(signal, frame):
    #     print('[ANALYSER] Stopping the fetch...')
    #     exit(0)

    # signal.signal(signal.SIGINT, exit_script)

    DATA_ROOT_FOLDER = "data/reviews/train/data/"
    POS_FOLDER = DATA_ROOT_FOLDER + "pos"
    NEG_FOLDER = DATA_ROOT_FOLDER + "neg"

    svm = LinearSVM()
    svm.train(POS_FOLDER, NEG_FOLDER)
    svm.dump("learning_database.pkl")

    # if args.consummer is None:
    #     args.consummer = "movie-topic"
    # if args.producer is None:
    #     args.producer = "movie-analyzed"
    # if args.positive is None:
    #     args.positive = POS_FOLDER
    # if args.negative is None:
    #     args.negative = NEG_FOLDER
    # if args.broker is None:
    #     args.broker = "localhost:9092"

    # # Prints message describing script inputs
    # print('[ANALYSER] Starting script with inputs:')
    # print('[ANALYSER]\t- broker: {}'.format(args.broker))
    # print('[ANALYSER]\t- consumer topic: {}'.format(args.consummer))
    # print('[ANALYSER]\t- producer topic: {}'.format(args.producer))

    # Setups Kafka consumer and producer
    #consumer = KafkaConsummerWrapper(args.consummer, args.broker)
    #producer = KafkaProducerWrapper(args.broker)

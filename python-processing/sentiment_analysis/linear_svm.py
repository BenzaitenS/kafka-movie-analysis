import os

import pickle

from sklearn.svm import LinearSVC
from sklearn.feature_extraction.text import TfidfVectorizer

class ProcessingContainer:
    def __init__(self, vectorizer, svm):
        self._vectorizer = vectorizer
        self._svm = svm

class LinearSVM:
    def train(self, positive_folder_path, negative_folder_path):
        print('[SVM] Starting training on \'{}\' and \'{}\'...'
                .format(positive_folder_path, negative_folder_path))

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
            print('[SVM] The positive folder has not been found or is empty.')
            exit(1)

        # Loads pos folder
        for root, sub, files in os.walk(negative_folder_path):
            for file_name in files:
                url = os.path.join(root, file_name)
                content = open(url, 'r', encoding='utf-8', errors='ignore').read()
                train_data.append(content)
                train_labels.append('neg')

        if len_positive == len(train_data):
            print('[SVM] The negative folder has not been found or is empty.')
            exit(1)

        # Computes frequencies
        train_vectors = self._processing_container._vectorizer.fit_transform(train_data)
        
        # Trains the SVM
        self._processing_container._svm.fit(train_vectors, train_labels)

        print('[SVM] Training completed!')

    def project(self, string):
        test_vect = self._processing_container._vectorizer.transform(string)
        return self._processing_container._svm.predict(test_vect)

    def dump(self, path):
        print('[SVM] Starting to dump trained data...')
        with open(path, 'wb') as output:
            try:
                pickle.dump(self._processing_container, output)
            except:
                print('[SVM]: The dump could not be save to \'{}\'.'.format(path))
                exit(1)

        print('[SVM] Trained data successfully dumped.')

    def set_linear_svm(self, linear_svm):
        self._svm = linear_svm

    @staticmethod
    def load(path):
        print('[SVM] Starting to load trained data...')
        try:
            with open(path, 'rb') as input:
                dump = pickle.load(input)
                res = LinearSVM()
                res._processing_container = dump

                print('[SVM] Trained data successfully loaded.')
                return res
        except:
            print('[SVM]: The dump \'{}\' could not be loaded.'.format(path))
            exit(1)

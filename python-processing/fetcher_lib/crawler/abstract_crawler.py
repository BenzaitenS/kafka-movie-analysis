from abc import ABCMeta, abstractmethod
import requests

class AbstractCrawler:
    __metaclass__ = ABCMeta

    def __init__(self, routes, output_file_path = None):
        self._routes = routes
        # Opens the output file directly and keep it open
        # to avoid another process to use it and to reduce
        # I/O.
        self._file_path = output_file_path
        self._file = None

    def start(self):
        if self._file is None and not(self._file_path is None):
            self._file = open(self._file_path, 'a')

    def stop(self):
        if not(self._file is None):
            self._file.close()
            self._file = None

    @abstractmethod
    def crawl_sync(self, callback):
        raise NotImplementedError()

    def request(self, url, params):
        print('Requesting \'{}\'...'.format(url))
        return requests.get(url=url, params=params)

    def write(self, string):
        if not(self._file is None):
            self._file.write("{}\n".format(string))
            self._file.flush()

import os
import pickle


class ElasticsearchMock:
    def __init__(self, url):
        self._url = url

    def get_url(self):
        return self._url


class SearchMock:
    def __init__(self, using: ElasticsearchMock, index: str):
        self._es = using
        self._idx = index

    def scan(self):
        for file in self.get_file_list():
            yield self.read_dump_from_file(file)

    def count(self):
        return len(os.listdir(self._es.get_url()))

    def get_file_list(self):
        return [os.path.join(self._es.get_url(), file) for file in os.listdir(self._es.get_url())]

    @staticmethod
    def read_dump_from_file(file_path: str):
        with open(file_path, "rb") as file:
            return pickle.load(file)


class EmptySearchMock:
    def __init__(self):
        self._data = list()

    def scan(self):
        for item in self._data:
            yield item


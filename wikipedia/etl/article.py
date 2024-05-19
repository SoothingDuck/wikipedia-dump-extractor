import glob

from wikipedia.db import logger
from wikipedia.db import session
from wikipedia.db import engine

from wikipedia.db.model import Article
from sqlalchemy import Index

from wikipedia.etl.dump import Dump
from wikipedia.etl.extract import DumpFileExtractor


from abc import ABC


class ArticleExtractor(ABC):
    def __init__(self, dump):
        self._dump = dump

    def extract(self):
        raise Exception("Not Implemented")


class ArticleLoader(ABC):
    def __init__(self, dump):
        self._dump = dump

    def load(self):
        raise Exception("Not Implemented")


if __name__ == "__main__":
    import os

    print("test")

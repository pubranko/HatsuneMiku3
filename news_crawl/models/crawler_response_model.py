from pymongo import MongoClient
from pymongo.database import Database
import os
from news_crawl.models.mongo_model import MongoModel

#from typing import Final

#crawler_response
class CrawlerResponseModel(object):
    '''
    crawler_responseコレクション用モデル
    '''

    crawler_db:Database
    mongo:MongoModel

    def __init__(self,mongo:MongoModel):
        self.mongo = mongo

    def insert_one(self, item):
        self.mongo.crawler_db['crawler_response'].insert_one(item)

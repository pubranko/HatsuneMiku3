from pymongo import MongoClient
from pymongo.database import Database
import os
from news_crawl.models.mongo_model import MongoModel


class CrawlerControllerModel(object):
    '''
    crawler_controllerコレクション用モデル
    '''

    crawler_db:Database
    mongo:MongoModel

    def __init__(self,mongo:MongoModel):
        self.mongo = mongo

    def insert_one(self, record:dict):
        self.mongo.crawler_db['crawler_controller'].insert_one(record)

from pymongo import MongoClient
from pymongo.database import Database
import os


class MongoModel(object):
    '''
    MongoDB用モデル
    '''
    __mongo_uri: str
    __mongo_port: str
    __mongo_db: str
    __mongo_user: str
    __mongo_pass: str
    __mongo_client: MongoClient
    crawler_db: Database
    collection: dict

    def __init__(self):
        self.__mongo_uri = os.environ['MONGO_SERVER']
        self.__mongo_port = os.environ['MONGO_PORT']
        self.__mongo_db = os.environ['MONGO_USE_DB']
        self.__mongo_user = os.environ['MONGO_USER']
        self.__mongo_pass = os.environ['MONGO_PASS']
        self.collection: dict = {
            'crawler_response': os.environ['MONGO_CRAWLER_RESPONSE'],
            'crawler_controller': os.environ['MONGO_CRAWLER_CONTROLLER'],
        }

        self.__mongo_client = MongoClient(
            self.__mongo_uri, int(self.__mongo_port),
        )
        self.crawler_db = self.__mongo_client[self.__mongo_db]
        self.crawler_db.authenticate(self.__mongo_user, self.__mongo_pass)

    def close(self):
        '''
        MongoClientを閉じる。
        '''
        self.__mongo_client.close()
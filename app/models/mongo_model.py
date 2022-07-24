#from pymongo import MongoClient
from pymongo.mongo_client import MongoClient
from pymongo.database import Database
import os
from urllib.parse import quote_plus

class MongoModel(object):
    '''
    MongoDB用モデル
    '''
    __mongo_uri: str
    __mongo_port: str
    __mongo_db_name: str
    __mongo_user: str
    __mongo_pass: str
    __mongo_client: MongoClient
    mongo_db: Database

    def __init__(self):
        self.__mongo_uri = os.environ['MONGO_SERVER']
        self.__mongo_port = os.environ['MONGO_PORT']
        self.__mongo_db_name = os.environ['MONGO_USE_DB']
        self.__mongo_user = os.environ['MONGO_USER']
        self.__mongo_pass = os.environ['MONGO_PASS']
        self.__mongo_query = os.environ['MONGO_QUERY']

        uri = f'mongodb://{quote_plus(self.__mongo_user)}:{quote_plus(self.__mongo_pass)}@{self.__mongo_uri}/{self.__mongo_db_name}?{self.__mongo_query}'
        self.__mongo_client = MongoClient(
            host=uri, port=int(self.__mongo_port),
        )
        self.mongo_db = self.__mongo_client[self.__mongo_db_name]

    def close(self):
        '''
        MongoClientを閉じる。
        '''
        self.__mongo_client.close()
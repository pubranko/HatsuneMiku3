from pymongo import collection
from models.mongo_model import MongoModel

class MongoCommonModel(object):
    '''
    mongoDBへの共通アクセス処理。
    各コレクション別のクラスでは当クラスを継承することで共通の関数を定義する必要がなくなる。
    '''
    mongo: MongoModel
    collection_name: str = 'sample'

    def __init__(self, mongo: MongoModel):
        self.mongo = mongo

    def find_one(self, projection=None, filter=None):
        return self.mongo.mongo_db[self.collection_name].find_one(projection=projection, filter=filter)

    #def find(self, projection=None, filter=None, sort=None, index=None):
    def find(self, projection=None, filter=None, sort=None):
        return self.mongo.mongo_db[self.collection_name].find(projection=projection, filter=filter, sort=sort)

    def insert_one(self, item):
        self.mongo.mongo_db[self.collection_name].insert_one(item)

    def insert(self, items: list):
        self.mongo.mongo_db[self.collection_name].insert(items)

    def update(self, filter, record: dict) -> None:
        self.mongo.mongo_db[self.collection_name].update(
            filter, record, upsert=True)

    def aggregate(self, aggregate_key: str):
        '''渡された集計keyによる集計結果を返す。'''
        pipeline = [
            {'$unwind': '$' + aggregate_key},
            {'$group': {'_id': '$' + aggregate_key, 'count': {'$sum': 1}}},
        ]
        return self.mongo.mongo_db[self.collection_name].aggregate(pipeline=pipeline)

from models.mongo_model import MongoModel
from models.mongo_common_model import MongoCommonModel


class AsynchronousReportModel(MongoCommonModel):
    '''
    非同期レポートコレクション用モデル
    '''
    mongo: MongoModel
    collection_name: str = 'asynchronous_report'

# class AsynchronousReportModel(object):
#     '''
#     非同期レポートコレクション用モデル
#     '''
#     mongo: MongoModel

#     def __init__(self, mongo: MongoModel):
#         self.mongo = mongo

#     def insert_one(self, item: dict):
#         self.mongo.mongo_db['asynchronous_report'].insert_one(item)

#     def find_one(self, filter=None, projection=None):
#         return self.mongo.mongo_db['asynchronous_report'].find_one(projection=projection, filter=filter)

#     def find(self, filter=None, projection=None, sort=None):
#         return self.mongo.mongo_db['asynchronous_report'].find(projection=projection, filter=filter, sort=sort)

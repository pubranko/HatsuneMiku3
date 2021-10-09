from models.mongo_model import MongoModel
from models.mongo_common_model import MongoCommonModel


class CrawlerLogsModel(MongoCommonModel):
    '''
    crawler_logsコレクション用モデル
    '''
    mongo: MongoModel
    collection_name: str = 'crawler_logs'

# class CrawlerLogsModel(object):
#     '''
#     crawler_logsコレクション用モデル
#     '''
#     mongo: MongoModel

#     def __init__(self, mongo: MongoModel):
#         self.mongo = mongo

#     def insert_one(self, item):
#         self.mongo.mongo_db['crawler_logs'].insert_one(item)

#     def find_one(self, projection=None, filter=None):
#         return self.mongo.mongo_db['crawler_logs'].find_one(projection=projection, filter=filter)

#     def find(self, projection=None, filter=None, sort=None):
#         return self.mongo.mongo_db['crawler_logs'].find(projection=projection, filter=filter, sort=sort)

#     def aggregate(self, aggregate_key: str):
#         '''渡された集計keyによる集計結果を返す。'''
#         pipeline = [
#             {'$unwind': '$' + aggregate_key},
#             {'$group': {'_id': '$' + aggregate_key, 'count': {'$sum': 1}}},
#         ]
#         return self.mongo.mongo_db['crawler_logs'].aggregate(pipeline=pipeline)

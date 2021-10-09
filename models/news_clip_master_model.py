from models.mongo_model import MongoModel
from models.mongo_common_model import MongoCommonModel


class NewsClipMasterModel(MongoCommonModel):
    '''
    news_clip_masterコレクション用モデル
    '''
    mongo: MongoModel
    collection_name: str = 'news_clip_master'

# class NewsClipMaster(object):
#     '''
#     news_clip_masterコレクション用モデル
#     '''
#     mongo: MongoModel

#     def __init__(self, mongo: MongoModel):
#         self.mongo = mongo

#     def insert_one(self, item:dict):
#         self.mongo.mongo_db['news_clip_master'].insert_one(item)

#     def find_one(self, projection=None,filter=None):
#         return self.mongo.mongo_db['news_clip_master'].find_one(projection=projection,filter=filter)

#     def find(self, projection=None,filter=None, sort=None):
#         return self.mongo.mongo_db['news_clip_master'].find(projection=projection,filter=filter,sort=sort)

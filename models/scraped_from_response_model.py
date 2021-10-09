from models.mongo_model import MongoModel
from models.mongo_common_model import MongoCommonModel


class ScrapedFromResponse(MongoCommonModel):
    '''
    scraped_from_responseコレクション用モデル
    '''
    mongo: MongoModel
    collection_name: str = 'scraped_from_response'

# class ScrapedFromResponse(object):
#     '''
#     scraped_from_responseコレクション用モデル
#     '''
#     mongo: MongoModel

#     def __init__(self, mongo: MongoModel):
#         self.mongo = mongo

#     def insert_one(self, item:dict):
#         self.mongo.mongo_db['scraped_from_response'].insert_one(item)

#     def insert(self, items:list):
#         self.mongo.mongo_db['scraped_from_response'].insert(items)

#     def find_one(self, projection=None,filter=None):
#         return self.mongo.mongo_db['scraped_from_response'].find_one(projection=projection,filter=filter)

#     def find(self, projection=None,filter=None, sort=None):
#         return self.mongo.mongo_db['scraped_from_response'].find(projection=projection,filter=filter,sort=sort)

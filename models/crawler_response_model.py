from models.mongo_model import MongoModel


class CrawlerResponseModel(object):
    '''
    crawler_responseコレクション用モデル
    '''
    mongo: MongoModel

    def __init__(self, mongo: MongoModel):
        self.mongo = mongo

    def insert_one(self, item):
        self.mongo.crawler_db['crawler_response'].insert_one(item)

    def find_one(self, key):
        return self.mongo.crawler_db['crawler_response'].find_one(key)

    def find(self, projection=None,filter=None, sort=None):
        return self.mongo.crawler_db['crawler_response'].find(projection=projection,filter=filter,sort=sort)

from models.mongo_model import MongoModel


class CrawlerLogsModel(object):
    '''
    crawler_logsコレクション用モデル
    '''
    mongo: MongoModel

    def __init__(self, mongo: MongoModel):
        self.mongo = mongo

    def insert_one(self, item):
        self.mongo.crawler_db['crawler_logs'].insert_one(item)

    def find_one(self, projection=None,filter=None):
        return self.mongo.crawler_db['crawler_logs'].find_one(projection=projection,filter=filter)

    def find(self, projection=None,filter=None, sort=None):
        return self.mongo.crawler_db['crawler_logs'].find(projection=projection,filter=filter,sort=sort)


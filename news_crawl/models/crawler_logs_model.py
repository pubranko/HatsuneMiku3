from news_crawl.models.mongo_model import MongoModel


class CrawlerLogsModel(object):
    '''
    crawler_logsコレクション用モデル
    '''
    mongo: MongoModel

    def __init__(self, mongo: MongoModel):
        self.mongo = mongo

    def insert_one(self, item):
        self.mongo.crawler_db['crawler_logs'].insert_one(item)

    def find_one(self, key):
        return self.mongo.crawler_db['crawler_logs'].find_one(key)

    def find(self, key):
        return self.mongo.crawler_db['crawler_logs'].find(key)


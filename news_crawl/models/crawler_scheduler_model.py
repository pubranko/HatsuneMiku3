from news_crawl.models.mongo_model import MongoModel


class CrawlerSchedulerModel(object):
    '''
    crawler_responseコレクション用モデル
    '''
    mongo: MongoModel

    def __init__(self, mongo: MongoModel):
        self.mongo = mongo

    def insert_one(self, item):
        self.mongo.crawler_db['crawler_scheduler'].insert_one(item)

    def find_one(self, key):
        return self.mongo.crawler_db['crawler_scheduler'].find_one(key)

    def find(self, key):
        return self.mongo.crawler_db['crawler_scheduler'].find(key)

    def update(self, filter, record: dict):
        self.mongo.crawler_db['crawler_scheduler'].update(
            filter, record, upsert=True)

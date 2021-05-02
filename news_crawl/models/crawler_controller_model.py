from news_crawl.models.mongo_model import MongoModel


class CrawlerControllerModel(object):
    '''
    crawler_controllerコレクション用モデル
    '''

    # crawler_db:Database
    mongo: MongoModel

    def __init__(self, mongo: MongoModel):
        self.mongo = mongo

    def find_one(self, key):
        return self.mongo.crawler_db['crawler_controller'].find_one(key)

    def update(self, filter, record: dict):
        self.mongo.crawler_db['crawler_controller'].update(
            filter, record, upsert=True)

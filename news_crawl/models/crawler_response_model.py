from news_crawl.models.mongo_model import MongoModel


class CrawlerResponseModel(object):
    '''
    crawler_responseコレクション用モデル
    '''
    mongo: MongoModel

    def __init__(self, mongo: MongoModel):
        self.mongo = mongo

    def insert_one(self, item):
        self.mongo.crawler_db['crawler_response'].insert_one(item)

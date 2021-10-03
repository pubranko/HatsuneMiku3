from typing import Any
from datetime import datetime
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

    def find_one(self, filter=None, projection=None):
        return self.mongo.crawler_db['crawler_response'].find_one(projection=projection,filter=filter)

    def find(self, projection=None,filter=None, sort=None):
        return self.mongo.crawler_db['crawler_response'].find(projection=projection,filter=filter,sort=sort)

    def update(self, filter, record: dict) -> None:
        self.mongo.crawler_db['crawler_response'].update(
            filter, record, upsert=True)

    def news_clip_master_register_result(self, url: str, response_time: datetime,news_clip_master_register:str) -> None:
        '''news_clip_masterへの登録結果を反映させる'''
        record: Any = self.find_one(
            filter={'$and': [{'url': url}, {'response_time': response_time}]})
        if record == None:
            pass
        else:
            record['news_clip_master_register'] = news_clip_master_register

        self.update(
            {'url': url, 'response_time': response_time},
            record,
        )


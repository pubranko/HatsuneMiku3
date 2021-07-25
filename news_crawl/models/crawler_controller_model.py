from news_crawl.models.mongo_model import MongoModel
from typing import Any
import sys,os,psutil
from datetime import datetime
import re

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

    def next_crawl_point_get(self, domain_name: str, spider_name: str) -> dict:
        '''
        次回のクロールポイント情報(lastmod,urlなど)を取得し返す。
        まだ存在しない場合、空のdictを返す。
        '''
        record: Any = self.find_one(key={'$and': [{'domain': domain_name}, {'document_type': 'next_crawl_point'}]})

        next_point_info: dict = {}
        #レコードが存在し、かつ、同じスパイダーでクロール実績がある場合
        if not record == None:
            if spider_name in record:
                next_point_info = record[spider_name]

        return next_point_info

    def next_crawl_point_update(self, domain_name: str, spider_name: str, next_point_info: dict):
        '''次回のクロールポイント情報(lastmod,urlなど)を更新する'''
        record: Any = self.find_one(key={'$and': [{'domain': domain_name}, {'document_type': 'next_crawl_point'}]})
        if record == None:  # ドメインに対して初クロールの場合
            record = {
                'document_type': 'next_crawl_point',
                'domain': domain_name,
                spider_name: next_point_info,
            }
        else:
            record[spider_name] = next_point_info

        self.update({'domain': domain_name}, record,)
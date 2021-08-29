from models.mongo_model import MongoModel
from typing import Any
import sys,os,psutil
from datetime import datetime
import re

class ControllerModel(object):
    '''
    controllerコレクション用モデル
    '''
    mongo: MongoModel

    def __init__(self, mongo: MongoModel):
        self.mongo = mongo

    def find_one(self, key):
        return self.mongo.crawler_db['controller'].find_one(key)

    def update(self, filter, record: dict) -> None:
        self.mongo.crawler_db['controller'].update(
            filter, record, upsert=True)

    def crawl_point_get(self, domain_name: str, spider_name: str) -> dict:
        '''
        次回のクロールポイント情報(lastmod,urlなど)を取得し返す。
        まだ存在しない場合、空のdictを返す。
        '''
        record: Any = self.find_one(key={'$and': [{'domain': domain_name}, {'document_type': 'crawl_point'}]})

        next_point_record: dict = {}
        #レコードが存在し、かつ、同じスパイダーでクロール実績がある場合
        if not record == None:
            if spider_name in record:
                next_point_record = record[spider_name]

        return next_point_record

    def crawl_point_update(self, domain_name: str, spider_name: str, next_point_info: dict) -> None:
        '''次回のクロールポイント情報(lastmod,urlなど)を更新する'''
        record: Any = self.find_one(key={'$and': [{'domain': domain_name}, {'document_type': 'crawl_point'}]})
        if record == None:  # ドメインに対して初クロールの場合
            record = {
                'document_type': 'crawl_point',
                'domain': domain_name,
                spider_name: next_point_info,
            }
        else:
            record[spider_name] = next_point_info

        self.update({'domain': domain_name}, record,)

    # def regular_observation_point_get(self,) -> dict:
    #     '''
    #     直近で定期観測を行ったstart_timeを取得する。
    #     '''
    #     record: Any = self.find_one(key={'$and': [{'document_type': 'regular_observation_point'}]})

    #     if record == None:
    #         record = {}

    #     return record

    # def regular_observation_point_update(self, start_time:datetime) -> None:
    #     '''
    #     今回の定期観測のstart_timeを保存する。
    #     '''
    #     record: Any = self.find_one(key={'$and': [{'document_type': 'regular_observation_point'}]})

    #     if record == None:  # 初回の場合
    #         record = {
    #             'document_type': 'regular_observation_point',
    #             'start_time': start_time,
    #         }
    #     else:
    #         record['start_time'] = start_time

    #     self.update({'document_type': 'regular_observation_point'}, record,)

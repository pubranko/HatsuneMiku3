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

    def crawling_stop_domain_list_get(self,) -> list:
        '''
        stop_controllerからクローリング停止ドメインリストを取得して返す
        '''
        record: Any = self.find_one(key={'$and': [{'document_type': 'stop_controller'}]})

        if record == None:
            return []
        elif not 'crawling_stop_domain_list' in record:
            return []
        else:
            return record['crawling_stop_domain_list']

    def crawling_stop_domain_list_update(self, crawling_stop_domain_list:list) -> None:
        '''
        stop_controllerのクローリング停止ドメインリストを更新する。
        '''
        record: Any = self.find_one(key={'$and': [{'document_type': 'stop_controller'}]})

        if record == None:  # 初回の場合
            record = {
                'document_type': 'stop_controller',
                'crawling_stop_domain_list': crawling_stop_domain_list,
            }
        else:
            record['crawling_stop_domain_list'] = crawling_stop_domain_list

        self.update({'document_type': 'stop_controller'}, record,)

    def scrapying_stop_domain_list_get(self,) -> list:
        '''
        stop_controllerからスクレイピング停止ドメインリストを取得して返す。
        '''
        record: Any = self.find_one(key={'$and': [{'document_type': 'stop_controller'}]})

        if record == None:
            return []
        elif not 'scrapying_stop_domain_list' in record:
            return []
        else:
            return record['scrapying_stop_domain_list']

    def scrapying_stop_domain_list_update(self, scrapying_stop_domain_list:list) -> None:
        '''
        stop_controllerのスクレイピング停止ドメインリストを更新する。
        '''
        record: Any = self.find_one(key={'$and': [{'document_type': 'stop_controller'}]})

        if record == None:  # 初回の場合
            record = {
                'document_type': 'stop_controller',
                'scrapying_stop_domain_list': scrapying_stop_domain_list,
            }
        else:
            record['scrapying_stop_domain_list'] = scrapying_stop_domain_list

        self.update({'document_type': 'stop_controller'}, record,)

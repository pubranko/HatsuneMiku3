import os
import sys
import logging
from typing import Any, Union
from logging import Logger, StreamHandler
from datetime import datetime
from pymongo.cursor import Cursor
path = os.getcwd()
sys.path.append(path)
from models.mongo_model import MongoModel
from models.crawler_response_model import CrawlerResponseModel
from prefect_lib.settings import TIMEZONE

logger:Logger = logging.getLogger('prefect.' +
                        sys._getframe().f_code.co_name)

def scrapying_deco(func):
    global logger

    def exec(kwargs):
        logger.info('=== exec:', kwargs)
        starting_time: datetime = kwargs['starting_time']
        mongo: MongoModel = kwargs['mongo']
        kwargs['crawler_response'] = CrawlerResponseModel(mongo)
        # 初期処理
        pass
        # 主処理
        func(kwargs)
        # 終了処理
        pass
    return exec


@scrapying_deco
def test1(kwargs):
    '''あとで'''
    global logger
    starting_time: datetime = kwargs['starting_time']
    crawler_response: CrawlerResponseModel = kwargs['crawler_response']
    domain: str = kwargs['domain']
    response_time_from: datetime = kwargs['response_time_from']
    response_time_to: datetime = kwargs['response_time_to']

    conditions :list = []
    if domain:
        conditions.append({'domain':domain})
    if response_time_from:
        conditions.append({'response_time':{'$gte':response_time_from}})
    if response_time_to:
        conditions.append({'response_time':{'$lte':response_time_to}})
    filter: dict = {'$and':conditions }
    logger.info('=== crawler_responseへのfilter: ' + str(filter))

    records: Cursor = crawler_response.find(
        projection=None,
        filter=filter,
        #filter={"url": "https://www.sankei.com/article/20210808-LDAOZVXFTBJY3N2CR4CDMLGMM4/",},
        #filter={'response_time': {'$gte':datetime(2021,7,1),}},
        #filter={'response_time_from': {'$gte':response_time_from,}},
        # filter={'$and':[
        #             {'response_time': {'$gte': response_time_from, }},
        #             {'response_time': {'$lte': response_time_to, }},
        #         ],},
        #filter={'$and': [{'domain': domain_name}, {'document_type': 'next_crawl_point'}]}
        sort=None
    )
    # record: Cursor = crawler_response.find(
    #     {})

    for record in records:
        logger.info('=== record_info : '
            + str(record['_id']) + '\n'
            + str(record['url']) + '\n'
            + str(record['response_time'])
        )
    logger.info('=== crawler_response取得件数: ' + str(records.count()))

import os
import sys
import logging
from typing import Any, Union
from logging import Logger, StreamHandler
from datetime import datetime
from importlib import import_module
from pymongo.cursor import Cursor
path = os.getcwd()
sys.path.append(path)
from models.crawler_response_model import CrawlerResponseModel
from models.scraped_from_response_model import ScrapedFromResponse
from prefect_lib.settings import TIMEZONE

logger:Logger = logging.getLogger('prefect.run.scrapying_deco')

def scrapying_deco(func):
    global logger

    def exec(kwargs):
        # 初期処理
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
    scraped_from_response: ScrapedFromResponse = kwargs['scraped_from_response']
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

    if conditions:
        filter: Any = {'$and': conditions}
    else:
        filter = None
    logger.info('=== crawler_responseへのfilter: ' + str(filter))

    # スクレイピング対象件数を確認
    record_count = crawler_response.find(
        projection=None,
        filter=filter,
        sort=None
    ).count()
    logger.info('=== crawler_response スクレイピング対象件数 : ' + str(record_count))

    # 100件単位でスクレイピング処理を実施
    limit:int = 100
    skip_list = list(range(0,record_count, limit))

    for skip in skip_list:
        records: Cursor = crawler_response.find(
            projection=None,
            filter=filter,
            sort=None
        ).skip(skip).limit(limit)

        for record in records:
            # 各サイト共通の項目を設定
            scraped:dict = {}
            scraped['domain'] = record['domain']
            scraped['url'] = record['url']
            scraped['response_time'] = record['response_time']
            scraped['scraped_starting_time'] = starting_time

            # 各サイトのスクレイピングした項目を結合
            module_name = record['domain'].replace('.','_')
            mod: Any = import_module('prefect_lib.scraper.' + module_name)
            scraped.update(getattr(mod, 'exec')(record))

            scraped_from_response.insert_one(scraped)

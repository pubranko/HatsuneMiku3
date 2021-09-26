import os
import sys
import logging
from typing import Any
from logging import Logger
from datetime import datetime
from importlib import import_module
from pymongo.cursor import Cursor
path = os.getcwd()
sys.path.append(path)
from models.crawler_response_model import CrawlerResponseModel
from models.scraped_from_response_model import ScrapedFromResponse
from models.controller_model import ControllerModel
from common_lib.timezone_recovery import timezone_recovery
from prefect_lib.common_module.scraped_record_error_check import scraped_record_error_check

logger: Logger = logging.getLogger('prefect.run.scrapying_deco')

import time

def exec(kwargs:dict):
    '''あとで'''
    global logger
    start_time: datetime = kwargs['start_time']
    crawler_response: CrawlerResponseModel = kwargs['crawler_response']
    scraped_from_response: ScrapedFromResponse = kwargs['scraped_from_response']
    controller:ControllerModel = kwargs['controller']
    domain: str = kwargs['domain']
    crawling_start_time_from: datetime = kwargs['crawling_start_time_from']
    crawling_start_time_to: datetime = kwargs['crawling_start_time_to']
    #print('=== urls:',type(kwargs['urls']))
    urls: list = kwargs['urls']

    stop_domain:list = controller.scrapying_stop_domain_list_get()

    conditions: list = []
    if domain:
        conditions.append({'domain': domain})
    if crawling_start_time_from:
        conditions.append({'crawling_start_time': {'$gte': crawling_start_time_from}})
    if crawling_start_time_to:
        conditions.append({'crawling_start_time': {'$lte': crawling_start_time_to}})
    if urls:
        conditions.append({'url':{'$in': urls}})
    if len(stop_domain) > 0:
        conditions.append({'domain':{'$nin': stop_domain}})


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
    limit: int = 100
    skip_list = list(range(0, record_count, limit))

    scraped: dict = {}
    scraper_mod:dict = {}

    for skip in skip_list:
        records: Cursor = crawler_response.find(
            projection=None,
            filter=filter,
            sort=None
        ).skip(skip).limit(limit)

        #items:list = [None] # * limit #リストの枠を予め確保。処理速度がわずかに早くなるらしい。

        for record in records:
            # 各サイト共通の項目を設定
            scraped = {}
            scraped['domain'] = record['domain']
            scraped['url'] = record['url']
            scraped['response_time'] = timezone_recovery(record['response_time'])
            scraped['crawling_start_time'] = timezone_recovery(record['crawling_start_time'])
            scraped['scrapying_start_time'] = start_time
            scraped['source_of_information'] = record['source_of_information']

            # 各サイトのスクレイピングした項目を結合
            module_name = str(record['domain']).replace('.', '_')
            if not module_name in scraper_mod:
                scraper_mod[module_name] = import_module('prefect_lib.scraper.' + module_name)
            scraped.update(getattr(scraper_mod[module_name], 'exec')(record,kwargs))
            # mod: Any = import_module('prefect_lib.scraper.' + module_name)
            # scraped.update(getattr(mod, 'exec')(record,kwargs))

            # データチェック
            error_flg:bool = scraped_record_error_check(scraped)
            if not error_flg:
                #items = list(scraped)
                #一時停止中！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！
                pass
                scraped_from_response.insert_one(scraped)

        #scraped_from_response.insert(items)

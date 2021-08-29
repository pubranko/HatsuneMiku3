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
from common_lib.timezone_recovery import timezone_recovery
from prefect_lib.common_module.scraped_record_error_check import scraped_record_error_check

logger: Logger = logging.getLogger('prefect.run.scrapying_deco')


def exec(kwargs:dict):
    '''あとで'''
    global logger
    start_time: datetime = kwargs['start_time']
    crawler_response: CrawlerResponseModel = kwargs['crawler_response']
    scraped_from_response: ScrapedFromResponse = kwargs['scraped_from_response']
    domain: str = kwargs['domain']
    crawling_start_time_from: datetime = kwargs['crawling_start_time_from']
    crawling_start_time_to: datetime = kwargs['crawling_start_time_to']

    conditions: list = []
    if domain:
        conditions.append({'domain': domain})
    if crawling_start_time_from:
        conditions.append({'crawling_start_time': {'$gte': crawling_start_time_from}})
    if crawling_start_time_to:
        conditions.append({'crawling_start_time': {'$lte': crawling_start_time_to}})

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

    for skip in skip_list:
        records: Cursor = crawler_response.find(
            projection=None,
            filter=filter,
            sort=None
        ).skip(skip).limit(limit)

        for record in records:
            # 各サイト共通の項目を設定
            scraped: dict = {}
            scraped['domain'] = record['domain']
            scraped['url'] = record['url']
            scraped['response_time'] = timezone_recovery(record['response_time'])
            scraped['crawling_start_time'] = timezone_recovery(record['crawling_start_time'])
            scraped['scrapying_start_time'] = start_time

            # 各サイトのスクレイピングした項目を結合
            module_name = record['domain'].replace('.', '_')
            mod: Any = import_module('prefect_lib.scraper.' + module_name)
            scraped.update(getattr(mod, 'exec')(record))

            # データチェック
            error_flg:bool = scraped_record_error_check(scraped)
            if not error_flg:
                scraped_from_response.insert_one(scraped)

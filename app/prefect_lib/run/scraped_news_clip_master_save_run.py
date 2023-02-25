import os
import sys
import logging
from typing import Any
from logging import Logger
from datetime import datetime
from pymongo import ASCENDING
from pymongo.cursor import Cursor
path = os.getcwd()
sys.path.append(path)
from BrownieAtelierMongo.models.mongo_model import MongoModel
from BrownieAtelierMongo.models.scraped_from_response_model import ScrapedFromResponseModel
from BrownieAtelierMongo.models.crawler_response_model import CrawlerResponseModel
from BrownieAtelierMongo.models.news_clip_master_model import NewsClipMasterModel
from prefect_lib.common_module.scraped_record_error_check import scraped_record_error_check

logger: Logger = logging.getLogger('prefect.run.news_clip_master_save')


def check_and_save(kwargs: dict):
    '''あとで'''
    global logger
    start_time: datetime = kwargs['start_time']
    mongo:MongoModel = kwargs['mongo']
    scraped_from_response: ScrapedFromResponseModel = ScrapedFromResponseModel(mongo)
    news_clip_master: NewsClipMasterModel = NewsClipMasterModel(mongo)
    crawler_response: CrawlerResponseModel = CrawlerResponseModel(mongo)

    domain: str = kwargs['domain']
    scrapying_start_time_from: datetime = kwargs['scrapying_start_time_from']
    scrapying_start_time_to: datetime = kwargs['scrapying_start_time_to']

    conditions: list = []
    if domain:
        conditions.append({'domain': domain})
    if scrapying_start_time_from:
        conditions.append(
            {'scrapying_start_time': {'$gte': scrapying_start_time_from}})
    if scrapying_start_time_to:
        conditions.append(
            {'scrapying_start_time': {'$lte': scrapying_start_time_to}})

    if conditions:
        filter: Any = {'$and': conditions}
    else:
        filter = None

    logger.info('=== scraped_from_response へのfilter: ' + str(filter))

    # 対象件数を確認
    record_count = scraped_from_response.count(filter=filter,)
    logger.info('=== news_clip_master への登録チェック対象件数 : ' + str(record_count))

    # 100件単位で処理を実施
    limit: int = 100
    skip_list = list(range(0, record_count, limit))

    for skip in skip_list:
        records: Cursor = scraped_from_response.find(
            filter=filter,
            sort=[('response_time',ASCENDING)],
        ).skip(skip).limit(limit)

        for record in records:
            # データチェック
            if not scraped_record_error_check(record):
                # 重複チェック
                filter={'$and': [
                        {'url': record['url']},
                        {'title': record['title']},
                        {'article': record['article']},
                    ]}
                news_clip_records = news_clip_master.find(filter=filter)

                # 重複するレコードがなければ保存する。
                if news_clip_master.count(filter) == 0:
                    record['scraped_save_start_time'] = start_time
                    news_clip_master.insert_one(record)
                    logger.info('=== news_clip_master への登録 : ' + record['url'])
                    news_clip_master_register:str = '登録完了'
                    crawler_response.news_clip_master_register_result(record['url'],record['response_time'],news_clip_master_register)
                else:
                    for news_clip_record in news_clip_records:
                        if news_clip_record['response_time'] == record['response_time']:
                            logger.info(
                                '=== news_clip_master への登録処理済みデータのためスキップ : ' + record['url'])
                        else:
                            news_clip_master_register:str = '登録内容に差異なしのため不要'
                            crawler_response.news_clip_master_register_result(record['url'],record['response_time'],news_clip_master_register)
                            logger.info(
                                '=== news_clip_master の登録内容に変更がないためスキップ : ' + record['url'])

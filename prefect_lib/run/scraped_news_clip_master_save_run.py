import os
import sys
import logging
from typing import Any
from logging import Logger
from datetime import datetime
from pymongo.cursor import Cursor
path = os.getcwd()
sys.path.append(path)
from models.scraped_from_response_model import ScrapedFromResponse
from models.news_clip_master_model import NewsClipMaster
from common_lib.timezone_recovery import timezone_recovery
from prefect_lib.common_module.scraped_record_error_check import scraped_record_error_check

logger: Logger = logging.getLogger('prefect.run.news_clip_master_save')


def check_and_save(kwargs: dict):
    '''あとで'''
    global logger
    start_time: datetime = kwargs['start_time']
    scraped_from_response: ScrapedFromResponse = kwargs['scraped_from_response']
    news_clip_master: NewsClipMaster = kwargs['news_clip_master']

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
    record_count = scraped_from_response.find(
        filter=filter,
    ).count()
    logger.info('=== news_clip_master への登録チェック対象件数 : ' + str(record_count))

    # 100件単位で処理を実施
    limit: int = 100
    skip_list = list(range(0, record_count, limit))

    for skip in skip_list:
        records: Cursor = scraped_from_response.find(
            filter=filter,
        ).skip(skip).limit(limit)

        for record in records:

            # データチェック
            error_flg: bool = scraped_record_error_check(record)

            if not error_flg:
                # 重複チェック
                news_clip_duplicate_count = news_clip_master.find(
                    filter={'$and': [
                        {'url': record['url']},
                        {'title': record['title']},
                        {'article': record['article']},
                        {'publish_date': timezone_recovery(
                            record['publish_date'])},
                    ]},
                ).count()

                # 重複するレコードがなければ保存する。
                if news_clip_duplicate_count == 0:
                    record['scraped_save_start_time'] = start_time
                    news_clip_master.insert_one(record)
                    logger.info('=== news_clip_master への登録 : ' + record['url'])
                else:
                    logger.info(
                        '=== news_clip_master への登録スキップ : ' + record['url'])

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
from models.news_clip_master_model import NewsClipMaster
from prefect_lib.settings import TIMEZONE
from dateutil import tz

logger: Logger = logging.getLogger('prefect.run.news_clip_master_save')


def check_and_save(kwargs):
    '''あとで'''
    global logger
    starting_time: datetime = kwargs['starting_time']
    scraped_from_response: ScrapedFromResponse = kwargs['scraped_from_response']
    news_clip_master: NewsClipMaster = kwargs['news_clip_master']

    domain: str = kwargs['domain']
    scraped_starting_time_from: datetime = kwargs['scraped_starting_time_from']
    scraped_starting_time_to: datetime = kwargs['scraped_starting_time_to']

    conditions: list = []
    if domain:
        conditions.append({'domain': domain})
    if scraped_starting_time_from:
        conditions.append(
            {'scraped_starting_time': {'$gte': scraped_starting_time_from}})
    if scraped_starting_time_to:
        conditions.append(
            {'scraped_starting_time': {'$lte': scraped_starting_time_to}})

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

            # 重複チェック
            news_clip_duplicate_count = news_clip_master.find(
                filter={'$and': [
                    {'url': record['url']},
                    {'title': record['title']},
                    {'article': record['article']},
                    {'publish_date': record['publish_date']},
                ]},
            ).count()

            # 取得したレコードのscraped_starting_timeのタイムゾーンを修正(MongoDB? PyMongo?のバグのらしい)
            UTC = tz.gettz("UTC")
            dt: datetime = record['scraped_starting_time']
            dt = dt.replace(tzinfo=UTC)
            dt = dt.astimezone(TIMEZONE)

            # 重複するレコードがなければ保存する。
            st: str = dt.isoformat()
            if news_clip_duplicate_count == 0:
                record['scraped_save_starting_time'] = starting_time
                news_clip_master.insert_one(record)
                logger.info('=== news_clip_master への登録 : ' +
                            st + ' : ' + record['url'])
            else:
                logger.info('=== news_clip_master への登録スキップ : ' +
                            st + ' : ' + record['url'])

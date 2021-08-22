import os
import sys
import logging
from logging import Logger
from datetime import datetime
from typing import Any
from pymongo.cursor import Cursor
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run.news_clip_master_save import check_and_save
from models.mongo_model import MongoModel
from models.crawler_response_model import CrawlerResponseModel
from models.scraped_from_response_model import ScrapedFromResponse
from models.news_clip_master_model import NewsClipMaster
from prefect_lib.settings import TIMEZONE


class ScrapedSaveTask(ExtensionsTask):
    '''
    スクレイプ結果の保存タスク
    '''

    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''

        logger: Logger = self.logger
        #starting_time: datetime = self.starting_time
        mongo: MongoModel = self.mongo
        #scraped_from_response = ScrapedFromResponse(mongo)
        #news_clip_master = NewsClipMaster(mongo)

        kwargs['starting_time'] = self.starting_time
        kwargs['scraped_from_response'] = ScrapedFromResponse(mongo)
        kwargs['news_clip_master'] = NewsClipMaster(mongo)

        logger.info('=== Scraped Save run kwargs : ' + str(kwargs))
        check_and_save(kwargs)

        # domain: str = kwargs['domain']
        # scraped_starting_time_from: datetime = kwargs['scraped_starting_time_from']
        # scraped_starting_time_to: datetime = kwargs['scraped_starting_time_to']

        # conditions: list = []
        # if domain:
        #     conditions.append({'domain': domain})
        # if scraped_starting_time_from:
        #     conditions.append({'scraped_starting_time': {'$gte': scraped_starting_time_from}})
        # if scraped_starting_time_to:
        #     conditions.append({'scraped_starting_time': {'$lte': scraped_starting_time_to}})

        # if conditions:
        #     filter: Any = {'$and': conditions}
        # else:
        #     filter = None

        # logger.info('=== scraped_from_response へのfilter: ' + str(filter))

        # # 対象件数を確認
        # record_count = scraped_from_response.find(
        #     filter=filter,
        # ).count()
        # logger.info('=== news_clip_master への登録チェック対象件数 : ' + str(record_count))

        # # 100件単位で処理を実施
        # limit: int = 100
        # skip_list = list(range(0, record_count, limit))

        # for skip in skip_list:
        #     records: Cursor = scraped_from_response.find(
        #         filter=filter,
        #     ).skip(skip).limit(limit)

        #     for record in records:

        #         # 重複チェック
        #         news_clip_duplicate_count = news_clip_master.find(
        #             filter={'$and':[
        #                 {'url':record['url']},
        #                 {'title':record['title']},
        #                 {'article':record['article']},
        #                 {'publish_date':record['publish_date']},
        #             ]},
        #         ).count()

        #         #重複するレコードがなければ保存する。
        #         dt:datetime = record['scraped_starting_time']
        #         st:str = dt.isoformat()
        #         if news_clip_duplicate_count == 0:
        #             record['scraped_save_starting_time'] = self.starting_time
        #             news_clip_master.insert_one(record)
        #             logger.info('=== news_clip_master への登録 : ' + st + ' : ' + record['url'])
        #         else:
        #             logger.info('=== news_clip_master への登録スキップ : ' + st + ' : '  + record['url'])


        # 終了処理
        self.closed()
        # return ''

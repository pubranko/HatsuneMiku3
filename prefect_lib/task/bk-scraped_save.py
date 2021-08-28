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
from prefect_lib.settings import TIMEZONE
from models.mongo_model import MongoModel
from models.crawler_response_model import CrawlerResponseModel
from models.scraped_from_response_model import ScrapedFromResponse
from models.news_clip_master_model import NewsClipMaster

class ScrapedSaveTask(ExtensionsTask):
    '''
    スクレイプ結果の保存タスク
    '''

    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''

        logger: Logger = self.logger
        mongo: MongoModel = self.mongo

        kwargs['starting_time'] = self.starting_time
        kwargs['scraped_from_response'] = ScrapedFromResponse(mongo)
        kwargs['news_clip_master'] = NewsClipMaster(mongo)

        logger.info('=== Scraped Save run kwargs : ' + str(kwargs))
        check_and_save(kwargs)

        # 終了処理
        self.closed()
        # return ''

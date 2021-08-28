# pylint: disable=E1101
import os
import sys
from typing import Any
import logging
from logging import Logger
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import scrapy_crawling_continue_run,scrapying_run,scraped_news_clip_master_save_run,solr_news_clip_save_run
from models.mongo_model import MongoModel
from models.crawler_response_model import CrawlerResponseModel
from models.scraped_from_response_model import ScrapedFromResponse
from models.news_clip_master_model import NewsClipMaster

import threading

class RegularObservationTask(ExtensionsTask):
    '''
    定期観測用タスク
    '''
    def run(self):
        '''ここがprefectで起動するメイン処理'''

        kwargs:dict = {}
        kwargs['starting_time'] = self.starting_time
        mongo: MongoModel = self.mongo
        kwargs['crawler_response'] = CrawlerResponseModel(mongo)
        kwargs['scraped_from_response'] = ScrapedFromResponse(mongo)
        kwargs['news_clip_master'] = NewsClipMaster(mongo)

        kwargs['domain'] = None
        kwargs['response_time_from'] = None
        kwargs['response_time_to'] = None
        kwargs['scrapying_starting_time_from'] = None
        kwargs['scrapying_starting_time_to'] = None
        kwargs['scraped_save_starting_time_from'] = None
        kwargs['scraped_save_starting_time_to'] = None

        logger: Logger = self.logger
        logger.info('=== ScrapyingTask run kwargs : ' + str(kwargs))

        thread = threading.Thread(target=scrapy_crawling_continue_run.exec(self.starting_time))
        # マルチプロセスで動いているScrapyの終了を待ってから後続の処理を実行する。
        thread.start()
        thread.join()

        scrapying_run.exec(kwargs)
        scraped_news_clip_master_save_run.check_and_save(kwargs)
        solr_news_clip_save_run.check_and_save(kwargs)

        # 終了処理
        self.closed()
        #return ''

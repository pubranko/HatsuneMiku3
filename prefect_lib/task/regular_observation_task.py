# pylint: disable=E1101
import os
import sys
from logging import Logger
import threading
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import scrapy_crawling_run, scrapying_run, scraped_news_clip_master_save_run, solr_news_clip_save_run
from models.mongo_model import MongoModel
from models.crawler_response_model import CrawlerResponseModel
from models.scraped_from_response_model import ScrapedFromResponse
from models.news_clip_master_model import NewsClipMaster
from models.controller_model import ControllerModel
from common_lib.directory_search import directory_search


class RegularObservationTask(ExtensionsTask):
    '''
    定期観測用タスク
    '''

    def run(self):
        '''ここがprefectで起動するメイン処理'''
        logger: Logger = self.logger

        kwargs: dict = {}
        kwargs['start_time'] = self.start_time
        kwargs['logger'] = self.logger
        mongo: MongoModel = self.mongo
        kwargs['crawler_response'] = CrawlerResponseModel(mongo)
        kwargs['scraped_from_response'] = ScrapedFromResponse(mongo)
        kwargs['news_clip_master'] = NewsClipMaster(mongo)
        kwargs['controller'] = ControllerModel(self.mongo)

        kwargs['domain'] = None
        kwargs['crawling_start_time_from'] = self.start_time
        kwargs['crawling_start_time_to'] = self.start_time
        kwargs['scrapying_start_time_from'] = self.start_time
        kwargs['scrapying_start_time_to'] = self.start_time
        kwargs['urls'] = []
        kwargs['scraped_save_start_time_from'] = self.start_time
        kwargs['scraped_save_start_time_to'] = self.start_time

        spiders_info = directory_search()
        controller: ControllerModel = kwargs['controller']
        spider_name_set: set = controller.regular_observation_spider_name_set_get()

        # 定点観測の対象・対象外spiderを振り分け
        crawling_target_spiders: list = []
        crawling_target_spiders_name: list = []
        crawling_subject_spiders_name: list = []
        for spider_info in spiders_info:
            if spider_info['spider_name'] in spider_name_set:
                crawling_target_spiders.append(spider_info)
                crawling_target_spiders_name.append(spider_info['spider_name'])
            else:
                crawling_subject_spiders_name.append(
                    spider_info['spider_name'])

        kwargs['spiders_info'] = crawling_target_spiders

        logger.info('=== 定点観測対象スパイダー : ' +
                    str(crawling_target_spiders_name) + '\n')
        logger.info('=== 定点観測対象外スパイダー : ' +
                    str(crawling_subject_spiders_name) + '\n')
        logger.info('=== 定点観測 run kwargs : ' + str(kwargs) + '\n')

        thread = threading.Thread(
            target=scrapy_crawling_run.continued_run(kwargs))

        # マルチプロセスで動いているScrapyの終了を待ってから後続の処理を実行する。
        thread.start()
        thread.join()

        scrapying_run.exec(kwargs)
        scraped_news_clip_master_save_run.check_and_save(kwargs)
        solr_news_clip_save_run.check_and_save(kwargs)

        # 終了処理
        self.closed()
        # return ''

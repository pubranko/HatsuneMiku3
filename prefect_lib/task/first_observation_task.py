import os
import sys
from logging import Logger
import threading
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import scrapy_crawling_run, scrapying_run, scraped_news_clip_master_save_run, solr_news_clip_save_run
from common_lib.directory_search import directory_search
from models.controller_model import ControllerModel


class FirstObservationTask(ExtensionsTask):
    '''
    初回観測用タスク：以下の条件を満たすスパイダーを対象とする。
    １．定期観測コントローラーに登録のあるスパイダー
    ２．前回クロール情報がないスパイダー
    '''

    def run(self):
        '''ここがprefectで起動するメイン処理'''
        logger: Logger = self.logger

        kwargs: dict = {}
        kwargs['start_time'] = self.start_time
        kwargs['logger'] = self.logger
        kwargs['mongo'] = self.mongo
        kwargs['domain'] = None
        kwargs['crawling_start_time_from'] = self.start_time
        kwargs['crawling_start_time_to'] = self.start_time
        kwargs['scrapying_start_time_from'] = self.start_time
        kwargs['scrapying_start_time_to'] = self.start_time
        kwargs['urls'] = []
        kwargs['scraped_save_start_time_from'] = self.start_time
        kwargs['scraped_save_start_time_to'] = self.start_time

        # 初回観測の対象spiders_infoを抽出
        spiders_info = directory_search()
        controller: ControllerModel = ControllerModel(self.mongo)
        regular_observation_spider_name_set: set = controller.regular_observation_spider_name_set_get()

        crawling_target_spiders: list = []
        crawling_target_spiders_name: list = []
        for spider_info in spiders_info:

            crawl_point_record: dict = controller.crawl_point_get(
                spider_info['domain_name'], spider_info['spider_name'],)

            if not spider_info['spider_name'] in regular_observation_spider_name_set:
                pass    #定期観測に登録がないスパイダーは対象外
            elif len(crawl_point_record):
                pass    #クロールポイントがある（既にクロール実績がある）スパイダーは対象外
            else:
                crawling_target_spiders.append(spider_info)
                crawling_target_spiders_name.append(spider_info['spider_name'])


        kwargs['spiders_info'] = crawling_target_spiders

        logger.info('=== 初回観測対象スパイダー : ' +
                    str(crawling_target_spiders_name) + '\n')
        logger.info('=== 初回観測 run kwargs : ' + str(kwargs) + '\n')

        thread = threading.Thread(
            target=scrapy_crawling_run.first_run(kwargs))

        # マルチプロセスで動いているScrapyの終了を待ってから後続の処理を実行する。
        thread.start()
        thread.join()

        scrapying_run.exec(kwargs)
        scraped_news_clip_master_save_run.check_and_save(kwargs)
        solr_news_clip_save_run.check_and_save(kwargs)

        # 終了処理
        self.closed()
        # return ''



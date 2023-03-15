import os
import sys
import threading
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import scrapy_crawling_run, scrapying_run, scraped_news_clip_master_save_run, solr_news_clip_save_run
from shared.directory_search_spiders import DirectorySearchSpiders
from BrownieAtelierMongo.collection_models.controller_model import ControllerModel
from news_crawl.news_crawl_input import NewsCrawlInput


class FirstObservationTask(ExtensionsTask):
    '''
    初回観測用タスク：以下の条件を満たすスパイダーを対象とする。
    １．定期観測コントローラーに登録のあるスパイダー
    ２．前回クロール情報がないスパイダー
    '''

    def run(self):
        '''ここがprefectで起動するメイン処理'''
        self.run_init()

        # 初回観測の対象spiders_infoを抽出
        directory_search_spiders = DirectorySearchSpiders()
        controller: ControllerModel = ControllerModel(self.mongo)
        regular_observation_spider_name_set: set = controller.regular_observation_spider_name_set_get()

        # 初回観測の対象スパイダー情報、スパイダー名称保存リスト
        crawling_target_spiders: list = []
        crawling_target_spiders_name: list = []

        # 対象スパイダーの絞り込み
        for spider_name in directory_search_spiders.spiders_name_list_get():
            spider_info = directory_search_spiders.spiders_info[spider_name]
            crawl_point_record: dict = controller.crawl_point_get(
                spider_info[directory_search_spiders.DOMAIN_NAME], spider_name,)

            if not spider_name in regular_observation_spider_name_set:
                self.logger.info(f'=== 定期観測に登録がないスパイダーは対象外 : {spider_name}')
            elif len(crawl_point_record):
                self.logger.info(f'=== クロールポイントがある（既にクロール実績がある）スパイダーは対象外 : {spider_name}')
            else:
                crawling_target_spiders.append(spider_info)
                crawling_target_spiders_name.append(spider_name)

        if len(crawling_target_spiders_name):
            news_crawl_input = NewsCrawlInput(**dict(
                crawling_start_time = self.start_time,
                page_span_from = 1,
                page_span_to = 3,
                lastmod_term_minutes_from = 60,
            ))

            self.logger.info(f'=== 初回観測対象スパイダー : {str(crawling_target_spiders_name)}')
            self.logger.info(f'=== 初回観測 run kwargs : {news_crawl_input.__dict__}')

            thread = threading.Thread(
                target=scrapy_crawling_run.custom_runner_run(
                    logger = self.logger,
                    start_time = self.start_time,
                    scrapy_crawling_kwargs = news_crawl_input.__dict__,
                    spiders_info = crawling_target_spiders))

            # マルチプロセスで動いているScrapyの終了を待ってから後続の処理を実行する。
            thread.start()
            thread.join()

            scrapying_run.exec(
                start_time = self.start_time,
                mongo = self.mongo,
                domain = None,
                urls = [],
                target_start_time_from = self.start_time,
                target_start_time_to = self.start_time)
            scraped_news_clip_master_save_run.check_and_save(
                start_time = self.start_time,
                mongo = self.mongo,
                domain = None,
                target_start_time_from = self.start_time,
                target_start_time_to = self.start_time)

            ### 本格開発までsolrへの連動を一時停止 ###
            # solr_news_clip_save_run.check_and_save(kwargs)
        else:
            self.logger.warn(f'=== 初回観測対象スパイダーなし')

        # 終了処理
        self.closed()
        # return ''

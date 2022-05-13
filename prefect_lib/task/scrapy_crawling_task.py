# pylint: disable=E1101
import os
import sys
from typing import Any
from prefect.engine import state
from prefect.engine.runner import ENDRUN
import threading
path = os.getcwd()
sys.path.append(path)
from common_lib.directory_search_spiders import DirectorySearchSpiders
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import scrapy_crawling_run, scrapying_run, scraped_news_clip_master_save_run, solr_news_clip_save_run
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.data_models.scrapy_crawling_kwargs_input import ScrapyCrawlingKwargsInput
import pprint
from twisted.internet import reactor


class ScrapyCrawlingTask(ExtensionsTask):
    '''
    クローリング用タスク
    '''

    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''
        kwargs['logger'] = self.logger
        self.logger.info('=== Scrapy crawling run kwargs : ' + str(kwargs))

        error_spider_names: list = []
        threads: list[threading.Thread] = []
        directory_search_spiders = DirectorySearchSpiders()

        # spidersディレクトリより取得した一覧に存在するかチェック
        args_spiders_name = set(kwargs['spider_names'])
        for args_spider_name in args_spiders_name:
            # spidersディレクトリより取得した一覧に存在するかチェック
            if not args_spider_name in directory_search_spiders.spiders_name_list_get():
                error_spider_names.append(args_spider_name)
        if len(error_spider_names):
            self.logger.error(
                f'=== scrapy crwal run : 指定されたspider_nameは存在しませんでした : {error_spider_names}')
            raise ENDRUN(state=state.Failed())



        # spider_kwargsで指定された引数より、scrapyを実行するための引数へ補正を行う。
        scrapy_crawling_kwargs_input = ScrapyCrawlingKwargsInput(
            kwargs['spider_kwargs'])
        # seleniumの使用有無により分けられた単位でマルチスレッド処理を実行する。
        #for separate_spiders_info in directory_search_spiders.separate_spider_using_selenium(args_spiders_name):
            # threads.append(
            #     threading.Thread(target=scrapy_crawling_run.custom_runner_run(
            #         logger=self.logger,
            #         start_time=self.start_time,
            #         scrapy_crawling_kwargs=scrapy_crawling_kwargs_input.spider_kwargs_correction(),
            #         spiders_info=separate_spiders_info)))


        thread = threading.Thread(target=scrapy_crawling_run.custom_crawl_run(
                logger=self.logger,
                start_time=self.start_time,
                scrapy_crawling_kwargs=scrapy_crawling_kwargs_input.spider_kwargs_correction(),
                spiders_info=directory_search_spiders.separate_spider_using_selenium(args_spiders_name)[0]))
        thread.start()
        thread.join()

        #for thread in threads:
        #    print('start ',thread)
        #    thread.start()
        #reac: Any = reactor
        #run.addBoth(lambda _: reac.stop())
        #reac.run()
        # 各スレッドが終了するまで待機
        # for thread in threads:
        #     print('join ',thread)
        #     thread.join()

        #reac.stop()

        if kwargs['following_processing_execution'] == 'Yes':
            # 必要な引数設定
            kwargs['start_time'] = self.start_time
            kwargs['mongo'] = self.mongo
            kwargs['domain'] = None
            kwargs['crawling_start_time_from'] = self.start_time
            kwargs['crawling_start_time_to'] = self.start_time
            kwargs['urls'] = []
            kwargs['scrapying_start_time_from'] = self.start_time
            kwargs['scrapying_start_time_to'] = self.start_time
            kwargs['scraped_save_start_time_from'] = self.start_time
            kwargs['scraped_save_start_time_to'] = self.start_time

            scrapying_run.exec(kwargs)
            scraped_news_clip_master_save_run.check_and_save(kwargs)

            ### 本格開発までsolrへの連動を一時停止 ###
            # solr_news_clip_save_run.check_and_save(kwargs)

        self.closed()

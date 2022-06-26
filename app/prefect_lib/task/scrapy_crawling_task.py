# pylint: disable=E1101
import os
import sys
from typing import Any
from prefect.engine import state
from prefect.engine.runner import ENDRUN
import threading
#from multiprocessing import Process
path = os.getcwd()
sys.path.append(path)
from common_lib.directory_search_spiders import DirectorySearchSpiders
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import scrapy_crawling_run, scrapying_run, scraped_news_clip_master_save_run, solr_news_clip_save_run
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.data_models.scrapy_crawling_kwargs_input import ScrapyCrawlingKwargsInput
#import pprint
#from twisted.internet import reactor
#from scrapy.utils.reactor import install_reactor
#install_reactor('twisted.internet.asyncioreactor.AsyncioSelectorReactor')


class ScrapyCrawlingTask(ExtensionsTask):
    '''
    クローリング用タスク
    '''

    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''
        self.run_init()

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
        for separate_spiders_info in directory_search_spiders.spiders_info_list_get(args_spiders_name):
            threads.append(
                threading.Thread(target=scrapy_crawling_run.custom_runner_run(
                    logger=self.logger,
                    start_time=self.start_time,
                    scrapy_crawling_kwargs=scrapy_crawling_kwargs_input.spider_kwargs_correction(),
                    spiders_info=separate_spiders_info)))
                # threading.Thread(target=scrapy_crawling_run.custom_crawl_run(
                #     logger=self.logger,
                #     start_time=self.start_time,
                #     scrapy_crawling_kwargs=scrapy_crawling_kwargs_input.spider_kwargs_correction(),
                #     spiders_info=separate_spiders_info)))

        '''twisted reactorの制御は難しかった、、、今後の課題とする。とりあえず以下の残骸を残しておく。'''
        # print('===\n\n １回めのスレッド実行 \n\n')
        # thread = threading.Thread(target=scrapy_crawling_run.custom_runner_run(
        #         logger=self.logger,
        #         start_time=self.start_time,
        #         scrapy_crawling_kwargs=scrapy_crawling_kwargs_input.spider_kwargs_correction(),
        #         spiders_info=directory_search_spiders.separate_spider_using_selenium(args_spiders_name)[0]))
        # thread.start()
        # thread.join()

        # print('===\n\n ２回めのスレッド実行 \n\n')
        # #install_reactor('twisted.internet.asyncioreactor.AsyncioSelectorReactor')
        # #reactor.stop()
        # thread = threading.Thread(target=scrapy_crawling_run.custom_runner_run(
        #         logger=self.logger,
        #         start_time=self.start_time,
        #         scrapy_crawling_kwargs=scrapy_crawling_kwargs_input.spider_kwargs_correction(),
        #         spiders_info=directory_search_spiders.separate_spider_using_selenium(args_spiders_name)[0]))
        # thread.start()
        # thread.join()

        # print('===\n\n １回めのプロセス実行 \n\n')
        # process = Process(target=scrapy_crawling_run.custom_crawl_run(
        #         logger=self.logger,
        #         start_time=self.start_time,
        #         scrapy_crawling_kwargs=scrapy_crawling_kwargs_input.spider_kwargs_correction(),
        #         spiders_info=directory_search_spiders.separate_spider_using_selenium(args_spiders_name)[0]))
        # process.start()
        # process.join()

        # print('===\n\n ２回めのプロセス実行 \n\n')
        # process = Process(target=scrapy_crawling_run.custom_crawl_run(
        #         logger=self.logger,
        #         start_time=self.start_time,
        #         scrapy_crawling_kwargs=scrapy_crawling_kwargs_input.spider_kwargs_correction(),
        #         spiders_info=directory_search_spiders.separate_spider_using_selenium(args_spiders_name)[0]))
        # process.start()
        # process.join()

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

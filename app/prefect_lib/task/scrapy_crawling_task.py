# pylint: disable=E1101
import os
import sys
from typing import Any
from prefect.engine import state
from prefect.engine.runner import ENDRUN
import threading
path = os.getcwd()
sys.path.append(path)
from shared.directory_search_spiders import DirectorySearchSpiders
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import scrapy_crawling_run, scrapying_run, scraped_news_clip_master_save_run, solr_news_clip_save_run
from prefect_lib.task.extentions_task import ExtensionsTask
from news_crawl.news_crawl_input import NewsCrawlInput


class ScrapyCrawlingTask(ExtensionsTask):
    '''
    クローリング用タスク
    '''

    def run(self, spider_names: list[str], spider_kwargs: dict, following_processing_execution: bool):
        '''ここがprefectで起動するメイン処理'''
        self.run_init()

        self.logger.info(f'=== Scrapy crawling run kwargs : spider_names={spider_names}, spider_kwargs={spider_kwargs}, following_processing_execution={following_processing_execution}')

        error_spider_names: list = []
        threads: list[threading.Thread] = []
        directory_search_spiders = DirectorySearchSpiders()

        # spidersディレクトリより取得した一覧に存在するかチェック
        args_spiders_name = set(spider_names)
        for args_spider_name in args_spiders_name:
            # spidersディレクトリより取得した一覧に存在するかチェック
            if not args_spider_name in directory_search_spiders.spiders_name_list_get():
                error_spider_names.append(args_spider_name)
        if len(error_spider_names):
            self.logger.error(
                f'=== scrapy crwal run : 指定されたspider_nameは存在しませんでした : {error_spider_names}')
            raise ENDRUN(state=state.Failed())

        # spider_kwargsで指定された引数にスタートタイムを追加し、scrapyを実行するための引数へ補正を行う。
        _ = dict(crawling_start_time=self.start_time)
        _.update(spider_kwargs)
        news_crawl_input = NewsCrawlInput(**_)
        # seleniumの使用有無により分けられた単位でマルチスレッド処理を実行する。
        for separate_spiders_info in directory_search_spiders.spiders_info_list_get(args_spiders_name):
            threads.append(
                threading.Thread(target=scrapy_crawling_run.custom_runner_run(
                    logger=self.logger,
                    start_time=self.start_time,
                    scrapy_crawling_kwargs=news_crawl_input.__dict__,
                    spiders_info=separate_spiders_info)))

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

        if following_processing_execution:
            # 後続のスクレイピング処理を実行
            scrapying_run.exec(
                start_time = self.start_time,
                mongo = self.mongo,
                domain = None,
                urls = [],
                target_start_time_from = self.start_time,
                target_start_time_to = self.start_time,
            )

            # 後続のスクレイプ結果のニュースクリップマスターへの保存処理を実行
            scraped_news_clip_master_save_run.check_and_save(
                start_time = self.start_time,
                mongo = self.mongo,
                domain = None,
                target_start_time_from = self.start_time,
                target_start_time_to = self.start_time,
            )

            ### 本格開発までsolrへの連動を一時停止 ###
            # solr_news_clip_save_run.check_and_save(kwargs)

        self.closed()

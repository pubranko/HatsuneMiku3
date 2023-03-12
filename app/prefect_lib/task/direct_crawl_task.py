import os
import sys
import threading
from prefect.engine import state
from prefect.engine.runner import ENDRUN
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import scrapy_crawling_run
from shared.directory_search_spiders import DirectorySearchSpiders
from shared.settings import DATA_DIR__DIRECT_CRAWL_FILES_DIR
# from prefect_lib.data_models.scrapy_crawling_kwargs_input import ScrapyCrawlingKwargsInput
from news_crawl.news_crawl_input import NewsCrawlInput


class DirectCrawlTask(ExtensionsTask):
    '''
    '''
    def run(self, **kwargs):
        '''Direct Crawl Task'''
        self.run_init()

        kwargs['start_time'] = self.start_time
        kwargs['logger'] = self.logger
        self.logger.info(f'=== Direct crwal run kwargs : {str(kwargs)}')

        spider_name: str = kwargs['spider_name']
        file: str = kwargs['file']
        file_path: str = os.path.join(DATA_DIR__DIRECT_CRAWL_FILES_DIR, file)
        urls: list = []
        if os.path.exists(file_path):
            with open(file_path) as f:
                urls = [line for line in f.readlines()]
        else:
            self.logger.error(
                f'=== Direct crwal run : 指定されたファイルは存在しませんでした : {file_path}')
            raise ENDRUN(state=state.Failed())
        if len(urls) == 0:
            self.logger.error(
                f'=== Direct crwal run : 指定されたファイルが空です : {file_path}')
            raise ENDRUN(state=state.Failed())

        directory_search_spiders = DirectorySearchSpiders()
        if not spider_name in directory_search_spiders.spiders_name_list_get():
            self.logger.error(
                f'=== Direct crwal run : 指定されたspider_nameは存在しませんでした : {spider_name}')
            raise ENDRUN(state=state.Failed())

        spider_info = directory_search_spiders.spiders_info[spider_name]

        # spider_kwargsで指定された引数より、scrapyを実行するための引数へ補正を行う。
        # scrapy_crawling_kwargs_input = ScrapyCrawlingKwargsInput({
        #     'direct_crawl_urls':urls,})
        # kwargs['crawling_start_time'] = self.start_time
        # kwargs['direct_crawl_urls'] = urls
        news_crawl_input = NewsCrawlInput(**dict(
            crawling_start_time = self.start_time,
            direct_crawl_urls = urls,
        ))
        # news_crawl_input.DIRECT_CRAWL_URLS: urls,

        # news_crawl_input = NewsCrawlInput(**kwargs)


        self.logger.info(f'=== ダイレクト 対象スパイダー : {spider_name}')
        self.logger.info(f'=== ダイレクト run kwargs : {news_crawl_input.__dict__}')

        thread = threading.Thread(
            target=scrapy_crawling_run.custom_runner_run(
                logger=self.logger,
                start_time=self.start_time,
                # scrapy_crawling_kwargs=scrapy_crawling_kwargs_input.spider_kwargs_correction(),
                scrapy_crawling_kwargs=news_crawl_input.__dict__,
                spiders_info=[spider_info]))

        # マルチプロセスで動いているScrapyの終了を待ってから後続の処理を実行する。
        thread.start()
        thread.join()

        self.closed()

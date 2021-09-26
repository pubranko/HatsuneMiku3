import os
import sys
from logging import Logger
from prefect.engine import state
from prefect.engine.runner import ENDRUN
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import scrapy_crawling_run
from common_lib.directory_search import directory_search



class DirectCrawlTask(ExtensionsTask):
    '''
    '''
    def run(self, **kwargs):
        '''Direct Crawl Task'''

        logger: Logger = self.logger
        kwargs['start_time'] = self.start_time
        kwargs['logger'] = self.logger
        logger.info('=== Direct crwal run kwargs : ' + str(kwargs))

        spider_name: str = kwargs['spider_name']
        file: str = kwargs['file']
        file_path: str = os.path.join('static', 'direct_crawl_files', file)
        urls: list = []
        if os.path.exists(file_path):
            with open(file_path) as f:
                urls = [line for line in f.readlines()]
        else:
            logger.error(
                '=== Direct crwal run : 指定されたファイルは存在しませんでした : ' + file_path)
            raise ENDRUN(state=state.Failed())
        if len(urls) == 0:
            logger.error(
                '=== Direct crwal run : 指定されたファイルが空です : ' + file_path)
            raise ENDRUN(state=state.Failed())
        kwargs['urls'] = urls

        error_flg: bool = True
        spiders_info: list = directory_search()
        for spider in spiders_info:
            if spider['spider_name'] == spider_name:
                kwargs['class_instans'] = spider['class_instans']
                scrapy_crawling_run.direct_crawl_run(kwargs)
                error_flg: bool = False

        if error_flg:
            logger.error(
                '=== Direct crwal run : 指定されたspider_nameは存在しませんでした : ' + spider_name)
            raise ENDRUN(state=state.Failed())

        self.closed()
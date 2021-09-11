import os
import sys
from logging import Logger
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import solr_news_clip_save_run
from prefect_lib.run import direct_crawl_run
from common_lib.directory_search import directory_search

from prefect.engine import state
from prefect.engine.runner import ENDRUN


class DirectCrawlTask(ExtensionsTask):
    '''
    '''

    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''

        logger: Logger = self.logger
        kwargs['start_time'] = self.start_time

        spider_name: str = kwargs['spider_name']
        file: str = kwargs['file']

        file_path: str = os.path.join('static', 'direct_crawl_files', file)

        urls: list = []
        if os.path.exists(file_path):
            with open(file_path) as f:
                for line in f.readlines():
                    urls.append(line)
        else:
            logger.error(
                '=== Direct crwal run : 指定されたファイルは存在しませんでした : ' + file_path)
            raise ENDRUN(state=state.Failed())
        if len(urls) == 0:
            logger.error(
                '=== Direct crwal run : 指定されたファイルが空です : ' + file_path)
            raise ENDRUN(state=state.Failed())

        logger.info('=== Direct crwal run kwargs : ' + str(kwargs))
        # solr_news_clip_save_run.check_and_save(kwargs)

        error_flg: bool = True

        spiders: list = directory_search()
        for spider in spiders:
            if spider['spider_name'] == spider_name:
                #mod: Any = import_module(module)
                #mod = spider['class_instans']
                #getattr(mod, method)(self.start_time)

                direct_crawl_run.exec(
                    self.start_time, urls, spider['class_instans'])

                error_flg: bool = False

            # spider['class_name']
            # spider['class_instans']
            # spider['domain']
            # spider['spider_name']

        if error_flg:
            logger.error(
                '=== Direct crwal run : 指定されたspider_nameは存在しませんでした : ' + spider_name)
            raise ENDRUN(state=state.Failed())

        self.closed()

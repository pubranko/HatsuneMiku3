from prefect_lib.run import crawl_urls_sync_check_run
from prefect_lib.task.extentions_task import ExtensionsTask
import os
import sys
from datetime import datetime
from typing import Final
path = os.getcwd()
sys.path.append(path)


class CrawlUrlsSyncCheckTask(ExtensionsTask):
    '''あとで'''

    ################
    # 定数
    ################

    DOMAIN: Final[str] = 'domain'
    '''定数: domain'''
    START_TIME_FROM: Final[str] = 'start_time_from'
    '''定数: start_time_from'''
    START_TIME_TO: Final[str] = 'start_time_to'
    '''定数: start_time_to'''

    # def run(self, **kwargs):
    def run(self, domain: str, start_time_from: datetime, start_time_to: datetime):
        ''''''
        self.run_init()

        # kwargs['start_time'] = self.start_time
        # kwargs['mongo'] = self.mongo
        # kwargs[ExtensionsTask.START_TIME] = self.start_time
        # kwargs[ExtensionsTask.MONGO] = self.mongo
        self.logger.info(
            f'=== CrawlUrlsSyncCheckTask run kwargs : domain: {domain}, start_time_from: {start_time_from}, start_time_to: {start_time_to}')

        # crawl_urls_sync_check_run.check(kwargs)
        crawl_urls_sync_check_run.check(
            start_time=self.start_time,
            mongo=self.mongo,
            domain=domain,
            start_time_from=start_time_from,
            start_time_to=start_time_to,
        )

        # 終了処理
        self.closed()
        # return ''

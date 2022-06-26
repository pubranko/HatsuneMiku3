import os
import sys
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import crawl_urls_sync_check_run


class CrawlUrlsSyncCheckTask(ExtensionsTask):
    ''''''

    def run(self, **kwargs):
        ''''''
        self.run_init()

        kwargs['start_time'] = self.start_time
        kwargs['mongo'] = self.mongo
        self.logger.info('=== CrawlUrlsSyncCheckTask run kwargs : ' + str(kwargs))

        crawl_urls_sync_check_run.check(kwargs)

        # 終了処理
        self.closed()
        # return ''

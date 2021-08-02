# 単一プロセスでcrawlerprocessを使った例（クラスバージョン）
import os
import sys
path = os.getcwd()
sys.path.append(path)
from prefect_flow.common_module.regular_crawler_run import regular_crawler_run
from prefect_flow.common_module.extentions_crawl_task import ExtensionsCrawlTask


class RegularObservationTask(ExtensionsCrawlTask):
    ''''定期観測クロール'''

    log_file_path = os.path.join(
        'logs', 'regular_observation_spider.log')

    def run(self,):
        '''ここがprefectで起動するメイン処理'''
        #raise signals.FAIL(message="Failed!! テストです")

        regular_crawler_run(self.crawl_start_time)

        self.end()

        #return self.crawl_start_time

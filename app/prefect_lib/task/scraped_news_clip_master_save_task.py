import os
import sys
from datetime import datetime
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import scraped_news_clip_master_save_run
from prefect_lib.data_models.scrapying_input import ScrapyingInput


class ScrapedNewsClipMasterSaveTask(ExtensionsTask):
    '''
    スクレイプ結果の保存タスク
    '''
    def run(self,
            domain: str,
            target_start_time_from: datetime,
            target_start_time_to: datetime,
            urls: list[str],                            # 現在未使用
            following_processing_execution: bool):      # 現在未使用
        '''あとで'''

        self.run_init()
        scrapying_input = ScrapyingInput(
            domain = domain,
            target_start_time_from = target_start_time_from,
            target_start_time_to = target_start_time_to,
            urls=urls,
            following_processing_execution=following_processing_execution)

        self.logger.info(f'=== Scraped Save run kwargs : \
                            {scrapying_input.DOMAIN} = {domain}, \
                            {scrapying_input.TARGET_START_TIME_FROM} = {target_start_time_from}, \
                            {scrapying_input.TARGET_START_TIME_TO} = {target_start_time_from}, \
                            {scrapying_input.URLS} = {urls}, \
                            {scrapying_input.FOLLOWING_PROCESSING_EXECUTION} = {following_processing_execution}')


        scraped_news_clip_master_save_run.check_and_save(
            start_time = self.start_time,
            mongo = self.mongo,
            domain = domain,
            target_start_time_from = target_start_time_from,
            target_start_time_to = target_start_time_to)

        # 終了処理
        self.closed()
        # return ''

import os
import sys
from datetime import datetime
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import scrapying_run, scraped_news_clip_master_save_run, solr_news_clip_save_run
from prefect_lib.data_models.scrapying_input import ScrapyingInput


class ScrapyingTask(ExtensionsTask):
    '''
    スクレイピング用タスク
    '''

    def run(self,
            domain: str,
            target_start_time_from: datetime,
            target_start_time_to: datetime,
            urls: list[str],
            following_processing_execution: bool):
        '''あとで'''
        self.run_init()

        scrapying_input = ScrapyingInput(
            domain = domain,
            target_start_time_from = target_start_time_from,
            target_start_time_to = target_start_time_to,
            urls=urls,
            following_processing_execution=following_processing_execution)

        self.logger.info(f'=== ScrapyingTask run kwargs : \
                            {scrapying_input.DOMAIN} = {domain}, \
                            {scrapying_input.TARGET_START_TIME_FROM} = {target_start_time_from}, \
                            {scrapying_input.TARGET_START_TIME_TO} = {target_start_time_from}, \
                            {scrapying_input.URLS} = {urls}, \
                            {scrapying_input.FOLLOWING_PROCESSING_EXECUTION} = {following_processing_execution}')

        # スクレイピング処理を実行
        scrapying_run.exec(
            start_time = self.start_time,
            mongo = self.mongo,
            domain = domain,
            urls = urls,
            target_start_time_from = target_start_time_from,
            target_start_time_to = target_start_time_to)

        if following_processing_execution:
            # 後続のスクレイプ結果のニュースクリップマスターへの保存処理を実行
            scraped_news_clip_master_save_run.check_and_save(
                # kwargs
                start_time = self.start_time,
                mongo = self.mongo,
                domain = None,
                target_start_time_from = target_start_time_from,
                target_start_time_to = target_start_time_to)
            ### 本格開発までsolrへの連動を一時停止 ###
            # solr_news_clip_save_run.check_and_save(kwargs)

        # 終了処理
        self.closed()
        # return ''

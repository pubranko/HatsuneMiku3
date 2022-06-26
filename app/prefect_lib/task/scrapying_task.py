import os
import sys
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import scrapying_run, scraped_news_clip_master_save_run, solr_news_clip_save_run


class ScrapyingTask(ExtensionsTask):
    '''
    スクレイピング用タスク
    '''

    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''
        self.run_init()

        kwargs['start_time'] = self.start_time
        kwargs['mongo'] = self.mongo

        self.logger.info('=== ScrapyingTask run kwargs : ' + str(kwargs))

        scrapying_run.exec(kwargs)

        if kwargs['following_processing_execution'] == 'Yes':
            # 必要な引数設定
            kwargs['scrapying_start_time_from'] = self.start_time
            kwargs['scrapying_start_time_to'] = self.start_time
            kwargs['scraped_save_start_time_from'] = self.start_time
            kwargs['scraped_save_start_time_to'] = self.start_time

            scraped_news_clip_master_save_run.check_and_save(kwargs)
            ### 本格開発までsolrへの連動を一時停止 ###
            # solr_news_clip_save_run.check_and_save(kwargs)

        # 終了処理
        self.closed()
        # return ''

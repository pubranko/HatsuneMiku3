import os
import sys
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import scraped_news_clip_master_save_run

class ScrapedNewsClipMasterSaveTask(ExtensionsTask):
    '''
    スクレイプ結果の保存タスク
    '''

    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''
        self.run_init()

        kwargs['mongo'] = self.mongo
        kwargs['start_time'] = self.start_time
        self.logger.info('=== Scraped Save run kwargs : ' + str(kwargs))
        scraped_news_clip_master_save_run.check_and_save(kwargs)

        # 終了処理
        self.closed()
        # return ''

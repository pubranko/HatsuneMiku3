import os
import sys
from logging import Logger
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import scraped_news_clip_master_save_run
from models.mongo_model import MongoModel
from models.scraped_from_response_model import ScrapedFromResponseModel
from models.crawler_response_model import CrawlerResponseModel
from models.news_clip_master_model import NewsClipMasterModel

class ScrapedNewsClipMasterSaveTask(ExtensionsTask):
    '''
    スクレイプ結果の保存タスク
    '''

    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''

        logger: Logger = self.logger
        #mongo: MongoModel = self.mongo
        kwargs['mongo'] = self.mongo
        kwargs['start_time'] = self.start_time
        # kwargs['scraped_from_response'] = ScrapedFromResponseModel(mongo)
        # kwargs['crawler_response'] = CrawlerResponseModel(mongo)
        # kwargs['news_clip_master'] = NewsClipMasterModel(mongo)
        logger.info('=== Scraped Save run kwargs : ' + str(kwargs))
        scraped_news_clip_master_save_run.check_and_save(kwargs)

        # 終了処理
        self.closed()
        # return ''

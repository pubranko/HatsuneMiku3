import os
import sys
from logging import Logger
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import scrapying_run
from models.mongo_model import MongoModel
from models.crawler_response_model import CrawlerResponseModel
from models.scraped_from_response_model import ScrapedFromResponse
from models.controller_model import ControllerModel


class ScrapyingTask(ExtensionsTask):
    '''
    スクレイピング用タスク
    '''
    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''

        kwargs['start_time'] = self.start_time
        kwargs['mongo'] = self.mongo
        mongo: MongoModel = kwargs['mongo']
        kwargs['crawler_response'] = CrawlerResponseModel(mongo)
        kwargs['scraped_from_response'] = ScrapedFromResponse(mongo)
        kwargs['controller'] = ControllerModel(mongo)

        logger: Logger = self.logger
        logger.info('=== ScrapyingTask run kwargs : ' + str(kwargs))

        scrapying_run.exec(kwargs)

        # 終了処理
        self.closed()
        # return ''
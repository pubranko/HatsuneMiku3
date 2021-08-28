import os
import sys
from logging import Logger
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run.news_clip_master_save import check_and_save
from prefect_lib.run.solr_news_clip_save import check_and_save
from models.mongo_model import MongoModel
from models.news_clip_master_model import NewsClipMaster


class SolrNewsClipSaveTask(ExtensionsTask):
    '''
    スクレイプ結果の保存タスク
    '''

    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''

        logger: Logger = self.logger
        mongo: MongoModel = self.mongo

        kwargs['starting_time'] = self.starting_time
        kwargs['news_clip_master'] = NewsClipMaster(mongo)

        logger.info('=== Scrapyed Save run kwargs : ' + str(kwargs))
        check_and_save(kwargs)

        self.closed()

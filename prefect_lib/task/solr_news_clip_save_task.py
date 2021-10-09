import os
import sys
from logging import Logger
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import solr_news_clip_save_run
from models.mongo_model import MongoModel
from models.news_clip_master_model import NewsClipMasterModel


class SolrNewsClipSaveTask(ExtensionsTask):
    '''
    スクレイプ結果の保存タスク
    '''

    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''

        logger: Logger = self.logger
        #mongo: MongoModel = self.mongo
        kwargs['mongo'] = self.mongo
        kwargs['start_time'] = self.start_time
        #kwargs['news_clip_master'] = NewsClipMasterModel(mongo)

        logger.info('=== Scrapyed Save run kwargs : ' + str(kwargs))
        solr_news_clip_save_run.check_and_save(kwargs)

        self.closed()

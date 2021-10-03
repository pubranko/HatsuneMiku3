import os
import sys
from logging import Logger
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import crawl_urls_sync_check_run
from models.mongo_model import MongoModel
# from models.crawler_logs_model import CrawlerLogsModel
# from models.crawler_response_model import CrawlerResponseModel
# from models.news_clip_master_model import NewsClipMaster
# from models.solr_news_clip_model import SolrNewsClip



class CrawlUrlsSyncCheckTask(ExtensionsTask):
    '''
    '''
    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''

        kwargs['start_time'] = self.start_time
        kwargs['mongo'] = self.mongo
        # mongo: MongoModel = kwargs['mongo']
        # kwargs['crawler_logs'] = CrawlerLogsModel(mongo)
        # kwargs['crawler_response'] = CrawlerResponseModel(mongo)
        # kwargs['news_clip_master'] = NewsClipMaster(mongo)
        # kwargs['solr_news_clip'] = SolrNewsClip(mongo)

        logger: Logger = self.logger
        logger.info('=== CrawlUrlsSyncCheckTask run kwargs : ' + str(kwargs))

        #scrapying_run.exec(kwargs)
        crawl_urls_sync_check_run.check(kwargs)

        # 終了処理
        self.closed()
        # return ''
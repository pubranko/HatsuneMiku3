import os
import sys
import pickle
from typing import Any, Union
from logging import Logger
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pymongo import ASCENDING
from pymongo.cursor import Cursor
from prefect.engine import state
from prefect.engine.runner import ENDRUN
path = os.getcwd()
sys.path.append(path)
from prefect_lib.settings import TIMEZONE
from prefect_lib.task.extentions_task import ExtensionsTask
from models.crawler_response_model import CrawlerResponseModel
from models.scraped_from_response_model import ScrapedFromResponseModel
from models.news_clip_master_model import NewsClipMasterModel
from models.crawler_logs_model import CrawlerLogsModel
from models.controller_model import ControllerModel
from models.asynchronous_report_model import AsynchronousReportModel


class DailyClearTask(ExtensionsTask):
    '''
    '''

    def run(self, **kwargs):
        ''''''
        def delete_non_filter(collection_name: str, collection: Union[ScrapedFromResponseModel],) -> None:
            delete_count: int = collection.delete_many(filter={})
            logger.info(f'=== DailyClearTask run delete_non_filter : 削除件数({collection_name}) : {delete_count}件')

        ###################################################################################
        logger: Logger = self.logger
        collections_name: list = ['scraped_from_response']
        collection = ScrapedFromResponseModel(self.mongo)

        for collection_name in collections_name:
            collection = ScrapedFromResponseModel(self.mongo)
            # データ全削除
            delete_non_filter(
                collection_name, collection)

        # 終了処理
        self.closed()
        # return ''

import os
import sys
import pickle
from typing import Any, Union
from logging import Logger
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pymongo import ASCENDING
from pymongo.cursor import Cursor
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


class MonthlyDeleteTask(ExtensionsTask):
    '''
    一定月数経過したデータを月単位で削除する。
    引数(base_month)の指定がなければ３ヶ月前のデータが削除対象となる。
    '''

    def run(self, **kwargs):
        def delete_all(
            collection_name: str,
            collection: Union[ControllerModel],
        ):
            '''コレクション内のデータを全て削除する。'''
            delete_count = collection.delete_many({})
            logger.info(
                f'=== {collection_name} 削除対象件数 : {str(delete_count)}')

        def delete_time_filter(
            collection_name: str,
            delete_time_from: datetime,
            delete_time_to: datetime,
            conditions_field: str,
            collection: Union[CrawlerResponseModel, ScrapedFromResponseModel,
                              NewsClipMasterModel, CrawlerLogsModel, AsynchronousReportModel],
        ):
            '''指定された期間のデータを削除する。'''
            conditions: list = []
            conditions.append(
                {conditions_field: {'$gte': delete_time_from}})
            conditions.append(
                {conditions_field: {'$lte': delete_time_to}})

            filter: Any = {'$and': conditions}
            logger.info(f'=== {collection_name} へのfilter: {str(filter)}')

            delete_count = collection.delete_many(filter=filter)
            logger.info(
                f'=== {collection_name} 削除対象件数 : {str(delete_count)}')

        #####################################################
        logger: Logger = self.logger
        logger.info(f'=== Monthly delete task run kwargs : {str(kwargs)}')

        collections_name: list = kwargs['collections_name']
        # base_monthの指定がなかった場合は自動補正
        if kwargs['base_month']:
            base_yyyy: int = int(str(kwargs['base_month'])[0:4])
            base_mm: int = int(str(kwargs['base_month'])[5:7])
        else:
            previous_month = date.today() - relativedelta(months=3) # ３ヶ月前
            base_yyyy: int = int(previous_month.strftime('%Y'))
            base_mm: int = int(previous_month.strftime('%m'))

        delete_time_from: datetime = datetime(
            base_yyyy, base_mm, 1, 0, 0, 0).astimezone(TIMEZONE)
        delete_time_to: datetime = datetime(
            base_yyyy, base_mm, 1, 23, 59, 59, 999999).astimezone(TIMEZONE) + relativedelta(day=99)

        for collection_name in collections_name:
            if collection_name == 'crawler_response':
                conditions_field = 'crawling_start_time'
                collection = CrawlerResponseModel(self.mongo)
                delete_time_filter(
                    collection_name, delete_time_from, delete_time_to, conditions_field, collection)

            elif collection_name == 'scraped_from_response':
                conditions_field = 'scrapying_start_time'
                collection = ScrapedFromResponseModel(self.mongo)
                delete_time_filter(
                    collection_name, delete_time_from, delete_time_to, conditions_field, collection)

            elif collection_name == 'news_clip_master':
                conditions_field = 'scraped_save_start_time'
                collection = NewsClipMasterModel(self.mongo)
                delete_time_filter(
                    collection_name, delete_time_from, delete_time_to, conditions_field, collection)

            elif collection_name == 'crawler_logs':
                conditions_field = 'start_time'
                collection = CrawlerLogsModel(self.mongo)
                delete_time_filter(
                    collection_name, delete_time_from, delete_time_to, conditions_field, collection)

            elif collection_name == 'asynchronous_report':
                conditions_field = 'start_time'
                collection = AsynchronousReportModel(self.mongo)
                delete_time_filter(
                    collection_name, delete_time_from, delete_time_to, conditions_field, collection)

            elif collection_name == 'controller':
                collection = ControllerModel(self.mongo)
                delete_all(collection_name, collection)

        # 終了処理
        self.closed()
        # return ''

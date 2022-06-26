import os
import sys
from typing import Any, Union
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
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
        def delete_exec(
            collection_name: str,
            filter: Any,
            collection: Union[CrawlerResponseModel, ScrapedFromResponseModel, ControllerModel,
                              NewsClipMasterModel, CrawlerLogsModel, AsynchronousReportModel],
        ):
            self.logger.info(
                f'=== {collection_name} 削除条件 : {filter}')
            delete_count = collection.delete_many(filter=filter)
            self.logger.info(
                f'=== {collection_name} 削除対象件数 : {str(delete_count)}')

        #####################################################
        self.run_init()

        self.logger.info(f'=== Monthly delete task run kwargs : {str(kwargs)}')

        collections_name: list = kwargs['collections_name']
        # base_monthの指定がなかった場合は自動補正
        # if kwargs['base_month']:
        #     base_yyyy: int = int(str(kwargs['base_month'])[0:4])
        #     base_mm: int = int(str(kwargs['base_month'])[5:7])
        # else:
        #     previous_month = date.today() - relativedelta(months=3)  # ３ヶ月前
        #     base_yyyy: int = int(previous_month.strftime('%Y'))
        #     base_mm: int = int(previous_month.strftime('%m'))

        # delete_time_from: datetime = datetime(
        #     base_yyyy, base_mm, 1, 0, 0, 0).astimezone(TIMEZONE)
        # delete_time_to: datetime = datetime(
        #     base_yyyy, base_mm, 1, 23, 59, 59, 999999).astimezone(TIMEZONE) + relativedelta(day=99)

        #
        delete_time_from: datetime = datetime.min
        delete_time_to: datetime = datetime.max
        if kwargs['delete_period_from']:
            from_yyyy: int = int(str(kwargs['delete_period_from'])[0:4])
            from_mm: int = int(str(kwargs['delete_period_from'])[5:7])
            delete_time_from: datetime = datetime(
                from_yyyy, from_mm, 1, 0, 0, 0).astimezone(TIMEZONE)
        if kwargs['delete_period_to']:
            to_yyyy: int = int(str(kwargs['delete_period_to'])[0:4])
            to_mm: int = int(str(kwargs['delete_period_to'])[5:7])
            delete_time_to: datetime = datetime(
                to_yyyy, to_mm, 1, 23, 59, 59, 999999).astimezone(TIMEZONE) + relativedelta(day=99)

        # 削除期間のfrom/toの両方指定がない（本番運用）
        # 現在の３ヶ月前の１ヶ月分を削除期間とする。
        if kwargs['delete_period_from'] == None and kwargs['delete_period_to'] == None:
            previous_month = date.today() - relativedelta(months=3)  # ３ヶ月前
            from_yyyy = int(previous_month.strftime('%Y'))
            from_mm = int(previous_month.strftime('%m'))
            to_yyyy = int(previous_month.strftime('%Y'))
            to_mm = int(previous_month.strftime('%m'))
            delete_time_from: datetime = datetime(
                from_yyyy, from_mm, 1, 0, 0, 0).astimezone(TIMEZONE)
            delete_time_to: datetime = datetime(
                to_yyyy, to_mm, 1, 23, 59, 59, 999999).astimezone(TIMEZONE) + relativedelta(day=99)

        collection = None
        for collection_name in collections_name:
            conditions: list = []
            if collection_name == 'crawler_response':
                collection = CrawlerResponseModel(self.mongo)
                conditions.append(
                    {'crawling_start_time': {'$gte': delete_time_from}})
                conditions.append(
                    {'crawling_start_time': {'$lte': delete_time_to}})

            elif collection_name == 'scraped_from_response':
                collection = ScrapedFromResponseModel(self.mongo)
                conditions.append(
                    {'scrapying_start_time': {'$gte': delete_time_from}})
                conditions.append(
                    {'scrapying_start_time': {'$lte': delete_time_to}})

            elif collection_name == 'news_clip_master':
                collection = NewsClipMasterModel(self.mongo)
                conditions.append(
                    {'scraped_save_start_time': {'$gte': delete_time_from}})
                conditions.append(
                    {'scraped_save_start_time': {'$lte': delete_time_to}})

            elif collection_name == 'crawler_logs':
                collection = CrawlerLogsModel(self.mongo)
                conditions.append(
                    {'start_time': {'$gte': delete_time_from}})
                conditions.append(
                    {'start_time': {'$lte': delete_time_to}})

            elif collection_name == 'asynchronous_report':
                collection = AsynchronousReportModel(self.mongo)
                conditions.append(
                    {'start_time': {'$gte': delete_time_from}})
                conditions.append(
                    {'start_time': {'$lte': delete_time_to}})

            elif collection_name == 'controller':
                collection = ControllerModel(self.mongo)

            if collection:
                filter: Any = {'$and': conditions} if conditions else {}
                delete_exec(collection_name, filter, collection)

        # 終了処理
        self.closed()
        # return ''

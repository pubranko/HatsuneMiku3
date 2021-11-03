import os
import sys
import pickle
from typing import Any, Union
from logging import Logger
from datetime import datetime
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


class MongoExportSelectorTask(ExtensionsTask):
    '''
    '''

    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''

        def export_non_filter(
            collection_name: str,
            collection: Union[ControllerModel],
            file_path: str,
        ):

            # エクスポート対象件数を確認
            record_count = collection.find().count()
            logger.info('=== ' + collection_name + ' バックアップ対象件数 : ' +
                        str(record_count))

            # 100件単位で処理を実施
            limit: int = 100
            skip_list = list(range(0, record_count, limit))

            # ファイルにリストオブジェクトを追記していく
            for skip in skip_list:
                records: Cursor = collection.find().skip(skip).limit(limit)
                record_list: list = [record for record in records]

                with open(file_path, 'ab') as file:
                    file.write(pickle.dumps(record_list))

        #####################################################
        def export_time_filter(
            collection_name: str,
            conditions_field: str,
            sort_parameter: list,
            collection: Union[CrawlerResponseModel, ScrapedFromResponseModel,
                              NewsClipMasterModel, CrawlerLogsModel, AsynchronousReportModel],
            file_path: str,
        ):

            conditions: list = []
            if start_time_from:
                conditions.append(
                    {conditions_field: {'$gte': start_time_from}})
            if start_time_to:
                conditions.append(
                    {conditions_field: {'$lte': start_time_to}})

            if conditions:
                filter: Any = {'$and': conditions}
            else:
                filter = None
            logger.info('=== ' + collection_name + ' へのfilter: ' + str(filter))

            # エクスポート対象件数を確認
            record_count = collection.find(
                filter=filter,
            ).count()
            logger.info('=== ' + collection_name + ' バックアップ対象件数 : ' +
                        str(record_count))

            # 100件単位で処理を実施
            limit: int = 100
            skip_list = list(range(0, record_count, limit))

            # ファイルにリストオブジェクトを追記していく
            for skip in skip_list:
                records: Cursor = collection.find(
                    filter=filter,
                    sort=sort_parameter,
                ).skip(skip).limit(limit)
                record_list: list = [record for record in records]

                with open(file_path, 'ab') as file:
                    file.write(pickle.dumps(record_list))

        #####################################################
        logger: Logger = self.logger
        logger.info('=== MongoExportSelector run kwargs : ' + str(kwargs))

        collections_name: list = kwargs['collections_name']
        # 月初、月末
        _ = str(kwargs['backup_yyyymm']).split('-')
        start_time_from: datetime = datetime(
            int(_[0]), int(_[1]), 1, 0, 0, 0).astimezone(TIMEZONE)
        start_time_to: datetime = datetime(
            int(_[0]), int(_[1]), 1, 23, 59, 59, 999999).astimezone(TIMEZONE) + relativedelta(day=99)

        for collection_name in collections_name:

            # ファイル名 ＝ コレクション名_現在日時
            file_path: str = os.path.join(
                'backup_files', collection_name + '-(' + str(kwargs['backup_yyyymm']) + ')-' + self.start_time.strftime('%Y%m%d_%H%M%S'))

            if collection_name == 'crawler_response':
                conditions_field = 'crawling_start_time'
                collection = CrawlerResponseModel(self.mongo)
                sort_parameter = [('response_time', ASCENDING)]
                export_time_filter(
                    collection_name, conditions_field, sort_parameter, collection, file_path)

            elif collection_name == 'scraped_from_response':
                conditions_field = 'scrapying_start_time'
                collection = ScrapedFromResponseModel(self.mongo)
                sort_parameter = []
                export_time_filter(
                    collection_name, conditions_field, sort_parameter, collection, file_path)

            elif collection_name == 'news_clip_master':
                conditions_field = 'scraped_save_start_time'
                collection = NewsClipMasterModel(self.mongo)
                sort_parameter = [('response_time', ASCENDING)]
                export_time_filter(
                    collection_name, conditions_field, sort_parameter, collection, file_path)

            elif collection_name == 'crawler_logs':
                conditions_field = 'start_time'
                collection = CrawlerLogsModel(self.mongo)
                sort_parameter = []
                export_time_filter(
                    collection_name, conditions_field, sort_parameter, collection, file_path)

            elif collection_name == 'asynchronous_report':
                conditions_field = 'start_time'
                collection = AsynchronousReportModel(self.mongo)
                sort_parameter = []
                export_time_filter(
                    collection_name, conditions_field, sort_parameter, collection, file_path)

            elif collection_name == 'controller':
                collection = ControllerModel(self.mongo)
                export_non_filter(
                    collection_name, collection, file_path)

        # 終了処理
        self.closed()
        # return ''

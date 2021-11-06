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


class MongoImportSelectorTask(ExtensionsTask):
    '''
    '''

    def run(self, **kwargs):
        ''''''
        def delete_before_importing_non_filter(collection_name: str, collection: Union[ControllerModel],) -> None:
            delete_count: int = collection.delete_many(filter={})
            logger.info('=== MongoImportSelectorTask run delete_before_importing_non_filter : インポート前削除件数(%s) : %s件' % (
                collection_name, str(delete_count)))

        def delete_before_importing_time_filter(
            collection_name: str,
            reference_month: str,
            conditions_field: str,
            collection: Union[CrawlerResponseModel, ScrapedFromResponseModel,
                              NewsClipMasterModel, CrawlerLogsModel, AsynchronousReportModel],
        ) -> None:
            yyyy = int(reference_month[1:5])
            mm = int(reference_month[6:8])
            print('reference_month :', reference_month, ' ', yyyy, ' ', mm)
            beginning_of_month = datetime(yyyy, mm, 1, 0, 0, 0)
            end_of_month = datetime(
                yyyy, mm, 1, 23, 59, 59, 999999) + relativedelta(day=99)

            conditions: list = []
            conditions.append(
                {conditions_field: {'$gte': beginning_of_month}})
            conditions.append(
                {conditions_field: {'$lte': end_of_month}})
            filter: Any = {'$and': conditions}

            delete_count: int = collection.delete_many(filter)

            logger.info('=== MongoImportSelectorTask run delete_before_importing : インポート前削除件数(%s) : %s件' % (
                collection_name, str(delete_count)))

        ###################################################################################
        logger: Logger = self.logger
        logger.info('=== MongoImportSelectorTask run kwargs : ' + str(kwargs))

        collections_name: list = kwargs['collections_name']
        time_stamp_from: datetime = kwargs['time_stamp_from']
        time_stamp_to: datetime = kwargs['time_stamp_to']

        # インポート元ファイルの一覧を作成
        import_files_info: list = []
        file_list: list = os.listdir('backup_files')
        for file in file_list:
            temp: list = file.split('@', )
            import_files_info.append({
                'file': file,
                'collection_name': temp[0],
                'reference_month': temp[1],
                'time_stamp': datetime.strptime(temp[2], '%Y%m%d_%H%M%S').astimezone(TIMEZONE)
            })

        # 抽出条件を満たすファイルの一覧を作成
        select_files_info: list = []
        for import_file_info in import_files_info:
            select_flg = True

            # コレクションに指定がある場合、指定されたコレクション以外は対象外とする。
            if len(collections_name):
                if not import_file_info['collection_name'] in collections_name:
                    select_flg = False

            # バックアップのタイムスタンプの期間指定がある場合、その期間外は対象外とする。
            if time_stamp_from:
                if time_stamp_from > import_file_info['time_stamp']:
                    select_flg = False
            if time_stamp_to:
                if time_stamp_to < import_file_info['time_stamp']:
                    select_flg = False

            if select_flg:
                select_files_info.append(import_file_info)

        select_file_list = [_['file'] for _ in select_files_info]
        logger.info(
            '=== MongoImportSelectorTask run : インポート対象ファイル : ' + str(select_file_list))

        # ファイルからオブジェクトを復元しリストに保存。ただし"_id"は削除する。
        if len(select_files_info) > 0:
            for select_file in select_files_info:
                collection_records: list = []
                file_path: str = os.path.join(
                    'backup_files', select_file['file'])

                with open(file_path, 'rb') as file:
                    documents: list = pickle.loads(file.read())
                    for document in documents:
                        del document['_id']
                        collection_records.append(document)

                # filter
                collection = None
                conditions_field: str = ''
                if select_file['collection_name'] == 'crawler_response':
                    collection = CrawlerResponseModel(self.mongo)
                    conditions_field = 'crawling_start_time'
                elif select_file['collection_name'] == 'scraped_from_response':
                    collection = ScrapedFromResponseModel(self.mongo)
                    conditions_field = 'scrapying_start_time'
                elif select_file['collection_name'] == 'news_clip_master':
                    collection = NewsClipMasterModel(self.mongo)
                    conditions_field = 'scraped_save_start_time'
                elif select_file['collection_name'] == 'crawler_logs':
                    collection = CrawlerLogsModel(self.mongo)
                    conditions_field = 'start_time'
                elif select_file['collection_name'] == 'asynchronous_report':
                    collection = AsynchronousReportModel(self.mongo)
                    conditions_field = 'start_time'

                if collection:
                    # インポート対象期間のデータがあれば削除する。
                    delete_before_importing_time_filter(
                        select_file['collection_name'], select_file['reference_month'], conditions_field, collection)
                    # インポート
                    collection.insert(collection_records)

                collection = None
                if select_file['collection_name'] == 'controller':
                    collection = ControllerModel(self.mongo)
                    conditions_field = ''

                if collection:
                    # インポート対象期間のデータがあれば削除する。
                    delete_before_importing_non_filter(
                        select_file['collection_name'], collection)
                    # インポート
                    collection.insert(collection_records)

                # 処理の終わったファイルオブジェクトを削除
                del collection_records

        # 終了処理
        self.closed()
        # return ''

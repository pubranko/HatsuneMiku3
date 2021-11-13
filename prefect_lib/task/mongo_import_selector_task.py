import os
import sys
import pickle
import glob
import re
from typing import Any, Union
from logging import Logger
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pymongo import ASCENDING
from pymongo.cursor import Cursor
from prefect.engine import state
from prefect.engine.runner import ENDRUN
path = os.getcwd()
sys.path.append(path)
from prefect_lib.settings import TIMEZONE, BACKUP_BASE_DIR
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
        # def delete_before_importing_non_filter(collection_name: str, collection: Union[ControllerModel],) -> None:
        #     delete_count: int = collection.delete_many(filter={})
        #     logger.info('=== MongoImportSelectorTask run delete_before_importing_non_filter : インポート前削除件数(%s) : %s件' % (
        #         collection_name, str(delete_count)))

        # def delete_before_importing_time_filter(
        #     collection_name: str,
        #     reference_month: str,
        #     conditions_field: str,
        #     collection: Union[CrawlerResponseModel, ScrapedFromResponseModel,
        #                       NewsClipMasterModel, CrawlerLogsModel, AsynchronousReportModel],
        # ) -> None:
        #     yyyy = int(reference_month[1:5])
        #     mm = int(reference_month[6:8])
        #     print('reference_month :', reference_month, ' ', yyyy, ' ', mm)
        #     beginning_of_month = datetime(yyyy, mm, 1, 0, 0, 0)
        #     end_of_month = datetime(
        #         yyyy, mm, 1, 23, 59, 59, 999999) + relativedelta(day=99)

        #     conditions: list = []
        #     conditions.append(
        #         {conditions_field: {'$gte': beginning_of_month}})
        #     conditions.append(
        #         {conditions_field: {'$lte': end_of_month}})
        #     filter: Any = {'$and': conditions}

        #     delete_count: int = collection.delete_many(filter)

        #     logger.info('=== MongoImportSelectorTask run delete_before_importing : インポート前削除件数(%s) : %s件' % (
        #         collection_name, str(delete_count)))

        ###################################################################################
        logger: Logger = self.logger
        logger.info('=== MongoImportSelectorTask run kwargs : ' + str(kwargs))

        collections_name: list = kwargs['collections_name']

        # エクスポート基準年月の月初、月末を求める。
        _ = str(kwargs['backup_dir_from']).split('-')
        base_monthly_from: date = date(
            int(_[0]), int(_[1]), 1)
        _ = str(kwargs['backup_dir_to']).split('-')
        base_monthly_to: date = date(
            int(_[0]), int(_[1]), 1) + relativedelta(day=99)

        print(base_monthly_from, base_monthly_to)

        # インポート元ファイルの一覧を作成
        import_files_info: list = []

        # 頭がyyyy-mmで始まるディレクトリ内のファイル情報を取得する。
        file_list: list = glob.glob(os.path.join(BACKUP_BASE_DIR, '**', '*'))
        for file in file_list:
            path_info = file.split(os.sep)
            if re.match(r'[0-9]{4}-[0[1-9]|1[0-2]]', path_info[1]):

                _ = str(path_info[1]).split('-')
                base_monthly: date = date(
                    int(_[0]), int(_[1][0:2]), 1) + relativedelta(day=99)

                _ = path_info[2].split('-')
                collection_name: str = _[1]

                import_files_info.append({
                    'dir': path_info[1],
                    'base_monthly': base_monthly,
                    'collection_name': collection_name,
                    'file': file,})

        if len(import_files_info) == 0:
            logger.error(
                '=== MongoImportSelectorTask run : インポート可能なディレクトリがありません。')
            raise ENDRUN(state=state.Failed())

        print(import_files_info)

        # 抽出条件を満たすファイルの一覧を作成
        select_files_info: list = []
        for import_file_info in import_files_info:
            select_flg = True

            # コレクションに指定がある場合、指定されたコレクション以外は対象外とする。
            if len(collections_name):
                if not import_file_info['collection_name'] in collections_name:
                    select_flg = False
            # 基準年月の期間指定がある場合、その期間外は対象外とする。
            if base_monthly_from:
                if base_monthly_from > import_file_info['base_monthly']:
                    select_flg = False
            if base_monthly_to:
                if base_monthly_to < import_file_info['base_monthly']:
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

                with open(select_file['file'], 'rb') as file:
                    documents: list = pickle.loads(file.read())
                    for document in documents:
                        del document['_id']
                        collection_records.append(document)

                # filter
                collection = None
                conditions_field: str = ''
                if select_file['collection_name'] == 'crawler_response':
                    collection = CrawlerResponseModel(self.mongo)
                elif select_file['collection_name'] == 'scraped_from_response':
                    collection = ScrapedFromResponseModel(self.mongo)
                elif select_file['collection_name'] == 'news_clip_master':
                    collection = NewsClipMasterModel(self.mongo)
                elif select_file['collection_name'] == 'crawler_logs':
                    collection = CrawlerLogsModel(self.mongo)
                elif select_file['collection_name'] == 'asynchronous_report':
                    collection = AsynchronousReportModel(self.mongo)
                elif select_file['collection_name'] == 'controller':
                    collection = ControllerModel(self.mongo)

                if collection:
                    # インポート
                    collection.insert(collection_records)

                # 処理の終わったファイルオブジェクトを削除
                del collection_records

        # 終了処理
        self.closed()
        # return ''

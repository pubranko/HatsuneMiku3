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


class MonthlyDeleteTask(ExtensionsTask):
    '''
    '''

    def run(self, **kwargs):
        '''
        一定月数経過したデータを削除する。
        '''

        def delete_id_filter(
            collection_name: str,
            id_list: list,
            collection: Union[CrawlerResponseModel, ScrapedFromResponseModel, ControllerModel,
                              NewsClipMasterModel, CrawlerLogsModel, AsynchronousReportModel]
        ):

            # conditions: list = []
            # conditions.append({'_id': {'$in': id_list}})
            # filter: Any = {'$and': conditions}
            filter = {'_id': {'$in': id_list}}
            #logger.info(f'=== {collection_name} へのfilter: {filter}')
            delete_count = collection.delete_many(filter)
            logger.info(f'=== {collection_name}  削除件数 : {delete_count}')

        #####################################################
        logger: Logger = self.logger
        logger.info('=== Monthly delete task run kwargs : ' + str(kwargs))

        collections_name: list = kwargs['collections_name']
        number_of_months_elapsed: int = kwargs['number_of_months_elapsed']
        delete_time = datetime.now().astimezone(TIMEZONE) - \
            relativedelta(month=number_of_months_elapsed)
        delete_time_from: datetime = delete_time + \
            relativedelta(day=1, hour=0, minute=0, second=0, microsecond=0)
        delete_time_to: datetime = delete_time + \
            relativedelta(day=99, hour=23, minute=59,
                          second=59, microsecond=999999)

        print('=== delete_time ', delete_time,
              delete_time_from, delete_time_to)

        # エクスポート済みファイルの一覧を作成
        export_files_info: list = []
        file_dir: list = os.listdir('backup_files')
        file_list = [f for f in file_dir if os.path.isfile(
            os.path.join(path, f))]
        for file in file_list:
            # file名のサンプル => crawler_logs@(2021-10)@20211112_153210
            temp: list = file.split('@', )
            _ = str(temp[1])[1:-1].split('-')
            reference_month: datetime = datetime(
                int(_[0]), int(_[1]), 1, 0, 0, 0).astimezone(TIMEZONE)
            export_files_info.append({
                'file': file,
                'collection_name': temp[0],
                'reference_month': reference_month,
                'time_stamp': datetime.strptime(temp[2], '%Y%m%d_%H%M%S').astimezone(TIMEZONE)
            })

        # 抽出条件を満たすファイルの一覧を作成
        select_files_info: list = []
        for export_file_info in export_files_info:
            select_flg = True

            # コレクションに指定がある場合、指定されたコレクション以外は対象外とする。
            if len(collections_name):
                if not export_file_info['collection_name'] in collections_name:
                    select_flg = False
                    print('コレクションで除外', export_file_info['collection_name'])

            # バックアップのタイムスタンプの期間指定がある場合、その期間外は対象外とする。
            if delete_time_from > export_file_info['reference_month']:
                select_flg = False
            if delete_time_to < export_file_info['reference_month']:
                select_flg = False

            if select_flg:
                select_files_info.append(export_file_info)

        select_file_list = [_['file'] for _ in select_files_info]
        logger.info(
            f'=== MongoImportSelectorTask run : インポート対象ファイル : {select_file_list}')

        # ファイルからオブジェクトを復元し_idをリストに保存。
        if len(select_files_info) > 0:
            for select_file in select_files_info:
                id_list: list = []
                file_path: str = os.path.join(
                    'backup_files', select_file['file'])

                with open(file_path, 'rb') as file:
                    documents: list = pickle.loads(file.read())
                    for document in documents:
                        id_list.append(document['_id'])

                # filter
                collection = None
                if select_file['collection_name'] == 'crawler_response':
                    collection = CrawlerResponseModel(self.mongo)
                elif select_file['collection_name'] == 'crawler_logs':
                    collection = CrawlerLogsModel(self.mongo)
                elif select_file['collection_name'] == 'asynchronous_report':
                    collection = AsynchronousReportModel(self.mongo)

                if collection:
                    print('=== id_list : ', id_list)
                    # インポート対象期間のデータがあれば削除する。
                    delete_id_filter(
                        select_file['collection_name'], id_list, collection)

                # 処理の終わったファイルオブジェクトを削除
                del documents

        # 終了処理
        self.closed()
        # return ''

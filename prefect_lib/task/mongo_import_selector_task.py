import os
import sys
import pickle
from typing import Any
from logging import Logger
from datetime import datetime
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
        '''ここがprefectで起動するメイン処理'''
        logger: Logger = self.logger
        logger.info('=== MongoImportSelectorTask run kwargs : ' + str(kwargs))

        collections_name: list = kwargs['collections_name']
        from_when: datetime = kwargs['from_when']
        to_when: datetime = kwargs['to_when']

        # インポート元ファイルの一覧を作成
        import_files_info: list = []
        file_list: list = os.listdir('backup_files')
        for file in file_list:
            temp: list = file.split('@', )
            import_files_info.append({
                'file': file,
                'collection_name': temp[0],
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

            # 期間指定がある場合、その期間外は対象外とする。
            if from_when:
                if from_when > import_file_info['time_stamp']:
                    select_flg = False
            if to_when:
                if to_when < import_file_info['time_stamp']:
                    select_flg = False

            if select_flg:
                select_files_info.append(import_file_info)

        select_file_list = [ _['file']  for _ in select_files_info]
        logger.info('=== MongoImportSelectorTask run : インポート対象ファイル : ' + str(select_file_list))

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

                collection = None
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
                    collection.insert(collection_records)



        # 終了処理
        self.closed()
        # return ''

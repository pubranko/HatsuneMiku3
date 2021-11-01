import os
import sys
import pickle
from typing import Any
from logging import Logger
from datetime import datetime
from pymongo import ASCENDING
from pymongo.cursor import Cursor
path = os.getcwd()
sys.path.append(path)
from prefect_lib.settings import TIMEZONE
from prefect_lib.task.extentions_task import ExtensionsTask
from models.crawler_response_model import CrawlerResponseModel


class MongoImportSelector(ExtensionsTask):
    '''
    '''

    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''
        logger: Logger = self.logger
        logger.info('=== MongoExportSelector run kwargs : ' + str(kwargs))

        kwargs['start_time'] = self.start_time

        collections: list = kwargs['collections']
        from_when: datetime = kwargs['from_when']
        to_when: datetime = kwargs['to_when']

        # インポート元ファイルの一覧を作成
        import_files_info: list = []
        file_list: list = os.listdir('backup_files')
        for file in file_list:
            temp: list = file.rsplit('-', 1)
            import_files_info.append({
                'file': file,
                'collection_name': temp[0],
                'time_stamp': datetime.strptime(temp[1], '%Y%m%d_%H%M%S').astimezone(TIMEZONE)
            })

        # 抽出条件を満たすファイルの一覧を作成
        select_files_info: list = []
        for import_file_info in import_files_info:
            select_flg = True

            # コレクションに指定がある場合、指定されたコレクション以外は対象外とする。
            if len(collections):
                if not import_file_info['collection_name'] in collections:
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

        # ファイルからオブジェクトを復元しリストに保存。ただし"_id"は削除する。
        collection_records: list = []
        for select_file in select_files_info:
            file_path: str = os.path.join(
                'backup_files', select_file['file'])

            with open(file_path, 'rb') as file:
                documents: list = pickle.loads(file.read())
                for document in documents:
                    del document['_id']
                    collection_records.append(document)

        # 一括で保存する。
        crawler_response: CrawlerResponseModel = CrawlerResponseModel(
            self.mongo)
        crawler_response.insert(collection_records)

        # 終了処理
        self.closed()
        # return ''
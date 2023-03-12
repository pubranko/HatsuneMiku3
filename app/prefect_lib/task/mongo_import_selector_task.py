import os
import sys
import pickle
import glob
import re
from datetime import date
from dateutil.relativedelta import relativedelta
from prefect.engine import state
from prefect.engine.runner import ENDRUN
path = os.getcwd()
sys.path.append(path)
from shared.settings import DATA_DIR__BACKUP_BASE_DIR
from prefect_lib.task.extentions_task import ExtensionsTask
from BrownieAtelierMongo.collection_models.crawler_response_model import CrawlerResponseModel
from BrownieAtelierMongo.collection_models.scraped_from_response_model import ScrapedFromResponseModel
from BrownieAtelierMongo.collection_models.news_clip_master_model import NewsClipMasterModel
from BrownieAtelierMongo.collection_models.crawler_logs_model import CrawlerLogsModel
from BrownieAtelierMongo.collection_models.controller_model import ControllerModel
from BrownieAtelierMongo.collection_models.asynchronous_report_model import AsynchronousReportModel


class MongoImportSelectorTask(ExtensionsTask):
    '''
    '''

    def run(self, **kwargs):
        ''''''
        self.run_init()

        self.logger.info(f'=== MongoImportSelectorTask run kwargs : {str(kwargs)}')

        collections_name: list = kwargs['collections_name']

        if not kwargs['prefix']:
            kwargs['prefix'] = ''

        # エクスポート基準年月の月初、月末を求める。
        _ = str(kwargs['backup_dir_from']).split('-')
        base_monthly_from: date = date(
            int(_[0]), int(_[1]), 1)
        _ = str(kwargs['backup_dir_to']).split('-')
        base_monthly_to: date = date(
            int(_[0]), int(_[1]), 1) + relativedelta(day=99)

        # インポート元ファイルの一覧を作成
        import_files_info: list = []

        # 頭がyyyy-mmで始まるディレクトリ内のファイル情報を取得し、
        # そのファイルのディレクトリ、基準年月、コレクション名、ファイルパスをリストに保存する。
        # 参考）~/backup_files/2021-10/20211114_153856-asynchronous_report
        file_list: list = glob.glob(os.path.join(DATA_DIR__BACKUP_BASE_DIR, '**', '*'))
        for file in file_list:
            path_info = file.split(os.sep)
            # path_info[1] = backup_files
            # path_info[2] = 2022-02、または、prefix_2022-02
            # if kwargs["prefix"] == '':
            #     _ = path_info[2]
            # else:
            #     _ = f'{kwargs["prefix"]}_{path_info[2]}'
            if re.search(r'[0-9]{4}-[0[1-9]|1[0-2]]', path_info[2]):
                if kwargs["prefix"] == '':
                    _ = path_info[2]
                else:
                    _ = path_info[2].replace(f'{kwargs["prefix"]}_','')
                yyyy_mm = str(_).split('-')
                base_monthly: date = date(
                    int(yyyy_mm[0]), int(yyyy_mm[1]), 1) + relativedelta(day=99)
                _ = path_info[3].split('-')
                collection_name: str = _[1]

                import_files_info.append({
                    'dir': path_info[1],
                    'base_monthly': base_monthly,
                    'collection_name': collection_name,
                    'file': file,})

        if len(import_files_info) == 0:
            self.logger.error(
                '=== MongoImportSelectorTask run : インポート可能なディレクトリがありません。')
            raise ENDRUN(state=state.Failed())

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
        self.logger.info(
            f'=== MongoImportSelectorTask run : インポート対象ファイル : {str(select_file_list)}')

        # インポート対象ファイルがある場合、ファイルからオブジェクトを復元しインポートを実施する。
        # その際、空ファイルを除き、"_id"を除去してインポートする。
        if len(select_files_info) > 0:
            for select_file in select_files_info:
                #ファイルからオブジェクトへ復元
                collection_records: list = []
                if os.path.getsize(select_file['file']):
                    with open(select_file['file'], 'rb') as file:
                        documents: list = pickle.loads(file.read())
                        for document in documents:
                            del document['_id']
                            collection_records.append(document)

                # 空ファイル以外はコレクションごとにインポートを実施
                if len(collection_records):
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
                        # インポート
                        collection.insert(collection_records)
                        self.logger.info(
                            f'=== MongoImportSelectorTask run : コレクション({select_file["collection_name"]})  : {len(collection_records)}件')

                    # 処理の終わったファイルオブジェクトを削除
                    del collection_records

        # 終了処理
        self.closed()
        # return ''

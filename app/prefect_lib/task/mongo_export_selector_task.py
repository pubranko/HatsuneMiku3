import os
import sys
import pickle
import copy
from typing import Any, Union
from datetime import datetime, date, time
from dateutil.relativedelta import relativedelta
from pymongo import ASCENDING
from pymongo.cursor import Cursor
from prefect.engine import state
from prefect.engine.runner import ENDRUN
path = os.getcwd()
sys.path.append(path)
from shared.settings import TIMEZONE, BACKUP_BASE_DIR
from prefect_lib.task.extentions_task import ExtensionsTask
from BrownieAtelierMongo.models.crawler_response_model import CrawlerResponseModel
from BrownieAtelierMongo.models.scraped_from_response_model import ScrapedFromResponseModel
from BrownieAtelierMongo.models.news_clip_master_model import NewsClipMasterModel
from BrownieAtelierMongo.models.crawler_logs_model import CrawlerLogsModel
from BrownieAtelierMongo.models.controller_model import ControllerModel
from BrownieAtelierMongo.models.asynchronous_report_model import AsynchronousReportModel


class MongoExportSelectorTask(ExtensionsTask):
    '''
    '''

    def run(self, **kwargs):
        ''''''
        def export_exec(
            collection_name: str,
            filter: Any,
            sort_parameter: list,
            collection: Union[CrawlerResponseModel, ScrapedFromResponseModel, NewsClipMasterModel,
                              CrawlerLogsModel, AsynchronousReportModel, ControllerModel],
            file_path: str,
        ):
            '''指定されたフィルターに基づき、コレクションから取得したレコードをファイルへエクスポートする。'''

            self.logger.info(f'=== {collection_name} へのfilter: {str(filter)}')

            # エクスポート対象件数を確認
            # if filter:
            #     record_count = collection.count_documents(filter=filter)
            # else:
            #     # filterなしの場合、総数のカウント
            #     record_count = collection.estimated_document_count()
            record_count = collection.count(filter)
            #record_count = collection.find(filter=filter,).count()
            self.logger.info(
                f'=== {collection_name} バックアップ対象件数 : {str(record_count)}')

            # 100件単位で処理を実施
            limit: int = 100
            skip_list = list(range(0, record_count, limit))

            # ファイルにリストオブジェクトを追記していく
            with open(file_path, 'ab') as file:
                pass  # 空ファイル作成
            for skip in skip_list:
                records: Cursor = collection.find(
                    filter=filter,
                    sort=sort_parameter,
                ).skip(skip).limit(limit)
                record_list: list = [record for record in records]
                with open(file_path, 'ab') as file:
                    file.write(pickle.dumps(record_list))

        def export_by_month(
            start_time_from: datetime,
            start_time_to: datetime,
            export_dir: str,
            collections_name: list
        ):
            '''
            月別にエクスポート先フォルダを作成し、各コレクションより対象期間のドキュメントをエクスポートする。
            '''
            # バックアップフォルダ直下に基準年月ごとのフォルダを作る。
            # 同一フォルダへのエクスポートは禁止。
            dir_path = os.path.join(BACKUP_BASE_DIR, export_dir)
            if os.path.exists(dir_path):
                self.logger.error(
                    f'=== MongoExportSelector run : backup_dirパラメータエラー : {export_dir} は既に存在します。')
                raise ENDRUN(state=state.Failed())
            else:
                os.mkdir(dir_path)

            for collection_name in collections_name:
                # ファイル名 ＝ 現在日時_コレクション名
                file_path: str = os.path.join(
                    dir_path,
                    f"{self.start_time.strftime('%Y%m%d_%H%M%S')}-{collection_name}")

                sort_parameter: list = []
                collection = None
                conditions: list = []

                if collection_name == 'crawler_response':
                    collection = CrawlerResponseModel(self.mongo)
                    sort_parameter = [('response_time', ASCENDING)]
                    if kwargs['crawler_response__registered']:
                        conditions.append(
                            {'crawling_start_time': {'$gte': start_time_from}})
                        conditions.append(
                            {'crawling_start_time': {'$lte': start_time_to}})
                        conditions.append(
                            {'news_clip_master_register': '登録完了'})  # crawler_responseの場合、登録完了のレコードのみ保存する。

                elif collection_name == 'scraped_from_response':
                    collection = ScrapedFromResponseModel(self.mongo)
                    sort_parameter = []
                    conditions.append(
                        {'scrapying_start_time': {'$gte': start_time_from}})
                    conditions.append(
                        {'scrapying_start_time': {'$lte': start_time_to}})

                elif collection_name == 'news_clip_master':
                    collection = NewsClipMasterModel(self.mongo)
                    sort_parameter = [('response_time', ASCENDING)]
                    conditions.append(
                        {'scraped_save_start_time': {'$gte': start_time_from}})
                    conditions.append(
                        {'scraped_save_start_time': {'$lte': start_time_to}})

                elif collection_name == 'crawler_logs':
                    collection = CrawlerLogsModel(self.mongo)
                    conditions.append(
                        {'start_time': {'$gte': start_time_from}})
                    conditions.append({'start_time': {'$lte': start_time_to}})

                elif collection_name == 'asynchronous_report':
                    collection = AsynchronousReportModel(self.mongo)
                    conditions.append(
                        {'start_time': {'$gte': start_time_from}})
                    conditions.append({'start_time': {'$lte': start_time_to}})

                elif collection_name == 'controller':
                    collection = ControllerModel(self.mongo)

                if collection:
                    filter: Any = {'$and': conditions} if conditions else None
                    export_exec(collection_name, filter,
                                sort_parameter, collection, file_path)

                # 誤更新防止のため、ファイルの権限を参照に限定
                os.chmod(file_path, 0o444)

        #####################################################
        self.run_init()

        self.logger.info(f'=== MongoExportSelector run kwargs : {str(kwargs)}')

        collections_name: list = kwargs['collections_name']
        # base_monthとbackup_dirの指定がなかった場合は自動補正
        previous_month = date.today() - relativedelta(months=1)
        _ = previous_month.strftime('%Y-%m')
        # if not kwargs['base_month']:
        #     kwargs['base_month'] = _
        if not kwargs['export_period_from']:
            kwargs['export_period_from'] = _
            kwargs['export_period_to'] = _
        if not kwargs['export_period_to']:
            kwargs['export_period_to'] = date.today().strftime('%Y-%m')
        if not kwargs['prefix']:
            kwargs['prefix'] = ''
        # if not kwargs['backup_dir']:
        #    kwargs['backup_dir'] = _

        _ = str(kwargs['export_period_from']).split('-')
        start_month: date = date(int(_[0]), int(_[1]), 1)
        _ = str(kwargs['export_period_to']).split('-')
        end_month: date = date(int(_[0]), int(_[1]), 1)

        period_list: list[date] = []
        _ = copy.deepcopy(start_month)
        while _ <= end_month:
            period_list.append(_)
            _ = _ + relativedelta(months=1)

        for base_date in period_list:
            start_time_from: datetime = datetime.combine(
                base_date, time(0, 0, 0)).astimezone(TIMEZONE)
            start_time_to: datetime = datetime.combine(
                base_date, time(23, 59, 59, 999999)).astimezone(TIMEZONE) + relativedelta(day=99)
            if kwargs['prefix'] == '':
                export_dir = base_date.strftime('%Y-%m')  # 拡張名 + yyyy-mm
            else:
                export_dir = f'{kwargs["prefix"]}_{base_date.strftime("%Y-%m")}'
                  # 拡張名 + yyyy-mm
            # export_dir = base_date.strftime(
            #     '%Y-%m') + kwargs['prefix']  # yyyy-mm + 拡張名

            self.logger.info(
                f"=== MongoExportSelector run {base_date.strftime('%Y年%m月')}のエクスポート開始")
            export_by_month(start_time_from, start_time_to,
                            export_dir, collections_name)

        # 終了処理
        self.closed()
        # return ''

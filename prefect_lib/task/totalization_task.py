from __future__ import annotations
import os
import sys
from typing import Any, Sequence, Dict, List, Tuple, Union
from datetime import datetime
from pydantic import ValidationError
from prefect.engine import state
from prefect.engine.runner import ENDRUN
from pymongo.cursor import Cursor
from urllib.parse import urlparse

from openpyxl import Workbook
from openpyxl.chart.bar_chart import BarChart
from openpyxl.styles import PatternFill, Border, Side, Alignment, Protection, Font

path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from common_lib.timezone_recovery import timezone_recovery
from common_lib.mail_send import mail_send
from prefect_lib.data_models.totalization_input import TotalizationInput
from prefect_lib.data_models.asynchronous_report_totalization_data import AsynchronousReportTotalizationData
from models.asynchronous_report_model import AsynchronousReportModel
from models.crawler_logs_model import CrawlerLogsModel
from models.log_totalization_model import LogTotalizationModel


class TotalizationTask(ExtensionsTask):
    '''
    '''

    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''

        self.start_time
        self.mongo
        self.logger.info(
            f'=== LogAnalysisReportTask run kwargs : {kwargs}')

        try:
            totalization = TotalizationInput(
                start_time=self.start_time,
                base_date=kwargs['base_date'],
            )
        except ValidationError as e:
            # e.json()エラー結果をjson形式で見れる。
            # e.errors()エラー結果をdict形式で見れる。
            # str(e)エラー結果をlist形式で見れる。
            self.logger.error(
                f'=== TotalizationTask run エラー内容: {e.errors()}')
            raise ENDRUN(state=state.Failed())

        self.logger.info(
            f'=== TotalizationTask run 基準日from ~ to : {totalization.base_date_get()}')

        '''
        asynchronous_report
        crawler_logs
        '''
        ### 非同期リストの集計
        asynchronous_report_data = AsynchronousReportTotalizationData()
        self.asynchronous_report_totalization(
            totalization, asynchronous_report_data)
        print(asynchronous_report_data.data)

        ### クローラーログの集計
        # ここで
        self.crawler_logs_totalization(totalization)


        ### 集計結果を保存
        log_totalization_model = LogTotalizationModel(self.mongo)   #集計結果保存用のモデル、まだまだ作成中

        # 各コレクション、solrの件数を取得して同期確認
        # ここで上記の解析結果よりレポートを作成する？

        # 終了処理
        self.closed()
        # return ''

    def asynchronous_report_totalization(
            self, totalization: TotalizationInput,
            asynchronous_report_data: AsynchronousReportTotalizationData) -> None:
        '''
        非同期レポートの集計を行う。
        '''
        '''
        まずデータの有無。
        指定期間内に非同期データがあればレポート要。
        record_type、start_time、async_listの3項目。
        record_typeは3種 : news_crawl_async, news_clip_master_async, solr_news_clip_async。
        async_listから総件数、ドメイン別の件数。
        '''

        asynchronous_report_model = AsynchronousReportModel(self.mongo)

        base_date_from, base_date_to = totalization.base_date_get()

        #
        conditions: list = []
        conditions.append(
            {'start_time': {'$gte': base_date_from}})
        conditions.append(
            {'start_time': {'$lt': base_date_to}})

        filter: Any = {'$and': conditions}

        asynchronous_report_records: Cursor = asynchronous_report_model.find(
            filter=filter,
            projection={'_id': 0, 'parameter': 0})
        self.logger.info(
            f'=== 非同期レポート件数({asynchronous_report_records.count()})')

        for asynchronous_report_record in asynchronous_report_records:

            # 新規のレコードタイプの場合初期化する。
            if asynchronous_report_data.record_type_get(asynchronous_report_record
                                                        ['record_type']) == {}:
                asynchronous_report_data.record_type_set(
                    asynchronous_report_record['record_type'])

            # レコードタイプ別に集計を行う。
            asynchronous_report_data.record_type_counter(
                asynchronous_report_record['record_type'])

            # ドメイン別の集計を行う。
            asynchronous_report_data.by_domain_counter(
                asynchronous_report_record['record_type'],
                asynchronous_report_record['async_list']
            )

    def crawler_logs_totalization(self, totalization: TotalizationInput):
        '''
        ログレベルワーニング、エラー、クリティカルの発生件数
        record_type = spider_reports
            start_time  record_type domain  spider_name stats
        record_type = その他（タスク）※実行ログ
        '''
        crawler_logs = CrawlerLogsModel(self.mongo)

        base_date_from, base_date_to = totalization.base_date_get()

        #
        conditions: list = []
        conditions.append(
            {'start_time': {'$gte': base_date_from}})
        conditions.append(
            {'start_time': {'$lt': base_date_to}})

        filter: Any = {'$and': conditions}

        crawler_logs_records: Cursor = crawler_logs.find(
            filter=filter,
            projection={'_id': 0, 'crawl_urls_list': 0}  # crawl_urls_listは不要
        )
        self.logger.info(
            f'=== ログ件数({crawler_logs_records.count()})')

        print('=============')
        print('=============')
        print('=============')
        # for crawler_logs_record in crawler_logs_records:
        #    print(crawler_logs_record)

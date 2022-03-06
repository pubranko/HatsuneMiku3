from __future__ import annotations
import os
import sys
from typing import Any, Sequence
from datetime import datetime
from pydantic import ValidationError
from prefect.engine import state
from prefect.engine.runner import ENDRUN
from pymongo.cursor import Cursor
from urllib.parse import urlparse

from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.chart.bar_chart import BarChart
from openpyxl.styles import PatternFill, Border, Side, Alignment, Protection, Font

path = os.getcwd()
sys.path.append(path)
from common_lib.timezone_recovery import timezone_recovery
from common_lib.mail_send import mail_send
from models.mongo_model import MongoModel
from models.stats_info_collect_model import StatsInfoCollectModel
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.data_models.stats_analysis_report_input import StatsAnalysisReportInput
from prefect_lib.data_models.stats_info_collect_data import StatsInfoCollectData


class StatsAnalysisReportTask(ExtensionsTask):
    '''
    '''

    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''

        self.start_time
        self.mongo
        self.logger.info(
            f'=== StatsAnalysisReportTask run kwargs : {kwargs}')

        try:
            stats_analysis_report_input = StatsAnalysisReportInput(
                start_time=self.start_time,
                report_term=kwargs['report_term'],
                base_date=kwargs['base_date'],
            )
        except ValidationError as e:
            # e.json()エラー結果をjson形式で見れる。
            # e.errors()エラー結果をdict形式で見れる。
            # str(e)エラー結果をlist形式で見れる。
            self.logger.error(
                f'=== StatsAnalysisReportTask run : {e.errors()}')
            raise ENDRUN(state=state.Failed())

        self.logger.info(
            f'=== StatsAnalysisReportTask run 基準日from ~ to : {stats_analysis_report_input.base_date_get()}')

        '''
        asynchronous_report
        crawler_logs
        '''
        # self.asynchronous_report_analysis(log_analysis_report)
        # self.crawler_logs_analysis(log_analysis_report)
        self.stats_analysis_report_create(stats_analysis_report_input)

        # 各コレクション、solrの件数を取得して同期確認
        # ここで上記の解析結果よりレポートを作成する？

        # 終了処理
        self.closed()
        # return ''

    def stats_analysis_report_create(self, stats_analysis_report_input: StatsAnalysisReportInput):
        '''  '''
        stats_info_collect_model = StatsInfoCollectModel(self.mongo)
        base_date_from, base_date_to = stats_analysis_report_input.base_date_get()

        conditions: list = []
        conditions.append(
            {'start_time': {'$gte': base_date_from}})
        conditions.append(
            {'start_time': {'$lt': base_date_to}})

        log_filter: Any = {'$and': conditions}

        stats_info_collect_records: Cursor = stats_info_collect_model.find(
            filter=log_filter,
            projection={'_id': 0, 'parameter': 0}
        )
        self.logger.info(
            f'=== 統計情報レポート件数({stats_info_collect_records.count()})')

        stats_info_collect_data = StatsInfoCollectData()
        for stats_info_collect_record in stats_info_collect_records:
            stats_info_collect_data.dataframe_recovery(
                stats_info_collect_record)

        # print(stats_info_collect_data.robots_response_status_dataframe.to_dict(orient='records'))
        # print(stats_info_collect_data.downloader_response_status_dataframe.to_dict(orient='records'))
        # print(stats_info_collect_data.spider_stats_datafram.to_dict(orient='records'))

        workbook = Workbook()     # ワークブックの新規作成
        self.report_edit_header(workbook)
        self.report_edit_body(workbook)
        # ws = workbook.active      # アクティブなワークシートを選択
        # ws['a1'] = 10
        # cell = ws['a1']
        # # cellメソッドでセルに書き込み
        # ws.cell(row=1, column=1).value = 1
        # #cell.font = Font()

        workbook.save('test.xlsx')    # ワークブックの新規作成と保存

    def report_edit_header(self, workbook: Workbook):
        ws: Worksheet = workbook.active      # アクティブなワークシートを選択
        head1 = ['基準日', 'スパイダー名', '実行回数',
                 'ログレベル件数',
                 '処理時間',
                 'メモリ使用量',
                 '総リクエスト数',
                 '総レスポンス数',

                 'robotsレスポンスステータス',  #
                 'レスポンスステータス',  #

                 'リクエストの深さ',
                 'レスポンスのバイト数',
                 'リトライ件数',
                 '保存件数',
                 '終了理由', ]
        head2 = ['', '', '',
                 'CRITICAL', 'ERROR', 'WARNING',
                 '最小', '最大', '合計', '平均',
                 '最小', '最大', '平均',
                 '最小', '最大', '合計', '平均',
                 '最小', '最大', '合計', '平均',

                 'sss/nnn sss/nnn ~',
                 'sss/nnn sss/nnn ~',

                 '最大'
                 '平均', '合計',
                 '平均', '合計',
                 '平均', '合計',
                 '', ]
        ### 見出し１行目
        row: int = 1
        col: int = 1
        ws.cell(row, col, '基準日')
        ws.cell(row, col:=col+1, 'スパイダー名')
        ws.cell(row, col:=col+1, '実行回数')
        ws.cell(row, col:=col+1, 'ログレベル件数')
        ws.cell(row, col:=col+3, '処理時間')
        ws.cell(row, col:=col+4, 'メモリ使用量')
        ws.cell(row, col:=col+3, '総リクエスト数')
        ws.cell(row, col:=col+4, '総レスポンス数')
        ws.cell(row, col:=col+4, 'robotsレスポンスステータス')
        ws.cell(row, col:=col+1, 'レスポンスステータス')
        ws.cell(row, col:=col+1, 'リクエストの深さ')
        ws.cell(row, col:=col+1, 'レスポンスのバイト数')
        ws.cell(row, col:=col+2, 'リトライ件数')
        ws.cell(row, col:=col+2, '保存件数')
        ws.cell(row, col:=col+2, '終了理由')
        for cells in ws["a1:ae1"]:
            for cell in cells:
                cell.alignment = Alignment(horizontal="centerContinuous")

        ### 見出し２行目
        row: int = 2
        col: int = 4
        ws.cell(row, col, 'CRITICAL')
        ws.cell(row, col:=col+1, 'ERROR')
        ws.cell(row, col:=col+1, 'WARNING')
        ws.cell(row, col:=col+1, '最小')
        ws.cell(row, col:=col+1, '最大')
        ws.cell(row, col:=col+1, '合計')
        ws.cell(row, col:=col+1, '平均')
        ws.cell(row, col:=col+1, '最小')
        ws.cell(row, col:=col+1, '最大')
        ws.cell(row, col:=col+1, '平均')
        ws.cell(row, col:=col+1, '最小')
        ws.cell(row, col:=col+1, '最大')
        ws.cell(row, col:=col+1, '合計')
        ws.cell(row, col:=col+1, '平均')
        ws.cell(row, col:=col+1, '最小')
        ws.cell(row, col:=col+1, '最大')
        ws.cell(row, col:=col+1, '合計')
        ws.cell(row, col:=col+1, '平均')
        ws.cell(row, col:=col+3, '最大')
        ws.cell(row, col:=col+1, '合計')
        ws.cell(row, col:=col+1, '平均')
        ws.cell(row, col:=col+1, '合計')
        ws.cell(row, col:=col+1, '平均')
        ws.cell(row, col:=col+1, '合計')
        ws.cell(row, col:=col+1, '平均')
        for cells in ws["a2:ae2"]:
            for cell in cells:
                cell.alignment = Alignment(horizontal="center")

    def report_edit_body(self, workbook: Workbook):
        ws = workbook.active      # アクティブなワークシートを選択

    # def asynchronous_report_analysis(self, log_analysis_report: StatsAnalysisReportInput):
    #     '''
    #     まずデータの有無。
    #     指定期間内に非同期データがあればレポート要。
    #     record_type、start_time、async_listの3項目。
    #     record_typeは3種 : news_crawl_async, news_clip_master_async, solr_news_clip_async。
    #     async_listから総件数、ドメイン別の件数。
    #     '''

    #     asynchronous_report_model = AsynchronousReportModel(self.mongo)

    #     base_date_from, base_date_to = log_analysis_report.base_date_get()

    #     #
    #     conditions: list = []
    #     conditions.append(
    #         {'start_time': {'$gte': base_date_from}})
    #     conditions.append(
    #         {'start_time': {'$lt': base_date_to}})

    #     log_filter: Any = {'$and': conditions}

    #     asynchronous_report_records: Cursor = asynchronous_report_model.find(
    #         filter=log_filter,
    #         projection={'_id': 0, 'parameter': 0}
    #     )
    #     self.logger.info(
    #         f'=== 非同期レポート件数({asynchronous_report_records.count()})')

    #     # レコードタイプ別カウントエリア
    #     by_record_type_count: dict[str, int] = {
    #         'news_crawl_async': 0,
    #         'news_clip_master_async': 0,
    #         'solr_news_clip_async': 0,
    #     }
    #     # レコードタイプ別・ドメイン別カウントエリア
    #     by_record_type_by_domain_count: dict[str, dict[str, int]] = {
    #         'news_crawl_async': {},
    #         'news_clip_master_async': {},
    #         'solr_news_clip_async': {},
    #     }

    #     for asynchronous_report_record in asynchronous_report_records:
    #         # print(asynchronous_report_record)

    #         # レコードタイプ別に集計を行う。
    #         by_record_type_count[asynchronous_report_record['record_type']] += 1
    #         self.by_domain_asynchronous_report_count(
    #             asynchronous_report_record['async_list'], by_record_type_by_domain_count[asynchronous_report_record['record_type']])

    #     print(by_record_type_count)
    #     print(by_record_type_by_domain_count)

    #     wb = Workbook()
    #     ws = wb.active
    #     ws['a1'] = 10
    #     cell = ws['a1']
    #     #cell.font = Font()

    #     wb.save('test.xlsx')

    # def by_domain_asynchronous_report_count(self, async_list: list, by_domain_count: dict):
    #     for url in async_list:
    #         url_parse = urlparse(url)
    #         if url_parse.netloc not in by_domain_count:
    #             by_domain_count[url_parse.netloc] = 0
    #         by_domain_count[url_parse.netloc] += 1

    # def crawler_logs_analysis(self, log_analysis_report: StatsAnalysisReportInput):
    #     '''
    #     ログレベルワーニング、エラー、クリティカルの発生件数
    #     record_type = spider_reports
    #         start_time  record_type domain  spider_name stats
    #     record_type = その他（タスク）※実行ログ
    #     '''
    #     crawler_logs = CrawlerLogsModel(self.mongo)

    #     base_date_from, base_date_to = log_analysis_report.base_date_get()

    #     #
    #     conditions: list = []
    #     conditions.append(
    #         {'start_time': {'$gte': base_date_from}})
    #     conditions.append(
    #         {'start_time': {'$lt': base_date_to}})

    #     log_filter: Any = {'$and': conditions}

    #     crawler_logs_records: Cursor = crawler_logs.find(
    #         filter=log_filter,
    #         projection={'_id': 0, 'crawl_urls_list': 0}  # crawl_urls_listは不要
    #     )
    #     self.logger.info(
    #         f'=== ログ件数({crawler_logs_records.count()})')

    #     print('=============')
    #     print('=============')
    #     print('=============')
    #     # for crawler_logs_record in crawler_logs_records:
    #     #    print(crawler_logs_record)

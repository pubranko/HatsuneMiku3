from __future__ import annotations
import os
import sys
from typing import Any, Sequence
from datetime import datetime
import pandas as pd
from pydantic import ValidationError
from prefect.engine import state
from prefect.engine.runner import ENDRUN
from pymongo.cursor import Cursor
from urllib.parse import urlparse

from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.cell import Cell, MergedCell
from openpyxl.chart.bar_chart import BarChart
from openpyxl.styles import PatternFill, Border, Side, Alignment, Protection, Font
from openpyxl.utils import get_column_letter

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
    columns_info: list = [
        #{見出し１行目, 見出し２行目, データフレーム列名}
        {'head1': '集計日〜', 'head2': '',
            'col': 'aggregate_base_term'},
        {'head1': 'スパイダー名', 'head2': '',
            'col': 'spider_name'},
        {'head1': 'ログレベル件数', 'head2': 'CRITICAL',
            'col': 'log_count/CRITICAL'},
        {'head1': '', 'head2': 'ERROR',
            'col': 'log_count/ERROR'},
        {'head1': '', 'head2': 'WARNING',
            'col': 'log_count/WARNING'},
        {'head1': '処理時間', 'head2': '最小',
            'col': 'elapsed_time_seconds_min'},
        {'head1': '', 'head2': '最大',
            'col': 'elapsed_time_seconds_max'},
        {'head1': '', 'head2': '合計',
            'col': 'elapsed_time_seconds'},
        {'head1': '', 'head2': '平均',
            'col': 'elapsed_time_seconds_mean'},
        {'head1': 'メモリ使用量', 'head2': '最小',
            'col': 'memusage/max_min'},
        {'head1': '', 'head2': '最大',
            'col': 'memusage/max_max'},
        {'head1': '', 'head2': '平均',
            'col': 'memusage/max_mean'},
        {'head1': '総リクエスト数', 'head2': '最小',
            'col': 'downloader/request_count_min'},
        {'head1': '', 'head2': '最大',
            'col': 'downloader/request_count_max'},
        {'head1': '', 'head2': '合計',
            'col': 'downloader/request_count'},
        {'head1': '', 'head2': '平均',
            'col': 'downloader/request_count_mean'},
        {'head1': '総レスポンス数', 'head2': '最小',
            'col': 'downloader/response_count_min'},
        {'head1': '', 'head2': '最大',
            'col': 'downloader/response_count_max'},
        {'head1': '', 'head2': '合計',
            'col': 'downloader/response_count'},
        {'head1': '', 'head2': '平均',
            'col': 'downloader/response_count_mean'},
        {'head1': 'リクエストの深さ', 'head2': '最大',
            'col': 'request_depth_max_max'},
        {'head1': 'レスポンスのバイト数', 'head2': '合計',
            'col': 'downloader/response_bytes'},
        {'head1': '', 'head2': '平均',
            'col': 'downloader/response_bytes_mean'},
        {'head1': 'リトライ件数', 'head2': '合計',
            'col': 'retry/count'},
        {'head1': '', 'head2': '平均',
            'col': 'retry/count_mean'},
        {'head1': '保存件数', 'head2': '合計',
            'col': 'item_scraped_count'},
        {'head1': '', 'head2': '平均',
            'col': 'item_scraped_count_mean'},
        # {'head1': '実行回数', 'head2': '',
        #    'col': ''},
        # {'head1': 'robotsレスポンスステータス', 'head2': '',
        #     'col': ''},
        # {'head1': 'レスポンスステータス', 'head2': '',
        #     'col': ''},
    ]

    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''

        self.start_time
        self.mongo
        self.logger.info(
            f'=== StatsAnalysisReportTask run kwargs : {kwargs}')

        try:
            self.stats_analysis_report_input = StatsAnalysisReportInput(
                start_time=self.start_time,
                report_term=kwargs['report_term'],
                totalling_term=kwargs['totalling_term'],
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
            f'=== StatsAnalysisReportTask run 基準日from ~ to : {self.stats_analysis_report_input.base_date_get()}')

        '''
        asynchronous_report
        crawler_logs
        '''
        # self.asynchronous_report_analysis(log_analysis_report)
        # self.crawler_logs_analysis(log_analysis_report)
        self.stats_analysis_report_create()

        # 各コレクション、solrの件数を取得して同期確認
        # ここで上記の解析結果よりレポートを作成する？

        # 終了処理
        self.closed()
        # return ''

    def stats_analysis_report_create(self):
        '''  '''
        stats_info_collect_model = StatsInfoCollectModel(self.mongo)
        base_date_from, base_date_to = self.stats_analysis_report_input.base_date_get()

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

        collect_data = StatsInfoCollectData()
        for stats_info_collect_record in stats_info_collect_records:
            collect_data.dataframe_recovery(
                stats_info_collect_record)

        # 集計期間リストごとに解析を実行
        spider_result_all_df = collect_data.stats_analysis_exec(
            self.stats_analysis_report_input.datetime_term_list())

        workbook = Workbook()     # ワークブックの新規作成
        self.report_edit_header(workbook)
        self.report_edit_body(workbook, spider_result_all_df)
        workbook.save('test.xlsx')    # ワークブックの新規作成と保存

    def report_edit_header(self, workbook: Workbook):
        '''レポート用Excelの見出し編集'''
        ws: Worksheet = workbook.active      # アクティブなワークシートを選択

        # 罫線定義
        side = Side(style='thin', color='000000')
        border = Border(top=side, bottom=side, left=side, right=side)

        for i, col_info in enumerate(self.columns_info):
            ws.cell(1, i + 1, col_info['head1'])
            ws.cell(2, i + 1, col_info['head2'])

        max_row = ws.max_row
        max_column = ws.max_column
        # 型ヒントでエラーがでるので仕方なくAnyを使用
        max_cell: Any = ws.cell(row=max_row, column=max_column)
        fill = PatternFill(patternType='solid', fgColor='2986E8')

        for cells in ws[f'a1:{max_cell.coordinate}']:
            for cell in cells:
                cell: Cell
                cell.border = border
                cell.alignment = Alignment(
                    horizontal="centerContinuous")   # 選択範囲内中央寄せ
                cell.fill = fill


    def report_edit_body(self, workbook: Workbook, spider_result_all_df: pd.DataFrame):
        # ワークシートを選択
        ws = workbook.active

        # 罫線定義
        side = Side(style='thin', color='000000')
        border = Border(top=side, bottom=side, left=side, right=side)

        # 列ごとにエクセルに編集
        for col_idx, col_info in enumerate(self.columns_info):
            for row_idx, value in enumerate(spider_result_all_df[col_info['col']]):
                ws.cell(row_idx + 3, col_idx + 1, value)

        max_cell:str = get_column_letter(ws.max_column) + str(ws.max_row)   #"BC55"のようなセル番地を生成
        print(max_cell)
        for cells in ws[f'a3:{max_cell}']:
            for cell in cells:
                cell:Cell
                cell.border = border

        for cells in ws[f'c3:{max_cell}']:
            for cell in cells:
                #cell:Cell
                cell.number_format = '0.0'

        ### 列ごとに次の処理を行う。
        ### 最大幅を確認
        ### それに合わせた幅を設定する。
        for col in ws.columns:
            max_length = 0
            column = col[0].column_letter   #列名A,Bなどを取得
            for cell in col:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            ws.column_dimensions[column].width = (max_length + 2) * 1.2

        ws.freeze_panes = 'c3'

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

        # head1 = ['基準日', 'スパイダー名', '実行回数',
        #          'ログレベル件数',
        #          '処理時間',
        #          'メモリ使用量',
        #          '総リクエスト数',
        #          '総レスポンス数',

        #          'robotsレスポンスステータス',  #
        #          'レスポンスステータス',  #

        #          'リクエストの深さ',
        #          'レスポンスのバイト数',
        #          'リトライ件数',
        #          '保存件数',
        #          '終了理由', ]
        # head2 = ['', '', '',
        #          'CRITICAL', 'ERROR', 'WARNING',
        #          '最小', '最大', '合計', '平均',
        #          '最小', '最大', '平均',
        #          '最小', '最大', '合計', '平均',
        #          '最小', '最大', '合計', '平均',

        #          'sss/nnn sss/nnn ~',
        #          'sss/nnn sss/nnn ~',

        #          '最大'
        #          '平均', '合計',
        #          '平均', '合計',
        #          '平均', '合計',
        #          '', ]

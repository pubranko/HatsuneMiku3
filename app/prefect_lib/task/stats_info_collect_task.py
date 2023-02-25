import os
import sys
from typing import Any
from pydantic import ValidationError
from prefect.engine import state
from prefect.engine.runner import ENDRUN
from pymongo.cursor import Cursor
from urllib.parse import urlparse
# from openpyxl import Workbook
# from openpyxl.chart.bar_chart import BarChart
# from openpyxl.styles import PatternFill, Border, Side, Alignment, Protection, Font

path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from shared.timezone_recovery import timezone_recovery
from prefect_lib.data_models.stats_info_collect_input import StatsInfoCollectInput
#from prefect_lib.data_models.asynchronous_report_totalization_data import AsynchronousReportTotalizationData
from prefect_lib.data_models.stats_info_collect_data import StatsInfoCollectData
from BrownieAtelierMongo.models.asynchronous_report_model import AsynchronousReportModel
from BrownieAtelierMongo.models.crawler_logs_model import CrawlerLogsModel
from BrownieAtelierMongo.models.stats_info_collect_model import StatsInfoCollectModel


class StatsInfoCollectTask(ExtensionsTask):
    '''
    '''

    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''
        self.run_init()

        self.logger.info(
            f'=== StatsInfoCollectTask run kwargs : {kwargs}')

        try:
            stats_info_collect_input = StatsInfoCollectInput(
                start_time=self.start_time,
                base_date=kwargs['base_date'],
            )
        except ValidationError as e:
            # e.json()エラー結果をjson形式で見れる。
            # e.errors()エラー結果をdict形式で見れる。
            # str(e)エラー結果をlist形式で見れる。
            self.logger.error(
                f'=== StatsInfoCollectTask run エラー内容: {e.errors()}')
            raise ENDRUN(state=state.Failed())

        self.logger.info(
            f'=== StatsInfoCollectTask run 基準日from ~ to : {stats_info_collect_input.base_date_get()}')

        # 非同期リストの集計
        # asynchronous_report_data = AsynchronousReportTotalizationData()
        # self.asynchronous_report_totalization(
        #     totalization, asynchronous_report_data)

        # クローラーログの集計
        crawler_logs_data = StatsInfoCollectData()
        self.crawler_logs_stats_info_collect(
            stats_info_collect_input, crawler_logs_data)

        # 集計結果を保存
        stats_info_collect_model = StatsInfoCollectModel(
            self.mongo)
        # レコードタイプ = spider_stats
        stats_info_collect_model.stats_update(
            crawler_logs_data.spider_df.to_dict(orient='records'))
        # レコードタイプ = robots_response_status
        stats_info_collect_model.stats_update(
            crawler_logs_data.robots_df.to_dict(orient='records'), 'robots_response_status')
        # レコードタイプ = downloader_response_status
        stats_info_collect_model.stats_update(
            crawler_logs_data.downloader_df.to_dict(orient='records'), 'downloader_response_status')

        # 終了処理
        self.closed()

    def crawler_logs_stats_info_collect(self, stats_info_collect_input: StatsInfoCollectInput, crawler_logs_data: StatsInfoCollectData):
        '''
        ログレベルワーニング、エラー、クリティカルの発生件数
        record_type = spider_reports
            start_time  record_type domain  spider_name stats
        不要（record_type = その他（タスク）※実行ログ）
        '''
        crawler_logs = CrawlerLogsModel(self.mongo)
        base_date_from, base_date_to = stats_info_collect_input.base_date_get()

        #
        conditions: list = []
        conditions.append(
            {'record_type': 'spider_reports'})
        conditions.append(
            {'start_time': {'$gte': base_date_from}})
        conditions.append(
            {'start_time': {'$lt': base_date_to}})

        filter: Any = {'$and': conditions}

        count = crawler_logs.count(filter=filter)
        crawler_logs_records: Cursor = crawler_logs.find(
            filter=filter,
            # idやcrawl_urls_listは不要
            projection={'_id': 0, 'crawl_urls_list': 0}
        )
        self.logger.info(
            f'=== クローラーログ対象件数({count})')

        for crawler_logs_record in crawler_logs_records:
            crawler_logs_data.spider_stats_store(
                timezone_recovery(crawler_logs_record['start_time']), crawler_logs_record['spider_name'], crawler_logs_record['stats'])

    # def asynchronous_report_totalization(
    #         self, stats_info_collect_input: StatsInfoCollectInput,
    #         asynchronous_report_data: AsynchronousReportTotalizationData) -> None:
    #     '''
    #     非同期レポートの集計を行う。
    #     '''
    #     '''
    #     まずデータの有無。
    #     指定期間内に非同期データがあればレポート要。
    #     record_type、start_time、async_listの3項目。
    #     record_typeは3種 : news_crawl_async, news_clip_master_async, solr_news_clip_async。
    #     async_listから総件数、ドメイン別の件数。
    #     '''

    #     asynchronous_report_model = AsynchronousReportModel(self.mongo)

    #     base_date_from, base_date_to = stats_info_collect_input.base_date_get()

    #     #
    #     conditions: list = []
    #     conditions.append(
    #         {'start_time': {'$gte': base_date_from}})
    #     conditions.append(
    #         {'start_time': {'$lt': base_date_to}})

    #     filter: Any = {'$and': conditions}

    #     asynchronous_report_records: Cursor = asynchronous_report_model.find(
    #         filter=filter,
    #         projection={'_id': 0, 'parameter': 0})
    #     self.logger.info(
    #         f'=== 非同期レポート対象件数({asynchronous_report_records.count()})')

    #     for asynchronous_report_record in asynchronous_report_records:

    #         # 新規のレコードタイプの場合初期化する。
    #         if asynchronous_report_data.record_type_get(asynchronous_report_record
    #                                                     ['record_type']) == {}:
    #             asynchronous_report_data.record_type_set(
    #                 asynchronous_report_record['record_type'])

    #         # レコードタイプ別に集計を行う。
    #         asynchronous_report_data.record_type_counter(
    #             asynchronous_report_record['record_type'])

    #         # ドメイン別の集計を行う。
    #         asynchronous_report_data.by_domain_counter(
    #             asynchronous_report_record['record_type'],
    #             asynchronous_report_record['async_list']
    #         )

import os
import sys
import pickle
from typing import Any
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


class MongoExportSelector(ExtensionsTask):
    '''
    '''

    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''

        logger: Logger = self.logger
        logger.info('=== MongoExportSelector run kwargs : ' + str(kwargs))

        kwargs['start_time'] = self.start_time

        collections: list = kwargs['collections']
        # 月初、月末
        _ = str(kwargs['backup_yyyymm']).split('-')
        start_time_from: datetime = datetime(
            int(_[0]), int(_[1]), 1, 0, 0, 0).astimezone(TIMEZONE)
        start_time_to: datetime = datetime(
            int(_[0]), int(_[1]), 1, 23, 59, 59, 999999).astimezone(TIMEZONE) + relativedelta(day=99)

        crawler_response: CrawlerResponseModel = CrawlerResponseModel(
            self.mongo)

        conditions: list = []
        if start_time_from:
            conditions.append(
                {'crawling_start_time': {'$gte': start_time_from}})
        if start_time_to:
            conditions.append(
                {'crawling_start_time': {'$lte': start_time_to}})

        if conditions:
            filter: Any = {'$and': conditions}
        else:
            filter = None
        logger.info('=== crawler_responseへのfilter: ' + str(filter))

        # スクレイピング対象件数を確認
        record_count = crawler_response.find(
            projection=None,
            filter=filter,
        ).count()
        logger.info('=== crawler_response バックアップ対象件数 : ' + str(record_count))

        # 件数制限でスクレイピング処理を実施
        limit: int = 100
        skip_list = list(range(0, record_count, limit))

        now = datetime.now()
        # ファイル名 ＝ コレクション名_現在日時
        file_path: str = os.path.join(
            'backup_files', 'crawler_response(' + str(kwargs['backup_yyyymm']) + ')-' + now.strftime('%Y%m%d_%H%M%S'))

        for skip in skip_list:
            records: Cursor = crawler_response.find(
                projection=None,
                filter=filter,
                sort=[('response_time', ASCENDING)],
            ).skip(skip).limit(limit)

            record_list: list = [record for record in records]

            with open(file_path, 'ab') as file:
                file.write(pickle.dumps(record_list))

        # 終了処理
        self.closed()
        # return ''

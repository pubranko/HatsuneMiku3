import os
from typing import Any
from models.mongo_model import MongoModel
from models.mongo_common_model import MongoCommonModel


class StatsInfoCollectModel(MongoCommonModel):
    '''
    ログ集計コレクション用モデル
    '''
    mongo: MongoModel
    collection_name: str = os.environ['MONGO_STATS_INFO_COLLECT']

    def stats_update(self, records: list, status_key: str = ''):
        ''' '''
        for record in records:
            conditions: list = []
            conditions.append(
                {'record_type': record['record_type']})
            conditions.append(
                {'start_time': record['start_time']})
            conditions.append(
                {'spider_name': record['spider_name']})
            if len(status_key):
                conditions.append(
                    {status_key: record[status_key]})
            filter: Any = {'$and': conditions}

            self.update(filter=filter, record=record)

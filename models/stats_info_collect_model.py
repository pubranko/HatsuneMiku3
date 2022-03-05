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

    def all_update(self, records: list):
        ''' '''
        for record in records:
            conditions: list = []
            conditions.append(
                {'record_type': record['record_type']})
            conditions.append(
                {'start_time': record['start_time']})
            conditions.append(
                {'spider_name': record['spider_name']})
            filter: Any = {'$and': conditions}

            self.update(filter=filter, record=record)

import os
from models.mongo_model import MongoModel
from models.mongo_common_model import MongoCommonModel


class CrawlerLogsModel(MongoCommonModel):
    '''
    crawler_logsコレクション用モデル
    '''
    mongo: MongoModel
    collection_name: str = os.environ['MONGO_CRAWLER_LOGS']

    def __init__(self, mongo: MongoModel):
        super().__init__(mongo)

        # インデックスの有無を確認し、なければ作成する。
        # ※sort使用時、indexがないとメモリ不足となるため。
        create_index_flg:bool = True
        for indexes in self.mongo.mongo_db[self.collection_name].list_indexes():
            for idx in indexes['key']:
                if idx == 'start_time':
                    create_index_flg = False
        if create_index_flg:
            self.mongo.mongo_db[self.collection_name].create_index('start_time')

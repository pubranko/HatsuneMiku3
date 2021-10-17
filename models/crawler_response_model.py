from typing import Any
from datetime import datetime
from models.mongo_model import MongoModel
from models.mongo_common_model import MongoCommonModel


class CrawlerResponseModel(MongoCommonModel):
    '''
    crawler_responseコレクション用モデル
    '''
    mongo: MongoModel
    collection_name: str = 'crawler_response'

    def __init__(self, mongo: MongoModel):
        super().__init__(mongo)

        # インデックスの有無を確認し、なければ作成する。
        # ※sort使用時、indexがないとメモリ不足となるため。
        create_index_flg:bool = True
        for indexes in self.mongo.mongo_db[self.collection_name].list_indexes():
            for idx in indexes['key']:
                if idx == 'response_time':
                    create_index_flg = False
        if create_index_flg:
            self.mongo.mongo_db[self.collection_name].create_index('response_time')

    def news_clip_master_register_result(self, url: str, response_time: datetime, news_clip_master_register: str) -> None:
        '''news_clip_masterへの登録結果を反映させる'''
        record: Any = self.find_one(
            filter={'$and': [{'url': url}, {'response_time': response_time}]})
        if record == None:
            pass
        else:
            record['news_clip_master_register'] = news_clip_master_register

        self.update(
            {'url': url, 'response_time': response_time},
            record,
        )

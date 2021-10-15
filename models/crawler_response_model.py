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

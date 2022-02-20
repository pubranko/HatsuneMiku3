import os
from models.mongo_model import MongoModel
from models.mongo_common_model import MongoCommonModel


class LogTotalizationModel(MongoCommonModel):
    '''
    ログ集計コレクション用モデル
    '''
    mongo: MongoModel
    collection_name: str = os.environ['MONGO_LOGTOTALIZATION']

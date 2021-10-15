from models.mongo_model import MongoModel
from models.mongo_common_model import MongoCommonModel


class AsynchronousReportModel(MongoCommonModel):
    '''
    非同期レポートコレクション用モデル
    '''
    mongo: MongoModel
    collection_name: str = 'asynchronous_report'

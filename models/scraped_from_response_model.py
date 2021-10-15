from models.mongo_model import MongoModel
from models.mongo_common_model import MongoCommonModel


class ScrapedFromResponseModel(MongoCommonModel):
    '''
    scraped_from_responseコレクション用モデル
    '''
    mongo: MongoModel
    collection_name: str = 'scraped_from_response'

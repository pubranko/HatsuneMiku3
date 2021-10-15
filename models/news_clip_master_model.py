from models.mongo_model import MongoModel
from models.mongo_common_model import MongoCommonModel


class NewsClipMasterModel(MongoCommonModel):
    '''
    news_clip_masterコレクション用モデル
    '''
    mongo: MongoModel
    collection_name: str = 'news_clip_master'

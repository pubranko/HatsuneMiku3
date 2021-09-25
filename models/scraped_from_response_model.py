from models.mongo_model import MongoModel


class ScrapedFromResponse(object):
    '''
    scraped_from_responseコレクション用モデル
    '''
    mongo: MongoModel

    def __init__(self, mongo: MongoModel):
        self.mongo = mongo

    def insert_one(self, item:dict):
        self.mongo.crawler_db['scraped_from_response'].insert_one(item)

    def insert(self, items:list):
        self.mongo.crawler_db['scraped_from_response'].insert(items)

    def find_one(self, projection=None,filter=None):
        return self.mongo.crawler_db['scraped_from_response'].find_one(projection=projection,filter=filter)

    def find(self, projection=None,filter=None, sort=None):
        return self.mongo.crawler_db['scraped_from_response'].find(projection=projection,filter=filter,sort=sort)


# (method) insert_one: (document: Unknown, bypass_document_validation: Unknown = False, session: Unknown = None) -> InsertOneResult

# (method) insert: (doc_or_docs: Unknown, manipulate: Unknown = True, check_keys: Unknown = True, continue_on_error: Unknown = False, **kwargs: Unknown) -> Unknown | list[Unknown] | None
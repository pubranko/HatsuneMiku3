from typing import Union
from models.mongo_model import MongoModel
from pymongo.cursor import Cursor


class MongoCommonModel(object):
    '''
    mongoDBへの共通アクセス処理。
    各コレクション別のクラスでは当クラスを継承することで共通の関数を定義する必要がなくなる。
    '''
    mongo: MongoModel
    collection_name: str = 'sample'

    def __init__(self, mongo: MongoModel):
        self.mongo = mongo

    def count(self, filter:Union[dict,None]=None):
        '''
        コレクションのカウント。
        絞り込み条件がある場合、filterを指定してください。
        コレクション内ドキュメント総数のカウントであれば、filterにはNoneを指定してください。
        '''
        if type(filter) is dict:
            return self.count_documents(filter)
        else:
            return self.estimated_document_count()

    def count_documents(self, filter:dict):
        '''
        コレクション内の条件付き件数のカウント。
        絞り込み条件がある場合、filterを指定してください。
        '''
        return self.mongo.mongo_db[self.collection_name].count_documents(filter=filter)

    def estimated_document_count(self):
        '''コレクション内のドキュメント総数のカウント'''
        return self.mongo.mongo_db[self.collection_name].estimated_document_count()

    def find_one(self, projection=None, filter=None):
        return self.mongo.mongo_db[self.collection_name].find_one(projection=projection, filter=filter)

    # def find(self, projection=None, filter=None, sort=None, index=None):
    def find(self, projection=None, filter=None, sort=None):
        return self.mongo.mongo_db[self.collection_name].find(projection=projection, filter=filter, sort=sort)

    def insert_one(self, item):
        self.mongo.mongo_db[self.collection_name].insert_one(item)

    def insert(self, items: list):
        self.mongo.mongo_db[self.collection_name].insert_many(items)

    def update_one(self, filter, record: dict) -> None:
        self.mongo.mongo_db[self.collection_name].update_one(
            filter, record, upsert=True)

    # def update_many(self, filter, record: dict) -> None:
    #     self.mongo.mongo_db[self.collection_name].update_many(
    #         filter, record, upsert=True)

    def delete_many(self, filter) -> int:
        result = self.mongo.mongo_db[self.collection_name].delete_many(
            filter=filter)
        return int(result.deleted_count)

    def aggregate(self, aggregate_key: str):
        '''渡された集計keyによる集計結果を返す。'''
        pipeline = [
            {'$unwind': '$' + aggregate_key},
            {'$group': {'_id': '$' + aggregate_key, 'count': {'$sum': 1}}},
        ]
        return self.mongo.mongo_db[self.collection_name].aggregate(pipeline=pipeline)

    def limited_find(self, projection=None, filter:dict[str,list] ={}, sort=None, limit: int = 100):
        '''
        ・findした結果をレコード単位で返すジェネレーター。
        ・デフォルトで100件単位でデータを取得するが、当メソッドの呼び出し元では
          取得件数の制限を意識すること無く検索結果を参照できる。
        ・以下のような繰り返し処理で使用することを想定
            for record in news_clip_master.limited_find(filter=filter):
                pass
        '''
        # 対象件数を確認
        #record_count = self.find(filter=filter).count()
        record_count = self.mongo.mongo_db[self.collection_name].count_documents(filter=filter)
        # 100件単位で処理を実施
        skip_list = list(range(0, record_count, limit))
        for skip in skip_list:
            records: Cursor = self.find(
                filter=filter, projection=projection, sort=sort).skip(skip).limit(limit)
            for record in records:
                yield record


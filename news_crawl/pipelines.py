# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface

from itemadapter import ItemAdapter
from pymongo import MongoClient
import os

class MongoPipeline(object):

    def __init__(self):
        #print("=== pipelines.py __init__ start")
        self.mongo_uri = os.environ['MONGO_SERVER']
        self.mongo_port = os.environ['MONGO_PORT']
        self.mongo_db = os.environ['MONGO_USE_DB']
        self.mongo_user = os.environ['MONGO_USER']
        self.mongo_pass = os.environ['MONGO_PASS']
        self.mongo_collection = os.environ['MONGO_COLLECTION']

    def open_spider(self, spider):
        #print("=== pipelines.py open_spider start")
        self.client = MongoClient(self.mongo_uri,int(self.mongo_port))
        self.db = self.client[self.mongo_db]
        self.db.authenticate(self.mongo_user, self.mongo_pass)

        self.collection = self.db[self.mongo_collection]

    def close_spider(self, spider):
        #print("=== pipelines.py close_spider start")
        self.client.close()

    def process_item(self, item, spider):
        #print("=== pipelines.py process start")
        #dict：keyのリストと値のリストを辞書形式へ変換。これで、items.pyで短縮したresponseが元に戻る
        self.collection.insert_one(dict(item))
        return item

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface

from itemadapter import ItemAdapter
from pymongo import MongoClient
from news_crawl.models.mongo_model import MongoModel
from news_crawl.models.crawler_response_model import CrawlerResponseModel
import os

class MongoPipeline(object):

    def __init__(self):
        pass

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        spider.mongo.close()

    def process_item(self, item, spider):

        mongo:MongoModel = spider.mongo
        crawler_response = CrawlerResponseModel(mongo)
        crawler_response.insert_one(dict(item))

        #spider.mongo.insert_one('crawler_response', dict(item))
        return item

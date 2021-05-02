# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface

from itemadapter import ItemAdapter
from news_crawl.models.mongo_model import MongoModel
from news_crawl.models.crawler_response_model import CrawlerResponseModel
from news_crawl.models.crawler_controller_model import CrawlerControllerModel


class MongoPipeline(object):

    def __init__(self):
        pass

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):

        # SitemapSpiderの場合、sitemapでどこまで見たか記録する。
        if spider.spider_type == 'SitemapSpider':
            crawler_controller = CrawlerControllerModel(spider.mongo)
            crawler_controller.update(
                {'domain': spider.domain_name},
                {'domain': spider.domain_name,
                 'spider_name': {
                     spider.name: {
                         'latest_lastmod': spider.latest_lastmod,
                         'latest_url': spider.latest_url,
                         'crawl_start_time': spider.crawl_start_time_iso
                     }
                 }}
            )

        spider.mongo.close()

    def process_item(self, item, spider):

        mongo: MongoModel = spider.mongo
        crawler_response = CrawlerResponseModel(mongo)
        crawler_response.insert_one(dict(item))

        return item

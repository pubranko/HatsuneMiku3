# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from BrownieAtelierMongo.models.crawler_response_model import CrawlerResponseModel


class NewsCrawlItem(scrapy.Item):
    _id = scrapy.Field()
    domain = scrapy.Field()
    url = scrapy.Field()
    response_time = scrapy.Field()
    response_headers = scrapy.Field()
    response_body = scrapy.Field()
    spider_version_info = scrapy.Field()
    crawling_start_time = scrapy.Field()
    source_of_information = scrapy.Field()

    def __repr__(self):
        # 当クラスのurl,title,contentを引数に、当クラスのインスタンス化をしているようだ。
        # ログからresponse_headers,response_bodyを削除するために細工
        p: NewsCrawlItem = NewsCrawlItem(self)
        del p[CrawlerResponseModel.DOMAIN]
        del p[CrawlerResponseModel.URL]
        del p[CrawlerResponseModel.RESPONSE_TIME]
        del p[CrawlerResponseModel.RESPONSE_HEADERS]
        del p[CrawlerResponseModel.RESPONSE_BODY]
        del p[CrawlerResponseModel.SPIDER_VERSION_INFO]
        del p[CrawlerResponseModel.CRAWLING_START_TIME]
        del p[CrawlerResponseModel.SOURCE_OF_INFORMATION]
        return super(NewsCrawlItem, p).__repr__()

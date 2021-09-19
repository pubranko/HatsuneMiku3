# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class NewsCrawlItem(scrapy.Item):
    _id = scrapy.Field()
    domain = scrapy.Field()
    url = scrapy.Field()
    response_time = scrapy.Field()
    response_headers = scrapy.Field()
    response_body = scrapy.Field()
    spider_version_info = scrapy.Field()
    crawling_start_time = scrapy.Field()
    sitemap_data = scrapy.Field()

    def __repr__(self):
        # 当クラスのurl,title,contentを引数に、当クラスのインスタンス化をしているようだ。
        # ログからresponse_headers,response_bodyを削除するために細工
        p: NewsCrawlItem = NewsCrawlItem(self)
        del p['domain']
        del p['url']
        del p['response_time']
        del p['response_headers']
        del p['response_body']
        del p['spider_version_info']
        del p['crawling_start_time']
        del p['sitemap_data']
        return super(NewsCrawlItem, p).__repr__()

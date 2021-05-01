# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class NewsCrawlItem(scrapy.Item):
    _id = scrapy.Field()
    response_time = scrapy.Field()
    url = scrapy.Field()
    response_headers = scrapy.Field()
    response_body = scrapy.Field()

    #ログからresponse_headers,response_bodyを削除するために細工
    def __repr__(self):
        p = NewsCrawlItem(self)              #当クラスのurl,title,contentを引数に、当クラスのインスタンス化をしているようだ。
        del p['response_headers']
        del p['response_body']
        return super(NewsCrawlItem,p).__repr__()     #super。クラスの多重継承（？）ができるらしい。初心者には難しいよ〜、、、

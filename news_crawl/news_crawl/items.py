# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class NewsCrawlItem(scrapy.Item):
    _id = scrapy.Field()
    response_time = scrapy.Field()
    url = scrapy.Field()
    response = scrapy.Field()

    #ログへの出力時、長くなりすぎないように工夫しているようだ、、、難しいw
    def __repr__(self):
        #print('=== items.py ===')   #pipelinesのprocess_itemより呼び出されているようだ、、、
        p = NewsCrawlItem(self)              #当クラスのurl,title,contentを引数に、当クラスのインスタンス化をしているようだ。
        p['response'] = p['response'][:10]
        return super(NewsCrawlItem,p).__repr__()     #super。クラスの多重継承（？）ができるらしい。初心者には難しいよ〜、、、

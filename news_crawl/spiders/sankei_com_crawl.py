import scrapy
from news_crawl.spiders.extensions_sitemap import ExtensionsSitemapSpider

class SankeiComCrawlSpider(ExtensionsSitemapSpider):
    name = 'sankei_com_sitemap'
    allowed_domains = ['sankei.com']
    sitemap_urls = ['https://www.sankei.com/sitemap.xml', ]
    _domain_name = 'sample_com'      # 各種処理で使用するドメイン名の一元管理


    #def parse(self, response):
    #    pass

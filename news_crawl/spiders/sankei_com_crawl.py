import scrapy
from news_crawl.spiders.extensions_sitemap_filter_type1 import ExtensionsSitemapFilterTyep1Spider

class SankeiComCrawlSpider(ExtensionsSitemapFilterTyep1Spider):
    name = 'sankei_com_sitemap'
    allowed_domains = ['sankei.com']
    sitemap_urls = ['https://www.sankei.com/sitemap.xml', ]
    _domain_name:str = 'sankei_com'         # 各種処理で使用するドメイン名の一元管理

    #def parse(self, response):
    #    pass

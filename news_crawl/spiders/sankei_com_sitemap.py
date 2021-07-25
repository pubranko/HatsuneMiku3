from news_crawl.spiders.extensions_class.extensions_sitemap import ExtensionsSitemapSpider


class SankeiComSitemapSpider(ExtensionsSitemapSpider):
    name: str = 'sankei_com_sitemap'
    allowed_domains: list = ['sankei.com']
    sitemap_urls: list = ['https://feed.etf.sankei.com/global/sitemap', ]
    _domain_name: str = 'sankei_com'        # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

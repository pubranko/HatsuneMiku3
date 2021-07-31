from news_crawl.spiders.extensions_class.extensions_sitemap import ExtensionsSitemapSpider


class SankeiComSitemapSpider(ExtensionsSitemapSpider):
    name: str = 'sankei_com_sitemap'
    allowed_domains: list = ['sankei.com']
    sitemap_urls: list = [
        'https://www.sankei.com/robots.txt',
        #'https://feed.etf.sankei.com/global/sitemap',
    ]
    _domain_name: str = 'sankei_com'        # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    custom_settings: dict = {
        'DEPTH_LIMIT': 2,
        'DEPTH_STATS_VERBOSE': True,
        # 'LOG_FILE' : 'logs/test_log('+ name +').txt',
    }
